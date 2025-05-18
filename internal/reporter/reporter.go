package reporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/senbaris/clustereye-agent/internal/collector/mongo"
	"github.com/senbaris/clustereye-agent/internal/collector/mssql"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/alarm"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/pkg/utils"
	"google.golang.org/grpc/backoff"
)

// QueryProcessor, sorgu işleme mantığını temsil eder
type QueryProcessor struct {
	db  *sql.DB
	cfg *config.AgentConfig
}

// NewQueryProcessor, yeni bir sorgu işleyici oluşturur
func NewQueryProcessor(db *sql.DB, cfg *config.AgentConfig) *QueryProcessor {
	return &QueryProcessor{
		db:  db,
		cfg: cfg,
	}
}

// processQuery, gelen sorguyu işler ve sonucu döndürür
func (p *QueryProcessor) processQuery(command string, database string) map[string]interface{} {
	const (
		maxRows          = 100         // Maksimum satır sayısı
		maxValueLen      = 1000        // Maksimum değer uzunluğu
		maxTotalDataSize = 1024 * 1024 // Maksimum toplam veri boyutu (1 MB)
	)

	// Debug için hangi veritabanında çalıştığımızı kontrol et
	var currentDB string
	p.db.QueryRow("SELECT current_database()").Scan(&currentDB)
	log.Printf("[DEBUG] Mevcut veritabanı: %s, İstenen veritabanı: %s", currentDB, database)

	// Veritabanını seç
	if database != "" && database != currentDB {
		log.Printf("[DEBUG] Veritabanı değişikliği gerekiyor: %s -> %s", currentDB, database)

		// Mevcut bağlantıdan host bilgisini al
		var currentHost string
		err := p.db.QueryRow("SELECT inet_server_addr()").Scan(&currentHost)
		if err != nil {
			currentHost = "localhost"
			log.Printf("[DEBUG] Host bilgisi alınamadı, varsayılan olarak localhost kullanılacak")
		}

		// Mevcut bağlantıdan port bilgisini al
		var currentPort string
		err = p.db.QueryRow("SELECT inet_server_port()").Scan(&currentPort)
		if err != nil {
			currentPort = "5432"
			log.Printf("[DEBUG] Port bilgisi alınamadı, varsayılan olarak 5432 kullanılacak")
		}

		// Config'den kullanıcı adı ve şifreyi al
		user := p.cfg.PostgreSQL.User
		pass := p.cfg.PostgreSQL.Pass

		// Connection string'i oluştur
		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			currentHost, currentPort, user, pass, database)

		log.Printf("[DEBUG] Yeni connection string oluşturuldu (kullanıcı: %s, host: %s, port: %s, db: %s)",
			user, currentHost, currentPort, database)

		// Mevcut bağlantıyı kapat
		if p.db != nil {
			if err := p.db.Close(); err != nil {
				log.Printf("[WARN] Mevcut bağlantı kapatılırken hata: %v", err)
			}
		}

		// Yeni bağlantı aç
		newDB, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Printf("[ERROR] Yeni veritabanı bağlantısı açılamadı: %v", err)
			return map[string]interface{}{
				"status":  "error",
				"message": fmt.Sprintf("Yeni veritabanı bağlantısı açılamadı: %v", err),
			}
		}

		// Bağlantıyı test et
		if err := newDB.Ping(); err != nil {
			log.Printf("[ERROR] Yeni veritabanı bağlantısı test edilemedi: %v", err)
			newDB.Close()
			return map[string]interface{}{
				"status":  "error",
				"message": fmt.Sprintf("Yeni veritabanı bağlantısı test edilemedi: %v", err),
			}
		}

		// Bağlantıyı güncelle
		p.db = newDB

		// Search path'i ayarla
		_, err = p.db.Exec("SET search_path TO public, pg_catalog")
		if err != nil {
			log.Printf("[WARN] Search path ayarlanamadı: %v", err)
		}

		// Seçilen veritabanını doğrula
		err = p.db.QueryRow("SELECT current_database()").Scan(&currentDB)
		if err != nil {
			log.Printf("[ERROR] Veritabanı kontrolü yapılamadı: %v", err)
		} else {
			log.Printf("[DEBUG] Şu anki veritabanı: %s", currentDB)
		}
	}

	// İstatistik görünümlerine erişim kontrolü
	var hasAccess bool
	err := p.db.QueryRow("SELECT TRUE FROM pg_roles WHERE rolname = current_user AND rolsuper").Scan(&hasAccess)
	if err != nil || !hasAccess {
		log.Printf("İstatistik görünümlerine erişim için superuser yetkisi gerekiyor: %v", err)
	}

	// "ping" komutu için özel işleme
	if strings.ToLower(command) == "ping" {
		log.Printf("QueryProcessor: Ping komutu algılandı, özel yanıt dönüyor")
		return map[string]interface{}{
			"status":  "success",
			"message": "pong",
		}
	}

	// Query başlangıç zamanını kaydet
	startTime := time.Now()

	// Özel sorgu kontrolü - unused indexes
	if strings.Contains(command, "unused_indexes") || strings.Contains(command, "stats_child.idx_scan = 0") {
		log.Printf("[DEBUG] Unused indexes sorgusu tespit edildi, özel sorgu kullanılacak")

		// Debug için önce tüm indexleri listele
		debugRows, err := p.db.Query(`
			SELECT 
				n.nspname as schemaname,
				t.relname as table_name,
				i.relname as index_name,
				s.idx_scan,
				pg_relation_size(i.oid) as index_size,
				idx.indisunique,
				EXISTS (
					SELECT 1 
					FROM pg_catalog.pg_constraint cc 
					WHERE cc.conindid = i.oid
				) as is_constraint,
				array_to_string(idx.indkey, ',') as index_cols
			FROM pg_class i
			JOIN pg_index idx ON idx.indexrelid = i.oid
			JOIN pg_class t ON t.oid = idx.indrelid
			JOIN pg_namespace n ON n.oid = i.relnamespace
			LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
			WHERE i.relkind = 'i'
			AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
			ORDER BY n.nspname, t.relname;
		`)
		if err != nil {
			log.Printf("[DEBUG] Tüm indexler listelenirken hata: %v", err)
		} else {
			defer debugRows.Close()
			log.Printf("[DEBUG] Veritabanındaki tüm indexler:")
			for debugRows.Next() {
				var schema, table, index string
				var idxScan *int
				var size int64
				var isUnique, isConstraint bool
				var indexCols string
				if err := debugRows.Scan(&schema, &table, &index, &idxScan, &size, &isUnique, &isConstraint, &indexCols); err == nil {
					scanCount := 0
					if idxScan != nil {
						scanCount = *idxScan
					}
					log.Printf("[DEBUG] Index: %s.%s.%s, Scan Count: %d, Size: %d, Unique: %v, Constraint: %v, Cols: %s",
						schema, table, index, scanCount, size, isUnique, isConstraint, indexCols)
				} else {
					log.Printf("[ERROR] Index satırı okunurken hata: %v", err)
				}
			}
		}

		// Ana sorguyu çalıştır
		command = `
		SELECT 
			'regular index' as indextype,
			n.nspname as schemaname,
			t.relname as tablename,
			i.relname as indexname,
			pg_catalog.pg_get_indexdef(i.oid) as idx_columns,
			COALESCE(s.idx_scan, 0) as idx_scan_count,
			pg_size_pretty(pg_relation_size(i.oid)) as index_size,
			pg_relation_size(i.oid) as index_size_bytes
		FROM pg_class i
		JOIN pg_index idx ON idx.indexrelid = i.oid
		JOIN pg_class t ON t.oid = idx.indrelid
		JOIN pg_namespace n ON n.oid = i.relnamespace
		LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
		WHERE i.relkind = 'i'
		AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
		AND NOT idx.indisunique
		AND NOT EXISTS (
			SELECT 1 
			FROM pg_catalog.pg_constraint cc 
			WHERE cc.conindid = i.oid
		)
		AND NOT EXISTS (
			SELECT 1 
			FROM pg_inherits pi 
			WHERE pi.inhrelid = t.oid
		)
		AND COALESCE(s.idx_scan, 0) = 0
		ORDER BY n.nspname, t.relname, pg_relation_size(i.oid) DESC;`
	}

	// Sorguyu çalıştır
	log.Printf("[DEBUG] Çalıştırılan sorgu: %s", command)
	rows, err := p.db.Query(command)
	if err != nil {
		log.Printf("[ERROR] Sorgu çalıştırma hatası: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}
	}
	defer rows.Close()

	// Sütun isimlerini al
	columns, err := rows.Columns()
	if err != nil {
		log.Printf("[ERROR] Sütun bilgileri alınamadı: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}
	}
	log.Printf("[DEBUG] Sorgu sonucu sütunlar: %v", columns)

	// Sorgu açıklaması için ID al
	var queryDesc string
	if strings.Contains(strings.ToLower(command), "pg_cachehitratio") {
		queryDesc = "Cache Hit Ratio"
	} else {
		queryDesc = command
	}
	log.Printf("İşleniyor: %s", trimString(queryDesc, 100))

	// Sonuçları işle
	var results []map[string]interface{}
	var totalDataSize int
	rowCount := 0

	for rows.Next() {
		// Satır sayısını kontrol et
		if rowCount >= maxRows {
			log.Printf("Maksimum satır sayısına ulaşıldı (%d), sonuçlar kırpılıyor", maxRows)
			break
		}
		rowCount++

		// Sütun sayısı kadar pointer oluştur
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Satırı oku
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("[ERROR] Satır okuma hatası: %v", err)
			return map[string]interface{}{
				"status":  "error",
				"message": err.Error(),
			}
		}

		// Satırı map'e dönüştür
		rowMap := make(map[string]interface{})
		rowSize := 0
		for i, col := range columns {
			val := values[i]
			var strVal string

			switch v := val.(type) {
			case []byte:
				strVal = string(v)
				if len(strVal) > maxValueLen {
					strVal = strVal[:maxValueLen]
					log.Printf("Değer uzunluğu kırpıldı: %s = %d karakterden %d karaktere",
						col, len(v), maxValueLen)
				}
				rowMap[col] = strVal
				rowSize += len(strVal)
			case time.Time:
				strVal = v.Format(time.RFC3339)
				rowMap[col] = strVal
				rowSize += len(strVal)
			case nil:
				log.Printf("[DEBUG] NULL değer: %s", col)
				rowMap[col] = ""
			default:
				// Diğer tipleri string'e çevir
				strVal = fmt.Sprintf("%v", v)
				if len(strVal) > maxValueLen {
					strVal = strVal[:maxValueLen]
					log.Printf("Değer uzunluğu kırpıldı: %s = %d karakterden %d karaktere",
						col, len(strVal), maxValueLen)
				}
				rowMap[col] = v
				rowSize += len(strVal)
			}
		}

		log.Printf("[DEBUG] Satır: %d, İçerik: %v", rowCount, rowMap)

		// Toplam veri boyutunu kontrol et
		totalDataSize += rowSize
		if totalDataSize > maxTotalDataSize {
			log.Printf("Maksimum veri boyutuna ulaşıldı (%d), sonuçlar kırpılıyor", maxTotalDataSize)
			break
		}

		results = append(results, rowMap)
	}

	// Sonuçları kontrol et
	if err := rows.Err(); err != nil {
		log.Printf("[ERROR] Satır işlemede hata: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}
	}

	// İşlem süresini hesapla
	duration := time.Since(startTime).Milliseconds()
	log.Printf("Sorgu işleme tamamlandı: %d satır, %d bayt, %d ms",
		rowCount, totalDataSize, duration)

	// Sonuç satırlarının sayısını kontrol et
	if len(results) == 0 {
		log.Printf("[INFO] Sorgu sonuç döndürmedi (empty result set)")
		// Boş sonuç durumunda bile bilgi döndür
		return map[string]interface{}{
			"status":        "success",
			"message":       "Sorgu başarıyla çalıştı ama sonuç bulunamadı",
			"rows_returned": 0,
			"duration_ms":   duration,
		}
	}

	// Tek satır sonuç varsa düz map olarak döndür
	if len(results) == 1 {
		result := results[0]
		result["status"] = "success"
		result["rows_processed"] = rowCount
		result["data_size"] = totalDataSize
		result["duration_ms"] = duration
		return result
	}

	// Birden fazla satır varsa düz bir map yapısına dönüştür
	flatResult := make(map[string]interface{})
	flatResult["status"] = "success"
	flatResult["row_count"] = len(results)
	flatResult["rows_processed"] = rowCount
	flatResult["data_size"] = totalDataSize
	flatResult["duration_ms"] = duration
	flatResult["data_truncated"] = (rowCount >= maxRows || totalDataSize >= maxTotalDataSize)

	// Her bir satırı düz bir map'e ekle
	for i, row := range results {
		for key, value := range row {
			// Her bir alanı index ile birlikte sakla
			flatResult[fmt.Sprintf("%s_%d", key, i)] = value
		}
	}

	return flatResult
}

// Reporter toplanan verileri merkezi sunucuya raporlar
type Reporter struct {
	cfg          *config.AgentConfig
	grpcClient   *grpc.ClientConn
	stream       pb.AgentService_ConnectClient
	stopCh       chan struct{}
	isListening  bool
	reportTicker *time.Ticker        // Periyodik raporlama için ticker
	alarmMonitor *alarm.AlarmMonitor // Alarm izleme sistemi
	platform     string              // Added to keep track of the platform
}

// NewReporter yeni bir Reporter örneği oluşturur
func NewReporter(cfg *config.AgentConfig) *Reporter {
	return &Reporter{
		cfg:         cfg,
		stopCh:      make(chan struct{}),
		isListening: false,
		platform:    "", // Platform bilgisi AgentRegistration sırasında set edilecek
	}
}

// Connect GRPC sunucusuna bağlanır
func (r *Reporter) Connect() error {
	// GRPC bağlantısı oluştur
	log.Printf("ClusterEye sunucusuna bağlanıyor: %s", r.cfg.GRPC.ServerAddress)

	// gRPC bağlantı seçeneklerini yapılandır
	opts := []grpc.DialOption{
		// TLS olmadan bağlantı için
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		// Bağlantı parametrelerini ayarla
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1.0 * time.Second,
				Multiplier: 1.5,
				Jitter:     0.2,
				MaxDelay:   20 * time.Second,
			},
			MinConnectTimeout: 10 * time.Second,
		}),

		// Otomatik yeniden bağlantı etkinleştir
		grpc.WithDisableServiceConfig(),

		// Keep-alive seçenekleri
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second, // 60 saniyede bir ping (daha önce 10 saniyeydi, arttırdık)
			Timeout:             10 * time.Second, // 10 saniye ping timeout
			PermitWithoutStream: false,            // Stream yokken ping gönderme (daha önce true'ydu)
		}),
	}

	// Bağlantı için context oluştur
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()

	// gRPC bağlantısını oluştur
	conn, err := grpc.DialContext(dialCtx, r.cfg.GRPC.ServerAddress, opts...)
	if err != nil {
		return fmt.Errorf("gRPC bağlantısı kurulamadı: %v", err)
	}

	r.grpcClient = conn

	// gRPC client oluştur
	client := pb.NewAgentServiceClient(conn)

	// ÖNEMLİ: Stream bağlantısı için iptal EDİLMEYEN bir context kullan
	// defer cancel() çağrılıyordu ve fonksiyon çıkışında stream iptal ediliyordu!
	streamCtx := context.Background()

	// Stream bağlantısını başlat
	stream, err := client.Connect(streamCtx)
	if err != nil {
		r.grpcClient.Close() // Bağlantıyı temizle
		return fmt.Errorf("stream bağlantısı kurulamadı: %v", err)
	}

	r.stream = stream
	log.Printf("ClusterEye sunucusuna bağlandı: %s", r.cfg.GRPC.ServerAddress)

	return nil
}

// Disconnect GRPC bağlantısını kapatır
func (r *Reporter) Disconnect() {
	close(r.stopCh)
	r.isListening = false
	if r.reportTicker != nil {
		r.reportTicker.Stop()
	}
	if r.alarmMonitor != nil {
		r.alarmMonitor.Stop()
	}
	if r.grpcClient != nil {
		r.grpcClient.Close()
		log.Printf("GRPC bağlantısı kapatıldı")
	}
}

// Report verileri merkezi sunucuya gönderir
func (r *Reporter) Report(data *pb.PostgresInfo) error {
	// Create the complete AgentMessage with PostgresInfo as the payload
	message := &pb.AgentMessage{
		Payload: &pb.AgentMessage_PostgresInfo{
			PostgresInfo: data,
		},
	}

	// Send the complete message
	err := r.stream.Send(message)
	if err != nil {
		return err
	}

	log.Printf("Veriler raporlandı: %s", data.Hostname)
	return nil
}

// SendMSSQLInfo MSSQL bilgilerini toplar ve sunucuya gönderir
func (r *Reporter) SendMSSQLInfo() error {
	// MSSQL kolektörünü oluştur
	log.Printf("MSSQL bilgileri toplanıyor...")
	collector := mssql.NewMSSQLCollector(r.cfg)

	// MSSQL bilgilerini al
	mssqlInfo := collector.GetMSSQLInfo()
	if mssqlInfo == nil {
		return fmt.Errorf("MSSQL bilgileri alınamadı")
	}

	// MSSQL bilgilerini protobuf mesajına dönüştür
	protoInfo := mssqlInfo.ToProto()

	// MSSQL bilgilerini log'a yaz
	log.Printf("MSSQL bilgileri: IP=%s, Hostname=%s, NodeStatus=%s, Version=%s, Status=%s, Instance=%s",
		protoInfo.Ip, protoInfo.Hostname, protoInfo.NodeStatus, protoInfo.Version, protoInfo.Status, protoInfo.Instance)

	// Yeni SendMSSQLInfo RPC'sini kullanarak verileri gönder
	client := pb.NewAgentServiceClient(r.grpcClient)

	// MSSQLInfoRequest oluştur
	request := &pb.MSSQLInfoRequest{
		MssqlInfo: protoInfo,
	}

	// SendMSSQLInfo RPC'sini çağır
	response, err := client.SendMSSQLInfo(context.Background(), request)
	if err != nil {
		log.Printf("MSSQL bilgileri yeni RPC ile gönderilemedi: %v. Eski yöntem deneniyor...", err)

		// Eski yöntem: Stream üzerinden gönder
		err = r.ReportMSSQLInfo(protoInfo)
		if err != nil {
			log.Printf("MSSQL bilgileri eski yöntemle de gönderilemedi: %v", err)
			return err
		}

		log.Println("MSSQL bilgileri başarıyla eski yöntemle gönderildi")
		return nil
	}

	log.Printf("MSSQL bilgileri başarıyla gönderildi. Sunucu durumu: %s", response.Status)
	return nil
}

// ReportMSSQLInfo MSSQL bilgilerini sunucuya gönderir
func (r *Reporter) ReportMSSQLInfo(data *pb.MSSQLInfo) error {
	// Create the complete AgentMessage with MSSQLInfo as the payload
	message := &pb.AgentMessage{
		Payload: &pb.AgentMessage_MssqlInfo{
			MssqlInfo: data,
		},
	}

	// Send the complete message
	err := r.stream.Send(message)
	if err != nil {
		return fmt.Errorf("MSSQL bilgileri gönderilemedi: %v", err)
	}

	log.Printf("MSSQL bilgileri başarıyla raporlandı: %s", data.Hostname)
	return nil
}

// AgentRegistration agent bilgilerini sunucuya gönderir
func (r *Reporter) AgentRegistration(testResult string, platform string) error {
	hostname, _ := os.Hostname()
	ip := utils.GetLocalIP()

	// Platform bilgisini kaydet
	r.platform = platform

	// Agent bilgilerini hazırla
	agentInfo := &pb.AgentInfo{
		Key:          r.cfg.Key,
		AgentId:      "agent_" + hostname,
		Hostname:     hostname,
		Ip:           ip,
		Platform:     platform,
		Auth:         getAuthStatus(platform, r.cfg),
		Test:         testResult,
		PostgresUser: r.cfg.PostgreSQL.User,
		PostgresPass: r.cfg.PostgreSQL.Pass,
	}

	// Stream üzerinden agent bilgilerini göndermeyi birkaç kez deneyelim
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Stream sorunu varsa, yeniden bağlantı kur
		if attempt > 0 {
			log.Printf("Stream bağlantısı yenileniyor (deneme %d/%d)...", attempt+1, maxRetries)
			if err := r.reconnect(); err != nil {
				log.Printf("Stream yeniden bağlantı hatası: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
		}

		// Agent Message oluştur
		agentMessage := &pb.AgentMessage{
			Payload: &pb.AgentMessage_AgentInfo{
				AgentInfo: agentInfo,
			},
		}

		// Kısa bir timeout ile mesajı gönder
		sendDone := make(chan error, 1)
		go func() {
			// Mesaj gönderirken herhangi bir context iptalinden etkilenmemesi için
			// doğrudan gönder
			sendDone <- r.stream.Send(agentMessage)
		}()

		// En fazla 5 saniye bekle
		var err error
		select {
		case err = <-sendDone:
			// Mesaj gönderimi tamamlandı
		case <-time.After(5 * time.Second):
			err = fmt.Errorf("agent bilgileri gönderimi zaman aşımına uğradı")
			// Bu timeout durumunda stream hala açık olabilir, zorla kapatma
			log.Printf("Zaman aşımı nedeniyle stream yenileniyor...")
			// Burada stream'i kapatmaya çalışmak yerine, yeni bir stream oluştur
			time.Sleep(1 * time.Second)
		}

		if err != nil {
			lastErr = fmt.Errorf("agent bilgileri gönderilemedi: %v", err)
			log.Printf("Agent bilgisi gönderimi başarısız (deneme %d/%d): %v",
				attempt+1, maxRetries, err)

			// EOF veya bağlantı hatası olup olmadığını kontrol et
			if err == io.EOF || strings.Contains(err.Error(), "transport") ||
				strings.Contains(err.Error(), "connection") {
				log.Printf("Bağlantı hatası tespit edildi, yeniden bağlanılacak...")
				time.Sleep(2 * time.Second)
				continue
			}

			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		log.Printf("Agent bilgileri stream üzerinden gönderildi (Platform: %s)", platform)

		// Kayıt işlemi tamamlandıktan sonra komut dinlemeyi başlat
		if !r.isListening {
			go r.listenForCommands()
			r.isListening = true
			log.Println("Sunucudan komut dinleme başlatıldı")
		}

		// Periyodik raporlamayı başlat
		r.StartPeriodicReporting(30*time.Second, platform)

		// Platform seçimine göre ilk bilgileri gönder
		if platform == "postgres" {
			// İlk PostgreSQL bilgilerini gönder
			if err := r.SendPostgresInfo(); err != nil {
				log.Printf("PostgreSQL bilgileri gönderilemedi: %v", err)
			}
		} else if platform == "mongo" {
			// İlk MongoDB bilgilerini gönder
			if err := r.SendMongoInfo(); err != nil {
				log.Printf("MongoDB bilgileri gönderilemedi: %v", err)
			}
		} else if platform == "mssql" {
			// İlk MSSQL bilgilerini gönder
			if err := r.SendMSSQLInfo(); err != nil {
				log.Printf("MSSQL bilgileri gönderilemedi: %v", err)
			}
		}

		// Alarm izleme sistemini başlat
		agentID := "agent_" + hostname
		client := pb.NewAgentServiceClient(r.grpcClient)

		// Client yenileme callback'i tanımla
		clientRefreshCallback := func() (pb.AgentServiceClient, error) {
			// Client'ı yenilemek için önce bağlantıyı yenileyip sonra yeni bir client oluştur
			if err := r.reconnect(); err != nil {
				return nil, fmt.Errorf("client yenilenemedi: %v", err)
			}
			return pb.NewAgentServiceClient(r.grpcClient), nil
		}

		r.alarmMonitor = alarm.NewAlarmMonitor(client, agentID, r.cfg, platform, clientRefreshCallback)
		r.alarmMonitor.Start()

		return nil // Başarılı
	}

	return fmt.Errorf("agent bilgileri gönderilemedi: %v", lastErr)
}

// getAuthStatus belirli platform için auth durumunu döndürür
func getAuthStatus(platform string, cfg *config.AgentConfig) bool {
	if platform == "postgres" {
		return cfg.PostgreSQL.Auth
	} else if platform == "mongo" {
		return cfg.Mongo.Auth
	}
	return false
}

// SendSystemMetrics sistem metriklerini sunucuya gönderir
func (r *Reporter) SendSystemMetrics(ctx context.Context, req *pb.SystemMetricsRequest) (*pb.SystemMetricsResponse, error) {
	// Sistem metriklerini topla
	metrics := postgres.GetSystemMetrics()

	// Metrik içeriğini logla
	log.Printf("Gönderilen metrikler boyutları:")
	log.Printf("  cpu_usage: %v (size: ~8 bytes)", metrics.CpuUsage)
	log.Printf("  cpu_cores: %v (size: ~4 bytes)", metrics.CpuCores)
	log.Printf("  memory_usage: %v (size: ~8 bytes)", metrics.MemoryUsage)
	log.Printf("  total_memory: %v (size: ~8 bytes)", metrics.TotalMemory)
	log.Printf("  free_memory: %v (size: ~8 bytes)", metrics.FreeMemory)
	log.Printf("  load_average_1m: %v (size: ~8 bytes)", metrics.LoadAverage_1M)
	log.Printf("  load_average_5m: %v (size: ~8 bytes)", metrics.LoadAverage_5M)
	log.Printf("  load_average_15m: %v (size: ~8 bytes)", metrics.LoadAverage_15M)
	log.Printf("  total_disk: %v (size: ~8 bytes)", metrics.TotalDisk)
	log.Printf("  free_disk: %v (size: ~8 bytes)", metrics.FreeDisk)
	log.Printf("  os_version: %v (size: %d bytes)", metrics.OsVersion, len(metrics.OsVersion))
	log.Printf("  kernel_version: %v (size: %d bytes)", metrics.KernelVersion, len(metrics.KernelVersion))
	log.Printf("  uptime: %v (size: ~8 bytes)", metrics.Uptime)

	// Yanıtı oluştur
	response := &pb.SystemMetricsResponse{
		Status:  "success",
		Metrics: metrics,
	}

	return response, nil
}

// listenForCommands sunucudan gelen komutları dinler
func (r *Reporter) listenForCommands() {
	log.Println("Komut dinleme döngüsü başlatıldı")

	var db *sql.DB
	var processor *QueryProcessor
	var mssqlProcessor *MSSQLQueryProcessor

	// Platform bazlı veritabanı bağlantısı ve işleyici oluştur
	if r.platform == "postgres" {
		// Veritabanı bağlantısını aç
		var err error
		db, err = postgres.OpenDB()
		if err != nil {
			log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
			return
		}
		defer db.Close()

		// Sorgu işleyiciyi oluştur
		processor = NewQueryProcessor(db, r.cfg)
	} else if r.platform == "mssql" {
		// MSSQL sorgu işleyiciyi oluştur
		mssqlProcessor = NewMSSQLQueryProcessor(r.cfg)
		log.Println("MSSQL sorgu işleyicisi oluşturuldu")
	}

	// Mesaj işleme durumu
	isProcessingMetrics := false
	isProcessingQuery := false
	lastMessageType := "none"

	// Bağlantı hata sayacı - üst üste hata olduğunda yeniden bağlantı kurma
	connectionErrorCount := 0
	maxConnectionErrors := 3

	// Panic recovery - eğer beklenmeyen bir hata olursa, process'i çökertme
	defer func() {
		if panicVal := recover(); panicVal != nil {
			log.Printf("Komut dinleme işlemi panic nedeniyle durduruldu: %v", panicVal)

			// Yeniden bağlanmayı dene
			time.Sleep(3 * time.Second) // Kısa bir bekleme
			if err := r.reconnect(); err != nil {
				log.Printf("Yeniden bağlantı başarısız: %v", err)
				return
			}

			// Yeniden dinlemeyi başlat
			log.Println("Dinleme işlemi yeniden başlatılıyor...")
			time.Sleep(1 * time.Second)
			go r.listenForCommands()
		}
	}()

	for {
		select {
		case <-r.stopCh:
			log.Println("Komut dinleme durduruldu")
			return
		default:
			// Her iterasyonda kısa bir bekleme süresi ekleyelim
			// Bu şekilde sürekli polling yapmayı önleriz
			time.Sleep(200 * time.Millisecond)

			// Önceki mesaj işleme tamamlandıysa durum bilgisini temizle
			if !isProcessingMetrics && !isProcessingQuery {
				lastMessageType = "none"
			}

			// Stream bağlantısı var mı kontrol et
			if r.stream == nil {
				log.Println("Stream bağlantısı yok, yeniden bağlanılıyor...")
				if err := r.reconnect(); err != nil {
					log.Printf("Yeniden bağlantı başarısız: %v. 5 saniye sonra tekrar denenecek.", err)
					time.Sleep(5 * time.Second)
					continue
				}
			}

			// Sunucudan mesaj bekle
			log.Printf("Sunucudan mesaj bekleniyor... (Son mesaj tipi: %s)", lastMessageType)

			// Stream'den mesaj al
			msg, err := r.stream.Recv()
			if err != nil {
				connectionErrorCount++

				// Özel ENHANCE_YOUR_CALM hatası tespiti
				if strings.Contains(err.Error(), "ENHANCE_YOUR_CALM") || strings.Contains(err.Error(), "too_many_pings") {
					log.Printf("Sunucu ping frekansından dolayı bağlantıyı kapattı (ENHANCE_YOUR_CALM). Bekleyip yeniden bağlanılacak...")

					// ENHANCE_YOUR_CALM hatası durumunda 10 saniye bekle
					time.Sleep(10 * time.Second)

					// ENHANCE_YOUR_CALM durumunda hemen yeniden bağlan
					if err := r.reconnect(); err != nil {
						log.Printf("ENHANCE_YOUR_CALM sonrası yeniden bağlantı başarısız: %v. 30 saniye sonra tekrar denenecek.", err)
						time.Sleep(30 * time.Second) // Uzun bir bekleme süresi
					} else {
						log.Printf("ENHANCE_YOUR_CALM sonrası yeniden bağlantı başarılı!")
						connectionErrorCount = 0 // Sayacı sıfırla
					}

					continue
				} else if err == io.EOF {
					log.Printf("Sunucu bağlantıyı kapattı (EOF). Yeniden bağlanmayı deneyeceğim...")
					// EOF durumunda doğrudan yeniden bağlan
					if err := r.reconnect(); err != nil {
						log.Printf("EOF sonrası yeniden bağlantı başarısız: %v. 5 saniye sonra tekrar denenecek.", err)
						time.Sleep(5 * time.Second)
					} else {
						log.Printf("EOF sonrası yeniden bağlantı başarılı!")
						connectionErrorCount = 0 // Sayacı sıfırla
					}
				} else if strings.Contains(err.Error(), "transport is closing") || strings.Contains(err.Error(), "connection is closing") {
					log.Printf("Sunucu bağlantıyı kapatıyor: %v. Yeniden bağlanmayı deneyeceğim...", err)
					// Bağlantı kapanma durumunda doğrudan yeniden bağlan
					time.Sleep(2 * time.Second) // Kısa bir bekleme
					if err := r.reconnect(); err != nil {
						log.Printf("Bağlantı kapanma sonrası yeniden bağlantı başarısız: %v. 5 saniye sonra tekrar denenecek.", err)
						time.Sleep(5 * time.Second)
					} else {
						log.Printf("Bağlantı kapanma sonrası yeniden bağlantı başarılı!")
						connectionErrorCount = 0 // Sayacı sıfırla
					}
				} else {
					log.Printf("Sunucudan mesaj alınırken hata: %v", err)
				}

				// Üst üste bağlantı hataları olursa yeniden bağlan
				if connectionErrorCount >= maxConnectionErrors {
					log.Printf("Üst üste %d bağlantı hatası. Yeniden bağlanmayı deniyorum...", connectionErrorCount)
					// 5 saniye bekle ve yeniden bağlanmayı dene
					time.Sleep(5 * time.Second)
					if err := r.reconnect(); err != nil {
						log.Printf("Yeniden bağlantı başarısız: %v. 10 saniye sonra tekrar denenecek.", err)
						time.Sleep(10 * time.Second)
					} else {
						connectionErrorCount = 0 // Sayacı sıfırla
					}
				}

				time.Sleep(1 * time.Second) // Kısa bir bekleme
				continue
			}

			// Bağlantı hata sayacını sıfırla - başarılı bir mesaj aldık
			connectionErrorCount = 0

			// Gelen mesajın tipini logla
			messageType := "unknown"
			if query := msg.GetQuery(); query != nil {
				messageType = "query"
			} else if metricsReq := msg.GetMetricsRequest(); metricsReq != nil {
				messageType = "metrics_request"
			}
			log.Printf("Sunucudan mesaj alındı - Tip: %s (Son mesaj tipi: %s)", messageType, lastMessageType)

			// NOT: Burada bir sleep konulmuştu, fakat döngünün başında genel bir sleep zaten eklendi

			if query := msg.GetQuery(); query != nil {
				// Eğer şu anda metrik işleme devam ediyorsa, uyarı ver
				if isProcessingMetrics {
					log.Printf("UYARI: Metrik işleme devam ederken sorgu alındı")
				}

				isProcessingQuery = true
				lastMessageType = "query"
				log.Printf("Yeni sorgu geldi: %s (ID: %s)", trimString(query.Command, 100), query.QueryId)

				// PostgreSQL promotion özel işleme:
				// Format: postgres_promote|<data_directory>|<query_id>
				if strings.HasPrefix(query.Command, "postgres_promote") {
					log.Printf("PostgreSQL promotion komutu tespit edildi: %s", query.Command)

					// Komuttan parametreleri çıkar
					parts := strings.Split(query.Command, "|")
					if len(parts) >= 2 {
						// Extract data directory (ikinci parametre)
						dataDir := parts[1]

						// Extract job ID (üçüncü parametre, varsa)
						jobID := query.QueryId // Varsayılan olarak mevcut sorgu ID'sini kullan
						if len(parts) >= 3 && parts[2] != "" {
							jobID = parts[2] // Özel job ID kullan
						}

						log.Printf("PostgreSQL promotion işlemi başlatılıyor. DataDir=%s, JobID=%s", dataDir, jobID)

						// Hostname'i al
						hostname, _ := os.Hostname()
						agentID := "agent_" + hostname

						// RPC isteği oluştur
						promoteReq := &pb.PostgresPromoteMasterRequest{
							JobId:         jobID,
							AgentId:       agentID,
							NodeHostname:  hostname,
							DataDirectory: dataDir,
						}

						// PromotePostgresToMaster RPC'sini çağır
						go func() {
							// Bu işlemi arka planda çalıştır, yoksa uzun sürebilir
							promoteResp, err := r.PromotePostgresToMaster(context.Background(), promoteReq)
							if err != nil {
								log.Printf("PostgreSQL promotion RPC işlemi başarısız: %v", err)
								// Hata durumunda sonucu gönder
								errorResult := map[string]interface{}{
									"status":  "error",
									"message": fmt.Sprintf("PostgreSQL promotion RPC işlemi başarısız: %v", err),
								}
								sendQueryResult(r.stream, query.QueryId, errorResult)
							} else {
								// RPC yanıtını dönüştür
								var statusStr string
								if promoteResp.Status == pb.JobStatus_JOB_STATUS_COMPLETED {
									statusStr = "success"
								} else {
									statusStr = "error"
								}

								// Sonucu gönder
								result := map[string]interface{}{
									"status":  statusStr,
									"message": promoteResp.Result,
									"job_id":  jobID,
								}

								if promoteResp.ErrorMessage != "" {
									result["error"] = promoteResp.ErrorMessage
								}

								sendQueryResult(r.stream, query.QueryId, result)
							}
						}()

						// Hemen başlatıldı bilgisi dön
						initialResult := map[string]interface{}{
							"status":  "accepted",
							"message": fmt.Sprintf("PostgreSQL promotion işlemi başlatıldı (JobID: %s, DataDir: %s)", jobID, dataDir),
							"job_id":  jobID,
						}
						sendQueryResult(r.stream, query.QueryId, initialResult)

						isProcessingQuery = false
						continue
					} else {
						// Eksik parametre
						errorResult := map[string]interface{}{
							"status":  "error",
							"message": "Geçersiz PostgreSQL promotion komutu. Doğru format: postgres_promote|/data/directory|[job_id]",
						}
						sendQueryResult(r.stream, query.QueryId, errorResult)
						isProcessingQuery = false
						continue
					}
				}

				// MSSQL sorguları için özel işleme
				if r.platform == "mssql" && mssqlProcessor != nil {
					// MSSQL sorgu ID'lerine göre özel işleme
					if query.QueryId == "mssql_top_queries" || query.QueryId == "mssql_blocking" ||
						strings.HasPrefix(query.QueryId, "mssql_") {

						log.Printf("MSSQL sorgusu tespit edildi (ID: %s)", query.QueryId)

						// MSSQL sorgu açıklama (explain) isteği için özel işleme
						if strings.HasPrefix(query.Command, "MSSQL_EXPLAIN") {
							log.Printf("MSSQL explain sorgusu tespit edildi: %s", shortenString(query.Command, 100))

							// Protokol formatını parse et - ilk pipe (|) karakterini bul
							firstPipeIndex := strings.Index(query.Command, "|")
							if firstPipeIndex == -1 {
								log.Printf("Geçersiz MSSQL_EXPLAIN format: %s", shortenString(query.Command, 100))

								errorResult := map[string]interface{}{
									"status":  "error",
									"message": "Geçersiz MSSQL_EXPLAIN format. Doğru format: MSSQL_EXPLAIN|database|query",
								}

								sendQueryResult(r.stream, query.QueryId, errorResult)
								isProcessingQuery = false
								continue
							}

							// Komuttaki ilk kısmı atla (MSSQL_EXPLAIN)
							remainingCommand := query.Command[firstPipeIndex+1:]

							// İkinci pipe karakterini bul
							secondPipeIndex := strings.Index(remainingCommand, "|")
							if secondPipeIndex == -1 {
								log.Printf("Geçersiz MSSQL_EXPLAIN format: %s", shortenString(query.Command, 100))

								errorResult := map[string]interface{}{
									"status":  "error",
									"message": "Geçersiz MSSQL_EXPLAIN format. Doğru format: MSSQL_EXPLAIN|database|query",
								}

								sendQueryResult(r.stream, query.QueryId, errorResult)
								isProcessingQuery = false
								continue
							}

							// Database ve sorguyu çıkar
							database := remainingCommand[:secondPipeIndex]
							actualQuery := remainingCommand[secondPipeIndex+1:]

							log.Printf("MSSQL explain işleniyor: Database=%s, Sorgu=%s", database, shortenString(actualQuery, 100))

							// XML execution planını al
							result := mssqlProcessor.processExplainMSSQLQuery(actualQuery, database)

							// Sonucu gönder
							sendQueryResult(r.stream, query.QueryId, result)
							isProcessingQuery = false
							continue
						}

						// MSSQL Best Practices Analysis için özel işleme
						if strings.HasPrefix(query.Command, "MSSQL_BPA") {
							log.Printf("MSSQL Best Practices Analysis sorgusu tespit edildi: %s", shortenString(query.Command, 100))

							// Komut parametrelerini parse et (opsiyonel database ve server)
							var databaseName, serverName string
							parts := strings.Split(query.Command, "|")

							if len(parts) > 1 && len(strings.TrimSpace(parts[1])) > 0 {
								databaseName = strings.TrimSpace(parts[1])
							}

							if len(parts) > 2 && len(strings.TrimSpace(parts[2])) > 0 {
								serverName = strings.TrimSpace(parts[2])
							}

							log.Printf("MSSQL Best Practices Analysis başlatılıyor: Database=%s, Server=%s",
								databaseName, serverName)

							// MSSQL Collector oluştur
							collector := mssql.NewMSSQLCollector(r.cfg)

							// Best Practices Analysis çalıştır
							results, err := collector.RunBestPracticesAnalysis()
							if err != nil {
								log.Printf("MSSQL Best Practices Analysis başarısız: %v", err)
								errorResult := map[string]interface{}{
									"status":  "error",
									"message": fmt.Sprintf("MSSQL Best Practices Analysis başarısız: %v", err),
								}

								sendQueryResult(r.stream, query.QueryId, errorResult)
								isProcessingQuery = false
								continue
							}

							// Önce tüm time.Time değerlerini string'e dönüştür
							processedResults := convertTimeValues(results).(map[string]interface{})

							// Serialization through JSON to ensure all types are properly converted
							processedResults, err = serializeViaJSON(processedResults)
							if err != nil {
								log.Printf("Serialization error: %v, continuing with original data", err)
							}

							// Analiz sonuçlarını başarılı olarak döndür
							processedResults["status"] = "success"
							processedResults["analysis_timestamp"] = time.Now().Format(time.RFC3339)
							if databaseName != "" {
								processedResults["database_name"] = databaseName
							}
							if serverName != "" {
								processedResults["server_name"] = serverName
							}

							// Kompleks veri yapılarını düzleştir
							flattenedResults := flattenMap(processedResults)

							// Son analizin ne zaman yapıldığını logla
							log.Printf("MSSQL Best Practices Analysis tamamlandı. %d kategori analiz edildi, %d öğe düzleştirildi",
								len(processedResults), len(flattenedResults))

							// Düzleştirilmiş sonucu gönder
							sendQueryResult(r.stream, query.QueryId, flattenedResults)
							isProcessingQuery = false
							continue
						}

						// Normal MSSQL sorgusunu işle
						result := mssqlProcessor.processMSSQLQuery(query.Command, query.Database)

						// Sonucu gönder
						sendQueryResult(r.stream, query.QueryId, result)
						isProcessingQuery = false
						continue
					}
				}

				// MongoDB rs.freeze() komutu için özel işleme
				if r.platform == "mongo" && strings.HasPrefix(query.Command, "rs.freeze") {
					log.Printf("MongoDB rs.freeze komutu tespit edildi: %s", query.Command)

					// Hostname ve port bilgisini config'den al
					hostname := r.cfg.Mongo.Host
					if hostname == "" {
						hostname = "localhost"
					}

					port, err := strconv.Atoi(r.cfg.Mongo.Port)
					if err != nil {
						port = 27017 // varsayılan MongoDB portu
					}

					// ReplicaSet bilgisini al
					mongoCollector, err := r.importMongoCollector()
					if err != nil {
						log.Printf("MongoDB kolektörü import edilemedi: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					replicaSet := mongoCollector.GetReplicaSetName()
					if replicaSet == "" {
						replicaSet = "rs0" // varsayılan replica set adı
					}

					// Seconds değerini komuttan çıkar
					seconds := 60 // varsayılan değer
					pattern := regexp.MustCompile(`rs\.freeze\((\d+)\)`)
					matches := pattern.FindStringSubmatch(query.Command)
					if len(matches) > 1 {
						secs, err := strconv.Atoi(matches[1])
						if err == nil {
							seconds = secs
						}
					}

					// Bir hostname ve agentId oluştur
					hostname, _ = os.Hostname()
					agentID := "agent_" + hostname

					// FreezeMongoSecondary işlemini başlat
					freezeReq := &pb.MongoFreezeSecondaryRequest{
						JobId:        query.QueryId,
						AgentId:      agentID,
						NodeHostname: hostname,
						Port:         int32(port),
						ReplicaSet:   replicaSet,
						Seconds:      int32(seconds),
					}

					freezeResp, err := r.FreezeMongoSecondary(context.Background(), freezeReq)
					if err != nil {
						log.Printf("MongoDB freeze işlemi başarısız: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB freeze işlemi başarısız: %v", err),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// İşlem sonuçlarını dön
					var statusStr string
					if freezeResp.Status == pb.JobStatus_JOB_STATUS_COMPLETED {
						statusStr = "success"
					} else {
						statusStr = "error"
					}

					queryResult := map[string]interface{}{
						"status":  statusStr,
						"message": freezeResp.Result,
					}

					sendQueryResult(r.stream, query.QueryId, queryResult)
					isProcessingQuery = false
					continue
				}

				// MongoDB rs.stepDown() komutu için özel işleme
				if r.platform == "mongo" && strings.HasPrefix(query.Command, "rs.stepDown") {
					log.Printf("MongoDB stepDown komutu tespit edildi: %s", query.Command)

					// Hostname ve port bilgisini config'den al
					hostname := r.cfg.Mongo.Host
					if hostname == "" {
						hostname = "localhost"
					}

					port, err := strconv.Atoi(r.cfg.Mongo.Port)
					if err != nil {
						port = 27017 // varsayılan MongoDB portu
					}

					// MongoDB kolektörünü import et
					mongoCollector, err := r.importMongoCollector()
					if err != nil {
						log.Printf("MongoDB kolektörü import edilemedi: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// ReplicaSet bilgisini al
					replicaSet := mongoCollector.GetReplicaSetName()
					if replicaSet == "" {
						replicaSet = "rs0" // varsayılan replica set adı
					}

					// MongoDB servis durumunu kontrol et - sadece primary node'da stepDown çalışır
					serviceStatus := mongoCollector.GetMongoServiceStatus()
					if serviceStatus == nil || serviceStatus.CurrentState != "PRIMARY" {
						log.Printf("UYARI: StepDown komutu sadece PRIMARY node'da çalışır, mevcut node durumu: %s",
							serviceStatus.CurrentState)
						queryResult := map[string]interface{}{
							"status": "error",
							"message": fmt.Sprintf("StepDown komutu sadece PRIMARY node'da çalışır, mevcut node durumu: %s",
								serviceStatus.CurrentState),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// Bir hostname ve agentId oluştur
					hostname, _ = os.Hostname()
					agentID := "agent_" + hostname

					// PromoteMongoToPrimary işlemini başlat (aslında node'u stepDown yapar)
					promoteReq := &pb.MongoPromotePrimaryRequest{
						JobId:        query.QueryId,
						AgentId:      agentID,
						NodeHostname: hostname,
						Port:         int32(port),
						ReplicaSet:   replicaSet,
						NodeStatus:   serviceStatus.CurrentState,
					}

					// PromoteMongoToPrimary çağrısı rs.stepDown() işlemini gerçekleştirir
					promoteResp, err := r.PromoteMongoToPrimary(context.Background(), promoteReq)
					if err != nil {
						log.Printf("MongoDB stepDown işlemi başarısız: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB stepDown işlemi başarısız: %v", err),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// İşlem sonuçlarını dön
					var statusStr string
					if promoteResp.Status == pb.JobStatus_JOB_STATUS_COMPLETED {
						statusStr = "success"
					} else {
						statusStr = "error"
					}

					queryResult := map[string]interface{}{
						"status":  statusStr,
						"message": promoteResp.Result,
					}

					sendQueryResult(r.stream, query.QueryId, queryResult)
					isProcessingQuery = false
					continue
				}

				// MongoDB db.hello() komutu için özel işleme
				if r.platform == "mongo" && (strings.HasPrefix(query.Command, "db.hello()") || strings.HasPrefix(query.Command, "db.hello(") || strings.HasPrefix(query.Command, "rs.status()")) {
					log.Printf("MongoDB node durumu sorgusu tespit edildi: %s", query.Command)

					// MongoDB kolektörünü import et
					mongoCollector, err := r.importMongoCollector()
					if err != nil {
						log.Printf("MongoDB kolektörü import edilemedi: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// MongoDB durumunu al
					nodeStatus := mongoCollector.GetNodeStatus()
					replicaSet := mongoCollector.GetReplicaSetName()
					mongoStatus := mongoCollector.GetMongoStatus()
					mongoVersion := mongoCollector.GetMongoVersion()

					// Tam MongoDB servis durumunu al
					serviceStatus := mongoCollector.GetMongoServiceStatus()
					stateDescription := "Unknown"
					if serviceStatus != nil {
						stateDescription = serviceStatus.CurrentState
						if stateDescription == "" {
							stateDescription = "STANDALONE"
						}
					}

					// Hostname bilgisini al
					hostname, _ := os.Hostname()

					// Yanıt oluştur
					responseData := map[string]interface{}{
						"status":        "success",
						"node_status":   nodeStatus,
						"replica_set":   replicaSet,
						"mongo_status":  mongoStatus,
						"mongo_version": mongoVersion,
						"hostname":      hostname,
						"state":         stateDescription,
						"message":       fmt.Sprintf("MongoDB node durumu: %s, replica set: %s", stateDescription, replicaSet),
					}

					sendQueryResult(r.stream, query.QueryId, responseData)
					isProcessingQuery = false
					continue
				}

				// PostgreSQL master promotion için pg_ctl promote komutu algılama
				if r.platform == "postgres" && strings.HasPrefix(query.Command, "pg_ctl promote") {
					log.Printf("PostgreSQL master promotion komutu tespit edildi: %s", query.Command)

					// Komuttan data directory bilgisini çıkar
					dataDir := ""
					parts := strings.Split(query.Command, "-D")
					if len(parts) > 1 {
						dataDir = strings.TrimSpace(parts[1])
					}

					// Bir hostname ve agentId oluştur
					hostname, _ := os.Hostname()
					agentID := "agent_" + hostname

					// PromotePostgresToMaster RPC çağrısı
					promoteReq := &pb.PostgresPromoteMasterRequest{
						JobId:         query.QueryId,
						AgentId:       agentID,
						NodeHostname:  hostname,
						DataDirectory: dataDir,
					}

					log.Printf("PostgreSQL promotion işlemi başlatılıyor. Data directory: %s", dataDir)
					promoteResp, err := r.PromotePostgresToMaster(context.Background(), promoteReq)
					if err != nil {
						log.Printf("PostgreSQL master promotion işlemi başarısız: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("PostgreSQL master promotion işlemi başarısız: %v", err),
						}

						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// İşlem sonucunu dön
					var statusStr string
					if promoteResp.Status == pb.JobStatus_JOB_STATUS_COMPLETED {
						statusStr = "success"
					} else {
						statusStr = "error"
					}

					queryResult := map[string]interface{}{
						"status":  statusStr,
						"message": promoteResp.Result,
					}

					if promoteResp.ErrorMessage != "" {
						queryResult["error"] = promoteResp.ErrorMessage
					}

					sendQueryResult(r.stream, query.QueryId, queryResult)
					isProcessingQuery = false
					continue
				}

				// Ping sorgusunu özel olarak işle
				if strings.ToLower(query.Command) == "ping" {
					log.Printf("Ping komutu algılandı, özel yanıt gönderiliyor")

					pingResult := map[string]interface{}{
						"status":  "success",
						"message": "pong",
					}

					sendQueryResult(r.stream, query.QueryId, pingResult)
					isProcessingQuery = false
					continue
				}

				// MongoDB log dosyaları için özel işleme
				if strings.HasPrefix(query.Command, "list_mongo_logs") {
					log.Printf("MongoDB log dosyaları sorgusu tespit edildi")

					// Sorgudan log path'i çıkar
					logPath := ""
					parts := strings.Split(query.Command, "|")
					if len(parts) > 1 {
						logPath = strings.TrimSpace(parts[1])
					}

					// MongoDB kolektörünü import et
					mongoCollector, err := r.importMongoCollector()
					if err != nil {
						log.Printf("MongoDB kolektörü import edilemedi: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Log dosyalarını bul
					logFiles, err := mongoCollector.FindMongoLogFiles(logPath)
					if err != nil {
						log.Printf("MongoDB log dosyaları bulunamadı: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB log dosyaları bulunamadı: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Log dosyalarını detaylı logla
					log.Printf("Bulunan MongoDB log dosyaları (%d adet):", len(logFiles))
					for i, file := range logFiles {
						log.Printf("  %d. %s (Boyut: %d, Son değiştirilme: %s)",
							i+1, file.Path, file.Size, time.Unix(file.LastModified, 0).Format(time.RFC3339))
					}

					// Log dosyalarını map'e dönüştür
					logFilesData := make([]interface{}, 0, len(logFiles))
					for _, file := range logFiles {
						fileMap := map[string]interface{}{
							"name":          file.Name,
							"path":          file.Path,
							"size":          file.Size,
							"last_modified": file.LastModified,
						}
						logFilesData = append(logFilesData, fileMap)
					}

					// Sonuç struct'ını oluştur
					result := map[string]interface{}{
						"status":     "success",
						"log_files":  logFilesData,
						"file_count": len(logFiles),
					}

					// structpb'ye dönüştür
					resultStruct, err := structpb.NewStruct(result)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						isProcessingQuery = false
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						isProcessingQuery = false
						continue
					}

					// Yanıtı gönder
					response := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					if err := r.stream.Send(response); err != nil {
						log.Printf("Sorgu cevabı gönderilemedi: %v", err)
					} else {
						log.Printf("MongoDB log dosyaları başarıyla gönderildi (%d dosya)", len(logFiles))
					}

					isProcessingQuery = false
					continue
				}

				// Sistem metrikleri sorgusu için özel işleme
				if query.Command == "get_system_metrics" {
					log.Printf("Sistem metrikleri sorgusu alındı (QueryID: %s)", query.QueryId)

					// Metrikleri topla
					metrics := postgres.GetSystemMetrics()

					// Metrics verilerini structpb.Struct formatına dönüştür
					metricsStruct := &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"cpu_usage":        structpb.NewNumberValue(metrics.CpuUsage),
							"cpu_cores":        structpb.NewNumberValue(float64(metrics.CpuCores)),
							"memory_usage":     structpb.NewNumberValue(metrics.MemoryUsage),
							"total_memory":     structpb.NewNumberValue(float64(metrics.TotalMemory)),
							"free_memory":      structpb.NewNumberValue(float64(metrics.FreeMemory)),
							"load_average_1m":  structpb.NewNumberValue(metrics.LoadAverage_1M),
							"load_average_5m":  structpb.NewNumberValue(metrics.LoadAverage_5M),
							"load_average_15m": structpb.NewNumberValue(metrics.LoadAverage_15M),
							"total_disk":       structpb.NewNumberValue(float64(metrics.TotalDisk)),
							"free_disk":        structpb.NewNumberValue(float64(metrics.FreeDisk)),
							"os_version":       structpb.NewStringValue(metrics.OsVersion),
							"kernel_version":   structpb.NewStringValue(metrics.KernelVersion),
							"uptime":           structpb.NewNumberValue(float64(metrics.Uptime)),
						},
					}

					// structpb.Struct'ı Any tipine çevir
					anyValue, err := anypb.New(metricsStruct)
					if err != nil {
						log.Printf("Metrics struct'ı Any tipine çevrilemedi: %v", err)
						// Hata durumunda boş bir yanıt gönder
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("Metrics struct'ı Any tipine çevrilemedi: %v", err),
						}
						sendQueryResult(r.stream, query.QueryId, queryResult)
						isProcessingQuery = false
						continue
					}

					// Yanıtı oluştur ve gönder
					queryResult := &pb.QueryResult{
						QueryId: query.QueryId,
						Result:  anyValue,
					}

					// QueryResult mesajı olarak gönder
					err = r.stream.Send(&pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: queryResult,
						},
					})

					if err != nil {
						log.Printf("Sistem metrikleri sorgusu yanıtı gönderilemedi: %v", err)
						if err := r.reconnect(); err != nil {
							log.Printf("Yeniden bağlantı başarısız: %v", err)
						}
					} else {
						log.Printf("Sistem metrikleri sorgusu başarıyla yanıtlandı (QueryID: %s)", query.QueryId)
					}

					isProcessingQuery = false
					continue
				}

				// PostgreSQL log dosyaları için özel işleme
				if strings.HasPrefix(query.Command, "list_postgres_logs") {
					log.Printf("PostgreSQL log dosyaları sorgusu tespit edildi")

					// Sorgudan log path'i çıkar
					logPath := ""
					parts := strings.Split(query.Command, "|")
					if len(parts) > 1 {
						logPath = strings.TrimSpace(parts[1])
					}

					// Log dosyalarını bul
					logFiles, err := postgres.FindPostgresLogFiles(logPath)
					if err != nil {
						log.Printf("PostgreSQL log dosyaları bulunamadı: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("PostgreSQL log dosyaları bulunamadı: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Log dosyalarını detaylı logla
					log.Printf("Bulunan PostgreSQL log dosyaları (%d adet):", len(logFiles))
					for i, file := range logFiles {
						log.Printf("  %d. %s (Boyut: %d, Son değiştirilme: %s)",
							i+1, file.Path, file.Size, time.Unix(file.LastModified, 0).Format(time.RFC3339))
					}

					// Log dosyalarını map'e dönüştür
					logFilesData := make([]interface{}, 0, len(logFiles))
					for _, file := range logFiles {
						fileMap := map[string]interface{}{
							"name":          file.Name,
							"path":          file.Path,
							"size":          file.Size,
							"last_modified": file.LastModified,
						}
						logFilesData = append(logFilesData, fileMap)
					}

					// Sonuç struct'ını oluştur
					result := map[string]interface{}{
						"status":     "success",
						"log_files":  logFilesData,
						"file_count": len(logFiles),
					}

					// structpb'ye dönüştür
					resultStruct, err := structpb.NewStruct(result)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						isProcessingQuery = false
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						isProcessingQuery = false
						continue
					}

					// Yanıtı gönder
					response := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					if err := r.stream.Send(response); err != nil {
						log.Printf("Sorgu cevabı gönderilemedi: %v", err)
					} else {
						log.Printf("PostgreSQL log dosyaları başarıyla gönderildi (%d dosya)", len(logFiles))
					}

					isProcessingQuery = false
					continue
				}

				// PostgreSQL konfigürasyon sorgusu için özel işleme
				if strings.HasPrefix(query.Command, "read_postgres_config") {
					log.Printf("PostgreSQL config sorgusu tespit edildi: %s", query.Command)

					// Komut parametresini parse et (configPath)
					configPath := ""
					parts := strings.Split(query.Command, "|")
					if len(parts) > 1 {
						configPath = strings.TrimSpace(parts[1])
					}

					log.Printf("PostgreSQL konfigürasyon dosyası okunuyor: %s", configPath)

					// ConfigRequest oluştur
					hostname, _ := os.Hostname()
					agentID := "agent_" + hostname
					configReq := &pb.PostgresConfigRequest{
						AgentId:    agentID,
						ConfigPath: configPath,
					}

					// ReadPostgresConfig fonksiyonunu çağır
					configResp, err := r.ReadPostgresConfig(context.Background(), configReq)
					if err != nil {
						log.Printf("PostgreSQL konfigürasyon dosyası okunamadı: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("PostgreSQL konfigürasyon dosyası okunamadı: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Konfigürasyon girdilerini map'e dönüştür
					configEntries := make([]interface{}, 0, len(configResp.Configurations))
					for _, entry := range configResp.Configurations {
						entryMap := map[string]interface{}{
							"parameter":   entry.Parameter,
							"value":       entry.Value,
							"description": entry.Description,
							"is_default":  entry.IsDefault,
							"category":    entry.Category,
						}
						configEntries = append(configEntries, entryMap)
					}

					// Sonuç struct'ını oluştur
					result := map[string]interface{}{
						"status":         "success",
						"config_path":    configResp.ConfigPath,
						"configurations": configEntries,
						"count":          len(configResp.Configurations),
					}

					// structpb'ye dönüştür
					resultStruct, err := structpb.NewStruct(result)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						isProcessingQuery = false
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						isProcessingQuery = false
						continue
					}

					// Yanıtı gönder
					response := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					if err := r.stream.Send(response); err != nil {
						log.Printf("Sorgu cevabı gönderilemedi: %v", err)
					} else {
						log.Printf("PostgreSQL konfigürasyon bilgileri başarıyla gönderildi (%d parametre)", len(configResp.Configurations))
					}

					isProcessingQuery = false
					continue
				}

				// PostgreSQL log analizi sorgusu için özel işlem
				if strings.HasPrefix(query.Command, "analyze_postgres_log") {
					log.Printf("PostgreSQL log analizi sorgusu tespit edildi: %s", query.Command)

					// Parse command parameters (log_file_path|slow_query_threshold_ms)
					parts := strings.Split(query.Command, "|")
					if len(parts) < 2 {
						log.Printf("Geçersiz sorgu formatı: %s", query.Command)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": "Geçersiz sorgu formatı, beklenen format: analyze_postgres_log|/path/to/logfile.log|1000",
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Extract log file path and threshold
					logFilePath := strings.TrimSpace(parts[1])
					slowQueryThresholdMs := int64(0)
					if len(parts) > 2 {
						threshold, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
						if err == nil {
							slowQueryThresholdMs = threshold
						}
					}

					log.Printf("PostgreSQL log analizi başlatılıyor: Dosya=%s, Eşik=%d ms",
						logFilePath, slowQueryThresholdMs)

					// Analyze the log file
					analyzeResponse, err := postgres.AnalyzePostgresLog(logFilePath, slowQueryThresholdMs)
					if err != nil {
						log.Printf("PostgreSQL log analizi başarısız: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("PostgreSQL log analizi başarısız: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					log.Printf("PostgreSQL log analizi tamamlandı. %d kayıt bulundu", len(analyzeResponse.LogEntries))

					// Log a few sample entries
					log.Printf("Sunucuya gönderilen ilk girişler (maksimum 5):")
					for i, entry := range analyzeResponse.LogEntries {
						if i >= 5 {
							break
						}

						log.Printf("  Girdi #%d: %s %s (PID: %s) (%dms) - %s",
							i+1,
							time.Unix(entry.Timestamp, 0).Format("2006-01-02 15:04:05"),
							entry.LogLevel,
							entry.ProcessId,
							entry.DurationMs,
							trimString(entry.Message, 80))
					}

					// If we have more than 5 entries, show a summary
					if len(analyzeResponse.LogEntries) > 5 {
						log.Printf("... ve %d girdi daha", len(analyzeResponse.LogEntries)-5)
					}

					// Convert log entries to a map for structpb
					logEntriesData := make([]interface{}, 0, len(analyzeResponse.LogEntries))
					for _, entry := range analyzeResponse.LogEntries {
						entryMap := map[string]interface{}{
							"timestamp":              entry.Timestamp,
							"log_level":              entry.LogLevel,
							"user_name":              entry.UserName,
							"database":               entry.Database,
							"process_id":             entry.ProcessId,
							"connection_from":        entry.ConnectionFrom,
							"session_id":             entry.SessionId,
							"session_line_num":       entry.SessionLineNum,
							"command_tag":            entry.CommandTag,
							"session_start_time":     entry.SessionStartTime,
							"virtual_transaction_id": entry.VirtualTransactionId,
							"transaction_id":         entry.TransactionId,
							"error_severity":         entry.ErrorSeverity,
							"sql_state_code":         entry.SqlStateCode,
							"message":                entry.Message,
							"detail":                 entry.Detail,
							"hint":                   entry.Hint,
							"internal_query":         entry.InternalQuery,
							"duration_ms":            entry.DurationMs,
						}
						logEntriesData = append(logEntriesData, entryMap)
					}

					// Create the result struct
					analysisResult := map[string]interface{}{
						"status":      "success",
						"log_entries": logEntriesData,
						"count":       len(analyzeResponse.LogEntries),
					}

					// Convert to structpb
					resultStruct, err := structpb.NewStruct(analysisResult)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						isProcessingQuery = false
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						isProcessingQuery = false
						continue
					}

					// Send the response
					streamResponse := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					log.Printf("Stream üzerinden analiz sonuçları gönderiliyor...")
					err = r.stream.Send(streamResponse)
					if err != nil {
						log.Printf("Stream yanıtı gönderilemedi: %v", err)
					} else {
						log.Printf("PostgreSQL log analizi sonuçları başarıyla gönderildi (%d girdi)", len(analyzeResponse.LogEntries))
					}

					isProcessingQuery = false
					continue
				}

				// MongoDB log analizi sorgusu için özel işlem
				if strings.HasPrefix(query.Command, "analyze_mongo_log") {
					log.Printf("MongoDB log analizi sorgusu tespit edildi: %s", query.Command)

					// Sorgudan parametreleri çıkar (log_file_path|slow_query_threshold_ms)
					parts := strings.Split(query.Command, "|")
					if len(parts) < 2 {
						log.Printf("Geçersiz sorgu formatı: %s", query.Command)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": "Geçersiz sorgu formatı, beklenen format: analyze_mongo_log|/path/to/logfile.log|1000",
						}

						// Sonucu structpb'ye dönüştür
						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						// Yanıtı gönder
						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Log dosya yolunu ve threshold değerini çıkar
					logFilePath := strings.TrimSpace(parts[1])
					slowQueryThresholdMs := int64(0) // Default 0 means return ALL entries
					if len(parts) > 2 {
						thresholdStr := strings.TrimSpace(parts[2])
						threshold, err := strconv.ParseInt(thresholdStr, 10, 64)
						if err == nil {
							slowQueryThresholdMs = threshold
						}
					}

					log.Printf("MongoDB log analizi başlatılıyor: Dosya=%s, Eşik=%d ms",
						logFilePath, slowQueryThresholdMs)

					log.Printf("====== MONGO LOG ANALİZİ BAŞLIYOR ======")

					// Hostname bilgisini al (agent ID için gerekiyor)
					hostName, _ := os.Hostname()
					agentID := "agent_" + hostName

					// MongoLogAnalyzeRequest oluştur
					logAnalyzeReq := &pb.MongoLogAnalyzeRequest{
						LogFilePath:          logFilePath,
						SlowQueryThresholdMs: slowQueryThresholdMs,
						AgentId:              agentID,
					}

					// Gerçek log analizi fonksiyonunu çağır
					log.Printf("AnalyzeMongoLog fonksiyonu çağrılıyor...")
					logAnalyzeResp, err := r.AnalyzeMongoLog(logAnalyzeReq)
					if err != nil {
						log.Printf("MongoDB log analizi başarısız: %v", err)
						queryResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB log analizi başarısız: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(queryResult)
						anyResult, _ := anypb.New(resultStruct)

						// Yanıtı gönder
						response := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := r.stream.Send(response); err != nil {
							log.Printf("Sorgu cevabı gönderilemedi: %v", err)
						}

						isProcessingQuery = false
						continue
					}

					// Analiz sonuçlarını hazırla
					log.Printf("MongoDB log analizi tamamlandı. %d kayıt bulundu", len(logAnalyzeResp.LogEntries))

					// LogEntries'i bir dizi map'e dönüştür
					logEntriesData := make([]interface{}, 0, len(logAnalyzeResp.LogEntries))
					for _, entry := range logAnalyzeResp.LogEntries {
						// Komut alanını string'e dönüştür (nil olabilir, veya farklı bir tipte olabilir)
						commandStr := ""
						if entry.Command != "" {
							commandStr = entry.Command
						}

						// Timestamp Unix formatında
						timestamp := entry.Timestamp
						if timestamp == 0 {
							timestamp = time.Now().Unix()
						}

						entryMap := map[string]interface{}{
							"timestamp":       timestamp,
							"severity":        entry.Severity,
							"component":       entry.Component,
							"context":         entry.Context,
							"message":         entry.Message,
							"db_name":         entry.DbName,
							"duration_millis": entry.DurationMillis,
							"command":         commandStr,
							"plan_summary":    entry.PlanSummary,
							"namespace":       entry.Namespace,
						}
						logEntriesData = append(logEntriesData, entryMap)
					}

					// Analiz sonuçlarını structpb'ye dönüştür
					analysisResult := map[string]interface{}{
						"status":      "success",
						"log_entries": logEntriesData,
						"count":       len(logAnalyzeResp.LogEntries),
					}

					// structpb'ye dönüştür
					resultStruct, err := structpb.NewStruct(analysisResult)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						isProcessingQuery = false
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						isProcessingQuery = false
						continue
					}

					// QueryResult olarak yanıt gönder
					streamResponse := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					log.Printf("Stream üzerinden analiz sonuçları gönderiliyor...")
					err = r.stream.Send(streamResponse)
					if err != nil {
						log.Printf("Stream yanıtı gönderilemedi: %v", err)
					} else {
						log.Printf("MongoDB log analizi sonuçları başarıyla gönderildi (%d girdi)", len(logAnalyzeResp.LogEntries))
					}

					isProcessingQuery = false
					continue
				}

				// pg_cachehitratio sorgusu için özel işlem
				if query.QueryId == "pg_cachehitratio" {
					log.Printf("pg_cachehitratio sorgusu tespit edildi, boyut sınırlaması uygulanıyor")
				}

				// Normal PostgreSQL sorgusu işleme (yalnızca postgres platform için)
				if r.platform == "postgres" && processor != nil {
					result := processor.processQuery(query.Command, query.Database)

					// Sonucu structpb'ye dönüştür
					resultStruct, err := structpb.NewStruct(result)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						isProcessingQuery = false
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						isProcessingQuery = false
						continue
					}

					// Yanıtı gönder
					response := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					if err := r.stream.Send(response); err != nil {
						log.Printf("Sorgu cevabı gönderilemedi: %v", err)
					}
				} else if r.platform == "mongo" {
					// MongoDB platform için sorguysa ve diğer durumlarda işlenmediyse hata bildir
					log.Printf("MongoDB platformunda geçersiz sorgu: %s", query.Command)

					// MongoDB sorgu açıklama işlevi için kontrol ekleyelim
					if strings.Contains(query.Command, "explain") || strings.Contains(query.Command, "aggregate") ||
						strings.Contains(query.Command, "find") || strings.Contains(query.Command, "pipeline") ||
						strings.Contains(query.Command, "MONGODB_EXPLAIN") || strings.Contains(query.Command, "MONGO_EXPLAIN") {

						log.Printf("MongoDB sorgu açıklama isteği tespit edildi, ham sorgu: %s", query.Command)

						// Server protokol formatını kontrol et ve işle (MONGO_EXPLAIN|<database>|<query_json>)
						var queryStr string
						var database string

						if strings.HasPrefix(query.Command, "MONGODB_EXPLAIN") {
							log.Printf("Server protokol formatı tespit edildi: MONGODB_EXPLAIN")

							// Protokol formatını parse et
							parts := strings.Split(query.Command, "||")
							if len(parts) >= 3 {
								// Format: MONGODB_EXPLAIN||<database>||<query_json>
								database = parts[1]
								queryStr = parts[2]
								log.Printf("Protokol parse edildi: database=%s, sorgu uzunluğu=%d",
									database, len(queryStr))
							} else if len(parts) == 2 {
								// Format: MONGODB_EXPLAIN||<query_json>
								queryStr = parts[1]
								database = query.Database // Orijinal database'i kullan
								log.Printf("Eski protokol parse edildi: database=%s, sorgu uzunluğu=%d",
									database, len(queryStr))
							} else {
								log.Printf("Geçersiz protokol formatı, sorguyu olduğu gibi kullanıyorum")
								queryStr = query.Command
								database = query.Database
							}
						} else if strings.HasPrefix(query.Command, "MONGO_EXPLAIN") {
							log.Printf("Server protokol formatı tespit edildi: MONGO_EXPLAIN")

							// Protokol formatını parse et - | ayracını kullanıyor
							parts := strings.Split(query.Command, "|")
							if len(parts) >= 3 {
								// Format: MONGO_EXPLAIN|<database>|<query_json>
								database = parts[1]
								queryStr = strings.Join(parts[2:], "|") // Eğer sorgu içinde | karakteri varsa birleştir
								log.Printf("Protokol parse edildi: database=%s, sorgu uzunluğu=%d",
									database, len(queryStr))
							} else if len(parts) == 2 {
								// Format: MONGO_EXPLAIN|<query_json>
								queryStr = parts[1]
								database = query.Database // Orijinal database'i kullan
								log.Printf("Eski protokol parse edildi: database=%s, sorgu uzunluğu=%d",
									database, len(queryStr))
							} else {
								log.Printf("Geçersiz protokol formatı, sorguyu olduğu gibi kullanıyorum")
								queryStr = query.Command
								database = query.Database
							}
						} else {
							// Normal sorgu
							queryStr = query.Command
							database = query.Database
						}

						// Sorguyu logla
						if len(queryStr) > 100 {
							log.Printf("MongoDB sorgusu (ilk 100 karakter): %s...", queryStr[:100])
						} else {
							log.Printf("MongoDB sorgusu: %s", queryStr)
						}

						// Sorgu formatını kontrol et - eğer JSON wrapper formatındaysa parse et
						var finalQuery string
						if strings.Contains(queryStr, "\"database\"") && strings.Contains(queryStr, "\"query\"") {
							log.Printf("JSON wrapper formatı tespit edildi, içerik çıkarılıyor")

							// JSON'u parse et ve içindeki sorguyu al
							var wrapper struct {
								Database string          `json:"database"`
								Query    json.RawMessage `json:"query"`
							}

							if err := json.Unmarshal([]byte(queryStr), &wrapper); err == nil {
								// Database'i güncelle
								if wrapper.Database != "" && wrapper.Database != "null" {
									database = wrapper.Database
									log.Printf("JSON wrapper'dan veritabanı alındı: %s", database)
								}

								// İç sorguyu çıkar
								finalQuery = string(wrapper.Query)

								// Eğer sorgu çift tırnak içindeyse, string escape karakterleri temizle
								if strings.HasPrefix(finalQuery, "\"") && strings.HasSuffix(finalQuery, "\"") {
									unquoted, err := strconv.Unquote(finalQuery)
									if err == nil {
										finalQuery = unquoted
										log.Printf("String unquote başarılı, temizlenmiş sorgu uzunluğu: %d", len(finalQuery))
									} else {
										log.Printf("String unquote başarısız: %v", err)
									}
								}
							} else {
								log.Printf("JSON wrapper parse hatası: %v, ham sorgu kullanılacak", err)
								finalQuery = queryStr
							}
						} else {
							finalQuery = queryStr
						}

						// Hostname'den agentID oluştur
						hostname, _ := os.Hostname()
						agentID := "agent_" + hostname

						explainReq := &pb.ExplainQueryRequest{
							AgentId:  agentID,
							Database: database,   // Protokolden gelen veya orijinal database
							Query:    finalQuery, // İşlenmiş sorgu
						}

						explainResp, err := r.ExplainMongoQuery(context.Background(), explainReq)
						if err != nil {
							log.Printf("MongoDB sorgu açıklama hatası: %v", err)
							queryResult := map[string]interface{}{
								"status":  "error",
								"message": fmt.Sprintf("MongoDB sorgu açıklama hatası: %v", err),
							}

							sendQueryResult(r.stream, query.QueryId, queryResult)
						} else {
							// Başarılı yanıtı gönder
							// Plan metni doğrudan düz metin olarak geliyor
							planText := explainResp.Plan

							// Başarılı yanıtı oluştur
							response := &pb.QueryResult{
								QueryId: query.QueryId,
							}

							// Plan yanıtını direkt olarak structpb'ye çevir
							responseData := map[string]interface{}{
								"status": "success",
								"plan":   planText,
							}

							if explainResp.ErrorMessage != "" {
								responseData["message"] = explainResp.ErrorMessage
							}

							// Map'i structpb'ye dönüştür
							resultStruct, err := structpb.NewStruct(responseData)
							if err != nil {
								log.Printf("Explain sonucu struct'a dönüştürme hatası: %v", err)
								queryResult := map[string]interface{}{
									"status":  "error",
									"message": fmt.Sprintf("Explain sonucu dönüştürme hatası: %v", err),
								}
								sendQueryResult(r.stream, query.QueryId, queryResult)
								continue
							}

							// structpb'yi Any tipine çevir
							anyResult, err := anypb.New(resultStruct)
							if err != nil {
								log.Printf("Explain sonucu Any tipine dönüştürme hatası: %v", err)
								queryResult := map[string]interface{}{
									"status":  "error",
									"message": fmt.Sprintf("Explain sonucu dönüştürme hatası: %v", err),
								}
								sendQueryResult(r.stream, query.QueryId, queryResult)
								continue
							}

							// Yanıtı ayarla
							response.Result = anyResult

							// Yanıtı gönder
							msg := &pb.AgentMessage{
								Payload: &pb.AgentMessage_QueryResult{
									QueryResult: response,
								},
							}

							if err := r.stream.Send(msg); err != nil {
								log.Printf("Explain sorgu yanıtı gönderilemedi: %v", err)
							} else {
								log.Printf("MongoDB explain sorgu yanıtı başarıyla gönderildi")
							}
						}

						isProcessingQuery = false
						continue
					}

					queryResult := map[string]interface{}{
						"status":  "error",
						"message": "Bu komut MongoDB platformunda desteklenmiyor",
					}

					sendQueryResult(r.stream, query.QueryId, queryResult)
				}

				isProcessingQuery = false
				continue
			} else if metricsReq := msg.GetMetricsRequest(); metricsReq != nil {
				// Eğer şu anda sorgu işleme devam ediyorsa, uyarı ver
				if isProcessingQuery {
					log.Printf("UYARI: Sorgu işleme devam ederken metrik isteği alındı")
				}

				isProcessingMetrics = true
				lastMessageType = "metrics_request"
				log.Printf("Yeni sistem metrikleri isteği geldi (Agent ID: %s)", metricsReq.AgentId)

				log.Printf("Sistem metrikleri toplanıyor...")
				metrics := postgres.GetSystemMetrics()

				// Debug için metrikleri logla
				log.Printf("Toplanan metrikler boyutları:")
				log.Printf("  cpu_usage: %v (size: ~8 bytes)", metrics.CpuUsage)
				log.Printf("  cpu_cores: %v (size: ~4 bytes)", metrics.CpuCores)
				log.Printf("  memory_usage: %v (size: ~8 bytes)", metrics.MemoryUsage)
				log.Printf("  total_memory: %v (size: ~8 bytes)", metrics.TotalMemory)
				log.Printf("  free_memory: %v (size: ~8 bytes)", metrics.FreeMemory)
				log.Printf("  load_average_1m: %v (size: ~8 bytes)", metrics.LoadAverage_1M)
				log.Printf("  load_average_5m: %v (size: ~8 bytes)", metrics.LoadAverage_5M)
				log.Printf("  load_average_15m: %v (size: ~8 bytes)", metrics.LoadAverage_15M)
				log.Printf("  total_disk: %v (size: ~8 bytes)", metrics.TotalDisk)
				log.Printf("  free_disk: %v (size: ~8 bytes)", metrics.FreeDisk)
				log.Printf("  os_version: %v (size: %d bytes)", metrics.OsVersion, len(metrics.OsVersion))
				log.Printf("  kernel_version: %v (size: %d bytes)", metrics.KernelVersion, len(metrics.KernelVersion))
				log.Printf("  uptime: %v (size: ~8 bytes)", metrics.Uptime)

				// Ayrı RPC çağrısı yerine mevcut stream üzerinden metrikleri gönder
				log.Printf("Sistem metrikleri stream üzerinden gönderiliyor... (Agent ID: %s)", metricsReq.AgentId)

				// AgentMessage_SystemMetrics olarak metrikleri gönder
				message := &pb.AgentMessage{
					Payload: &pb.AgentMessage_SystemMetrics{
						SystemMetrics: metrics,
					},
				}

				// Stream üzerinden gönder
				err = r.stream.Send(message)
				if err != nil {
					log.Printf("Sistem metrikleri stream üzerinden gönderilemedi: %v", err)
					if err := r.reconnect(); err != nil {
						log.Printf("Yeniden bağlantı başarısız: %v", err)
					}
				} else {
					log.Printf("Sistem metrikleri başarıyla stream üzerinden gönderildi")
				}

				// Belleği temizle
				metrics = nil

				// Debug için bellek kullanımını logla
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				log.Printf("Bellek durumu - Alloc: %v MB, Sys: %v MB, NumGC: %v",
					memStats.Alloc/1024/1024,
					memStats.Sys/1024/1024,
					memStats.NumGC)

				isProcessingMetrics = false

				// İşlem tamamlandıktan sonra kısa bir bekleme ekleyelim
				time.Sleep(500 * time.Millisecond)
			} else {
				log.Printf("Bilinmeyen mesaj tipi alındı")
			}
		}
	}
}

// sendQueryResult yardımcı fonksiyonu, map sonucunu gRPC yanıtına dönüştürüp gönderir
func sendQueryResult(stream pb.AgentService_ConnectClient, queryId string, result map[string]interface{}) {
	resultStruct, err := structpb.NewStruct(result)
	if err != nil {
		log.Printf("Struct'a dönüştürme hatası: %v", err)
		return
	}

	anyResult, err := anypb.New(resultStruct)
	if err != nil {
		log.Printf("Any tipine dönüştürülemedi: %v", err)
		return
	}

	response := &pb.AgentMessage{
		Payload: &pb.AgentMessage_QueryResult{
			QueryResult: &pb.QueryResult{
				QueryId: queryId,
				Result:  anyResult,
			},
		},
	}

	if err := stream.Send(response); err != nil {
		log.Printf("Sorgu cevabı gönderilemedi: %v", err)
	} else {
		log.Printf("Sorgu cevabı başarıyla gönderildi")
	}
}

// trimString, string'i belirtilen maksimum uzunluğa kısaltır
func trimString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// shortenString, bir string'i belirli bir uzunlukta kısaltır
func shortenString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// convertTimeValues recursively traverses a data structure and converts time.Time values to RFC3339 strings
func convertTimeValues(data interface{}) interface{} {
	switch v := data.(type) {
	case time.Time:
		// Convert time.Time to RFC3339 string
		return v.Format(time.RFC3339)

	case map[string]interface{}:
		// Process each value in the map
		result := make(map[string]interface{})
		for key, value := range v {
			result[key] = convertTimeValues(value)
		}
		return result

	case []interface{}:
		// Process each item in the slice
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = convertTimeValues(item)
		}
		return result

	case []map[string]interface{}:
		// Process each map in the slice
		result := make([]map[string]interface{}, len(v))
		for i, item := range v {
			result[i] = convertTimeValues(item).(map[string]interface{})
		}
		return result

	default:
		// Return other types as is
		return v
	}
}

// flattenMap converts a nested map structure into a flat map with concatenated keys
func flattenMap(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// First, extract and debug any problematic map[string][]map[string]interface{} structures
	for key, value := range data {
		// Check for map[string][]map[string]interface{} pattern that might cause issues
		if nestedMap, ok := value.(map[string]interface{}); ok {
			for nestedKey, nestedValue := range nestedMap {
				if arrayOfMaps, isArrayOfMaps := nestedValue.([]map[string]interface{}); isArrayOfMaps {
					log.Printf("Found map[string][]map[string]interface{} at %s.%s with %d elements",
						key, nestedKey, len(arrayOfMaps))
				}
			}
		}
	}

	// Recursive helper function to flatten the map
	var flatten func(prefix string, value interface{})
	flatten = func(prefix string, value interface{}) {
		switch v := value.(type) {
		case map[string]interface{}:
			// For maps, recursively call flatten with each key-value pair
			for k, val := range v {
				key := k
				if prefix != "" {
					key = prefix + "_" + k
				}

				// Special handling for array of maps inside maps
				if arrayOfMaps, isArrayOfMaps := val.([]map[string]interface{}); isArrayOfMaps {
					log.Printf("Array düzleştiriliyor: %s_%s (eleman sayısı: %d)", prefix, k, len(arrayOfMaps))
					result[key+"_count"] = len(arrayOfMaps)
					for i, item := range arrayOfMaps {
						for itemKey, itemValue := range item {
							result[fmt.Sprintf("%s_%s_%d_%s", prefix, k, i, itemKey)] = itemValue
						}
					}
				} else if arrayInterface, isArrayInterface := val.([]interface{}); isArrayInterface {
					// Handle []interface{} that might contain maps
					result[key+"_count"] = len(arrayInterface)
					for i, item := range arrayInterface {
						if mapItem, isMap := item.(map[string]interface{}); isMap {
							for itemKey, itemValue := range mapItem {
								result[fmt.Sprintf("%s_%s_%d_%s", prefix, k, i, itemKey)] = itemValue
							}
						} else {
							result[fmt.Sprintf("%s_%s_%d", prefix, k, i)] = item
						}
					}
				} else {
					// Regular recursion for other types
					flatten(key, val)
				}
			}

		case []interface{}:
			// For slices, add a count and flatten each element with an index
			if len(v) > 0 {
				result[prefix+"_count"] = len(v)
				for i, val := range v {
					if mapItem, isMap := val.(map[string]interface{}); isMap {
						for itemKey, itemValue := range mapItem {
							result[fmt.Sprintf("%s_%d_%s", prefix, i, itemKey)] = itemValue
						}
					} else {
						result[fmt.Sprintf("%s_%d", prefix, i)] = val
					}
				}
			} else {
				// Empty slice, just record the count
				result[prefix+"_count"] = 0
			}

		case []map[string]interface{}:
			// For slices of maps, log it and handle similarly to []interface{}
			log.Printf("Array düzleştiriliyor: %s (eleman sayısı: %d)", prefix, len(v))
			result[prefix+"_count"] = len(v)
			for i, item := range v {
				for itemKey, itemValue := range item {
					result[fmt.Sprintf("%s_%d_%s", prefix, i, itemKey)] = itemValue
				}
			}

		default:
			// For primitive values, just add them directly
			result[prefix] = v
		}
	}

	// Special handling for top-level keys that should not be nested further
	for key, value := range data {
		if key == "status" || key == "analysis_timestamp" || key == "database_name" || key == "server_name" {
			result[key] = value
			continue
		}

		switch v := value.(type) {
		case map[string]interface{}:
			// Log when flattening a nested map for debugging
			if hasNestedArrays(v) {
				log.Printf("İç içe map düzleştiriliyor: %s", key)
			}
			flatten(key, v)

		case []map[string]interface{}:
			// Log when flattening an array
			log.Printf("Array düzleştiriliyor: %s (eleman sayısı: %d)", key, len(v))
			result[key+"_count"] = len(v)
			for i, item := range v {
				for itemKey, itemValue := range item {
					result[fmt.Sprintf("%s_%d_%s", key, i, itemKey)] = itemValue
				}
			}

		case []interface{}:
			// Handle top-level []interface{} arrays
			log.Printf("İnteface array düzleştiriliyor: %s (eleman sayısı: %d)", key, len(v))
			result[key+"_count"] = len(v)
			for i, item := range v {
				if mapItem, isMap := item.(map[string]interface{}); isMap {
					for itemKey, itemValue := range mapItem {
						result[fmt.Sprintf("%s_%d_%s", key, i, itemKey)] = itemValue
					}
				} else {
					result[fmt.Sprintf("%s_%d", key, i)] = item
				}
			}

		default:
			// For other types, pass directly to the flatten function
			flatten(key, value)
		}
	}

	return result
}

// hasNestedArrays checks if a map contains any nested arrays of maps
func hasNestedArrays(data map[string]interface{}) bool {
	for _, value := range data {
		switch v := value.(type) {
		case []map[string]interface{}, []interface{}:
			return true
		case map[string]interface{}:
			if hasNestedArrays(v) {
				return true
			}
		}
	}
	return false
}

// serializeViaJSON converts the data to JSON and back to ensure all types are properly converted
func serializeViaJSON(data map[string]interface{}) (map[string]interface{}, error) {
	// Convert to JSON
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return data, fmt.Errorf("JSON serialization error: %v", err)
	}

	// Convert back to map[string]interface{}
	var result map[string]interface{}
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		return data, fmt.Errorf("JSON deserialization error: %v", err)
	}

	return result, nil
}

// reconnect bağlantıyı yeniden kurar ve agent kaydını tekrarlar
func (r *Reporter) reconnect() error {
	// Eski bağlantıyı kapat
	if r.grpcClient != nil {
		r.grpcClient.Close()
	}

	// Yeniden bağlantı için bir kaç deneme yapalım
	maxRetries := 5
	retryDelay := 2 * time.Second

	var err error
	for i := 0; i < maxRetries; i++ {
		log.Printf("gRPC sunucusuna yeniden bağlanılıyor (deneme %d/%d)...", i+1, maxRetries)

		// Yeni bağlantı kur
		err = r.Connect()
		if err == nil {
			break // Bağlantı başarılı
		}

		log.Printf("Yeniden bağlantı başarısız (%d/%d): %v, %v saniye sonra tekrar denenecek",
			i+1, maxRetries, err, retryDelay.Seconds())

		if i < maxRetries-1 {
			time.Sleep(retryDelay)
			// Her denemede bekleme süresini artır (exponential backoff)
			retryDelay = time.Duration(float64(retryDelay) * 1.5)
		}
	}

	if err != nil {
		return fmt.Errorf("maksimum yeniden bağlantı denemesi aşıldı: %v", err)
	}

	// Agent kaydını tekrarla (test sonucu "reconnected" olarak gönder)
	// Mevcut platform bilgisini kullanarak agent kaydını yenile
	if err := r.AgentRegistration("reconnected", r.platform); err != nil {
		log.Printf("Agent yeniden kaydı başarısız: %v. Tekrar deneniyor...", err)

		// Agent kaydı için bir deneme daha yap
		time.Sleep(2 * time.Second)
		if err := r.AgentRegistration("reconnected", r.platform); err != nil {
			return fmt.Errorf("agent yeniden kaydı başarısız: %v", err)
		}
	}

	// Alarm monitörünün client'ını güncelle
	if r.alarmMonitor != nil {
		client := pb.NewAgentServiceClient(r.grpcClient)
		r.alarmMonitor.UpdateClient(client)
		log.Printf("Alarm monitörü client'ı yeniden bağlantı sonrası güncellendi")
	}

	log.Printf("Yeniden bağlantı tamamlandı. Platform: %s", r.platform)
	return nil
}

// SendPostgresInfo PostgreSQL bilgilerini sunucuya gönderir
func (r *Reporter) SendPostgresInfo() error {
	log.Printf("SendPostgresInfo metodu çağrıldı")
	log.Println("PostgreSQL bilgileri toplanıyor...")

	// Hostname ve IP bilgilerini al
	hostname, _ := os.Hostname()
	ip := utils.GetLocalIP()

	// Disk kullanım bilgilerini al
	freeDisk, fdPercent := postgres.GetDiskUsage()

	// Node durumunu al
	nodeStatus := postgres.GetNodeStatus()

	// Toplam vCPU sayısını al
	totalvCpu := postgres.GetTotalvCpu()

	// Toplam RAM miktarını al
	totalMemory := postgres.GetTotalMemory()

	// PostgreSQL konfigürasyon dosyasının yolunu al
	configPath, _ := postgres.FindPostgresConfigFile()

	// PostgreSQL data directory yolunu al
	dataPath, err := postgres.GetDataDirectory()
	if err != nil {
		log.Printf("PostgreSQL veri dizini bulunamadı: %v, varsayılan değer boş kullanılacak", err)
		dataPath = ""
	}
	log.Printf("PostgreSQL veri dizini: %s", dataPath)

	// PostgreSQL bilgilerini oluştur
	pgInfo := &pb.PostgresInfo{
		ClusterName:       r.cfg.PostgreSQL.Cluster,
		Location:          r.cfg.PostgreSQL.Location,
		Hostname:          hostname,
		Ip:                ip,
		PgServiceStatus:   postgres.GetPGServiceStatus(),
		PgBouncerStatus:   "N/A", // PgBouncer durumu artık kontrol edilmiyor
		NodeStatus:        nodeStatus,
		PgVersion:         postgres.GetPGVersion(),
		ReplicationLagSec: int64(postgres.GetReplicationLagSec()),
		FreeDisk:          freeDisk,
		FdPercent:         int32(fdPercent),
		TotalVcpu:         totalvCpu,
		TotalMemory:       totalMemory,
		ConfigPath:        configPath,
		DataPath:          dataPath,
	}

	// PostgreSQL bilgilerini logla
	log.Printf("PostgreSQL bilgileri alındı: %v", pgInfo)

	// Yeni SendPostgresInfo RPC'sini kullanarak verileri gönder
	client := pb.NewAgentServiceClient(r.grpcClient)

	// PostgresInfoRequest oluştur
	request := &pb.PostgresInfoRequest{
		PostgresInfo: pgInfo,
	}

	// SendPostgresInfo RPC'sini çağır
	response, err := client.SendPostgresInfo(context.Background(), request)
	if err != nil {
		log.Printf("PostgreSQL bilgileri yeni RPC ile gönderilemedi: %v. Eski yöntem deneniyor...", err)

		// Eski yöntem: Stream üzerinden gönder
		err = r.Report(pgInfo)
		if err != nil {
			log.Printf("PostgreSQL bilgileri eski yöntemle de gönderilemedi: %v", err)
			return err
		}

		log.Println("PostgreSQL bilgileri başarıyla eski yöntemle gönderildi")
		return nil
	}

	log.Printf("PostgreSQL bilgileri başarıyla gönderildi. Sunucu durumu: %s", response.Status)
	return nil
}

// SendMongoInfo MongoDB bilgilerini sunucuya gönderir
func (r *Reporter) SendMongoInfo() error {
	log.Println("MongoDB bilgileri toplanıyor...")

	// MongoDB kolektörünü import et
	mongoCollector, err := r.importMongoCollector()
	if err != nil {
		return err
	}

	// MongoDB bilgilerini topla
	mongoInfo := mongoCollector.GetMongoInfo()

	// Bilgileri logla
	log.Printf("MongoDB Bilgileri: Cluster=%s, Status=%s, NodeStatus=%s, Version=%s",
		mongoInfo.ClusterName, mongoInfo.MongoStatus, mongoInfo.NodeStatus, mongoInfo.MongoVersion)

	// Hostname ve IP bilgilerini al
	hostname, _ := os.Hostname()
	ip := utils.GetLocalIP()

	// MongoDB bilgilerini proto mesajına dönüştür
	mongoProto := mongoInfo.ToProto()

	// Eksik alanları doldur (gerekirse)
	if mongoProto.Hostname == "" {
		mongoProto.Hostname = hostname
	}
	if mongoProto.Ip == "" {
		mongoProto.Ip = ip
	}

	// Yeni SendMongoInfo RPC'sini kullanarak verileri gönder
	client := pb.NewAgentServiceClient(r.grpcClient)

	// MongoInfoRequest oluştur
	request := &pb.MongoInfoRequest{
		MongoInfo: mongoProto,
	}

	// SendMongoInfo RPC'sini çağır
	response, err := client.SendMongoInfo(context.Background(), request)
	if err != nil {
		log.Printf("MongoDB bilgileri yeni RPC ile gönderilemedi: %v. Eski yöntem deneniyor...", err)

		// Eski yöntem: Stream üzerinden gönder
		err = r.ReportMongo(mongoProto)
		if err != nil {
			log.Printf("MongoDB bilgileri eski yöntemle de gönderilemedi: %v", err)
			return err
		}

		log.Println("MongoDB bilgileri başarıyla eski yöntemle gönderildi")
		return nil
	}

	log.Printf("MongoDB bilgileri başarıyla gönderildi. Sunucu durumu: %s", response.Status)
	return nil
}

// ReportMongo MongoDB verilerini merkezi sunucuya gönderir (eski yöntem)
func (r *Reporter) ReportMongo(data *pb.MongoInfo) error {
	// Create the complete AgentMessage with MongoInfo as the payload
	message := &pb.AgentMessage{
		Payload: &pb.AgentMessage_MongoInfo{
			MongoInfo: data,
		},
	}

	// Send the complete message
	err := r.stream.Send(message)
	if err != nil {
		return err
	}

	log.Printf("MongoDB verileri raporlandı: %s", data.Hostname)
	return nil
}

// importMongoCollector MongoDB kolektörünü import eder
func (r *Reporter) importMongoCollector() (*mongo.MongoCollector, error) {
	// MongoDB collector'ı oluştur
	return mongo.NewMongoCollector(r.cfg), nil
}

// ListMongoLogs MongoDB log dosyalarını listeler
func (r *Reporter) ListMongoLogs(ctx context.Context, req *pb.MongoLogListRequest) (*pb.MongoLogListResponse, error) {
	log.Printf("MongoDB log dosyaları listeleniyor. İstek: %+v", req)

	// MongoDB kolektörünü import et
	mongoCollector, err := r.importMongoCollector()
	if err != nil {
		return nil, fmt.Errorf("MongoDB kolektörü import edilemedi: %v", err)
	}

	// Log dosyalarını bul
	logFiles, err := mongoCollector.FindMongoLogFiles(req.LogPath)
	if err != nil {
		log.Printf("MongoDB log dosyaları listelenirken hata: %v", err)
		return nil, fmt.Errorf("MongoDB log dosyaları listelenirken hata: %v", err)
	}

	// Yanıtı oluştur
	response := &pb.MongoLogListResponse{
		LogFiles: logFiles,
	}

	log.Printf("%d adet MongoDB log dosyası listelendi", len(logFiles))

	// Dosya bilgilerini daha detaylı logla
	for i, file := range logFiles {
		log.Printf("Log dosyası %d: %s (Boyut: %d, Son değiştirilme: %s)",
			i+1, file.Path, file.Size, time.Unix(file.LastModified, 0).Format(time.RFC3339))
	}

	return response, nil
}

// StartPeriodicReporting periyodik raporlamayı başlatır
func (r *Reporter) StartPeriodicReporting(interval time.Duration, platform string) {
	// Önceki timer varsa durdur
	if r.reportTicker != nil {
		r.reportTicker.Stop()
	}

	// Yeni bir timer başlat
	r.reportTicker = time.NewTicker(interval)

	// Timer için dinleme işlemi başlat
	go func() {
		for {
			select {
			case <-r.reportTicker.C:
				log.Printf("Periyodik raporlama başlatılıyor (platform: %s)...", platform)
				var err error

				// Platform tipine göre verileri topla ve gönder
				if platform == "postgres" {
					err = r.SendPostgresInfo()
				} else if platform == "mongo" {
					err = r.SendMongoInfo()
				} else if platform == "mssql" {
					err = r.SendMSSQLInfo()
				}

				if err != nil {
					log.Printf("Periyodik raporlama hatası: %v", err)
				}
			case <-r.stopCh:
				if r.reportTicker != nil {
					r.reportTicker.Stop()
				}
				return
			}
		}
	}()

	log.Printf("Periyodik raporlama başlatıldı: Her %v", interval)
}

// AnalyzeMongoLog, MongoDB log dosyasını analiz eder ve önemli log girdilerini döndürür
func (r *Reporter) AnalyzeMongoLog(req *pb.MongoLogAnalyzeRequest) (*pb.MongoLogAnalyzeResponse, error) {
	log.Printf("MongoDB log analizi: Dosya=%s, Eşik=%d ms", req.LogFilePath, req.SlowQueryThresholdMs)

	// Import MongoDB collector
	log.Printf("MongoDB kolektörü import ediliyor...")
	mongoCollector, err := r.importMongoCollector()
	if err != nil {
		log.Printf("MongoDB kolektörü import hatası: %v", err)
		return nil, fmt.Errorf("failed to import MongoDB collector: %v", err)
	}
	log.Printf("MongoDB kolektörü başarıyla import edildi")

	// Analyze logs
	log.Printf("MongoDB kolektörüne analiz talebi gönderiliyor...")
	response, err := mongoCollector.AnalyzeMongoLog(req)
	if err != nil {
		log.Printf("MongoDB log analizi başarısız: %v", err)
		return nil, err
	}

	log.Printf("MongoDB log analizi tamamlandı. %d kayıt bulundu", len(response.LogEntries))
	return response, nil
}

// ReadPostgresConfig PostgreSQL konfigürasyon dosyasını okur ve belirtilen parametrelerin değerlerini döndürür
func (r *Reporter) ReadPostgresConfig(ctx context.Context, req *pb.PostgresConfigRequest) (*pb.PostgresConfigResponse, error) {
	log.Printf("ReadPostgresConfig metodu çağrıldı. AgentID: %s, ConfigPath: %s", req.AgentId, req.ConfigPath)

	// Konfigürasyon dosyası yolunu kontrol et
	configPath := req.ConfigPath
	if configPath == "" {
		// Eğer konfigürasyon dosyası yolu belirtilmemişse, otomatik bul
		var err error
		configPath, err = postgres.FindPostgresConfigFile()
		if err != nil {
			log.Printf("PostgreSQL konfigürasyon dosyası bulunamadı: %v", err)
			return &pb.PostgresConfigResponse{
				Status:     "error",
				ConfigPath: "",
			}, fmt.Errorf("PostgreSQL konfigürasyon dosyası bulunamadı: %v", err)
		}
	}

	log.Printf("PostgreSQL konfigürasyon dosyası okunuyor: %s", configPath)

	// Dosyanın var olup olmadığını kontrol et
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Printf("Konfigürasyon dosyası bulunamadı: %s", configPath)
		return &pb.PostgresConfigResponse{
			Status:     "error",
			ConfigPath: configPath,
		}, fmt.Errorf("konfigürasyon dosyası bulunamadı: %s", configPath)
	}

	// Konfigürasyon dosyasını oku
	configs, err := postgres.ReadPostgresConfig(configPath)
	if err != nil {
		log.Printf("Konfigürasyon dosyası okunamadı: %v", err)
		return &pb.PostgresConfigResponse{
			Status:     "error",
			ConfigPath: configPath,
		}, fmt.Errorf("konfigürasyon dosyası okunamadı: %v", err)
	}

	// Okunan değerleri logla
	log.Printf("PostgreSQL konfigürasyon dosyası okundu: %s, %d adet parametre bulundu", configPath, len(configs))
	for _, cfg := range configs {
		log.Printf("PostgreSQL Konfigürasyonu: %s = %s (%s)", cfg.Parameter, cfg.Value, cfg.Category)
	}

	// Yanıtı oluştur
	response := &pb.PostgresConfigResponse{
		Status:         "success",
		ConfigPath:     configPath,
		Configurations: configs,
	}

	return response, nil
}

// PromoteMongoToPrimary MongoDB node'unu primary'ye yükseltir
func (r *Reporter) PromoteMongoToPrimary(ctx context.Context, req *pb.MongoPromotePrimaryRequest) (*pb.MongoPromotePrimaryResponse, error) {
	log.Printf("MongoDB Primary Promotion işlemi başlatılıyor. JobID: %s, AgentID: %s, Hostname: %s, Port: %d, ReplicaSet: %s",
		req.JobId, req.AgentId, req.NodeHostname, req.Port, req.ReplicaSet)

	// MongoDB kolektörünü import et
	mongoCollector, err := r.importMongoCollector()
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.MongoPromotePrimaryResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED, // FAILED - değer proto dosyasından alınmalı
			ErrorMessage: errMsg,
		}, nil
	}

	// Primary promotion işlemini başlat
	result, err := mongoCollector.PromoteToPrimary(req.NodeHostname, int(req.Port), req.ReplicaSet)
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB primary promotion başarısız: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.MongoPromotePrimaryResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED, // FAILED - değer proto dosyasından alınmalı
			ErrorMessage: errMsg,
		}, nil
	}

	log.Printf("MongoDB Primary Promotion işlemi başarılı: %s", result)
	return &pb.MongoPromotePrimaryResponse{
		JobId:  req.JobId,
		Status: pb.JobStatus_JOB_STATUS_COMPLETED, // COMPLETED - değer proto dosyasından alınmalı
		Result: result,
	}, nil
}

// FreezeMongoSecondary MongoDB secondary node'larında rs.freeze() komutunu çalıştırır
func (r *Reporter) FreezeMongoSecondary(ctx context.Context, req *pb.MongoFreezeSecondaryRequest) (*pb.MongoFreezeSecondaryResponse, error) {
	log.Printf("MongoDB Freeze Secondary işlemi başlatılıyor. JobID: %s, AgentID: %s, Hostname: %s, Port: %d, ReplicaSet: %s, Seconds: %d",
		req.JobId, req.AgentId, req.NodeHostname, req.Port, req.ReplicaSet, req.Seconds)

	// Seconds değeri 0 ise varsayılan olarak 60 kullan
	seconds := int(req.Seconds)
	if seconds <= 0 {
		seconds = 60
		log.Printf("Seconds değeri 0 veya negatif, varsayılan değer kullanılıyor: %d", seconds)
	}

	// MongoDB kolektörünü import et
	mongoCollector, err := r.importMongoCollector()
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.MongoFreezeSecondaryResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED,
			ErrorMessage: errMsg,
		}, nil
	}

	// Freeze işlemini başlat
	result, err := mongoCollector.FreezeMongoSecondary(req.NodeHostname, int(req.Port), req.ReplicaSet, seconds)
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB freeze secondary başarısız: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.MongoFreezeSecondaryResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED,
			ErrorMessage: errMsg,
		}, nil
	}

	log.Printf("MongoDB Freeze Secondary işlemi başarılı: %s", result)
	return &pb.MongoFreezeSecondaryResponse{
		JobId:  req.JobId,
		Status: pb.JobStatus_JOB_STATUS_COMPLETED,
		Result: result,
	}, nil
}

// PromotePostgresToMaster PostgreSQL standby node'unu master'a yükseltir
func (r *Reporter) PromotePostgresToMaster(ctx context.Context, req *pb.PostgresPromoteMasterRequest) (*pb.PostgresPromoteMasterResponse, error) {
	log.Printf("PostgreSQL Master Promotion işlemi başlatılıyor. JobID: %s, AgentID: %s, Hostname: %s, DataDirectory: %s",
		req.JobId, req.AgentId, req.NodeHostname, req.DataDirectory)

	// Log izleyici oluştur ve başlat
	// Burada req.JobId kullanılıyor çünkü işlem ID'si dışarıdan geliyor
	client := pb.NewAgentServiceClient(r.grpcClient)
	logger := NewProcessLogger(client, req.AgentId, req.JobId, "postgresql_promotion")
	logger.Start()

	// Fonksiyon sonlandığında logger'ı durdur
	var loggerStopped bool = false
	defer func() {
		// İşlem başarısız olursa "failed", başarılı olursa "completed" olarak işaretle
		var finalStatus string = "completed" // Varsayılan olarak başarılı
		// Fonksiyon içinde return ile çıkılmışsa, yani bir hata durumu varsa, "failed" olarak ayarla
		if r := recover(); r != nil {
			finalStatus = "failed"
			logger.LogMessage(fmt.Sprintf("İşlem sırasında beklenmeyen bir hata oluştu: %v", r))
		}

		// Eğer logger daha önce durdurulmuş ise tekrar durdurma
		if !loggerStopped {
			logger.Stop(finalStatus)
		}
	}()

	// İlk log mesajı
	logger.LogMessage(fmt.Sprintf("PostgreSQL Master Promotion işlemi başlatılıyor. Data directory: %s", req.DataDirectory))

	// Metadata olarak hostname, data directory ve node status ekle
	logger.AddMetadata("hostname", req.NodeHostname)
	logger.AddMetadata("data_directory", req.DataDirectory)

	// Mevcut PostgreSQL sürümünü metadata'ya ekle
	currentPgVersion := postgres.GetPGVersion()
	logger.AddMetadata("pg_version", currentPgVersion)

	// Başlangıç node durumunu metadata'ya ekle
	initialNodeStatus := postgres.GetNodeStatus()
	logger.AddMetadata("initial_node_status", initialNodeStatus)

	// Data Directory kontrolü
	dataDir := req.DataDirectory
	if dataDir == "" {
		logger.LogMessage("Data directory belirtilmemiş, PostgreSQL konfigürasyonundan bulmaya çalışılıyor...")

		// Eğer data directory verilmemişse, konfigürasyon dosyasından bulmaya çalış
		configFile, err := postgres.FindPostgresConfigFile()
		if err == nil {
			logger.LogMessage(fmt.Sprintf("PostgreSQL konfigürasyon dosyası bulundu: %s", configFile))
			content, err := os.ReadFile(configFile)
			if err == nil {
				lines := strings.Split(string(content), "\n")
				for _, line := range lines {
					line = strings.TrimSpace(line)
					// Yorumları kaldır
					if idx := strings.Index(line, "#"); idx >= 0 {
						line = line[:idx]
					}
					if strings.HasPrefix(line, "data_directory") {
						parts := strings.SplitN(line, "=", 2)
						if len(parts) >= 2 {
							// Tırnak işaretlerini ve boşlukları kaldır
							dataDir = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
							logger.LogMessage(fmt.Sprintf("Konfigürasyondan data directory bulundu: %s", dataDir))
							break
						}
					}
				}
			} else {
				logger.LogMessage(fmt.Sprintf("Konfigürasyon dosyası okunamadı: %v", err))
			}
		} else {
			logger.LogMessage(fmt.Sprintf("PostgreSQL konfigürasyon dosyası bulunamadı: %v", err))
		}

		// Yine bulunamadıysa ps çıktısını kontrol et
		if dataDir == "" {
			logger.LogMessage("Data directory konfigürasyondan bulunamadı, process çıktısı kontrol ediliyor...")
			cmd := exec.Command("ps", "aux")
			output, err := cmd.Output()
			if err == nil {
				lines := strings.Split(string(output), "\n")
				for _, line := range lines {
					if strings.Contains(line, "postgres") || strings.Contains(line, "postmaster") {
						if idx := strings.Index(line, "-D"); idx >= 0 {
							fields := strings.Fields(line[idx:])
							if len(fields) >= 2 {
								dataDir = fields[1]
								logger.LogMessage(fmt.Sprintf("Data directory process çıktısından bulundu: %s", dataDir))
								break
							}
						}
					}
				}
			} else {
				logger.LogMessage(fmt.Sprintf("Process listesi alınamadı: %v", err))
			}
		}

		// Yine bulunamadıysa varsayılan değeri kullan
		if dataDir == "" {
			dataDir = "/var/lib/postgresql/data"
			logger.LogMessage(fmt.Sprintf("Data directory bulunamadı, varsayılan değer kullanılıyor: %s", dataDir))
		}
	}

	// Data directory varlığını kontrol et
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		errMsg := fmt.Sprintf("Data directory bulunamadı: %s", dataDir)
		logger.LogMessage(errMsg)

		// Logger için durumu güncelle ve durdur
		logger.Stop("failed")
		loggerStopped = true

		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED,
			ErrorMessage: errMsg,
		}, nil
	}

	// Node'un çalışıp çalışmadığını kontrol et
	pgStatus := postgres.GetPGServiceStatus()
	logger.LogMessage(fmt.Sprintf("PostgreSQL servis durumu: %s", pgStatus))
	logger.AddMetadata("service_status", pgStatus)

	if pgStatus != "RUNNING" {
		errMsg := "PostgreSQL servisi çalışmıyor, promotion yapılamaz"
		logger.LogError(errMsg, nil)

		// Logger'ı durdur
		logger.Stop("failed")
		loggerStopped = true

		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED,
			ErrorMessage: errMsg,
		}, nil
	}

	// Node durumunu kontrol et - standby olup olmadığı
	nodeStatus := postgres.GetNodeStatus()
	logger.LogMessage(fmt.Sprintf("PostgreSQL node durumu: %s", nodeStatus))
	logger.AddMetadata("initial_node_status", nodeStatus)

	if nodeStatus != "SLAVE" && nodeStatus != "STANDBY" {
		errMsg := fmt.Sprintf("Bu node zaten master/primary durumunda (%s), promotion gerekmez", nodeStatus)
		logger.LogMessage(errMsg)

		// Logger'ı durdur
		logger.Stop("completed") // Zaten master durumunda olduğu için başarılı sayılır
		loggerStopped = true

		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_COMPLETED,
			ErrorMessage: errMsg,
			Result:       fmt.Sprintf("Node zaten primary/master durumunda (%s)", nodeStatus),
		}, nil
	}

	// İki yöntem deneyeceğiz:
	// 1. Promotion trigger file oluşturma (PostgreSQL 12+)
	// 2. pg_ctl promote komutunu çalıştırma (fallback)

	// 1. Promotion işlemi için trigger dosyası yöntemi (PostgreSQL 12+)
	logger.LogMessage("PostgreSQL master promotion metod 1: Trigger dosyası yöntemi deneniyor...")

	triggerFilePath := filepath.Join(dataDir, "promote.trigger")
	standbySignalPath := filepath.Join(dataDir, "standby.signal")
	recoverySignalPath := filepath.Join(dataDir, "recovery.signal")

	// Standby sinyali dosyasının varlığını kontrol et
	isStandby := false
	if _, err := os.Stat(standbySignalPath); err == nil {
		isStandby = true
		logger.LogMessage(fmt.Sprintf("standby.signal dosyası bulundu: %s", standbySignalPath))
	} else if _, err := os.Stat(recoverySignalPath); err == nil {
		isStandby = true
		logger.LogMessage(fmt.Sprintf("recovery.signal dosyası bulundu: %s", recoverySignalPath))
	} else {
		logger.LogMessage("Standby sinyal dosyaları bulunamadı, alternatif metot deneyeceğiz")
	}

	promoted := false

	if isStandby {
		// Trigger dosyasını oluştur
		logger.LogMessage(fmt.Sprintf("Promotion trigger dosyası oluşturuluyor: %s", triggerFilePath))
		file, err := os.Create(triggerFilePath)
		if err != nil {
			logger.LogMessage(fmt.Sprintf("Promotion trigger dosyası oluşturulamadı: %v, alternatif metot deneyeceğiz", err))
		} else {
			file.Close()
			logger.LogMessage("Promotion trigger dosyası başarıyla oluşturuldu")

			// PostgreSQL'in dosyayı farketmesi için biraz bekle
			logger.LogMessage("PostgreSQL'in promotion trigger dosyasını farketmesi bekleniyor...")
			time.Sleep(5 * time.Second)

			// Promocyonun gerçekleşip gerçekleşmediğini kontrol et
			checkCount := 0
			maxChecks := 12 // 60 saniye toplam (5s * 12)

			for checkCount < maxChecks {
				// Standby sinyal dosyası hala var mı kontrol et
				_, standbyExists := os.Stat(standbySignalPath)
				_, recoveryExists := os.Stat(recoverySignalPath)

				if standbyExists != nil && recoveryExists != nil {
					// Standby sinyalleri kaldırılmış, promotion başarılı
					logger.LogMessage("Standby sinyal dosyaları kaldırılmış, promotion başarılı!")
					promoted = true
					break
				}

				// Node durumunu kontrol et
				currentStatus := postgres.GetNodeStatus()
				logger.LogMessage(fmt.Sprintf("Mevcut node durumu: %s", currentStatus))

				if currentStatus == "MASTER" || currentStatus == "PRIMARY" {
					logger.LogMessage(fmt.Sprintf("Node durumu artık %s, promotion başarılı!", currentStatus))
					promoted = true
					break
				}

				logger.LogMessage(fmt.Sprintf("Promotion hala tamamlanmadı, 5 saniye daha bekleniyor... (%d/%d)", checkCount+1, maxChecks))
				time.Sleep(5 * time.Second)
				checkCount++
			}
		}
	}

	// Eğer ilk yöntem başarısız olduysa veya standby sinyal dosyaları bulunamadıysa ikinci yöntemi dene
	if !promoted {
		logger.LogMessage("PostgreSQL master promotion metod 2: pg_ctl promote komutu deneniyor...")

		// PostgreSQL Version bilgisini al
		pgVersion := postgres.GetPGVersion()
		logger.LogMessage(fmt.Sprintf("PostgreSQL version: %s", pgVersion))

		// pg_ctl komutunu bul - birkaç olası yolu dene
		pgCtlPaths := []string{
			"pg_ctl",                           // Normal PATH'te varsa
			"/usr/lib/postgresql/*/bin/pg_ctl", // Debian/Ubuntu
			"/usr/pgsql-*/bin/pg_ctl",          // RHEL/CentOS
			"/usr/local/bin/pg_ctl",            // Homebrew/MacOS
			"/opt/PostgreSQL/*/bin/pg_ctl",     // EnterpriseDB
			"/var/lib/postgresql/*/bin/pg_ctl", // Custom
			"/usr/bin/pg_ctl",                  // Alternative
			"/bin/pg_ctl",                      // Alternative
			fmt.Sprintf("/usr/lib/postgresql/%s/bin/pg_ctl", pgVersion), // Sürüm belirterek
		}

		pgCtlCmd := ""
		for _, path := range pgCtlPaths {
			// Glob pattern'leri genişlet
			if strings.Contains(path, "*") {
				matches, err := filepath.Glob(path)
				if err == nil && len(matches) > 0 {
					// En son sürümü seç (glob patterns sıralı olmadığından en son ekleneni alalım)
					pgCtlCmd = matches[len(matches)-1]
					logger.LogMessage(fmt.Sprintf("pg_ctl bulundu (glob): %s", pgCtlCmd))
					break
				}
			} else {
				// Normal dosya kontrolü
				_, err := exec.LookPath(path)
				if err == nil {
					pgCtlCmd = path
					logger.LogMessage(fmt.Sprintf("pg_ctl bulundu (normal): %s", pgCtlCmd))
					break
				}
			}
		}

		// Son çare olarak doğrudan PostgreSQL binary path'ini kullan
		if pgCtlCmd == "" {
			// PostgreSQL'in data directory'sine göreceli bin dizinini dene
			dataDirParts := strings.Split(dataDir, "/")
			if len(dataDirParts) > 3 {
				// Genellikle /var/lib/postgresql/15/main gibi bir format olur
				// Bu durumda /usr/lib/postgresql/15/bin/pg_ctl deneyebiliriz
				possibleVersion := dataDirParts[len(dataDirParts)-2]
				pgCtlCmd = fmt.Sprintf("/usr/lib/postgresql/%s/bin/pg_ctl", possibleVersion)
				logger.LogMessage(fmt.Sprintf("pg_ctl yolu tahmin edildi: %s", pgCtlCmd))
			} else {
				// Bir şey bulamazsak normal pg_ctl kullan, sudo gerekebilir
				pgCtlCmd = "pg_ctl"
				logger.LogMessage("pg_ctl bulunamadı, varsayılan 'pg_ctl' kullanılacak")
			}
		}

		// sudo komutuyla çalıştırmanın gerekip gerekmediğini kontrol et
		needsSudo := os.Geteuid() != 0 && !strings.HasPrefix(dataDir, "/home/")

		// pg_ctl komutu ile promote yap
		var promoteCmdStr string
		if needsSudo {
			promoteCmdStr = fmt.Sprintf("sudo %s promote -D %s", pgCtlCmd, dataDir)
			logger.LogMessage(fmt.Sprintf("Sudo ile çalıştırılacak komut: %s", promoteCmdStr))
		} else {
			promoteCmdStr = fmt.Sprintf("%s promote -D %s", pgCtlCmd, dataDir)
			logger.LogMessage(fmt.Sprintf("Çalıştırılacak komut: %s", promoteCmdStr))
		}

		cmd := exec.Command("sh", "-c", promoteCmdStr)
		output, err := cmd.CombinedOutput()
		if err != nil {
			// İlk deneme başarısız olduysa, postgresql kullanıcısı olarak deneyebiliriz
			logger.LogMessage(fmt.Sprintf("İlk promote denemesi başarısız: %v - Çıktı: %s", err, string(output)))

			// PostgreSQL kullanıcısına geçiş yap - daha güvenli bir yaklaşım
			// sudo -u postgresql
			postgresUserCmd := fmt.Sprintf("sudo -u postgres %s promote -D %s", pgCtlCmd, dataDir)
			logger.LogMessage(fmt.Sprintf("PostgreSQL kullanıcısı ile çalıştırılacak komut: %s", postgresUserCmd))

			cmd = exec.Command("sh", "-c", postgresUserCmd)
			output, err = cmd.CombinedOutput()

			if err != nil {
				// Teşhis bilgilerini topla
				var diagnostics string

				// pg_ctl'nin var olup olmadığını kontrol et
				pgCtlExists := "Kontrol edilemiyor"
				if _, err := os.Stat(pgCtlCmd); err == nil {
					pgCtlExists = "Var"
				} else {
					pgCtlExists = fmt.Sprintf("Yok (%v)", err)
				}

				// Dizin izinlerini kontrol et
				var dirPermissions string
				dirInfo, err := os.Stat(dataDir)
				if err == nil {
					dirPermissions = fmt.Sprintf("%o", dirInfo.Mode().Perm())
				} else {
					dirPermissions = fmt.Sprintf("Okunamadı: %v", err)
				}

				// PostgreSQL servis durumunu kontrol et
				pgStatus := postgres.GetPGServiceStatus()

				// Diagnostic bilgileri
				diagnostics = fmt.Sprintf("Teşhis Bilgileri:\n- pg_ctl dosyası: %s\n- pg_ctl varlığı: %s\n- Veri dizini: %s\n- Veri dizini izinleri: %s\n- PostgreSQL servis durumu: %s",
					pgCtlCmd, pgCtlExists, dataDir, dirPermissions, pgStatus)

				logger.LogMessage(diagnostics)

				// pg_ctl komutu başarısız olduysa hata mesajını yazdır
				errMsg := fmt.Sprintf("pg_ctl promote komutu başarısız: %v - Çıktı: %s\n\n%s", err, string(output), diagnostics)
				logger.LogMessage(errMsg)

				// Logger'ı durdur
				logger.Stop("failed")
				loggerStopped = true

				// Her iki yöntem de başarısız, hata yanıtı döndür
				return &pb.PostgresPromoteMasterResponse{
					JobId:        req.JobId,
					Status:       pb.JobStatus_JOB_STATUS_FAILED,
					ErrorMessage: errMsg,
				}, nil
			}
		}

		logger.LogMessage(fmt.Sprintf("pg_ctl promote komutu çıktısı: %s", string(output)))

		// Promotion sonrası node durumunu kontrol et
		checkCount := 0
		maxChecks := 12 // 60 saniye toplam (5s * 12)

		for checkCount < maxChecks {
			currentStatus := postgres.GetNodeStatus()
			logger.LogMessage(fmt.Sprintf("Mevcut node durumu: %s", currentStatus))

			if currentStatus == "MASTER" || currentStatus == "PRIMARY" {
				logger.LogMessage(fmt.Sprintf("Node durumu artık %s, promotion başarılı!", currentStatus))
				promoted = true
				break
			}

			logger.LogMessage(fmt.Sprintf("Promotion sonrası node durumu hala %s, 5 saniye daha bekleniyor... (%d/%d)",
				currentStatus, checkCount+1, maxChecks))
			time.Sleep(5 * time.Second)
			checkCount++
		}
	}

	if !promoted {
		errMsg := "PostgreSQL promotion zaman aşımına uğradı, node hala master'a yükseltilemedi"
		logger.LogMessage(errMsg)

		// Logger'ı durdur
		logger.Stop("failed")
		loggerStopped = true

		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED,
			ErrorMessage: errMsg,
		}, nil
	}

	// Son kontrol - gerçekten PRIMARY/MASTER mı?
	finalStatus := postgres.GetNodeStatus()
	logger.LogMessage(fmt.Sprintf("Son node durumu: %s", finalStatus))

	if finalStatus != "MASTER" && finalStatus != "PRIMARY" {
		errMsg := fmt.Sprintf("PostgreSQL promotion başarısız, son durum: %s", finalStatus)
		logger.LogMessage(errMsg)

		// Logger'ı durdur
		logger.Stop("failed")
		loggerStopped = true

		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED,
			ErrorMessage: errMsg,
		}, nil
	}

	// Promotion başarılı
	successMsg := fmt.Sprintf("PostgreSQL node başarıyla master'a yükseltildi! Son durum: %s", finalStatus)
	logger.LogMessage(successMsg)

	// Logger'ı başarılı olarak durdur
	logger.Stop("completed")
	loggerStopped = true

	return &pb.PostgresPromoteMasterResponse{
		JobId:  req.JobId,
		Status: pb.JobStatus_JOB_STATUS_COMPLETED,
		Result: successMsg,
	}, nil
}

// ExplainQuery PostgreSQL sorgu planını alır ve döndürür
func (r *Reporter) ExplainQuery(ctx context.Context, req *pb.ExplainQueryRequest) (*pb.ExplainQueryResponse, error) {
	log.Printf("PostgreSQL sorgu planı isteği alındı. AgentID: %s, Veritabanı: %s", req.AgentId, req.Database)

	// Veritabanı bağlantısını aç
	db, err := postgres.OpenDB()
	if err != nil {
		errMsg := fmt.Sprintf("Veritabanı bağlantısı açılamadı: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}
	defer db.Close()

	// İstenen veritabanına geçiş yap (eğer belirtilmişse)
	if req.Database != "" {
		// Mevcut bağlantı bilgilerini al
		var currentDB string
		err := db.QueryRow("SELECT current_database()").Scan(&currentDB)
		if err != nil {
			log.Printf("UYARI: Mevcut veritabanı bilgisi alınamadı: %v", err)
		}

		if req.Database != currentDB {
			log.Printf("Veritabanı değiştiriliyor: %s -> %s", currentDB, req.Database)

			// Bağlantı bilgilerini al
			var currentHost string
			db.QueryRow("SELECT inet_server_addr()").Scan(&currentHost)
			if currentHost == "" {
				currentHost = "localhost"
			}

			var currentPort string
			db.QueryRow("SELECT inet_server_port()").Scan(&currentPort)
			if currentPort == "" {
				currentPort = "5432"
			}

			// Mevcut bağlantıyı kapat
			db.Close()

			// Yeni veritabanına bağlan
			connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
				currentHost, currentPort, r.cfg.PostgreSQL.User, r.cfg.PostgreSQL.Pass, req.Database)

			newDB, err := sql.Open("postgres", connStr)
			if err != nil {
				errMsg := fmt.Sprintf("Veritabanı bağlantısı açılamadı: %v", err)
				log.Printf("HATA: %s", errMsg)
				return &pb.ExplainQueryResponse{
					Status:       "error",
					ErrorMessage: errMsg,
				}, nil
			}

			// Bağlantıyı test et
			if err := newDB.Ping(); err != nil {
				errMsg := fmt.Sprintf("Veritabanı bağlantısı test edilemedi: %v", err)
				log.Printf("HATA: %s", errMsg)
				newDB.Close()
				return &pb.ExplainQueryResponse{
					Status:       "error",
					ErrorMessage: errMsg,
				}, nil
			}

			// Bağlantıyı güncelle
			db = newDB
		}
	}

	// Sorguyu doğrudan çalıştır - server tarafından sarmalanmış olarak gelecek
	log.Printf("Sorgu çalıştırılıyor: %s", req.Query)

	// Sorguyu çalıştır
	rows, err := db.Query(req.Query)
	if err != nil {
		errMsg := fmt.Sprintf("Sorgu planı alınamadı: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}
	defer rows.Close()

	// Sonuçları topla
	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			errMsg := fmt.Sprintf("Sonuç satırı okunamadı: %v", err)
			log.Printf("UYARI: %s", errMsg)
			continue
		}
		planLines = append(planLines, line)
	}

	// Satır okumada hata olup olmadığını kontrol et
	if err := rows.Err(); err != nil {
		errMsg := fmt.Sprintf("Sorgu planı sonuçları okunurken hata: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}

	// Sonuç satırları yoksa, boş bir plan döndür
	if len(planLines) == 0 {
		log.Printf("Sorgu planı alındı, ancak sonuç yok")
		return &pb.ExplainQueryResponse{
			Status: "success",
			Plan:   "Sorgu planı sonucu bulunamadı.",
		}, nil
	}

	// Tüm plan satırlarını birleştir
	plan := strings.Join(planLines, "\n")
	log.Printf("Sorgu planı başarıyla alındı (%d satır)", len(planLines))

	return &pb.ExplainQueryResponse{
		Status: "success",
		Plan:   plan,
	}, nil
}

// ExplainMongoQuery returns an execution plan for a MongoDB query using explain()
func (r *Reporter) ExplainMongoQuery(ctx context.Context, req *pb.ExplainQueryRequest) (*pb.ExplainQueryResponse, error) {
	log.Printf("MongoDB sorgu planı isteği alındı. AgentID: %s, Veritabanı: %s", req.AgentId, req.Database)

	// Veritabanını ve sorguyu kontrol et
	if req.Database == "" {
		errMsg := "Veritabanı adı gereklidir"
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}

	if req.Query == "" {
		errMsg := "Sorgu metni gereklidir"
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}

	// Sorgu protokol kontrol - server'dan gelen protokol formatını kontrol et
	if strings.HasPrefix(req.Query, "MONGODB_EXPLAIN||") {
		log.Printf("Server protokol formatı tespit edildi: MONGODB_EXPLAIN|| prefixli sorgu")
		// Prefix'i kaldır ve asıl sorguyu al
		req.Query = strings.TrimPrefix(req.Query, "MONGODB_EXPLAIN||")
	} else if strings.HasPrefix(req.Query, "MONGO_EXPLAIN|") {
		log.Printf("Server protokol formatı tespit edildi: MONGO_EXPLAIN| prefixli sorgu")
		// Protokol formatını parse et
		parts := strings.Split(req.Query, "|")
		if len(parts) >= 3 {
			// Format: MONGO_EXPLAIN|<database>|<query_json>
			// Database bilgisini güncelle
			newDatabase := parts[1]
			if newDatabase != "" && newDatabase != "null" {
				req.Database = newDatabase
				log.Printf("Protokolden veritabanı alındı: %s", req.Database)
			}
			// Sorguyu güncelle - 2. indeksten sonraki tüm parçaları birleştir
			// (sorgu içinde pipe karakteri olabilir)
			req.Query = strings.Join(parts[2:], "|")
		} else if len(parts) == 2 {
			// Format: MONGO_EXPLAIN|<query_json>
			req.Query = parts[1]
		}
	}

	// Sorgunun içeriğini kontrol et, eğer {"database": "...", "query": ...} formatında ise, iç sorguyu çıkar
	if strings.Contains(req.Query, "\"database\"") && strings.Contains(req.Query, "\"query\"") {
		log.Printf("Server JSON wrapper formatı tespit edildi: {database, query} içeren sorgu")

		// JSON'u parse et
		var wrapper struct {
			Database string          `json:"database"`
			Query    json.RawMessage `json:"query"`
		}

		if err := json.Unmarshal([]byte(req.Query), &wrapper); err == nil {
			// Database bilgisini güncelle eğer boşsa
			if wrapper.Database != "" && wrapper.Database != "null" && req.Database == "" {
				req.Database = wrapper.Database
				log.Printf("JSON içinden veritabanı alındı: %s", req.Database)
			}

			// Query içeriğini asıl sorgu olarak kullan
			if len(wrapper.Query) > 0 {
				req.Query = string(wrapper.Query)

				// JSON string olabilir, escape karakterlerini temizle
				if strings.HasPrefix(req.Query, "\"") && strings.HasSuffix(req.Query, "\"") {
					// String içindeki tırnak işaretlerini kaldırma
					unquotedStr, err := strconv.Unquote(req.Query)
					if err == nil {
						req.Query = unquotedStr
					}
				}
			}
		}
	}

	// MongoDB kolektörünü oluştur
	mongoCollector, err := r.importMongoCollector()
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB kolektörü import edilemedi: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}

	// ExplainMongoQuery fonksiyonunu çağır
	plan, err := mongoCollector.ExplainMongoQuery(req.Database, req.Query)
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB sorgu planı alınamadı: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}

	// Artık plan string formatında, doğrudan döndür
	return &pb.ExplainQueryResponse{
		Status: "success",
		Plan:   plan,
	}, nil
}

// isValidJSON, string'in geçerli bir JSON olup olmadığını kontrol eder
func isValidJSON(s string) bool {
	var js interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

// min returns the smaller of x or y.
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Okunabilir plan oluşturmak için yardımcı fonksiyon
func createReadablePlan(resultBytes []byte) string {
	var planText string
	var resultMap map[string]interface{}

	if err := json.Unmarshal(resultBytes, &resultMap); err != nil {
		log.Printf("UYARI: JSON parse edilemedi, ham sonuç kullanılacak: %v", err)
		return string(resultBytes)
	}

	// MongoDB explain sonuçları farklı anahtarlarda olabilir
	if explainData, ok := resultMap["queryPlanner"]; ok {
		// queryPlanner formatında
		explainBytes, err := json.MarshalIndent(explainData, "", "  ")
		if err == nil {
			planText += "## Query Planner\n" + string(explainBytes) + "\n\n"
		}
	}

	if explainData, ok := resultMap["executionStats"]; ok {
		// executionStats formatında
		explainBytes, err := json.MarshalIndent(explainData, "", "  ")
		if err == nil {
			planText += "## Execution Stats\n" + string(explainBytes) + "\n\n"
		}
	}

	if explainData, ok := resultMap["serverInfo"]; ok {
		// serverInfo formatında
		explainBytes, err := json.MarshalIndent(explainData, "", "  ")
		if err == nil {
			planText += "## Server Info\n" + string(explainBytes) + "\n\n"
		}
	}

	// Eğer özel formatlar bulunamadıysa, tüm JSON yanıtını kullan
	if planText == "" {
		// İkinci bir şans olarak command sonucuna bak
		if cmdResult, ok := resultMap["command"]; ok {
			cmdBytes, err := json.MarshalIndent(cmdResult, "", "  ")
			if err == nil {
				planText = "## Command\n" + string(cmdBytes) + "\n\n"
			}
		}

		// Hala boşsa, tüm sonucu kullan
		if planText == "" {
			resultBytes, _ := json.MarshalIndent(resultMap, "", "  ")
			planText = string(resultBytes)
		}
	}

	return planText
}

// MSSQLQueryProcessor, MSSQL sorgu işleme mantığını temsil eder
type MSSQLQueryProcessor struct {
	cfg *config.AgentConfig
}

// NewMSSQLQueryProcessor, yeni bir MSSQL sorgu işleyici oluşturur
func NewMSSQLQueryProcessor(cfg *config.AgentConfig) *MSSQLQueryProcessor {
	return &MSSQLQueryProcessor{
		cfg: cfg,
	}
}

// processExplainMSSQLQuery, MSSQL sorgusunun execution planını XML formatında döndürür
func (p *MSSQLQueryProcessor) processExplainMSSQLQuery(query string, database string) map[string]interface{} {
	log.Printf("[DEBUG] MSSQL Explain sorgusu işleniyor: %s", query)
	log.Printf("[DEBUG] Hedef veritabanı: %s", database)

	// İşlem başlangıç zamanını kaydet
	startTime := time.Now()

	// MSSQL kolektörünü oluştur
	collector := mssql.NewMSSQLCollector(p.cfg)

	// MSSQL bağlantısını aç
	db, err := collector.GetClient()
	if err != nil {
		log.Printf("[ERROR] MSSQL bağlantısı kurulamadı: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("MSSQL bağlantısı kurulamadı: %v", err),
		}
	}
	defer db.Close()

	// Veritabanını kullanmaya ayarla
	if database != "" {
		useDbCmd := fmt.Sprintf("USE [%s]", database)
		log.Printf("[DEBUG] Veritabanı değiştiriliyor: %s", useDbCmd)
		_, err = db.Exec(useDbCmd)
		if err != nil {
			log.Printf("[ERROR] MSSQL veritabanı değiştirilemedi: %v", err)
			return map[string]interface{}{
				"status":  "error",
				"message": fmt.Sprintf("MSSQL veritabanı değiştirilemedi: %v", err),
			}
		}
		log.Printf("[DEBUG] MSSQL veritabanı '%s' olarak ayarlandı", database)
	}

	// SSMS'nin kullandığı yöntemi kullan - STATISTICS XML
	// Bu yöntem hem sorgu sonuçlarını hem de XML planı döndürür (iki ayrı result set)
	statsQuery := fmt.Sprintf(`
SET STATISTICS XML ON;
%s
SET STATISTICS XML OFF;`, query)

	log.Printf("[DEBUG] STATISTICS XML sorgusu çalıştırılıyor...")

	// Sorguyu çalıştır
	rows, err := db.Query(statsQuery)
	if err != nil {
		log.Printf("[ERROR] STATISTICS XML sorgusu çalıştırılırken hata: %v", err)
		return createBasicPlanXML(query, database, startTime)
	}
	defer rows.Close()

	// İlk result set sorgu sonuçları içerir - bu set boş olabilir, hata almamak için kontrol et
	var xmlPlan string

	// İlk result set için sütun bilgilerini logla
	columns, err := rows.Columns()
	if err != nil {
		log.Printf("[ERROR] Result set sütunlarını okuma hatası: %v", err)
	} else {
		log.Printf("[DEBUG] İlk result set %d sütun içeriyor: %v", len(columns), columns)

		// İlk result set satırlarını kontrol et ve logla
		rowCount := 0
		for rows.Next() {
			rowCount++

			// Dinamik olarak sütunları oku
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Printf("[ERROR] İlk result set satırı okunamadı: %v", err)
				continue
			}

			// Satır verilerini logla (ilk 5 satır, çok fazla veri olmaması için)
			if rowCount <= 5 {
				logRow := make(map[string]interface{})
				for i, col := range columns {
					var strVal string
					val := values[i]

					if val == nil {
						strVal = "NULL"
					} else {
						switch v := val.(type) {
						case []byte:
							// XML veya büyük veriler için kısa özet göster
							if len(v) > 100 {
								strVal = fmt.Sprintf("BINARY_DATA(%d bytes)", len(v))
								// XML içeriği kontrol et
								if strings.Contains(string(v), "<ShowPlanXML") {
									xmlPlan = string(v)
									log.Printf("[DEBUG] İlk result sette XML plan bulundu (sütun: %s)", col)
								}
							} else {
								strVal = string(v)
							}
						default:
							strVal = fmt.Sprintf("%v", v)
						}
					}

					logRow[col] = strVal
				}
				log.Printf("[DEBUG] İlk result set, Satır %d: %v", rowCount, logRow)
			}
		}

		log.Printf("[DEBUG] İlk result sette toplam %d satır bulundu", rowCount)

		// Satır okuma hatasını kontrol et
		if rows.Err() != nil {
			log.Printf("[ERROR] İlk result set okunurken hata: %v", rows.Err())
		}
	}

	// İkinci result seti kontrol et - burası XML planı içermeli
	if rows.NextResultSet() {
		log.Printf("[DEBUG] İkinci result set (XML plan) bulundu")

		// İkinci result set için sütun bilgilerini logla
		columns2, err := rows.Columns()
		if err != nil {
			log.Printf("[ERROR] İkinci result set sütunlarını okuma hatası: %v", err)
		} else {
			log.Printf("[DEBUG] İkinci result set %d sütun içeriyor: %v", len(columns2), columns2)
		}

		if rows.Next() {
			// İlk sütun XML planını içermeli
			if err := rows.Scan(&xmlPlan); err != nil {
				log.Printf("[ERROR] XML plan okunurken hata: %v", err)
				return createBasicPlanXML(query, database, startTime)
			}

			// XML plan içeriğinin başlangıcını kontrol et
			if !strings.Contains(xmlPlan, "<ShowPlanXML") {
				log.Printf("[ERROR] Geçersiz XML plan formatı: %s", trimString(xmlPlan, 100))
				return createBasicPlanXML(query, database, startTime)
			}

			log.Printf("[DEBUG] XML plan başarıyla okundu, uzunluk: %d karakterr", len(xmlPlan))
		} else {
			log.Printf("[WARN] İkinci result sette satır yok")
			return createBasicPlanXML(query, database, startTime)
		}
	} else {
		log.Printf("[WARN] XML plan için ikinci result set bulunamadı")

		// STATISTICS XML result seti yoksa, belki ilk result sette vardır
		// (bazı SQL Server sürümlerinde düzen farklı olabilir)
		if len(xmlPlan) > 0 {
			log.Printf("[DEBUG] İlk result sette XML plan bulunmuştu, kullanılıyor")
		} else {
			// Alternatif bir yaklaşım deneyelim - doğrudan execution planı sorgulayalım
			log.Printf("[DEBUG] Alternatif yöntem: Doğrudan execution plan sorgusu")
			directPlanQuery := fmt.Sprintf(`
			DECLARE @plan_handle VARBINARY(64);
			DECLARE @query NVARCHAR(MAX) = N'%s';
			
			-- Sorguyu çalıştır ve plan handle'ı al
			EXEC sp_executesql @query;
			
			-- En son çalıştırılan sorgunun plan handle'ını al
			SELECT TOP 1 @plan_handle = plan_handle 
			FROM sys.dm_exec_query_stats 
			ORDER BY last_execution_time DESC;
			
			-- Plan handle kullanarak XML execution planını al
			SELECT query_plan 
			FROM sys.dm_exec_query_plan(@plan_handle);
			`, query)

			planRows, planErr := db.Query(directPlanQuery)
			if planErr != nil {
				log.Printf("[ERROR] Alternatif execution plan sorgusu başarısız: %v", planErr)
			} else {
				defer planRows.Close()
				if planRows.Next() {
					if err := planRows.Scan(&xmlPlan); err != nil {
						log.Printf("[ERROR] Alternatif execution plan okuma hatası: %v", err)
					} else if strings.Contains(xmlPlan, "<ShowPlanXML") {
						log.Printf("[DEBUG] Alternatif yöntemle XML plan bulundu, uzunluk: %d karakter", len(xmlPlan))
					} else {
						log.Printf("[ERROR] Alternatif yöntemde geçersiz XML plan: %s", trimString(xmlPlan, 100))
						xmlPlan = ""
					}
				} else {
					log.Printf("[WARN] Alternatif yöntemde satır yok")
				}
			}

			if xmlPlan == "" {
				log.Printf("[ERROR] Hiçbir result sette veri yok")
				return createBasicPlanXML(query, database, startTime)
			}
		}
	}

	// İşlem süresini hesapla
	duration := time.Since(startTime).Milliseconds()
	log.Printf("MSSQL explain sorgusu işleme tamamlandı: %d ms, XML plan alındı (uzunluk: %d)",
		duration, len(xmlPlan))

	// XML planını döndür
	return map[string]interface{}{
		"status":      "success",
		"plan":        xmlPlan,
		"duration_ms": duration,
	}
}

// processMSSQLQuery, gelen MSSQL sorgusunu işler ve sonucu döndürür
func (p *MSSQLQueryProcessor) processMSSQLQuery(command string, database string) map[string]interface{} {
	const (
		maxRows          = 100         // Maksimum satır sayısı
		maxValueLen      = 1000        // Maksimum değer uzunluğu
		maxTotalDataSize = 1024 * 1024 // Maksimum toplam veri boyutu (1 MB)
	)

	log.Printf("[DEBUG] MSSQL sorgusu işleniyor: %s", command)
	log.Printf("[DEBUG] Hedef veritabanı: %s", database)

	// İşlem başlangıç zamanını kaydet
	startTime := time.Now()

	// MSSQL kolektörünü oluştur
	collector := mssql.NewMSSQLCollector(p.cfg)

	// MSSQL bağlantısını aç
	db, err := collector.GetClient()
	if err != nil {
		log.Printf("[ERROR] MSSQL bağlantısı kurulamadı: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("MSSQL bağlantısı kurulamadı: %v", err),
		}
	}
	defer db.Close()

	// Veritabanını kullanmaya ayarla (eğer belirtilmişse)
	if database != "" {
		_, err = db.Exec(fmt.Sprintf("USE [%s]", database))
		if err != nil {
			log.Printf("[ERROR] MSSQL veritabanı değiştirilemedi: %v", err)
			return map[string]interface{}{
				"status":  "error",
				"message": fmt.Sprintf("MSSQL veritabanı değiştirilemedi: %v", err),
			}
		}
		log.Printf("[DEBUG] MSSQL veritabanı '%s' olarak ayarlandı", database)
	}

	// Sorguyu çalıştır
	rows, err := db.Query(command)
	if err != nil {
		log.Printf("[ERROR] MSSQL sorgusu çalıştırılırken hata: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("MSSQL sorgusu çalıştırılırken hata: %v", err),
		}
	}
	defer rows.Close()

	// Sütun isimlerini al
	columns, err := rows.Columns()
	if err != nil {
		log.Printf("[ERROR] MSSQL sorgusu sütun bilgileri alınamadı: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("MSSQL sorgusu sütun bilgileri alınamadı: %v", err),
		}
	}
	log.Printf("[DEBUG] MSSQL sorgusu sütunlar: %v", columns)

	// Sorgu açıklaması için ID al
	var queryDesc string
	if strings.Contains(strings.ToLower(command), "pg_cachehitratio") {
		queryDesc = "Cache Hit Ratio"
	} else {
		queryDesc = command
	}
	log.Printf("İşleniyor: %s", trimString(queryDesc, 100))

	// Sonuçları işle
	var results []map[string]interface{}
	var totalDataSize int
	rowCount := 0

	for rows.Next() {
		// Satır sayısını kontrol et
		if rowCount >= maxRows {
			log.Printf("Maksimum satır sayısına ulaşıldı (%d), sonuçlar kırpılıyor", maxRows)
			break
		}
		rowCount++

		// Sütun sayısı kadar pointer oluştur
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Satırı oku
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("[ERROR] MSSQL sorgusu satır okunurken hata: %v", err)
			return map[string]interface{}{
				"status":  "error",
				"message": fmt.Sprintf("MSSQL sorgusu satır okunurken hata: %v", err),
			}
		}

		// Satırı map'e dönüştür
		rowMap := make(map[string]interface{})
		rowSize := 0
		for i, col := range columns {
			val := values[i]
			var strVal string

			switch v := val.(type) {
			case []byte:
				strVal = string(v)
				if len(strVal) > maxValueLen {
					strVal = strVal[:maxValueLen]
					log.Printf("Değer uzunluğu kırpıldı: %s = %d karakterden %d karaktere",
						col, len(v), maxValueLen)
				}
				rowMap[col] = strVal
				rowSize += len(strVal)
			case time.Time:
				strVal = v.Format(time.RFC3339)
				rowMap[col] = strVal
				rowSize += len(strVal)
			case nil:
				log.Printf("[DEBUG] NULL değer: %s", col)
				rowMap[col] = ""
			default:
				// Diğer tipleri string'e çevir
				strVal = fmt.Sprintf("%v", v)
				if len(strVal) > maxValueLen {
					strVal = strVal[:maxValueLen]
					log.Printf("Değer uzunluğu kırpıldı: %s = %d karakterden %d karaktere",
						col, len(strVal), maxValueLen)
				}
				rowMap[col] = v
				rowSize += len(strVal)
			}
		}

		log.Printf("[DEBUG] Satır: %d, İçerik: %v", rowCount, rowMap)

		// Toplam veri boyutunu kontrol et
		totalDataSize += rowSize
		if totalDataSize > maxTotalDataSize {
			log.Printf("Maksimum veri boyutuna ulaşıldı (%d), sonuçlar kırpılıyor", maxTotalDataSize)
			break
		}

		results = append(results, rowMap)
	}

	// Sonuçları kontrol et
	if err := rows.Err(); err != nil {
		log.Printf("[ERROR] MSSQL sorgusu sonuçları okunurken hata: %v", err)
		return map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("MSSQL sorgusu sonuçları okunurken hata: %v", err),
		}
	}

	// İşlem süresini hesapla
	duration := time.Since(startTime).Milliseconds()
	log.Printf("MSSQL sorgusu işleme tamamlandı: %d satır, %d bayt, %d ms",
		rowCount, totalDataSize, duration)

	// Sonuç satırlarının sayısını kontrol et
	if len(results) == 0 {
		log.Printf("[INFO] MSSQL sorgusu sonuç döndürmedi (empty result set)")
		// Boş sonuç durumunda bile bilgi döndür
		return map[string]interface{}{
			"status":        "success",
			"message":       "MSSQL sorgusu başarıyla çalıştı ama sonuç bulunamadı",
			"rows_returned": 0,
			"duration_ms":   duration,
		}
	}

	// Tek satır sonuç varsa düz map olarak döndür
	if len(results) == 1 {
		result := results[0]
		result["status"] = "success"
		result["rows_processed"] = rowCount
		result["data_size"] = totalDataSize
		result["duration_ms"] = duration
		return result
	}

	// Birden fazla satır varsa düz bir map yapısına dönüştür
	flatResult := make(map[string]interface{})
	flatResult["status"] = "success"
	flatResult["row_count"] = len(results)
	flatResult["rows_processed"] = rowCount
	flatResult["data_size"] = totalDataSize
	flatResult["duration_ms"] = duration
	flatResult["data_truncated"] = (rowCount >= maxRows || totalDataSize >= maxTotalDataSize)

	// Her bir satırı düz bir map'e ekle
	for i, row := range results {
		for key, value := range row {
			// Her bir alanı index ile birlikte sakla
			flatResult[fmt.Sprintf("%s_%d", key, i)] = value
		}
	}

	return flatResult
}

// createBasicPlanXML basit bir execution plan XML'i oluşturur
func createBasicPlanXML(query string, database string, startTime time.Time) map[string]interface{} {
	duration := time.Since(startTime).Milliseconds()

	// Sorgudan temel bilgileri çıkar
	tableName := "Bilinmiyor"

	// FROM ifadesinden sonraki tabloyu bulmaya çalış
	fromIndex := strings.Index(strings.ToUpper(query), "FROM")
	if fromIndex > 0 {
		afterFrom := query[fromIndex+5:]
		// Tablonun sonundaki WHERE, GROUP BY, vb. ifadeleri temizle
		whereIndex := strings.Index(strings.ToUpper(afterFrom), "WHERE")
		if whereIndex > 0 {
			afterFrom = afterFrom[:whereIndex]
		}

		// Temizlenmiş tablo adını al
		tableName = strings.TrimSpace(afterFrom)
		// Alias'ları temizle
		spaceIndex := strings.Index(tableName, " ")
		if spaceIndex > 0 {
			tableName = tableName[:spaceIndex]
		}
	}

	// Temel özellikleri içeren bir execution plan şablonu
	xmlPlan := fmt.Sprintf(`<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.5">
  <BatchSequence>
    <Batch>
      <Statements>
        <StmtSimple StatementText="%s" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="0.01" StatementEstRows="1" StatementOptmLevel="TRIVIAL">
          <StatementSetOptions QUOTED_IDENTIFIER="true" ARITHABORT="true" CONCAT_NULL_YIELDS_NULL="true" ANSI_NULLS="true" ANSI_PADDING="true" ANSI_WARNINGS="true" NUMERIC_ROUNDABORT="false" />
          <QueryPlan>
            <MissingIndexes>
              <MissingIndexGroup Impact="99">
                <MissingIndex Database="[%s]" Schema="[dbo]" Table="[%s]">
                  <ColumnGroup Usage="INEQUALITY">
                    <Column Name="Id" ColumnId="1" />
                  </ColumnGroup>
                </MissingIndex>
              </MissingIndexGroup>
            </MissingIndexes>
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.01" EstimateCPU="0.0001" AvgRowSize="182" EstimatedTotalSubtreeCost="0.01" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0">
              <OutputList>
                <ColumnReference Database="[%s]" Schema="[dbo]" Table="[%s]" Column="Id" />
              </OutputList>
              <TableScan Ordered="0" ForcedIndex="0" NoExpandHint="0" Storage="RowStore">
                <DefinedValues>
                  <DefinedValue>
                    <ColumnReference Database="[%s]" Schema="[dbo]" Table="[%s]" Column="Id" />
                  </DefinedValue>
                </DefinedValues>
                <Object Database="[%s]" Schema="[dbo]" Table="[%s]" />
              </TableScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
      </Statements>
    </Batch>
  </BatchSequence>
</ShowPlanXML>`,
		query, database, tableName, database, tableName, database, tableName, database, tableName)

	log.Printf("[DEBUG] Temel execution plan şablonu oluşturuldu (temel özelliklerle)")

	return map[string]interface{}{
		"status":      "success",
		"plan":        xmlPlan,
		"duration_ms": duration,
		"estimated":   true, // Gerçek plan olmadığını belirt
		"message":     "Gerçek execution plan alınamadı, tahmini temel bir plan oluşturuldu",
	}
}
