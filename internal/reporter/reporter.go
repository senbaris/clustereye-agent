package reporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/senbaris/clustereye-agent/internal/collector/mongo"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"go.mongodb.org/mongo-driver/bson"
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
		}

		// Alarm izleme sistemini başlat
		agentID := "agent_" + hostname
		client := pb.NewAgentServiceClient(r.grpcClient)
		r.alarmMonitor = alarm.NewAlarmMonitor(client, agentID, r.cfg, platform)
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

	// Sadece PostgreSQL platformu için veritabanı bağlantısı kur
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
								queryStr = parts[2]
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

						// Hostname'den agentID oluştur
						hostname, _ := os.Hostname()
						agentID := "agent_" + hostname

						explainReq := &pb.ExplainQueryRequest{
							AgentId:  agentID,
							Database: database, // Protokolden gelen veya orijinal database
							Query:    queryStr, // Protokolden çıkarılan sorgu
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
							queryResult := map[string]interface{}{
								"status": explainResp.Status,
								"plan":   explainResp.Plan,
							}

							if explainResp.ErrorMessage != "" {
								queryResult["message"] = explainResp.ErrorMessage
							}

							sendQueryResult(r.stream, query.QueryId, queryResult)
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
	r.reportTicker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-r.stopCh:
				r.reportTicker.Stop()
				return
			case <-r.reportTicker.C:
				// Platform seçimine göre raporlama yap
				if platform == "postgres" {
					if err := r.SendPostgresInfo(); err != nil {
						log.Printf("Periyodik PostgreSQL raporlama hatası: %v", err)
					}
				} else if platform == "mongo" {
					if err := r.SendMongoInfo(); err != nil {
						log.Printf("Periyodik MongoDB raporlama hatası: %v", err)
					}
				} else {
					log.Printf("Bilinmeyen platform tipi: %s", platform)
				}
			}
		}
	}()

	log.Printf("Periyodik raporlama başlatıldı (aralık: %v, platform: %s)", interval, platform)
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

	// Data Directory kontrolü
	dataDir := req.DataDirectory
	if dataDir == "" {
		// Eğer data directory verilmemişse, konfigürasyon dosyasından bulmaya çalış
		configFile, err := postgres.FindPostgresConfigFile()
		if err == nil {
			content, err := os.ReadFile(configFile)
			if err == nil {
				lines := strings.Split(string(content), "\n")
				for _, line := range lines {
					if strings.Contains(line, "data_directory") {
						parts := strings.Split(line, "'")
						if len(parts) >= 2 {
							dataDir = strings.TrimSpace(parts[1])
							break
						}
					}
				}
			}
		}

		// Yine bulunamadıysa varsayılan değeri kullan
		if dataDir == "" {
			dataDir = "/var/lib/postgresql/data"
			log.Printf("Data directory bulunamadı, varsayılan değer kullanılıyor: %s", dataDir)
		}
	}

	// Promotion işlemi için trigger dosyası yolunu belirle
	triggerFilePath := filepath.Join(dataDir, "promote.trigger")
	standbySignalPath := filepath.Join(dataDir, "standby.signal")
	recoverySignalPath := filepath.Join(dataDir, "recovery.signal")

	// Standby sinyali dosyasının varlığını kontrol et
	isStandby := false
	if _, err := os.Stat(standbySignalPath); err == nil {
		isStandby = true
	} else if _, err := os.Stat(recoverySignalPath); err == nil {
		isStandby = true
	}

	if !isStandby {
		errMsg := "Bu node zaten master/primary durumunda, promotion gerekmez"
		log.Printf("UYARI: %s", errMsg)
		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       1, // FAILED - değer proto dosyasından alınmalı
			ErrorMessage: errMsg,
			Result:       "Node zaten primary/master durumunda",
		}, nil
	}

	// Trigger dosyasını oluştur
	log.Printf("Promotion trigger dosyası oluşturuluyor: %s", triggerFilePath)
	file, err := os.Create(triggerFilePath)
	if err != nil {
		errMsg := fmt.Sprintf("Promotion trigger dosyası oluşturulamadı: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       1, // FAILED - değer proto dosyasından alınmalı
			ErrorMessage: errMsg,
		}, nil
	}
	file.Close()

	// PostgreSQL'in dosyayı farketmesi için biraz bekle
	log.Println("PostgreSQL'in promotion trigger dosyasını farketmesi bekleniyor...")
	time.Sleep(5 * time.Second)

	// Promocyonun gerçekleşip gerçekleşmediğini kontrol et
	checkCount := 0
	maxChecks := 12 // 60 saniye toplam (5s * 12)
	promoted := false

	for checkCount < maxChecks {
		// Standby sinyal dosyası hala var mı kontrol et
		_, standbyExists := os.Stat(standbySignalPath)
		_, recoveryExists := os.Stat(recoverySignalPath)

		if standbyExists != nil && recoveryExists != nil {
			// Standby sinyalleri kaldırılmış, promotion başarılı
			promoted = true
			break
		}

		log.Printf("Promotion hala tamamlanmadı, 5 saniye daha bekleniyor... (%d/%d)", checkCount+1, maxChecks)
		time.Sleep(5 * time.Second)
		checkCount++
	}

	if !promoted {
		errMsg := "PostgreSQL promotion zaman aşımına uğradı"
		log.Printf("HATA: %s", errMsg)
		return &pb.PostgresPromoteMasterResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_FAILED, // FAILED - değer proto dosyasından alındı
			ErrorMessage: errMsg,
		}, nil
	}

	// Promotion başarılı
	log.Printf("PostgreSQL node başarıyla master'a yükseltildi")
	return &pb.PostgresPromoteMasterResponse{
		JobId:  req.JobId,
		Status: pb.JobStatus_JOB_STATUS_COMPLETED, // COMPLETED - değer proto dosyasından alındı
		Result: "PostgreSQL node başarıyla master'a yükseltildi",
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

	// Detaylı sorgu incelemesi - sorgu formatı analizi
	log.Printf("Gelen ham sorgu string (ilk 50 karakter): %q", req.Query[:min(len(req.Query), 50)])
	log.Printf("Sorgu uzunluğu: %d bytes", len(req.Query))
	// İlk 10 byte'ı hex formatında logla
	if len(req.Query) > 0 {
		firstBytes := []byte(req.Query[:min(len(req.Query), 20)])
		log.Printf("İlk 20 karakter hex: %x", firstBytes)
	}

	// Sorgu protokol kontrol - server'dan gelen protokol formatını kontrol et
	if strings.HasPrefix(req.Query, "MONGODB_EXPLAIN||") {
		log.Printf("Server protokol formatı tespit edildi: MONGODB_EXPLAIN|| prefixli sorgu")
		// Prefix'i kaldır ve asıl sorguyu al
		req.Query = strings.TrimPrefix(req.Query, "MONGODB_EXPLAIN||")
		log.Printf("Prefix kaldırıldı, yeni sorgu (ilk 50 karakter): %q", req.Query[:min(len(req.Query), 50)])
	} else if strings.HasPrefix(req.Query, "MONGO_EXPLAIN|") {
		log.Printf("Server protokol formatı tespit edildi: MONGO_EXPLAIN| prefixli sorgu")
		// Protokol formatını parse et
		parts := strings.Split(req.Query, "|")
		if len(parts) >= 3 {
			// Format: MONGO_EXPLAIN|<database>|<query_json>
			// Database bilgisini güncelle
			newDatabase := parts[1]
			if newDatabase != "" && req.Database == "" {
				req.Database = newDatabase
				log.Printf("Protokolden veritabanı alındı: %s", req.Database)
			}
			// Sorguyu güncelle
			req.Query = parts[2]
		} else if len(parts) == 2 {
			// Format: MONGO_EXPLAIN|<query_json>
			req.Query = parts[1]
		}
		log.Printf("Prefix kaldırıldı, yeni sorgu (ilk 50 karakter): %q", req.Query[:min(len(req.Query), 50)])
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
			if wrapper.Database != "" && req.Database == "" {
				req.Database = wrapper.Database
				log.Printf("JSON içinden veritabanı alındı: %s", req.Database)
			}

			// Query içeriğini asıl sorgu olarak kullan
			if len(wrapper.Query) > 0 {
				req.Query = string(wrapper.Query)
				log.Printf("JSON içinden sorgu çıkarıldı (ilk 50 karakter): %q", req.Query[:min(len(req.Query), 50)])

				// JSON string olabilir, escape karakterlerini temizle
				if strings.HasPrefix(req.Query, "\"") && strings.HasSuffix(req.Query, "\"") {
					// String içindeki tırnak işaretlerini kaldırma
					unquotedStr, err := strconv.Unquote(req.Query)
					if err == nil {
						req.Query = unquotedStr
						log.Printf("JSON string içindeki escape karakterleri temizlendi (ilk 50 karakter): %q",
							req.Query[:min(len(req.Query), 50)])
					} else {
						log.Printf("String unquote hatası: %v", err)
					}
				}
			}
		} else {
			log.Printf("JSON wrapper parse edilemedi: %v", err)
		}
	}

	// MongoDB kolektörünü oluştur
	mongoCollector := mongo.NewMongoCollector(r.cfg)

	// MongoDB bağlantısı oluştur
	client, err := mongoCollector.GetClient()
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB bağlantısı açılamadı: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}
	defer client.Disconnect(context.Background())

	// Sorgu tipini tespit etme
	var explainResult bson.M
	queryString := req.Query

	// JSON formatını analiz et - başta ve sonda olabilecek boşlukları temizle
	queryString = strings.TrimSpace(queryString)

	// Log JSON string formatını debug etmek için
	log.Printf("MongoDB sorgu JSON string: %s", queryString)

	// BSON/JSON parse etme
	// Önce MongoDB Extended JSON formatını normal haline getirip decode edelim
	// Tüm yeni satır ve boşlukları temizleyelim
	queryString = strings.ReplaceAll(queryString, "\n", " ")
	queryString = strings.ReplaceAll(queryString, "\r", "")
	queryString = strings.ReplaceAll(queryString, "\t", " ")

	// Gereksiz boşlukları temizleyelim
	// Birden fazla boşluğu tek boşluğa indirgeme
	for strings.Contains(queryString, "  ") {
		queryString = strings.ReplaceAll(queryString, "  ", " ")
	}

	log.Printf("Temizlenmiş sorgu: %s", queryString)

	// Doğrudan MongoDB sürücüsünün BSON parser'ını kullanalım
	var queryDoc bson.M
	err = bson.UnmarshalExtJSON([]byte(queryString), true, &queryDoc)

	if err != nil {
		log.Printf("MongoDB Extended JSON parse hatası: %v", err)
		log.Printf("Parse hata detayı: %T - %v", err, err)

		// JSON parse hatasından önce, sorgunun geçerli JSON olup olmadığını kontrol et
		if !isValidJSON(queryString) {
			log.Printf("Sorgu geçerli bir JSON değil, server protokol formatı tekrar kontrol ediliyor...")

			// JSON değilse ve server protokol formatı olabilir, başka bir ayrıştırma dene
			if strings.Contains(queryString, "MONGODB_EXPLAIN") {
				log.Printf("Sorgu içinde 'MONGODB_EXPLAIN' ifadesi bulundu, protokol formatı olabilir")
				parts := strings.Split(queryString, "||")
				if len(parts) > 1 {
					queryString = parts[1]
					log.Printf("Protokol ayrıştırması sonrası sorgu: %s", queryString)
				}
			}
		}

		// BSON parser başarısız olursa, standart JSON parser'ı deneyelim
		err = json.Unmarshal([]byte(queryString), &queryDoc)

		if err != nil {
			log.Printf("Standart JSON parse de başarısız: %v", err)

			// Sorgu geldiği gibi MongoDB'ye göndermeyi deneyelim
			// Önce find ve aggregate komutlarını kontrol edelim
			if strings.Contains(queryString, "\"find\"") {
				// Find sorgusu olabilir, basit bir find sorgusu oluşturalım
				collName := ""
				if findRegex := regexp.MustCompile(`"find"\s*:\s*"([^"]+)"`); findRegex.MatchString(queryString) {
					matches := findRegex.FindStringSubmatch(queryString)
					if len(matches) > 1 {
						collName = matches[1]
						log.Printf("Find collection adı: %s", collName)

						// Basit bir find sorgusu oluştur
						queryDoc = bson.M{
							"find":   collName,
							"filter": bson.M{},
						}

						// Filter varsa onu da almaya çalışalım
						if filterRegex := regexp.MustCompile(`"filter"\s*:\s*({[^}]+})`); filterRegex.MatchString(queryString) {
							matches := filterRegex.FindStringSubmatch(queryString)
							if len(matches) > 1 {
								filterStr := matches[1]
								var filterDoc bson.M
								if err := json.Unmarshal([]byte(filterStr), &filterDoc); err == nil {
									queryDoc["filter"] = filterDoc
								}
							}
						}

						// Hata sıfırlayalım
						err = nil
					}
				}
			} else if strings.Contains(queryString, "\"aggregate\"") {
				// Aggregate sorgusu olabilir
				collName := ""
				if aggRegex := regexp.MustCompile(`"aggregate"\s*:\s*"([^"]+)"`); aggRegex.MatchString(queryString) {
					matches := aggRegex.FindStringSubmatch(queryString)
					if len(matches) > 1 {
						collName = matches[1]
						log.Printf("Aggregate collection adı: %s", collName)

						// Basit bir aggregate sorgusu oluştur
						queryDoc = bson.M{
							"aggregate": collName,
							"pipeline":  bson.A{},
							"cursor":    bson.M{},
						}

						// Pipeline varsa onu da almaya çalışalım
						if pipelineRegex := regexp.MustCompile(`"pipeline"\s*:\s*(\[[^\]]+\])`); pipelineRegex.MatchString(queryString) {
							matches := pipelineRegex.FindStringSubmatch(queryString)
							if len(matches) > 1 {
								pipelineStr := matches[1]
								var pipelineArray []interface{}
								if err := json.Unmarshal([]byte(pipelineStr), &pipelineArray); err == nil {
									queryDoc["pipeline"] = pipelineArray
								}
							}
						}

						// Hata sıfırlayalım
						err = nil
					}
				}
			}

			// Yine başarısız olursa direkt hata dön
			if err != nil {
				errMsg := fmt.Sprintf("Sorgu JSON formatında değil: %v", err)
				log.Printf("HATA: %s", errMsg)
				return &pb.ExplainQueryResponse{
					Status:       "error",
					ErrorMessage: errMsg,
					Plan:         "",
				}, nil
			}
		}
	}

	// Sorgu tipini belirle ve direct yöntem kullan
	log.Printf("Sorgu dokümanı: %+v", queryDoc)

	// Veritabanı bağlantısı
	database := client.Database(req.Database)

	// Açıklanacak komutun çekirdeğini oluştur
	// Gereksiz MongoDB bağlantı meta verilerini kaldır
	coreCmdDoc := bson.M{}

	// Şimdi önemli alanları kopyala
	for k, v := range queryDoc {
		// $clusterTime, $db, lsid gibi meta alanları atla
		if !strings.HasPrefix(k, "$") && k != "lsid" {
			coreCmdDoc[k] = v
		}
	}

	log.Printf("Temizlenmiş komut: %+v", coreCmdDoc)

	// Sorgu tipine göre işlem yap
	if collName, hasAggregate := coreCmdDoc["aggregate"]; hasAggregate {
		// Aggregate sorgusu
		log.Printf("Aggregate sorgusu tespit edildi")

		var pipeline bson.A
		if pipelineInterface, hasPipeline := coreCmdDoc["pipeline"]; hasPipeline {
			if pipelineArray, ok := pipelineInterface.(bson.A); ok {
				pipeline = pipelineArray
			} else if pipelineArray, ok := pipelineInterface.([]interface{}); ok {
				pipeline = bson.A(pipelineArray)
			} else {
				// Pipeline'ı BSON'a çevirmeyi dene
				pipelineBson, err := bson.Marshal(pipelineInterface)
				if err != nil {
					errMsg := fmt.Sprintf("Pipeline BSON'a dönüştürülemedi: %v", err)
					log.Printf("HATA: %s", errMsg)
					return &pb.ExplainQueryResponse{
						Status:       "error",
						ErrorMessage: errMsg,
					}, nil
				}

				err = bson.Unmarshal(pipelineBson, &pipeline)
				if err != nil {
					errMsg := fmt.Sprintf("Pipeline array'e dönüştürülemedi: %v", err)
					log.Printf("HATA: %s", errMsg)
					return &pb.ExplainQueryResponse{
						Status:       "error",
						ErrorMessage: errMsg,
					}, nil
				}
			}
		}

		// Explain komutunu oluştur
		cmdDoc := bson.D{
			{Key: "aggregate", Value: collName},
			{Key: "pipeline", Value: pipeline},
			{Key: "cursor", Value: bson.D{}},
		}

		// Diğer parametreleri (limit, skip vb.) ekle
		for k, v := range coreCmdDoc {
			if k != "aggregate" && k != "pipeline" && k != "cursor" {
				cmdDoc = append(cmdDoc, bson.E{Key: k, Value: v})
			}
		}

		explainOpts := bson.D{
			{Key: "explain", Value: cmdDoc},
			{Key: "verbosity", Value: "allPlansExecution"},
		}

		log.Printf("MongoDB explain komutu: %+v", explainOpts)
		err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
	} else if collName, hasFind := coreCmdDoc["find"]; hasFind {
		// Find sorgusu
		log.Printf("Find sorgusu tespit edildi")

		// Find komutunu oluştur, tüm ek parametreleri de ekle
		cmdDoc := bson.D{{Key: "find", Value: collName}}

		// Diğer parametreleri ekle (filter, sort, limit, skip)
		for k, v := range coreCmdDoc {
			if k != "find" {
				cmdDoc = append(cmdDoc, bson.E{Key: k, Value: v})
			}
		}

		// Explain komutu
		explainOpts := bson.D{
			{Key: "explain", Value: cmdDoc},
			{Key: "verbosity", Value: "allPlansExecution"},
		}

		log.Printf("MongoDB explain komutu: %+v", explainOpts)
		err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
	} else if explainCmd, hasExplain := coreCmdDoc["explain"]; hasExplain {
		// İç içe explain komutu var, bunu düzelt
		log.Printf("İç içe explain sorgusu tespit edildi: %v", explainCmd)

		// İç explain'i parse et
		var innerExplain bson.M
		if innerMap, ok := explainCmd.(map[string]interface{}); ok {
			innerExplain = innerMap
			// Doğru explain komutunu oluştur
			var explainCmd bson.D

			// find ve filter'ı çıkar
			if findVal, hasFind := innerExplain["find"]; hasFind {
				explainCmd = append(explainCmd, bson.E{Key: "find", Value: findVal})

				// Diğer özellikleri ekle (filter, sort, limit, skip vb.)
				for k, v := range innerExplain {
					if k != "find" {
						explainCmd = append(explainCmd, bson.E{Key: k, Value: v})
					}
				}

				// Explain komutu
				explainOpts := bson.D{
					{Key: "explain", Value: explainCmd},
					{Key: "verbosity", Value: "allPlansExecution"},
				}

				log.Printf("Düzeltilmiş MongoDB explain komutu: %+v", explainOpts)
				err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
			} else if aggVal, hasAgg := innerExplain["aggregate"]; hasAgg {
				// Aggregate komutunu işle
				explainCmd = append(explainCmd, bson.E{Key: "aggregate", Value: aggVal})

				// Diğer özellikleri ekle (pipeline, cursor vb.)
				for k, v := range innerExplain {
					if k != "aggregate" {
						explainCmd = append(explainCmd, bson.E{Key: k, Value: v})
					}
				}

				// Explain komutu
				explainOpts := bson.D{
					{Key: "explain", Value: explainCmd},
					{Key: "verbosity", Value: "allPlansExecution"},
				}

				log.Printf("Düzeltilmiş MongoDB aggregate explain komutu: %+v", explainOpts)
				err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
			} else {
				log.Printf("İç explain içinde find veya aggregate komutu bulunamadı")
				// Olduğu gibi gönder son çare olarak
				explainOpts := bson.D{
					{Key: "explain", Value: innerExplain},
					{Key: "verbosity", Value: "allPlansExecution"},
				}
				err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
			}
		} else {
			log.Printf("İç explain map formatında değil, doğrudan gönderiliyor")
			// Diğer tip sorgular - temizlenmiş sorguyu doğrudan aç
			explainOpts := bson.D{
				{Key: "explain", Value: coreCmdDoc},
				{Key: "verbosity", Value: "allPlansExecution"},
			}

			log.Printf("MongoDB explain komutu (diğer tip): %+v", explainOpts)
			err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
		}
	} else {
		// Diğer tip sorgular - temizlenmiş sorguyu doğrudan aç
		explainOpts := bson.D{
			{Key: "explain", Value: coreCmdDoc},
			{Key: "verbosity", Value: "allPlansExecution"},
		}

		log.Printf("MongoDB explain komutu (diğer tip): %+v", explainOpts)
		err = database.RunCommand(ctx, explainOpts).Decode(&explainResult)
	}

	if err != nil {
		// Son bir şans - doğrudan sorguyu çalıştırmayı dene
		log.Printf("Direkt explain başarısız oldu, farklı yaklaşım deneniyor: %v", err)

		// MongoDB'nin native bson formatına dönüştürelim
		cmdBytes, err := bson.Marshal(coreCmdDoc)
		if err != nil {
			log.Printf("BSON marshal hatası: %v", err)
		} else {
			var cmdNative bson.D
			if err := bson.Unmarshal(cmdBytes, &cmdNative); err != nil {
				log.Printf("BSON unmarshal hatası: %v", err)
			} else {
				// Native formatla explain dene
				explainCmd := bson.D{
					{Key: "explain", Value: cmdNative},
					{Key: "verbosity", Value: "allPlansExecution"},
				}

				log.Printf("Son şans MongoDB explain (native BSON): %+v", explainCmd)
				err = database.RunCommand(ctx, explainCmd).Decode(&explainResult)

				if err != nil {
					// Admin veritabanında dene
					adminDB := client.Database("admin")
					err = adminDB.RunCommand(ctx, explainCmd).Decode(&explainResult)

					if err != nil {
						errMsg := fmt.Sprintf("MongoDB sorgu planı alınamadı: %v", err)
						log.Printf("HATA: %s", errMsg)
						return &pb.ExplainQueryResponse{
							Status:       "error",
							ErrorMessage: errMsg,
							Plan:         "",
						}, nil
					}
				}
			}
		}
	}

	// Sonucu JSON'a dönüştür
	resultBytes, err := bson.MarshalExtJSON(explainResult, true, true)
	if err != nil {
		errMsg := fmt.Sprintf("MongoDB sorgu planı JSON'a dönüştürülemedi: %v", err)
		log.Printf("HATA: %s", errMsg)
		return &pb.ExplainQueryResponse{
			Status:       "error",
			ErrorMessage: errMsg,
		}, nil
	}

	// Debug için JSON çıktısını logla
	log.Printf("MongoDB explain JSON sonucu: %s", string(resultBytes))

	// Okunabilir bir format oluştur
	var planText string
	var resultMap map[string]interface{}

	if err := json.Unmarshal(resultBytes, &resultMap); err != nil {
		log.Printf("UYARI: JSON parse edilemedi, ham sonuç kullanılacak: %v", err)
		planText = string(resultBytes)
	} else {
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
	}

	log.Printf("MongoDB sorgu planı başarıyla alındı")
	return &pb.ExplainQueryResponse{
		Status: "success",
		Plan:   planText,
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
