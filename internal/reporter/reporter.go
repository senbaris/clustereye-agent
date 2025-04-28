package reporter

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/senbaris/clustereye-agent/internal/collector/mongo"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/alarm"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/pkg/utils"
)

// QueryProcessor, sorgu işleme mantığını temsil eder
type QueryProcessor struct {
	db *sql.DB
}

// NewQueryProcessor, yeni bir sorgu işleyici oluşturur
func NewQueryProcessor(db *sql.DB) *QueryProcessor {
	return &QueryProcessor{db: db}
}

// processQuery, gelen sorguyu işler ve sonucu döndürür
func (p *QueryProcessor) processQuery(command string, database string) map[string]interface{} {
	const (
		maxRows          = 100         // Maksimum satır sayısı
		maxValueLen      = 1000        // Maksimum değer uzunluğu
		maxTotalDataSize = 1024 * 1024 // Maksimum toplam veri boyutu (1 MB)
	)

	// Veritabanını seç
	if database != "" {
		_, err := p.db.Exec(fmt.Sprintf("SET search_path TO %s", database))
		if err != nil {
			return map[string]interface{}{
				"status":  "error",
				"message": fmt.Sprintf("Veritabanı seçilemedi: %v", err),
			}
		}
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

	// Sorguyu çalıştır
	rows, err := p.db.Query(command)
	if err != nil {
		return map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}
	}
	defer rows.Close()

	// Sütun isimlerini al
	columns, err := rows.Columns()
	if err != nil {
		return map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}
	}

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
		return map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}
	}

	// İşlem süresini hesapla
	duration := time.Since(startTime).Milliseconds()
	log.Printf("Sorgu işleme tamamlandı: %d satır, %d bayt, %d ms",
		rowCount, totalDataSize, duration)

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
	conn, err := grpc.Dial(
		r.cfg.GRPC.ServerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	r.grpcClient = conn

	// gRPC client oluştur
	client := pb.NewAgentServiceClient(conn)
	stream, err := client.Connect(context.Background())
	if err != nil {
		return err
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

	// Stream üzerinden agent bilgilerini gönder
	agentMessage := &pb.AgentMessage{
		Payload: &pb.AgentMessage_AgentInfo{
			AgentInfo: agentInfo,
		},
	}

	err := r.stream.Send(agentMessage)
	if err != nil {
		return fmt.Errorf("agent bilgileri gönderilemedi: %v", err)
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

	return nil
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
		processor = NewQueryProcessor(db)
	}

	// Mesaj işleme durumu
	isProcessingMetrics := false
	isProcessingQuery := false
	lastMessageType := "none"

	for {
		select {
		case <-r.stopCh:
			log.Println("Komut dinleme durduruldu")
			return
		default:
			// Önceki mesaj işleme tamamlandıysa durum bilgisini temizle
			if !isProcessingMetrics && !isProcessingQuery {
				lastMessageType = "none"
			}

			log.Printf("Sunucudan mesaj bekleniyor... (Son mesaj tipi: %s)", lastMessageType)
			in, err := r.stream.Recv()
			if err != nil {
				log.Printf("Cloud API bağlantısı kapandı: %v", err)
				time.Sleep(5 * time.Second)
				if err := r.reconnect(); err != nil {
					log.Printf("Yeniden bağlantı başarısız: %v", err)
					time.Sleep(10 * time.Second)
				}
				continue
			}

			// Gelen mesajın tipini logla
			messageType := "unknown"
			if query := in.GetQuery(); query != nil {
				messageType = "query"
			} else if metricsReq := in.GetMetricsRequest(); metricsReq != nil {
				messageType = "metrics_request"
			}
			log.Printf("Sunucudan mesaj alındı - Tip: %s (Son mesaj tipi: %s)", messageType, lastMessageType)

			if query := in.GetQuery(); query != nil {
				// Eğer şu anda metrik işleme devam ediyorsa, uyarı ver
				if isProcessingMetrics {
					log.Printf("UYARI: Metrik işleme devam ederken sorgu alındı")
				}

				isProcessingQuery = true
				lastMessageType = "query"
				log.Printf("Yeni sorgu geldi: %s (ID: %s)", trimString(query.Command, 100), query.QueryId)

				// Ping sorgusunu özel olarak işle
				if strings.ToLower(query.Command) == "ping" {
					log.Printf("Ping komutu algılandı, özel yanıt gönderiliyor")

					pingResult := map[string]interface{}{
						"status":  "success",
						"message": "pong",
					}

					resultStruct, _ := structpb.NewStruct(pingResult)
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
						log.Printf("Ping cevabı gönderilemedi: %v", err)
					} else {
						log.Printf("Ping cevabı başarıyla gönderildi")
					}

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

				// Normal PostgreSQL sorgusu işleme
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

				isProcessingQuery = false
				continue
			} else if metricsReq := in.GetMetricsRequest(); metricsReq != nil {
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

	// Yeni bağlantı kur
	if err := r.Connect(); err != nil {
		return err
	}

	// Agent kaydını tekrarla (test sonucu "reconnected" olarak gönder)
	// Mevcut platform bilgisini kullanarak agent kaydını yenile
	if err := r.AgentRegistration("reconnected", r.platform); err != nil {
		return err
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
