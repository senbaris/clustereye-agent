package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"               // PostgreSQL sürücüsü
	_ "github.com/microsoft/go-mssqldb" // MSSQL sürücüsü
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/reporter"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// PostgreSQL test sonucunu ve bilgilerini JSON olarak hazırla
type PostgresInfo struct {
	Status   string `json:"status"`
	User     string `json:"user"`
	Password string `json:"password"`
	Cluster  string `json:"cluster"`
}

// AgentService implements the agent service
type AgentService struct {
	pb.UnimplementedAgentServiceServer
	reporter *reporter.Reporter
}

func setupLogging() (*os.File, error) {
	var logPath string

	// İşletim sistemine göre log dosyası yolunu belirle
	if runtime.GOOS == "windows" {
		// Windows için C:\Clustereye klasörünü oluştur
		logPath = filepath.Join("C:", "Clustereye")
		err := os.MkdirAll(logPath, 0755)
		if err != nil {
			// Ana klasör oluşturulamazsa, geçici klasöre yazalım
			logPath = os.TempDir()
		}
		logPath = filepath.Join(logPath, "clustereye-agent.log")
	} else {
		// Linux/macOS için
		logPath = "/var/log/clustereye-agent.log"

		// Eğer dizine yazma hakkımız yoksa home klasörüne yazalım
		if _, err := os.Stat("/var/log"); os.IsPermission(err) || os.IsNotExist(err) {
			homeDir, err := os.UserHomeDir()
			if err == nil {
				logPath = filepath.Join(homeDir, ".clustereye", "agent.log")
				// Klasörü oluştur
				os.MkdirAll(filepath.Dir(logPath), 0755)
			} else {
				// Son çare olarak geçici dizini kullan
				logPath = filepath.Join(os.TempDir(), "clustereye-agent.log")
			}
		}
	}

	// Log dosyasını aç (mevcut değilse oluştur, mevcutsa ekle)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Log kütüphanesine dosyaya yazmayı söyle
	multiWriter := struct{ io.Writer }{io.MultiWriter(os.Stdout, f)}
	log.SetOutput(multiWriter)

	// Zaman damgası ve dosya bilgisi içeren log formatı
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Printf("Logger başlatıldı. Log dosyası: %s", logPath)

	return f, nil
}

func main() {
	// Log dosyasını kur
	logFile, err := setupLogging()
	if err != nil {
		log.Printf("UYARI: Log dosyası açılamadı: %v", err)
	} else {
		defer logFile.Close()
		log.Printf("ClusterEye Agent başlatılıyor, versiyon 1.0.23")
	}

	// Konfigürasyonu yükle
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Fatalf("Konfigürasyon yüklenemedi: %v", err)
	}

	// Global Reporter instance kullan - multiple instance leak'ini önler
	rptr := reporter.GetGlobalReporter(cfg)

	// AgentService yapısını oluştur
	service := &AgentService{
		reporter: rptr,
	}

	// GRPC Bağlantısı kur
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB - Increased for large deadlock XML
			grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB - Increased for large deadlock XML
		),
	}

	log.Printf("gRPC sunucusuna bağlanılıyor: %s", cfg.GRPC.ServerAddress)

	conn, err := grpc.Dial(cfg.GRPC.ServerAddress, opts...)
	if err != nil {
		log.Fatalf("gRPC sunucusuna bağlanılamadı: %v", err)
	}
	defer conn.Close()

	// gRPC client oluştur
	client := pb.NewAgentServiceClient(conn)

	// Stream bağlantısını başlat
	stream, err := client.Connect(context.Background())
	if err != nil {
		log.Fatalf("Connect stream oluşturulamadı: %v", err)
	}

	// Sistem bilgilerini al
	hostname, _ := os.Hostname()
	ip := getLocalIP()

	// Test all database connections and select the platform
	var platform string
	var testResult string
	var auth bool

	// Test PostgreSQL
	pgResult := testDBConnection(cfg)
	if strings.HasPrefix(pgResult, "success") {
		platform = "postgres"
		testResult = pgResult
		auth = cfg.PostgreSQL.Auth
		log.Printf("PostgreSQL bağlantısı başarılı, bu platform ile devam ediliyor")
	} else {
		log.Printf("PostgreSQL bağlantısı başarısız, MongoDB deneniyor")

		// Test MongoDB
		mongoResult := "fail:not_tested"
		mongoURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/?authSource=admin",
			cfg.Mongo.User, cfg.Mongo.Pass, cfg.Mongo.Host, cfg.Mongo.Port)

		if !cfg.Mongo.Auth {
			mongoURI = fmt.Sprintf("mongodb://%s:%s", cfg.Mongo.Host, cfg.Mongo.Port)
		}

		log.Printf("MongoDB bağlantısı test ediliyor: %s", mongoURI)
		// Burada basit bir ağ bağlantısı kontrolü yap
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", cfg.Mongo.Host, cfg.Mongo.Port), 2*time.Second)
		if err == nil {
			conn.Close()
			mongoResult = "success"
			platform = "mongo"
			testResult = mongoResult
			auth = cfg.Mongo.Auth
			log.Printf("MongoDB bağlantısı başarılı, bu platform ile devam ediliyor")
		} else {
			log.Printf("MongoDB bağlantısı başarısız, MSSQL deneniyor")

			// Test MSSQL
			mssqlResult := testMSSQLConnection(cfg)
			if strings.HasPrefix(mssqlResult, "success") {
				platform = "mssql"
				testResult = mssqlResult
				auth = cfg.MSSQL.Auth
				log.Printf("MSSQL bağlantısı başarılı, bu platform ile devam ediliyor")
			} else {
				log.Printf("MSSQL bağlantısı da başarısız, varsayılan olarak PostgreSQL seçiliyor")
				platform = "postgres" // Varsayılan olarak PostgreSQL kullan
				testResult = pgResult
				auth = cfg.PostgreSQL.Auth
			}
		}
	}

	// Agent bilgilerini gönder
	agentInfo := &pb.AgentMessage{
		Payload: &pb.AgentMessage_AgentInfo{
			AgentInfo: &pb.AgentInfo{
				Key:          cfg.Key,
				AgentId:      "agent_" + hostname,
				Hostname:     hostname,
				Ip:           ip,
				Platform:     platform,
				Auth:         auth,
				Test:         testResult,
				PostgresUser: cfg.PostgreSQL.User,
				PostgresPass: cfg.PostgreSQL.Pass,
			},
		},
	}

	if err := stream.Send(agentInfo); err != nil {
		log.Fatalf("Agent bilgisi gönderilemedi: %v", err)
	}

	log.Printf("ClusterEye sunucusuna bağlandı: %s", cfg.GRPC.ServerAddress)

	// Komut alma işlemi
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				log.Fatalf("Cloud API bağlantısı kapandı: %v", err)
			}

			if query := in.GetQuery(); query != nil {
				log.Printf("Yeni sorgu geldi: %s", query.Command)

				// MongoDB log analizi için özel işleme
				if strings.HasPrefix(query.Command, "analyze_mongo_log") {
					log.Printf("MongoDB log analizi talebi alındı: %s", query.Command)
					log.Printf("====== MONGO LOG ANALİZİ BAŞLIYOR ======")

					// Sorgudan parametreleri çıkar (log_file_path|slow_query_threshold_ms)
					parts := strings.Split(query.Command, "|")
					if len(parts) < 2 {
						log.Printf("Geçersiz sorgu formatı: %s", query.Command)

						// Hata sonucunu gönder
						errorResult := map[string]interface{}{
							"status":  "error",
							"message": "Geçersiz sorgu formatı. Doğru format: analyze_mongo_log|/path/to/log|threshold_ms",
						}

						resultStruct, _ := structpb.NewStruct(errorResult)
						anyResult, _ := anypb.New(resultStruct)

						result := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := stream.Send(result); err != nil {
							log.Printf("Hata sonucu gönderilemedi: %v", err)
						}
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

					// MongoLogAnalyzeRequest oluştur
					req := &pb.MongoLogAnalyzeRequest{
						LogFilePath:          logFilePath,
						SlowQueryThresholdMs: slowQueryThresholdMs,
						AgentId:              "agent_" + hostname,
					}

					// Reporter'a analiz için ilgili dosyayı gönder
					resp, err := service.reporter.AnalyzeMongoLog(req)
					if err != nil {
						log.Printf("MongoDB log analizi başarısız: %v", err)

						// Hata sonucunu gönder
						errorResult := map[string]interface{}{
							"status":  "error",
							"message": fmt.Sprintf("MongoDB log analizi başarısız: %v", err),
						}

						resultStruct, _ := structpb.NewStruct(errorResult)
						anyResult, _ := anypb.New(resultStruct)

						result := &pb.AgentMessage{
							Payload: &pb.AgentMessage_QueryResult{
								QueryResult: &pb.QueryResult{
									QueryId: query.QueryId,
									Result:  anyResult,
								},
							},
						}

						if err := stream.Send(result); err != nil {
							log.Printf("Hata sonucu gönderilemedi: %v", err)
						}
						continue
					}

					// MongoLogAnalyzeResponse'u map yapısına dönüştür
					logEntriesData := make([]interface{}, 0, len(resp.LogEntries))
					for _, entry := range resp.LogEntries {
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

					// Başarılı sonucu structpb'ye dönüştür
					analysisResult := map[string]interface{}{
						"status":      "success",
						"log_entries": logEntriesData,
						"count":       len(resp.LogEntries),
					}

					resultStruct, err := structpb.NewStruct(analysisResult)
					if err != nil {
						log.Printf("Struct'a dönüştürme hatası: %v", err)
						continue
					}

					anyResult, err := anypb.New(resultStruct)
					if err != nil {
						log.Printf("Any tipine dönüştürülemedi: %v", err)
						continue
					}

					// Sonucu gönder
					result := &pb.AgentMessage{
						Payload: &pb.AgentMessage_QueryResult{
							QueryResult: &pb.QueryResult{
								QueryId: query.QueryId,
								Result:  anyResult,
							},
						},
					}

					if err := stream.Send(result); err != nil {
						log.Printf("MongoDB log analizi sonucu gönderilemedi: %v", err)
					} else {
						log.Printf("MongoDB log analizi sonucu başarıyla gönderildi (ID: %s, %d log girişi)",
							query.QueryId, len(resp.LogEntries))
					}
					continue
				}

				// Diğer sorgular için normal işleme
				queryResult := processQuery(query.Command)

				// Sorgu sonucunu hazırla
				resultMap, err := structpb.NewStruct(queryResult)
				if err != nil {
					log.Printf("Sonuç haritası oluşturulamadı: %v", err)
					continue
				}

				// structpb'yi Any'e dönüştür
				anyResult, err := anypb.New(resultMap)
				if err != nil {
					log.Printf("Any tipine dönüştürülemedi: %v", err)
					continue
				}

				// Sorgu sonucunu gönder
				result := &pb.AgentMessage{
					Payload: &pb.AgentMessage_QueryResult{
						QueryResult: &pb.QueryResult{
							QueryId: query.QueryId,
							Result:  anyResult,
						},
					},
				}

				if err := stream.Send(result); err != nil {
					log.Fatalf("Sorgu cevabı gönderilemedi: %v", err)
				}
			}
		}
	}()

	// Agent bağlantısını canlı tutmak için basit bir döngü
	for {
		time.Sleep(time.Minute)
	}
}

// processQuery, gelen sorguyu işler ve sonucu hesaplar
func processQuery(command string) map[string]interface{} {
	// Bu basitçe bir test yanıtı, gerçek uygulamada burada komut çalıştırılabilir
	return map[string]interface{}{
		"status":  "success",
		"command": command,
		"result":  "Command executed successfully",
		"time":    time.Now().String(),
	}
}

// testDBConnection, konfigürasyondaki veritabanı bilgileriyle test bağlantısı yapar
func testDBConnection(cfg *config.AgentConfig) string {
	// PostgreSQL bağlantı bilgilerini yapılandırma dosyasından al
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.PostgreSQL.Host,
		cfg.PostgreSQL.Port,
		cfg.PostgreSQL.User,
		cfg.PostgreSQL.Pass,
		"postgres", // Varsayılan veritabanı adı
	)

	// Veritabanına bağlan
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		errMsg := fmt.Sprintf("fail:connection_error:%v", err)
		log.Printf("PostgreSQL bağlantısı açılamadı: %v", err)
		return errMsg
	}
	defer db.Close()

	// Bağlantıyı test et
	err = db.Ping()
	if err != nil {
		errMsg := fmt.Sprintf("fail:ping_error:%v", err)
		log.Printf("PostgreSQL bağlantı testi başarısız: %v", err)
		return errMsg
	}

	// Basit bir sorgu çalıştır
	var version string
	err = db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		errMsg := fmt.Sprintf("fail:query_error:%v", err)
		log.Printf("PostgreSQL sorgusu başarısız: %v", err)
		return errMsg
	}

	log.Printf("PostgreSQL bağlantısı başarılı. Versiyon: %s", version)
	return "success"
}

// testMSSQLConnection tests the MSSQL connection and returns a status string
func testMSSQLConnection(cfg *config.AgentConfig) string {
	var connStr string

	// Windows auth veya SQL auth
	if cfg.MSSQL.WindowsAuth {
		if cfg.MSSQL.Instance != "" {
			connStr = fmt.Sprintf("server=%s\\%s;database=%s;trusted_connection=yes",
				cfg.MSSQL.Host, cfg.MSSQL.Instance, cfg.MSSQL.Database)
		} else {
			connStr = fmt.Sprintf("server=%s,%s;database=%s;trusted_connection=yes",
				cfg.MSSQL.Host, cfg.MSSQL.Port, cfg.MSSQL.Database)
		}
	} else {
		if cfg.MSSQL.Instance != "" {
			connStr = fmt.Sprintf("server=%s\\%s;user id=%s;password=%s;database=%s",
				cfg.MSSQL.Host, cfg.MSSQL.Instance, cfg.MSSQL.User, cfg.MSSQL.Pass, cfg.MSSQL.Database)
		} else {
			connStr = fmt.Sprintf("server=%s,%s;user id=%s;password=%s;database=%s",
				cfg.MSSQL.Host, cfg.MSSQL.Port, cfg.MSSQL.User, cfg.MSSQL.Pass, cfg.MSSQL.Database)
		}
	}

	// Add TrustServerCertificate if needed
	if cfg.MSSQL.TrustCert {
		connStr += ";trustservercertificate=true"
	}

	connStr += ";connection timeout=5"

	// Bağlantıyı açmayı dene
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		errMsg := fmt.Sprintf("fail:connection_error:%v", err)
		log.Printf("MSSQL bağlantısı açılamadı: %v", err)
		return errMsg
	}
	defer db.Close()

	// Ping ile bağlantıyı test et
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("fail:ping_error:%v", err)
		log.Printf("MSSQL bağlantı testi başarısız: %v", err)
		return errMsg
	}

	// Version bilgisini al
	var version string
	err = db.QueryRow("SELECT @@VERSION").Scan(&version)
	if err != nil {
		errMsg := fmt.Sprintf("fail:query_error:%v", err)
		log.Printf("MSSQL sorgusu başarısız: %v", err)
		return errMsg
	}

	log.Printf("MSSQL bağlantısı başarılı. Versiyon: %s", version)
	return "success"
}

// getLocalIP, yerel IP'yi almak için yardımcı fonksiyon
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func getPlatformInfo() string {
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

// AnalyzeMongoLog handles MongoDB log analysis requests
func (s *AgentService) AnalyzeMongoLog(req *pb.MongoLogAnalyzeRequest) (*pb.MongoLogAnalyzeResponse, error) {
	log.Printf("AnalyzeMongoLog servis metodu çağrıldı: Dosya=%s, Eşik=%d ms",
		req.LogFilePath, req.SlowQueryThresholdMs)
	return s.reporter.AnalyzeMongoLog(req)
}
