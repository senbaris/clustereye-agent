package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq" // PostgreSQL sürücüsü
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

func main() {
	// Konfigürasyonu yükle
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Fatalf("Konfigürasyon yüklenemedi: %v", err)
	}

	// Reporter oluştur
	rptr := reporter.NewReporter(cfg)

	// AgentService yapısını oluştur
	service := &AgentService{
		reporter: rptr,
	}

	// GRPC Bağlantısı kur
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(32*1024*1024), // 32MB
			grpc.MaxCallSendMsgSize(32*1024*1024), // 32MB
		),
	}

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

	// Kimlik doğrulama ayarlarını al
	postgreAuth := cfg.PostgreSQL.Auth

	// PostgreSQL bağlantı testi yap
	testResult := testDBConnection(cfg)

	// Agent bilgilerini gönder
	agentInfo := &pb.AgentMessage{
		Payload: &pb.AgentMessage_AgentInfo{
			AgentInfo: &pb.AgentInfo{
				Key:          cfg.Key,
				AgentId:      "agent_" + hostname,
				Hostname:     hostname,
				Ip:           ip,
				Platform:     cfg.PostgreSQL.Cluster,
				Auth:         postgreAuth,
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
