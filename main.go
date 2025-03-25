package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	_ "github.com/lib/pq" // PostgreSQL sürücüsü
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"

	"google.golang.org/grpc"
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

func main() {
	// Konfigürasyonu yükle
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Fatalf("Konfigürasyon yüklenemedi: %v", err)
	}

	// GRPC Bağlantısı kur
	conn, err := grpc.Dial(cfg.GRPC.ServerAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("gRPC sunucusuna bağlanılamadı: %v", err)
	}
	defer conn.Close()

	// gRPC client oluştur
	client := pb.NewAgentServiceClient(conn)
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

				// Sorguyu işle ve sonucu hesapla
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
