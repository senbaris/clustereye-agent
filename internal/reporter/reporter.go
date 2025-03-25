package reporter

import (
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/pkg/utils"
)

// Reporter toplanan verileri merkezi sunucuya raporlar
type Reporter struct {
	cfg         *config.AgentConfig
	grpcClient  *grpc.ClientConn
	stream      pb.AgentService_ConnectClient
	stopCh      chan struct{}
	isListening bool
}

// NewReporter yeni bir Reporter örneği oluşturur
func NewReporter(cfg *config.AgentConfig) *Reporter {
	return &Reporter{
		cfg:         cfg,
		stopCh:      make(chan struct{}),
		isListening: false,
	}
}

// Connect GRPC sunucusuna bağlanır
func (r *Reporter) Connect() error {
	// GRPC bağlantısı oluştur
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

// AgentRegistration agent bilgilerini sunucuya gönderir ve kayıt işlemini gerçekleştirir
func (r *Reporter) AgentRegistration(testResult string) error {
	hostname, _ := os.Hostname()
	ip := utils.GetLocalIP()

	// Agent bilgilerini gönder
	agentInfo := &pb.AgentMessage{
		Payload: &pb.AgentMessage_AgentInfo{
			AgentInfo: &pb.AgentInfo{
				Key:          r.cfg.Key,
				AgentId:      "agent_" + hostname,
				Hostname:     hostname,
				Ip:           ip,
				Platform:     r.cfg.PostgreSQL.Cluster,
				Auth:         r.cfg.PostgreSQL.Auth,
				Test:         testResult,
				PostgresUser: r.cfg.PostgreSQL.User,
				PostgresPass: r.cfg.PostgreSQL.Pass,
			},
		},
	}

	err := r.stream.Send(agentInfo)
	if err != nil {
		return err
	}

	log.Printf("Agent kaydı başarıyla tamamlandı. Test sonucu: %s", testResult)

	// Kayıt işlemi tamamlandıktan sonra komut dinlemeyi başlat
	if !r.isListening {
		go r.listenForCommands()
		r.isListening = true
		log.Println("Sunucudan komut dinleme başlatıldı")
	}

	return nil
}

// listenForCommands sunucudan gelen komutları dinler
func (r *Reporter) listenForCommands() {
	log.Println("Komut dinleme döngüsü başlatıldı")

	for {
		select {
		case <-r.stopCh:
			log.Println("Komut dinleme durduruldu")
			return
		default:
			in, err := r.stream.Recv()
			if err != nil {
				log.Printf("Cloud API bağlantısı kapandı: %v", err)
				// Bağlantıyı yeniden kurmaya çalış
				time.Sleep(5 * time.Second)
				if err := r.reconnect(); err != nil {
					log.Printf("Yeniden bağlantı başarısız: %v", err)
					time.Sleep(10 * time.Second) // Daha uzun süre bekle
				}
				continue
			}

			if query := in.GetQuery(); query != nil {
				log.Printf("Yeni sorgu geldi: %s (ID: %s)", query.Command, query.QueryId)

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

				if err := r.stream.Send(result); err != nil {
					log.Printf("Sorgu cevabı gönderilemedi: %v", err)
					// Bağlantı hatası olabilir, yeniden bağlanmayı dene
					if err := r.reconnect(); err != nil {
						log.Printf("Yeniden bağlantı başarısız: %v", err)
					}
				} else {
					log.Printf("Sorgu cevabı başarıyla gönderildi (ID: %s)", query.QueryId)
				}
			}
		}
	}
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
	return r.AgentRegistration("reconnected")
}

// processQuery, gelen sorguyu işler ve sonucu hesaplar
func processQuery(command string) map[string]interface{} {
	log.Printf("Sorgu işleniyor: %s", command)

	// Bu basitçe bir test yanıtı, gerçek uygulamada burada komut çalıştırılabilir
	return map[string]interface{}{
		"status":  "success",
		"command": command,
		"result":  "Command executed successfully",
		"time":    time.Now().String(),
	}
}
