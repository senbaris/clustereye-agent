package agent

import (
	"fmt"
	"log"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/reporter"
)

// Agent veri toplama ve raporlama işlemlerini yönetir
type Agent struct {
	cfg       *config.AgentConfig
	collector *collector.Collector
	reporter  *reporter.Reporter
	stopCh    chan struct{}
}

// NewAgent yeni bir Agent örneği oluşturur
func NewAgent(cfg *config.AgentConfig) *Agent {
	return &Agent{
		cfg:       cfg,
		collector: collector.NewCollector(cfg),
		reporter:  reporter.NewReporter(cfg),
		stopCh:    make(chan struct{}),
	}
}

// Agent tarafında
func (a *Agent) Start() error {
    log.Printf("Agent başlatılıyor: %s", a.cfg.Name)

    // GRPC bağlantısını başlat
    err := a.reporter.Connect()
    if err != nil {
        return err
    }

    // PostgreSQL bağlantı testi yap
    testResult := a.collector.TestPostgresConnection()
    log.Printf("PostgreSQL bağlantı testi sonucu: %s", testResult)

    // Agent bilgilerini stream üzerinden gönder
    err = a.reporter.AgentRegistration(testResult)
    if err != nil {
        return fmt.Errorf("agent bilgileri gönderilemedi: %v", err)
    }

    // Veri toplama işlemini başlat
    go a.collectData()

    log.Printf("Agent başarıyla başlatıldı: %s", a.cfg.Name)
    return nil
}

// Stop agent'ı durdurur
func (a *Agent) Stop() {
	log.Printf("Agent durduruluyor: %s", a.cfg.Name)
	close(a.stopCh)
	a.reporter.Disconnect()
}

// collectData periyodik olarak veri toplar ve raporlar
func (a *Agent) collectData() {

	info := pb.PostgresInfo{
		ClusterName:       "clusterName",
		Ip:                "127.0.0.1",
		Hostname:          "localhost",
		NodeStatus:        "RUNNING",
		PgVersion:         "15.1",
		Location:          "istanbul",
		PgBouncerStatus:   "RUNNING",
		PgServiceStatus:   postgres.GetPGServiceStatus(),
		ReplicationLagSec: 0,
		FreeDisk:          "100GB",
		FdPercent:         100,
	}

	a.reporter.Report(&info)
}
