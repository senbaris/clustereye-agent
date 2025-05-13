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
	platform  string // Hangi platform için çalışıyor (postgres, mongo veya mssql)
}

// NewAgent yeni bir Agent örneği oluşturur
func NewAgent(cfg *config.AgentConfig) *Agent {
	return &Agent{
		cfg:       cfg,
		collector: collector.NewCollector(cfg),
		reporter:  reporter.NewReporter(cfg),
		stopCh:    make(chan struct{}),
		platform:  "mssql", // Varsayılan olarak postgres
	}
}

// SetPlatform agent'ın çalışacağı platformu ayarlar
func (a *Agent) SetPlatform(platform string) {
	a.platform = platform
	log.Printf("Agent platformu ayarlandı: %s", platform)
}

// Agent tarafında
func (a *Agent) Start() error {
	log.Printf("Agent başlatılıyor: %s (Platform: %s)", a.cfg.Name, a.platform)

	// GRPC bağlantısını başlat
	err := a.reporter.Connect()
	if err != nil {
		return err
	}

	// Platforma göre bağlantı testi yap
	var testResult string
	if a.platform == "postgres" {
		testResult = a.collector.TestPostgresConnection()
		log.Printf("PostgreSQL bağlantı testi sonucu: %s", testResult)
	} else if a.platform == "mongo" {
		testResult = a.testMongoConnection()
		log.Printf("MongoDB bağlantı testi sonucu: %s", testResult)
	} else if a.platform == "mssql" {
		testResult = a.testMSSQLConnection()
		log.Printf("MSSQL bağlantı testi sonucu: %s", testResult)
	}

	// Agent bilgilerini stream üzerinden gönder
	err = a.reporter.AgentRegistration(testResult, a.platform)
	if err != nil {
		return fmt.Errorf("agent bilgileri gönderilemedi: %v", err)
	}

	// Veri toplama işlemini başlat
	go a.collectData()

	log.Printf("Agent başarıyla başlatıldı: %s", a.cfg.Name)
	return nil
}

// testMongoConnection MongoDB bağlantısını test eder
func (a *Agent) testMongoConnection() string {
	// MongoDB bağlantı testi yap
	log.Printf("MongoDB bağlantısı test ediliyor...")

	mongoStatus := "Unknown"

	// MongoDB bağlantısı test et
	result, err := a.collector.TestMongoConnection()
	if err != nil {
		mongoStatus = fmt.Sprintf("ERROR: %v", err)
	} else {
		mongoStatus = result
	}

	return mongoStatus
}

// testMSSQLConnection MSSQL bağlantısını test eder
func (a *Agent) testMSSQLConnection() string {
	// MSSQL bağlantı testi yap
	log.Printf("MSSQL bağlantısı test ediliyor...")

	// MSSQL bağlantısı test et
	result := a.collector.TestMSSQLConnection()
	return result
}

// Stop agent'ı durdurur
func (a *Agent) Stop() {
	log.Printf("Agent durduruluyor: %s", a.cfg.Name)
	close(a.stopCh)
	a.reporter.Disconnect()
}

// collectData periyodik olarak veri toplar ve raporlar
func (a *Agent) collectData() {
	// Platforma göre farklı veri toplama metodu kullan
	if a.platform == "postgres" {
		// PostgreSQL bilgilerini topla
		info := pb.PostgresInfo{
			ClusterName:       a.cfg.PostgreSQL.Cluster,
			Ip:                "127.0.0.1",
			Hostname:          "localhost",
			NodeStatus:        "RUNNING",
			PgVersion:         "15.1",
			Location:          a.cfg.PostgreSQL.Location,
			PgBouncerStatus:   postgres.GetPGBouncerStatus(),
			PgServiceStatus:   postgres.GetPGServiceStatus(),
			ReplicationLagSec: 0,
			FreeDisk:          "100GB",
			FdPercent:         100,
		}

		a.reporter.Report(&info)
	} else if a.platform == "mongo" {
		// MongoDB bilgilerini topla ve raporla
		a.reportMongoInfo()
	} else if a.platform == "mssql" {
		// MSSQL bilgilerini topla ve raporla
		a.reportMSSQLInfo()
	}
}

// reportMongoInfo MongoDB bilgilerini toplar ve raporlar
func (a *Agent) reportMongoInfo() {
	// MongoDB bilgilerini topla
	log.Printf("MongoDB bilgileri toplanıyor...")

	// MongoDB bilgilerini raporla
	err := a.reporter.SendMongoInfo()
	if err != nil {
		log.Printf("MongoDB bilgileri gönderilemedi: %v", err)
	} else {
		log.Printf("MongoDB bilgileri başarıyla raporlandı")
	}
}

// reportMSSQLInfo MSSQL bilgilerini toplar ve raporlar
func (a *Agent) reportMSSQLInfo() {
	// MSSQL bilgilerini topla
	log.Printf("MSSQL bilgileri toplanıyor...")

	// MSSQL bilgilerini raporla
	err := a.reporter.SendMSSQLInfo()
	if err != nil {
		log.Printf("MSSQL bilgileri gönderilemedi: %v", err)
	} else {
		log.Printf("MSSQL bilgileri başarıyla raporlandı")
	}
}
