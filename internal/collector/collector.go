package collector

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq" // PostgreSQL sürücüsü
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/model"
)

// Collector sistem verilerini toplar
type Collector struct {
	cfg *config.AgentConfig
}

// NewCollector yeni bir Collector örneği oluşturur
func NewCollector(cfg *config.AgentConfig) *Collector {
	return &Collector{
		cfg: cfg,
	}
}

// Collect sistem verilerini toplar
func (c *Collector) Collect() (*model.SystemData, error) {
	// PostgreSQL verilerini topla
	pgData, err := c.collectPostgresData()
	if err != nil {
		return nil, err
	}

	// MongoDB verilerini topla
	mongoData, err := c.collectMongoData()
	if err != nil {
		return nil, err
	}

	// Sistem verilerini topla
	sysData, err := c.collectSystemData()
	if err != nil {
		return nil, err
	}

	// Tüm verileri birleştir
	return &model.SystemData{
		AgentKey:   c.cfg.Key,
		AgentName:  c.cfg.Name,
		Timestamp:  model.GetCurrentTimestamp(),
		PostgreSQL: pgData,
		MongoDB:    mongoData,
		System:     sysData,
	}, nil
}

// TestPostgresConnection PostgreSQL bağlantısını test eder
func (c *Collector) TestPostgresConnection() string {
	// PostgreSQL bağlantı bilgilerini yapılandırma dosyasından al
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.cfg.PostgreSQL.Host,
		c.cfg.PostgreSQL.Port,
		c.cfg.PostgreSQL.User,
		c.cfg.PostgreSQL.Pass,
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

// collectPostgresData PostgreSQL verilerini toplar
func (c *Collector) collectPostgresData() (*model.PostgreSQLData, error) {
	// PostgreSQL bağlantısı ve veri toplama işlemleri
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.cfg.PostgreSQL.Host,
		c.cfg.PostgreSQL.Port,
		c.cfg.PostgreSQL.User,
		c.cfg.PostgreSQL.Pass,
		"postgres", // Varsayılan veritabanı adı
	)

	// Veritabanına bağlan
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("PostgreSQL bağlantısı açılamadı: %v", err)
		return &model.PostgreSQLData{Status: "error"}, nil
	}
	defer db.Close()

	// Bağlantıyı test et
	err = db.Ping()
	if err != nil {
		log.Printf("PostgreSQL bağlantı testi başarısız: %v", err)
		return &model.PostgreSQLData{Status: "error"}, nil
	}

	// Aktif bağlantı sayısını al
	var connections int
	err = db.QueryRow("SELECT count(*) FROM pg_stat_activity").Scan(&connections)
	if err != nil {
		log.Printf("PostgreSQL bağlantı sayısı alınamadı: %v", err)
		connections = 0
	}

	return &model.PostgreSQLData{
		Status:      "running",
		Connections: connections,
		// Diğer PostgreSQL verileri
	}, nil
}

// collectMongoData MongoDB verilerini toplar
func (c *Collector) collectMongoData() (*model.MongoDBData, error) {
	// MongoDB bağlantısı ve veri toplama işlemleri
	// Şimdilik basit bir veri döndürelim
	return &model.MongoDBData{
		Status: "running",
		// Diğer MongoDB verileri
	}, nil
}

// collectSystemData sistem verilerini toplar
func (c *Collector) collectSystemData() (*model.SystemMetrics, error) {
	// Sistem metriklerini toplama işlemleri
	// Şimdilik basit değerler döndürelim
	return &model.SystemMetrics{
		CPUUsage:    30.5,
		MemoryUsage: 45.2,
		DiskUsage:   60.8,
		// Diğer sistem metrikleri
	}, nil
}
