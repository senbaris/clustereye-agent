package agent

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/senbaris/clustereye-agent/internal/collector"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
	"github.com/senbaris/clustereye-agent/internal/reporter"
)

// Agent veri toplama ve raporlama işlemlerini yönetir
type Agent struct {
	cfg       *config.AgentConfig
	collector *collector.Collector
	reporter  *reporter.Reporter
	stopCh    chan struct{}
	platform  string // Hangi platform için çalışıyor (postgres, mongo veya mssql)
	isRunning bool   // Agent'ın çalışma durumu
}

// NewAgent yeni bir Agent örneği oluşturur
func NewAgent(cfg *config.AgentConfig) *Agent {
	return &Agent{
		cfg:       cfg,
		collector: collector.NewCollector(cfg),
		reporter:  reporter.NewReporter(cfg),
		stopCh:    make(chan struct{}),
		platform:  "mssql", // Varsayılan olarak mssql
		isRunning: false,
	}
}

// SetPlatform agent'ın çalışacağı platformu ayarlar
func (a *Agent) SetPlatform(platform string) {
	a.platform = platform
	logger.Info("Agent platformu ayarlandı: %s", platform)
}

// Agent tarafında
func (a *Agent) Start() error {
	// En üst seviyede panic recovery ekliyoruz
	defer func() {
		if r := recover(); r != nil {
			logger.Error("AGENT PANIC RECOVERY: %v", r)
			logger.Error("Agent servisini yeniden başlatıyorum...")

			// 5 saniye bekle ve agent'ı yeniden başlatmaya çalış
			time.Sleep(5 * time.Second)
			go a.restartAgent()
		}
	}()

	logger.Info("Agent başlatılıyor: %s (Platform: %s)", a.cfg.Name, a.platform)

	// Agent çalışma durumunu güncelle
	a.isRunning = true

	// Bağlantı kurma işlemini birkaç kez dene
	maxRetries := 3
	retryDelay := 5 * time.Second
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("GRPC bağlantısı yeniden deneniyor (%d/%d)...", attempt+1, maxRetries)
			time.Sleep(retryDelay)
			// Her denemede bekleme süresini arttır
			retryDelay *= 2
		}

		// GRPC bağlantısını başlat
		err = a.reporter.Connect()
		if err == nil {
			break // Bağlantı başarılı
		}

		logger.Warning("GRPC bağlantısı başarısız (deneme %d/%d): %v",
			attempt+1, maxRetries, err)
	}

	if err != nil {
		logger.Error("Maximum retries exceeded, could not connect to GRPC server: %v", err)
		// Başarısız olsa bile devam et, periyodik olarak tekrar denenecek
	}

	// Platforma göre bağlantı testi yap
	var testResult string
	if a.platform == "postgres" {
		testResult = a.collector.TestPostgresConnection()
		logger.Info("PostgreSQL bağlantı testi sonucu: %s", testResult)
	} else if a.platform == "mongo" {
		testResult = a.testMongoConnection()
		logger.Info("MongoDB bağlantı testi sonucu: %s", testResult)
	} else if a.platform == "mssql" {
		testResult = a.testMSSQLConnection()
		logger.Info("MSSQL bağlantı testi sonucu: %s", testResult)
	}

	// Agent bilgilerini stream üzerinden gönder (MSSQL için konfigürasyon yap)
	if a.platform == "mssql" && runtime.GOOS == "windows" {
		// Windows + MSSQL kombinasyonu için daha uzun timeout
		logger.Info("MSSQL platform yapılandırması uygulanıyor...")

		// Uzun bir timeout ile agent registration dene
		registerWithRetry(func() error {
			return a.reporter.AgentRegistration(testResult, a.platform)
		})
	} else {
		// Diğer platformlar için normal registration
		err = a.reporter.AgentRegistration(testResult, a.platform)
		if err != nil {
			logger.Error("Agent bilgileri gönderilemedi: %v - periyodik olarak tekrar denenecek", err)
			// Hata olsa bile devam et
		}
	}

	// Veri toplama işlemini başlat - bu kısmı eklemeyi unutmuştum
	go a.collectData()

	logger.Info("Agent başarıyla başlatıldı: %s", a.cfg.Name)
	return nil
}

// registerWithRetry tekrarlanan kayıt denemelerini yapar
func registerWithRetry(registerFunc func() error) {
	maxRetries := 5
	retryDelay := 5 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("Agent kaydı yeniden deneniyor (%d/%d)...", attempt+1, maxRetries)
			time.Sleep(retryDelay)
			// Her denemede bekleme süresini arttır
			retryDelay *= 2
		}

		err := registerFunc()
		if err == nil {
			logger.Info("Agent kaydı başarılı!")
			return
		}

		logger.Warning("Agent kaydı başarısız (deneme %d/%d): %v",
			attempt+1, maxRetries, err)
	}

	logger.Error("Maximum retries exceeded, agent registration will be retried periodically")
}

// restartAgent agent'ı yeniden başlatmaya çalışır
func (a *Agent) restartAgent() {
	// Panic recovery - yeniden başlatma sırasında bile çökme olmamalı
	defer func() {
		if r := recover(); r != nil {
			logger.Error("RESTART PANIC RECOVERY: %v", r)
			logger.Error("Yeniden başlatma başarısız, 30 saniye sonra tekrar denenecek...")
			time.Sleep(30 * time.Second)
			go a.restartAgent() // Tekrar dene
		}
	}()

	// Mevcut bağlantıları temizle
	a.reporter.Disconnect()

	// Yeni bağlantılar oluştur
	a.reporter = reporter.NewReporter(a.cfg)

	// Agent'ı başlat
	err := a.Start()
	if err != nil {
		logger.Error("Agent yeniden başlatılamadı: %v", err)
	} else {
		logger.Info("Agent başarıyla yeniden başlatıldı")
	}
}

// testMongoConnection MongoDB bağlantısını test eder
func (a *Agent) testMongoConnection() string {
	// MongoDB bağlantı testi yap
	logger.Info("MongoDB bağlantısı test ediliyor...")

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
	logger.Info("MSSQL bağlantısı test ediliyor...")

	// MSSQL bağlantısı test et - timeout ile
	var result string

	// İşlem zamanını ölç
	startTime := time.Now()
	result = a.collector.TestMSSQLConnection()
	elapsed := time.Since(startTime)

	logger.Info("MSSQL bağlantı testi tamamlandı, süre: %v", elapsed)
	return result
}

// Stop agent'ı durdurur
func (a *Agent) Stop() {
	logger.Info("Agent durduruluyor: %s", a.cfg.Name)
	a.isRunning = false
	close(a.stopCh)
	a.reporter.Disconnect()
}

// collectData periyodik olarak veri toplar ve raporlar
func (a *Agent) collectData() {
	// Panic recovery ekleyelim, herhangi bir nedenden dolayı çökmeyi engelle
	defer func() {
		if r := recover(); r != nil {
			logger.Error("COLLECT DATA PANIC RECOVERY: %v", r)
			logger.Error("Veri toplama işlemi yeniden başlatılıyor...")

			// Kısa bir gecikme sonra yeniden başlat
			time.Sleep(5 * time.Second)
			go a.collectData() // Yeniden başlat
		}
	}()

	logger.Info("Veri toplama işlemi başlatılıyor... (Platform: %s)", a.platform)

	// Periyodik raporlama için ayarlar
	interval := 60 * time.Second // Varsayılan 1 dakika

	// Windows'da MSSQL için daha uzun aralık kullan
	if a.platform == "mssql" && runtime.GOOS == "windows" {
		interval = 120 * time.Second // 2 dakika
		logger.Info("MSSQL için veri toplama aralığı 2 dakika olarak ayarlandı")
	}

	// Platform için ilk veri toplama ve raporlamayı yap
	a.collectAndReportForPlatform()

	// Ticker oluştur
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Watchdog ticker - aşırı uzun süredir veri toplanmamışsa zorla tetikle
	watchdogTicker := time.NewTicker(5 * time.Minute)
	defer watchdogTicker.Stop()

	// Son başarılı toplama zamanı
	lastCollectionTime := time.Now()

	for {
		select {
		case <-a.stopCh:
			logger.Info("Veri toplama durduruldu")
			return

		case <-ticker.C:
			logger.Debug("Periyodik veri toplama tetiklendi...")
			a.collectAndReportForPlatform()
			lastCollectionTime = time.Now()

		case <-watchdogTicker.C:
			// Son toplamadan bu yana uzun zaman geçtiyse zorla topla
			timeSinceLastCollection := time.Since(lastCollectionTime)
			if timeSinceLastCollection > 2*interval {
				logger.Warning("Watchdog: Son veri toplamadan bu yana %v geçti, zorla yeni toplama başlatılıyor...",
					timeSinceLastCollection)
				a.collectAndReportForPlatform()
				lastCollectionTime = time.Now()
			}
		}
	}
}

// collectAndReportForPlatform platform türüne göre veri toplar ve raporlar
func (a *Agent) collectAndReportForPlatform() {
	// Panic recovery için
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PLATFORM COLLECT PANIC RECOVERY: %v", r)
		}
	}()

	logger.Debug("Platform türüne göre veri toplanıyor: %s", a.platform)

	// Platforma göre farklı veri toplama metodu kullan
	if a.platform == "postgres" {
		// PostgreSQL bilgilerini topla
		a.reportPostgresInfo()
	} else if a.platform == "mongo" {
		// MongoDB bilgilerini topla ve raporla
		a.reportMongoInfo()
	} else if a.platform == "mssql" {
		// MSSQL bilgilerini topla ve raporla
		a.reportMSSQLInfo()
	}
}

// reportPostgresInfo PostgreSQL bilgilerini toplar ve raporlar
func (a *Agent) reportPostgresInfo() {
	// PostgreSQL bilgilerini topla
	logger.Info("PostgreSQL bilgileri toplanıyor...")

	// PostgreSQL bilgilerini raporla
	err := a.reporter.SendPostgresInfo()
	if err != nil {
		logger.Error("PostgreSQL bilgileri gönderilemedi: %v", err)
	} else {
		logger.Info("PostgreSQL bilgileri başarıyla raporlandı")
	}
}

// reportMongoInfo MongoDB bilgilerini toplar ve raporlar
func (a *Agent) reportMongoInfo() {
	// MongoDB bilgilerini topla
	logger.Info("MongoDB bilgileri toplanıyor...")

	// MongoDB bilgilerini raporla
	err := a.reporter.SendMongoInfo()
	if err != nil {
		logger.Error("MongoDB bilgileri gönderilemedi: %v", err)
	} else {
		logger.Info("MongoDB bilgileri başarıyla raporlandı")
	}
}

// reportMSSQLInfo MSSQL bilgilerini toplar ve raporlar
func (a *Agent) reportMSSQLInfo() {
	// Panik kurtarma mekanizması
	defer func() {
		if r := recover(); r != nil {
			logger.Error("MSSQL rapor panik kurtarma: %v", r)
		}
	}()

	// MSSQL bilgilerini topla
	logger.Info("MSSQL bilgileri toplanıyor...")

	// İşlem süresini ölç
	startTime := time.Now()

	// "database is closed" hatası durumunda yeniden denemek için
	var err error
	maxRetries := 3
	retryDelay := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logger.Warning("MSSQL bilgileri toplama yeniden deneniyor (%d/%d) - önceki hata: %v",
				attempt+1, maxRetries, err)
			time.Sleep(retryDelay)
			// Her denemede bekleme süresini arttır
			retryDelay *= 2
		}

		// MSSQL bilgilerini raporla - retry mekanizması reporter içinde
		err = a.reporter.SendMSSQLInfo()

		// Hata yoksa veya hatanın "database is closed" ile ilgisi yoksa döngüden çık
		if err == nil || !containsDBClosedError(err.Error()) {
			break
		}

		logger.Warning("'database is closed' hatası, collector'ı yeniden başlatmayı deniyorum...")
		// Collector'ı yenile - yeni bir veritabanı bağlantısı oluştur
		a.collector = collector.NewCollector(a.cfg)
	}

	elapsedTime := time.Since(startTime)
	if err != nil {
		logger.Error("MSSQL bilgileri gönderilemedi (%v içinde): %v", elapsedTime, err)
	} else {
		logger.Info("MSSQL bilgileri başarıyla raporlandı (süre: %v)", elapsedTime)
	}
}

// containsDBClosedError bir hata mesajında "database is closed" olup olmadığını kontrol eder
func containsDBClosedError(errMsg string) bool {
	return strings.Contains(errMsg, "database is closed") ||
		strings.Contains(errMsg, "sql: database is closed") ||
		strings.Contains(errMsg, "connection is closed")
}
