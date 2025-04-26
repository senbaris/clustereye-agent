package alarm

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector/mongo"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"github.com/senbaris/clustereye-agent/internal/config"
)

// AlarmMonitor alarm durumlarını izleyen ve raporlayan birim
type AlarmMonitor struct {
	client         pb.AgentServiceClient
	agentID        string
	stopCh         chan struct{}
	alarmCache     map[string]*pb.AlarmEvent // Gönderilen son alarmları saklar
	alarmCacheLock sync.RWMutex
	checkInterval  time.Duration
	config         *config.AgentConfig
	platform       string // "postgres" veya "mongo"
}

// NewAlarmMonitor yeni bir alarm monitörü oluşturur
func NewAlarmMonitor(client pb.AgentServiceClient, agentID string, cfg *config.AgentConfig, platform string) *AlarmMonitor {
	return &AlarmMonitor{
		client:        client,
		agentID:       agentID,
		stopCh:        make(chan struct{}),
		alarmCache:    make(map[string]*pb.AlarmEvent),
		checkInterval: 30 * time.Second, // Varsayılan kontrol aralığı 30 saniye
		config:        cfg,
		platform:      platform,
	}
}

// Start alarm kontrol işlemini başlatır
func (m *AlarmMonitor) Start() {
	go m.monitorLoop()
	log.Println("Alarm monitörü başlatıldı")
}

// Stop alarm kontrol işlemini durdurur
func (m *AlarmMonitor) Stop() {
	close(m.stopCh)
	log.Println("Alarm monitörü durduruldu")
}

// SetCheckInterval alarm kontrol aralığını değiştirir
func (m *AlarmMonitor) SetCheckInterval(interval time.Duration) {
	m.checkInterval = interval
}

// monitorLoop periyodik olarak sistemdeki alarm durumlarını kontrol eder
func (m *AlarmMonitor) monitorLoop() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	// Bir de daha sık checkalarm yapan bir denetim daha var o yüzden burada yapmamıza gerek yok
	// m.checkAlarms()

	// Periyodik kontrole başla
	for {
		select {
		case <-ticker.C:
			log.Printf("Periyodik alarm kontrolü yapılıyor (interval: %v)", m.checkInterval)
			m.checkAlarms()
		case <-m.stopCh:
			return
		}
	}
}

// checkAlarms tüm alarm koşullarını kontrol eder
func (m *AlarmMonitor) checkAlarms() {
	// Platform kontrolü yap
	if m.platform == "mongo" {
		// Sadece MongoDB servis durumunu ve failover durumunu kontrol et
		m.checkMongoDBStatus()
	} else if m.platform == "postgres" {
		// PostgreSQL servis durumunu kontrol et
		m.checkPostgreSQLServiceStatus()
	} else {
		log.Printf("Bilinmeyen platform: %s", m.platform)
	}

	// Diğer alarm kontrolleri burada eklenebilir
	// m.checkMemoryUsage()
	// m.checkDiskUsage()
	// m.checkCPUUsage()
}

// checkPostgreSQLServiceStatus PostgreSQL servis durumunu kontrol eder
func (m *AlarmMonitor) checkPostgreSQLServiceStatus() {
	status := postgres.GetPGServiceStatus()
	alarmKey := "postgresql_service_status"

	// Rate limiting için zaman kontrolü ekle
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 15 saniye içinde gönderilmişse, tekrar gönderme
	if exists {
		// Önceki alarmın zamanını parse et
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil {
			timeSinceLastAlarm := time.Since(prevTimestamp)
			// Son 15 saniye içinde gönderilmişse ve durum değişmemişse tekrar gönderme
			if timeSinceLastAlarm < 15*time.Second &&
				((status == "FAIL!" && prevAlarm.Status == "triggered") ||
					(status == "RUNNING" && prevAlarm.Status == "resolved")) {
				log.Printf("PostgreSQL servis durumu son %v önce raporlandı, tekrar gönderilmeyecek.", timeSinceLastAlarm)
				return
			}
		}
	}

	if status == "FAIL!" {
		// Önceki bir alarm varsa ve aynı durumda ise tekrar gönderme
		if exists && prevAlarm.Status == "triggered" {
			// Alarm zaten tetiklenmiş, tekrar gönderme (sadece log için)
			log.Printf("PostgreSQL servis durumu hala FAIL! Alarm zaten gönderildi (%s)", prevAlarm.Id)
			return
		}

		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "postgresql_service_status",
			MetricValue: status,
			Message:     "PostgreSQL service is having issues (FAIL!)",
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "critical",
		}

		// Alarmı gönder
		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("PostgreSQL servis alarmı gönderilemedi: %v", err)
		} else {
			// Başarıyla gönderildi, önbellekte sakla
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()

			log.Printf("PostgreSQL servis FAIL! alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if status == "RUNNING" {
		// Önceki bir alarm varsa ve tetiklenmişse, çözüldü mesajı gönder
		if exists && prevAlarm.Status == "triggered" {
			// Çözüldü mesajı oluştur
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "postgresql_service_status",
				MetricValue: status,
				Message:     "PostgreSQL service is running again (RUNNING)",
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "info",
			}

			// Çözüldü mesajını gönder
			if err := m.reportAlarm(resolvedEvent); err != nil {
				log.Printf("PostgreSQL servis çözüldü mesajı gönderilemedi: %v", err)
			} else {
				// Başarıyla gönderildi, önbellekte sakla
				m.alarmCacheLock.Lock()
				m.alarmCache[alarmKey] = resolvedEvent
				m.alarmCacheLock.Unlock()

				log.Printf("PostgreSQL servis çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
			}
		}
	}
}

// checkMongoDBStatus MongoDB servis durumunu ve failover durumunu kontrol eder
func (m *AlarmMonitor) checkMongoDBStatus() {
	// MongoDB kolektörünü oluştur
	mongoCollector := mongo.NewMongoCollector(m.config)

	// Servis durumunu kontrol et
	currentStatus := mongoCollector.GetMongoServiceStatus()
	alarmKey := "mongodb_service_status"

	// Rate limiting için zaman kontrolü
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 60 saniye içinde gönderilmişse, tekrar gönderme
	if exists {
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil {
			timeSinceLastAlarm := time.Since(prevTimestamp)
			if timeSinceLastAlarm < 60*time.Second {
				log.Printf("MongoDB servis durumu son %v önce raporlandı, tekrar gönderilmeyecek.", timeSinceLastAlarm)
				return
			}
		}
	}

	// Servis durumu alarmı
	if currentStatus.Status != "RUNNING" {
		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mongodb_service_status",
			MetricValue: currentStatus.Status,
			Message:     fmt.Sprintf("MongoDB service is having issues: %s (%s)", currentStatus.Status, currentStatus.ErrorMessage),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "critical",
		}

		// Alarmı gönder
		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("MongoDB servis alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MongoDB servis alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if exists && prevAlarm.Status == "triggered" {
		// Servis düzeldi, çözüldü mesajı gönder
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "mongodb_service_status",
			MetricValue: currentStatus.Status,
			Message:     fmt.Sprintf("MongoDB service is running again (%s)", currentStatus.CurrentState),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("MongoDB servis çözüldü mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MongoDB servis çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	}

	// Failover durumu kontrolü
	failoverKey := "mongodb_failover"
	m.alarmCacheLock.RLock()
	prevFailoverAlarm, failoverExists := m.alarmCache[failoverKey]
	m.alarmCacheLock.RUnlock()

	var prevStatus *mongo.MongoServiceStatus
	if failoverExists {
		// Önceki durumu doğru şekilde parse et
		prevState := ""
		if strings.Contains(prevFailoverAlarm.Message, "PRIMARY") {
			prevState = "PRIMARY"
		} else if strings.Contains(prevFailoverAlarm.Message, "SECONDARY") {
			prevState = "SECONDARY"
		} else if strings.Contains(prevFailoverAlarm.Message, "ARBITER") {
			prevState = "ARBITER"
		}

		prevStatus = &mongo.MongoServiceStatus{
			Status:       prevFailoverAlarm.MetricValue,
			CurrentState: prevState,
			IsReplSet:    true,
		}

		// Rate limiting kontrolü
		if prevFailoverAlarm.Status == "triggered" {
			prevTimestamp, err := time.Parse(time.RFC3339, prevFailoverAlarm.Timestamp)
			if err == nil {
				timeSinceLastAlarm := time.Since(prevTimestamp)
				if timeSinceLastAlarm < 60*time.Second {
					log.Printf("MongoDB failover durumu son %v önce raporlandı, tekrar gönderilmeyecek.", timeSinceLastAlarm)
					return
				}
			}
		}

		log.Printf("DEBUG: Önceki MongoDB durumu: Status=%s, State=%s",
			prevStatus.Status, prevStatus.CurrentState)
	} else {
		// İlk çalıştırma için özel durum - mevcut durumu kaydet
		initialStateEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     failoverKey,
			AgentId:     m.agentID,
			Status:      "initial",
			MetricName:  "mongodb_failover",
			MetricValue: currentStatus.Status,
			Message:     fmt.Sprintf("MongoDB başlangıç durumu: %s", currentStatus.CurrentState),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		// Başlangıç durumunu önbelleğe kaydet
		m.alarmCacheLock.Lock()
		m.alarmCache[failoverKey] = initialStateEvent
		m.alarmCacheLock.Unlock()

		log.Printf("DEBUG: MongoDB başlangıç durumu kaydedildi: %s", currentStatus.CurrentState)
		return // İlk çalıştırmada daha fazla kontrol yapma
	}

	log.Printf("DEBUG: Mevcut MongoDB durumu: Status=%s, State=%s, IsReplSet=%v",
		currentStatus.Status, currentStatus.CurrentState, currentStatus.IsReplSet)

	// Failover kontrolü
	if mongoCollector.CheckForFailover(prevStatus, currentStatus) {
		// Eğer önceki alarm hala "triggered" durumunda ve aynı durum devam ediyorsa, yeni alarm gönderme
		if failoverExists && prevFailoverAlarm.Status == "triggered" {
			// Önceki ve şimdiki durumları karşılaştır
			if (prevStatus.CurrentState == "PRIMARY" && currentStatus.CurrentState == "SECONDARY") ||
				(prevStatus.CurrentState == "SECONDARY" && currentStatus.CurrentState == "PRIMARY") {
				log.Printf("MongoDB failover durumu zaten raporlandı ve durum değişmedi. Alarm ID: %s", prevFailoverAlarm.Id)
				return
			}
		}

		// Failover alarmı oluştur
		failoverEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     failoverKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mongodb_failover",
			MetricValue: currentStatus.Status,
			Message: fmt.Sprintf("MongoDB failover detected! Previous state: %s, New state: %s, Replica Set: %v",
				prevStatus.CurrentState, currentStatus.CurrentState, currentStatus.IsReplSet),
			Timestamp: time.Now().Format(time.RFC3339),
			Severity:  "warning",
		}

		// Alarmı gönder
		if err := m.reportAlarm(failoverEvent); err != nil {
			log.Printf("MongoDB failover alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[failoverKey] = failoverEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MongoDB failover alarmı gönderildi (ID: %s)", failoverEvent.Id)
		}
	} else {
		// Durum normale döndüyse (örneğin: SECONDARY -> PRIMARY -> SECONDARY tamamlandı)
		if failoverExists && prevFailoverAlarm.Status == "triggered" {
			// Çözüldü mesajı oluştur
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     failoverKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "mongodb_failover",
				MetricValue: currentStatus.Status,
				Message:     fmt.Sprintf("MongoDB failover has done. Current state: %s", currentStatus.CurrentState),
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "info",
			}

			// Çözüldü mesajını gönder
			if err := m.reportAlarm(resolvedEvent); err != nil {
				log.Printf("MongoDB failover çözüldü mesajı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[failoverKey] = resolvedEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MongoDB failover çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
			}
		} else {
			// Normal durum güncellemesi
			updateEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     failoverKey,
				AgentId:     m.agentID,
				Status:      "update",
				MetricName:  "mongodb_failover",
				MetricValue: currentStatus.Status,
				Message:     fmt.Sprintf("MongoDB current state: %s", currentStatus.CurrentState),
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "info",
			}

			m.alarmCacheLock.Lock()
			m.alarmCache[failoverKey] = updateEvent
			m.alarmCacheLock.Unlock()
		}
	}
}

// reportAlarm bir alarm olayını API'ye bildirir
func (m *AlarmMonitor) reportAlarm(event *pb.AlarmEvent) error {
	maxRetries := 5            // Daha fazla deneme hakkı
	backoff := 2 * time.Second // Daha uzun bekleme süresi

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Her denemede yeni bir context oluştur (30 saniye zaman aşımı)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		// İstek gönder
		req := &pb.ReportAlarmRequest{
			AgentId: m.agentID,
			Events:  []*pb.AlarmEvent{event},
		}

		resp, err := m.client.ReportAlarm(ctx, req)
		cancel() // Context'i hemen temizle

		if err != nil {
			// Daha kapsamlı hata kontrol mekanizması
			isConnectionError := strings.Contains(err.Error(), "connection") ||
				strings.Contains(err.Error(), "transport") ||
				strings.Contains(err.Error(), "Canceled") ||
				strings.Contains(err.Error(), "Deadline") ||
				strings.Contains(err.Error(), "context")

			if attempt < maxRetries-1 && isConnectionError { // Son deneme değilse
				log.Printf("Alarm gönderimi başarısız (deneme %d/%d): %v. Yeniden deneniyor...",
					attempt+1, maxRetries, err)

				// Exponential backoff ile bekle
				time.Sleep(backoff * time.Duration(attempt+1))
				continue
			}

			return fmt.Errorf("alarm gönderilemedi (deneme %d/%d): %v",
				attempt+1, maxRetries, err)
		}

		// Başarılı
		log.Printf("Alarm raporu gönderildi, yanıt: %s (deneme %d/%d)",
			resp.Status, attempt+1, maxRetries)
		return nil
	}

	return fmt.Errorf("alarm %d deneme sonrasında gönderilemedi", maxRetries)
}

// UpdateClient, yeni bir gRPC client ile alarm monitörünün client'ını günceller
func (m *AlarmMonitor) UpdateClient(client pb.AgentServiceClient) {
	m.client = client
	log.Println("AlarmMonitor client'ı güncellendi")
}
