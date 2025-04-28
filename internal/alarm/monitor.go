package alarm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector/mongo"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"github.com/senbaris/clustereye-agent/internal/config"
)

const (
	// Agent versiyonu
	AgentVersion = "1.0.22" // Bu değeri CI/CD sürecinde otomatik güncelleyebilirsiniz
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
	platform       string                // "postgres" veya "mongo"
	thresholds     *pb.ThresholdSettings // Threshold değerleri
}

// NewAlarmMonitor yeni bir alarm monitörü oluşturur
func NewAlarmMonitor(client pb.AgentServiceClient, agentID string, cfg *config.AgentConfig, platform string) *AlarmMonitor {
	monitor := &AlarmMonitor{
		client:        client,
		agentID:       agentID,
		stopCh:        make(chan struct{}),
		alarmCache:    make(map[string]*pb.AlarmEvent),
		checkInterval: 30 * time.Second, // Varsayılan kontrol aralığı 30 saniye
		config:        cfg,
		platform:      platform,
	}

	// İlk threshold değerlerini al
	monitor.updateThresholds()

	return monitor
}

// updateThresholds API'den threshold değerlerini alır
func (m *AlarmMonitor) updateThresholds() {
	maxRetries := 5
	backoff := time.Second * 2 // başlangıç bekleme süresi

	for attempt := 0; attempt < maxRetries; attempt++ {
		req := &pb.GetThresholdSettingsRequest{
			AgentId: m.agentID,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := m.client.GetThresholdSettings(ctx, req)
		cancel()

		if err != nil {
			// Bağlantı hatalarını kontrol et
			isConnectionError := strings.Contains(err.Error(), "connection") ||
				strings.Contains(err.Error(), "transport") ||
				strings.Contains(err.Error(), "Canceled") ||
				strings.Contains(err.Error(), "Deadline") ||
				strings.Contains(err.Error(), "context")

			if attempt < maxRetries-1 && isConnectionError {
				waitTime := backoff * time.Duration(attempt+1) // exponential backoff
				log.Printf("Threshold değerleri alınamadı (deneme %d/%d): %v. %v sonra tekrar denenecek...",
					attempt+1, maxRetries, err, waitTime)
				time.Sleep(waitTime)
				continue
			}

			log.Printf("Threshold değerleri alınamadı (son deneme %d/%d): %v",
				attempt+1, maxRetries, err)
			return
		}

		m.thresholds = resp.Settings
		log.Printf("Threshold değerleri güncellendi: CPU=%.2f%%, Memory=%.2f%%, Disk=%.2f%%, SlowQuery=%dms, Connection=%d, ReplicationLag=%d",
			m.thresholds.CpuThreshold,
			m.thresholds.MemoryThreshold,
			m.thresholds.DiskThreshold,
			m.thresholds.SlowQueryThresholdMs,
			m.thresholds.ConnectionThreshold,
			m.thresholds.ReplicationLagThreshold)
		return
	}

	log.Printf("Threshold değerleri %d deneme sonrasında alınamadı", maxRetries)
}

// Start alarm kontrol işlemini başlatır
func (m *AlarmMonitor) Start() {
	go m.monitorLoop()
	go m.reportAgentVersion() // Version raporlama işlemini başlat
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
	thresholdUpdateTicker := time.NewTicker(5 * time.Minute) // Her 5 dakikada bir threshold değerlerini güncelle
	defer ticker.Stop()
	defer thresholdUpdateTicker.Stop()

	// Periyodik kontrole başla
	for {
		select {
		case <-ticker.C:
			log.Printf("Periyodik alarm kontrolü yapılıyor (interval: %v)", m.checkInterval)
			m.checkAlarms()
		case <-thresholdUpdateTicker.C:
			log.Printf("Threshold değerleri güncelleniyor...")
			m.updateThresholds()
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
		// Uzun süren sorguları kontrol et
		m.checkSlowQueries()
		// CPU kullanımını kontrol et
		m.checkCPUUsage()
		// Memory kullanımını kontrol et
		m.checkMemoryUsage()
		// Disk kullanımını kontrol et
		m.checkDiskUsage()
	} else {
		log.Printf("Bilinmeyen platform: %s", m.platform)
	}
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

// checkSlowQueries uzun süren sorguları kontrol eder ve alarm gönderir
func (m *AlarmMonitor) checkSlowQueries() {
	if m.thresholds == nil || m.thresholds.SlowQueryThresholdMs == 0 {
		log.Printf("Slow query threshold değeri ayarlanmamış")
		return
	}

	// pg_stat_activity'den uzun süren sorguları al
	query := fmt.Sprintf(`
		SELECT pid, usename, 
		       COALESCE(datname, '') as datname, 
		       COALESCE(query, '') as query, 
		       EXTRACT(EPOCH FROM now() - query_start) * 1000 as duration_ms
		FROM pg_stat_activity
		WHERE state = 'active'
		AND query NOT ILIKE '%%pg_stat_activity%%'
		AND query NOT ILIKE '%%START_REPLICATION SLOT%%'
		AND query_start < now() - interval '%d milliseconds'
	`, m.thresholds.SlowQueryThresholdMs)

	db, err := postgres.OpenDB()
	if err != nil {
		log.Printf("Veritabanı bağlantısı açılamadı: %v", err)
		return
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Uzun süren sorgular alınamadı: %v", err)
		return
	}
	defer rows.Close()

	var slowQueries []string
	var maxDuration float64
	for rows.Next() {
		var (
			pid        int
			username   sql.NullString
			database   string
			queryText  string
			durationMs float64
		)
		if err := rows.Scan(&pid, &username, &database, &queryText, &durationMs); err != nil {
			log.Printf("Sorgu bilgileri okunamadı: %v", err)
			continue
		}

		if durationMs > maxDuration {
			maxDuration = durationMs
		}

		// Sorgu metnini kısalt
		if len(queryText) > 200 {
			queryText = queryText[:197] + "..."
		}

		// NULL username kontrolü
		usernameStr := "unknown"
		if username.Valid {
			usernameStr = username.String
		}

		slowQueries = append(slowQueries, fmt.Sprintf("PID=%d, User=%s, DB=%s, Duration=%.2fms, Query=%s",
			pid, usernameStr, database, durationMs, queryText))
	}

	if len(slowQueries) > 0 {
		alarmKey := "postgresql_slow_queries"

		// Rate limiting kontrolü
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		if exists {
			prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
			if err == nil && time.Since(prevTimestamp) < 5*time.Minute {
				log.Printf("Slow query alarmı son 5 dakika içinde gönderildi, tekrar gönderilmeyecek")
				return
			}
		}

		message := fmt.Sprintf("Found %d slow queries exceeding %dms threshold. Max duration: %.2fms\n%s",
			len(slowQueries), m.thresholds.SlowQueryThresholdMs, maxDuration,
			strings.Join(slowQueries[:min(3, len(slowQueries))], "\n"))

		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "postgresql_slow_queries",
			MetricValue: fmt.Sprintf("%.2f", maxDuration),
			Message:     message,
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "warning",
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("Slow query alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("Slow query alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	}
}

// checkCPUUsage CPU kullanımını kontrol eder ve threshold'u aşarsa alarm üretir
func (m *AlarmMonitor) checkCPUUsage() {
	if m.thresholds == nil || m.thresholds.CpuThreshold == 0 {
		log.Printf("CPU threshold değeri ayarlanmamış")
		return
	}

	cpuUsage, err := getCPUUsage()
	if err != nil {
		log.Printf("CPU kullanımı alınamadı: %v", err)
		return
	}

	alarmKey := "system_cpu_usage"

	// Rate limiting kontrolü
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 5 dakika içinde gönderilmişse, tekrar gönderme
	if exists {
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil {
			timeSinceLastAlarm := time.Since(prevTimestamp)
			if timeSinceLastAlarm < 5*time.Minute {
				// Eğer önceki alarm "triggered" ve CPU hala yüksekse veya
				// önceki alarm "resolved" ve CPU hala normalse, yeni alarm gönderme
				prevTriggered := prevAlarm.Status == "triggered"
				currentHigh := cpuUsage >= m.thresholds.CpuThreshold
				if prevTriggered == currentHigh {
					return
				}
			}
		}
	}

	// CPU threshold'u aşıldı mı kontrol et
	if cpuUsage >= m.thresholds.CpuThreshold {
		// Eğer önceki alarm varsa ve zaten triggered durumundaysa, tekrar gönderme
		if exists && prevAlarm.Status == "triggered" {
			return
		}

		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "system_cpu_usage",
			MetricValue: fmt.Sprintf("%.2f", cpuUsage),
			Message:     fmt.Sprintf("High CPU usage detected: %.2f%% (threshold: %.2f%%)", cpuUsage, m.thresholds.CpuThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "warning",
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("CPU usage alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("CPU usage alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if exists && prevAlarm.Status == "triggered" {
		// CPU kullanımı normale döndü, resolved mesajı gönder
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "system_cpu_usage",
			MetricValue: fmt.Sprintf("%.2f", cpuUsage),
			Message:     fmt.Sprintf("CPU usage returned to normal: %.2f%% (threshold: %.2f%%)", cpuUsage, m.thresholds.CpuThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("CPU usage resolved mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("CPU usage resolved mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	}
}

// checkMemoryUsage memory kullanımını kontrol eder ve threshold'u aşarsa alarm üretir
func (m *AlarmMonitor) checkMemoryUsage() {
	if m.thresholds == nil || m.thresholds.MemoryThreshold == 0 {
		log.Printf("Memory threshold değeri ayarlanmamış")
		return
	}

	// Memory kullanımını al
	cmd := exec.Command("sh", "-c", "free | grep Mem | awk '{print ($3/$2) * 100}'")
	output, err := cmd.Output()
	if err != nil {
		log.Printf("Memory kullanımı alınamadı: %v", err)
		return
	}

	memStr := strings.TrimSpace(string(output))
	memUsage, err := strconv.ParseFloat(memStr, 64)
	if err != nil {
		log.Printf("Memory kullanımı parse edilemedi: %v", err)
		return
	}

	alarmKey := "system_memory_usage"

	// Rate limiting kontrolü
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 5 dakika içinde gönderilmişse, tekrar gönderme
	if exists {
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil && time.Since(prevTimestamp) < 5*time.Minute {
			prevTriggered := prevAlarm.Status == "triggered"
			currentHigh := memUsage >= m.thresholds.MemoryThreshold
			if prevTriggered == currentHigh {
				return
			}
		}
	}

	if memUsage >= m.thresholds.MemoryThreshold {
		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "system_memory_usage",
			MetricValue: fmt.Sprintf("%.2f", memUsage),
			Message:     fmt.Sprintf("High memory usage detected: %.2f%% (threshold: %.2f%%)", memUsage, m.thresholds.MemoryThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "warning",
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("Memory usage alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("Memory usage alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if exists && prevAlarm.Status == "triggered" {
		// Memory kullanımı normale döndü
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "system_memory_usage",
			MetricValue: fmt.Sprintf("%.2f", memUsage),
			Message:     fmt.Sprintf("Memory usage returned to normal: %.2f%% (threshold: %.2f%%)", memUsage, m.thresholds.MemoryThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("Memory usage resolved mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("Memory usage resolved mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	}
}

// checkDiskUsage disk kullanımını kontrol eder ve threshold'u aşarsa alarm üretir
func (m *AlarmMonitor) checkDiskUsage() {
	if m.thresholds == nil || m.thresholds.DiskThreshold == 0 {
		log.Printf("Disk threshold değeri ayarlanmamış")
		return
	}

	// PostgreSQL data dizinini bul
	dataDir := "/var/lib/postgresql" // varsayılan dizin
	configFile, err := postgres.FindPostgresConfigFile()
	if err == nil {
		// Konfigürasyon dosyasından data dizinini al
		content, err := os.ReadFile(configFile)
		if err == nil {
			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				if strings.Contains(line, "data_directory") {
					parts := strings.Split(line, "'")
					if len(parts) >= 2 {
						dataDir = strings.TrimSpace(parts[1])
						break
					}
				}
			}
		}
	}

	// Disk kullanımını kontrol et
	cmd := exec.Command("df", "-h", dataDir)
	output, err := cmd.Output()
	if err != nil {
		log.Printf("Disk kullanımı alınamadı: %v", err)
		return
	}

	// df çıktısını parse et (son satırı al)
	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		log.Printf("Disk kullanım bilgisi parse edilemedi")
		return
	}

	// Son satırı al ve kullanım yüzdesini çıkar
	fields := strings.Fields(lines[1])
	if len(fields) < 5 {
		log.Printf("Disk kullanım alanları parse edilemedi")
		return
	}

	// Yüzde işaretini kaldır
	usageStr := strings.TrimSuffix(fields[4], "%")
	diskUsage, err := strconv.ParseFloat(usageStr, 64)
	if err != nil {
		log.Printf("Disk kullanımı parse edilemedi: %v", err)
		return
	}

	alarmKey := "system_disk_usage"

	// Rate limiting kontrolü
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 5 dakika içinde gönderilmişse, tekrar gönderme
	if exists {
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil && time.Since(prevTimestamp) < 5*time.Minute {
			prevTriggered := prevAlarm.Status == "triggered"
			currentHigh := diskUsage >= m.thresholds.DiskThreshold
			if prevTriggered == currentHigh {
				return
			}
		}
	}

	if diskUsage >= m.thresholds.DiskThreshold {
		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "system_disk_usage",
			MetricValue: fmt.Sprintf("%.2f", diskUsage),
			Message:     fmt.Sprintf("High disk usage detected on %s: %.2f%% (threshold: %.2f%%)", dataDir, diskUsage, m.thresholds.DiskThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "warning",
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("Disk usage alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("Disk usage alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if exists && prevAlarm.Status == "triggered" {
		// Disk kullanımı normale döndü
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "system_disk_usage",
			MetricValue: fmt.Sprintf("%.2f", diskUsage),
			Message:     fmt.Sprintf("Disk usage returned to normal on %s: %.2f%% (threshold: %.2f%%)", dataDir, diskUsage, m.thresholds.DiskThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("Disk usage resolved mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("Disk usage resolved mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	}
}

// min iki sayıdan küçük olanı döndürür
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// getCPUUsage sistem CPU kullanımını yüzde olarak döndürür
func getCPUUsage() (float64, error) {
	// Linux için /proc/stat kullan
	if _, err := os.Stat("/proc/stat"); err == nil {
		// Linux sistemi
		cmd := exec.Command("sh", "-c", "top -bn1 | grep '%Cpu' | awk '{print $2}'")
		output, err := cmd.Output()
		if err != nil {
			return 0, fmt.Errorf("CPU kullanımı alınamadı: %v", err)
		}
		cpuStr := strings.TrimSpace(string(output))
		cpuUsage, err := strconv.ParseFloat(cpuStr, 64)
		if err != nil {
			return 0, fmt.Errorf("CPU kullanımı parse edilemedi: %v", err)
		}
		return cpuUsage, nil
	}

	// macOS için (fallback)
	cmd := exec.Command("sh", "-c", "top -l 1 | grep -E '^CPU' | awk '{print $3}' | tr -d '%'")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("CPU kullanımı alınamadı: %v", err)
	}
	cpuStr := strings.TrimSpace(string(output))
	cpuUsage, err := strconv.ParseFloat(cpuStr, 64)
	if err != nil {
		return 0, fmt.Errorf("CPU kullanımı parse edilemedi: %v", err)
	}
	return cpuUsage, nil
}

// reportAgentVersion agent'ın versiyon bilgilerini periyodik olarak raporlar
func (m *AlarmMonitor) reportAgentVersion() {
	// Her 1 saatte bir versiyon bilgisini gönder
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	// İlk çalıştırmada hemen gönder
	m.sendVersionInfo()

	for {
		select {
		case <-ticker.C:
			m.sendVersionInfo()
		case <-m.stopCh:
			return
		}
	}
}

// sendVersionInfo agent'ın versiyon ve platform bilgilerini server'a gönderir
func (m *AlarmMonitor) sendVersionInfo() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	versionInfo := &pb.AgentVersionInfo{
		Version:      AgentVersion,
		Platform:     runtime.GOOS,
		Architecture: runtime.GOARCH,
		Hostname:     hostname,
		OsVersion:    getOSVersion(),
		GoVersion:    runtime.Version(),
	}

	maxRetries := 3
	backoff := time.Second * 2

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		req := &pb.ReportVersionRequest{
			AgentId:     m.agentID,
			VersionInfo: versionInfo,
		}

		resp, err := m.client.ReportVersion(ctx, req)
		cancel()

		if err != nil {
			isConnectionError := strings.Contains(err.Error(), "connection") ||
				strings.Contains(err.Error(), "transport") ||
				strings.Contains(err.Error(), "Canceled") ||
				strings.Contains(err.Error(), "Deadline") ||
				strings.Contains(err.Error(), "context")

			if attempt < maxRetries-1 && isConnectionError {
				waitTime := backoff * time.Duration(attempt+1)
				log.Printf("Versiyon bilgisi gönderilemedi (deneme %d/%d): %v. %v sonra tekrar denenecek...",
					attempt+1, maxRetries, err, waitTime)
				time.Sleep(waitTime)
				continue
			}

			log.Printf("Versiyon bilgisi gönderilemedi (son deneme %d/%d): %v",
				attempt+1, maxRetries, err)
			return
		}

		log.Printf("Versiyon bilgisi başarıyla gönderildi: %s", resp.Status)
		return
	}
}

// getOSVersion işletim sistemi versiyonunu döndürür
func getOSVersion() string {
	if runtime.GOOS == "linux" {
		// Linux için /etc/os-release dosyasını oku
		content, err := os.ReadFile("/etc/os-release")
		if err == nil {
			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "VERSION_ID=") {
					return strings.Trim(strings.TrimPrefix(line, "VERSION_ID="), "\"")
				}
			}
		}
		return "unknown"
	}

	// Diğer sistemler için uname kullan
	cmd := exec.Command("uname", "-r")
	output, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}
