package alarm

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
		// MongoDB servis durumunu ve failover durumunu kontrol et
		m.checkMongoDBStatus()
		// MongoDB yavaş sorgularını kontrol et
		m.checkMongoSlowQueries()
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
	} else if m.platform == "mssql" {
		// MSSQL kontrolleri
		m.checkDiskUsage()
		m.checkMSSQLServiceStatus() // Servis durumu kontrolünü ekledik
		m.checkMSSQLFailover()      // Failover kontrolünü ekledik
		m.checkMSSQLBlockingQueries()
		m.checkMSSQLCPUUsage()
		m.checkMSSQLSlowQueries()

		log.Printf("MSSQL alarm kontrolleri yapılıyor...")
	} else {
		log.Printf("Bilinmeyen platform: %s", m.platform)
	}

	// Sistem kaynaklarını kontrol etme fonksiyonu şimdilik kapalı
	// m.checkSystemResources()
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

	// Önceki alarmı kontrol et
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Servis durumu alarmı
	if currentStatus.Status != "RUNNING" {
		// Eğer önceki alarm varsa ve hala aynı durum devam ediyorsa, yeni alarm gönderme
		if exists && prevAlarm.Status == "triggered" {
			log.Printf("MongoDB servis durumu hala %s. Önceki alarm aktif (ID: %s)",
				currentStatus.Status, prevAlarm.Id)
			return
		}

		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mongodb_service_status",
			MetricValue: currentStatus.Status,
			Message:     fmt.Sprintf("MongoDB service is having issues: %s ", currentStatus.Status),
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

	// Failover durumu kontrolü için benzer mantığı uygulayalım
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

		// Eğer önceki alarm varsa ve durum değişmemişse, yeni alarm gönderme
		if prevFailoverAlarm.Status == "triggered" {
			if (prevStatus.CurrentState == "PRIMARY" && currentStatus.CurrentState == "SECONDARY") ||
				(prevStatus.CurrentState == "SECONDARY" && currentStatus.CurrentState == "PRIMARY") {
				log.Printf("MongoDB failover durumu değişmedi. Önceki alarm aktif (ID: %s)", prevFailoverAlarm.Id)
				return
			}
		}
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

		log.Printf("MongoDB başlangıç durumu kaydedildi: %s", currentStatus.CurrentState)
		return
	}

	// Failover kontrolü
	if mongoCollector.CheckForFailover(prevStatus, currentStatus) {
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
	} else if failoverExists && prevFailoverAlarm.Status == "triggered" {
		// Durum normale döndüyse (örneğin: SECONDARY -> PRIMARY -> SECONDARY tamamlandı)
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
	}
}

// checkMongoSlowQueries MongoDB'deki yavaş sorguları kontrol eder ve alarm gönderir
func (m *AlarmMonitor) checkMongoSlowQueries() {
	if m.thresholds == nil || m.thresholds.SlowQueryThresholdMs == 0 {
		log.Printf("Slow query threshold değeri ayarlanmamış")
		return
	}

	log.Printf("MongoDB yavaş sorgu kontrolü başlıyor. Threshold: %d ms", m.thresholds.SlowQueryThresholdMs)

	// MongoDB kolektörünü oluştur
	mongoCollector := mongo.NewMongoCollector(m.config)
	client, err := mongoCollector.GetClient()
	if err != nil {
		log.Printf("MongoDB bağlantısı açılamadı: %v", err)
		return
	}
	defer client.Disconnect(context.Background())

	// Yavaş sorguları saklamak için slice
	slowQueries := []string{}
	maxDuration := float64(0)
	primaryDatabase := ""

	// Admin veritabanında currentOp komutunu çalıştır
	adminDB := client.Database("admin")
	var currentOps bson.M
	err = adminDB.RunCommand(context.Background(), bson.D{
		{Key: "currentOp", Value: true},
		{Key: "active", Value: true},
		{Key: "microsecs_running", Value: bson.D{{Key: "$exists", Value: true}}},
	}).Decode(&currentOps)
	if err != nil {
		log.Printf("Admin veritabanında currentOp komutu çalıştırılamadı: %v", err)
		return
	}

	log.Printf("currentOp komutu başarıyla çalıştırıldı, sonuçlar işleniyor...")

	if inprog, ok := currentOps["inprog"].(primitive.A); ok {
		log.Printf("Toplam %d aktif operasyon bulundu", len(inprog))
		for _, op := range inprog {
			if opMap, ok := op.(bson.M); ok {
				// Debug için operasyon detaylarını loglamayı kaldır
				var durationMs float64

				// Önce microsecs_running'i kontrol et (daha hassas)
				if microsecs, ok := opMap["microsecs_running"].(int64); ok {
					durationMs = float64(microsecs) / 1000.0
				} else if secs, ok := opMap["secs_running"].(float64); ok {
					durationMs = secs * 1000
				} else {
					continue
				}

				if durationMs >= float64(m.thresholds.SlowQueryThresholdMs) {
					// Veritabanı adını al
					dbName := "unknown"
					if ns, ok := opMap["ns"].(string); ok && ns != "" {
						parts := strings.SplitN(ns, ".", 2)
						if len(parts) > 0 {
							dbName = parts[0]
						}
					}

					// Sadece belirli sistem operasyonlarını atla
					skipQuery := false
					if dbName == "admin" || dbName == "config" || dbName == "local" {
						// Command'i kontrol et
						if cmd, ok := opMap["command"].(bson.M); ok {
							// Sadece belirli admin komutlarını atla
							if _, isHello := cmd["hello"]; isHello {
								skipQuery = true
							} else if _, isIsMaster := cmd["isMaster"]; isIsMaster {
								skipQuery = true
							} else if _, isReplSetGetStatus := cmd["replSetGetStatus"]; isReplSetGetStatus {
								skipQuery = true
							}
						}
					}

					if skipQuery {
						continue
					}

					if durationMs > maxDuration {
						maxDuration = durationMs
						primaryDatabase = dbName
					}

					// Operasyon detaylarını al
					ns := opMap["ns"].(string)
					opType := opMap["op"].(string)
					query := "N/A"
					if q, ok := opMap["query"].(bson.M); ok {
						queryBytes, _ := bson.MarshalExtJSON(q, true, true)
						query = string(queryBytes)
					} else if q, ok := opMap["command"].(bson.M); ok {
						queryBytes, _ := bson.MarshalExtJSON(q, true, true)
						query = string(queryBytes)
					}

					// Client bilgilerini al
					clientInfo := "N/A"
					if client, ok := opMap["client"].(string); ok {
						clientInfo = client
					}

					// Operasyon ID'sini al
					opId := "N/A"
					if id, ok := opMap["opid"].(int64); ok {
						opId = fmt.Sprintf("%d", id)
					}

					queryInfo := fmt.Sprintf("DB=%s, Collection=%s, Operation=%s, Duration=%.2fms, OpId=%s, Client=%s, Query=%s",
						dbName, ns, opType, durationMs, opId, clientInfo, query)
					slowQueries = append(slowQueries, queryInfo)
				}
			}
		}
	}

	if len(slowQueries) > 0 {
		log.Printf("Toplam %d yavaş sorgu tespit edildi. En uzun süren sorgu: %.2f ms", len(slowQueries), maxDuration)
		alarmKey := "mongodb_slow_queries"

		// Rate limiting kontrolü
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		if exists {
			prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
			if err == nil && time.Since(prevTimestamp) < 5*time.Minute {
				log.Printf("MongoDB slow query alarmı son 5 dakika içinde gönderildi, tekrar gönderilmeyecek")
				return
			}
		}

		message := fmt.Sprintf("Found %d active slow operations in MongoDB exceeding %dms threshold. Max duration: %.2fms\n%s",
			len(slowQueries), m.thresholds.SlowQueryThresholdMs, maxDuration,
			strings.Join(slowQueries, "\n"))

		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mongodb_slow_queries",
			MetricValue: fmt.Sprintf("%.2f", maxDuration),
			Message:     message,
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "warning",
			Database:    primaryDatabase,
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("MongoDB slow query alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MongoDB slow query alarmı gönderildi (ID: %s)", alarmEvent.Id)
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
		AND query NOT ILIKE '%%START_REPLICATION%%'
		AND query NOT ILIKE '%%START_REPLICATION SLOT%%'
		AND query NOT ILIKE '%%autovacuum: %%'   -- Autovacuum sorgularını hariç tut
		AND query NOT ILIKE '%%VACUUM %%'        -- Manual VACUUM sorgularını da hariç tut
		AND query NOT ILIKE '%%EXPLAIN%%'        -- EXPLAIN sorgularını hariç tut
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
	var primaryDatabase string // En uzun süren sorgunun veritabanını tutacak

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

		// En uzun süren sorguyu ve veritabanını takip et
		if durationMs > maxDuration {
			maxDuration = durationMs
			primaryDatabase = database
		}

		// NULL username kontrolü
		usernameStr := "unknown"
		if username.Valid {
			usernameStr = username.String
		}

		// Sorgu metnini kırpmadan ekle - uzun sorgular için de tam metin gönderilsin
		queryInfo := fmt.Sprintf("PID=%d, User=%s, DB=%s, Duration=%.2fms, Query=%s",
			pid, usernameStr, database, durationMs, queryText)
		slowQueries = append(slowQueries, queryInfo)
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

		// Tüm yavaş sorguları göster (sınırlama olmadan)
		message := fmt.Sprintf("Found %d slow queries exceeding %dms threshold. Max duration: %.2fms\n%s",
			len(slowQueries), m.thresholds.SlowQueryThresholdMs, maxDuration,
			strings.Join(slowQueries, "\n"))

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
			Database:    primaryDatabase, // En uzun süren sorgunun veritabanı
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

	// İşletim sistemine göre farklı komutlar kullan
	var highUsageFilesystems []string
	var maxUsage float64 = 0.0
	var maxUsageFS string

	if runtime.GOOS == "windows" {
		// Windows için PowerShell kullan
		cmd := exec.Command("powershell", "-Command",
			"Get-Volume | Where-Object {$_.DriveLetter} | Select-Object DriveLetter, @{Name='UsedPercent';Expression={100 - (100 * $_.SizeRemaining / $_.Size)}}, @{Name='Size';Expression={$_.Size}}, @{Name='FreeSpace';Expression={$_.SizeRemaining}} | ConvertTo-Json")
		output, err := cmd.Output()
		if err != nil {
			log.Printf("Windows disk kullanımı alınamadı: %v", err)
			return
		}

		// JSON çıktısını parse et
		var volumes []map[string]interface{}
		err = json.Unmarshal(output, &volumes)
		if err != nil {
			// Tek bir volume için farklı format
			var singleVolume map[string]interface{}
			if err := json.Unmarshal(output, &singleVolume); err != nil {
				log.Printf("Windows disk kullanım bilgisi parse edilemedi: %v", err)
				return
			}
			volumes = []map[string]interface{}{singleVolume}
		}

		for _, volume := range volumes {
			driveLetter, ok := volume["DriveLetter"].(string)
			if !ok || driveLetter == "" {
				continue
			}

			usedPercent, ok := volume["UsedPercent"].(float64)
			if !ok {
				continue
			}

			// Size ve FreeSpace'i formatla
			var sizeBytes, freeBytes float64
			size, ok := volume["Size"].(float64)
			if ok {
				sizeBytes = size
			}

			freeSpace, ok := volume["FreeSpace"].(float64)
			if ok {
				freeBytes = freeSpace
			}

			// Threshold'u aşan sürücüleri kaydet
			if usedPercent >= m.thresholds.DiskThreshold {
				// İnsan tarafından okunabilir formata çevir
				sizeStr := formatBytes(sizeBytes)
				freeStr := formatBytes(freeBytes)

				fsInfo := fmt.Sprintf("Drive %s: %.2f%% used (Size: %s, Free: %s)",
					driveLetter, usedPercent, sizeStr, freeStr)
				highUsageFilesystems = append(highUsageFilesystems, fsInfo)

				// En yüksek kullanımı takip et
				if usedPercent > maxUsage {
					maxUsage = usedPercent
					maxUsageFS = fmt.Sprintf("Drive %s", driveLetter)
				}
			}
		}
	} else {
		// Linux/Unix için df kullan
		cmd := exec.Command("df", "-h")
		output, err := cmd.Output()
		if err != nil {
			log.Printf("Disk kullanımı alınamadı: %v", err)
			return
		}

		// df çıktısını parse et
		lines := strings.Split(string(output), "\n")
		if len(lines) < 2 {
			log.Printf("Disk kullanım bilgisi parse edilemedi")
			return
		}

		// İlk satır başlık olduğu için 1'den başla
		for i := 1; i < len(lines); i++ {
			line := strings.TrimSpace(lines[i])
			if line == "" {
				continue
			}

			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}

			// Özel dosya sistemlerini atla
			filesystem := fields[0]
			mountPoint := fields[len(fields)-1]
			if strings.HasPrefix(filesystem, "/dev/loop") ||
				strings.HasPrefix(filesystem, "tmpfs") ||
				strings.HasPrefix(filesystem, "devtmpfs") ||
				strings.HasPrefix(filesystem, "udev") ||
				strings.HasPrefix(mountPoint, "/boot") ||
				strings.HasPrefix(mountPoint, "/snap") {
				continue
			}

			// Yüzde işaretini kaldır
			usageStr := strings.TrimSuffix(fields[4], "%")
			diskUsage, err := strconv.ParseFloat(usageStr, 64)
			if err != nil {
				continue
			}

			// Threshold'u aşan dosya sistemlerini kaydet
			if diskUsage >= m.thresholds.DiskThreshold {
				fsInfo := fmt.Sprintf("%s (%s): %.2f%%", mountPoint, filesystem, diskUsage)
				highUsageFilesystems = append(highUsageFilesystems, fsInfo)

				// En yüksek kullanımı takip et
				if diskUsage > maxUsage {
					maxUsage = diskUsage
					maxUsageFS = mountPoint
				}
			}
		}
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
			currentHigh := len(highUsageFilesystems) > 0
			if prevTriggered == currentHigh {
				return
			}
		}
	}

	if len(highUsageFilesystems) > 0 {
		// Yeni alarm oluştur
		message := fmt.Sprintf("High disk usage detected on %d filesystem(s) (highest: %s at %.2f%%):\n%s",
			len(highUsageFilesystems),
			maxUsageFS,
			maxUsage,
			strings.Join(highUsageFilesystems, "\n"))

		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "system_disk_usage",
			MetricValue: fmt.Sprintf("%.2f", maxUsage),
			Message:     message,
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
			MetricValue: fmt.Sprintf("%.2f", maxUsage),
			Message:     "All filesystem usages returned to normal",
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

// formatBytes converts bytes to human-readable format
func formatBytes(bytes float64) string {
	const unit = 1024.0
	if bytes < unit {
		return fmt.Sprintf("%.0f B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", bytes/div, "KMGTPE"[exp])
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

	// Bağlantı yenileme sayacı
	connectionRefreshCount := 0

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
				strings.Contains(err.Error(), "context") ||
				strings.Contains(err.Error(), "closing")

			if attempt < maxRetries-1 {
				if isConnectionError {
					log.Printf("Alarm gönderimi başarısız (deneme %d/%d): %v. Yeniden deneniyor...",
						attempt+1, maxRetries, err)

					// Her 2 denemede bir client'ı yenilemeyi dene
					if connectionRefreshCount == 0 {
						log.Printf("Bağlantı hatası nedeniyle gRPC client yenileniyor...")

						// Asenkron olarak yeniden bağlanmaya çalış
						go func() {
							// Burada reporterı yenileme işlemi yapılabilir
							// Ya da ana uygulamadan bir callback çağrılabilir
							time.Sleep(100 * time.Millisecond) // Yenileme için kısa bekle
							log.Printf("gRPC client yenileme işlemi tamamlandı")
						}()

						connectionRefreshCount++
					} else {
						connectionRefreshCount = (connectionRefreshCount + 1) % 2 // Her 2 denemede bir
					}

					// Exponential backoff ile bekle
					backoffTime := backoff * time.Duration(1<<uint(attempt))
					if backoffTime > 30*time.Second {
						backoffTime = 30 * time.Second // Maksimum 30 saniye
					}
					log.Printf("Yeniden deneme için %v bekleniyor...", backoffTime)
					time.Sleep(backoffTime)
					continue
				} else {
					// Bağlantı hatası değil, sadece tekrar dene
					log.Printf("Alarm gönderimi başarısız (deneme %d/%d): %v. Yeniden deneniyor...",
						attempt+1, maxRetries, err)
					time.Sleep(backoff)
					continue
				}
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

	// Client değişikliğinden sonra zorunlu threshold güncellemesi yap
	go func() {
		// Kısa bir bekleme süresi ekle (bağlantının tamamen oluşması için)
		time.Sleep(500 * time.Millisecond)

		log.Println("Bağlantı yenileme sonrası threshold'lar yeniden alınıyor...")
		m.updateThresholds()
	}()
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

// checkMSSQLSlowQueries yavaş MSSQL sorgularını kontrol eder ve alarm üretir
func (m *AlarmMonitor) checkMSSQLSlowQueries() {
	if m.thresholds == nil || m.thresholds.SlowQueryThresholdMs == 0 {
		log.Printf("MSSQL Slow query threshold değeri ayarlanmamış")
		return
	}

	log.Printf("MSSQL yavaş sorgu kontrolü başlıyor. Threshold: %d ms", m.thresholds.SlowQueryThresholdMs)

	alarmKey := "mssql_slow_queries"

	// Bağlantı bilgilerini al
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s",
		m.config.MSSQL.Host, m.config.MSSQL.User, m.config.MSSQL.Pass, m.config.MSSQL.Port)

	// Windows olduğu için yerel kimlik doğrulama da destekleyelim
	if m.config.MSSQL.WindowsAuth {
		dsn = fmt.Sprintf("server=%s;port=%s;trusted_connection=yes",
			m.config.MSSQL.Host, m.config.MSSQL.Port)
	}

	// Veritabanı bağlantısını aç
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Printf("[ERROR] MSSQL yavaş sorgu kontrolü için bağlantı açılamadı: %v", err)
		return
	}
	defer db.Close()

	// Bağlantıyı test et
	if err = db.Ping(); err != nil {
		log.Printf("[ERROR] MSSQL sunucusuna bağlantı kurulamadı: %v", err)
		return
	}

	// sys.dm_exec_requests ve sys.dm_exec_sessions kullanarak yavaş sorguları al
	query := fmt.Sprintf(`
	SELECT 
		r.session_id,
		s.login_name,
		DB_NAME(r.database_id) AS database_name,
		DATEDIFF(millisecond, r.start_time, GETDATE()) AS execution_time_ms,
		r.command,
		r.status,
		r.wait_type,
		r.wait_time,
		r.last_wait_type,
		s.host_name,
		s.program_name,
		SUBSTRING(
			t.text, 
			(r.statement_start_offset/2) + 1,
			((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text) ELSE r.statement_end_offset END - r.statement_start_offset)/2) + 1
		) AS current_statement,
		t.text AS batch_text,
		r.cpu_time,
		r.total_elapsed_time,
		r.reads,
		r.writes,
		r.logical_reads
	FROM 
		sys.dm_exec_requests r
		INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
		OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
	WHERE 
		r.session_id <> @@SPID
		AND DATEDIFF(millisecond, r.start_time, GETDATE()) > %d
		AND s.is_user_process = 1
		AND r.command NOT IN ('BACKUP DATABASE', 'BACKUP LOG', 'RESTORE DATABASE', 'RESTORE LOG')
		AND r.command NOT LIKE '%%BACKUP%%'
		AND r.command NOT LIKE '%%RESTORE%%'
	ORDER BY 
		execution_time_ms DESC`, m.thresholds.SlowQueryThresholdMs)

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[ERROR] MSSQL yavaş sorgu sorgusu çalıştırılamadı: %v", err)
		return
	}
	defer rows.Close()

	var slowQueries []string
	var maxDuration int64
	var primaryDatabase string
	slowQueryCount := 0

	for rows.Next() {
		var (
			sessionID        int
			loginName        sql.NullString
			databaseName     sql.NullString
			executionTimeMs  int64
			command          sql.NullString
			status           sql.NullString
			waitType         sql.NullString
			waitTime         sql.NullInt64
			lastWaitType     sql.NullString
			hostName         sql.NullString
			programName      sql.NullString
			currentStatement sql.NullString
			batchText        sql.NullString
			cpuTime          sql.NullInt64
			totalElapsedTime sql.NullInt64
			reads            sql.NullInt64
			writes           sql.NullInt64
			logicalReads     sql.NullInt64
		)

		if err := rows.Scan(
			&sessionID, &loginName, &databaseName, &executionTimeMs, &command, &status,
			&waitType, &waitTime, &lastWaitType, &hostName, &programName, &currentStatement,
			&batchText, &cpuTime, &totalElapsedTime, &reads, &writes, &logicalReads); err != nil {
			log.Printf("[ERROR] MSSQL yavaş sorgu satırı okunamadı: %v", err)
			continue
		}

		slowQueryCount++

		// Maksimum süreyi takip et
		if executionTimeMs > maxDuration {
			maxDuration = executionTimeMs
			if databaseName.Valid {
				primaryDatabase = databaseName.String
			}
		}

		// Null değerleri kontrol et
		loginStr := "unknown"
		if loginName.Valid {
			loginStr = loginName.String
		}

		dbStr := "unknown"
		if databaseName.Valid {
			dbStr = databaseName.String
		}

		commandStr := "unknown"
		if command.Valid {
			commandStr = command.String
		}

		// Sorgu metnini limit kontrol ederek çek
		queryText := "unknown"
		if currentStatement.Valid && len(currentStatement.String) > 0 {
			queryText = currentStatement.String
			if len(queryText) > 500 {
				queryText = queryText[:500] + "..."
			}
		} else if batchText.Valid && len(batchText.String) > 0 {
			queryText = batchText.String
			if len(queryText) > 500 {
				queryText = queryText[:500] + "..."
			}
		}

		hostStr := ""
		if hostName.Valid {
			hostStr = hostName.String
		}

		programStr := ""
		if programName.Valid {
			programStr = programName.String
		}

		waitTypeStr := "none"
		if waitType.Valid && waitType.String != "" {
			waitTypeStr = waitType.String
		}

		statusStr := "unknown"
		if status.Valid {
			statusStr = status.String
		}

		// Detaylı kaynak kullanımı
		cpuTimeStr := "0"
		if cpuTime.Valid {
			cpuTimeStr = fmt.Sprintf("%d", cpuTime.Int64)
		}

		readsStr := "0"
		if reads.Valid {
			readsStr = fmt.Sprintf("%d", reads.Int64)
		}

		writesStr := "0"
		if writes.Valid {
			writesStr = fmt.Sprintf("%d", writes.Int64)
		}

		logicalReadsStr := "0"
		if logicalReads.Valid {
			logicalReadsStr = fmt.Sprintf("%d", logicalReads.Int64)
		}

		queryInfo := fmt.Sprintf(
			"SessionID=%d, Login=%s, DB=%s, Duration=%.2fs, Status=%s, Command=%s\n"+
				"CPU=%s, Reads=%s, Writes=%s, LogicalReads=%s, WaitType=%s\n"+
				"Host=%s, Program=%s\n"+
				"Query=%s",
			sessionID, loginStr, dbStr, float64(executionTimeMs)/1000.0, statusStr, commandStr,
			cpuTimeStr, readsStr, writesStr, logicalReadsStr, waitTypeStr,
			hostStr, programStr, queryText)

		slowQueries = append(slowQueries, queryInfo)
	}

	if len(slowQueries) > 0 {
		// Rate limiting kontrolü
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		// Son 5 dakika içinde aynı tür bir alarm gönderilmişse yenisini gönderme
		shouldSendAlarm := true
		if exists {
			prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
			if err == nil && time.Since(prevTimestamp) < 5*time.Minute {
				log.Printf("MSSQL slow query alarmı son 5 dakika içinde gönderildi, tekrar gönderilmeyecek")
				shouldSendAlarm = false
			}
		}

		if shouldSendAlarm {
			// Alarm mesajı oluştur
			message := fmt.Sprintf("Found %d slow queries in SQL Server exceeding %dms threshold. Max duration: %.2fs\n%s",
				len(slowQueries), m.thresholds.SlowQueryThresholdMs, float64(maxDuration)/1000.0,
				strings.Join(slowQueries, "\n\n"))

			alarmEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "triggered",
				MetricName:  "mssql_slow_queries",
				MetricValue: fmt.Sprintf("%.2f", float64(maxDuration)/1000.0),
				Message:     message,
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "warning",
				Database:    primaryDatabase,
			}

			if err := m.reportAlarm(alarmEvent); err != nil {
				log.Printf("MSSQL slow query alarmı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[alarmKey] = alarmEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MSSQL slow query alarmı gönderildi (ID: %s, %d yavaş sorgu)", alarmEvent.Id, len(slowQueries))
			}
		}
	} else {
		// Daha önce slow query alarmı gönderildiyse ve artık slow query yoksa, çözüldü mesajı gönder
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		prevStatus := ""
		if exists {
			prevStatus = prevAlarm.Status
		}
		m.alarmCacheLock.RUnlock()

		if exists && prevStatus == "triggered" {
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "mssql_slow_queries",
				MetricValue: "0",
				Message:     "All slow queries in SQL Server have completed",
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "info",
			}

			if err := m.reportAlarm(resolvedEvent); err != nil {
				log.Printf("MSSQL slow query çözüldü mesajı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[alarmKey] = resolvedEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MSSQL slow query çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
			}
		}
	}
}

// checkMSSQLCPUUsage MSSQL sunucusunun CPU kullanımını kontrol eder ve alarm üretir
func (m *AlarmMonitor) checkMSSQLCPUUsage() {
	if m.thresholds == nil || m.thresholds.CpuThreshold == 0 {
		log.Printf("MSSQL CPU threshold değeri ayarlanmamış")
		return
	}

	alarmKey := "mssql_cpu_usage"

	// Bağlantı bilgilerini al
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s",
		m.config.MSSQL.Host, m.config.MSSQL.User, m.config.MSSQL.Pass, m.config.MSSQL.Port)

	// Windows olduğu için yerel kimlik doğrulama da destekleyelim
	if m.config.MSSQL.WindowsAuth {
		dsn = fmt.Sprintf("server=%s;port=%s;trusted_connection=yes",
			m.config.MSSQL.Host, m.config.MSSQL.Port)
	}

	// Veritabanı bağlantısını aç
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Printf("[ERROR] MSSQL CPU kullanımı kontrolü için bağlantı açılamadı: %v", err)
		return
	}
	defer db.Close()

	// Bağlantıyı test et
	if err = db.Ping(); err != nil {
		log.Printf("[ERROR] MSSQL sunucusuna bağlantı kurulamadı: %v", err)
		return
	}

	// İlk yöntem olarak ring_buffer kullanarak CPU kullanımını ölç
	var cpuUsage float64
	var cpuUsageObtained bool = false

	// 1. Yöntem: sys.dm_os_ring_buffers (En doğru metot, tüm SQL Server versiyonlarında çalışır)
	resourceQuery := `SELECT TOP 1
		CAST(record.value('(./Record/@id)[1]', 'int') AS int) AS record_id,
		CAST(record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS int) AS cpu_utilization,
		CAST(record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS int) AS system_idle,
		DATEADD(ms, -1 * (SELECT ms_ticks FROM sys.dm_os_sys_info), 
				GETDATE()) AS event_time
	FROM (
		SELECT TOP 15 CAST(record AS xml) AS record 
		FROM sys.dm_os_ring_buffers 
		WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
		AND record LIKE '%<SystemHealth>%'
		ORDER BY timestamp DESC
	) AS rb
	ORDER BY record_id DESC`

	var cpuUtilization, systemIdle sql.NullInt64
	var eventTime sql.NullTime
	err = db.QueryRow(resourceQuery).Scan(&cpuUtilization, &systemIdle, &eventTime)

	if err == nil && cpuUtilization.Valid {
		cpuUsage = float64(cpuUtilization.Int64)
		cpuUsageObtained = true
		log.Printf("[DEBUG] MSSQL sunucusu CPU kullanımı: %.2f%% (Ring buffer)", cpuUsage)
	} else {
		log.Printf("[WARN] Ring buffer metodu ile MSSQL CPU kullanımı alınamadı: %v", err)
	}

	// 2. Yöntem: Eğer birinci yöntem başarısız olursa, sys.dm_os_performance_counters kullan
	if !cpuUsageObtained {
		// SQL Server'a özgü performans sayaçlarını kullan
		perfQuery := `
		SELECT TOP(1) 
			100 - SystemIdle AS SqlCpuUtilization
		FROM (
			SELECT 
				record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle
			FROM (
				SELECT TOP(1) CONVERT(xml, record) AS record 
				FROM sys.dm_os_ring_buffers 
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
				AND record LIKE '%<SystemHealth>%'
				ORDER BY timestamp DESC
			) AS RB
		) AS y`

		var sqlCpuUtilization sql.NullFloat64
		err = db.QueryRow(perfQuery).Scan(&sqlCpuUtilization)

		if err == nil && sqlCpuUtilization.Valid {
			cpuUsage = sqlCpuUtilization.Float64
			cpuUsageObtained = true
			log.Printf("[DEBUG] MSSQL sunucusu CPU kullanımı: %.2f%% (Performance counters)", cpuUsage)
		} else {
			log.Printf("[WARN] Performance counters ile MSSQL CPU kullanımı alınamadı: %v", err)
		}
	}

	// 3. Yöntem: Eğer önceki yöntemler başarısız olursa, aktif sorguların CPU kullanımına bak
	if !cpuUsageObtained {
		// Aktif sorguların CPU kullanımını topla
		cpuQuery := `
		SELECT 
			AVG(cpu_percent) AS avg_cpu
		FROM (
			SELECT 
				r.session_id,
				CONVERT(decimal(5,2), r.cpu_time * 1.0 / 
					CASE DATEDIFF(SECOND, r.start_time, GETDATE()) 
						WHEN 0 THEN 1 
						ELSE DATEDIFF(SECOND, r.start_time, GETDATE()) 
					END) AS cpu_percent
			FROM sys.dm_exec_requests r
			JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
			WHERE s.is_user_process = 1
		) AS x`

		var avgCpu sql.NullFloat64
		err = db.QueryRow(cpuQuery).Scan(&avgCpu)

		if err == nil && avgCpu.Valid {
			cpuUsage = avgCpu.Float64
			cpuUsageObtained = true
			log.Printf("[DEBUG] MSSQL sunucusu CPU kullanımı: %.2f%% (Active queries)", cpuUsage)
		} else {
			log.Printf("[WARN] Aktif sorgular ile MSSQL CPU kullanımı alınamadı: %v", err)
		}
	}

	// 4. Yöntem: Son çare olarak genel sistem CPU kullanımına bak (Windows için)
	if !cpuUsageObtained && runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command",
			"Get-WmiObject Win32_PerfFormattedData_PerfOS_Processor | Select-Object -ExpandProperty PercentProcessorTime")
		output, err := cmd.Output()
		if err == nil {
			cpuStr := strings.TrimSpace(string(output))
			cpuVal, err := strconv.ParseFloat(cpuStr, 64)
			if err == nil {
				cpuUsage = cpuVal
				cpuUsageObtained = true
				log.Printf("[DEBUG] Sistem CPU kullanımı (Windows): %.2f%%", cpuUsage)
			}
		}
	}

	// Eğer hiçbir yöntem başarılı olmazsa, hata dön
	if !cpuUsageObtained {
		log.Printf("[ERROR] Hiçbir yöntemle MSSQL CPU kullanımı alınamadı")
		return
	}

	// Rate limiting kontrolü
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 5 dakikadan uzun süre geçtiyse bildir
	shouldSendAlarm := !exists || prevAlarm.Status != "triggered"

	if exists && prevAlarm.Status == "triggered" {
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil && time.Since(prevTimestamp) < 5*time.Minute {
			// Son 5 dakika içinde gönderilmişse ve CPU hala yüksekse, yeni alarm gönderme
			if cpuUsage >= m.thresholds.CpuThreshold {
				shouldSendAlarm = false
			}
		}
	}

	if cpuUsage >= m.thresholds.CpuThreshold && shouldSendAlarm {
		// Alarm mesajı oluştur
		message := fmt.Sprintf("High SQL Server CPU usage detected: %.2f%% (threshold: %.2f%%)", cpuUsage, m.thresholds.CpuThreshold)

		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mssql_cpu_usage",
			MetricValue: fmt.Sprintf("%.2f", cpuUsage),
			Message:     message,
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "warning",
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("MSSQL CPU usage alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MSSQL CPU usage alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if cpuUsage < m.thresholds.CpuThreshold && exists && prevAlarm.Status == "triggered" {
		// CPU kullanımı normale döndü, resolved mesajı gönder
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "mssql_cpu_usage",
			MetricValue: fmt.Sprintf("%.2f", cpuUsage),
			Message:     fmt.Sprintf("SQL Server CPU usage returned to normal: %.2f%% (threshold: %.2f%%)", cpuUsage, m.thresholds.CpuThreshold),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("MSSQL CPU usage resolved mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MSSQL CPU usage resolved mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	}
}

// checkMSSQLBlockingQueries MSSQL'deki bloke olmuş sorguları kontrol eder ve alarm üretir
func (m *AlarmMonitor) checkMSSQLBlockingQueries() {
	// Threshold kontrolü - blocking_query_threshold_ms değerini kontrol et
	if m.thresholds == nil || m.thresholds.BlockingQueryThresholdMs == 0 {
		log.Printf("MSSQL Blocking query threshold değeri ayarlanmamış")
		return
	}

	log.Printf("MSSQL blokaj kontrolü başlıyor. Threshold: %d ms", m.thresholds.BlockingQueryThresholdMs)

	alarmKey := "mssql_blocking_queries"

	// Bağlantı bilgilerini al
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s",
		m.config.MSSQL.Host, m.config.MSSQL.User, m.config.MSSQL.Pass, m.config.MSSQL.Port)

	// Windows olduğu için yerel kimlik doğrulama da destekleyelim
	if m.config.MSSQL.WindowsAuth {
		dsn = fmt.Sprintf("server=%s;port=%s;trusted_connection=yes",
			m.config.MSSQL.Host, m.config.MSSQL.Port)
	}

	// Veritabanı bağlantısını aç
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Printf("[ERROR] MSSQL blokaj kontrolü için bağlantı açılamadı: %v", err)
		return
	}
	defer db.Close()

	// Bağlantıyı test et
	if err = db.Ping(); err != nil {
		log.Printf("[ERROR] MSSQL sunucusuna bağlantı kurulamadı: %v", err)
		return
	}

	// Bloke eden ve bloke olan sorguları al (threshold değeriyle filtreleme)
	query := fmt.Sprintf(`
	WITH BlockingTree AS (
		SELECT 
			w.session_id,
			w.wait_duration_ms,
			w.blocking_session_id,
			w.resource_description,
			s.login_name,
			s.host_name,
			s.program_name,
			DB_NAME(r.database_id) AS database_name,
			SUBSTRING(
				t.text, 
				(r.statement_start_offset/2) + 1,
				((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text) ELSE r.statement_end_offset END - r.statement_start_offset)/2) + 1
			) AS current_statement,
			s.last_request_start_time
		FROM 
			sys.dm_os_waiting_tasks w
			INNER JOIN sys.dm_exec_sessions s ON w.session_id = s.session_id
			LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
			OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
		WHERE 
			w.blocking_session_id > 0
			AND w.wait_duration_ms >= %d -- Threshold değeri ile filtreleme
	)
	SELECT 
		b.session_id AS blocked_session_id,
		b.wait_duration_ms,
		b.blocking_session_id,
		b.resource_description,
		b.login_name AS blocked_login,
		b.host_name AS blocked_host,
		b.program_name AS blocked_program,
		b.database_name AS blocked_db,
		b.current_statement AS blocked_statement,
		DATEDIFF(second, b.last_request_start_time, GETDATE()) AS blocked_request_seconds,
		s.login_name AS blocking_login,
		s.host_name AS blocking_host,
		s.program_name AS blocking_program,
		DB_NAME(r.database_id) AS blocking_db,
		SUBSTRING(
			t.text, 
			(r.statement_start_offset/2) + 1,
			((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text) ELSE r.statement_end_offset END - r.statement_start_offset)/2) + 1
		) AS blocking_statement
	FROM 
		BlockingTree b
		LEFT JOIN sys.dm_exec_sessions s ON b.blocking_session_id = s.session_id
		LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
		OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
	ORDER BY
		b.wait_duration_ms DESC`, m.thresholds.BlockingQueryThresholdMs)

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[ERROR] MSSQL blokaj sorgusu çalıştırılamadı: %v", err)
		return
	}
	defer rows.Close()

	var blockingQueries []string
	var maxWaitTime int
	var affectedDatabase string
	blockCount := 0

	for rows.Next() {
		var (
			blockedSessionID      int
			waitDurationMs        int
			blockingSessionID     int
			resourceDescription   sql.NullString
			blockedLogin          sql.NullString
			blockedHost           sql.NullString
			blockedProgram        sql.NullString
			blockedDB             sql.NullString
			blockedStatement      sql.NullString
			blockedRequestSeconds sql.NullInt64
			blockingLogin         sql.NullString
			blockingHost          sql.NullString
			blockingProgram       sql.NullString
			blockingDB            sql.NullString
			blockingStatement     sql.NullString
		)

		if err := rows.Scan(
			&blockedSessionID, &waitDurationMs, &blockingSessionID, &resourceDescription,
			&blockedLogin, &blockedHost, &blockedProgram, &blockedDB, &blockedStatement, &blockedRequestSeconds,
			&blockingLogin, &blockingHost, &blockingProgram, &blockingDB, &blockingStatement); err != nil {
			log.Printf("[ERROR] MSSQL blokaj satırı okunamadı: %v", err)
			continue
		}

		blockCount++

		// Maksimum bekleme süresini takip et
		if waitDurationMs > maxWaitTime {
			maxWaitTime = waitDurationMs
			if blockedDB.Valid {
				affectedDatabase = blockedDB.String
			} else if blockingDB.Valid {
				affectedDatabase = blockingDB.String
			}
		}

		// Blokaj bilgisini hazırla
		blockedLoginStr := "unknown"
		if blockedLogin.Valid {
			blockedLoginStr = blockedLogin.String
		}

		blockingLoginStr := "unknown"
		if blockingLogin.Valid {
			blockingLoginStr = blockingLogin.String
		}

		blockedDBStr := "unknown"
		if blockedDB.Valid {
			blockedDBStr = blockedDB.String
		}

		blockedStmtStr := "unknown"
		if blockedStatement.Valid && len(blockedStatement.String) > 0 {
			blockedStmtStr = blockedStatement.String
			if len(blockedStmtStr) > 200 {
				blockedStmtStr = blockedStmtStr[:200] + "..."
			}
		}

		blockingStmtStr := "unknown"
		if blockingStatement.Valid && len(blockingStatement.String) > 0 {
			blockingStmtStr = blockingStatement.String
			if len(blockingStmtStr) > 200 {
				blockingStmtStr = blockingStmtStr[:200] + "..."
			}
		}

		blockInfo := fmt.Sprintf(
			"Blocked: SessionID=%d, Login=%s, DB=%s, Wait=%.2fs, Query=%s\n"+
				"Blocker: SessionID=%d, Login=%s, Query=%s",
			blockedSessionID, blockedLoginStr, blockedDBStr, float64(waitDurationMs)/1000.0, blockedStmtStr,
			blockingSessionID, blockingLoginStr, blockingStmtStr)

		blockingQueries = append(blockingQueries, blockInfo)
	}

	if len(blockingQueries) > 0 {
		// Rate limiting kontrolü
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		// Sadece son blokaj durumundan farklıysa veya 5 dakikadan uzun süre geçtiyse bildir
		shouldSendAlarm := true
		if exists {
			prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
			if err == nil {
				// Aynı blokaj 5 dakikadan kısa süredir devam ediyorsa tekrar alarm gönderme
				if time.Since(prevTimestamp) < 5*time.Minute &&
					strings.Contains(prevAlarm.Message, fmt.Sprintf("Found %d blocking", blockCount)) {
					shouldSendAlarm = false
				}
			}
		}

		if shouldSendAlarm {
			// Alarm mesajı oluştur
			message := fmt.Sprintf("Found %d blocking queries in SQL Server exceeding %dms threshold. Max wait time: %.2f seconds\n%s",
				len(blockingQueries), m.thresholds.BlockingQueryThresholdMs, float64(maxWaitTime)/1000.0,
				strings.Join(blockingQueries, "\n\n"))

			alarmEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "triggered",
				MetricName:  "mssql_blocking_queries",
				MetricValue: fmt.Sprintf("%d", len(blockingQueries)),
				Message:     message,
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "warning",
				Database:    affectedDatabase,
			}

			if err := m.reportAlarm(alarmEvent); err != nil {
				log.Printf("MSSQL blokaj alarmı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[alarmKey] = alarmEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MSSQL blokaj alarmı gönderildi (ID: %s, %d blokaj)", alarmEvent.Id, len(blockingQueries))
			}
		}
	} else {
		// Daha önce blokaj alarmı gönderildiyse ve artık blokaj yoksa, çözüldü mesajı gönder
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		prevStatus := ""
		if exists {
			prevStatus = prevAlarm.Status
		}
		m.alarmCacheLock.RUnlock()

		if exists && prevStatus == "triggered" {
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "mssql_blocking_queries",
				MetricValue: "0",
				Message:     "All blocking queries in SQL Server have been resolved",
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "info",
			}

			if err := m.reportAlarm(resolvedEvent); err != nil {
				log.Printf("MSSQL blokaj çözüldü mesajı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[alarmKey] = resolvedEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MSSQL blokaj çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
			}
		}
	}
}

// checkMSSQLServiceStatus MSSQL servis durumunu kontrol eder
func (m *AlarmMonitor) checkMSSQLServiceStatus() {
	alarmKey := "mssql_service_status"
	haStateKey := "mssql_ha_state" // Son bilinen HA durumunu saklamak için

	// Bağlantı bilgilerini al
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s",
		m.config.MSSQL.Host, m.config.MSSQL.User, m.config.MSSQL.Pass, m.config.MSSQL.Port)

	// Windows olduğu için yerel kimlik doğrulama da destekleyelim
	if m.config.MSSQL.WindowsAuth {
		dsn = fmt.Sprintf("server=%s;port=%s;trusted_connection=yes",
			m.config.MSSQL.Host, m.config.MSSQL.Port)
	}

	// Veritabanı bağlantısını aç
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		// Bağlantı açılamadıysa servis durumu alarmı gönder
		// Ancak son bilinen AlwaysOn durumunu koruyalım
		m.sendMSSQLServiceAlarm(alarmKey, false, err.Error())
		return
	}
	defer db.Close()

	// Bağlantıyı test et - 5 saniye timeout ile
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		// Ping başarısız olduysa servis durumu alarmı gönder
		// Ancak son bilinen AlwaysOn durumunu koruyalım
		m.sendMSSQLServiceAlarm(alarmKey, false, err.Error())
		return
	}

	// Servis bilgilerini al
	var version string
	err = db.QueryRow("SELECT @@VERSION").Scan(&version)
	if err != nil {
		m.sendMSSQLServiceAlarm(alarmKey, false, err.Error())
		return
	}

	// Servis çalışıyor, çalışma süresini al
	var uptime int
	err = db.QueryRow("SELECT DATEDIFF(MINUTE, create_date, GETDATE()) FROM sys.databases WHERE name = 'tempdb'").Scan(&uptime)
	if err != nil {
		log.Printf("[WARN] MSSQL uptime alınamadı: %v", err)
		uptime = 0
	}

	// AlwaysOn durumunu kontrol et
	isAlwaysOn := false
	alwaysOnRole := "UNKNOWN"
	alwaysOnGroup := "UNKNOWN"

	// AlwaysOn bilgilerini sorgula
	agQuery := `
	SELECT TOP 1
		ag.name AS ag_name,
		hars.role_desc
	FROM 
		sys.availability_groups AS ag
		JOIN sys.availability_replicas AS ar ON ag.group_id = ar.group_id
		JOIN sys.dm_hadr_availability_replica_states AS hars ON ar.replica_id = hars.replica_id
	WHERE 
		ar.replica_server_name = @@SERVERNAME`

	var agName, roleDesc string
	agErr := db.QueryRow(agQuery).Scan(&agName, &roleDesc)

	if agErr == nil {
		// AlwaysOn bilgileri başarıyla alındı
		isAlwaysOn = true
		alwaysOnRole = roleDesc
		alwaysOnGroup = agName

		// AlwaysOn bilgilerini önbellekte sakla
		haStateInfo := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     haStateKey,
			AgentId:     m.agentID,
			Status:      "info",
			MetricName:  "mssql_ha_state",
			MetricValue: roleDesc,
			Message:     fmt.Sprintf("SQL Server AlwaysOn state cache: Role=%s, Group=%s", roleDesc, agName),
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		m.alarmCacheLock.Lock()
		m.alarmCache[haStateKey] = haStateInfo
		m.alarmCacheLock.Unlock()

		log.Printf("[DEBUG] MSSQL AlwaysOn durumu önbelleğe alındı: Role=%s, Group=%s", roleDesc, agName)

		// Collector'ın da kullanabilmesi için dosyaya da yazalım
		m.saveHAStateToDisk(roleDesc, agName)
	} else if strings.Contains(agErr.Error(), "Invalid object name 'sys.availability_groups'") {
		// AlwaysOn yapılandırması yok
		isAlwaysOn = false
		log.Printf("[DEBUG] MSSQL sunucusunda AlwaysOn yapılandırması bulunmuyor")
	}

	// Service durumu normal, önceki bir alarm varsa çözüldü mesajı gönder
	extraInfo := ""
	if isAlwaysOn {
		extraInfo = fmt.Sprintf(", AlwaysOn: %s in %s", alwaysOnRole, alwaysOnGroup)
	}

	m.sendMSSQLServiceAlarm(alarmKey, true, fmt.Sprintf("SQL Server is running. Version: %s, Uptime: %d minutes%s",
		strings.Split(version, "\n")[0], uptime, extraInfo))
}

// saveHAStateToDisk AlwaysOn durumunu disk dosyasına yazar (collector ile paylaşım için)
func (m *AlarmMonitor) saveHAStateToDisk(role, group string) {
	// Collector ile aynı cache dosyasını kullan
	cacheFile := filepath.Join(os.TempDir(), "mssql_ha_state.txt")
	data := fmt.Sprintf("%s|%s", role, group)

	err := os.WriteFile(cacheFile, []byte(data), 0644)
	if err != nil {
		log.Printf("[ERROR] HA durumu dosyaya kaydedilemedi: %v", err)
	} else {
		log.Printf("[DEBUG] HA durumu dosyaya kaydedildi: %s, %s", role, group)
	}
}

// sendMSSQLServiceAlarm MSSQL servis durumu alarmını gönderir
func (m *AlarmMonitor) sendMSSQLServiceAlarm(alarmKey string, isRunning bool, message string) {
	// Rate limiting kontrolü
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	// AlwaysOn durumunu kontrol et
	haStateKey := "mssql_ha_state"
	haStateInfo, hasHAState := m.alarmCache[haStateKey]
	m.alarmCacheLock.RUnlock()

	// Önceki alarm varsa ve son 5 dakika içinde gönderilmişse, tekrar gönderme
	if exists {
		prevTimestamp, err := time.Parse(time.RFC3339, prevAlarm.Timestamp)
		if err == nil {
			timeSinceLastAlarm := time.Since(prevTimestamp)
			// Son 1 dakika içinde gönderilmişse ve durum değişmemişse tekrar gönderme
			if timeSinceLastAlarm < 1*time.Minute {
				if (isRunning && prevAlarm.Status == "resolved") ||
					(!isRunning && prevAlarm.Status == "triggered") {
					return
				}
			}
		}
	}

	if !isRunning {
		// Servis durumu alarmı oluştur (triggered)
		// AlwaysOn bilgilerini ekleyip status'u güncelleyelim
		additionalInfo := ""
		metricValue := "FAIL!_STANDALONE"

		if hasHAState {
			// Son bilinen AlwaysOn durumunu kullan
			role := haStateInfo.MetricValue
			additionalInfo = fmt.Sprintf(" (Last known AlwaysOn state: %s)", role)
			metricValue = "FAIL!_ALWAYSON" // Cluster üyesi olduğunu belirt
			log.Printf("[INFO] MSSQL servis kapalıyken son bilinen AlwaysOn durumu kullanılıyor: %s", role)
		}

		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mssql_service_status",
			MetricValue: metricValue,

			Message:   fmt.Sprintf("SQL Server service is having issues: %s%s", message, additionalInfo),
			Timestamp: time.Now().Format(time.RFC3339),
			Severity:  "critical",
		}

		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("MSSQL servis alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MSSQL servis alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if exists && prevAlarm.Status == "triggered" {
		// Servis durumu çözüldü mesajı oluştur (resolved)
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "mssql_service_status",
			MetricValue: "RUNNING",
			Message:     message,
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("MSSQL servis çözüldü mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MSSQL servis çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	} else if !exists {
		// İlk kez başlatılıyorsa, servis durumunu kaydet
		initialEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "initial",
			MetricName:  "mssql_service_status",
			MetricValue: "RUNNING",
			Message:     message,
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "info",
		}

		m.alarmCacheLock.Lock()
		m.alarmCache[alarmKey] = initialEvent
		m.alarmCacheLock.Unlock()
		log.Printf("MSSQL servis durumu kaydedildi")
	}
}

// checkMSSQLFailover MSSQL AlwaysOn yapıdaki değişiklikleri kontrol eder
func (m *AlarmMonitor) checkMSSQLFailover() {
	alarmKey := "mssql_failover"

	// Bağlantı bilgilerini al
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s",
		m.config.MSSQL.Host, m.config.MSSQL.User, m.config.MSSQL.Pass, m.config.MSSQL.Port)

	// Windows olduğu için yerel kimlik doğrulama da destekleyelim
	if m.config.MSSQL.WindowsAuth {
		dsn = fmt.Sprintf("server=%s;port=%s;trusted_connection=yes",
			m.config.MSSQL.Host, m.config.MSSQL.Port)
	}

	// Veritabanı bağlantısını aç
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Printf("[ERROR] MSSQL failover kontrolü için bağlantı açılamadı: %v", err)
		return
	}
	defer db.Close()

	// Bağlantıyı test et
	if err = db.Ping(); err != nil {
		log.Printf("[ERROR] MSSQL sunucusuna bağlantı kurulamadı: %v", err)
		return
	}

	// AlwaysOn durumunu kontrol et
	query := `
	SELECT 
		ag.name AS ag_name,
		ar.replica_server_name,
		hars.role_desc, 
		hars.operational_state_desc,
		hars.connected_state_desc,
		hars.synchronization_health_desc,
		hars.recovery_health_desc
	FROM 
		sys.availability_groups AS ag
		JOIN sys.availability_replicas AS ar 
			ON ag.group_id = ar.group_id
		JOIN sys.dm_hadr_availability_replica_states AS hars 
			ON ar.replica_id = hars.replica_id
	WHERE 
		ar.replica_server_name = @@SERVERNAME`

	rows, err := db.Query(query)
	if err != nil {
		// Bu sorgu hatası AlwaysOn'un olmadığı ortamlarda normal
		if strings.Contains(err.Error(), "Invalid object name 'sys.availability_groups'") {
			log.Printf("[INFO] MSSQL sunucusunda AlwaysOn yapılandırması bulunmuyor")
			return
		}

		log.Printf("[ERROR] MSSQL AlwaysOn durumu sorgulanamadı: %v", err)
		return
	}
	defer rows.Close()

	// Mevcut durumu sakla
	var currentState struct {
		AgName                string
		ReplicaServerName     string
		RoleDesc              string
		OperationalStateDesc  string
		ConnectedStateDesc    string
		SynchronizationHealth string
		RecoveryHealth        string
	}

	// Satır varsa oku
	hasData := false
	for rows.Next() {
		hasData = true
		if err := rows.Scan(
			&currentState.AgName,
			&currentState.ReplicaServerName,
			&currentState.RoleDesc,
			&currentState.OperationalStateDesc,
			&currentState.ConnectedStateDesc,
			&currentState.SynchronizationHealth,
			&currentState.RecoveryHealth); err != nil {
			log.Printf("[ERROR] MSSQL AlwaysOn satırı okunamadı: %v", err)
			return
		}

		// İlk satırı al
		break
	}

	if !hasData {
		log.Printf("[INFO] MSSQL sunucusunda aktif AlwaysOn yapılandırması bulunamadı")
		return
	}

	// Önceki durumu kontrol et
	m.alarmCacheLock.RLock()
	prevAlarm, exists := m.alarmCache[alarmKey]
	m.alarmCacheLock.RUnlock()

	// İlk çalıştırma kontrolü yap
	if !exists {
		// İlk çalıştırma için özel durum - mevcut durumu kaydet
		initialStateEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "initial",
			MetricName:  "mssql_failover",
			MetricValue: currentState.RoleDesc,
			Message: fmt.Sprintf("SQL Server AlwaysOn başlangıç durumu - Grup: %s, Rol: %s, Durum: %s",
				currentState.AgName, currentState.RoleDesc, currentState.OperationalStateDesc),
			Timestamp: time.Now().Format(time.RFC3339),
			Severity:  "info",
		}

		// Başlangıç durumunu önbelleğe kaydet
		m.alarmCacheLock.Lock()
		m.alarmCache[alarmKey] = initialStateEvent
		m.alarmCacheLock.Unlock()

		log.Printf("MSSQL AlwaysOn başlangıç durumu kaydedildi: %s", currentState.RoleDesc)
		return
	}

	// Önceki rol durumunu parse et
	prevRole := ""
	if prevAlarm.MetricValue == "PRIMARY" || prevAlarm.MetricValue == "SECONDARY" {
		prevRole = prevAlarm.MetricValue
	} else {
		// Mesajdan çıkarmaya çalış
		if strings.Contains(prevAlarm.Message, "PRIMARY") {
			prevRole = "PRIMARY"
		} else if strings.Contains(prevAlarm.Message, "SECONDARY") {
			prevRole = "SECONDARY"
		}
	}

	// Failover kontrolü
	if prevRole != "" && prevRole != currentState.RoleDesc {
		// PRIMARY <-> SECONDARY değişimi olmuşsa failover
		failoverEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "mssql_failover",
			MetricValue: currentState.RoleDesc,
			Message: fmt.Sprintf("SQL Server failover detected! Previous state: %s, New state: %s, Group: %s, Operational: %s, Connected: %s",
				prevRole, currentState.RoleDesc, currentState.AgName,
				currentState.OperationalStateDesc, currentState.ConnectedStateDesc),
			Timestamp: time.Now().Format(time.RFC3339),
			Severity:  "critical",
		}

		// Alarmı gönder
		if err := m.reportAlarm(failoverEvent); err != nil {
			log.Printf("MSSQL failover alarmı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = failoverEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MSSQL failover alarmı gönderildi (ID: %s)", failoverEvent.Id)
		}
	} else if exists && prevAlarm.Status == "triggered" &&
		currentState.OperationalStateDesc == "ONLINE" &&
		currentState.ConnectedStateDesc == "CONNECTED" &&
		currentState.SynchronizationHealth == "HEALTHY" {
		// Durum normale döndüyse çözüldü mesajı gönder
		resolvedEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "resolved",
			MetricName:  "mssql_failover",
			MetricValue: currentState.RoleDesc,
			Message: fmt.Sprintf("SQL Server failover has completed. Current state: %s, Group: %s, All health metrics normal",
				currentState.RoleDesc, currentState.AgName),
			Timestamp: time.Now().Format(time.RFC3339),
			Severity:  "info",
		}

		// Çözüldü mesajını gönder
		if err := m.reportAlarm(resolvedEvent); err != nil {
			log.Printf("MSSQL failover çözüldü mesajı gönderilemedi: %v", err)
		} else {
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = resolvedEvent
			m.alarmCacheLock.Unlock()
			log.Printf("MSSQL failover çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
		}
	}

	// Senkronizasyon veya bağlantı sorunları varsa ayrı alarm gönder
	if currentState.OperationalStateDesc != "ONLINE" ||
		currentState.ConnectedStateDesc != "CONNECTED" ||
		currentState.SynchronizationHealth != "HEALTHY" {

		syncAlarmKey := "mssql_ag_health"

		// Rate limiting kontrolü
		m.alarmCacheLock.RLock()
		prevSyncAlarm, syncExists := m.alarmCache[syncAlarmKey]
		m.alarmCacheLock.RUnlock()

		// Önceki alarm varsa ve 5 dakikadan uzun süre geçtiyse veya alarm yoksa
		shouldSendAlarm := !syncExists
		if syncExists {
			prevTimestamp, err := time.Parse(time.RFC3339, prevSyncAlarm.Timestamp)
			if err == nil {
				shouldSendAlarm = time.Since(prevTimestamp) > 5*time.Minute
			} else {
				shouldSendAlarm = true
			}
		}

		if shouldSendAlarm {
			healthEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     syncAlarmKey,
				AgentId:     m.agentID,
				Status:      "triggered",
				MetricName:  "mssql_ag_health",
				MetricValue: "WARNING",
				Message: fmt.Sprintf("SQL Server AlwaysOn health issues detected! Role: %s, Group: %s, Operational: %s, Connected: %s, Sync Health: %s",
					currentState.RoleDesc, currentState.AgName,
					currentState.OperationalStateDesc, currentState.ConnectedStateDesc,
					currentState.SynchronizationHealth),
				Timestamp: time.Now().Format(time.RFC3339),
				Severity:  "critical",
			}

			if err := m.reportAlarm(healthEvent); err != nil {
				log.Printf("MSSQL AG health alarmı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[syncAlarmKey] = healthEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MSSQL AG health alarmı gönderildi (ID: %s)", healthEvent.Id)
			}
		}
	} else {
		// Sağlık sorunları çözüldüyse alarm kapat
		syncAlarmKey := "mssql_ag_health"

		m.alarmCacheLock.RLock()
		prevSyncAlarm, syncExists := m.alarmCache[syncAlarmKey]
		m.alarmCacheLock.RUnlock()

		if syncExists && prevSyncAlarm.Status == "triggered" {
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     syncAlarmKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "mssql_ag_health",
				MetricValue: "HEALTHY",
				Message: fmt.Sprintf("SQL Server AlwaysOn health has returned to normal. Role: %s, Group: %s",
					currentState.RoleDesc, currentState.AgName),
				Timestamp: time.Now().Format(time.RFC3339),
				Severity:  "info",
			}

			if err := m.reportAlarm(resolvedEvent); err != nil {
				log.Printf("MSSQL AG health çözüldü mesajı gönderilemedi: %v", err)
			} else {
				m.alarmCacheLock.Lock()
				m.alarmCache[syncAlarmKey] = resolvedEvent
				m.alarmCacheLock.Unlock()
				log.Printf("MSSQL AG health çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
			}
		}
	}
}
