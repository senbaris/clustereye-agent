package alarm

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
)

// AlarmMonitor alarm durumlarını izleyen ve raporlayan birim
type AlarmMonitor struct {
	client         pb.AgentServiceClient
	agentID        string
	stopCh         chan struct{}
	alarmCache     map[string]*pb.AlarmEvent // Gönderilen son alarmları saklar
	alarmCacheLock sync.RWMutex
	checkInterval  time.Duration
}

// NewAlarmMonitor yeni bir alarm monitörü oluşturur
func NewAlarmMonitor(client pb.AgentServiceClient, agentID string) *AlarmMonitor {
	return &AlarmMonitor{
		client:        client,
		agentID:       agentID,
		stopCh:        make(chan struct{}),
		alarmCache:    make(map[string]*pb.AlarmEvent),
		checkInterval: 30 * time.Second, // Varsayılan kontrol aralığı 30 saniye
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

	// Hemen ilk kontrolü yap
	m.checkAlarms()

	// Periyodik kontrole başla
	for {
		select {
		case <-ticker.C:
			m.checkAlarms()
		case <-m.stopCh:
			return
		}
	}
}

// checkAlarms tüm alarm koşullarını kontrol eder
func (m *AlarmMonitor) checkAlarms() {
	// PostgreSQL servis durumunu kontrol et
	m.checkPostgreSQLServiceStatus()

	// PgBouncer durumunu kontrol et
	m.checkPgBouncerStatus()

	// Diğer alarm kontrolleri burada eklenebilir
	// m.checkMemoryUsage()
	// m.checkDiskUsage()
	// m.checkCPUUsage()
}

// checkPostgreSQLServiceStatus PostgreSQL servis durumunu kontrol eder
func (m *AlarmMonitor) checkPostgreSQLServiceStatus() {
	status := postgres.GetPGServiceStatus()
	alarmKey := "postgresql_service_status"

	if status == "FAIL!" {
		// Önceki bir alarm varsa ve aynı durumda ise tekrar gönderme
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		if exists && prevAlarm.Status == "triggered" {
			// Alarm zaten tetiklenmiş, tekrar gönderme
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
			Message:     "PostgreSQL servisi çalışmıyor (FAIL!)",
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
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		if exists && prevAlarm.Status == "triggered" {
			// Çözüldü mesajı oluştur
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "postgresql_service_status",
				MetricValue: status,
				Message:     "PostgreSQL servisi çalışıyor (RUNNING)",
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

// checkPgBouncerStatus PgBouncer servis durumunu kontrol eder
func (m *AlarmMonitor) checkPgBouncerStatus() {
	status := postgres.GetPGBouncerStatus()
	alarmKey := "pgbouncer_service_status"

	if status == "FAIL!" {
		// Önceki bir alarm varsa ve aynı durumda ise tekrar gönderme
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		if exists && prevAlarm.Status == "triggered" {
			// Alarm zaten tetiklenmiş, tekrar gönderme
			log.Printf("PgBouncer servis durumu hala FAIL! Alarm zaten gönderildi (%s)", prevAlarm.Id)
			return
		}

		// Yeni alarm oluştur
		alarmEvent := &pb.AlarmEvent{
			Id:          uuid.New().String(),
			AlarmId:     alarmKey,
			AgentId:     m.agentID,
			Status:      "triggered",
			MetricName:  "pgbouncer_service_status",
			MetricValue: status,
			Message:     "PgBouncer servisi çalışmıyor (FAIL!)",
			Timestamp:   time.Now().Format(time.RFC3339),
			Severity:    "critical",
		}

		// Alarmı gönder
		if err := m.reportAlarm(alarmEvent); err != nil {
			log.Printf("PgBouncer servis alarmı gönderilemedi: %v", err)
		} else {
			// Başarıyla gönderildi, önbellekte sakla
			m.alarmCacheLock.Lock()
			m.alarmCache[alarmKey] = alarmEvent
			m.alarmCacheLock.Unlock()

			log.Printf("PgBouncer servis FAIL! alarmı gönderildi (ID: %s)", alarmEvent.Id)
		}
	} else if status == "RUNNING" {
		// Önceki bir alarm varsa ve tetiklenmişse, çözüldü mesajı gönder
		m.alarmCacheLock.RLock()
		prevAlarm, exists := m.alarmCache[alarmKey]
		m.alarmCacheLock.RUnlock()

		if exists && prevAlarm.Status == "triggered" {
			// Çözüldü mesajı oluştur
			resolvedEvent := &pb.AlarmEvent{
				Id:          uuid.New().String(),
				AlarmId:     alarmKey,
				AgentId:     m.agentID,
				Status:      "resolved",
				MetricName:  "pgbouncer_service_status",
				MetricValue: status,
				Message:     "PgBouncer servisi çalışıyor (RUNNING)",
				Timestamp:   time.Now().Format(time.RFC3339),
				Severity:    "info",
			}

			// Çözüldü mesajını gönder
			if err := m.reportAlarm(resolvedEvent); err != nil {
				log.Printf("PgBouncer servis çözüldü mesajı gönderilemedi: %v", err)
			} else {
				// Başarıyla gönderildi, önbellekte sakla
				m.alarmCacheLock.Lock()
				m.alarmCache[alarmKey] = resolvedEvent
				m.alarmCacheLock.Unlock()

				log.Printf("PgBouncer servis çözüldü mesajı gönderildi (ID: %s)", resolvedEvent.Id)
			}
		}
	}
}

// reportAlarm bir alarm olayını API'ye bildirir
func (m *AlarmMonitor) reportAlarm(event *pb.AlarmEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.ReportAlarmRequest{
		AgentId: m.agentID,
		Events:  []*pb.AlarmEvent{event},
	}

	resp, err := m.client.ReportAlarm(ctx, req)
	if err != nil {
		return err
	}

	log.Printf("Alarm raporu gönderildi, yanıt: %s", resp.Status)
	return nil
}
