package reporter

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
)

// Bu yapı, uzun süren işlemlerin (PostgreSQL promotion, failover, vb.) log mesajlarını
// toplamak ve server'a göndermek için kullanılır.
type ProcessLogger struct {
	client         pb.AgentServiceClient // gRPC client
	agentID        string                // Agent ID
	processID      string                // İşlem ID (UUID)
	processType    string                // İşlem türü (postgresql_promotion, failover, vb.)
	logMessages    []string              // Toplanan log mesajları
	logMutex       sync.Mutex            // Log mesajları için mutex
	startTime      time.Time             // İşlem başlangıç zamanı
	status         string                // İşlem durumu (running, completed, failed)
	lastUpdateTime time.Time             // Son güncelleme zamanı
	updateInterval time.Duration         // Güncelleme aralığı
	stopCh         chan struct{}         // Durdurma kanalı
}

// NewProcessLogger yeni bir process logger oluşturur
func NewProcessLogger(client pb.AgentServiceClient, agentID, processID, processType string) *ProcessLogger {
	return &ProcessLogger{
		client:         client,
		agentID:        agentID,
		processID:      processID,
		processType:    processType,
		logMessages:    make([]string, 0),
		startTime:      time.Now(),
		status:         "running",
		lastUpdateTime: time.Now(),
		updateInterval: 2 * time.Second, // 2 saniyede bir güncelleme
		stopCh:         make(chan struct{}),
	}
}

// Start logger'ı başlatır ve periyodik olarak logları server'a gönderir
func (pl *ProcessLogger) Start() {
	go pl.periodicallyReportLogs()
	log.Printf("Process logger başlatıldı: ProcessID=%s, Type=%s", pl.processID, pl.processType)
}

// Stop logger'ı durdurur ve son logları gönderir
func (pl *ProcessLogger) Stop(finalStatus string) {
	pl.logMutex.Lock()
	pl.status = finalStatus
	pl.logMutex.Unlock()

	// Son logları gönder
	pl.sendLogs()

	// Periyodik görevleri durdur
	close(pl.stopCh)
	log.Printf("Process logger durduruldu: ProcessID=%s, Son durum=%s", pl.processID, finalStatus)
}

// LogMessage bir log mesajı ekler
func (pl *ProcessLogger) LogMessage(message string) {
	pl.logMutex.Lock()
	defer pl.logMutex.Unlock()

	// Zaman damgalı log mesajı
	timestampedMsg := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), message)
	pl.logMessages = append(pl.logMessages, timestampedMsg)
	log.Printf("Process log: %s", timestampedMsg) // Yerel loglama
}

// periodicallyReportLogs belirli aralıklarla logları server'a gönderir
func (pl *ProcessLogger) periodicallyReportLogs() {
	ticker := time.NewTicker(pl.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pl.sendLogs()
		case <-pl.stopCh:
			return
		}
	}
}

// sendLogs o ana kadar toplanan logları server'a gönderir
func (pl *ProcessLogger) sendLogs() {
	pl.logMutex.Lock()
	defer pl.logMutex.Unlock()

	// Eğer yeni log yoksa gönderme
	if len(pl.logMessages) == 0 {
		return
	}

	// Log mesajlarını kopyala ve sıfırla
	logsCopy := make([]string, len(pl.logMessages))
	copy(logsCopy, pl.logMessages)
	pl.logMessages = make([]string, 0) // Kuyruğu sıfırla

	// Süre bilgisi
	elapsedTime := time.Since(pl.startTime).Seconds()

	// ProcessLogUpdate mesajı oluştur
	// Not: ProcessLogUpdate proto tanımı henüz yapılmadı, bu kısım proto tanımları eklendikten sonra düzenlenecek
	logUpdate := &pb.ProcessLogUpdate{
		AgentId:      pl.agentID,
		ProcessId:    pl.processID,
		ProcessType:  pl.processType,
		LogMessages:  logsCopy,
		Status:       pl.status,
		ElapsedTimeS: float32(elapsedTime),
		UpdatedAt:    time.Now().Format(time.RFC3339),
	}

	// Server'a gönder
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Not: ReportProcessLogs henüz tanımlanmadı, proto dosyasında eklenmesi gerekecek
	response, err := pl.client.ReportProcessLogs(ctx, &pb.ProcessLogRequest{
		LogUpdate: logUpdate,
	})

	if err != nil {
		log.Printf("Process logları gönderilemedi: %v", err)
		// Başarısız olursa logları koruyalım
		pl.logMessages = append(pl.logMessages, logsCopy...)
	} else {
		log.Printf("Process logları başarıyla gönderildi: %s", response.Status)
		pl.lastUpdateTime = time.Now()
	}
}

// CreateProcessLogger belirtilen işlem için yeni bir log izleyici oluşturur ve başlatır
func CreateProcessLogger(client pb.AgentServiceClient, agentID, processType string) (*ProcessLogger, string) {
	// Yeni bir işlem ID'si oluştur
	processID := uuid.New().String()

	// ProcessLogger oluştur
	logger := NewProcessLogger(client, agentID, processID, processType)

	// İlk log mesajını ekle
	logger.LogMessage(fmt.Sprintf("%s işlemi başlatılıyor...", processType))

	// Logger'ı başlat
	logger.Start()

	return logger, processID
}
