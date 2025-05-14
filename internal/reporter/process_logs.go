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
	metadata       map[string]string     // İşlem meta verileri
	errorCount     int                   // Hata sayacı
	hasError       bool                  // Hata var mı
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
		metadata:       make(map[string]string),
		errorCount:     0,
		hasError:       false,
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

	// Hata varsa ve status completed ise, failed'a çevir
	if pl.hasError && finalStatus == "completed" {
		pl.LogMessage(fmt.Sprintf("İşlem sırasında %d hata oluştu, durum 'failed' olarak işaretleniyor", pl.errorCount))
		finalStatus = "failed"
	}

	pl.status = finalStatus

	// İşlem durumunu metadata'ya ekle
	pl.metadata["final_status"] = finalStatus
	pl.metadata["duration_s"] = fmt.Sprintf("%.2f", time.Since(pl.startTime).Seconds())

	// Bitiş zamanını da ekle
	pl.metadata["end_time"] = time.Now().Format(time.RFC3339)

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

// LogError bir hata mesajı ekler ve hata sayacını artırır
func (pl *ProcessLogger) LogError(message string, err error) {
	pl.logMutex.Lock()
	defer pl.logMutex.Unlock()

	// Hata sayacını artır
	pl.errorCount++
	pl.hasError = true

	// Hata mesajını oluştur
	var errorMsg string
	if err != nil {
		errorMsg = fmt.Sprintf("[%s] HATA: %s - %v", time.Now().Format("15:04:05"), message, err)
	} else {
		errorMsg = fmt.Sprintf("[%s] HATA: %s", time.Now().Format("15:04:05"), message)
	}

	// Log mesajını ekle
	pl.logMessages = append(pl.logMessages, errorMsg)
	log.Printf("Process error log: %s", errorMsg) // Yerel loglama
}

// AddMetadata işlem hakkında ek bilgi ekler
func (pl *ProcessLogger) AddMetadata(key, value string) {
	pl.logMutex.Lock()
	defer pl.logMutex.Unlock()

	pl.metadata[key] = value
	log.Printf("Process %s metadata eklendi: %s = %s", pl.processID, key, value)
}

// GetMetadata belirli bir metadata değerini döndürür
func (pl *ProcessLogger) GetMetadata(key string) (string, bool) {
	pl.logMutex.Lock()
	defer pl.logMutex.Unlock()

	value, exists := pl.metadata[key]
	return value, exists
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

	// Eğer yeni log yoksa ve durum değişmemişse gönderme
	if len(pl.logMessages) == 0 && pl.status != "completed" && pl.status != "failed" {
		return
	}

	// Log mesajlarını kopyala ve sıfırla
	logsCopy := make([]string, len(pl.logMessages))
	copy(logsCopy, pl.logMessages)
	pl.logMessages = make([]string, 0) // Kuyruğu sıfırla

	// Süre bilgisi
	elapsedTime := time.Since(pl.startTime).Seconds()

	// Metadata'yı proto tipine dönüştür
	metadataProto := make(map[string]string)
	for k, v := range pl.metadata {
		metadataProto[k] = v
	}

	// Ek process metadata'sı ekle
	metadataProto["error_count"] = fmt.Sprintf("%d", pl.errorCount)
	metadataProto["elapsed_time_s"] = fmt.Sprintf("%.2f", elapsedTime)

	// NOTLAR:
	// 1. ProcessLogUpdate ve ProcessLogRequest yapıları henüz proto dosyasında tanımlanmadı
	// 2. Bu kısım proto tanımları eklendikten sonra güncellenecek
	// 3. Şimdilik sadece logları yerel olarak tutuyoruz

	// Logları şimdilik yerel olarak logla
	log.Printf("Process %s (%s) logları (Proto tanımları henüz eklenmediği için server'a gönderilmiyor):",
		pl.processID, pl.processType)
	for _, logMsg := range logsCopy {
		log.Printf("  %s", logMsg)
	}

	// ProcessLogUpdate mesajı oluştur
	logUpdate := &pb.ProcessLogUpdate{
		AgentId:      pl.agentID,
		ProcessId:    pl.processID,
		ProcessType:  pl.processType,
		LogMessages:  logsCopy,
		Status:       pl.status,
		ElapsedTimeS: float32(elapsedTime),
		UpdatedAt:    time.Now().Format(time.RFC3339),
		Metadata:     metadataProto,
	}

	// Server'a gönder
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ReportProcessLogs RPC çağrısı
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

	pl.lastUpdateTime = time.Now()
}

// CreateProcessLogger belirtilen işlem için yeni bir log izleyici oluşturur ve başlatır
func CreateProcessLogger(client pb.AgentServiceClient, agentID, processType string) (*ProcessLogger, string) {
	// Yeni bir işlem ID'si oluştur
	processID := uuid.New().String()

	// ProcessLogger oluştur
	logger := NewProcessLogger(client, agentID, processID, processType)

	// İşlem başlangıç zamanını metadata'ya ekle
	logger.AddMetadata("start_time", time.Now().Format(time.RFC3339))
	logger.AddMetadata("process_type", processType)
	logger.AddMetadata("agent_id", agentID)

	// İlk log mesajını ekle
	logger.LogMessage(fmt.Sprintf("%s işlemi başlatılıyor...", processType))

	// Logger'ı başlat
	logger.Start()

	return logger, processID
}
