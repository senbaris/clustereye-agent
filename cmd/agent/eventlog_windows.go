//go:build windows

package main

import (
	"log"
	"os"
	"strings"

	"github.com/senbaris/clustereye-agent/internal/logger"
	"golang.org/x/sys/windows/svc/eventlog"
)

func init() {
	// Windows-specific eventlog yapılandırmasını ayarla
	// Bu fonksiyon sadece Windows'ta çalışacak
	configureWindowsEventLog = setupWindowsEventLogInternal
}

// setupWindowsEventLogInternal configures logging to use the Windows Event Log
// Sadece Windows platformunda kullanılır
func setupWindowsEventLogInternal() bool {
	const sourceName = "ClusterEyeAgent"

	// EventLog'a erişim sağla
	elog, err := eventlog.Open(sourceName)
	if err != nil {
		// EventLog kaynağı yoksa oluşturmayı dene
		err = eventlog.InstallAsEventCreate(sourceName, eventlog.Info|eventlog.Warning|eventlog.Error)
		if err != nil {
			// Yüksek yetkiler gerektirebilir, başarısız olursa dosya loguna geri dön
			log.Printf("Windows Event Log kurulumu başarısız: %v, dosya loguna dönülüyor", err)
			return false
		}

		// Yeni oluşturulan EventLog'a bağlanmayı dene
		elog, err = eventlog.Open(sourceName)
		if err != nil {
			log.Printf("Windows Event Log açılamadı: %v, dosya loguna dönülüyor", err)
			return false
		}
	}

	// EventLog yazıcısını ayarla
	writer := &eventLogWriter{elog: elog}

	// Standart log için output ayarla
	log.SetOutput(writer)

	// Logger package için output ayarla
	logger.SetOutput(writer)

	logger.Info("Windows Event Log başarıyla etkinleştirildi, log seviyesi: %s", logger.LevelToString(logger.GetLevel()))
	return true
}

// Windows Event Log yazmak için özel writer
type eventLogWriter struct {
	elog *eventlog.Log
}

func (w *eventLogWriter) Write(p []byte) (n int, err error) {
	message := string(p)

	// Mesajın log seviyesini belirle
	var logLevel int

	if strings.Contains(message, "[DEBUG]") {
		logLevel = logger.LevelDebug
	} else if strings.Contains(message, "[INFO]") || !strings.Contains(message, "[") {
		logLevel = logger.LevelInfo
	} else if strings.Contains(message, "[WARNING]") || strings.Contains(message, "[WARN]") {
		logLevel = logger.LevelWarning
	} else if strings.Contains(message, "[ERROR]") {
		logLevel = logger.LevelError
	} else if strings.Contains(message, "[FATAL]") {
		logLevel = logger.LevelFatal
	} else {
		logLevel = logger.LevelInfo // Varsayılan
	}

	// Log seviyesine göre EventLog'a yaz
	switch logLevel {
	case logger.LevelDebug, logger.LevelInfo:
		err = w.elog.Info(1, message)
	case logger.LevelWarning:
		err = w.elog.Warning(2, message)
	case logger.LevelError, logger.LevelFatal:
		err = w.elog.Error(3, message)
	}

	if err != nil {
		// Yazma hatası durumunda standart çıktıya yaz
		os.Stderr.Write(p)
		return 0, err
	}

	return len(p), nil
}
