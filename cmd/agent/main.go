package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/kardianos/service"
	"github.com/senbaris/clustereye-agent/internal/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
)

type program struct {
	agent    *agent.Agent
	cfg      *config.AgentConfig
	platform string
}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *program) run() {
	logger.Info("ClusterEye Agent starting... Platform: %s", p.platform)

	a := agent.NewAgent(p.cfg)
	a.SetPlatform(p.platform)
	p.agent = a

	if err := p.agent.Start(); err != nil {
		logger.Fatal("Agent could not start: %v", err)
	}

	// Block until killed
	select {}
}

func (p *program) Stop(s service.Service) error {
	if p.agent != nil {
		p.agent.Stop()
		logger.Info("Agent stopped.")
	}
	return nil
}

func main() {
	initLogging()

	platformFlag := flag.String("platform", "mssql", "Target platform (postgres, mongo, mssql)")
	helpFlag := flag.Bool("help", false, "Show help")
	versionFlag := flag.Bool("version", false, "Show version")
	svcFlag := flag.String("service", "", "Control the system service: install, uninstall, start, stop")
	logLevelFlag := flag.String("loglevel", "WARNING", "Log level: DEBUG, INFO, WARNING, ERROR, FATAL")

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		return
	}

	if *versionFlag {
		fmt.Println("ClusterEye Agent v1.0.23")
		return
	}

	// Set log level from command line flag
	if *logLevelFlag != "" {
		level := logger.ParseLevel(*logLevelFlag)
		logger.SetLevel(level)
		logger.Info("Log seviyesi ayarlandı: %s", logger.LevelToString(level))
	}

	cfg, err := config.LoadAgentConfig()
	if err != nil {
		logger.Fatal("Failed to load config: %v", err)
	}

	prg := &program{
		cfg:      cfg,
		platform: *platformFlag,
	}

	svcConfig := &service.Config{
		Name:        "ClusterEyeAgent",
		DisplayName: "ClusterEye Agent",
		Description: "Database monitoring agent for ClusterEye platform.",
	}

	s, err := service.New(prg, svcConfig)
	if err != nil {
		logger.Fatal("Failed to create service: %v", err)
	}

	if len(*svcFlag) > 0 {
		err := service.Control(s, *svcFlag)
		if err != nil {
			logger.Fatal("Service command failed: %v", err)
		}
		return
	}

	err = s.Run()
	if err != nil {
		logger.Fatal("Service run failed: %v", err)
	}
}

const maxLogSize = 10 * 1024 * 1024 // 10 MB

func initLogging() {
	if runtime.GOOS != "windows" {
		// Linux'ta log dosyasına yazma
		return
	}

	// Windows'ta dosya log sistemini kullan
	setupFileLogging()
}

// Dosya log sistemini kurma yardımcı fonksiyonu
func setupFileLogging() {
	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to get executable path: %v", err)
	}
	logDir := filepath.Dir(exePath)

	// Log rotasyon ayarları
	maxLogSize := int64(100 * 1024 * 1024) // 100MB
	maxLogFiles := 5                       // Saklanacak maksimum log dosyası sayısı

	// Ana log dosyası
	logFile := filepath.Join(logDir, "agent.log")

	// Mevcut log dosyasının boyutunu kontrol et
	info, err := os.Stat(logFile)
	if err == nil && info.Size() > maxLogSize {
		// Rotasyon gerekli, tarihe göre eski dosyayı adlandır
		timestamp := time.Now().Format("2006-01-02_15-04-05")
		backupFile := filepath.Join(logDir, fmt.Sprintf("agent_%s.log", timestamp))

		if err := os.Rename(logFile, backupFile); err != nil {
			log.Printf("Log rotasyonu yapılamadı: %v", err)
		} else {
			log.Printf("Log rotasyonu yapıldı: %s", backupFile)
		}

		// Eski log dosyalarını temizle (maksimum sayıyı aşarsa)
		cleanupOldLogs(logDir, maxLogFiles)
	}

	// Dosyayı aç
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Set the output for both standard log and our logger package
	log.SetOutput(f)
	logger.SetOutput(f)

	logger.Info("Dosya log sistemi başarıyla etkinleştirildi, log seviyesi: %s", logger.LevelToString(logger.GetLevel()))
}

// Eski log dosyalarını temizler, sadece en yeni n dosyayı tutar
func cleanupOldLogs(logDir string, keepCount int) {
	// Tüm log dosyalarını bul
	pattern := filepath.Join(logDir, "agent_*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("Eski log dosyaları bulunamadı: %v", err)
		return
	}

	// Log dosyalarını oluşturma tarihine göre sırala (en eskiler önce)
	sort.Slice(matches, func(i, j int) bool {
		infoI, errI := os.Stat(matches[i])
		infoJ, errJ := os.Stat(matches[j])

		if errI != nil || errJ != nil {
			return false
		}

		return infoI.ModTime().Before(infoJ.ModTime())
	})

	// Maksimum sayıyı aşan en eski dosyaları sil
	if len(matches) > keepCount {
		for i := 0; i < len(matches)-keepCount; i++ {
			if err := os.Remove(matches[i]); err != nil {
				log.Printf("Eski log dosyası silinemedi %s: %v", matches[i], err)
			} else {
				log.Printf("Eski log dosyası silindi: %s", matches[i])
			}
		}
	}
}
