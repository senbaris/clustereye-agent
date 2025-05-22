package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
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
	// Üst seviye panic recovery - agent çökerse bile servis çalışmaya devam etsin
	defer func() {
		if r := recover(); r != nil {
			logger.Error("AGENT PANIC RECOVERY: Agent çöktü: %v", r)
			logger.Error("Agent yeniden başlatılıyor...")

			// Kısa bir gecikme sonra agent'ı yeniden başlat
			go func() {
				time.Sleep(5 * time.Second)
				p.run() // Recursively restart
			}()
		}
	}()

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
	// En üst düzey panic recovery - tamamen çökmeyi önlemek için
	defer func() {
		if r := recover(); r != nil {
			log.Printf("FATAL PANIC RECOVERY: Ana uygulama çöktü: %v", r)
			log.Println("Uygulama yeniden başlatılacak...")

			// OS'a hata kodu döndür - Windows servisi otomatik yeniden başlatır
			os.Exit(1)
		}
	}()

	initLogging()

	platformFlag := flag.String("platform", "mssql", "Target platform (postgres, mongo, mssql)")
	helpFlag := flag.Bool("help", false, "Show help")
	versionFlag := flag.Bool("version", false, "Show version")
	svcFlag := flag.String("service", "", "Control the system service: install, uninstall, start, stop")
	logLevelFlag := flag.String("loglevel", "INFO", "Log level: DEBUG, INFO, WARNING, ERROR, FATAL")

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

	// Windows Event Log'unu kullanmayı dene
	if setupWindowsEventLog() {
		// Başarılı, Event Log kullanılıyor
		return
	}

	// Event Log başarısız olursa dosya log sistemini kullan
	setupFileLogging()
}

// Dosya log sistemini kurma yardımcı fonksiyonu
func setupFileLogging() {
	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to get executable path: %v", err)
	}
	logDir := filepath.Dir(exePath)
	logFile := filepath.Join(logDir, "agent.log")

	// Eğer log dosyası çok büyükse eski logu .old olarak taşı
	if info, err := os.Stat(logFile); err == nil && info.Size() > maxLogSize {
		_ = os.Rename(logFile, logFile+".old")
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
