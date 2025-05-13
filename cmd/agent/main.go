package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kardianos/service"
	"github.com/senbaris/clustereye-agent/internal/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
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
	log.Printf("ClusterEye Agent starting... Platform: %s", p.platform)

	a := agent.NewAgent(p.cfg)
	a.SetPlatform(p.platform)
	p.agent = a

	if err := p.agent.Start(); err != nil {
		log.Fatalf("Agent could not start: %v", err)
	}

	// Block until killed
	select {}
}

func (p *program) Stop(s service.Service) error {
	if p.agent != nil {
		p.agent.Stop()
		log.Println("Agent stopped.")
	}
	return nil
}

func main() {
	initLogging()

	platformFlag := flag.String("platform", "mssql", "Target platform (postgres, mongo, mssql)")
	helpFlag := flag.Bool("help", false, "Show help")
	versionFlag := flag.Bool("version", false, "Show version")
	svcFlag := flag.String("service", "", "Control the system service: install, uninstall, start, stop")

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		return
	}

	if *versionFlag {
		fmt.Println("ClusterEye Agent v1.0.23")
		return
	}

	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
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
		log.Fatal(err)
	}

	if len(*svcFlag) > 0 {
		err := service.Control(s, *svcFlag)
		if err != nil {
			log.Fatalf("Service command failed: %v", err)
		}
		return
	}

	err = s.Run()
	if err != nil {
		log.Fatal(err)
	}
}

const maxLogSize = 10 * 1024 * 1024 // 10 MB

func initLogging() {
	if runtime.GOOS != "windows" {
		// Linux'ta log dosyasına yazma
		return
	}

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

	// Dosyayı aç ve log çıktısını yönlendir
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(f)
}
