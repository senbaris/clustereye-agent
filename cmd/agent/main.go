package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/senbaris/clustereye-agent/internal/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
)

func main() {
	// Başlangıç mesajı
	log.Println("ClusterEye Agent başlatılıyor...")

	// Agent yapılandırmasını yükle
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Fatalf("Agent yapılandırması yüklenemedi: %v", err)
	}

	// Agent'ı oluştur
	a := agent.NewAgent(cfg)

	// Agent'ı başlat
	if err := a.Start(); err != nil {
		log.Fatalf("Agent başlatılamadı: %v", err)
	}

	// Graceful shutdown için sinyal yakalama
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Sinyal bekle
	sig := <-sigCh
	log.Printf("Sinyal alındı: %v, agent kapatılıyor...", sig)

	// Agent'ı durdur
	a.Stop()
	log.Println("Agent başarıyla kapatıldı.")
}
