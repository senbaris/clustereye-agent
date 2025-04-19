package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/senbaris/clustereye-agent/internal/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
)

func main() {
	// Command line argümanlarını analiz etmek için flag'ler tanımla
	platformFlag := flag.String("platform", "postgres", "Hangi platform için izleme yapılacak (postgres veya mongo)")
	helpFlag := flag.Bool("help", false, "Yardım mesajını görüntüle")
	versionFlag := flag.Bool("version", false, "Versiyon bilgisini görüntüle")

	// Flag'leri analiz et
	flag.Parse()

	// Yardım flag'i kontrol et
	if *helpFlag {
		fmt.Println("ClusterEye Agent - Database monitoring tool")
		fmt.Println("")
		fmt.Println("Kullanım:")
		fmt.Println("  agent [flags]")
		fmt.Println("")
		fmt.Println("Flags:")
		fmt.Println("  -platform string    Hangi platform için izleme yapılacak (postgres veya mongo) (default \"postgres\")")
		fmt.Println("  -help               Yardım mesajını görüntüle")
		fmt.Println("  -version            Versiyon bilgisini görüntüle")
		return
	}

	// Versiyon flag'i kontrol et
	if *versionFlag {
		fmt.Println("ClusterEye Agent v1.0.0")
		return
	}

	// Başlangıç mesajı
	log.Printf("ClusterEye Agent başlatılıyor... Platform: %s", *platformFlag)

	// Platform değerini kontrol et
	if *platformFlag != "postgres" && *platformFlag != "mongo" {
		log.Fatalf("Geçersiz platform değeri '%s'. 'postgres' veya 'mongo' kullanın.", *platformFlag)
	}

	// Agent yapılandırmasını yükle
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Fatalf("Agent yapılandırması yüklenemedi: %v", err)
	}

	// Agent'ı oluştur
	a := agent.NewAgent(cfg)

	// Platform bilgisini agent'a kaydet
	a.SetPlatform(*platformFlag)

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
