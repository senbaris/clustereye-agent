package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/senbaris/clustereye-agent/internal/config"
)

// OpenDB veritabanı bağlantısını açar
func OpenDB() (*sql.DB, error) {
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		return nil, fmt.Errorf("konfigürasyon yüklenemedi: %v", err)
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.PostgreSQL.Host,
		cfg.PostgreSQL.Port,
		cfg.PostgreSQL.User,
		cfg.PostgreSQL.Pass,
		"postgres",
	)

	return sql.Open("postgres", connStr)
}

func GetPGServiceStatus() string {

	out, err := exec.Command("sh", "-c", "ps -aux | grep postgres: | grep -v grep").Output()
	if err != nil || len(out) == 0 {
		fmt.Println(out, err)
		return "FAIL!"
	}
	return "RUNNING"

}

// GetPGVersion PostgreSQL versiyonunu döndürür
func GetPGVersion() string {
	db, err := OpenDB()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return "Unknown"
	}
	defer db.Close()

	var version string
	err = db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		log.Printf("Versiyon bilgisi alınamadı: %v", err)
		return "Unknown"
	}

	// PostgreSQL 14.5 gibi sadece ana versiyon numarasını çıkar
	re := regexp.MustCompile(`PostgreSQL (\d+\.\d+)`)
	matches := re.FindStringSubmatch(version)
	if len(matches) > 1 {
		return matches[1]
	}

	return "Unknown"
}

// GetNodeStatus node'un master/slave durumunu döndürür
func GetNodeStatus() string {
	db, err := OpenDB()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return "Unknown"
	}
	defer db.Close()

	// PostgreSQL 10 ve üzeri için
	var inRecovery bool
	err = db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		log.Printf("Node durumu alınamadı: %v", err)
		return "Unknown"
	}

	if inRecovery {
		return "SLAVE"
	}
	return "MASTER"
}

// convertSize bytes cinsinden boyutu okunabilir formata çevirir
func convertSize(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// findPostgresConfigFile PostgreSQL konfigürasyon dosyasını bulur
func findPostgresConfigFile() (string, error) {
	// Olası konfigürasyon dosyası konumları
	possiblePaths := []string{
		"/etc/postgresql/*/main/postgresql.conf",
		"/var/lib/pgsql/data/postgresql.conf",
		"/var/lib/postgresql/*/main/postgresql.conf",
		"/usr/local/var/postgresql*/postgresql.conf",
		"/opt/homebrew/var/postgresql*/postgresql.conf", // macOS için
	}

	// Her bir olası yolu kontrol et
	for _, pattern := range possiblePaths {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		if len(matches) > 0 {
			return matches[0], nil
		}
	}

	return "", fmt.Errorf("postgresql.conf dosyası bulunamadı")
}

// getDataDirectoryFromConfig postgresql.conf dosyasından data_directory parametresini okur
func getDataDirectoryFromConfig() (string, error) {
	configFile, err := findPostgresConfigFile()
	if err != nil {
		return "", fmt.Errorf("konfigürasyon dosyası bulunamadı: %v", err)
	}
	log.Printf("Config file found at: %s", configFile)

	content, err := os.ReadFile(configFile)
	if err != nil {
		return "", fmt.Errorf("konfigürasyon dosyası okunamadı: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		// Remove comments and trim whitespace
		line = strings.Split(line, "#")[0]
		line = strings.TrimSpace(line)

		log.Printf("Checking line: %s", line) // Debug log

		if strings.HasPrefix(line, "data_directory") {
			// Split on = and handle any whitespace
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}
			// Clean up the value: remove quotes and whitespace
			value := strings.Trim(strings.Trim(parts[1], " '\""), " ")
			log.Printf("Found data_directory: %s", value) // Debug log
			return value, nil
		}
	}

	return "", fmt.Errorf("data_directory parametresi bulunamadı")
}

// GetDiskUsage disk kullanım bilgilerini döndürür
func GetDiskUsage() (string, int) {
	// PostgreSQL veri dizinini bul
	dataDir, err := getDataDirectoryFromConfig()
	if err != nil {
		log.Printf("PostgreSQL veri dizini bulunamadı: %v", err)
		return "N/A", 0
	}

	// Disk kullanım bilgilerini al
	var stat syscall.Statfs_t
	err = syscall.Statfs(dataDir, &stat)
	if err != nil {
		log.Printf("Disk kullanım bilgileri alınamadı: %v", err)
		return "N/A", 0
	}

	// Boş alanı hesapla
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	totalBytes := stat.Blocks * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes

	// Yüzdeyi hesapla
	percent := int((float64(usedBytes) / float64(totalBytes)) * 100)

	// Boş alanı okunabilir formata çevir
	freeDisk := convertSize(freeBytes)

	return freeDisk, percent
}

// GetReplicationLagSec replication lag'i saniye cinsinden döndürür
func GetReplicationLagSec() float64 {
	db, err := OpenDB()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return 0
	}
	defer db.Close()

	nodeStatus := GetNodeStatus()
	if nodeStatus != "Slave" {
		return 0
	}

	var lag float64
	err = db.QueryRow(`
		SELECT CASE 
			WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() 
			THEN 0 
			ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) 
		END AS lag_delay
	`).Scan(&lag)
	if err != nil {
		log.Printf("Replication lag alınamadı: %v", err)
		return 0
	}

	return lag
}
