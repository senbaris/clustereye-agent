package postgres

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
)

// PostgresLogFile PostgreSQL log dosyasını temsil eder
type PostgresLogFile struct {
	Name         string
	Path         string
	Size         int64
	LastModified int64
}

// ToProto PostgreSQL log dosyasını proto mesajına dönüştürür
// NOT: Proto dosyasına FileInfo eklendiğinde bunu güncelleyin
func (p *PostgresLogFile) ToProto() map[string]interface{} {
	return map[string]interface{}{
		"name":          p.Name,
		"path":          p.Path,
		"size":          p.Size,
		"last_modified": p.LastModified,
	}
}

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

// GetPGBouncerStatus PgBouncer servisinin durumunu kontrol eder
func GetPGBouncerStatus() string {
	out, err := exec.Command("pgrep", "pgbouncer").Output()
	if err != nil || len(out) == 0 {
		log.Println("pgrep pgbouncer returns empty", out, err)
		return "FAIL!"
	}
	return "RUNNING"
}

// GetSystemMetrics sistem metriklerini toplar
func GetSystemMetrics() *pb.SystemMetrics {
	metrics := &pb.SystemMetrics{}

	// CPU kullanımı ve çekirdek sayısı
	if cpuPercent, err := getCPUUsage(); err == nil {
		metrics.CpuUsage = cpuPercent
	}
	if cpuCores, err := getCPUCores(); err == nil {
		metrics.CpuCores = cpuCores
	}

	// Load Average
	if loadAvg, err := getLoadAverage(); err == nil {
		metrics.LoadAverage_1M = loadAvg[0]
		metrics.LoadAverage_5M = loadAvg[1]
		metrics.LoadAverage_15M = loadAvg[2]
	}

	// RAM kullanımı
	if ramUsage, err := getRAMUsage(); err == nil {
		metrics.TotalMemory = ramUsage["total_mb"].(int64)
		metrics.FreeMemory = ramUsage["free_mb"].(int64)
		metrics.MemoryUsage = ramUsage["usage_percent"].(float64)
	}

	// Disk kullanımı
	if diskUsage, err := getDiskUsage(); err == nil {
		metrics.TotalDisk = diskUsage["total_gb"].(int64)
		metrics.FreeDisk = diskUsage["avail_gb"].(int64)
	}

	// OS ve Kernel bilgileri
	if osInfo, err := getOSInfo(); err == nil {
		metrics.OsVersion = osInfo["os_version"]
		metrics.KernelVersion = osInfo["kernel_version"]
	}

	// Uptime
	if uptime, err := getUptime(); err == nil {
		metrics.Uptime = uptime
	}

	return metrics
}

// getCPUUsage CPU kullanım yüzdesini döndürür
func getCPUUsage() (float64, error) {
	cmd := exec.Command("sh", "-c", "ps -A -o %cpu | awk '{s+=$1} END {print s}'")
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	cpuPercent, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0, err
	}
	return cpuPercent, nil
}

// getCPUCores CPU çekirdek sayısını döndürür
func getCPUCores() (int32, error) {
	cmd := exec.Command("sh", "-c", "sysctl -n hw.ncpu")
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	cores, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(cores), nil
}

// getLoadAverage sistem yükünü döndürür
func getLoadAverage() ([]float64, error) {
	cmd := exec.Command("sh", "-c", "sysctl -n vm.loadavg")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Çıktı formatı: { 1.23 1.45 1.67 }
	loadStr := strings.TrimSpace(string(out))
	loadStr = strings.Trim(loadStr, "{}")
	loadStrs := strings.Fields(loadStr)

	if len(loadStrs) < 3 {
		return nil, fmt.Errorf("unexpected loadavg format: %s", loadStr)
	}

	loads := make([]float64, 3)
	for i := 0; i < 3; i++ {
		load, err := strconv.ParseFloat(loadStrs[i], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse load value: %s", loadStrs[i])
		}
		loads[i] = load
	}
	return loads, nil
}

// getRAMUsage RAM kullanım bilgilerini döndürür
func getRAMUsage() (map[string]interface{}, error) {
	// Toplam RAM miktarını al
	cmd := exec.Command("sh", "-c", "sysctl -n hw.memsize")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("failed to get total memory with hw.memsize: %v", err)
		// Alternatif yöntem - top komutunu kullan
		return getRAMUsageWithTop()
	}

	totalBytes, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		log.Printf("failed to parse total memory: %v", err)
		return getRAMUsageWithTop()
	}
	totalMB := totalBytes / (1024 * 1024)

	// MacOS'da farklı bir yaklaşım deneyelim - vm_stat kullanarak
	cmd = exec.Command("sh", "-c", "vm_stat | grep 'Pages free\\|Pages active\\|Pages inactive\\|Pages speculative\\|Pages wired down'")
	out, err = cmd.Output()
	if err != nil {
		log.Printf("failed to get memory stats with vm_stat: %v", err)
		return getRAMUsageWithTop()
	}

	// Çıktıyı işle
	vmStatLines := strings.Split(string(out), "\n")

	// Sayfa boyutunu al
	cmd = exec.Command("sh", "-c", "sysctl -n hw.pagesize")
	out, err = cmd.Output()
	if err != nil {
		log.Printf("failed to get page size: %v", err)
		return getRAMUsageWithTop()
	}

	pageSize, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		log.Printf("failed to parse page size: %v", err)
		return getRAMUsageWithTop()
	}

	var freePages, activePages, inactivePages, speculativePages, wiredPages uint64

	// vm_stat çıktısını parse et
	for _, line := range vmStatLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		valueStr := strings.TrimSpace(parts[1])
		valueStr = strings.ReplaceAll(valueStr, ".", "") // Nokta karakterini kaldır
		value, err := strconv.ParseUint(valueStr, 10, 64)
		if err != nil {
			log.Printf("failed to parse vm_stat value for %s: %v", key, err)
			continue
		}

		switch {
		case strings.Contains(key, "Pages free"):
			freePages = value
		case strings.Contains(key, "Pages active"):
			activePages = value
		case strings.Contains(key, "Pages inactive"):
			inactivePages = value
		case strings.Contains(key, "Pages speculative"):
			speculativePages = value
		case strings.Contains(key, "Pages wired down"):
			wiredPages = value
		}
	}

	log.Printf("VM_STAT Parse - Free: %d, Active: %d, Inactive: %d, Speculative: %d, Wired: %d",
		freePages, activePages, inactivePages, speculativePages, wiredPages)

	// Kullanılan sayfaları hesapla
	usedPages := activePages + wiredPages
	usedBytes := usedPages * pageSize
	usedMB := usedBytes / (1024 * 1024)

	freeMB := (freePages * pageSize) / (1024 * 1024)

	// Eğer değerler sıfırsa, yaklaşık değerler kullan
	if totalMB == 0 || usedMB == 0 || freeMB == 0 {
		log.Printf("UYARI: Bellek hesaplamaları sıfır değerler içeriyor, top ile deneniyor")
		return getRAMUsageWithTop()
	}

	// Kullanım yüzdesini hesapla
	usagePercent := (float64(usedMB) / float64(totalMB)) * 100

	// Debug log
	log.Printf("RAM Debug - Page Size: %d, Total MB: %d, Used MB: %d, Free MB: %d, Usage: %.2f%%",
		pageSize, totalMB, usedMB, freeMB, usagePercent)

	return map[string]interface{}{
		"total_mb":      int64(totalMB),
		"used_mb":       int64(usedMB),
		"free_mb":       int64(freeMB),
		"usage_percent": usagePercent,
	}, nil
}

// getRAMUsageWithTop top komutunu kullanarak RAM kullanımını alır
func getRAMUsageWithTop() (map[string]interface{}, error) {
	log.Printf("top komutu ile bellek kullanımı alınıyor...")

	// top komutunu kullanarak bellek bilgisi al
	cmd := exec.Command("sh", "-c", "top -l 1 -n 0 | grep PhysMem")
	out, err := cmd.Output()
	if err != nil {
		return map[string]interface{}{
			"total_mb":      int64(16384), // 16GB varsayılan değer
			"free_mb":       int64(4096),  // 4GB varsayılan değer
			"used_mb":       int64(12288), // 12GB varsayılan değer
			"usage_percent": 75.0,         // %75 varsayılan kullanım
		}, nil
	}

	topOutput := strings.TrimSpace(string(out))
	log.Printf("TOP output: %s", topOutput)

	// PhysMem: 10G used (1.8G wired), 6.1G unused.
	// Bu formatı parse et

	reUsed := regexp.MustCompile(`(\d+(?:\.\d+)?)G used`)
	reUnused := regexp.MustCompile(`(\d+(?:\.\d+)?)G unused`)

	var usedGB, unusedGB float64

	if matches := reUsed.FindStringSubmatch(topOutput); len(matches) > 1 {
		usedGB, _ = strconv.ParseFloat(matches[1], 64)
	}

	if matches := reUnused.FindStringSubmatch(topOutput); len(matches) > 1 {
		unusedGB, _ = strconv.ParseFloat(matches[1], 64)
	}

	// GB'den MB'ye çevir
	usedMB := int64(usedGB * 1024)
	unusedMB := int64(unusedGB * 1024)
	totalMB := usedMB + unusedMB

	// Kullanım yüzdesini hesapla
	var usagePercent float64
	if totalMB > 0 {
		usagePercent = (float64(usedMB) / float64(totalMB)) * 100
	} else {
		usagePercent = 75.0 // Varsayılan değer
	}

	log.Printf("TOP Parse - Total MB: %d, Used MB: %d, Unused MB: %d, Usage: %.2f%%",
		totalMB, usedMB, unusedMB, usagePercent)

	// Eğer değerler hala sıfırsa, varsayılan değerler kullan
	if totalMB == 0 || usedMB == 0 || unusedMB == 0 {
		log.Printf("UYARI: TOP ile bellek hesaplamaları başarısız, varsayılan değerler kullanılıyor")
		return map[string]interface{}{
			"total_mb":      int64(16384), // 16GB
			"free_mb":       int64(4096),  // 4GB
			"used_mb":       int64(12288), // 12GB
			"usage_percent": 75.0,         // %75
		}, nil
	}

	return map[string]interface{}{
		"total_mb":      totalMB,
		"used_mb":       usedMB,
		"free_mb":       unusedMB,
		"usage_percent": usagePercent,
	}, nil
}

// getDiskUsage disk kullanım bilgilerini döndürür
func getDiskUsage() (map[string]interface{}, error) {
	cmd := exec.Command("sh", "-c", "df -g / | tail -1")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	fields := strings.Fields(string(out))
	if len(fields) < 5 {
		return nil, fmt.Errorf("unexpected df command output format")
	}

	total := strings.TrimSuffix(fields[1], "Gi")
	used := strings.TrimSuffix(fields[2], "Gi")
	avail := strings.TrimSuffix(fields[3], "Gi")

	totalGB, _ := strconv.ParseInt(total, 10, 64)
	usedGB, _ := strconv.ParseInt(used, 10, 64)
	availGB, _ := strconv.ParseInt(avail, 10, 64)

	return map[string]interface{}{
		"total_gb": totalGB,
		"used_gb":  usedGB,
		"avail_gb": availGB,
	}, nil
}

// getOSInfo işletim sistemi ve kernel bilgilerini döndürür
func getOSInfo() (map[string]string, error) {
	osInfo := make(map[string]string)

	// OS versiyonu
	cmd := exec.Command("sh", "-c", "sw_vers -productVersion")
	if out, err := cmd.Output(); err == nil {
		osVersion := "macOS " + strings.TrimSpace(string(out))
		// Boyut kontrolü - 50 karakterden uzunsa kısalt
		if len(osVersion) > 50 {
			osVersion = osVersion[:50]
		}
		osInfo["os_version"] = osVersion
		log.Printf("OS Version: %s (length: %d)", osVersion, len(osVersion))
	}

	// Kernel versiyonu
	cmd = exec.Command("sh", "-c", "uname -r")
	if out, err := cmd.Output(); err == nil {
		kernelVersion := strings.TrimSpace(string(out))
		// Boyut kontrolü - 50 karakterden uzunsa kısalt
		if len(kernelVersion) > 50 {
			kernelVersion = kernelVersion[:50]
		}
		osInfo["kernel_version"] = kernelVersion
		log.Printf("Kernel Version: %s (length: %d)", kernelVersion, len(kernelVersion))
	}

	return osInfo, nil
}

// getUptime sistemin çalışma süresini saniye cinsinden döndürür
func getUptime() (int64, error) {
	cmd := exec.Command("sh", "-c", "sysctl -n kern.boottime | awk '{print $4}' | tr -d ','")
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	bootTime, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, err
	}
	return time.Now().Unix() - bootTime, nil
}

// isMatchingPostgresLogName dosya adının PostgreSQL log dosyası kalıbına uyup uymadığını kontrol eder
func isMatchingPostgresLogName(fileName string) bool {
	// PostgreSQL log dosyası adı kalıpları
	patterns := []string{
		"postgresql",
		"postgres",
		"pg_log",
		"pglog",
		"pgsql",
	}

	lowerName := strings.ToLower(fileName)

	// Dosya adı kalıplarını kontrol et
	for _, pattern := range patterns {
		if strings.HasPrefix(lowerName, pattern) {
			return true
		}
	}

	// Tarih formatı içeren log dosyaları kontrolü (postgresql-2023-06-01.log gibi)
	postgresLogPattern := regexp.MustCompile(`(postgres|postgresql|pg_log|pglog).*\d{4}[-_]\d{2}[-_]\d{2}`)
	if postgresLogPattern.MatchString(lowerName) {
		return true
	}

	return false
}

// isPostgresArtifact bir dosyanın PostgreSQL log dosyası olup olmadığını kontrol eder
func isPostgresArtifact(name string) bool {
	// Dosya adını küçük harfe çevir
	nameLower := strings.ToLower(name)

	// 1. Dosya uzantısı kontrolü
	if !strings.HasSuffix(nameLower, ".log") &&
		!strings.HasSuffix(nameLower, ".csv") &&
		!strings.HasSuffix(nameLower, ".log.gz") {
		return false
	}

	// 2. PostgreSQL ile ilgili anahtar kelimeler kontrolü
	if !isMatchingPostgresLogName(nameLower) {
		return false
	}

	return true
}

// FindPostgresLogFiles PostgreSQL log dosyalarını bulur ve listeler
func FindPostgresLogFiles(logPath string) ([]*pb.PostgresLogFile, error) {
	// PostgreSQL çalışıyor mu kontrol et
	pgRunning := isPgRunning()
	if !pgRunning {
		return nil, fmt.Errorf("PostgreSQL servisi çalışmıyor, log dosyaları listelenemedi")
	}

	// Eğer logPath belirtilmemişse, varsayılan olarak bilinen lokasyonları kontrol et
	if logPath == "" {
		// PostgreSQL konfigürasyon dosyasını bul
		configFile, err := findPostgresConfigFile()
		if err == nil {
			// Konfigürasyondan log path'i oku
			if path := getLogPathFromConfig(configFile); path != "" {
				logPath = path
				log.Printf("PostgreSQL log dizini konfigürasyon dosyasından bulundu: %s", path)
			}
		}

		// Hala log path bulunamadıysa, bilinen dizinleri kontrol et
		if logPath == "" {
			// Bilinen olası PostgreSQL log dizinleri
			logDirs := []string{
				"/var/log/postgresql",
				"/var/log/postgres",
				"/var/lib/postgresql/*/main/pg_log",
				"/var/lib/postgresql/*/log",
				"/var/lib/pgsql/data/log",
				"/var/lib/pgsql/data/pg_log",
				"/usr/local/var/postgres/log",
				"/usr/local/var/postgres/pg_log",
				"/usr/local/pgsql/data/pg_log",
				"/usr/local/pgsql/data/log",
				"/opt/homebrew/var/postgres/log", // macOS Homebrew Apple Silicon
				"/usr/local/var/log/postgresql",
				"/data/postgres/*/log",
				"/data/postgres/*/pg_log",
			}

			// PostgreSQL veri dizinini bulmayı dene
			dataDir, err := getDataDirectoryFromConfig()
			if err == nil && dataDir != "" {
				// Data dizinindeki log dizinini kontrol et
				logDirs = append([]string{
					filepath.Join(dataDir, "log"),
					filepath.Join(dataDir, "pg_log"),
					filepath.Join(dataDir, "logs"),
				}, logDirs...)
				log.Printf("PostgreSQL veri dizini bulundu, log için kontrol ediliyor: %s/{log,pg_log,logs}", dataDir)
			}

			// İlk bulunan geçerli dizini kullan
			for _, dirPattern := range logDirs {
				// Glob pattern'ı destekle
				matches, err := filepath.Glob(dirPattern)
				if err != nil || len(matches) == 0 {
					continue
				}

				for _, dir := range matches {
					if _, err := os.Stat(dir); err == nil {
						logPath = dir
						log.Printf("PostgreSQL log dizini bulundu: %s", logPath)
						break
					}
				}
				if logPath != "" {
					break
				}
			}
		}
	}

	if logPath == "" {
		return nil, fmt.Errorf("PostgreSQL log dizini bulunamadı")
	}

	// logPath'in var olup olmadığını kontrol et
	info, err := os.Stat(logPath)
	if err != nil {
		return nil, fmt.Errorf("belirtilen log dizini bulunamadı: %v", err)
	}

	var logFiles []*pb.PostgresLogFile

	// Eğer belirtilen path bir dosya ise ve PostgreSQL log dosyası ise, direkt olarak onu ekle
	if !info.IsDir() {
		if isPostgresArtifact(filepath.Base(logPath)) {
			file := &pb.PostgresLogFile{
				Name:         filepath.Base(logPath),
				Path:         logPath,
				Size:         info.Size(),
				LastModified: info.ModTime().Unix(),
			}
			return []*pb.PostgresLogFile{file}, nil
		}
		return nil, fmt.Errorf("belirtilen dosya bir PostgreSQL log dosyası değil: %s", logPath)
	}

	// Dizindeki tüm dosyaları listele
	entries, err := os.ReadDir(logPath)
	if err != nil {
		return nil, fmt.Errorf("dizin içeriği listelenemedi: %v", err)
	}

	// Her bir dosyayı kontrol et
	for _, entry := range entries {
		// Sadece dosyaları işle
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		if isPostgresArtifact(fileName) {
			fileInfo, err := os.Stat(filepath.Join(logPath, fileName))
			if err != nil {
				log.Printf("Dosya bilgileri alınamadı: %v", err)
				continue
			}

			file := &pb.PostgresLogFile{
				Name:         fileName,
				Path:         filepath.Join(logPath, fileName),
				Size:         fileInfo.Size(),
				LastModified: fileInfo.ModTime().Unix(),
			}
			logFiles = append(logFiles, file)
			log.Printf("PostgreSQL log dosyası bulundu: %s", file.Path)
		}
	}

	if len(logFiles) == 0 {
		log.Printf("Belirtilen dizinde (%s) PostgreSQL log dosyası bulunamadı", logPath)
	} else {
		log.Printf("%d adet PostgreSQL log dosyası bulundu", len(logFiles))
	}

	return logFiles, nil
}

// isPgRunning PostgreSQL servisinin çalışıp çalışmadığını kontrol eder
func isPgRunning() bool {
	cmd := exec.Command("pgrep", "postgres")
	err := cmd.Run()
	return err == nil
}

// findPostgresLogPathFromProcess PostgreSQL process'inden log dosyası yolunu bulmayı dener
func findPostgresLogPathFromProcess() string {
	// 1. ps ile tüm PostgreSQL süreçlerini bul
	cmd := exec.Command("sh", "-c", "ps -ef | grep postgres | grep -v grep")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		// -D parametresinden sonra data directory'yi bul
		if idx := strings.Index(line, "-D"); idx != -1 {
			parts := strings.Fields(line[idx:])
			if len(parts) > 1 {
				dataDir := parts[1]
				// Data directory içinde log ve pg_log dizinlerini kontrol et
				for _, logDir := range []string{"log", "pg_log", "logs"} {
					fullPath := filepath.Join(dataDir, logDir)
					if _, err := os.Stat(fullPath); err == nil {
						return fullPath
					}
				}
			}
		}

		// -l veya --log parametresini ara
		if idx := strings.Index(line, "-l "); idx != -1 || strings.Index(line, "--log=") != -1 {
			parts := strings.Fields(line[idx:])
			if len(parts) > 0 {
				logArg := parts[0]
				if strings.Contains(logArg, "=") {
					parts = strings.Split(logArg, "=")
					if len(parts) > 1 {
						return strings.TrimSpace(parts[1])
					}
				} else if len(parts) > 1 {
					return strings.TrimSpace(parts[1])
				}
			}
		}
	}

	return ""
}

// findLogFileFromOpenFD açık dosya tanımlayıcılarını kontrol ederek PostgreSQL log dosyalarını bulmayı dener
func findLogFileFromOpenFD() string {
	// PostgreSQL PID'sini bul
	cmd := exec.Command("sh", "-c", "pgrep postgres")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	pid := strings.TrimSpace(string(out))
	if pid == "" {
		return ""
	}

	// lsof ile açık dosyaları listele
	cmd = exec.Command("sh", "-c", fmt.Sprintf("lsof -p %s | grep -i -E '(log|postgres)'", pid))
	out, err = cmd.Output()
	if err != nil {
		return ""
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 9 {
			filePath := fields[8]
			// Dosya yolunu kontrol et
			if strings.Contains(strings.ToLower(filePath), "postgres") && (strings.HasSuffix(strings.ToLower(filePath), ".log") ||
				strings.Contains(strings.ToLower(filePath), "pg_log") ||
				strings.Contains(strings.ToLower(filePath), "postgresql")) {
				return filePath
			}
		}
	}

	return ""
}

// isPostgresLogFile dosya içeriğini kontrol ederek PostgreSQL log formatına uygun olup olmadığını belirler
func isPostgresLogFile(filePath string) bool {
	// Dosyanın ilk birkaç satırını oku
	file, err := os.Open(filePath)
	if err != nil {
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	linesChecked := 0
	for scanner.Scan() && linesChecked < 5 {
		line := scanner.Text()
		// PostgreSQL log satırları genellikle tarih formatı ve bazı anahtar kelimeler içerir
		if strings.Contains(line, "postgres") ||
			strings.Contains(line, "postgresql") ||
			strings.Contains(line, "LOG:") ||
			strings.Contains(line, "ERROR:") ||
			strings.Contains(line, "FATAL:") ||
			strings.Contains(line, "WARNING:") ||
			strings.Contains(line, "HINT:") ||
			strings.Contains(line, "STATEMENT:") {
			return true
		}
		linesChecked++
	}

	return false
}

// checkPostgresFileDescriptors açık dosya tanımlayıcılarını kontrol ederek PostgreSQL log dosyalarını bulmayı dener
func checkPostgresFileDescriptors() string {
	// PostgreSQL PID'sini bul
	cmd := exec.Command("sh", "-c", "pgrep postgres")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	pid := strings.TrimSpace(string(out))
	if pid == "" {
		return ""
	}

	// lsof ile açık dosyaları listele
	cmd = exec.Command("sh", "-c", fmt.Sprintf("lsof -p %s | grep -i 'log'", pid))
	out, err = cmd.Output()
	if err != nil {
		return ""
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 9 {
			filePath := fields[8]
			// Dosya yolunu kontrol et
			if strings.HasSuffix(strings.ToLower(filePath), ".log") {
				return filePath
			}
		}
	}

	return ""
}

// getLogPathFromConfig postgresql.conf dosyasından log_directory ve log_filename parametrelerini okur
func getLogPathFromConfig(configFile string) string {
	content, err := os.ReadFile(configFile)
	if err != nil {
		log.Printf("Konfigürasyon dosyası okunamadı: %v", err)
		return ""
	}

	lines := strings.Split(string(content), "\n")

	var logDirectory string

	for _, line := range lines {
		// Yorumları kaldır ve boşlukları temizle
		line = strings.Split(line, "#")[0]
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "log_directory") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				logDirectory = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
				break
			}
		}
	}

	// Sadece log_directory bulundu ise
	if logDirectory != "" {
		if strings.HasPrefix(logDirectory, "/") {
			// Mutlak yol
			return logDirectory
		} else {
			// Göreceli yol, data directory ile birleştir
			dataDir, err := getDataDirectoryFromConfig()
			if err == nil && dataDir != "" {
				return filepath.Join(dataDir, logDirectory)
			}
		}
	}

	return ""
}
