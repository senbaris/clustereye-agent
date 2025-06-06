package postgres

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
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
	// Konfigürasyonu yükle
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		log.Printf("Konfigürasyon yüklenemedi: %v", err)
		return "FAIL!"
	}

	// PostgreSQL host ve port bilgilerini al
	host := cfg.PostgreSQL.Host
	if host == "" {
		host = "localhost"
	}

	port := cfg.PostgreSQL.Port
	if port == "" {
		port = "5432" // varsayılan PostgreSQL portu
	}

	// TCP bağlantısı ile port kontrolü yap
	address := fmt.Sprintf("%s:%s", host, port)
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		log.Printf("PostgreSQL at %s is not accessible: %v", address, err)
		return "FAIL!"
	}
	if conn != nil {
		conn.Close()
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

// FindPostgresConfigFile PostgreSQL konfigürasyon dosyasını bulur
func FindPostgresConfigFile() (string, error) {
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
	configFile, err := FindPostgresConfigFile()
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

// GetDataDirectory postgresql.conf dosyasından data_directory parametresini okur (public wrapper)
func GetDataDirectory() (string, error) {
	return getDataDirectoryFromConfig()
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
	// Windows veya Unix tabanlı sistemlere göre farklı fonksiyonları çağır
	if runtime.GOOS == "windows" {
		return getWindowsSystemMetrics()
	}

	// Unix/Linux/macOS sistemler için mevcut implementasyon
	return getUnixSystemMetrics()
}

// getWindowsSystemMetrics Windows işletim sistemlerinden sistem metriklerini toplar
func getWindowsSystemMetrics() *pb.SystemMetrics {
	metrics := &pb.SystemMetrics{}

	// CPU kullanımı
	cpuPercent, err := getWindowsCPUUsage()
	if err == nil {
		metrics.CpuUsage = cpuPercent
	} else {
		log.Printf("Windows CPU kullanımı alınamadı: %v", err)
	}

	// CPU çekirdek sayısı
	cpuCores, err := getWindowsCPUCores()
	if err == nil {
		metrics.CpuCores = cpuCores
	} else {
		log.Printf("Windows CPU çekirdek sayısı alınamadı: %v", err)
	}

	// Windows'ta doğrudan Load Average kavramı yok, ancak benzer bir şey hesaplayabiliriz
	// CPU kullanımını farklı aralıklarla ölçerek
	metrics.LoadAverage_1M = metrics.CpuUsage / 100 // 0-1 aralığına normalize et
	metrics.LoadAverage_5M = metrics.CpuUsage / 100
	metrics.LoadAverage_15M = metrics.CpuUsage / 100

	// RAM kullanımı
	ramUsage, err := getWindowsRAMUsage()
	if err == nil {
		metrics.TotalMemory = ramUsage["total_mb"].(int64)
		metrics.FreeMemory = ramUsage["free_mb"].(int64)
		metrics.MemoryUsage = ramUsage["usage_percent"].(float64)
	} else {
		log.Printf("Windows RAM kullanımı alınamadı: %v", err)
	}

	// Disk kullanımı
	diskUsage, err := getWindowsDiskUsage()
	if err == nil {
		metrics.TotalDisk = diskUsage["total_gb"].(int64)
		metrics.FreeDisk = diskUsage["avail_gb"].(int64)
	} else {
		log.Printf("Windows disk kullanımı alınamadı: %v", err)
	}

	// OS ve Kernel bilgileri
	osInfo, err := getWindowsOSInfo()
	if err == nil {
		metrics.OsVersion = osInfo["os_version"]
		metrics.KernelVersion = osInfo["kernel_version"]
	} else {
		log.Printf("Windows OS bilgileri alınamadı: %v", err)
	}

	// Uptime
	uptime, err := getWindowsUptime()
	if err == nil {
		metrics.Uptime = uptime
	} else {
		log.Printf("Windows uptime bilgisi alınamadı: %v", err)
	}

	return metrics
}

// getUnixSystemMetrics Unix tabanlı işletim sistemlerinden (Linux, macOS) sistem metriklerini toplar
func getUnixSystemMetrics() *pb.SystemMetrics {
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

// Windows için CPU kullanımı
func getWindowsCPUUsage() (float64, error) {
	// PowerShell kullanarak CPU kullanımını al
	cmd := exec.Command("powershell", "-Command", "Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average | Select-Object -ExpandProperty Average")
	out, err := cmd.Output()
	if err != nil {
		// Alternatif yöntem - WMIC
		cmd = exec.Command("wmic", "cpu", "get", "loadpercentage")
		out, err = cmd.Output()
		if err != nil {
			return 0, fmt.Errorf("Windows CPU kullanımı alınamadı: %v", err)
		}

		// WMIC çıktısını parse et
		lines := strings.Split(string(out), "\n")
		if len(lines) < 2 {
			return 0, fmt.Errorf("Geçersiz WMIC çıktısı")
		}

		// İkinci satırı al (ilk satır başlık)
		loadStr := strings.TrimSpace(lines[1])
		load, err := strconv.ParseFloat(loadStr, 64)
		if err != nil {
			return 0, fmt.Errorf("CPU yükü parse edilemedi: %v", err)
		}

		return load, nil
	}

	// PowerShell çıktısını parse et
	cpuPercent, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0, fmt.Errorf("CPU yüzdesi parse edilemedi: %v", err)
	}

	return cpuPercent, nil
}

// Windows için CPU çekirdek sayısı
func getWindowsCPUCores() (int32, error) {
	// PowerShell kullanarak mantıksal işlemci sayısını al
	cmd := exec.Command("powershell", "-Command", "(Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors")
	out, err := cmd.Output()
	if err != nil {
		// Alternatif yöntem - WMIC
		cmd = exec.Command("wmic", "cpu", "get", "NumberOfLogicalProcessors")
		out, err = cmd.Output()
		if err != nil {
			return 0, fmt.Errorf("CPU çekirdek sayısı alınamadı: %v", err)
		}

		// WMIC çıktısını parse et
		lines := strings.Split(string(out), "\n")
		if len(lines) < 2 {
			return 0, fmt.Errorf("Geçersiz WMIC çıktısı")
		}

		// İkinci satırı al
		coresStr := strings.TrimSpace(lines[1])
		cores, err := strconv.ParseInt(coresStr, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("CPU çekirdek sayısı parse edilemedi: %v", err)
		}

		return int32(cores), nil
	}

	// PowerShell çıktısını parse et
	cores, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("CPU çekirdek sayısı parse edilemedi: %v", err)
	}

	return int32(cores), nil
}

// Windows için RAM kullanımı
func getWindowsRAMUsage() (map[string]interface{}, error) {
	var totalMB, freeMB, usedMB int64
	var usagePercent float64

	// PowerShell komutlarının çalışması için birden fazla yöntem deneyin
	methods := []func() (int64, int64, error){
		// Yöntem 1: Get-CimInstance kullanma
		func() (int64, int64, error) {
			cmd := exec.Command("powershell", "-Command", "Get-CimInstance Win32_OperatingSystem | Select-Object TotalVisibleMemorySize, FreePhysicalMemory")
			out, err := cmd.Output()
			if err != nil {
				return 0, 0, err
			}

			// PowerShell çıktısını parse et
			lines := strings.Split(string(out), "\n")
			if len(lines) < 3 {
				return 0, 0, fmt.Errorf("Geçersiz bellek bilgisi çıktısı")
			}

			var totalMemKB, freeMemKB int64

			// İlk satır başlık, ikinci satır boş, üçüncü satır değerler
			valueLines := lines[2:]
			for _, line := range valueLines {
				if line == "" {
					continue
				}

				// Değerleri ayır
				values := strings.Fields(line)
				if len(values) >= 2 {
					totalMemKB, _ = strconv.ParseInt(values[0], 10, 64)
					freeMemKB, _ = strconv.ParseInt(values[1], 10, 64)
					break
				}
			}

			// KB'den MB'ye çevir
			totalMB := totalMemKB / 1024
			freeMB := freeMemKB / 1024

			// Sıfır kontrolü
			if totalMB <= 0 || freeMB <= 0 {
				return 0, 0, fmt.Errorf("Geçersiz bellek değerleri")
			}

			return totalMB, freeMB, nil
		},
		// Yöntem 2: WMIC ComputerSystem kullanma
		func() (int64, int64, error) {
			// Toplam fiziksel bellek
			cmd := exec.Command("wmic", "ComputerSystem", "get", "TotalPhysicalMemory")
			out, err := cmd.Output()
			if err != nil {
				return 0, 0, err
			}

			// Çıktıyı parse et
			lines := strings.Split(string(out), "\n")
			if len(lines) < 2 {
				return 0, 0, fmt.Errorf("Geçersiz bellek bilgisi çıktısı")
			}

			// İkinci satırı al (ilk satır başlık)
			memStr := strings.TrimSpace(lines[1])
			totalBytes, err := strconv.ParseInt(memStr, 10, 64)
			if err != nil {
				return 0, 0, err
			}

			// Boş bellek için OS kullanma
			cmd = exec.Command("wmic", "OS", "get", "FreePhysicalMemory")
			out, err = cmd.Output()
			if err != nil {
				return 0, 0, err
			}

			// Çıktıyı parse et
			lines = strings.Split(string(out), "\n")
			if len(lines) < 2 {
				return 0, 0, fmt.Errorf("Geçersiz boş bellek bilgisi çıktısı")
			}

			// İkinci satırı al
			freeStr := strings.TrimSpace(lines[1])
			freeKB, err := strconv.ParseInt(freeStr, 10, 64)
			if err != nil {
				return 0, 0, err
			}

			// Byte'dan MB'ye ve KB'den MB'ye çevir
			totalMB := totalBytes / (1024 * 1024)
			freeMB := freeKB / 1024

			// Sıfır kontrolü
			if totalMB <= 0 || freeMB <= 0 {
				return 0, 0, fmt.Errorf("Geçersiz bellek değerleri")
			}

			return totalMB, freeMB, nil
		},
		// Yöntem 3: memory çiplerini toplama
		func() (int64, int64, error) {
			// Toplam RAM'i hafıza çiplerinden hesapla
			cmd := exec.Command("wmic", "memorychip", "get", "Capacity")
			out, err := cmd.Output()
			if err != nil {
				return 0, 0, err
			}

			// Çıktıyı parse et
			lines := strings.Split(string(out), "\n")
			if len(lines) < 2 {
				return 0, 0, fmt.Errorf("Geçersiz memorychip çıktısı")
			}

			var totalBytes int64
			// İlk satır başlık, diğer satırlar RAM modülleri
			for i := 1; i < len(lines); i++ {
				line := strings.TrimSpace(lines[i])
				if line == "" {
					continue
				}

				bytes, err := strconv.ParseInt(line, 10, 64)
				if err != nil {
					continue
				}
				totalBytes += bytes
			}

			if totalBytes <= 0 {
				return 0, 0, fmt.Errorf("Geçersiz toplam bellek değeri")
			}

			// Boş bellek tahmini (genellikle toplam belleğin %25'i kadar)
			// Daha doğru değer için OS'dan FreePhysicalMemory değerini almalıyız
			totalMB := totalBytes / (1024 * 1024)
			freeMB := totalMB / 4 // Yaklaşık tahmin

			return totalMB, freeMB, nil
		},
		// Yöntem 4: Systeminformation kullanma (daha yeni PowerShell sürümleri için)
		func() (int64, int64, error) {
			cmd := exec.Command("powershell", "-Command", "Get-CimInstance -ClassName Win32_PhysicalMemory | Measure-Object -Property Capacity -Sum | Select-Object -ExpandProperty Sum")
			out, err := cmd.Output()
			if err != nil {
				return 0, 0, err
			}

			// Toplam RAM
			totalBytes, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err != nil {
				return 0, 0, err
			}

			// Boş RAM
			cmd = exec.Command("powershell", "-Command", "[math]::Round((Get-Counter '\\Memory\\Available MBytes').CounterSamples.CookedValue)")
			out, err = cmd.Output()
			if err != nil {
				// Sadece toplam RAM biliniyorsa, yaklaşık değer kullan
				totalMB := totalBytes / (1024 * 1024)
				freeMB := totalMB / 4 // Yaklaşık tahmin
				return totalMB, freeMB, nil
			}

			freeMB, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err != nil {
				// Sadece toplam RAM biliniyorsa, yaklaşık değer kullan
				totalMB := totalBytes / (1024 * 1024)
				freeMB := totalMB / 4 // Yaklaşık tahmin
				return totalMB, freeMB, nil
			}

			totalMB := totalBytes / (1024 * 1024)
			return totalMB, freeMB, nil
		},
	}

	// Tüm yöntemleri dene
	var methodSuccess bool
	for _, method := range methods {
		total, free, methodErr := method()
		if methodErr == nil && total > 0 && free > 0 {
			totalMB = total
			freeMB = free
			methodSuccess = true
			log.Printf("RAM bilgisi başarıyla alındı: %d MB toplam, %d MB boş", totalMB, freeMB)
			break
		}
	}

	// Tüm yöntemler başarısız olursa, düşük varsayılan değerler kullan
	if !methodSuccess || totalMB <= 0 || freeMB <= 0 {
		log.Printf("Windows RAM bilgisi alınamadı, varsayılan değerler kullanılıyor (4GB)")
		totalMB = 4096 // 4GB
		freeMB = 1024  // 1GB
		usedMB = 3072  // 3GB
		usagePercent = 75.0
	} else {
		// Kullanılan bellek ve yüzde hesapla
		usedMB = totalMB - freeMB
		usagePercent = (float64(usedMB) / float64(totalMB)) * 100
	}

	return map[string]interface{}{
		"total_mb":      totalMB,
		"free_mb":       freeMB,
		"used_mb":       usedMB,
		"usage_percent": usagePercent,
	}, nil
}

// Windows için disk kullanımı
func getWindowsDiskUsage() (map[string]interface{}, error) {
	// PowerShell kullanarak C: sürücüsünün kullanımını al
	cmd := exec.Command("powershell", "-Command", "Get-PSDrive C | Select-Object Used, Free")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("Disk kullanımı alınamadı: %v", err)
	}

	// PowerShell çıktısını parse et
	lines := strings.Split(string(out), "\n")
	if len(lines) < 3 {
		return nil, fmt.Errorf("Geçersiz disk bilgisi çıktısı")
	}

	var usedBytes, freeBytes int64

	// İlk satır başlık, ikinci satır boş, üçüncü satır değerler
	valueLines := lines[2:]
	for _, line := range valueLines {
		if line == "" {
			continue
		}

		// Değerleri ayır
		values := strings.Fields(line)
		if len(values) >= 2 {
			usedBytes, _ = strconv.ParseInt(values[0], 10, 64)
			freeBytes, _ = strconv.ParseInt(values[1], 10, 64)
			break
		}
	}

	// Bayt'tan GB'ye çevir
	usedGB := usedBytes / (1024 * 1024 * 1024)
	freeGB := freeBytes / (1024 * 1024 * 1024)
	totalGB := usedGB + freeGB

	return map[string]interface{}{
		"total_gb": totalGB,
		"used_gb":  usedGB,
		"avail_gb": freeGB,
	}, nil
}

// Windows için OS bilgileri
func getWindowsOSInfo() (map[string]string, error) {
	// Windows sürümünü al
	cmd := exec.Command("powershell", "-Command", "(Get-CimInstance Win32_OperatingSystem).Caption")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("Windows sürümü alınamadı: %v", err)
	}

	osVersion := strings.TrimSpace(string(out))

	// Build numarasını al
	cmd = exec.Command("powershell", "-Command", "(Get-CimInstance Win32_OperatingSystem).BuildNumber")
	out, err = cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("Windows build numarası alınamadı: %v", err)
	}

	buildNumber := strings.TrimSpace(string(out))

	// Tam sürüm bilgisini oluştur
	fullVersion := fmt.Sprintf("%s (Build %s)", osVersion, buildNumber)

	return map[string]string{
		"os_version":     fullVersion,
		"kernel_version": buildNumber,
	}, nil
}

// Windows için uptime
func getWindowsUptime() (int64, error) {
	// Son başlangıç zamanını al
	cmd := exec.Command("powershell", "-Command", "(Get-CimInstance Win32_OperatingSystem).LastBootUpTime")
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("Son başlangıç zamanı alınamadı: %v", err)
	}

	// Çıktıyı parse et - WMI DateTime formatı: 20210920095731.123456+180
	bootTimeStr := strings.TrimSpace(string(out))

	// Şu anki zamanı al
	now := time.Now()

	// ParseDateTime fonksiyonu olmadığından, basit bir yöntem kullanarak tarih/saat bilgisini çıkar
	// Format: YearMonthDayHourMinuteSecond.Milliseconds+Timezone
	var bootTime time.Time

	// Format dönüşümü için düzenli ifade
	re := regexp.MustCompile(`(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})`)
	matches := re.FindStringSubmatch(bootTimeStr)

	if len(matches) >= 7 {
		year, _ := strconv.Atoi(matches[1])
		month, _ := strconv.Atoi(matches[2])
		day, _ := strconv.Atoi(matches[3])
		hour, _ := strconv.Atoi(matches[4])
		minute, _ := strconv.Atoi(matches[5])
		second, _ := strconv.Atoi(matches[6])

		bootTime = time.Date(year, time.Month(month), day, hour, minute, second, 0, time.Local)
	} else {
		// Eğer parse edilemezse alternatif komut dene
		cmd = exec.Command("powershell", "-Command", "(Get-Date) - (Get-CimInstance Win32_OperatingSystem).LastBootUpTime | Select-Object -ExpandProperty TotalSeconds")
		out, err = cmd.Output()
		if err != nil {
			return 0, fmt.Errorf("Uptime hesaplanamadı: %v", err)
		}

		uptimeSeconds, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		if err != nil {
			return 0, fmt.Errorf("Uptime parse edilemedi: %v", err)
		}

		return int64(uptimeSeconds), nil
	}

	// Uptime hesapla (saniye cinsinden)
	uptimeDuration := now.Sub(bootTime)
	uptimeSeconds := int64(uptimeDuration.Seconds())

	return uptimeSeconds, nil
}

// getCPUUsage CPU kullanım yüzdesini döndürür
func getCPUUsage() (float64, error) {
	// Linux sistemlerde /proc/stat kullan
	if _, err := os.Stat("/proc/stat"); err == nil {
		// İlk ölçüm
		cpu1, err := readCPUStat()
		if err != nil {
			log.Printf("İlk CPU ölçümü hatası: %v", err)
			goto AlternativeMethod
		}

		// 500ms bekle (daha uzun süre ile daha doğru ölçüm)
		time.Sleep(500 * time.Millisecond)

		// İkinci ölçüm
		cpu2, err := readCPUStat()
		if err != nil {
			log.Printf("İkinci CPU ölçümü hatası: %v", err)
			goto AlternativeMethod
		}

		// Değişimleri hesapla
		userDiff := cpu2.user - cpu1.user
		niceDiff := cpu2.nice - cpu1.nice
		systemDiff := cpu2.system - cpu1.system
		idleDiff := cpu2.idle - cpu1.idle
		iowaitDiff := cpu2.iowait - cpu1.iowait
		irqDiff := cpu2.irq - cpu1.irq
		softirqDiff := cpu2.softirq - cpu1.softirq
		stealDiff := cpu2.steal - cpu1.steal
		totalDiff := cpu2.total - cpu1.total

		log.Printf("CPU farkları - User: %d, System: %d, Idle: %d, IOWait: %d, Total: %d",
			userDiff, systemDiff, idleDiff, iowaitDiff, totalDiff)

		if totalDiff == 0 {
			log.Printf("UYARI: Total diff 0, alternatif yönteme geçiliyor")
			goto AlternativeMethod
		}

		// CPU kullanımını hesapla (user + nice + system + irq + softirq + steal)
		activeDiff := userDiff + niceDiff + systemDiff + irqDiff + softirqDiff + stealDiff
		cpuUsage := (float64(activeDiff) / float64(totalDiff)) * 100

		// Geçerlilik kontrolü
		if cpuUsage < 0 || cpuUsage > 100 {
			log.Printf("UYARI: Geçersiz CPU kullanımı (%f), alternatif yönteme geçiliyor", cpuUsage)
			goto AlternativeMethod
		}

		log.Printf("Hesaplanan CPU kullanımı: %f", cpuUsage)
		return cpuUsage, nil
	}

AlternativeMethod:
	// Alternatif yöntem - mpstat kullan (daha doğru sonuçlar için)
	cmd := exec.Command("mpstat", "1", "1")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("mpstat komutu hatası: %v, top deneniyor", err)
		// top dene
		cmd = exec.Command("sh", "-c", "top -bn2 -d 0.5 | grep '^%Cpu' | tail -1 | awk '{print 100-$8}'")
		out, err = cmd.Output()
		if err != nil {
			log.Printf("top komutu hatası: %v, vmstat deneniyor", err)
			// vmstat dene
			cmd = exec.Command("sh", "-c", "vmstat 1 2 | tail -1 | awk '{print 100-$15}'")
			out, err = cmd.Output()
			if err != nil {
				log.Printf("vmstat komutu hatası: %v", err)
				return 0, err
			}
		}
	}

	cpuPercent, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		log.Printf("CPU yüzdesi parse hatası: %v", err)
		return 0, err
	}

	// Geçerlilik kontrolü
	if cpuPercent < 0 {
		cpuPercent = 0
	} else if cpuPercent > 100 {
		cpuPercent = 100
	}

	log.Printf("Alternatif yöntem CPU kullanımı: %f", cpuPercent)
	return cpuPercent, nil
}

type cpuStat struct {
	user    uint64
	nice    uint64
	system  uint64
	idle    uint64
	iowait  uint64
	irq     uint64
	softirq uint64
	steal   uint64
	total   uint64
}

func readCPUStat() (*cpuStat, error) {
	contents, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "cpu" {
			// En az 8 alan olmalı (cpu, user, nice, system, idle, iowait, irq, softirq)
			if len(fields) < 8 {
				return nil, fmt.Errorf("yetersiz CPU stat alanı")
			}

			stat := &cpuStat{}

			// Değerleri parse et
			values := make([]uint64, len(fields)-1)
			for i := 1; i < len(fields); i++ {
				val, err := strconv.ParseUint(fields[i], 10, 64)
				if err != nil {
					log.Printf("CPU stat parse hatası [%d]: %v", i, err)
					continue
				}
				values[i-1] = val
			}

			// Değerleri ata
			stat.user = values[0]
			stat.nice = values[1]
			stat.system = values[2]
			stat.idle = values[3]
			stat.iowait = values[4]
			stat.irq = values[5]
			stat.softirq = values[6]
			if len(values) > 7 {
				stat.steal = values[7]
			}

			// Toplam CPU zamanını hesapla
			stat.total = stat.user + stat.nice + stat.system + stat.idle +
				stat.iowait + stat.irq + stat.softirq + stat.steal

			log.Printf("CPU stat detaylı - User: %d, System: %d, Idle: %d, IOWait: %d, Total: %d",
				stat.user, stat.system, stat.idle, stat.iowait, stat.total)
			return stat, nil
		}
	}
	return nil, fmt.Errorf("CPU stats not found in /proc/stat")
}

// getCPUCores CPU çekirdek sayısını döndürür
func getCPUCores() (int32, error) {
	// Linux sistemlerde nproc komutunu kullan
	cmd := exec.Command("nproc")
	out, err := cmd.Output()
	if err != nil {
		// Alternatif olarak /proc/cpuinfo'dan say
		cmd = exec.Command("sh", "-c", "grep -c processor /proc/cpuinfo")
		out, err = cmd.Output()
		if err != nil {
			return 0, err
		}
	}
	cores, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(cores), nil
}

// getTotalvCpu sistemdeki toplam vCPU sayısını döndürür
func GetTotalvCpu() int32 {
	// UNIX/Linux sistemlerde nproc veya lscpu komutu kullanılabilir
	cmd := exec.Command("sh", "-c", "nproc")
	out, err := cmd.Output()
	if err != nil {
		// nproc çalışmadıysa, lscpu dene
		cmd = exec.Command("sh", "-c", "lscpu | grep 'CPU(s):' | head -n 1 | awk '{print $2}'")
		out, err = cmd.Output()
		if err != nil {
			// Hata varsa, getCPUCores'u kullan
			cores, err := getCPUCores()
			if err != nil {
				log.Printf("vCPU sayısı alınamadı: %v", err)
				return 0
			}
			return cores
		}
	}

	// Çıktıyı int32'ye çevir
	cpuCount, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
	if err != nil {
		log.Printf("vCPU sayısı parse edilemedi: %v", err)
		return 0
	}

	return int32(cpuCount)
}

// getTotalMemory sistemdeki toplam RAM miktarını byte cinsinden döndürür
func GetTotalMemory() int64 {
	// Linux sistemlerde /proc/meminfo dosyasından MemTotal değerini okuyabiliriz
	cmd := exec.Command("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'")
	out, err := cmd.Output()
	if err != nil {
		// Alternatif olarak free komutu deneyelim
		cmd = exec.Command("sh", "-c", "free -b | grep 'Mem:' | awk '{print $2}'")
		out, err = cmd.Output()
		if err != nil {
			// MacOS için sysctl'yi deneyelim
			cmd = exec.Command("sh", "-c", "sysctl -n hw.memsize")
			out, err = cmd.Output()
			if err != nil {
				log.Printf("Toplam RAM miktarı alınamadı: %v", err)
				return 0
			}
		}
	}

	// Çıktıyı int64'e çevir
	memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		log.Printf("Toplam RAM miktarı parse edilemedi: %v", err)
		return 0
	}

	// grep MemTotal kullanıldıysa KB cinsinden, bunu byte'a çevir
	if strings.Contains(cmd.String(), "MemTotal") {
		memTotal *= 1024
	}

	return memTotal
}

// getLoadAverage sistem yükünü döndürür
func getLoadAverage() ([]float64, error) {
	// Linux sistemlerde /proc/loadavg dosyasını oku
	content, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return nil, err
	}

	// Çıktı formatı: "0.00 0.00 0.00 ..."
	fields := strings.Fields(string(content))
	if len(fields) < 3 {
		return nil, fmt.Errorf("unexpected loadavg format: %s", content)
	}

	loads := make([]float64, 3)
	for i := 0; i < 3; i++ {
		load, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse load value: %s", fields[i])
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

	// OS versiyonu için /etc/os-release dosyasını oku
	content, err := os.ReadFile("/etc/os-release")
	if err == nil {
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "VERSION=") || strings.HasPrefix(line, "VERSION_ID=") {
				version := strings.Trim(strings.SplitN(line, "=", 2)[1], "\"")
				osInfo["os_version"] = version
				break
			}
		}
	}

	// Kernel versiyonu
	cmd := exec.Command("uname", "-r")
	if out, err := cmd.Output(); err == nil {
		kernelVersion := strings.TrimSpace(string(out))
		osInfo["kernel_version"] = kernelVersion
	}

	// Eğer os_version alınamadıysa, alternatif yöntem dene
	if _, exists := osInfo["os_version"]; !exists {
		if _, err := os.Stat("/etc/redhat-release"); err == nil {
			// CentOS/RHEL için
			content, err := os.ReadFile("/etc/redhat-release")
			if err == nil {
				osInfo["os_version"] = strings.TrimSpace(string(content))
			}
		} else {
			// Debian/Ubuntu için
			cmd := exec.Command("lsb_release", "-d")
			if out, err := cmd.Output(); err == nil {
				desc := strings.TrimSpace(string(out))
				if strings.HasPrefix(desc, "Description:") {
					osInfo["os_version"] = strings.TrimSpace(strings.TrimPrefix(desc, "Description:"))
				}
			}
		}
	}

	return osInfo, nil
}

// getUptime sistemin çalışma süresini saniye cinsinden döndürür
func getUptime() (int64, error) {
	// Linux sistemlerde /proc/uptime dosyasını oku
	content, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0, err
	}

	// İlk alan uptime değeridir (saniye cinsinden)
	uptime := strings.Fields(string(content))[0]
	uptimeFloat, err := strconv.ParseFloat(uptime, 64)
	if err != nil {
		return 0, err
	}

	return int64(uptimeFloat), nil
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
		configFile, err := FindPostgresConfigFile()
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

// ReadPostgresConfig belirtilen dosya yolundaki PostgreSQL konfigürasyon dosyasını okur ve
// belirtilen parametrelerin değerlerini döndürür
func ReadPostgresConfig(configPath string) ([]*pb.PostgresConfigEntry, error) {
	// Konfigürasyon dosyasını oku
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("konfigürasyon dosyası okunamadı: %v", err)
	}

	// İzlenecek parametreler
	targetParams := map[string]string{
		"max_connections":                  "Maximum number of concurrent connections",
		"shared_buffers":                   "Shared memory buffer size used by PostgreSQL",
		"effective_cache_size":             "Amount of memory available for disk caching",
		"maintenance_work_mem":             "Memory allocated for maintenance operations",
		"checkpoint_completion_target":     "Target completion time for checkpoint operations",
		"wal_buffers":                      "Memory allocated for WAL operations",
		"default_statistics_target":        "Default statistics target for table columns",
		"random_page_cost":                 "Cost estimate for random disk page access",
		"effective_io_concurrency":         "Effective I/O concurrency for disk operations",
		"work_mem":                         "Memory allocated for query operations",
		"huge_pages":                       "Use of huge memory pages",
		"min_wal_size":                     "Minimum size of WAL files",
		"max_wal_size":                     "Maximum size of WAL files",
		"max_worker_processes":             "Maximum number of background worker processes",
		"max_parallel_workers_per_gather":  "Maximum parallel workers per Gather operation",
		"max_parallel_workers":             "Maximum number of parallel workers",
		"max_parallel_maintenance_workers": "Maximum parallel workers for maintenance operations",
	}

	// Kategoriler - parametreleri gruplandırmak için
	paramCategories := map[string]string{
		"max_connections":                  "Connection",
		"shared_buffers":                   "Memory",
		"effective_cache_size":             "Memory",
		"maintenance_work_mem":             "Memory",
		"checkpoint_completion_target":     "WAL",
		"wal_buffers":                      "WAL",
		"default_statistics_target":        "Query Planning",
		"random_page_cost":                 "Query Planning",
		"effective_io_concurrency":         "Query Planning",
		"work_mem":                         "Memory",
		"huge_pages":                       "Memory",
		"min_wal_size":                     "WAL",
		"max_wal_size":                     "WAL",
		"max_worker_processes":             "Parallelism",
		"max_parallel_workers_per_gather":  "Parallelism",
		"max_parallel_workers":             "Parallelism",
		"max_parallel_maintenance_workers": "Parallelism",
	}

	lines := strings.Split(string(content), "\n")
	var configs []*pb.PostgresConfigEntry

	// Bulunan parametreleri izlemek için bir harita
	foundParams := make(map[string]bool)

	// Her satırı işle
	for _, line := range lines {
		isCommented := false

		// Yorum satırı mı kontrol et
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			// Satır yorumlanmış, # işaretini kaldır
			line = strings.TrimSpace(line[1:])
			isCommented = true
		}

		// Boş satırları atla
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		// Parametre ve değeri ayır
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		param := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Parametre hedeflenen listede mi kontrol et
		if desc, ok := targetParams[param]; ok {
			// Bu parametreyi işaretleyelim
			foundParams[param] = true

			// Değeri düzenle - tırnak işaretleri veya son noktalı virgülü kaldır
			value = strings.Trim(value, "'\"")
			value = strings.TrimSuffix(value, ";")

			// PostgresConfigEntry oluştur
			category := paramCategories[param]
			if category == "" {
				category = "Other"
			}

			configEntry := &pb.PostgresConfigEntry{
				Parameter:   param,
				Value:       value,
				Description: desc,
				IsDefault:   isCommented, // Yorum satırı ise varsayılan değer olarak işaretleyelim
				Category:    category,
			}

			configs = append(configs, configEntry)
			log.Printf("Konfigürasyon parametresi bulundu: %s = %s (Yorumlanmış: %t)", param, value, isCommented)
		}
	}

	// İstenen tüm parametreleri bulduk mu kontrol edelim
	for param := range targetParams {
		if !foundParams[param] {
			log.Printf("UYARI: '%s' parametresi config dosyasında bulunamadı", param)
		}
	}

	return configs, nil
}

// AnalyzePostgresLog PostgreSQL log dosyasını analiz eder
