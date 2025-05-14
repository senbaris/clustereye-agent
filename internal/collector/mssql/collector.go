package mssql

import (
	"context"
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

	_ "github.com/microsoft/go-mssqldb" // MSSQL driver
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
)

// MSSQLCollector mssql için veri toplama yapısı
type MSSQLCollector struct {
	cfg *config.AgentConfig
}

// MSSQLInfo SQL Server bilgilerini içeren yapı
type MSSQLInfo struct {
	ClusterName string
	IP          string
	Hostname    string
	NodeStatus  string
	Version     string
	Location    string
	Status      string
	Instance    string
	FreeDisk    string
	FdPercent   int32  // File descriptor usage percentage
	Port        string // SQL Server port
	TotalvCpu   int32  // Toplam vCPU sayısı
	TotalMemory int64  // Toplam RAM miktarı (byte cinsinden)
	ConfigPath  string // SQL Server configuration file path
	Database    string // Database name
	IsHAEnabled bool   // AlwaysOn or other HA configuration enabled
	HARole      string // Role in HA topology (PRIMARY, SECONDARY, etc.)
	Edition     string // SQL Server edition (Enterprise, Standard, etc.)
}

// NewMSSQLCollector yeni bir MSSQLCollector oluşturur
func NewMSSQLCollector(cfg *config.AgentConfig) *MSSQLCollector {
	return &MSSQLCollector{
		cfg: cfg,
	}
}

// GetClient returns a SQL Server connection
func (c *MSSQLCollector) GetClient() (*sql.DB, error) {
	var connStr string

	// Connection string components
	host := c.cfg.MSSQL.Host
	if host == "" {
		host = "localhost"
	}

	port := c.cfg.MSSQL.Port
	if port == "" {
		port = "1433" // Default SQL Server port
	}

	instance := c.cfg.MSSQL.Instance
	database := c.cfg.MSSQL.Database
	if database == "" {
		database = "master" // Default database
	}

	// If Windows authentication is enabled
	if c.cfg.MSSQL.WindowsAuth {
		if instance != "" {
			connStr = fmt.Sprintf("server=%s\\%s;database=%s;trusted_connection=yes", host, instance, database)
		} else {
			connStr = fmt.Sprintf("server=%s,%s;database=%s;trusted_connection=yes", host, port, database)
		}
	} else {
		// SQL Server authentication
		if instance != "" {
			connStr = fmt.Sprintf("server=%s\\%s;user id=%s;password=%s;database=%s",
				host, instance, c.cfg.MSSQL.User, c.cfg.MSSQL.Pass, database)
		} else {
			connStr = fmt.Sprintf("server=%s,%s;user id=%s;password=%s;database=%s",
				host, port, c.cfg.MSSQL.User, c.cfg.MSSQL.Pass, database)
		}
	}

	// Add TrustServerCertificate if needed
	if c.cfg.MSSQL.TrustCert {
		connStr += ";trustservercertificate=true"
	}

	// Additional connection parameters
	connStr += ";connection timeout=10"

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, fmt.Errorf("MSSQL bağlantısı kurulamadı: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("MSSQL bağlantı testi başarısız: %w", err)
	}

	return db, nil
}

// GetMSSQLStatus checks if SQL Server service is running by checking if the configured host:port is accessible
func (c *MSSQLCollector) GetMSSQLStatus() string {
	// First try to establish a DB connection
	db, err := c.GetClient()
	if err == nil {
		db.Close()
		return "RUNNING"
	}

	// Get SQL Server connection details from config
	host := c.cfg.MSSQL.Host
	if host == "" {
		host = "localhost"
	}

	port := c.cfg.MSSQL.Port
	if port == "" {
		port = "1433" // default SQL Server port
	}

	// If instance is specified, we can't check with TCP connection
	if c.cfg.MSSQL.Instance != "" {
		// On Windows, we can try checking the service status
		if runtime.GOOS == "windows" {
			cmd := exec.Command("sc", "query", "MSSQL$"+c.cfg.MSSQL.Instance)
			if err := cmd.Run(); err == nil {
				return "RUNNING"
			}
		}
		return "UNKNOWN" // Can't determine status via TCP for named instances
	}

	// Try to establish a TCP connection to check if the port is listening
	address := fmt.Sprintf("%s:%s", host, port)
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		log.Printf("MSSQL at %s is not accessible: %v", address, err)
		return "FAIL!"
	}
	if conn != nil {
		conn.Close()
		return "RUNNING"
	}

	return "FAIL!"
}

// GetMSSQLVersion returns SQL Server version information
func (c *MSSQLCollector) GetMSSQLVersion() (string, string) {
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return "Unknown", "Unknown"
	}
	defer db.Close()

	var version, edition string
	err = db.QueryRow("SELECT @@VERSION, SERVERPROPERTY('Edition')").Scan(&version, &edition)
	if err != nil {
		log.Printf("Versiyon bilgisi alınamadı: %v", err)
		return "Unknown", "Unknown"
	}

	// Log version değerinin tam içeriğini
	log.Printf("SQL Server versiyon ham verisi: %s", version)

	// Daha geniş bir regex pattern ile versiyonu algıla
	re := regexp.MustCompile(`Microsoft SQL Server\s+(\d+)(?:\s+\(RTM\))?.*?(\d+\.\d+\.\d+\.\d+)`)
	matches := re.FindStringSubmatch(version)
	if len(matches) > 2 {
		return matches[2], edition
	}

	// Alternatif regex pattern dene - direkt olarak sürüm numarasını bul
	re = regexp.MustCompile(`(\d+\.\d+\.\d+\.\d+)`)
	matches = re.FindStringSubmatch(version)
	if len(matches) > 1 {
		return matches[1], edition
	}

	// Eğer bu da bulamazsa, en azından SQL Server sürümünü al (2019, 2022 gibi)
	re = regexp.MustCompile(`Microsoft SQL Server\s+(\d{4})`)
	matches = re.FindStringSubmatch(version)
	if len(matches) > 1 {
		return matches[1], edition
	}

	// Hiçbir şey bulunamazsa, en kötü durum
	return "Unknown", edition
}

// GetNodeStatus returns the node's role in HA configuration
func (c *MSSQLCollector) GetNodeStatus() (string, bool) {
	// Önce önbellekteki bilgileri kontrol et
	haStateKey := "mssql_ha_state"

	// Alarm monitor cache'e erişim için bu fonksiyonu oluşturuyoruz
	haState, hasHAState := c.getHAStateFromCache(haStateKey)

	// Bağlantı dene
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)

		// Önbellekte HA bilgisi varsa onu kullan
		if hasHAState {
			log.Printf("Servis kapalı ama önbellekte HA durumu mevcut: %s, önbellekteki bilgiler kullanılıyor", haState.role)
			return haState.role, true
		}

		return "STANDALONE", false
	}
	defer db.Close()

	// Check if AlwaysOn is enabled
	var isHAEnabled int
	err = db.QueryRow(`
		SELECT CASE 
			WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 1
			ELSE 0
		END AS IsHAEnabled
	`).Scan(&isHAEnabled)

	if err != nil || isHAEnabled == 0 {
		log.Printf("AlwaysOn özelliği etkin değil veya kontrol edilemiyor: %v", err)
		return "STANDALONE", false
	}

	log.Printf("AlwaysOn özelliği etkin, node rolü tespit ediliyor...")

	// If AlwaysOn is enabled, check if this is a primary or secondary replica with a more reliable query
	var role string
	err = db.QueryRow(`
		SELECT
			CASE WHEN dm_hadr_availability_replica_states.role_desc IS NULL THEN 'UNKNOWN'
			     ELSE dm_hadr_availability_replica_states.role_desc 
			END AS role
		FROM sys.dm_hadr_availability_replica_states 
		JOIN sys.availability_replicas 
			ON dm_hadr_availability_replica_states.replica_id = availability_replicas.replica_id
		WHERE availability_replicas.replica_server_name = @@SERVERNAME
	`).Scan(&role)

	if err != nil {
		log.Printf("Node role could not be determined: %v", err)

		// Fallback için alternatif sorgu
		err = db.QueryRow(`
			SELECT CASE 
				WHEN EXISTS (
					SELECT 1 FROM sys.dm_hadr_availability_replica_states ars
					WHERE ars.role_desc = 'SECONDARY'
				) THEN 'SECONDARY'
				ELSE 'UNKNOWN'
			END
		`).Scan(&role)

		if err != nil {
			log.Printf("Fallback node role query failed: %v", err)
			return "UNKNOWN", true
		}
	}

	log.Printf("Node rolü tespit edildi: %s", role)
	return role, true
}

// getHAStateFromCache is a helper to get HA information from alarm cache
func (c *MSSQLCollector) getHAStateFromCache(key string) (struct{ role, group string }, bool) {
	// Boş durumu hazırla
	emptyState := struct{ role, group string }{"", ""}

	// alarm.go dosyasında tanımlı alarmCache gibi bir cache sistemi olmadığından
	// geçici bir dosya kullanarak son durumu saklayalım
	cacheFile := filepath.Join(os.TempDir(), "mssql_ha_state.txt")

	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return emptyState, false
	}

	parts := strings.Split(string(data), "|")
	if len(parts) >= 2 {
		return struct{ role, group string }{parts[0], parts[1]}, true
	}

	return emptyState, false
}

// saveHAStateToCache saves HA information to a cache file
func (c *MSSQLCollector) saveHAStateToCache(role, group string) {
	cacheFile := filepath.Join(os.TempDir(), "mssql_ha_state.txt")
	data := fmt.Sprintf("%s|%s", role, group)

	err := os.WriteFile(cacheFile, []byte(data), 0644)
	if err != nil {
		log.Printf("HA durumu önbelleğe kaydedilemedi: %v", err)
	} else {
		log.Printf("HA durumu önbelleğe kaydedildi: %s, %s", role, group)
	}
}

// GetHAClusterName returns the AlwaysOn Availability Group name
func (c *MSSQLCollector) GetHAClusterName() string {
	// Önce önbellekteki bilgileri kontrol et
	haState, hasHAState := c.getHAStateFromCache("mssql_ha_state")

	// Bağlantı dene
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)

		// Önbellekte HA bilgisi varsa onu kullan
		if hasHAState && haState.group != "" {
			log.Printf("Servis kapalı ama önbellekte cluster adı mevcut: %s, önbellekteki bilgiler kullanılıyor", haState.group)
			return haState.group
		}

		return ""
	}
	defer db.Close()

	log.Printf("AlwaysOn Availability Group ismi tespit ediliyor...")

	// Try to get AG name from availability groups
	var clusterName string
	err = db.QueryRow(`
		SELECT TOP 1 ag.name
		FROM sys.availability_groups ag
		JOIN sys.dm_hadr_availability_replica_states ars
			ON ag.group_id = ars.group_id
		JOIN sys.availability_replicas ar 
			ON ars.replica_id = ar.replica_id
		WHERE ar.replica_server_name = @@SERVERNAME
	`).Scan(&clusterName)

	if err != nil {
		log.Printf("İlk metod ile AG ismi tespit edilemedi: %v", err)

		// Alternatif yöntem dene - daha basit sorgu
		err = db.QueryRow(`
			SELECT TOP 1 ag.name 
			FROM sys.availability_groups ag
			JOIN sys.dm_hadr_availability_replica_states ars 
				ON ag.group_id = ars.group_id
		`).Scan(&clusterName)

		if err != nil {
			log.Printf("İkinci metod ile AG ismi tespit edilemedi: %v", err)

			// Son çare olarak "cluster_name" sistem bilgisini almayı dene
			err = db.QueryRow(`
				SELECT TOP 1 cluster_name 
				FROM sys.dm_hadr_cluster
			`).Scan(&clusterName)

			if err != nil {
				log.Printf("Cluster name could not be determined: %v", err)
				return ""
			}
		}
	}

	log.Printf("Tespit edilen Availability Group ismi: %s", clusterName)

	// Bilgileri önbelleğe kaydet
	if nodeRole, isHA := c.GetNodeStatus(); isHA {
		c.saveHAStateToCache(nodeRole, clusterName)
	}

	return clusterName
}

// GetDiskUsage returns disk usage information for the SQL Server data directory
func (c *MSSQLCollector) GetDiskUsage() (string, int) {
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return "N/A", 0
	}
	defer db.Close()

	// First get data directory
	var dataPath string
	err = db.QueryRow(`
		SELECT SERVERPROPERTY('InstanceDefaultDataPath')
	`).Scan(&dataPath)

	if err != nil || dataPath == "" {
		log.Printf("Veri dizini bilgisi alınamadı: %v", err)

		// Fallback to master database path
		err = db.QueryRow(`
			SELECT physical_name 
			FROM sys.master_files 
			WHERE database_id = 1 AND file_id = 1
		`).Scan(&dataPath)

		if err != nil || dataPath == "" {
			log.Printf("Master database yolu alınamadı: %v", err)
			return "N/A", 0
		}

		// Get directory from file path
		dataPath = filepath.Dir(dataPath)
	}

	// On Windows, use WMI query
	if runtime.GOOS == "windows" {
		// Extract drive letter
		driveLetter := strings.Split(dataPath, ":")[0]
		if driveLetter == "" {
			return "N/A", 0
		}

		// Get disk space using PowerShell
		cmd := exec.Command("powershell", "-Command",
			fmt.Sprintf("Get-Volume -DriveLetter %s | Select-Object SizeRemaining,Size | ConvertTo-Json", driveLetter))
		out, err := cmd.Output()
		if err != nil {
			log.Printf("Disk kullanım bilgileri alınamadı: %v", err)
			return "N/A", 0
		}

		// Parse output
		output := string(out)
		reFree := regexp.MustCompile(`"SizeRemaining"\s*:\s*(\d+)`)
		reTotal := regexp.MustCompile(`"Size"\s*:\s*(\d+)`)

		freeMatches := reFree.FindStringSubmatch(output)
		totalMatches := reTotal.FindStringSubmatch(output)

		if len(freeMatches) > 1 && len(totalMatches) > 1 {
			freeBytes, _ := strconv.ParseUint(freeMatches[1], 10, 64)
			totalBytes, _ := strconv.ParseUint(totalMatches[1], 10, 64)

			// Calculate usage percentage
			usedBytes := totalBytes - freeBytes
			usagePercent := int((float64(usedBytes) / float64(totalBytes)) * 100)

			// Format free space
			freeDisk := c.formatBytes(freeBytes)

			return freeDisk, usagePercent
		}

		return "N/A", 0
	}

	// For Linux systems, use df command
	cmd := exec.Command("df", "-h", dataPath)
	out, err := cmd.Output()
	if err != nil {
		log.Printf("Disk kullanım bilgileri alınamadı: %v", err)
		return "N/A", 0
	}

	// Parse df output
	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return "N/A", 0
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 5 {
		return "N/A", 0
	}

	usage := strings.TrimSuffix(fields[4], "%")
	usagePercent, _ := strconv.Atoi(usage)
	freeDisk := fields[3]

	return freeDisk, usagePercent
}

// formatBytes converts bytes to human-readable format
func (c *MSSQLCollector) formatBytes(bytes uint64) string {
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

// GetConfigPath returns the SQL Server configuration file path
func (c *MSSQLCollector) GetConfigPath() string {
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return ""
	}
	defer db.Close()

	var configPath string
	err = db.QueryRow(`
		SELECT SERVERPROPERTY('ErrorLogFileName')
	`).Scan(&configPath)

	if err != nil || configPath == "" {
		log.Printf("Error log path could not be determined: %v", err)

		// Try to find the SQL Server registry location
		if runtime.GOOS == "windows" {
			instance := c.cfg.MSSQL.Instance
			regPath := "MSSQL"
			if instance != "" {
				regPath = "MSSQL$" + instance
			}

			cmd := exec.Command("powershell", "-Command",
				fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name SQLPath", regPath))
			out, err := cmd.Output()
			if err == nil {
				rePath := regexp.MustCompile(`SQLPath\s+:\s+(.+)`)
				match := rePath.FindStringSubmatch(string(out))
				if len(match) > 1 {
					return filepath.Join(match[1], "MSSQL")
				}
			}
		}

		return ""
	}

	// Return directory containing the error log
	return filepath.Dir(configPath)
}

// GetMSSQLInfo collects SQL Server information
func (c *MSSQLCollector) GetMSSQLInfo() *MSSQLInfo {
	log.Printf("MSSQL bilgileri toplamaya başlanıyor...")

	hostname, _ := os.Hostname()
	ip := c.getLocalIP()
	log.Printf("Hostname: %s, IP: %s", hostname, ip)

	// Disk kullanımı al
	log.Printf("Disk kullanımı alınıyor...")
	freeDisk, usagePercent := c.GetDiskUsage()
	log.Printf("Disk kullanımı: %s boş, %d%% kullanımda", freeDisk, usagePercent)

	// Get version and edition
	log.Printf("SQL Server versiyon ve sürümü alınıyor...")
	version, edition := c.GetMSSQLVersion()
	log.Printf("SQL Server versiyon: %s, sürüm: %s", version, edition)

	// Get HA status
	log.Printf("High Availability durumu kontrol ediliyor...")
	nodeStatus, isHAEnabled := c.GetNodeStatus()
	log.Printf("HA durumu: %s, HA etkin: %v", nodeStatus, isHAEnabled)

	clusterName := ""
	if isHAEnabled {
		log.Printf("AlwaysOn Availability Group ismi alınıyor...")
		clusterName = c.GetHAClusterName()
		log.Printf("Cluster adı: %s", clusterName)
	} else {
		// Önbellekteki bilgileri kontrol et (service down olsa bile)
		haState, hasHAState := c.getHAStateFromCache("mssql_ha_state")
		if hasHAState && haState.group != "" {
			clusterName = haState.group
			log.Printf("Servis kapalı, önbellekteki cluster adı kullanılıyor: %s", clusterName)
			isHAEnabled = true
			nodeStatus = haState.role
		}
	}

	// System information
	log.Printf("Sistem bilgileri alınıyor...")
	totalvCpu := c.getTotalvCpu()
	totalMemory := c.getTotalMemory()
	log.Printf("vCPU: %d, Toplam Bellek: %d bayt", totalvCpu, totalMemory)

	log.Printf("Yapılandırma dosya yolu alınıyor...")
	configPath := c.GetConfigPath()
	log.Printf("Yapılandırma yolu: %s", configPath)

	// MSSQL servis durumu
	log.Printf("SQL Server servis durumu kontrol ediliyor...")
	serviceStatus := c.GetMSSQLStatus()
	log.Printf("SQL Server servis durumu: %s", serviceStatus)

	info := &MSSQLInfo{
		ClusterName: clusterName,
		IP:          ip,
		Hostname:    hostname,
		NodeStatus:  nodeStatus,
		Version:     version,
		Location:    c.cfg.MSSQL.Location,
		Status:      serviceStatus,
		Instance:    c.cfg.MSSQL.Instance,
		FreeDisk:    freeDisk,
		FdPercent:   int32(usagePercent),
		Port:        c.cfg.MSSQL.Port,
		TotalvCpu:   totalvCpu,
		TotalMemory: totalMemory,
		ConfigPath:  configPath,
		Database:    c.cfg.MSSQL.Database,
		IsHAEnabled: isHAEnabled,
		HARole:      nodeStatus,
		Edition:     edition,
	}

	log.Printf("MSSQL bilgileri toplandı: ClusterName=%s, NodeStatus=%s, Version=%s, HAEnabled=%v",
		info.ClusterName, info.NodeStatus, info.Version, info.IsHAEnabled)

	return info
}

// getLocalIP returns the local IP address
func (c *MSSQLCollector) getLocalIP() string {
	// İlk olarak standart kütüphaneyi kullanarak IP hesaplamayı dene - platform bağımsız çalışır
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					ipStr := ipnet.IP.String()
					log.Printf("Go stdlib ile bulunan IP adresi: %s", ipStr)
					return ipStr
				}
			}
		}
	}

	// Standart kütüphaneyle bulamazsak işletim sistemine özgü komutları dene
	if runtime.GOOS == "windows" {
		// Windows'ta IP adresini almak için birden fazla yöntem dene

		// 1. Sadece ipconfig/ifconfig kullanarak tüm adaptörleri kontrol et
		cmd := exec.Command("powershell", "-Command", "Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike '127.0.0.*' -and $_.IPAddress -notlike '169.254.*' } | Select-Object -First 1 -ExpandProperty IPAddress")
		out, err := cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			ipStr := strings.TrimSpace(string(out))
			log.Printf("PowerShell ile bulunan IP (yöntem 1): %s", ipStr)
			return ipStr
		}

		// 2. İkinci bir yaklaşım dene
		cmd = exec.Command("powershell", "-Command", "(Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway -ne $null -and $_.NetAdapter.Status -ne 'Disconnected' }).IPv4Address.IPAddress")
		out, err = cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			ipStr := strings.TrimSpace(string(out))
			log.Printf("PowerShell ile bulunan IP (yöntem 2): %s", ipStr)
			return ipStr
		}

		// 3. Daha temel bir IPConfig kullanımı dene
		cmd = exec.Command("powershell", "-Command", "ipconfig | Select-String -Pattern 'IPv4 Address' | ForEach-Object { $_.ToString().Trim().Split(':')[1].Trim() } | Select-Object -First 1")
		out, err = cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			ipStr := strings.TrimSpace(string(out))
			log.Printf("IPConfig ile bulunan IP: %s", ipStr)
			return ipStr
		}

		// 4. En temel yöntem olarak IP yapılandırmasını doğrudan al
		cmd = exec.Command("ipconfig")
		out, err = cmd.Output()
		if err == nil {
			// IPV4 adresini bul
			re := regexp.MustCompile(`IPv4 Address[^:]*:\s*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)`)
			matches := re.FindAllStringSubmatch(string(out), -1)
			for _, match := range matches {
				if len(match) > 1 && match[1] != "127.0.0.1" {
					ipStr := match[1]
					log.Printf("IPConfig regex ile bulunan IP: %s", ipStr)
					return ipStr
				}
			}
		}
	} else {
		// Unix/Linux sistemleri

		// 1. İlk yöntem: hostname -I
		cmd := exec.Command("sh", "-c", "hostname -I | awk '{print $1}'")
		out, err := cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			ipStr := strings.TrimSpace(string(out))
			log.Printf("hostname -I ile bulunan IP: %s", ipStr)
			return ipStr
		}

		// 2. İkinci yöntem: ip addr (modern Linux sistemleri)
		cmd = exec.Command("sh", "-c", "ip addr show | grep 'inet ' | grep -v '127.0.0.1' | head -n1 | awk '{print $2}' | cut -d/ -f1")
		out, err = cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			ipStr := strings.TrimSpace(string(out))
			log.Printf("ip addr ile bulunan IP: %s", ipStr)
			return ipStr
		}

		// 3. Üçüncü yöntem: ifconfig (eski Unix sistemleri)
		cmd = exec.Command("sh", "-c", "ifconfig | grep 'inet ' | grep -v '127.0.0.1' | head -n1 | awk '{print $2}' | cut -d: -f2")
		out, err = cmd.Output()
		if err == nil && len(strings.TrimSpace(string(out))) > 0 {
			ipStr := strings.TrimSpace(string(out))
			log.Printf("ifconfig ile bulunan IP: %s", ipStr)
			return ipStr
		}
	}

	// Hiçbir yöntemle bulunamazsa
	log.Printf("IP adresi bulunamadı, bütün yöntemler başarısız oldu")
	return "Unknown"
}

// getTotalvCpu returns the total number of vCPUs
func (c *MSSQLCollector) getTotalvCpu() int32 {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command", "(Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors")
		out, err := cmd.Output()
		if err == nil {
			cpuCount, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
			if err == nil {
				return int32(cpuCount)
			}
		}
	}

	// For Unix-like systems
	cmd := exec.Command("sh", "-c", "nproc")
	out, err := cmd.Output()
	if err == nil {
		cpuCount, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
		if err == nil {
			return int32(cpuCount)
		}
	}

	// Fallback to runtime.NumCPU()
	return int32(runtime.NumCPU())
}

// getTotalMemory returns the total memory in bytes
func (c *MSSQLCollector) getTotalMemory() int64 {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("powershell", "-Command", "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory")
		out, err := cmd.Output()
		if err == nil {
			memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err == nil {
				return memTotal
			}
		}
	}

	// For Unix-like systems
	cmd := exec.Command("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'")
	out, err := cmd.Output()
	if err == nil {
		memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err == nil {
			return memTotal * 1024 // Convert KB to bytes
		}
	}

	return 0
}

// ToProto converts MSSQLInfo to protobuf message
func (m *MSSQLInfo) ToProto() *pb.MSSQLInfo {
	return &pb.MSSQLInfo{
		ClusterName: m.ClusterName,
		Ip:          m.IP,
		Hostname:    m.Hostname,
		NodeStatus:  m.NodeStatus,
		Version:     m.Version,
		Location:    m.Location,
		Status:      m.Status,
		Instance:    m.Instance,
		FreeDisk:    m.FreeDisk,
		FdPercent:   m.FdPercent,
		Port:        m.Port,
		TotalVcpu:   m.TotalvCpu,
		TotalMemory: m.TotalMemory,
		ConfigPath:  m.ConfigPath,
		Database:    m.Database,
		IsHaEnabled: m.IsHAEnabled,
		HaRole:      m.HARole,
		Edition:     m.Edition,
	}
}

// CheckSlowQueries monitors for slow queries in SQL Server
func (c *MSSQLCollector) CheckSlowQueries(thresholdMs int) ([]*pb.SlowQuery, error) {
	db, err := c.GetClient()
	if err != nil {
		return nil, fmt.Errorf("veritabanı bağlantısı kurulamadı: %v", err)
	}
	defer db.Close()

	// Query to find slow-running queries
	query := `
	SELECT 
		r.session_id,
		s.login_name,
		DB_NAME(r.database_id) AS database_name,
		r.start_time,
		r.status,
		r.command,
		CONVERT(NVARCHAR(MAX), qt.text) AS query_text,
		r.wait_type,
		r.wait_time,
		r.total_elapsed_time,
		r.cpu_time,
		r.reads,
		r.writes,
		r.logical_reads
	FROM sys.dm_exec_requests r
	CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) as qt
	LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
	WHERE r.session_id > 50  -- Exclude system sessions
	AND r.total_elapsed_time > @threshold
	AND qt.text IS NOT NULL
	ORDER BY r.total_elapsed_time DESC
	`

	// Execute the query
	rows, err := db.Query(query, sql.Named("threshold", thresholdMs))
	if err != nil {
		return nil, fmt.Errorf("yavaş sorgu analizi başarısız: %v", err)
	}
	defer rows.Close()

	var slowQueries []*pb.SlowQuery
	var totalSlowQueries int

	// Process results
	for rows.Next() {
		var (
			sessionID      int
			loginName      sql.NullString
			databaseName   sql.NullString
			startTime      time.Time
			status         string
			command        string
			queryText      string
			waitType       sql.NullString
			waitTime       sql.NullInt64
			totalElapsedMs int64
			cpuTime        int64
			reads          int64
			writes         int64
			logicalReads   int64
		)

		err := rows.Scan(
			&sessionID,
			&loginName,
			&databaseName,
			&startTime,
			&status,
			&command,
			&queryText,
			&waitType,
			&waitTime,
			&totalElapsedMs,
			&cpuTime,
			&reads,
			&writes,
			&logicalReads,
		)
		if err != nil {
			log.Printf("Sorgu tarama hatası: %v", err)
			continue
		}

		// Calculate duration in milliseconds
		durationMs := float64(totalElapsedMs)

		// Skip queries that don't meet threshold
		if durationMs < float64(thresholdMs) {
			continue
		}

		// Skip system databases if necessary
		dbName := "unknown"
		if databaseName.Valid {
			dbName = databaseName.String
			if dbName == "master" || dbName == "msdb" || dbName == "tempdb" || dbName == "model" {
				// Consider if you want to skip system database queries
				// log.Printf("Sistem veritabanı sorgusu atlanıyor: %s", dbName)
				// continue
			}
		}

		// Truncate query text if very long
		if len(queryText) > 2000 {
			queryText = queryText[:2000] + "..."
		}

		// Create slow query record
		slowQuery := &pb.SlowQuery{
			QueryText:    queryText,
			Database:     dbName,
			DurationMs:   durationMs,
			User:         loginName.String,
			Client:       fmt.Sprintf("Session: %d", sessionID),
			Timestamp:    startTime.Unix(),
			Status:       status,
			Command:      command,
			WaitType:     waitType.String,
			WaitTimeMs:   float64(waitTime.Int64),
			CpuTimeMs:    float64(cpuTime),
			Reads:        reads,
			Writes:       writes,
			LogicalReads: logicalReads,
		}

		slowQueries = append(slowQueries, slowQuery)
		totalSlowQueries++

		log.Printf("Yavaş sorgu tespit edildi! Süre: %.2f ms (Threshold: %d ms)", durationMs, thresholdMs)
	}

	if err = rows.Err(); err != nil {
		return slowQueries, fmt.Errorf("sorgu sonuçları okunurken hata: %v", err)
	}

	log.Printf("Toplam %d yavaş sorgu bulundu", totalSlowQueries)
	return slowQueries, nil
}

// ExplainMSSQLQuery returns the execution plan for a SQL Server query
func (c *MSSQLCollector) ExplainMSSQLQuery(database, queryStr string) (string, error) {
	// Connect to the database
	db, err := c.GetClient()
	if err != nil {
		log.Printf("MSSQL explain bağlantısı açılamadı: %v", err)
		return "", fmt.Errorf("MSSQL bağlantısı açılamadı: %v", err)
	}
	defer db.Close()

	log.Printf("ExplainMSSQLQuery başlatılıyor. Veritabanı: %s, Sorgu Boyutu: %d bytes",
		database, len(queryStr))

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to use the specified database
	if database != "" && database != "master" {
		_, err = db.ExecContext(ctx, "USE "+database)
		if err != nil {
			log.Printf("Veritabanı değiştirilemedi %s: %v", database, err)
			return "", fmt.Errorf("veritabanı değiştirilemedi %s: %v", database, err)
		}
	}

	// Get the execution plan
	var planXML string
	err = db.QueryRowContext(ctx, "SET SHOWPLAN_XML ON; "+queryStr+"; SET SHOWPLAN_XML OFF;").Scan(&planXML)

	// If query doesn't return a resultset, try alternative approach
	if err != nil {
		log.Printf("Execution plan için sorgu başarısız (direct): %v", err)
		log.Printf("Alternatif execution plan yaklaşımı deneniyor...")

		// Reset connection
		db.Close()
		db, err = c.GetClient()
		if err != nil {
			return "", fmt.Errorf("bağlantı açılamadı: %v", err)
		}
		defer db.Close()

		// Use database
		if database != "" && database != "master" {
			_, err = db.ExecContext(ctx, "USE "+database)
			if err != nil {
				return "", fmt.Errorf("veritabanı değiştirilemedi: %v", err)
			}
		}

		// Method 2: Temporary stored procedure to capture the execution plan
		_, err = db.ExecContext(ctx, "IF OBJECT_ID('tempdb..#get_plan') IS NOT NULL DROP PROCEDURE #get_plan")
		if err != nil {
			log.Printf("Temporary procedure silme hatası: %v", err)
		}

		createProcQuery := `
		CREATE PROCEDURE #get_plan AS 
		BEGIN
			SET SHOWPLAN_XML ON;
			` + queryStr + `
			SET SHOWPLAN_XML OFF;
		END
		`

		_, err = db.ExecContext(ctx, createProcQuery)
		if err != nil {
			log.Printf("Temporary procedure oluşturma hatası: %v", err)

			// Method 3: Try SHOWPLAN_XML query directly
			planQuery := `
			SELECT query_plan
			FROM sys.dm_exec_query_plan(
				(SELECT TOP 1 plan_handle 
				 FROM sys.dm_exec_query_stats 
				 CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS st
				 WHERE st.text LIKE @query
				 ORDER BY last_execution_time DESC)
			)
			`

			err = db.QueryRowContext(ctx, planQuery, sql.Named("query", "%"+queryStr+"%")).Scan(&planXML)
			if err != nil {
				return "", fmt.Errorf("execution plan alınamadı: %v", err)
			}
		} else {
			// Execute the procedure to get the plan
			err = db.QueryRowContext(ctx, "EXEC sp_executesql N'EXEC #get_plan'; SELECT CAST(query_plan AS NVARCHAR(MAX)) FROM sys.dm_exec_query_plan(MOST_RECENT plan_handle);").Scan(&planXML)
			if err != nil {
				return "", fmt.Errorf("execution plan alınamadı: %v", err)
			}
		}
	}

	// Format the execution plan for easier reading
	prettyPlan := "## SQL Server Execution Plan\n```xml\n" + planXML + "\n```"

	return prettyPlan, nil
}

// FindMSSQLLogFiles finds SQL Server log files
func (c *MSSQLCollector) FindMSSQLLogFiles() ([]*pb.MSSQLLogFile, error) {
	// First try to get log directory from SQL Server
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı: %v", err)
		return nil, err
	}
	defer db.Close()

	var logDir string
	err = db.QueryRow(`SELECT LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(MAX)), 
		LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(MAX))) - 
		CHARINDEX('\', REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(MAX)))) + 1)`).Scan(&logDir)

	if err != nil || logDir == "" {
		// Try alternative approach
		if runtime.GOOS == "windows" {
			// Get SQL Server installation directory
			instance := c.cfg.MSSQL.Instance
			regPath := "MSSQL"
			if instance != "" {
				regPath = "MSSQL$" + instance
			}

			cmd := exec.Command("powershell", "-Command",
				fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name SQLPath | Select-Object -ExpandProperty SQLPath", regPath))
			out, err := cmd.Output()
			if err == nil {
				// Typical log directory based on SQL Server installation
				logDir = filepath.Join(strings.TrimSpace(string(out)), "Log")
			}
		}
	}

	if logDir == "" {
		return nil, fmt.Errorf("SQL Server log directory not found")
	}

	var logFiles []*pb.MSSQLLogFile

	// List files in the log directory
	files, err := os.ReadDir(logDir)
	if err != nil {
		return nil, fmt.Errorf("log directory could not be read: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Check if file is a SQL Server log file
		if strings.HasPrefix(strings.ToLower(file.Name()), "errorlog") ||
			strings.HasSuffix(strings.ToLower(file.Name()), ".trc") ||
			strings.Contains(strings.ToLower(file.Name()), "sqlserver") {

			fileInfo, err := file.Info()
			if err != nil {
				log.Printf("File info error: %v", err)
				continue
			}

			logFile := &pb.MSSQLLogFile{
				Name:         file.Name(),
				Path:         filepath.Join(logDir, file.Name()),
				Size:         fileInfo.Size(),
				LastModified: fileInfo.ModTime().Unix(),
			}

			logFiles = append(logFiles, logFile)
		}
	}

	return logFiles, nil
}

// GetBestPracticesCollector bir BestPracticesCollector oluşturur ve döndürür
func (c *MSSQLCollector) GetBestPracticesCollector() *BestPracticesCollector {
	return NewBestPracticesCollector(c)
}

// RunBestPracticesAnalysis SQL Server best practices analizini başlatır ve sonuçları döndürür
func (c *MSSQLCollector) RunBestPracticesAnalysis() (map[string]interface{}, error) {
	// BestPracticesCollector oluştur
	bpc := c.GetBestPracticesCollector()

	// Analizi çalıştır
	results := bpc.GetBestPracticesAnalysis()

	// Sonuç başarılı
	return results, nil
}
