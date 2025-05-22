package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb" // MSSQL driver
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
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

	// Get cache file path based on OS
	cacheFile := getCacheFilePath()

	// Log attempted cache read
	log.Printf("MSSQL HA durum önbelleği okunuyor: %s", cacheFile)

	// Check if file exists
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		log.Printf("MSSQL HA durum önbelleği bulunamadı, ilk çalıştırma olabilir")
		return emptyState, false
	}

	// Read cache file
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		log.Printf("MSSQL HA durum önbelleği okunamadı: %v", err)
		return emptyState, false
	}

	// Parse cached data from JSON
	var cachedData struct {
		Role        string    `json:"role"`
		Group       string    `json:"group"`
		LastUpdated time.Time `json:"last_updated"`
	}

	if err := json.Unmarshal(data, &cachedData); err != nil {
		log.Printf("MSSQL HA durum önbelleği ayrıştırılamadı (JSON hatası): %v", err)

		// Try legacy format for backward compatibility
		parts := strings.Split(string(data), "|")
		if len(parts) >= 2 {
			log.Printf("Eski format önbellekte HA bilgisi bulundu: %s, %s", parts[0], parts[1])
			return struct{ role, group string }{parts[0], parts[1]}, true
		}

		return emptyState, false
	}

	// Check if cache is stale (7 days max)
	if time.Since(cachedData.LastUpdated) > 7*24*time.Hour {
		log.Printf("MSSQL HA durum önbelleği çok eski (7 günden fazla): %s, yeni veri gerekli",
			cachedData.LastUpdated.Format("2006-01-02 15:04:05"))
		return emptyState, false
	}

	log.Printf("MSSQL HA durum önbelleğinden bilgi alındı: Role=%s, Group=%s, LastUpdated=%s",
		cachedData.Role, cachedData.Group, cachedData.LastUpdated.Format("2006-01-02 15:04:05"))

	return struct{ role, group string }{cachedData.Role, cachedData.Group}, true
}

// getCacheFilePath returns the appropriate path for the cache file based on OS
func getCacheFilePath() string {
	var basePath string

	if runtime.GOOS == "windows" {
		// Windows specific path
		basePath = filepath.Join("C:\\Program Files\\ClusterEyeAgent", "cache")
	} else {
		// Linux/Unix path
		basePath = "/var/lib/clustereye-agent/cache"

		// Try user's home if agent might be running as non-root
		if homeDir, err := os.UserHomeDir(); err == nil {
			basePath = filepath.Join(homeDir, ".clustereye", "cache")
		}
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		log.Printf("Cache dizini oluşturulamadı: %v, geçici dizin kullanılacak", err)
		// Fallback to temp directory if we can't create our preferred location
		basePath = os.TempDir()
	}

	return filepath.Join(basePath, "mssql_ha_state.json")
}

// saveHAStateToCache saves HA information to a cache file
func (c *MSSQLCollector) saveHAStateToCache(role, group string) {
	// Prepare cache data with timestamp
	cacheData := struct {
		Role        string    `json:"role"`
		Group       string    `json:"group"`
		LastUpdated time.Time `json:"last_updated"`
	}{
		Role:        role,
		Group:       group,
		LastUpdated: time.Now(),
	}

	// Convert to JSON
	jsonData, err := json.Marshal(cacheData)
	if err != nil {
		log.Printf("HA durumu JSON formatına dönüştürülemedi: %v", err)
		return
	}

	// Get cache file path
	cacheFile := getCacheFilePath()

	// Ensure directory exists
	cacheDir := filepath.Dir(cacheFile)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("Cache dizini oluşturulamadı: %v", err)
		return
	}

	// Write cache file
	err = os.WriteFile(cacheFile, jsonData, 0644)
	if err != nil {
		log.Printf("HA durumu önbelleğe kaydedilemedi: %v", err)
	} else {
		log.Printf("HA durumu başarıyla önbelleğe kaydedildi: Role=%s, Group=%s, Path=%s",
			role, group, cacheFile)
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
			log.Printf("SQL Server servis kapalı, önbellekteki cluster adı kullanılıyor: %s", haState.group)
			return haState.group
		}

		log.Printf("SQL Server kapalı ve önbellekte HA bilgisi bulunamadı")
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
				// Önbellekte bilgi varsa, veritabanından bilgi alınamasa bile önbellekteki bilgiyi geri döndür
				if hasHAState && haState.group != "" {
					log.Printf("Veritabanından bilgi alınamadı, önbellekteki grup bilgisini kullanılıyor: %s", haState.group)
					return haState.group
				}
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

// GetConfigPath returns the SQL Server configuration file path
func (c *MSSQLCollector) GetConfigPath() string {
	// Check cache first
	if cachedPath := c.getCachedConfigPath(); cachedPath != "" {
		log.Printf("Using cached SQL Server config path: %s", cachedPath)
		return cachedPath
	}

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

	if err == nil && configPath != "" {
		configDir := filepath.Dir(configPath)
		c.cacheConfigPath(configDir)
		return configDir
	}

	log.Printf("Error log path could not be determined: %v", err)

	// Try to find the SQL Server registry location
	if runtime.GOOS == "windows" {
		instance := c.cfg.MSSQL.Instance
		regPath := "MSSQL"
		if instance != "" {
			regPath = "MSSQL$" + instance
		}

		// Single optimized command instead of nested pipe operations
		// Use -NoProfile for faster startup
		cmd := exec.Command("powershell", "-NoProfile", "-Command",
			fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name SQLPath | Select-Object -First 1 -ExpandProperty SQLPath", regPath))
		out, err := cmd.Output()
		if err == nil {
			path := strings.TrimSpace(string(out))
			if path != "" {
				configDir := filepath.Join(path, "MSSQL")
				c.cacheConfigPath(configDir)
				return configDir
			}
		}
	}

	return ""
}

// Helper function to cache SQL Server config path
func (c *MSSQLCollector) cacheConfigPath(path string) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("Config path önbellek dizini oluşturulamadı: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_config_path.txt")
	if err := os.WriteFile(cacheFile, []byte(path), 0644); err != nil {
		log.Printf("Config path önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached SQL Server config path
func (c *MSSQLCollector) getCachedConfigPath() string {
	cacheFile := filepath.Join(getCacheDir(), "mssql_config_path.txt")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return ""
	}

	// Config path rarely changes, but we'll cache it for 7 days
	fileInfo, err := os.Stat(cacheFile)
	if err != nil {
		return ""
	}

	if time.Since(fileInfo.ModTime()) > 7*24*time.Hour {
		// Cache too old
		return ""
	}

	return strings.TrimSpace(string(data))
}

// GetDiskUsage returns disk usage information for the SQL Server data directory
func (c *MSSQLCollector) GetDiskUsage() (string, int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in GetDiskUsage: %v", r)
		}
	}()

	// Check if we have recent cached disk usage info (max 5 minutes old)
	if cachedInfo := c.getCachedDiskUsage(); cachedInfo != nil {
		var freeDisk string = "N/A"
		var usagePercent int = 0

		// Safely extract disk info with proper type checking
		if fd, ok := cachedInfo["free_disk"]; ok {
			if fdStr, ok := fd.(string); ok {
				freeDisk = fdStr
			}
		}
		if up, ok := cachedInfo["usage_percent"]; ok {
			switch v := up.(type) {
			case int:
				usagePercent = v
			case float64:
				usagePercent = int(v)
			}
		}
		log.Printf("Using cached disk usage: %s free, %d%% used", freeDisk, usagePercent)
		return freeDisk, usagePercent
	}

	var dataPath string

	// Try to get database path
	db, err := c.GetClient()
	if err != nil {
		log.Printf("Veritabanı bağlantısı kurulamadı, varsayılan disk kontrol ediliyor: %v", err)

		// If we can't connect to DB, try using C: drive directly
		if runtime.GOOS == "windows" {
			return c.getSafeDiskUsage("C")
		}
		return "N/A", 0
	}
	defer db.Close()

	// First get data directory using a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.QueryRowContext(ctx, `
		SELECT SERVERPROPERTY('InstanceDefaultDataPath')
	`).Scan(&dataPath)

	if err != nil || dataPath == "" {
		log.Printf("Veri dizini bilgisi alınamadı: %v", err)

		// Fallback to master database path
		err = db.QueryRowContext(ctx, `
			SELECT physical_name 
			FROM sys.master_files 
			WHERE database_id = 1 AND file_id = 1
		`).Scan(&dataPath)

		if err != nil || dataPath == "" {
			log.Printf("Master database yolu alınamadı: %v, varsayılan disk kontrol ediliyor", err)
			// Fallback to C: drive
			if runtime.GOOS == "windows" {
				return c.getSafeDiskUsage("C")
			}
			return "N/A", 0
		}

		// Get directory from file path
		dataPath = filepath.Dir(dataPath)
	}

	// On Windows, use WMI query
	if runtime.GOOS == "windows" {
		// Extract drive letter
		if len(dataPath) < 2 || !strings.Contains(dataPath, ":") {
			log.Printf("Geçersiz veri yolu: %s, varsayılan C: sürücüsü kontrol ediliyor", dataPath)
			return c.getSafeDiskUsage("C")
		}

		driveLetter := strings.ToUpper(dataPath[0:1]) // Just take the first character and make it uppercase
		if driveLetter == "" {
			log.Printf("Disk harfi çıkarılamadı: %s, varsayılan C: sürücüsü kontrol ediliyor", dataPath)
			return c.getSafeDiskUsage("C")
		}

		return c.getSafeDiskUsage(driveLetter)
	}

	// For Linux systems, use df command
	cmd := exec.Command("df", "-h", dataPath)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()

	if err != nil || ctx.Err() == context.DeadlineExceeded {
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

	// Cache the results
	c.cacheDiskUsage(freeDisk, usagePercent)

	return freeDisk, usagePercent
}

// This function has been replaced with an improved version at the bottom of the file

// isClusterVirtualIP checks if the given IP is a cluster virtual IP
func (c *MSSQLCollector) isClusterVirtualIP(ipAddress string) (bool, error) {
	// Try to query the cluster resources to see if this IP is a virtual IP
	db, err := c.GetClient()
	if err != nil {
		return false, err
	}
	defer db.Close()

	// Try to check if the IP is used for availability groups
	var agIPCount int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM sys.availability_group_listener_ip_addresses
		WHERE ip_address = @ip
	`, sql.Named("ip", ipAddress)).Scan(&agIPCount)

	if err == nil && agIPCount > 0 {
		return true, nil // This is an AG listener IP
	}

	// Try to check if IP belongs to a Windows Failover Cluster resource
	if c.canExecuteXPCmdShell() {
		var output sql.NullString
		err = db.QueryRow(`
			DECLARE @output TABLE (line nvarchar(512))
			INSERT INTO @output
			EXEC xp_cmdshell 'powershell -Command "Get-ClusterResource | Where-Object { $_.ResourceType -eq \"IP Address\" } | Format-List"'
			
			SELECT COUNT(*)
			FROM @output
			WHERE line LIKE '%' + @ip + '%'
		`, sql.Named("ip", ipAddress)).Scan(&output)

		if err == nil && output.Valid {
			ipCount, err := strconv.Atoi(strings.TrimSpace(output.String))
			if err == nil && ipCount > 0 {
				return true, nil // This is a cluster resource IP
			}
		}
	}

	return false, nil
}

// canExecuteXPCmdShell checks if the current connection can execute xp_cmdshell
func (c *MSSQLCollector) canExecuteXPCmdShell() bool {
	db, err := c.GetClient()
	if err != nil {
		return false
	}
	defer db.Close()

	var canExecute int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM sys.configurations 
		WHERE name = 'xp_cmdshell' AND value_in_use = 1
	`).Scan(&canExecute)

	return err == nil && canExecute > 0
}

// Helper function to cache disk usage information
func (c *MSSQLCollector) cacheDiskUsage(freeDisk string, usagePercent int) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("Disk usage önbellek dizini oluşturulamadı: %v", err)
		return
	}

	// Create a map and marshal to JSON
	diskInfo := map[string]interface{}{
		"free_disk":     freeDisk,
		"usage_percent": usagePercent,
		"timestamp":     time.Now().Unix(),
	}

	jsonData, err := json.Marshal(diskInfo)
	if err != nil {
		log.Printf("Disk usage JSON encode hatası: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_disk_usage.json")
	if err := os.WriteFile(cacheFile, jsonData, 0644); err != nil {
		log.Printf("Disk usage önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached disk usage information
func (c *MSSQLCollector) getCachedDiskUsage() map[string]interface{} {
	cacheFile := filepath.Join(getCacheDir(), "mssql_disk_usage.json")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return nil
	}

	var diskInfo map[string]interface{}
	if err := json.Unmarshal(data, &diskInfo); err != nil {
		return nil
	}

	// Check if cache is recent (within last 5 minutes)
	timestamp, ok := diskInfo["timestamp"].(float64)
	if !ok {
		return nil
	}

	if time.Now().Unix()-int64(timestamp) > 300 { // 5 minutes
		return nil
	}

	return diskInfo
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

// runPowerShellWithTimeout executes a PowerShell command with a timeout to prevent hanging
func (c *MSSQLCollector) runPowerShellWithTimeout(command string, timeoutSeconds int) ([]byte, error) {
	// Add performance optimization flags to PowerShell
	// -NoProfile: Don't load the PowerShell profile (faster startup)
	// -NonInteractive: Don't present an interactive prompt to the user
	// -NoLogo: Don't show the logo
	// -ExecutionPolicy Bypass: Don't check script execution policy (faster)
	// -WindowStyle Hidden: Don't show a window
	// -Command: The command to execute
	cmd := exec.Command(
		"powershell",
		"-NoProfile",
		"-NonInteractive",
		"-NoLogo",
		"-ExecutionPolicy", "Bypass",
		"-WindowStyle", "Hidden",
		"-Command", command)

	// Set timeout context
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	// Lower process priority to reduce CPU impact - Windows only feature
	// This is handled differently across operating systems
	// Only attempt on Windows systems

	// Set the command to run with the timeout context
	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)

	// Execute the command
	out, err := cmdWithTimeout.Output()
	if ctx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("command timed out after %d seconds", timeoutSeconds)
	}

	return out, err
}

// BatchCollectSystemMetrics collects all system metrics with a single PowerShell call
func (c *MSSQLCollector) BatchCollectSystemMetrics() map[string]interface{} {
	// Check if we have cached metrics - increase cache duration from 1 hour to 6 hours
	if cachedMetrics := c.getCachedSystemMetrics(); cachedMetrics != nil {
		log.Printf("Using cached system metrics. Keys: %v", getMapKeys(cachedMetrics))
		// Ensure cached metrics have consistent types
		standardizedMetrics := standardizeMetricTypes(cachedMetrics)
		return standardizedMetrics
	}

	// For non-Windows systems, fallback to individual calls
	if runtime.GOOS != "windows" {
		log.Printf("Non-Windows system detected, collecting metrics individually")
		metrics := map[string]interface{}{
			"cpu_count":    c.getTotalvCpu(),
			"total_memory": c.getTotalMemory(),
			"ip_address":   c.getLocalIP(),
		}

		// Cache the results
		c.cacheSystemMetrics(metrics)
		return metrics
	}

	log.Printf("Collecting system metrics with lighter weight methods")

	// Try to use WMI commands directly instead of PowerShell where possible
	metrics := make(map[string]interface{})

	// 1. Try getting CPU count using direct command
	cmd := exec.Command("wmic", "cpu", "get", "NumberOfLogicalProcessors", "/value")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil {
		// Parse the output
		cpuStr := strings.TrimSpace(string(out))
		parts := strings.Split(cpuStr, "=")
		if len(parts) == 2 {
			cpuCount, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err == nil && cpuCount > 0 {
				metrics["cpu_count"] = int32(cpuCount)
			}
		}
	}

	// 2. Try getting memory using direct wmic command
	cmd = exec.Command("wmic", "computersystem", "get", "TotalPhysicalMemory", "/value")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err = cmdWithTimeout.Output()
	if err == nil {
		// Parse the output
		memStr := strings.TrimSpace(string(out))
		parts := strings.Split(memStr, "=")
		if len(parts) == 2 {
			totalMem, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err == nil && totalMem > 0 {
				metrics["total_memory"] = totalMem
			}
		}
	}

	// If we still need to get some metrics, use a simplified PowerShell script
	// as a fallback, but make it as light as possible
	if metrics["cpu_count"] == nil || metrics["total_memory"] == nil || metrics["ip_address"] == nil {
		// Use a simpler, more focused PowerShell script
		psScript := `
		$result = @{
			CpuCount = 0
			TotalMemory = 0
			IpAddress = ""
		}

		# Only calculate what we're missing
		`

		if metrics["cpu_count"] == nil {
			psScript += `
			try { 
				$result.CpuCount = (Get-WmiObject -Class Win32_Processor -ErrorAction Stop | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum 
			} catch { 
				$result.CpuCount = 0 
			}
			`
		}

		if metrics["total_memory"] == nil {
			psScript += `
			try {
				$result.TotalMemory = (Get-WmiObject -Class Win32_ComputerSystem -ErrorAction Stop).TotalPhysicalMemory
			} catch {
				$result.TotalMemory = 0
			}
			`
		}

		if metrics["ip_address"] == nil {
			// Use the DNS method that proved to be the most reliable
			psScript += `
			try {
				$result.IpAddress = ([System.Net.Dns]::GetHostAddresses($env:COMPUTERNAME) | 
				Where-Object { $_.AddressFamily -eq 'InterNetwork' -and 
							 $_.IPAddressToString -notlike '169.254.*' -and 
							 $_.IPAddressToString -ne '127.0.0.1' } | 
				Select-Object -First 1).IPAddressToString
			} catch {
				$result.IpAddress = ""
			}
			`
		}

		psScript += `ConvertTo-Json -InputObject $result -Compress`

		// Run the minimum required PowerShell commands with reduced timeout
		out, err := c.runPowerShellWithTimeout(psScript, 5) // 5 seconds timeout instead of 15
		if err != nil {
			log.Printf("Lightweight PowerShell metrics collection failed: %v", err)
		} else {
			var metricsResult struct {
				CpuCount    int32  `json:"CpuCount"`
				TotalMemory int64  `json:"TotalMemory"`
				IpAddress   string `json:"IpAddress"`
			}

			if err := json.Unmarshal(out, &metricsResult); err == nil {
				// Only update metrics that are missing
				if metrics["cpu_count"] == nil && metricsResult.CpuCount > 0 {
					metrics["cpu_count"] = metricsResult.CpuCount
				}

				if metrics["total_memory"] == nil && metricsResult.TotalMemory > 0 {
					metrics["total_memory"] = metricsResult.TotalMemory
				}

				if metrics["ip_address"] == nil && metricsResult.IpAddress != "" {
					metrics["ip_address"] = metricsResult.IpAddress
				}
			}
		}
	}

	// If still missing any values, use existing fallback methods
	if metrics["cpu_count"] == nil {
		log.Printf("Using fallback for CPU count")
		metrics["cpu_count"] = c.getTotalvCpu()
	}

	if metrics["total_memory"] == nil {
		log.Printf("Using fallback for memory")
		metrics["total_memory"] = c.getTotalMemory()
	}

	if metrics["ip_address"] == nil {
		log.Printf("Using fallback for IP address")
		metrics["ip_address"] = c.getLocalIP()
	}

	// Cache the results for a longer period (6 hours)
	log.Printf("Collected metrics - CPU: %v, Memory: %v, IP: %s",
		metrics["cpu_count"], metrics["total_memory"], metrics["ip_address"])
	c.cacheSystemMetrics(metrics)
	return metrics
}

// standardizeMetricTypes ensures consistent types in metrics map
func standardizeMetricTypes(metrics map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy all metrics
	for k, v := range metrics {
		result[k] = v
	}

	// Standardize CPU count
	if cpuVal, ok := metrics["cpu_count"]; ok {
		switch v := cpuVal.(type) {
		case int32:
			result["cpu_count"] = v
		case int:
			result["cpu_count"] = int32(v)
		case int64:
			result["cpu_count"] = int32(v)
		case float64:
			result["cpu_count"] = int32(v)
		default:
			// Remove invalid type
			log.Printf("Removing invalid CPU count type: %T", cpuVal)
			delete(result, "cpu_count")
		}
	}

	// Standardize memory
	if memVal, ok := metrics["total_memory"]; ok {
		switch v := memVal.(type) {
		case int64:
			result["total_memory"] = v
		case int:
			result["total_memory"] = int64(v)
		case int32:
			result["total_memory"] = int64(v)
		case float64:
			result["total_memory"] = int64(v)
		default:
			// Remove invalid type
			log.Printf("Removing invalid memory type: %T", memVal)
			delete(result, "total_memory")
		}
	}

	// Standardize IP address
	if ipVal, ok := metrics["ip_address"]; ok {
		if ipStr, ok := ipVal.(string); ok {
			result["ip_address"] = ipStr
		} else {
			// Remove invalid type
			log.Printf("Removing invalid IP address type: %T", ipVal)
			delete(result, "ip_address")
		}
	}

	return result
}

// Helper function to cache system metrics
func (c *MSSQLCollector) cacheSystemMetrics(metrics map[string]interface{}) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("System metrics önbellek dizini oluşturulamadı: %v", err)
		return
	}

	// Add timestamp
	metricsCopy := make(map[string]interface{})
	for k, v := range metrics {
		metricsCopy[k] = v
	}
	metricsCopy["timestamp"] = time.Now().Unix()

	jsonData, err := json.Marshal(metricsCopy)
	if err != nil {
		log.Printf("System metrics JSON encode hatası: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_system_metrics.json")
	if err := os.WriteFile(cacheFile, jsonData, 0644); err != nil {
		log.Printf("System metrics önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached system metrics
func (c *MSSQLCollector) getCachedSystemMetrics() map[string]interface{} {
	cacheFile := filepath.Join(getCacheDir(), "mssql_system_metrics.json")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return nil
	}

	var metrics map[string]interface{}
	if err := json.Unmarshal(data, &metrics); err != nil {
		return nil
	}

	// Check if cache is recent (within last 6 hours instead of 1 hour)
	timestamp, ok := metrics["timestamp"].(float64)
	if !ok {
		return nil
	}

	if time.Now().Unix()-int64(timestamp) > 21600 { // 6 hours (6 * 60 * 60 = 21600 seconds)
		return nil
	}

	// Remove timestamp
	delete(metrics, "timestamp")
	return metrics
}

// GetMSSQLInfo collects SQL Server information
func (c *MSSQLCollector) GetMSSQLInfo() *MSSQLInfo {
	// Use internal logger and set level to INFO just for this function
	logger.Info("MSSQL bilgileri toplanmaya başlanıyor...")

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetMSSQLInfo: %v", r)
			// Log call stack for better debugging
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger.Error("STACK: %s", string(buf[:n]))
		}
	}()

	// Get hostname once
	hostname, _ := os.Hostname()
	logger.Info("Hostname: %s", hostname)

	// Use batch system metrics collection - optimized for lower CPU usage
	systemMetrics := c.BatchCollectSystemMetrics()

	// Safely extract metrics with proper type checking
	var ip string
	var totalvCpu int32
	var totalMemory int64

	// Debug log for systemMetrics
	if systemMetrics != nil {
		logger.Debug("Cached metrics found. Keys: %v", getMapKeys(systemMetrics))
	} else {
		logger.Debug("No cached metrics available")
	}

	// Safe extraction of metrics with error handling
	if systemMetrics != nil {
		// IP Address
		if ipVal, ok := systemMetrics["ip_address"]; ok {
			if ipStr, ok := ipVal.(string); ok {
				ip = ipStr
				logger.Debug("Using cached IP: %s", ip)
			} else {
				logger.Debug("Cached IP has wrong type: %T", ipVal)
				ip = c.getLocalIP() // Fallback
			}
		} else {
			logger.Debug("No cached IP found")
			ip = c.getLocalIP() // Fallback
		}

		// CPU Count
		if cpuVal, ok := systemMetrics["cpu_count"]; ok {
			switch v := cpuVal.(type) {
			case int32:
				totalvCpu = v
			case int:
				totalvCpu = int32(v)
			case int64:
				totalvCpu = int32(v)
			case float64:
				totalvCpu = int32(v)
			default:
				logger.Debug("Cached CPU count has wrong type: %T", cpuVal)
				totalvCpu = c.getTotalvCpu() // Fallback
			}
			logger.Debug("Using cached CPU count: %d", totalvCpu)
		} else {
			logger.Debug("No cached CPU count found")
			totalvCpu = c.getTotalvCpu() // Fallback
		}

		// Memory
		if memVal, ok := systemMetrics["total_memory"]; ok {
			switch v := memVal.(type) {
			case int64:
				totalMemory = v
			case int:
				totalMemory = int64(v)
			case float64:
				totalMemory = int64(v)
			default:
				logger.Debug("Cached memory has wrong type: %T", memVal)
				totalMemory = c.getTotalMemory() // Fallback
			}
			logger.Debug("Using cached memory: %d bytes", totalMemory)
		} else {
			logger.Debug("No cached memory found")
			totalMemory = c.getTotalMemory() // Fallback
		}
	} else {
		// Fallback to individual calls
		logger.Debug("Sistem metrikleri önbellekte bulunamadı, doğrudan alınıyor")
		ip = c.getLocalIP()
		totalvCpu = c.getTotalvCpu()
		totalMemory = c.getTotalMemory()
	}

	// Check if we can connect to the database - don't attempt multiple connections
	// if first connection fails
	var version, edition, serviceStatus, configPath, freeDisk, nodeStatus string
	var usagePercent int
	var clusterName string
	var isHAEnabled bool

	testDb, err := c.GetClient()
	if err == nil {
		// Keep the connection for use in multiple operations

		// 1. Get version and edition in a single query
		err = testDb.QueryRow("SELECT @@VERSION, SERVERPROPERTY('Edition')").Scan(&version, &edition)
		if err != nil {
			logger.Debug("Version bilgisi alınamadı: %v", err)
			version = "Unknown"
			edition = "Unknown"
		} else {
			logger.Info("SQL Server versiyon: %s, sürüm: %s", version, edition)
		}

		// 2. Get service status - we already know it's running if we got here
		serviceStatus = "RUNNING"

		// 3. Get HA status in a single efficient query
		err = testDb.QueryRow(`
			SELECT 
				CASE 
					WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 
						(SELECT TOP 1 ag.name 
						 FROM sys.availability_groups ag 
						 JOIN sys.dm_hadr_availability_replica_states ars ON ag.group_id = ars.group_id 
						 WHERE ars.role_desc IS NOT NULL)
					ELSE ''
				END AS cluster_name,
				CASE 
					WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 
						(SELECT TOP 1 role_desc 
						 FROM sys.dm_hadr_availability_replica_states ars 
						 JOIN sys.availability_replicas ar ON ars.replica_id = ar.replica_id 
						 WHERE ar.replica_server_name = @@SERVERNAME)
					ELSE 'STANDALONE' 
				END AS node_status,
				ISNULL(SERVERPROPERTY('IsHadrEnabled'), 0) AS is_ha_enabled
		`).Scan(&clusterName, &nodeStatus, &isHAEnabled)

		if err != nil {
			logger.Debug("HA status sorgusu başarısız: %v", err)
			nodeStatus = "STANDALONE"
			isHAEnabled = false
		} else {
			if isHAEnabled {
				logger.Info("Cluster adı: %s, Node rolü: %s", clusterName, nodeStatus)
				// Cache HA information for when service is down
				c.saveHAStateToCache(nodeStatus, clusterName)
			}
		}

		// 4. Get config path and disk usage in a single batch
		err = testDb.QueryRow(`
			SELECT LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(MAX)), 
				LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(MAX))) - 
				CHARINDEX('\', REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(MAX)))) + 1)
		`).Scan(&configPath)

		if err == nil && configPath != "" {
			// Cache config path
			c.cacheConfigPath(configPath)

			// Get disk usage for drive where SQL Server is installed
			if len(configPath) >= 1 {
				driveLetter := string(configPath[0])
				freeDisk, usagePercent = c.getSafeDiskUsage(driveLetter)
			} else {
				// Fallback to C drive
				freeDisk, usagePercent = c.getSafeDiskUsage("C")
			}
		} else {
			// Fallbacks
			configPath = c.getCachedConfigPath()
			freeDisk, usagePercent = c.getSafeDiskUsage("C")
		}

		// Clean up
		testDb.Close()
	} else {
		logger.Debug("Veritabanı bağlantısı kurulamadı: %v", err)

		// Set defaults for disconnected mode
		version = "Unknown"
		edition = "Unknown"
		serviceStatus = c.GetMSSQLStatus()

		// Try to get cached values
		configPath = c.getCachedConfigPath()
		freeDisk, usagePercent = c.getSafeDiskUsage("C")

		// Get cached HA state if available
		haState, hasHAState := c.getHAStateFromCache("mssql_ha_state")
		if hasHAState {
			nodeStatus = haState.role
			clusterName = haState.group
			isHAEnabled = (nodeStatus != "STANDALONE" && nodeStatus != "")
		} else {
			nodeStatus = "STANDALONE"
			isHAEnabled = false
		}
	}

	// Create info object with safely extracted values
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

	logger.Info("MSSQL bilgileri başarıyla toplandı. IP=%s, Status=%s, HAEnabled=%v",
		info.IP, info.Status, info.IsHAEnabled)

	return info
}

// Helper function to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getSafeDiskUsage gets disk usage information without requiring a database connection
func (c *MSSQLCollector) getSafeDiskUsage(driveLetter string) (string, int) {
	// Add panic recovery to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in getSafeDiskUsage: %v", r)
		}
	}()

	if runtime.GOOS != "windows" {
		return "N/A", 0
	}

	// Ensure we have a valid drive letter
	if driveLetter == "" {
		log.Printf("Empty drive letter provided, using C: instead")
		driveLetter = "C"
	}

	// Sanitize drive letter - just take the first character
	driveLetter = strings.ToUpper(string(driveLetter[0]))
	log.Printf("Getting disk usage for drive %s:", driveLetter)

	// Try to get disk usage from PowerShell safely
	// Use a more robust PowerShell script with error handling
	psScript := fmt.Sprintf(`
	try {
		$vol = Get-Volume -DriveLetter %s -ErrorAction Stop
		if ($vol) {
			$result = @{
				Free = $vol.SizeRemaining
				Total = $vol.Size
				Success = $true
			}
		} else {
			$result = @{
				Free = 0
				Total = 0
				Success = $false
				Error = "Volume not found"
			}
		}
	} catch {
		$result = @{
			Free = 0
			Total = 0
			Success = $false
			Error = $_.Exception.Message
		}
	}
	ConvertTo-Json -InputObject $result -Compress
	`, driveLetter)

	// Set timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Execute with timeout
	out, err := c.runPowerShellWithTimeout(psScript, 10)

	if err != nil || ctx.Err() == context.DeadlineExceeded {
		log.Printf("Disk kullanımı güvenli sorgusu başarısız: %v", err)
		return "N/A", 0
	}

	// Parse JSON output - with extended error handling
	type DiskResult struct {
		Free    uint64 `json:"Free"`
		Total   uint64 `json:"Total"`
		Success bool   `json:"Success"`
		Error   string `json:"Error,omitempty"`
	}

	var diskInfo DiskResult
	outStr := string(out)
	log.Printf("Raw disk info output: %s", strings.TrimSpace(outStr))

	if err := json.Unmarshal(out, &diskInfo); err != nil {
		log.Printf("Disk kullanımı JSON ayrıştırma hatası: %v. Raw output: %s", err, outStr)
		return "N/A", 0
	}

	// Check if the operation was successful
	if !diskInfo.Success {
		log.Printf("Disk bilgisi alınamadı: %s", diskInfo.Error)
		return "N/A", 0
	}

	// Calculate usage percentage
	usagePercent := 0
	if diskInfo.Total > 0 {
		usedBytes := diskInfo.Total - diskInfo.Free
		usagePercent = int((float64(usedBytes) / float64(diskInfo.Total)) * 100)
	}

	// Format free space
	freeDisk := c.formatBytes(diskInfo.Free)

	// Cache the results
	c.cacheDiskUsage(freeDisk, usagePercent)

	log.Printf("Disk kullanımı: %s boş, %d%% kullanımda", freeDisk, usagePercent)
	return freeDisk, usagePercent
}

// getLocalIP returns the local IP address
func (c *MSSQLCollector) getLocalIP() string {
	// Check if we have cached IP address
	if cachedIP := c.getCachedIP(); cachedIP != "" {
		log.Printf("Using cached IP address: %s", cachedIP)
		return cachedIP
	}

	// First, force using direct OS commands for physical IP detection
	// This is the most reliable method for getting the actual physical node IP
	physicalIP := c.getPhysicalNodeIP()
	if physicalIP != "" {
		log.Printf("Found physical node IP using direct methods: %s", physicalIP)
		c.cacheIPPermanently(physicalIP)
		return physicalIP
	}

	// Fallback to checking database configuration
	if c.cfg.MSSQL.Host != "" && c.cfg.MSSQL.Host != "localhost" && c.cfg.MSSQL.Host != "127.0.0.1" {
		// Check if host is an IP address
		if net.ParseIP(c.cfg.MSSQL.Host) != nil {
			log.Printf("Using configured IP address: %s", c.cfg.MSSQL.Host)
			c.cacheIPPermanently(c.cfg.MSSQL.Host)
			return c.cfg.MSSQL.Host
		}
	}

	// Last resort: Use SQL Server to get connection info
	dbIP := c.getIPFromSQLServer()
	if dbIP != "" {
		log.Printf("Using SQL Server-reported IP address: %s", dbIP)
		c.cacheIPPermanently(dbIP)
		return dbIP
	}

	// If all fails, use the standard Go library (which can still be wrong)
	log.Printf("All IP detection methods failed, using standard Go library as last resort")
	return c.getStandardLibraryIP()
}

// getPhysicalNodeIP attempts to get the physical node's IP address using direct OS commands
func (c *MSSQLCollector) getPhysicalNodeIP() string {
	if runtime.GOOS != "windows" {
		return ""
	}

	// First try with System.Net.Dns.GetHostAddresses - most reliable in Windows clusters
	psHostCmd := `
	try {
		$result = [System.Net.Dns]::GetHostAddresses($env:COMPUTERNAME) | 
		Where-Object { $_.AddressFamily -eq 'InterNetwork' -and
					  $_.IPAddressToString -notlike '169.254.*' -and
					  $_.IPAddressToString -notlike '127.0.0.*' } |
		Sort-Object -Property @{
			Expression = {
				if ($_.IPAddressToString -like '192.168.*') { 1 }
				elseif ($_.IPAddressToString -like '10.*') { 2 }
				else { 99 }
			}
		} | Select-Object -First 1 -ExpandProperty IPAddressToString
		
		if ($result) { 
			$result 
		} else { 
			Write-Output "" 
		}
	} catch {
		Write-Output ""
	}
	`

	physicalIP, err := c.runPowerShellWithTimeout(psHostCmd, 10)
	if err == nil {
		ipStr := strings.TrimSpace(string(physicalIP))
		if ipStr != "" && net.ParseIP(ipStr) != nil {
			log.Printf("IP from DNS resolution (System.Net.Dns): %s", ipStr)
			return ipStr
		}
	}

	// Rest of the existing implementation as fallback
	// Execute ipconfig directly to get all adapters
	// We'll parse the output ourselves to avoid PowerShell overhead
	cmd := exec.Command("ipconfig", "/all")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err != nil || ctx.Err() == context.DeadlineExceeded {
		log.Printf("Failed to run ipconfig: %v", err)
		return ""
	}

	// Parse output to find physical adapters and their IPs
	// We want to exclude:
	// - Virtual adapters (Hyper-V, VirtualBox, etc.)
	// - Cluster virtual adapters
	// - Disabled adapters
	// - Loopback adapters
	// - APIPA addresses (169.254.x.x)

	output := string(out)

	// First, split the output by adapter sections
	adapters := strings.Split(output, "Ethernet adapter")
	if len(adapters) <= 1 {
		adapters = strings.Split(output, "Wireless LAN adapter")
	}

	type AdapterInfo struct {
		Name            string
		Description     string
		PhysicalAddress string
		IPv4Address     string
		IsVirtual       bool
		IsCluster       bool
		IsDisabled      bool
	}

	var adapterInfos []AdapterInfo

	// Process each adapter section
	for _, adapter := range adapters {
		if len(adapter) < 10 { // Too short to be useful
			continue
		}

		// Extract adapter info
		info := AdapterInfo{}

		// Get name
		nameEnd := strings.Index(adapter, ":")
		if nameEnd > 0 {
			info.Name = strings.TrimSpace(adapter[:nameEnd])
		}

		// Check if disabled
		if strings.Contains(adapter, "Media disconnected") {
			info.IsDisabled = true
		}

		// Check for description to detect virtual adapters
		descLines := regexp.MustCompile(`Description[^:]*:\s*([^\r\n]+)`).FindStringSubmatch(adapter)
		if len(descLines) > 1 {
			info.Description = strings.TrimSpace(descLines[1])

			// Check for virtual adapters by description
			virtualKeywords := []string{
				"Hyper-V", "Virtual", "VMware", "VirtualBox", "Cluster", "Failover",
				"Microsoft Failover Cluster Virtual Adapter", "Teaming",
			}

			for _, keyword := range virtualKeywords {
				if strings.Contains(info.Description, keyword) {
					info.IsVirtual = true
					if strings.Contains(info.Description, "Cluster") || strings.Contains(info.Description, "Failover") {
						info.IsCluster = true
					}
					break
				}
			}
		}

		// Get physical address (MAC) - empty for virtual adapters
		macLines := regexp.MustCompile(`Physical Address[^:]*:\s*([^\r\n]+)`).FindStringSubmatch(adapter)
		if len(macLines) > 1 {
			info.PhysicalAddress = strings.TrimSpace(macLines[1])
			if info.PhysicalAddress == "" {
				info.IsVirtual = true // No MAC usually means virtual
			}
		}

		// Get IPv4 Address
		ipLines := regexp.MustCompile(`IPv4 Address[^:]*:\s*([^\r\n]+)`).FindStringSubmatch(adapter)
		if len(ipLines) > 1 {
			ipWithMask := strings.TrimSpace(ipLines[1])
			// Extract just the IP part (remove subnet mask if present)
			ip := regexp.MustCompile(`(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`).FindString(ipWithMask)
			info.IPv4Address = ip

			// Exclude APIPA addresses
			if strings.HasPrefix(ip, "169.254.") {
				info.IsVirtual = true // APIPA addresses are usually not what we want
			}
		}

		// Only add adapters with IP addresses
		if info.IPv4Address != "" {
			adapterInfos = append(adapterInfos, info)
		}
	}

	// Sort adapters by priority:
	// 1. Physical (non-virtual) adapters
	// 2. Non-cluster adapters
	// 3. Non-disabled adapters
	// 4. IP address preference (192.168.x.x > 10.x.x.x)
	sort.Slice(adapterInfos, func(i, j int) bool {
		// First priority: Physical adapters
		if !adapterInfos[i].IsVirtual && adapterInfos[j].IsVirtual {
			return true
		}
		if adapterInfos[i].IsVirtual && !adapterInfos[j].IsVirtual {
			return false
		}

		// Second priority: Non-cluster adapters
		if !adapterInfos[i].IsCluster && adapterInfos[j].IsCluster {
			return true
		}
		if adapterInfos[i].IsCluster && !adapterInfos[j].IsCluster {
			return false
		}

		// Third priority: Non-disabled adapters
		if !adapterInfos[i].IsDisabled && adapterInfos[j].IsDisabled {
			return true
		}
		if adapterInfos[i].IsDisabled && !adapterInfos[j].IsDisabled {
			return false
		}

		// Fourth priority: IP address preference
		if strings.HasPrefix(adapterInfos[i].IPv4Address, "192.168.") &&
			!strings.HasPrefix(adapterInfos[j].IPv4Address, "192.168.") {
			return true
		}
		if strings.HasPrefix(adapterInfos[i].IPv4Address, "10.") &&
			!strings.HasPrefix(adapterInfos[j].IPv4Address, "10.") &&
			!strings.HasPrefix(adapterInfos[j].IPv4Address, "192.168.") {
			return true
		}

		return false
	})

	// Log and return the best match
	if len(adapterInfos) > 0 {
		bestAdapter := adapterInfos[0]
		log.Printf("Best adapter match: Name=%s, Desc=%s, IP=%s, IsVirtual=%v, IsCluster=%v",
			bestAdapter.Name, bestAdapter.Description, bestAdapter.IPv4Address,
			bestAdapter.IsVirtual, bestAdapter.IsCluster)

		return bestAdapter.IPv4Address
	}

	// If we couldn't parse ipconfig output, try alternative methods
	return c.getIPFromDirectCommands()
}

// getIPFromDirectCommands uses direct Windows commands to find the physical IP
func (c *MSSQLCollector) getIPFromDirectCommands() string {
	if runtime.GOOS != "windows" {
		return ""
	}

	// Try using netsh command - more reliable than PowerShell for this
	cmd := exec.Command("netsh", "interface", "ipv4", "show", "ipaddress")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err != nil || ctx.Err() == context.DeadlineExceeded {
		log.Printf("Failed to run netsh: %v", err)
		return ""
	}

	// Parse the output to find IP addresses excluding virtual/cluster ones
	output := string(out)
	lines := strings.Split(output, "\n")

	var candidateIPs []string
	var currentAdapter string
	var isVirtual bool

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.Contains(line, "Configuration for interface") {
			// New adapter section
			currentAdapter = line
			isVirtual = strings.Contains(strings.ToLower(currentAdapter), "virtual") ||
				strings.Contains(strings.ToLower(currentAdapter), "cluster") ||
				strings.Contains(strings.ToLower(currentAdapter), "loopback")
			continue
		}

		if strings.Contains(line, "IP Address:") && !isVirtual {
			// Extract IP address
			ipMatch := regexp.MustCompile(`(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`).FindString(line)
			if ipMatch != "" && ipMatch != "127.0.0.1" && !strings.HasPrefix(ipMatch, "169.254.") {
				candidateIPs = append(candidateIPs, ipMatch)
			}
		}
	}

	// Sort IPs to prioritize 192.168.x.x, then 10.x.x.x
	sort.Slice(candidateIPs, func(i, j int) bool {
		if strings.HasPrefix(candidateIPs[i], "192.168.") && !strings.HasPrefix(candidateIPs[j], "192.168.") {
			return true
		}
		if strings.HasPrefix(candidateIPs[i], "10.") && !strings.HasPrefix(candidateIPs[j], "10.") && !strings.HasPrefix(candidateIPs[j], "192.168.") {
			return true
		}
		return false
	})

	if len(candidateIPs) > 0 {
		log.Printf("IP from netsh command: %s", candidateIPs[0])
		return candidateIPs[0]
	}

	// Last resort - try simple PowerShell command to get physical adapters only
	psScript := `
	try {
		Get-NetAdapter | 
		Where-Object { 
			$_.Status -eq 'Up' -and 
			-not $_.Virtual -and 
			$_.InterfaceDescription -notlike '*Cluster*' -and
			$_.InterfaceDescription -notlike '*Virtual*' -and
			$_.InterfaceDescription -notlike '*VMware*' -and
			$_.InterfaceDescription -notlike '*VirtualBox*'
		} | 
		Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop | 
		Where-Object {
			$_.IPAddress -notlike '127.0.0.1' -and
			$_.IPAddress -notlike '169.254.*'
		} |
		Sort-Object -Property @{
			Expression = {
				if ($_.IPAddress -like '192.168.*') { 1 }
				elseif ($_.IPAddress -like '10.*') { 2 }
				else { 99 }
			}
		} | 
		Select-Object -First 1 -ExpandProperty IPAddress
	} catch {
		Write-Output ""
	}
	`

	physicalIP, err := c.runPowerShellWithTimeout(psScript, 10)
	if err == nil && len(strings.TrimSpace(string(physicalIP))) > 0 {
		ipStr := strings.TrimSpace(string(physicalIP))
		log.Printf("IP from physical network adapter query: %s", ipStr)
		return ipStr
	}

	return ""
}

// getStandardLibraryIP uses the standard Go library to find the IP
func (c *MSSQLCollector) getStandardLibraryIP() string {
	// Utility function to check if an IP is likely to be a physical adapter IP
	isPreferredIP := func(ip string) bool {
		// Filter out APIPA addresses (169.254.x.x)
		if strings.HasPrefix(ip, "169.254.") {
			return false
		}

		// Prioritize specific private network ranges
		if strings.HasPrefix(ip, "192.168.") ||
			strings.HasPrefix(ip, "10.") {
			return true
		}

		// Check 172.16-31.x.x range
		if strings.HasPrefix(ip, "172.") {
			parts := strings.Split(ip, ".")
			if len(parts) > 1 {
				second, err := strconv.Atoi(parts[1])
				if err == nil && second >= 16 && second <= 31 {
					return true
				}
			}
		}

		return true
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Printf("Failed to get interface addresses: %v", err)
		return "Unknown"
	}

	var validIPs []string
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipStr := ipnet.IP.String()
				if isPreferredIP(ipStr) {
					validIPs = append(validIPs, ipStr)
				}
			}
		}
	}

	// Sort IPs by preference
	sort.Slice(validIPs, func(i, j int) bool {
		if strings.HasPrefix(validIPs[i], "192.168.") && !strings.HasPrefix(validIPs[j], "192.168.") {
			return true
		}
		if strings.HasPrefix(validIPs[i], "10.") && !strings.HasPrefix(validIPs[j], "10.") && !strings.HasPrefix(validIPs[j], "192.168.") {
			return true
		}
		return false
	})

	if len(validIPs) > 0 {
		return validIPs[0]
	}

	return "Unknown"
}

// getIPFromSQLServer attempts to get the IP address directly from SQL Server
func (c *MSSQLCollector) getIPFromSQLServer() string {
	db, err := c.GetClient()
	if err != nil {
		return ""
	}
	defer db.Close()

	// Get physical server NetBIOS name (not the cluster virtual name)
	var serverName string
	err = db.QueryRow("SELECT SERVERPROPERTY('ComputerNamePhysicalNetBIOS')").Scan(&serverName)
	if err == nil && serverName != "" {
		log.Printf("Physical server name: %s", serverName)

		// Try to use xp_cmdshell to get the IP address directly if available
		if c.canExecuteXPCmdShell() {
			// First try the most reliable method - System.Net.Dns
			var output sql.NullString
			err = db.QueryRow(`
				DECLARE @cmd nvarchar(1000)
				SET @cmd = 'powershell -Command "[System.Net.Dns]::GetHostAddresses('''+@@SERVERNAME+''') | Where-Object { $_.AddressFamily -eq ''InterNetwork'' -and $_.IPAddressToString -notlike ''169.254.*'' -and $_.IPAddressToString -notlike ''127.0.0.*'' } | Select-Object -First 1 -ExpandProperty IPAddressToString"'
				
				DECLARE @output TABLE (line nvarchar(500))
				INSERT INTO @output (line)
				EXEC xp_cmdshell @cmd
				
				SELECT TOP 1 line
				FROM @output
				WHERE line IS NOT NULL AND line <> ''
			`).Scan(&output)

			if err == nil && output.Valid {
				ipStr := strings.TrimSpace(output.String)
				log.Printf("IP from xp_cmdshell (System.Net.Dns): %s", ipStr)
				if net.ParseIP(ipStr) != nil {
					return ipStr
				}
			}

			// If first method fails, try using ipconfig command
			err = db.QueryRow(`
				DECLARE @cmd nvarchar(1000)
				SET @cmd = 'ipconfig /all'
				
				DECLARE @output TABLE (line nvarchar(1000))
				INSERT INTO @output (line)
				EXEC xp_cmdshell @cmd
				
				-- Extract sections with adapter info
				DECLARE @currentAdapter nvarchar(255) = ''
				DECLARE @isVirtual bit = 0
				DECLARE @adapterOutput TABLE (
					adapter nvarchar(255),
					isVirtual bit,
					line nvarchar(1000)
				)
				
				-- Process the ipconfig output line by line
				INSERT INTO @adapterOutput (adapter, isVirtual, line)
				SELECT 
					CASE 
						WHEN line LIKE 'Ethernet adapter%' OR line LIKE 'Wireless%' 
						THEN SUBSTRING(line, 1, CHARINDEX(':', line)-1)
						ELSE @currentAdapter 
					END,
					CASE
						WHEN (line LIKE '%Description%' AND (
							line LIKE '%Virtual%' OR 
							line LIKE '%Hyper-V%' OR 
							line LIKE '%VMware%' OR
							line LIKE '%VirtualBox%' OR
							line LIKE '%Cluster%' OR
							line LIKE '%Team%' OR
							line LIKE '%Microsoft Failover%'
						)) THEN 1
						ELSE 0
					END,
					line
				FROM @output
				WHERE line IS NOT NULL AND LEN(LTRIM(line)) > 0
				
				-- Now find IP addresses from non-virtual adapters
				SELECT TOP 1 SUBSTRING(line, CHARINDEX(':', line) + 1, LEN(line))
				FROM @adapterOutput
				WHERE line LIKE '%IPv4 Address%'
				AND adapter IN (
					SELECT DISTINCT adapter 
					FROM @adapterOutput 
					WHERE isVirtual = 0
				)
				AND line NOT LIKE '%169.254.%'
				AND line NOT LIKE '%127.0.0.1%'
				ORDER BY
					CASE
						WHEN line LIKE '%192.168.%' THEN 1
						WHEN line LIKE '%10.%' THEN 2
						ELSE 3
					END
			`).Scan(&output)

			if err == nil && output.Valid {
				// Extract the IP address from output
				ipStr := strings.TrimSpace(output.String)
				log.Printf("IP from xp_cmdshell (ipconfig): %s", ipStr)

				// Extract just the IP using regex
				re := regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
				matches := re.FindStringSubmatch(ipStr)
				if len(matches) > 0 {
					return matches[0]
				}
			}

			// If second method fails, try another PowerShell method
			err = db.QueryRow(`
				DECLARE @cmd nvarchar(1000)
				SET @cmd = 'powershell -Command "Get-NetAdapter | Where-Object { -not $_.Virtual -and $_.Status -eq ''Up'' } | Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike ''169.254.*'' -and $_.IPAddress -notlike ''127.0.0.1'' } | Select-Object -First 1 -ExpandProperty IPAddress"'
				
				DECLARE @output TABLE (line nvarchar(500))
				INSERT INTO @output (line)
				EXEC xp_cmdshell @cmd
				
				SELECT TOP 1 line
				FROM @output
				WHERE line IS NOT NULL AND line <> ''
			`).Scan(&output)

			if err == nil && output.Valid {
				ipStr := strings.TrimSpace(output.String)
				log.Printf("IP from xp_cmdshell (Get-NetAdapter): %s", ipStr)
				if net.ParseIP(ipStr) != nil {
					return ipStr
				}
			}
		}

		// Try to use DNS resolution as fallback
		log.Printf("Trying to resolve physical host name: %s", serverName)
		if ips, err := net.LookupIP(serverName); err == nil && len(ips) > 0 {
			// Filter and sort IPs
			var validIPs []string
			for _, ip := range ips {
				ipStr := ip.String()
				if ip.To4() != nil && ipStr != "127.0.0.1" && !strings.HasPrefix(ipStr, "169.254.") {
					validIPs = append(validIPs, ipStr)
				}
			}

			// Sort IPs to prioritize 192.168.x.x, then 10.x.x.x
			sort.Slice(validIPs, func(i, j int) bool {
				if strings.HasPrefix(validIPs[i], "192.168.") && !strings.HasPrefix(validIPs[j], "192.168.") {
					return true
				}
				if strings.HasPrefix(validIPs[i], "10.") && !strings.HasPrefix(validIPs[j], "10.") && !strings.HasPrefix(validIPs[j], "192.168.") {
					return true
				}
				return false
			})

			if len(validIPs) > 0 {
				log.Printf("Resolved physical server name %s to IP: %s", serverName, validIPs[0])
				return validIPs[0]
			}
		}
	}

	// Try to get IP addresses from TCP connections and filter out cluster IPs
	var serverIP string
	err = db.QueryRow(`
		WITH NetworkInfo AS (
			-- Get all local IP addresses used for connections
			SELECT 
				local_net_address,
				CASE
					WHEN local_net_address LIKE '192.168.%' THEN 1
					WHEN local_net_address LIKE '10.%' THEN 2
					ELSE 3
				END AS priority
			FROM sys.dm_exec_connections 
			WHERE local_net_address IS NOT NULL 
			AND local_net_address <> '127.0.0.1'
			AND local_net_address NOT LIKE '169.254.%'
		)
		SELECT TOP 1 local_net_address
		FROM NetworkInfo
		ORDER BY priority
	`).Scan(&serverIP)

	if err == nil && serverIP != "" {
		// Check if this might be a cluster IP
		isClusterIP := false

		// Try to verify against availability group listener IPs
		var listenerIPCount int
		err = db.QueryRow(`
			IF OBJECT_ID('sys.availability_group_listener_ip_addresses') IS NOT NULL
			BEGIN
				SELECT COUNT(*) 
				FROM sys.availability_group_listener_ip_addresses 
				WHERE ip_address = @ip
			END
			ELSE
			BEGIN
				SELECT 0
			END
		`, sql.Named("ip", serverIP)).Scan(&listenerIPCount)

		if err == nil && listenerIPCount > 0 {
			isClusterIP = true
			log.Printf("IP %s appears to be an availability group listener IP", serverIP)
		}

		if !isClusterIP {
			log.Printf("Using SQL connection IP: %s", serverIP)
			return serverIP
		}
	}

	// Fallback to basic SQL Server query for network adapters
	err = db.QueryRow(`
		SELECT TOP 1 
			CASE 
				WHEN CHARINDEX(',', ip_address) > 0 
				THEN SUBSTRING(ip_address, 1, CHARINDEX(',', ip_address) - 1) 
				ELSE ip_address 
			END
		FROM sys.dm_tcp_listener_states
		WHERE ip_address <> '127.0.0.1'
		  AND ip_address NOT LIKE '169.254.%'
		  -- Exclude IPs used by availability group listeners
		  AND ip_address NOT IN (
				SELECT ISNULL(ip_address, '') 
				FROM sys.availability_group_listener_ip_addresses
				WHERE ip_address IS NOT NULL
		  )
		ORDER BY 
			CASE 
				WHEN ip_address LIKE '192.168.%' THEN 1
				WHEN ip_address LIKE '10.%' THEN 2 
				ELSE 3
			END
	`).Scan(&serverIP)

	if err == nil && serverIP != "" {
		log.Printf("Using TCP listener IP (filtered): %s", serverIP)
		return serverIP
	}

	log.Printf("Could not get IP address from SQL Server")
	return ""
}

// cacheIPPermanently caches the IP address with a much longer expiration
func (c *MSSQLCollector) cacheIPPermanently(ip string) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("IP önbellek dizini oluşturulamadı: %v", err)
		return
	}

	// Create a persistent IP cache file
	cacheFile := filepath.Join(cacheDir, "mssql_ip_permanent.txt")
	if err := os.WriteFile(cacheFile, []byte(ip), 0644); err != nil {
		log.Printf("IP kalıcı önbelleğe kaydedilemedi: %v", err)
	} else {
		log.Printf("IP adresi kalıcı önbelleğe kaydedildi: %s", ip)
	}

	// Also update the regular cache
	c.cacheIP(ip)
}

// Override getCachedIP to check permanent cache first
func (c *MSSQLCollector) getCachedIP() string {
	// First check the permanent cache
	permanentCacheFile := filepath.Join(getCacheDir(), "mssql_ip_permanent.txt")
	data, err := os.ReadFile(permanentCacheFile)
	if err == nil {
		ipStr := strings.TrimSpace(string(data))
		if ipStr != "" && ipStr != "Unknown" {
			log.Printf("Kalıcı önbellekten IP adresi alındı: %s", ipStr)
			return ipStr
		}
	}

	// Fall back to regular cache
	regularCacheFile := filepath.Join(getCacheDir(), "mssql_ip_cache.txt")
	data, err = os.ReadFile(regularCacheFile)
	if err != nil {
		return ""
	}

	// Check if cache is still valid (24 hours)
	fileInfo, err := os.Stat(regularCacheFile)
	if err != nil {
		return ""
	}

	if time.Since(fileInfo.ModTime()) > 24*time.Hour {
		// Cache too old
		return ""
	}

	return strings.TrimSpace(string(data))
}

func getCacheDir() string {
	if runtime.GOOS == "windows" {
		return filepath.Join("C:\\Program Files\\ClusterEyeAgent", "cache")
	} else {
		dir := "/var/lib/clustereye-agent/cache"
		if homeDir, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(homeDir, ".clustereye", "cache")
		}
		return dir
	}
}

// getTotalvCpu returns the total number of vCPUs
func (c *MSSQLCollector) getTotalvCpu() int32 {
	// Check for cached CPU count
	if cachedCount := c.getCachedCPUCount(); cachedCount > 0 {
		log.Printf("Using cached CPU count: %d", cachedCount)
		return cachedCount
	}

	var cpuCount int32

	if runtime.GOOS == "windows" {
		// Use a more direct method to get CPU count that's lighter weight
		psCommand := `try { (Get-CimInstance Win32_ComputerSystem -ErrorAction Stop).NumberOfLogicalProcessors } catch { Write-Output "0" }`
		out, err := c.runPowerShellWithTimeout(psCommand, 5) // 5 seconds timeout
		if err == nil {
			count, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
			if err == nil && count > 0 {
				cpuCount = int32(count)
				// Cache the result
				c.cacheCPUCount(cpuCount)
				return cpuCount
			}
		}
	}

	// For Unix-like systems
	cmd := exec.Command("sh", "-c", "nproc")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil && ctx.Err() != context.DeadlineExceeded {
		count, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
		if err == nil {
			cpuCount = int32(count)
			// Cache the result
			c.cacheCPUCount(cpuCount)
			return cpuCount
		}
	}

	// Fallback to runtime.NumCPU()
	cpuCount = int32(runtime.NumCPU())
	// Cache the result
	c.cacheCPUCount(cpuCount)
	return cpuCount
}

// Helper function to cache CPU count
func (c *MSSQLCollector) cacheCPUCount(count int32) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("CPU count önbellek dizini oluşturulamadı: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_cpu_count.txt")
	if err := os.WriteFile(cacheFile, []byte(strconv.Itoa(int(count))), 0644); err != nil {
		log.Printf("CPU count önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached CPU count
func (c *MSSQLCollector) getCachedCPUCount() int32 {
	cacheFile := filepath.Join(getCacheDir(), "mssql_cpu_count.txt")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return 0
	}

	// CPU count rarely changes, so we only invalidate cache on agent restart
	count, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 32)
	if err != nil {
		return 0
	}

	return int32(count)
}

// getTotalMemory returns the total memory in bytes
func (c *MSSQLCollector) getTotalMemory() int64 {
	// Check for cached memory value
	if cachedMem := c.getCachedMemory(); cachedMem > 0 {
		log.Printf("Using cached memory value: %d bytes", cachedMem)
		return cachedMem
	}

	var totalMemory int64

	if runtime.GOOS == "windows" {
		// Use a more efficient command with -NoProfile to speed up PowerShell startup
		psCommand := `try { (Get-CimInstance Win32_ComputerSystem -ErrorAction Stop).TotalPhysicalMemory } catch { Write-Output "0" }`
		out, err := c.runPowerShellWithTimeout(psCommand, 5) // 5 seconds timeout
		if err == nil {
			memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err == nil && memTotal > 0 {
				totalMemory = memTotal
				// Cache the result
				c.cacheMemory(totalMemory)
				return totalMemory
			}
		}
	}

	// For Unix-like systems
	cmd := exec.Command("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil && ctx.Err() != context.DeadlineExceeded {
		memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err == nil {
			totalMemory = memTotal * 1024 // Convert KB to bytes
			// Cache the result
			c.cacheMemory(totalMemory)
			return totalMemory
		}
	}

	// If we couldn't get memory, return a reasonable default
	if totalMemory <= 0 {
		// Default to 8GB if we can't determine
		totalMemory = 8 * 1024 * 1024 * 1024
	}

	return totalMemory
}

// Helper function to cache memory value
func (c *MSSQLCollector) cacheMemory(memBytes int64) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("Memory önbellek dizini oluşturulamadı: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_memory.txt")
	if err := os.WriteFile(cacheFile, []byte(strconv.FormatInt(memBytes, 10)), 0644); err != nil {
		log.Printf("Memory önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached memory value
func (c *MSSQLCollector) getCachedMemory() int64 {
	cacheFile := filepath.Join(getCacheDir(), "mssql_memory.txt")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return 0
	}

	// Total physical memory rarely changes, so cache is valid until agent restart
	mem, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}

	return mem
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

// cacheIP caches the IP address with 24 hour expiration
func (c *MSSQLCollector) cacheIP(ip string) {
	cacheDir := getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("IP önbellek dizini oluşturulamadı: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_ip_cache.txt")
	if err := os.WriteFile(cacheFile, []byte(ip), 0644); err != nil {
		log.Printf("IP önbelleğe kaydedilemedi: %v", err)
	}
}
