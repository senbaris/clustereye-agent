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
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb" // MSSQL driver
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
)

// Windows platformu için gerekli paketleri koşullu olarak yükleyeceğiz
var useWMI = runtime.GOOS == "windows"

// Global collector instance for backward compatibility and thread safety
var defaultMSSQLCollector *MSSQLCollector
var collectorMutex sync.RWMutex // Thread-safe access to collector instance

// Global collector instance management functions for thread safety
var startupRecoveryOnce sync.Once // Prevent multiple startup recovery goroutines

// MSSQLCollector mssql için veri toplama yapısı
type MSSQLCollector struct {
	cfg                *config.AgentConfig
	lastCollectionTime time.Time
	collectionInterval time.Duration
	maxRetries         int
	backoffDuration    time.Duration
	isHealthy          bool
	lastHealthCheck    time.Time
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
	FdPercent   int32            // File descriptor usage percentage
	Port        string           // SQL Server port
	TotalvCpu   int32            // Toplam vCPU sayısı
	TotalMemory int64            // Toplam RAM miktarı (byte cinsinden)
	ConfigPath  string           // SQL Server configuration file path
	Database    string           // Database name
	IsHAEnabled bool             // AlwaysOn or other HA configuration enabled
	HARole      string           // Role in HA topology (PRIMARY, SECONDARY, etc.)
	Edition     string           // SQL Server edition (Enterprise, Standard, etc.)
	AlwaysOn    *AlwaysOnMetrics // Detailed AlwaysOn metrics
}

// NewMSSQLCollector creates a new MSSQL collector with rate limiting
func NewMSSQLCollector(cfg *config.AgentConfig) *MSSQLCollector {
	return &MSSQLCollector{
		cfg:                cfg,
		collectionInterval: 25 * time.Second, // Reduced from 30s to 25s to prevent conflict with 30s periodic reporting
		maxRetries:         3,
		backoffDuration:    5 * time.Second,
		isHealthy:          true,
		lastHealthCheck:    time.Now(),
	}
}

// GetClient returns a SQL Server connection
func (c *MSSQLCollector) GetClient() (*sql.DB, error) {
	// Ensure collector is initialized
	EnsureDefaultCollector()

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetClient: %v", r)
			// Mark as unhealthy after panic - thread-safe
			if collector := GetDefaultCollectorSafe(); collector != nil {
				collectorMutex.Lock()
				collector.isHealthy = false
				collector.collectionInterval = 5 * time.Minute
				collectorMutex.Unlock()
			}
		}
	}()

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

	// Additional connection parameters with aggressive timeouts
	connStr += ";connection timeout=10;dial timeout=5;keepalive=30"

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, fmt.Errorf("MSSQL bağlantısı kurulamadı: %w", err)
	}

	// Set connection pool limits to prevent resource exhaustion
	db.SetMaxOpenConns(2)                   // Maximum 2 concurrent connections
	db.SetMaxIdleConns(1)                   // Keep maximum 1 idle connection
	db.SetConnMaxLifetime(30 * time.Second) // Close connections after 30 seconds
	db.SetConnMaxIdleTime(10 * time.Second) // Close idle connections after 10 seconds

	// Test the connection with a shorter timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	// Ensure collector is initialized
	EnsureDefaultCollector()

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetMSSQLStatus: %v", r)
			// Mark as unhealthy after panic - thread-safe
			if collector := GetDefaultCollectorSafe(); collector != nil {
				collectorMutex.Lock()
				collector.isHealthy = false
				collector.collectionInterval = 5 * time.Minute
				collectorMutex.Unlock()
			}
		}
	}()

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
	// Ensure collector is initialized
	EnsureDefaultCollector()

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetMSSQLVersion: %v", r)
			// Mark as unhealthy after panic - thread-safe
			if collector := GetDefaultCollectorSafe(); collector != nil {
				collectorMutex.Lock()
				collector.isHealthy = false
				collector.collectionInterval = 5 * time.Minute
				collectorMutex.Unlock()
			}
		}
	}()

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
	// Comprehensive debug logging
	logger.Debug("GetNodeStatus: Starting function")

	// Ensure collector is initialized
	EnsureDefaultCollector()
	logger.Debug("GetNodeStatus: EnsureDefaultCollector completed")

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetNodeStatus: %v", r)
			// Log detailed stack trace
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger.Error("GetNodeStatus STACK TRACE: %s", string(buf[:n]))

			// Mark as unhealthy after panic - thread-safe
			if collector := GetDefaultCollectorSafe(); collector != nil {
				collectorMutex.Lock()
				collector.isHealthy = false
				collector.collectionInterval = 5 * time.Minute
				collectorMutex.Unlock()
				logger.Error("GetNodeStatus: Marked collector as unhealthy after panic")
			} else {
				logger.Error("GetNodeStatus: Collector is nil after panic!")
			}
		}
	}()

	// Check collector state
	collector := GetDefaultCollectorSafe()
	if collector == nil {
		logger.Error("GetNodeStatus: Collector is nil, attempting re-initialization")
		EnsureDefaultCollector()
		collector = GetDefaultCollectorSafe()
		if collector == nil {
			logger.Error("GetNodeStatus: Collector still nil after re-initialization")
			return "UNKNOWN", false
		}
	}
	logger.Debug("GetNodeStatus: Collector state validated")

	// Önce önbellekteki bilgileri kontrol et
	haStateKey := "mssql_ha_state"

	// Alarm monitor cache'e erişim için bu fonksiyonu oluşturuyoruz
	haState, hasHAState := c.getHAStateFromCache(haStateKey)

	// Bağlantı dene
	logger.Debug("GetNodeStatus: About to call GetClient")
	db, err := c.GetClient()
	if err != nil {
		logger.Error("GetNodeStatus: Database connection failed: %v", err)

		// Önbellekte HA bilgisi varsa onu kullan
		if hasHAState {
			log.Printf("Servis kapalı ama önbellekte HA durumu mevcut: %s, önbellekteki bilgiler kullanılıyor", haState.role)
			return haState.role, true
		}

		return "STANDALONE", false
	}
	defer func() {
		logger.Debug("GetNodeStatus: Closing database connection")
		if closeErr := db.Close(); closeErr != nil {
			logger.Error("GetNodeStatus: Error closing DB: %v", closeErr)
		}
	}()
	logger.Debug("GetNodeStatus: Database connection established")

	// Check if AlwaysOn is enabled
	var isHAEnabled int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Debug("GetNodeStatus: About to execute IsHadrEnabled query")
	err = db.QueryRowContext(ctx, `
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
	logger.Debug("GetNodeStatus: About to execute role query")
	err = db.QueryRowContext(ctx, `
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
		err = db.QueryRowContext(ctx, `
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

	logger.Debug("GetNodeStatus: Query completed, role=%s", role)
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

// Helper function to cache SQL Server config path
func (c *MSSQLCollector) cacheConfigPath(path string) {
	cacheDir := c.getCacheDir()
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
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_config_path.txt")
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
	cacheDir := c.getCacheDir()
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
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_disk_usage.json")
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

// BatchCollectSystemMetrics collects all system metrics with more efficient direct commands
func (c *MSSQLCollector) BatchCollectSystemMetrics() map[string]interface{} {
	// Get cached STATIC metrics (CPU count, total memory, IP address) - 6 hour cache
	cachedStaticMetrics := c.getCachedSystemMetrics()

	// Always collect DYNAMIC metrics fresh (CPU usage, memory usage, disk usage)
	metrics := make(map[string]interface{})

	// Start with cached static metrics if available
	if cachedStaticMetrics != nil {
		log.Printf("Using cached static metrics: %v", getMapKeys(cachedStaticMetrics))
		for k, v := range cachedStaticMetrics {
			if k != "timestamp" { // Don't copy timestamp
				metrics[k] = v
			}
		}
	}

	// Measure MSSQL response time (ALWAYS fresh - don't cache)
	c.measureResponseTime(metrics)

	// For non-Windows systems, fallback to individual calls
	if runtime.GOOS != "windows" {
		log.Printf("Non-Windows system detected, collecting metrics individually")

		totalMem := int64(c.getTotalMemory())
		freeMem := int64(float64(totalMem) * 0.2) // Estimate 20% free
		memUsage := float64(80)                   // Estimate 80% usage

		metrics := map[string]interface{}{
			"cpu_count":        int32(c.getTotalvCpu()),         // Ensure int32
			"cpu_usage":        float64(0),                      // Default to 0% for Unix
			"total_memory":     totalMem,                        // Ensure int64
			"free_memory":      freeMem,                         // Estimated free memory
			"memory_usage":     memUsage,                        // Estimated memory usage
			"total_disk":       int64(100 * 1024 * 1024 * 1024), // 100GB default
			"free_disk":        int64(30 * 1024 * 1024 * 1024),  // 30GB default
			"ip_address":       string(c.getLocalIP()),          // Ensure string
			"response_time_ms": float64(0),
		}

		// Add response time for non-Windows systems too
		c.measureResponseTime(metrics)

		// Cache the results
		c.cacheSystemMetrics(metrics)
		return metrics
	}

	log.Printf("Collecting system metrics with lightweight direct commands")

	// Use direct wmic commands for better performance
	// metrics map is already initialized above

	// 1. Try getting CPU count and usage - First try computersystem for total count
	cmd := exec.Command("wmic", "computersystem", "get", "NumberOfLogicalProcessors", "/value")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil {
		// Parse the output for total logical processors
		outStr := strings.TrimSpace(string(out))
		if matches := regexp.MustCompile(`NumberOfLogicalProcessors=(\d+)`).FindStringSubmatch(outStr); len(matches) > 1 {
			if count, err := strconv.ParseInt(matches[1], 10, 32); err == nil && count > 0 {
				metrics["cpu_count"] = int32(count)
			}
		}
	}

	// Get CPU usage percentage (ALWAYS fresh - don't cache)
	cmd = exec.Command("wmic", "cpu", "get", "loadpercentage", "/value")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err = cmdWithTimeout.Output()
	if err == nil {
		outStr := strings.TrimSpace(string(out))
		if matches := regexp.MustCompile(`LoadPercentage=(\d+)`).FindStringSubmatch(outStr); len(matches) > 1 {
			if usage, err := strconv.ParseFloat(matches[1], 64); err == nil {
				metrics["cpu_usage"] = usage
			}
		}
	}

	// If computersystem failed, fall back to summing individual CPUs
	if metrics["cpu_count"] == nil {
		cmd = exec.Command("wmic", "cpu", "get", "NumberOfLogicalProcessors", "/value")
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		out, err = cmdWithTimeout.Output()
		if err == nil {
			// Parse all CPU entries and sum them up
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			totalCores := int32(0)
			cpuCount := 0

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if strings.Contains(line, "NumberOfLogicalProcessors=") {
					parts := strings.Split(line, "=")
					if len(parts) == 2 {
						cpuCountStr := strings.TrimSpace(parts[1])
						if cpuCountStr != "" {
							if count, err := strconv.ParseInt(cpuCountStr, 10, 32); err == nil && count > 0 {
								totalCores += int32(count)
								cpuCount++
							}
						}
					}
				}
			}

			if totalCores > 0 {
				metrics["cpu_count"] = totalCores
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
		// Parse the output line by line
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		for _, line := range lines {
			if strings.Contains(line, "TotalPhysicalMemory=") {
				parts := strings.Split(line, "=")
				if len(parts) == 2 {
					memStr := strings.TrimSpace(parts[1])
					if memStr != "" {
						// Parse as int64 to avoid scientific notation
						if totalMem, err := strconv.ParseInt(memStr, 10, 64); err == nil && totalMem > 0 {
							metrics["total_memory"] = int64(totalMem) // Ensure int64 type
							break
						}
					}
				}
			}
		}
	}

	// Get free memory and calculate memory usage (ALWAYS fresh - don't cache)
	cmd = exec.Command("wmic", "OS", "get", "FreePhysicalMemory", "/value")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err = cmdWithTimeout.Output()
	if err == nil {
		outStr := strings.TrimSpace(string(out))
		if matches := regexp.MustCompile(`FreePhysicalMemory=(\d+)`).FindStringSubmatch(outStr); len(matches) > 1 {
			if freeMem, err := strconv.ParseInt(matches[1], 10, 64); err == nil {
				freeMemBytes := freeMem * 1024 // Convert KB to bytes
				metrics["free_memory"] = freeMemBytes

				// Calculate memory usage percentage if we have total memory
				if totalMem, ok := metrics["total_memory"].(int64); ok && totalMem > 0 {
					usedMem := totalMem - freeMemBytes
					memUsage := float64(usedMem) / float64(totalMem) * 100
					metrics["memory_usage"] = memUsage
				}
			}
		}
	}

	// 3. Try getting IP address using direct ipconfig command
	cmd = exec.Command("ipconfig", "/all")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err = cmdWithTimeout.Output()
	if err == nil {
		// Parse the output
		ipStr := string(out)
		ipv4Regex := regexp.MustCompile(`IPv4 Address[^:]*:\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`)
		matches := ipv4Regex.FindAllStringSubmatch(ipStr, -1)

		var validIPs []string
		for _, match := range matches {
			if len(match) >= 2 {
				ip := strings.TrimSpace(match[1])
				if ip != "127.0.0.1" && !strings.HasPrefix(ip, "169.254.") {
					validIPs = append(validIPs, ip)
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
			metrics["ip_address"] = string(validIPs[0]) // Ensure string type
		}
	}

	// 4. Get disk space information for C: drive (ALWAYS fresh - don't cache)
	cmd = exec.Command("wmic", "logicaldisk", "where", "DeviceID='C:'", "get", "Size,FreeSpace", "/value")
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err = cmdWithTimeout.Output()
	if err == nil {
		outStr := strings.TrimSpace(string(out))

		// Parse total disk size
		if matches := regexp.MustCompile(`Size=(\d+)`).FindStringSubmatch(outStr); len(matches) > 1 {
			if totalDisk, err := strconv.ParseInt(matches[1], 10, 64); err == nil {
				metrics["total_disk"] = totalDisk
			}
		}

		// Parse free disk space
		if matches := regexp.MustCompile(`FreeSpace=(\d+)`).FindStringSubmatch(outStr); len(matches) > 1 {
			if freeDisk, err := strconv.ParseInt(matches[1], 10, 64); err == nil {
				metrics["free_disk"] = freeDisk
			}
		}
	}

	// Fallback ONLY for STATIC metrics that couldn't be collected
	if metrics["cpu_count"] == nil {
		log.Printf("Using fallback for static CPU count")
		metrics["cpu_count"] = int32(c.getTotalvCpu()) // Ensure int32
	}

	if metrics["total_memory"] == nil {
		log.Printf("Using fallback for static total memory")
		metrics["total_memory"] = int64(c.getTotalMemory()) // Ensure int64
	}

	if metrics["ip_address"] == nil {
		log.Printf("Using fallback for static IP address")
		metrics["ip_address"] = string(c.getLocalIP()) // Ensure string
	}

	// DYNAMIC metrics are ALWAYS collected fresh - no fallbacks
	// cpu_usage, memory_usage, free_memory, total_disk, free_disk
	// These will be collected fresh every time above

	// Cache the results for a longer period (6 hours)
	c.cacheSystemMetrics(metrics)
	return metrics
}

// Helper function to cache ONLY STATIC system metrics (not dynamic ones like CPU usage, memory usage, disk usage)
func (c *MSSQLCollector) cacheSystemMetrics(metrics map[string]interface{}) {
	cacheDir := c.getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("System metrics önbellek dizini oluşturulamadı: %v", err)
		return
	}

	// ONLY cache static metrics that don't change frequently
	staticMetrics := make(map[string]interface{})

	// Cache ONLY static values - exclude dynamic metrics
	for k, v := range metrics {
		switch k {
		case "total_memory":
			// Total memory rarely changes - cache it
			if memVal, ok := v.(int64); ok && memVal > 0 {
				staticMetrics[k] = memVal
			} else {
				staticMetrics[k] = int64(8 * 1024 * 1024 * 1024) // 8GB default
			}
		case "cpu_count":
			// CPU count rarely changes - cache it
			if cpuVal, ok := v.(int32); ok && cpuVal > 0 {
				staticMetrics[k] = cpuVal
			} else {
				staticMetrics[k] = int32(1) // Default to 1 CPU
			}
		case "ip_address":
			// IP address rarely changes - cache it
			if ipVal, ok := v.(string); ok {
				staticMetrics[k] = strings.TrimSpace(ipVal)
			} else {
				staticMetrics[k] = "Unknown"
			}
		case "cpu_usage", "memory_usage", "free_memory", "total_disk", "free_disk", "response_time_ms":
			// DON'T cache dynamic metrics - they change frequently
			// These will be collected fresh every time
		default:
			// Copy other static values as-is
			staticMetrics[k] = v
		}
	}

	// Add timestamp with consistent format
	staticMetrics["timestamp"] = time.Now().Unix()

	// Cache only static metrics

	jsonData, err := json.Marshal(staticMetrics)
	if err != nil {
		log.Printf("Static metrics JSON encode hatası: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_system_metrics.json")
	if err := os.WriteFile(cacheFile, jsonData, 0644); err != nil {
		log.Printf("Static metrics önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached system metrics
func (c *MSSQLCollector) getCachedSystemMetrics() map[string]interface{} {
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_system_metrics.json")
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
	// Check if we should skip collection due to rate limiting or health issues
	if c.ShouldSkipCollection() {
		logger.Debug("Collection skipped due to rate limiting or health checks")
		return c.getLastKnownGoodInfo() // Return cached info instead
	}

	// Update collection time at the start to prevent concurrent collections
	c.SetCollectionTime()

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

			// Mark as unhealthy after panic
			c.isHealthy = false
			c.collectionInterval = 5 * time.Minute // Increase interval significantly
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
			logger.Debug("Raw cached CPU value: %v (type: %T)", cpuVal, cpuVal)
			switch v := cpuVal.(type) {
			case int32:
				totalvCpu = v
				logger.Debug("CPU converted from int32: %d", totalvCpu)
			case int:
				totalvCpu = int32(v)
				logger.Debug("CPU converted from int: %d", totalvCpu)
			case int64:
				totalvCpu = int32(v)
				logger.Debug("CPU converted from int64: %d", totalvCpu)
			case float64:
				totalvCpu = int32(v)
				logger.Debug("CPU converted from float64: %d", totalvCpu)
			default:
				logger.Debug("Cached CPU count has wrong type: %T, using fallback", cpuVal)
				totalvCpu = c.getTotalvCpu() // Fallback
			}
		} else {
			logger.Debug("No cached CPU count found")
			totalvCpu = c.getTotalvCpu() // Fallback
		}

		// Memory
		if memVal, ok := systemMetrics["total_memory"]; ok {
			logger.Debug("Raw cached memory value: %v (type: %T)", memVal, memVal)
			switch v := memVal.(type) {
			case int64:
				totalMemory = v
				logger.Debug("Memory converted from int64: %d", totalMemory)
			case int:
				totalMemory = int64(v)
				logger.Debug("Memory converted from int: %d", totalMemory)
			case float64:
				totalMemory = int64(v)
				logger.Debug("Memory converted from float64: %d (was: %g)", totalMemory, v)
			default:
				logger.Debug("Cached memory has wrong type: %T, using fallback", memVal)
				totalMemory = c.getTotalMemory() // Fallback
			}
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
	var alwaysOnMetrics *AlwaysOnMetrics

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

				// Collect detailed AlwaysOn metrics if HA is enabled
				// Only collect every 5 minutes to reduce database load
				if c.shouldCollectAlwaysOnMetrics() {
					alwaysOnMetrics, err = c.GetAlwaysOnMetrics()
					if err != nil {
						logger.Warning("AlwaysOn metrics collection failed: %v", err)
					} else {
						logger.Info("AlwaysOn metrics collected successfully")

						// Log AlwaysOn metrics for diagnostic purposes
						alwaysOnJSON, _ := c.SerializeAlwaysOnMetrics(alwaysOnMetrics)
						logger.Debug("AlwaysOn metrics: %s", alwaysOnJSON)
					}
				} else {
					// Try to get cached metrics if available
					alwaysOnMetrics = c.getCachedAlwaysOnMetrics()
					if alwaysOnMetrics != nil {
						logger.Debug("Using cached AlwaysOn metrics to reduce load")
					}
				}
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
				logger.Debug("Disk usage from SQL Server drive %s: FreeDisk=%s, UsagePercent=%d", driveLetter, freeDisk, usagePercent)
			} else {
				// Fallback to C drive
				freeDisk, usagePercent = c.getSafeDiskUsage("C")
				logger.Debug("Disk usage from fallback C drive: FreeDisk=%s, UsagePercent=%d", freeDisk, usagePercent)
			}
		} else {
			// Fallbacks
			configPath = c.getCachedConfigPath()
			freeDisk, usagePercent = c.getSafeDiskUsage("C")
			logger.Debug("Disk usage from cached/fallback: FreeDisk=%s, UsagePercent=%d", freeDisk, usagePercent)
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
		AlwaysOn:    alwaysOnMetrics,
	}

	// Debug logging before normalization
	logger.Debug("BEFORE normalization - TotalMemory: %v (type: %T), TotalvCpu: %v (type: %T), FdPercent: %v (type: %T)",
		info.TotalMemory, info.TotalMemory, info.TotalvCpu, info.TotalvCpu, info.FdPercent, info.FdPercent)

	// Normalize data to ensure consistent formatting and avoid false change detection
	info.NormalizeData()

	// Debug logging after normalization
	logger.Debug("AFTER normalization - TotalMemory: %v (type: %T), TotalvCpu: %v (type: %T), FdPercent: %v (type: %T)",
		info.TotalMemory, info.TotalMemory, info.TotalvCpu, info.TotalvCpu, info.FdPercent, info.FdPercent)

	logger.Info("MSSQL bilgileri başarıyla toplandı. IP=%s, Status=%s, HAEnabled=%v",
		info.IP, info.Status, info.IsHAEnabled)

	return info
}

// SerializeAlwaysOnMetrics converts AlwaysOnMetrics to a JSON string for logging and diagnostics
func (c *MSSQLCollector) SerializeAlwaysOnMetrics(metrics *AlwaysOnMetrics) (string, error) {
	if metrics == nil {
		return "{}", nil
	}

	data, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error serializing AlwaysOn metrics: %w", err)
	}

	return string(data), nil
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

	// Use direct wmic command instead of PowerShell
	// First get the total size and free space of the drive
	drivePath := fmt.Sprintf("%s:", driveLetter)
	cmd := exec.Command("wmic", "logicaldisk", "where", fmt.Sprintf("DeviceID='%s'", drivePath), "get", "Size,FreeSpace", "/value")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err != nil || ctx.Err() == context.DeadlineExceeded {
		log.Printf("Failed to get disk usage with wmic: %v", err)
		return "N/A", 0
	}

	// Parse the output
	outStr := string(out)
	sizeMatches := regexp.MustCompile(`Size=(\d+)`).FindStringSubmatch(outStr)
	freeMatches := regexp.MustCompile(`FreeSpace=(\d+)`).FindStringSubmatch(outStr)

	if len(sizeMatches) < 2 || len(freeMatches) < 2 {
		log.Printf("Could not parse wmic output: %s", outStr)
		return "N/A", 0
	}

	size, err := strconv.ParseUint(sizeMatches[1], 10, 64)
	if err != nil {
		log.Printf("Failed to parse size: %v", err)
		return "N/A", 0
	}

	free, err := strconv.ParseUint(freeMatches[1], 10, 64)
	if err != nil {
		log.Printf("Failed to parse free space: %v", err)
		return "N/A", 0
	}

	// Calculate usage percentage
	usagePercent := 0
	if size > 0 {
		usedBytes := size - free
		usagePercent = int((float64(usedBytes) / float64(size)) * 100)
	}

	// Format free space
	freeDisk := c.formatBytes(free)

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

	// On Windows, use direct commands without PowerShell
	if runtime.GOOS == "windows" {
		// Try with ipconfig command directly
		cmd := exec.Command("ipconfig", "/all")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		out, err := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...).Output()
		if err == nil {
			// Parse the output to extract IP addresses
			output := string(out)

			// Extract IPv4 addresses
			ipv4Regex := regexp.MustCompile(`IPv4 Address[^:]*:\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`)
			matches := ipv4Regex.FindAllStringSubmatch(output, -1)

			var validIPs []string
			for _, match := range matches {
				if len(match) >= 2 {
					ip := match[1]
					if ip != "127.0.0.1" && !strings.HasPrefix(ip, "169.254.") {
						validIPs = append(validIPs, ip)
					}
				}
			}

			// Sort IPs by preference (192.168.x.x first, then 10.x.x.x)
			sort.Slice(validIPs, func(i, j int) bool {
				if strings.HasPrefix(validIPs[i], "192.168.") &&
					!strings.HasPrefix(validIPs[j], "192.168.") {
					return true
				}
				if strings.HasPrefix(validIPs[i], "10.") &&
					!strings.HasPrefix(validIPs[j], "10.") &&
					!strings.HasPrefix(validIPs[j], "192.168.") {
					return true
				}
				return false
			})

			if len(validIPs) > 0 {
				log.Printf("Found IP using ipconfig: %s", validIPs[0])
				c.cacheIP(validIPs[0])
				return validIPs[0]
			}
		}
	}

	// Fallback to checking database configuration
	if c.cfg.MSSQL.Host != "" && c.cfg.MSSQL.Host != "localhost" && c.cfg.MSSQL.Host != "127.0.0.1" {
		// Check if host is an IP address
		if net.ParseIP(c.cfg.MSSQL.Host) != nil {
			log.Printf("Using configured IP address: %s", c.cfg.MSSQL.Host)
			c.cacheIP(c.cfg.MSSQL.Host)
			return c.cfg.MSSQL.Host
		}
	}

	// Use standard library as fallback
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
				// Filter out APIPA addresses
				if !strings.HasPrefix(ipStr, "169.254.") {
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
		log.Printf("Using IP from Go standard library: %s", validIPs[0])
		c.cacheIP(validIPs[0])
		return validIPs[0]
	}

	return "Unknown"
}

// Helper function to cache IP address for 24 hours
func (c *MSSQLCollector) cacheIP(ip string) {
	cacheDir := c.getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("IP önbellek dizini oluşturulamadı: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_ip_cache.txt")
	if err := os.WriteFile(cacheFile, []byte(ip), 0644); err != nil {
		log.Printf("IP önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached IP address
func (c *MSSQLCollector) getCachedIP() string {
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_ip_cache.txt")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return ""
	}

	// Check if cache is still valid (24 hours)
	fileInfo, err := os.Stat(cacheFile)
	if err != nil {
		return ""
	}

	if time.Since(fileInfo.ModTime()) > 24*time.Hour {
		// Cache too old
		return ""
	}

	return strings.TrimSpace(string(data))
}

// Helper function to get cache directory
func (c *MSSQLCollector) getCacheDir() string {
	if runtime.GOOS == "windows" {
		dir := filepath.Join("C:\\Program Files\\ClusterEyeAgent", "cache")
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Windows önbellek dizini oluşturulamadı: %v, geçici dizin kullanılacak", err)
			return os.TempDir()
		}
		return dir
	} else {
		dir := "/var/lib/clustereye-agent/cache"
		if homeDir, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(homeDir, ".clustereye", "cache")
		}

		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Unix önbellek dizini oluşturulamadı: %v, geçici dizin kullanılacak", err)
			return os.TempDir()
		}
		return dir
	}
}

// Define new structs for WMI queries
// Win32_NetworkAdapter represents a network adapter in Windows
type Win32_NetworkAdapter struct {
	NetConnectionID string
	Name            string
	Description     string
	MACAddress      string
	PhysicalAdapter bool
	NetEnabled      bool
	AdapterType     string
}

// Win32_NetworkAdapterConfiguration represents network adapter configuration
type Win32_NetworkAdapterConfiguration struct {
	IPAddress        []string
	IPEnabled        bool
	MACAddress       string
	DefaultIPGateway []string
	DHCPEnabled      bool
}

// Win32_ComputerSystem for system information
type Win32_ComputerSystem struct {
	NumberOfLogicalProcessors uint32
	TotalPhysicalMemory       uint64
}

// Win32_OperatingSystem for system information
type Win32_OperatingSystem struct {
	FreePhysicalMemory     uint64
	TotalVisibleMemorySize uint64
}

// Win32_LogicalDisk for disk information
type Win32_LogicalDisk struct {
	DeviceID  string
	FreeSpace uint64
	Size      uint64
	DriveType uint32
}

// getPhysicalNodeIP attempts to get the physical node's IP address
func (c *MSSQLCollector) getPhysicalNodeIP() string {
	if runtime.GOOS != "windows" {
		return ""
	}

	// Windows'a özel işlemler için fallback implementasyon
	// ipconfig çıktısını direkt olarak parse edelim
	cmd := exec.Command("ipconfig", "/all")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil {
		// Parse output with Go code
		output := string(out)

		// Extract IPv4 addresses from ipconfig output
		ipv4Regex := regexp.MustCompile(`IPv4 Address[^:]*:\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`)
		matches := ipv4Regex.FindAllStringSubmatch(output, -1)

		var validIPs []string
		for _, match := range matches {
			if len(match) >= 2 {
				ip := match[1]
				if ip != "127.0.0.1" && !strings.HasPrefix(ip, "169.254.") {
					validIPs = append(validIPs, ip)
				}
			}
		}

		// Sort IPs by preferred ranges
		sort.Slice(validIPs, func(i, j int) bool {
			if strings.HasPrefix(validIPs[i], "192.168.") &&
				!strings.HasPrefix(validIPs[j], "192.168.") {
				return true
			}
			if strings.HasPrefix(validIPs[i], "10.") &&
				!strings.HasPrefix(validIPs[j], "10.") &&
				!strings.HasPrefix(validIPs[j], "192.168.") {
				return true
			}
			return false
		})

		if len(validIPs) > 0 {
			log.Printf("Found IP using ipconfig parsing: %s", validIPs[0])
			return validIPs[0]
		}
	}

	// Standart kütüphane kullanarak IP'yi bul
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Printf("Failed to get interface addresses: %v", err)
		return ""
	}

	var validIPs []string
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipStr := ipnet.IP.String()
				if ipStr != "127.0.0.1" && !strings.HasPrefix(ipStr, "169.254.") {
					validIPs = append(validIPs, ipStr)
				}
			}
		}
	}

	// Sort IPs by preferred ranges
	sort.Slice(validIPs, func(i, j int) bool {
		if strings.HasPrefix(validIPs[i], "192.168.") &&
			!strings.HasPrefix(validIPs[j], "192.168.") {
			return true
		}
		if strings.HasPrefix(validIPs[i], "10.") &&
			!strings.HasPrefix(validIPs[j], "10.") &&
			!strings.HasPrefix(validIPs[j], "192.168.") {
			return true
		}
		return false
	})

	if len(validIPs) > 0 {
		log.Printf("Found IP using standard library: %s", validIPs[0])
		return validIPs[0]
	}

	return ""
}

// Helper function to cache CPU count
func (c *MSSQLCollector) cacheCPUCount(count int32) {
	cacheDir := c.getCacheDir()
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
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_cpu_count.txt")
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
		// Direct command instead of PowerShell
		cmd := exec.Command("wmic", "computersystem", "get", "TotalPhysicalMemory", "/value")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		out, err := cmdWithTimeout.Output()
		if err == nil {
			// Parse the output
			memStr := strings.TrimSpace(string(out))
			lines := strings.Split(memStr, "\n")
			for _, line := range lines {
				if strings.Contains(line, "TotalPhysicalMemory=") {
					parts := strings.Split(line, "=")
					if len(parts) == 2 {
						memValueStr := strings.TrimSpace(parts[1])
						if memValueStr != "" {
							// Convert to int64 ensuring no scientific notation
							if mem, err := strconv.ParseInt(memValueStr, 10, 64); err == nil && mem > 0 {
								totalMemory = mem
								// Cache the result
								c.cacheMemory(totalMemory)
								return totalMemory
							}
						}
					}
				}
			}
		}
	} else {
		// For Unix-like systems
		cmd := exec.Command("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		out, err := cmdWithTimeout.Output()
		if err == nil && ctx.Err() != context.DeadlineExceeded {
			if memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64); err == nil {
				totalMemory = memTotal * 1024 // Convert KB to bytes
				// Cache the result
				c.cacheMemory(totalMemory)
				return totalMemory
			}
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
	// Ensure memory value is positive and reasonable
	if memBytes <= 0 {
		log.Printf("Invalid memory value for caching: %d, using default 8GB", memBytes)
		memBytes = 8 * 1024 * 1024 * 1024 // 8GB default
	}

	cacheDir := c.getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("Memory önbellek dizini oluşturulamadı: %v", err)
		return
	}

	// Use explicit integer formatting to avoid scientific notation
	memoryStr := fmt.Sprintf("%d", memBytes)

	cacheFile := filepath.Join(cacheDir, "mssql_memory.txt")
	if err := os.WriteFile(cacheFile, []byte(memoryStr), 0644); err != nil {
		log.Printf("Memory önbelleğe kaydedilemedi: %v", err)
	}
}

// Helper function to get cached memory value
func (c *MSSQLCollector) getCachedMemory() int64 {
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_memory.txt")
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
	// Ensure consistent numeric formatting to prevent false change detection
	// API compares values as strings, so we need to avoid scientific notation

	// Normalize TotalMemory to prevent scientific notation
	normalizedMemory := m.TotalMemory
	if normalizedMemory < 0 {
		normalizedMemory = 0
	}

	// Normalize TotalvCpu to reasonable range
	normalizedCpu := m.TotalvCpu
	if normalizedCpu <= 0 {
		normalizedCpu = 1
	}

	// Normalize FdPercent to valid range
	normalizedFdPercent := m.FdPercent
	if normalizedFdPercent < 0 {
		normalizedFdPercent = 0
	} else if normalizedFdPercent > 100 {
		normalizedFdPercent = 100
	}

	proto := &pb.MSSQLInfo{
		ClusterName: m.ClusterName,
		Ip:          m.IP,
		Hostname:    m.Hostname,
		NodeStatus:  m.NodeStatus,
		Version:     m.Version,
		Location:    m.Location,
		Status:      m.Status,
		Instance:    m.Instance,
		FreeDisk:    m.FreeDisk,
		FdPercent:   normalizedFdPercent,
		Port:        m.Port,
		TotalVcpu:   normalizedCpu,
		TotalMemory: normalizedMemory,
		ConfigPath:  m.ConfigPath,
		Database:    m.Database,
		IsHaEnabled: m.IsHAEnabled,
		HaRole:      m.HARole,
		Edition:     m.Edition,
	}

	// Enhanced debug logging with format consistency check
	log.Printf("PROTOBUF VALUES (NORMALIZED) - TotalMemory: %d, TotalVcpu: %d, FdPercent: %d",
		proto.TotalMemory, proto.TotalVcpu, proto.FdPercent)

	// Additional format verification log
	log.Printf("MEMORY FORMAT CHECK - Original: %d, Normalized: %d, Scientific: %e",
		m.TotalMemory, normalizedMemory, float64(normalizedMemory))

	// Convert AlwaysOn metrics to protobuf if available
	if m.AlwaysOn != nil {
		proto.AlwaysOnMetrics = &pb.AlwaysOnMetrics{
			ClusterName:         m.AlwaysOn.ClusterName,
			HealthState:         m.AlwaysOn.HealthState,
			OperationalState:    m.AlwaysOn.OperationalState,
			SynchronizationMode: m.AlwaysOn.SynchronizationMode,
			FailoverMode:        m.AlwaysOn.FailoverMode,
			PrimaryReplica:      m.AlwaysOn.PrimaryReplica,
			LocalRole:           m.AlwaysOn.LocalRole,
			LastFailoverTime:    m.AlwaysOn.LastFailoverTime,
			ReplicationLagMs:    m.AlwaysOn.ReplicationLag,
			LogSendQueueKb:      m.AlwaysOn.LogSendQueue,
			RedoQueueKb:         m.AlwaysOn.RedoQueue,
		}

		// Convert replicas
		for _, replica := range m.AlwaysOn.Replicas {
			proto.AlwaysOnMetrics.Replicas = append(proto.AlwaysOnMetrics.Replicas, &pb.ReplicaMetrics{
				ReplicaName:         replica.ReplicaName,
				Role:                replica.Role,
				ConnectionState:     replica.ConnectionState,
				SynchronizationMode: replica.SynchronizationMode,
				FailoverMode:        replica.FailoverMode,
				AvailabilityMode:    replica.AvailabilityMode,
				JoinState:           replica.JoinState,
				ConnectedState:      replica.ConnectedState,
				SuspendReason:       replica.SuspendReason,
			})
		}

		// Convert databases
		for _, db := range m.AlwaysOn.Databases {
			proto.AlwaysOnMetrics.Databases = append(proto.AlwaysOnMetrics.Databases, &pb.DatabaseReplicaStatus{
				DatabaseName:         db.DatabaseName,
				ReplicaName:          db.ReplicaName,
				SynchronizationState: db.SynchronizationState,
				SuspendReason:        db.SuspendReason,
				LastSentTime:         db.LastSentTime,
				LastReceivedTime:     db.LastReceivedTime,
				LastHardenedTime:     db.LastHardenedTime,
				LastRedoneTime:       db.LastRedoneTime,
				LogSendQueueKb:       db.LogSendQueueSize,
				LogSendRateKbPerSec:  db.LogSendRate,
				RedoQueueKb:          db.RedoQueueSize,
				RedoRateKbPerSec:     db.RedoRate,
				EndOfLogLsn:          db.EndOfLogLSN,
				RecoveryLsn:          db.RecoveryLSN,
				TruncationLsn:        db.TruncationLSN,
				LastCommitLsn:        db.LastCommitLSN,
				LastCommitTime:       db.LastCommitTime,
			})
		}

		// Convert listeners
		for _, listener := range m.AlwaysOn.Listeners {
			proto.AlwaysOnMetrics.Listeners = append(proto.AlwaysOnMetrics.Listeners, &pb.ListenerInfo{
				ListenerName:  listener.ListenerName,
				IpAddresses:   listener.IPAddresses,
				Port:          int32(listener.Port),
				SubnetMask:    listener.SubnetMask,
				ListenerState: listener.ListenerState,
				DnsName:       listener.DNSName,
			})
		}
	}

	return proto
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
			// Get SQL Server installation directory using WMIC instead of PowerShell
			instance := c.cfg.MSSQL.Instance
			regPath := "MSSQL"
			if instance != "" {
				regPath = "MSSQL$" + instance
			}

			// Try to find SQL Server installation directory using registry entries
			// Using 'reg query' command which is more lightweight than PowerShell
			cmd := exec.Command("reg", "query", "HKLM\\SOFTWARE\\Microsoft\\Microsoft SQL Server", "/s", "/f", regPath)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			out, err := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...).Output()
			if err == nil {
				// Parse the output to find the SQL path
				output := string(out)
				lines := strings.Split(output, "\n")
				for i, line := range lines {
					if strings.Contains(line, "SQLPath") && i < len(lines)-1 {
						// Extract the path value
						parts := strings.Split(line, "REG_SZ")
						if len(parts) > 1 {
							path := strings.TrimSpace(parts[1])
							if path != "" {
								// Typical log directory based on SQL Server installation
								logDir = filepath.Join(path, "Log")
								break
							}
						}
					}
				}
			}

			// If reg query failed, try with WMIC as fallback
			if logDir == "" {
				cmd = exec.Command("wmic", "service", "where", "name like '%SQLServer%'", "get", "PathName", "/value")
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				out, err = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...).Output()
				if err == nil {
					output := string(out)
					// Extract the path from PathName
					if pathMatch := regexp.MustCompile(`PathName=(.+)`).FindStringSubmatch(output); len(pathMatch) > 1 {
						sqlPath := pathMatch[1]
						// Extract the directory part
						sqlDir := filepath.Dir(sqlPath)
						if sqlDir != "" {
							// Go up one level to the main SQL Server directory
							baseDir := filepath.Dir(sqlDir)
							logDir = filepath.Join(baseDir, "Log")
						}
					}
				}
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

// getTotalvCpu returns the total number of vCPUs
func (c *MSSQLCollector) getTotalvCpu() int32 {
	// Check for cached CPU count
	if cachedCount := c.getCachedCPUCount(); cachedCount > 0 {
		log.Printf("Using cached CPU count: %d", cachedCount)
		return cachedCount
	}

	var result int32 = 0

	if runtime.GOOS == "windows" {
		// First try: Get total number of logical processors from computersystem
		cmd := exec.Command("wmic", "computersystem", "get", "NumberOfLogicalProcessors", "/value")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		out, err := cmdWithTimeout.Output()
		if err == nil {
			// Parse the output
			outStr := strings.TrimSpace(string(out))
			if matches := regexp.MustCompile(`NumberOfLogicalProcessors=(\d+)`).FindStringSubmatch(outStr); len(matches) > 1 {
				if count, err := strconv.ParseInt(matches[1], 10, 32); err == nil && count > 0 {
					result = int32(count)
					// Cache the result
					c.cacheCPUCount(result)
					return result
				}
			}
		}

		// Fallback: Sum up all CPU cores
		cmd = exec.Command("wmic", "cpu", "get", "NumberOfLogicalProcessors", "/value")
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmdWithTimeout = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		out, err = cmdWithTimeout.Output()
		if err == nil {
			// Parse all CPU entries and sum them up
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			totalCores := int32(0)
			cpuCount := 0

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if strings.Contains(line, "NumberOfLogicalProcessors=") {
					parts := strings.Split(line, "=")
					if len(parts) == 2 {
						cpuCountStr := strings.TrimSpace(parts[1])
						if cpuCountStr != "" {
							if count, err := strconv.ParseInt(cpuCountStr, 10, 32); err == nil && count > 0 {
								totalCores += int32(count)
								cpuCount++
							}
						}
					}
				}
			}

			if totalCores > 0 {
				result = totalCores
				// Cache the result
				c.cacheCPUCount(result)
				return result
			}
		}
	} else {
		// For Unix-like systems
		cmd := exec.Command("sh", "-c", "nproc")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		out, err := cmdWithTimeout.Output()
		if err == nil && ctx.Err() != context.DeadlineExceeded {
			if count, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32); err == nil {
				result = int32(count)
				// Cache the result
				c.cacheCPUCount(result)
				return result
			}
		}
	}

	// Fallback to runtime.NumCPU()
	if result <= 0 {
		result = int32(runtime.NumCPU())
		// Cache the result
		c.cacheCPUCount(result)
	}

	return result
}

// AlwaysOnMetrics represents detailed AlwaysOn metrics
type AlwaysOnMetrics struct {
	ClusterName         string                  `json:"cluster_name"`
	HealthState         string                  `json:"health_state"`
	OperationalState    string                  `json:"operational_state"`
	SynchronizationMode string                  `json:"synchronization_mode"`
	FailoverMode        string                  `json:"failover_mode"`
	PrimaryReplica      string                  `json:"primary_replica"`
	LocalRole           string                  `json:"local_role"`
	Replicas            []ReplicaMetrics        `json:"replicas"`
	Databases           []DatabaseReplicaStatus `json:"databases"`
	Listeners           []ListenerInfo          `json:"listeners"`
	LastFailoverTime    string                  `json:"last_failover_time"`
	ReplicationLag      int64                   `json:"replication_lag_ms"`
	LogSendQueue        int64                   `json:"log_send_queue_kb"`
	RedoQueue           int64                   `json:"redo_queue_kb"`
}

// ReplicaMetrics represents metrics for a single replica in the AG
type ReplicaMetrics struct {
	ReplicaName         string `json:"replica_name"`
	Role                string `json:"role"`
	ConnectionState     string `json:"connection_state"`
	SynchronizationMode string `json:"synchronization_mode"`
	FailoverMode        string `json:"failover_mode"`
	AvailabilityMode    string `json:"availability_mode"`
	JoinState           string `json:"join_state"`
	ConnectedState      bool   `json:"connected_state"`
	SuspendReason       string `json:"suspend_reason,omitempty"`
}

// DatabaseReplicaStatus represents database-level replication status
type DatabaseReplicaStatus struct {
	DatabaseName         string `json:"database_name"`
	ReplicaName          string `json:"replica_name"`
	SynchronizationState string `json:"synchronization_state"`
	SuspendReason        string `json:"suspend_reason,omitempty"`
	LastSentTime         string `json:"last_sent_time,omitempty"`
	LastReceivedTime     string `json:"last_received_time,omitempty"`
	LastHardenedTime     string `json:"last_hardened_time,omitempty"`
	LastRedoneTime       string `json:"last_redone_time,omitempty"`
	LogSendQueueSize     int64  `json:"log_send_queue_kb"`
	LogSendRate          int64  `json:"log_send_rate_kb_per_sec"`
	RedoQueueSize        int64  `json:"redo_queue_kb"`
	RedoRate             int64  `json:"redo_rate_kb_per_sec"`
	FileStreamSendRate   int64  `json:"filestream_send_rate_kb_per_sec"`
	EndOfLogLSN          string `json:"end_of_log_lsn,omitempty"`
	RecoveryLSN          string `json:"recovery_lsn,omitempty"`
	TruncationLSN        string `json:"truncation_lsn,omitempty"`
	LastCommitLSN        string `json:"last_commit_lsn,omitempty"`
	LastCommitTime       string `json:"last_commit_time,omitempty"`
}

// ListenerInfo represents availability group listener information
type ListenerInfo struct {
	ListenerName  string   `json:"listener_name"`
	IPAddresses   []string `json:"ip_addresses"`
	Port          int      `json:"port"`
	SubnetMask    string   `json:"subnet_mask,omitempty"`
	ListenerState string   `json:"listener_state"`
	DNSName       string   `json:"dns_name,omitempty"`
}

// GetAlwaysOnMetrics collects comprehensive AlwaysOn metrics and health information
// This function is optimized to reduce database load and prevent CPU spikes
func (c *MSSQLCollector) GetAlwaysOnMetrics() (*AlwaysOnMetrics, error) {
	// Check if we have recent cached AlwaysOn metrics (cache for 5 minutes)
	if cachedMetrics := c.getCachedAlwaysOnMetrics(); cachedMetrics != nil {
		log.Printf("Using cached AlwaysOn metrics to avoid database overload")
		return cachedMetrics, nil
	}

	// Return nil if not connected to SQL Server
	db, err := c.GetClient()
	if err != nil {
		return nil, fmt.Errorf("could not connect to SQL Server to collect AlwaysOn metrics: %w", err)
	}
	defer db.Close()

	// Set connection timeout to prevent hanging
	db.SetConnMaxLifetime(10 * time.Second)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// First check if AlwaysOn is enabled
	var isHAEnabled int
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.QueryRowContext(ctx, `
		SELECT CASE 
			WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 1
			ELSE 0
		END AS IsHAEnabled
	`).Scan(&isHAEnabled)

	if err != nil || isHAEnabled == 0 {
		return nil, fmt.Errorf("AlwaysOn is not enabled on this instance")
	}

	metrics := &AlwaysOnMetrics{}

	// Get basic AG information with a single optimized query
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.QueryRowContext(ctx, `
		SELECT TOP 1 
			ISNULL(ag.name, '') AS cluster_name,
			ISNULL(ags.primary_replica, '') AS primary_replica,
			ISNULL(ags.synchronization_health_desc, 'UNKNOWN') AS health_state,
			ISNULL(ars.role_desc, 'UNKNOWN') AS local_role
		FROM sys.availability_groups ag
		JOIN sys.dm_hadr_availability_group_states ags ON ag.group_id = ags.group_id
		JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
		JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id
		WHERE ar.replica_server_name = @@SERVERNAME
	`).Scan(
		&metrics.ClusterName,
		&metrics.PrimaryReplica,
		&metrics.HealthState,
		&metrics.LocalRole,
	)

	if err != nil {
		log.Printf("Error getting basic AG information: %v", err)
		// Try simplified fallback query
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = db.QueryRowContext(ctx, `
			SELECT TOP 1 
				ISNULL(ag.name, 'Unknown') AS cluster_name,
				'Unknown' AS primary_replica,
				'Unknown' AS health_state,
				'Unknown' AS operational_state,
				'Unknown' AS local_role
			FROM sys.availability_groups ag
		`).Scan(
			&metrics.ClusterName,
			&metrics.PrimaryReplica,
			&metrics.HealthState,
			&metrics.OperationalState,
			&metrics.LocalRole,
		)

		if err != nil {
			log.Printf("Fallback AG query also failed: %v", err)
			metrics.ClusterName = "Unknown"
			metrics.HealthState = "Unknown"
			metrics.OperationalState = "Unknown"
			metrics.LocalRole = "Unknown"
		}
	}

	// Get replica information with limited results and timeout
	ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
		SELECT TOP 10
			ISNULL(ar.replica_server_name, '') AS replica_name,
			ISNULL(ars.role_desc, 'UNKNOWN') AS role,
			ISNULL(ars.connected_state_desc, 'UNKNOWN') AS connection_state,
			ISNULL(ar.availability_mode_desc, 'UNKNOWN') AS synchronization_mode,
			ISNULL(ar.failover_mode_desc, 'UNKNOWN') AS failover_mode,
			ISNULL(ar.availability_mode_desc, 'UNKNOWN') AS availability_mode,
			CAST(ISNULL(ars.connected_state, 0) AS BIT) AS connected_state
		FROM sys.availability_replicas ar
		LEFT JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id
		WHERE ar.group_id IN (
			SELECT TOP 1 group_id FROM sys.availability_replicas WHERE replica_server_name = @@SERVERNAME
		)
	`)

	if err != nil {
		log.Printf("Error getting replica information (non-critical): %v", err)
	} else {
		defer rows.Close()
		replicaCount := 0
		for rows.Next() && replicaCount < 10 { // Limit to 10 replicas max
			var replica ReplicaMetrics
			var operationalState string // temporary variable for operational_state

			err := rows.Scan(
				&replica.ReplicaName,
				&replica.Role,
				&replica.ConnectionState,
				&replica.SynchronizationMode,
				&replica.FailoverMode,
				&replica.AvailabilityMode,
				&replica.ConnectedState,
			)
			if err != nil {
				log.Printf("Error scanning replica row: %v", err)
				continue
			}

			// Map operational_state to join_state or use a default value
			replica.JoinState = operationalState // or set to "UNKNOWN" if needed

			metrics.Replicas = append(metrics.Replicas, replica)
			replicaCount++
		}
	}

	// Extract synchronization mode and failover mode for current server from replica data
	hostname, _ := os.Hostname()
	for _, replica := range metrics.Replicas {
		if replica.ReplicaName == hostname {
			metrics.SynchronizationMode = replica.SynchronizationMode
			metrics.FailoverMode = replica.FailoverMode
			log.Printf("Current server modes - Synchronization: %s, Failover: %s",
				metrics.SynchronizationMode, metrics.FailoverMode)
			break
		}
	}

	// Get database replica status with limited results and enhanced metrics
	ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	rows, err = db.QueryContext(ctx, `
		SELECT TOP 20
			ISNULL(DB_NAME(drs.database_id), '') AS database_name,
			ISNULL(ar.replica_server_name, '') AS replica_name,
			ISNULL(drs.synchronization_state_desc, 'UNKNOWN') AS synchronization_state,
			ISNULL(drs.suspend_reason_desc, '') AS suspend_reason,
			ISNULL(drs.log_send_queue_size, 0) AS log_send_queue_size,
			ISNULL(drs.redo_queue_size, 0) AS redo_queue_size,
			ISNULL(drs.log_send_rate, 0) AS log_send_rate,
			ISNULL(drs.redo_rate, 0) AS redo_rate,
			ISNULL(drs.filestream_send_rate, 0) AS filestream_send_rate,
			ISNULL(CONVERT(NVARCHAR(30), drs.last_sent_time, 121), '') AS last_sent_time,
			ISNULL(CONVERT(NVARCHAR(30), drs.last_received_time, 121), '') AS last_received_time,
			ISNULL(CONVERT(NVARCHAR(30), drs.last_hardened_time, 121), '') AS last_hardened_time,
			ISNULL(CONVERT(NVARCHAR(30), drs.last_redone_time, 121), '') AS last_redone_time,
			ISNULL(CONVERT(NVARCHAR(30), drs.last_commit_time, 121), '') AS last_commit_time,
			ISNULL(CONVERT(NVARCHAR(30), drs.last_commit_lsn), '') AS last_commit_lsn,
			ISNULL(CONVERT(NVARCHAR(30), drs.end_of_log_lsn), '') AS end_of_log_lsn,
			ISNULL(CONVERT(NVARCHAR(30), drs.recovery_lsn), '') AS recovery_lsn,
			ISNULL(CONVERT(NVARCHAR(30), drs.truncation_lsn), '') AS truncation_lsn
		FROM sys.dm_hadr_database_replica_states drs
		LEFT JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id
		WHERE drs.group_id IN (
			SELECT TOP 1 group_id FROM sys.availability_replicas WHERE replica_server_name = @@SERVERNAME
		)
		AND drs.database_id IS NOT NULL
	`)

	if err != nil {
		log.Printf("Error getting database replica status (non-critical): %v", err)
	} else {
		defer rows.Close()
		dbCount := 0
		for rows.Next() && dbCount < 20 { // Limit to 20 databases max
			var dbStatus DatabaseReplicaStatus
			err := rows.Scan(
				&dbStatus.DatabaseName,
				&dbStatus.ReplicaName,
				&dbStatus.SynchronizationState,
				&dbStatus.SuspendReason,
				&dbStatus.LogSendQueueSize,
				&dbStatus.RedoQueueSize,
				&dbStatus.LogSendRate,
				&dbStatus.RedoRate,
				&dbStatus.FileStreamSendRate,
				&dbStatus.LastSentTime,
				&dbStatus.LastReceivedTime,
				&dbStatus.LastHardenedTime,
				&dbStatus.LastRedoneTime,
				&dbStatus.LastCommitTime,
				&dbStatus.LastCommitLSN,
				&dbStatus.EndOfLogLSN,
				&dbStatus.RecoveryLSN,
				&dbStatus.TruncationLSN,
			)
			if err != nil {
				log.Printf("Error scanning database replica status row: %v", err)
				continue
			}

			metrics.Databases = append(metrics.Databases, dbStatus)

			// If this is our current server, get basic replication lag metrics
			hostname, _ := os.Hostname()
			if dbStatus.ReplicaName == hostname && metrics.LocalRole == "SECONDARY" {
				metrics.LogSendQueue = dbStatus.LogSendQueueSize
				metrics.RedoQueue = dbStatus.RedoQueueSize

				// Calculate replication lag in milliseconds using timestamps
				if dbStatus.LastCommitTime != "" && dbStatus.LastReceivedTime != "" {
					if lastCommitTime, err := time.Parse("2006-01-02 15:04:05.000", dbStatus.LastCommitTime); err == nil {
						if lastReceivedTime, err := time.Parse("2006-01-02 15:04:05.000", dbStatus.LastReceivedTime); err == nil {
							lagMs := lastCommitTime.Sub(lastReceivedTime).Milliseconds()
							if lagMs > 0 && lagMs < 86400000 { // Less than 24 hours (reasonable lag)
								metrics.ReplicationLag = lagMs
								log.Printf("Calculated replication lag: %d ms for database %s", lagMs, dbStatus.DatabaseName)
							}
						}
					}
				}

				// Alternative lag calculation using last_hardened_time if available
				if metrics.ReplicationLag == 0 && dbStatus.LastCommitTime != "" && dbStatus.LastHardenedTime != "" {
					if lastCommitTime, err := time.Parse("2006-01-02 15:04:05.000", dbStatus.LastCommitTime); err == nil {
						if lastHardenedTime, err := time.Parse("2006-01-02 15:04:05.000", dbStatus.LastHardenedTime); err == nil {
							lagMs := lastCommitTime.Sub(lastHardenedTime).Milliseconds()
							if lagMs > 0 && lagMs < 86400000 { // Less than 24 hours (reasonable lag)
								metrics.ReplicationLag = lagMs
								log.Printf("Calculated replication lag (hardened): %d ms for database %s", lagMs, dbStatus.DatabaseName)
							}
						}
					}
				}
			}
			dbCount++
		}
	}

	// Collect listener information for complete AlwaysOn metrics
	ctx, cancel = context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	rows, err = db.QueryContext(ctx, `
		SELECT 
			ISNULL(agl.dns_name, '') AS listener_name,
			ISNULL(agl.dns_name, '') AS dns_name,
			ISNULL(agl.port, 1433) AS port,
			CASE WHEN EXISTS (
				SELECT 1 FROM sys.availability_group_listener_ip_addresses 
				WHERE listener_id = agl.listener_id
			) THEN 'ONLINE' ELSE 'OFFLINE' END AS listener_state
		FROM sys.availability_group_listeners agl
		WHERE agl.group_id IN (
			SELECT TOP 1 group_id FROM sys.availability_replicas WHERE replica_server_name = @@SERVERNAME
		)
	`)

	if err != nil {
		log.Printf("Error getting listener information (non-critical): %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var listener ListenerInfo
			err := rows.Scan(
				&listener.ListenerName,
				&listener.DNSName,
				&listener.Port,
				&listener.ListenerState,
			)
			if err != nil {
				log.Printf("Error scanning listener row: %v", err)
				continue
			}

			// Get IP addresses for this listener
			ipRows, err := db.QueryContext(ctx, `
				SELECT ip_address 
				FROM sys.availability_group_listener_ip_addresses 
				WHERE listener_id = (
					SELECT listener_id FROM sys.availability_group_listeners 
					WHERE dns_name = ? AND group_id IN (
						SELECT TOP 1 group_id FROM sys.availability_replicas WHERE replica_server_name = @@SERVERNAME
					)
				)
			`, listener.ListenerName)

			if err == nil {
				defer ipRows.Close()
				var ipAddresses []string
				for ipRows.Next() {
					var ip string
					if err := ipRows.Scan(&ip); err == nil {
						ipAddresses = append(ipAddresses, ip)
					}
				}
				listener.IPAddresses = ipAddresses
			}

			metrics.Listeners = append(metrics.Listeners, listener)
		}
	}

	// Set defaults only if not already set from replica data
	if metrics.SynchronizationMode == "" {
		metrics.SynchronizationMode = "Unknown"
	}
	if metrics.FailoverMode == "" {
		metrics.FailoverMode = "Unknown"
	}

	// Set last failover time (would require additional complex queries to determine)
	metrics.LastFailoverTime = "Unknown"

	// ReplicationLag should have been calculated above from database timestamps
	if metrics.ReplicationLag == 0 {
		log.Printf("Replication lag could not be calculated from database timestamps")
	}

	// Cache the results for 5 minutes to avoid frequent database hits
	c.cacheAlwaysOnMetrics(metrics)

	log.Printf("AlwaysOn metrics collection completed successfully (optimized)")
	return metrics, nil
}

// ExportAlwaysOnMetrics exports the detailed AlwaysOn metrics as a JSON object that can be
// consumed by monitoring systems. This provides a way to access the metrics since they
// can't be included in the protobuf message yet.
func (c *MSSQLCollector) ExportAlwaysOnMetrics() (map[string]interface{}, error) {
	metrics, err := c.GetAlwaysOnMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to collect AlwaysOn metrics: %w", err)
	}

	if metrics == nil {
		return nil, fmt.Errorf("no AlwaysOn configuration found on this server")
	}

	// Convert structured metrics to a map for easier extension and compatibility
	result := map[string]interface{}{
		"cluster_name":          metrics.ClusterName,
		"health_state":          metrics.HealthState,
		"operational_state":     metrics.OperationalState,
		"synchronization_mode":  metrics.SynchronizationMode,
		"failover_mode":         metrics.FailoverMode,
		"primary_replica":       metrics.PrimaryReplica,
		"local_role":            metrics.LocalRole,
		"last_failover_time":    metrics.LastFailoverTime,
		"replication_lag_ms":    metrics.ReplicationLag,
		"log_send_queue_kb":     metrics.LogSendQueue,
		"redo_queue_kb":         metrics.RedoQueue,
		"replicas":              metrics.Replicas,
		"databases":             metrics.Databases,
		"listeners":             metrics.Listeners,
		"timestamp":             time.Now().Unix(),
		"collection_successful": true,
	}

	return result, nil
}

// GetAlwaysOnStatus returns a simplified status of the AlwaysOn configuration
// for use in monitoring dashboards and alerts
func (c *MSSQLCollector) GetAlwaysOnStatus() map[string]interface{} {
	metrics, err := c.GetAlwaysOnMetrics()
	if err != nil {
		return map[string]interface{}{
			"is_enabled": false,
			"error":      err.Error(),
			"status":     "ERROR",
			"timestamp":  time.Now().Unix(),
		}
	}

	if metrics == nil {
		return map[string]interface{}{
			"is_enabled": false,
			"status":     "DISABLED",
			"timestamp":  time.Now().Unix(),
		}
	}

	// Determine overall status based on health state
	status := "HEALTHY"
	if metrics.HealthState != "HEALTHY" {
		status = "UNHEALTHY"
	}

	// Count number of connected replicas
	var connectedReplicas int
	var totalReplicas = len(metrics.Replicas)

	for _, replica := range metrics.Replicas {
		if replica.ConnectedState {
			connectedReplicas++
		}
	}

	// Return simplified status for monitoring
	return map[string]interface{}{
		"is_enabled":         true,
		"status":             status,
		"health_state":       metrics.HealthState,
		"operational_state":  metrics.OperationalState,
		"local_role":         metrics.LocalRole,
		"primary_replica":    metrics.PrimaryReplica,
		"replication_lag_ms": metrics.ReplicationLag,
		"connected_replicas": connectedReplicas,
		"total_replicas":     totalReplicas,
		"timestamp":          time.Now().Unix(),
	}
}

// NormalizeData ensures all data types and formats are consistent to avoid false change detection
func (m *MSSQLInfo) NormalizeData() {
	// Ensure consistent memory format (always as int64, no scientific notation)
	if m.TotalMemory < 0 {
		m.TotalMemory = 0
	}

	// Ensure CPU count is reasonable and consistent
	if m.TotalvCpu <= 0 {
		m.TotalvCpu = 1 // Default to 1 if invalid
	}

	// Ensure FD percentage is within valid range
	if m.FdPercent < 0 {
		m.FdPercent = 0
	} else if m.FdPercent > 100 {
		m.FdPercent = 100
	}

	// Trim and normalize string fields to avoid whitespace comparison issues
	m.ClusterName = strings.TrimSpace(m.ClusterName)
	m.IP = strings.TrimSpace(m.IP)
	m.Hostname = strings.TrimSpace(m.Hostname)
	m.NodeStatus = strings.TrimSpace(m.NodeStatus)
	m.Version = strings.TrimSpace(m.Version)
	m.Location = strings.TrimSpace(m.Location)
	m.Status = strings.TrimSpace(m.Status)
	m.Instance = strings.TrimSpace(m.Instance)
	m.FreeDisk = strings.TrimSpace(m.FreeDisk)
	m.Port = strings.TrimSpace(m.Port)
	m.ConfigPath = strings.TrimSpace(m.ConfigPath)
	m.Database = strings.TrimSpace(m.Database)
	m.HARole = strings.TrimSpace(m.HARole)
	m.Edition = strings.TrimSpace(m.Edition)

	// Normalize AlwaysOn metrics if present
	if m.AlwaysOn != nil {
		m.normalizeAlwaysOnMetrics()
	}
}

// normalizeAlwaysOnMetrics normalizes AlwaysOn metrics data
func (m *MSSQLInfo) normalizeAlwaysOnMetrics() {
	if m.AlwaysOn == nil {
		return
	}

	// Normalize string fields
	m.AlwaysOn.ClusterName = strings.TrimSpace(m.AlwaysOn.ClusterName)
	m.AlwaysOn.HealthState = strings.TrimSpace(m.AlwaysOn.HealthState)
	m.AlwaysOn.OperationalState = strings.TrimSpace(m.AlwaysOn.OperationalState)
	m.AlwaysOn.SynchronizationMode = strings.TrimSpace(m.AlwaysOn.SynchronizationMode)
	m.AlwaysOn.FailoverMode = strings.TrimSpace(m.AlwaysOn.FailoverMode)
	m.AlwaysOn.PrimaryReplica = strings.TrimSpace(m.AlwaysOn.PrimaryReplica)
	m.AlwaysOn.LocalRole = strings.TrimSpace(m.AlwaysOn.LocalRole)
	m.AlwaysOn.LastFailoverTime = strings.TrimSpace(m.AlwaysOn.LastFailoverTime)

	// Ensure numeric values are reasonable
	if m.AlwaysOn.ReplicationLag < 0 {
		m.AlwaysOn.ReplicationLag = 0
	}
	if m.AlwaysOn.LogSendQueue < 0 {
		m.AlwaysOn.LogSendQueue = 0
	}
	if m.AlwaysOn.RedoQueue < 0 {
		m.AlwaysOn.RedoQueue = 0
	}

	// Normalize replica metrics
	for i := range m.AlwaysOn.Replicas {
		replica := &m.AlwaysOn.Replicas[i]
		replica.ReplicaName = strings.TrimSpace(replica.ReplicaName)
		replica.Role = strings.TrimSpace(replica.Role)
		replica.ConnectionState = strings.TrimSpace(replica.ConnectionState)
		replica.SynchronizationMode = strings.TrimSpace(replica.SynchronizationMode)
		replica.FailoverMode = strings.TrimSpace(replica.FailoverMode)
		replica.AvailabilityMode = strings.TrimSpace(replica.AvailabilityMode)
		replica.JoinState = strings.TrimSpace(replica.JoinState)
		replica.SuspendReason = strings.TrimSpace(replica.SuspendReason)
	}

	// Normalize database replica status
	for i := range m.AlwaysOn.Databases {
		db := &m.AlwaysOn.Databases[i]
		db.DatabaseName = strings.TrimSpace(db.DatabaseName)
		db.ReplicaName = strings.TrimSpace(db.ReplicaName)
		db.SynchronizationState = strings.TrimSpace(db.SynchronizationState)
		db.SuspendReason = strings.TrimSpace(db.SuspendReason)
		db.LastSentTime = strings.TrimSpace(db.LastSentTime)
		db.LastReceivedTime = strings.TrimSpace(db.LastReceivedTime)
		db.LastHardenedTime = strings.TrimSpace(db.LastHardenedTime)
		db.LastRedoneTime = strings.TrimSpace(db.LastRedoneTime)
		db.EndOfLogLSN = strings.TrimSpace(db.EndOfLogLSN)
		db.RecoveryLSN = strings.TrimSpace(db.RecoveryLSN)
		db.TruncationLSN = strings.TrimSpace(db.TruncationLSN)
		db.LastCommitLSN = strings.TrimSpace(db.LastCommitLSN)
		db.LastCommitTime = strings.TrimSpace(db.LastCommitTime)

		// Ensure numeric values are reasonable
		if db.LogSendQueueSize < 0 {
			db.LogSendQueueSize = 0
		}
		if db.LogSendRate < 0 {
			db.LogSendRate = 0
		}
		if db.RedoQueueSize < 0 {
			db.RedoQueueSize = 0
		}
		if db.RedoRate < 0 {
			db.RedoRate = 0
		}
		if db.FileStreamSendRate < 0 {
			db.FileStreamSendRate = 0
		}
	}

	// Normalize listener info
	for i := range m.AlwaysOn.Listeners {
		listener := &m.AlwaysOn.Listeners[i]
		listener.ListenerName = strings.TrimSpace(listener.ListenerName)
		listener.SubnetMask = strings.TrimSpace(listener.SubnetMask)
		listener.ListenerState = strings.TrimSpace(listener.ListenerState)
		listener.DNSName = strings.TrimSpace(listener.DNSName)

		// Normalize IP addresses
		for j := range listener.IPAddresses {
			listener.IPAddresses[j] = strings.TrimSpace(listener.IPAddresses[j])
		}

		// Ensure port is reasonable
		if listener.Port <= 0 || listener.Port > 65535 {
			listener.Port = 1433 // Default SQL Server port
		}
	}
}

// Helper function to cache AlwaysOn metrics for 5 minutes
func (c *MSSQLCollector) cacheAlwaysOnMetrics(metrics *AlwaysOnMetrics) {
	cacheDir := c.getCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("AlwaysOn metrics önbellek dizini oluşturulamadı: %v", err)
		return
	}

	// Add timestamp to metrics data
	cacheData := struct {
		Metrics   *AlwaysOnMetrics `json:"metrics"`
		Timestamp int64            `json:"timestamp"`
	}{
		Metrics:   metrics,
		Timestamp: time.Now().Unix(),
	}

	jsonData, err := json.Marshal(cacheData)
	if err != nil {
		log.Printf("AlwaysOn metrics JSON encode hatası: %v", err)
		return
	}

	cacheFile := filepath.Join(cacheDir, "mssql_alwayson_metrics.json")
	if err := os.WriteFile(cacheFile, jsonData, 0644); err != nil {
		log.Printf("AlwaysOn metrics önbelleğe kaydedilemedi: %v", err)
	} else {
		log.Printf("AlwaysOn metrics başarıyla önbelleğe kaydedildi")
	}
}

// Helper function to get cached AlwaysOn metrics
func (c *MSSQLCollector) getCachedAlwaysOnMetrics() *AlwaysOnMetrics {
	cacheFile := filepath.Join(c.getCacheDir(), "mssql_alwayson_metrics.json")
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return nil
	}

	var cacheData struct {
		Metrics   *AlwaysOnMetrics `json:"metrics"`
		Timestamp int64            `json:"timestamp"`
	}

	if err := json.Unmarshal(data, &cacheData); err != nil {
		log.Printf("AlwaysOn metrics cache parse hatası: %v", err)
		return nil
	}

	// Check if cache is recent (within last 5 minutes = 300 seconds)
	if time.Now().Unix()-cacheData.Timestamp > 300 {
		log.Printf("AlwaysOn metrics cache çok eski, yeni veri toplanacak")
		return nil
	}

	log.Printf("AlwaysOn metrics cache'den alındı")
	return cacheData.Metrics
}

// shouldCollectAlwaysOnMetrics checks if it's time to collect AlwaysOn metrics
func (c *MSSQLCollector) shouldCollectAlwaysOnMetrics() bool {
	// If collector is unhealthy, don't collect AlwaysOn metrics
	if !c.IsHealthy() {
		log.Printf("Skipping AlwaysOn metrics collection - collector unhealthy")
		return false
	}

	// Check if cached metrics exist and are still fresh
	cachedMetrics := c.getCachedAlwaysOnMetrics()

	// If no cached metrics or cache is older than 4 minutes, collect new metrics
	// This ensures we don't hit the database too frequently
	if cachedMetrics == nil {
		log.Printf("No cached AlwaysOn metrics found, collecting new data")
		return true
	}

	// Cache exists and is fresh (less than 5 minutes old), skip collection
	log.Printf("Fresh AlwaysOn metrics cache available, skipping collection to reduce load")
	return false
}

// ClearCaches clears all MSSQL related cache files to remove any inconsistent data
func (c *MSSQLCollector) ClearCaches() error {
	cacheDir := c.getCacheDir()

	// List of cache files to clear
	cacheFiles := []string{
		"mssql_system_metrics.json",
		"mssql_disk_usage.json",
		"mssql_ip_cache.txt",
		"mssql_cpu_count.txt",
		"mssql_memory.txt",
		"mssql_config_path.txt",
		"mssql_alwayson_metrics.json",
		"mssql_ha_state.json",
	}

	var errors []string

	for _, fileName := range cacheFiles {
		filePath := filepath.Join(cacheDir, fileName)
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			errors = append(errors, fmt.Sprintf("%s: %v", fileName, err))
			log.Printf("Cache file temizleme hatası %s: %v", fileName, err)
		} else if err == nil {
			log.Printf("Cache file temizlendi: %s", fileName)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("some cache files could not be cleared: %s", strings.Join(errors, ", "))
	}

	log.Printf("Tüm MSSQL cache dosyaları başarıyla temizlendi")
	return nil
}

// TestAlwaysOnSystemViews tests if AlwaysOn system views and columns are accessible
func (c *MSSQLCollector) TestAlwaysOnSystemViews() error {
	db, err := c.GetClient()
	if err != nil {
		return fmt.Errorf("could not connect to SQL Server: %w", err)
	}
	defer db.Close()

	// Test basic availability groups query
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var testCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM sys.availability_groups ag
		JOIN sys.dm_hadr_availability_group_states ags ON ag.group_id = ags.group_id
	`).Scan(&testCount)

	if err != nil {
		return fmt.Errorf("AlwaysOn system views test failed: %w", err)
	}

	log.Printf("AlwaysOn system views test successful, found %d availability groups", testCount)
	return nil
}

// CanCollect checks if enough time has passed since last collection to prevent rate limiting
func (c *MSSQLCollector) CanCollect() bool {
	if time.Since(c.lastCollectionTime) < c.collectionInterval {
		logger.Debug("Rate limiting: Not enough time passed since last collection (min interval: %v)", c.collectionInterval)
		return false
	}
	return true
}

// SetCollectionTime updates the last collection time
func (c *MSSQLCollector) SetCollectionTime() {
	c.lastCollectionTime = time.Now()
}

// IsHealthy returns the current health state
func (c *MSSQLCollector) IsHealthy() bool {
	// Check health every 2 minutes
	if time.Since(c.lastHealthCheck) > 2*time.Minute {
		c.checkHealth()
	}
	return c.isHealthy
}

// checkHealth performs a simple health check - INDEPENDENT of rate limiting
func (c *MSSQLCollector) checkHealth() {
	c.lastHealthCheck = time.Now()

	// CRITICAL: Health check must be independent of rate limiting to allow recovery
	// Don't use c.GetClient() because it might check rate limiting
	cfg, err := config.LoadAgentConfig()
	if err != nil {
		c.isHealthy = false
		logger.Warning("MSSQL health check failed - config load error: %v", err)
		c.collectionInterval = 2 * time.Minute
		return
	}

	// Direct connection for health check - bypass all rate limiting
	var connStr string
	host := cfg.MSSQL.Host
	if host == "" {
		host = "localhost"
	}

	port := cfg.MSSQL.Port
	if port == "" {
		port = "1433"
	}

	instance := cfg.MSSQL.Instance
	database := cfg.MSSQL.Database
	if database == "" {
		database = "master"
	}

	// Build connection string for health check
	if cfg.MSSQL.WindowsAuth {
		if instance != "" {
			connStr = fmt.Sprintf("server=%s\\%s;database=%s;trusted_connection=yes;connection timeout=3", host, instance, database)
		} else {
			connStr = fmt.Sprintf("server=%s,%s;database=%s;trusted_connection=yes;connection timeout=3", host, port, database)
		}
	} else {
		if instance != "" {
			connStr = fmt.Sprintf("server=%s\\%s;user id=%s;password=%s;database=%s;connection timeout=3",
				host, instance, cfg.MSSQL.User, cfg.MSSQL.Pass, database)
		} else {
			connStr = fmt.Sprintf("server=%s,%s;user id=%s;password=%s;database=%s;connection timeout=3",
				host, port, cfg.MSSQL.User, cfg.MSSQL.Pass, database)
		}
	}

	if cfg.MSSQL.TrustCert {
		connStr += ";trustservercertificate=true"
	}

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		c.isHealthy = false
		logger.Warning("MSSQL health check failed - connection open error: %v", err)
		c.collectionInterval = 2 * time.Minute
		return
	}

	// Set minimal connection limits for health check
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Second)

	// Quick test query with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	db.Close()

	if err != nil {
		c.isHealthy = false
		logger.Warning("MSSQL health check query failed - marking collector as unhealthy: %v", err)
		c.collectionInterval = 2 * time.Minute
	} else {
		// RECOVERY: If we were unhealthy, log recovery
		if !c.isHealthy {
			logger.Info("MSSQL collector RECOVERED - marking as healthy")
		}
		c.isHealthy = true
		logger.Debug("MSSQL health check passed - collector is healthy")
		c.collectionInterval = 25 * time.Second // Reset to optimized interval to prevent timing conflicts
	}
}

// ShouldSkipCollection determines if collection should be skipped due to rate limiting or health issues
func (c *MSSQLCollector) ShouldSkipCollection() bool {
	if !c.CanCollect() {
		return true
	}

	// Check health but allow more frequent recovery attempts
	if !c.IsHealthy() {
		// AGGRESSIVE RECOVERY: Try recovery every 2 minutes instead of 5
		if time.Since(c.lastHealthCheck) > 2*time.Minute {
			logger.Info("MSSQL collector has been unhealthy for 2+ minutes, attempting aggressive recovery...")
			c.ForceHealthCheck()

			// If still unhealthy after forced check, skip collection
			if !c.isHealthy {
				logger.Debug("MSSQL recovery attempt failed, skipping collection")
				return true
			}
			logger.Info("MSSQL collector successfully recovered!")
		} else {
			logger.Debug("Skipping MSSQL collection due to unhealthy state")
			return true
		}
	}

	return false
}

// ForceHealthCheck forces an immediate health check - useful for recovery
func (c *MSSQLCollector) ForceHealthCheck() {
	logger.Info("MSSQL collector forcing health check for recovery")
	c.checkHealth()
}

// ResetToHealthy forces the collector to healthy state - useful for startup recovery
func (c *MSSQLCollector) ResetToHealthy() {
	if c != nil {
		c.isHealthy = true
		c.collectionInterval = 25 * time.Second // Reduced from 30s to prevent timing conflicts with periodic reporting
		c.lastHealthCheck = time.Now()
		c.lastCollectionTime = time.Time{} // Reset to allow immediate collection
		logger.Info("MSSQL collector forcefully reset to healthy state")
	}
}

// StartupRecovery performs recovery checks at agent startup
func (c *MSSQLCollector) StartupRecovery() {
	logger.Info("MSSQL collector performing startup recovery check...")

	// Give it 3 attempts at startup
	for i := 0; i < 3; i++ {
		c.ForceHealthCheck()
		if c.isHealthy {
			logger.Info("MSSQL collector startup recovery successful on attempt %d", i+1)
			return
		}
		logger.Warning("MSSQL collector startup recovery attempt %d failed, retrying...", i+1)
		time.Sleep(2 * time.Second)
	}

	// If all attempts failed, force reset to healthy
	logger.Warning("MSSQL collector startup recovery failed after 3 attempts, forcing healthy state")
	c.ResetToHealthy()
}

// getLastKnownGoodInfo returns the last cached information to avoid excessive DB calls
func (c *MSSQLCollector) getLastKnownGoodInfo() *MSSQLInfo {
	// Try to get cached system metrics
	systemMetrics := c.getCachedSystemMetrics()

	hostname, _ := os.Hostname()

	// Create a basic info structure with cached or default values
	info := &MSSQLInfo{
		ClusterName: "",
		IP:          "",
		Hostname:    hostname,
		NodeStatus:  "UNKNOWN",
		Version:     "Unknown",
		Location:    c.cfg.MSSQL.Location,
		Status:      "RATE_LIMITED", // Indicate this is a rate-limited response
		Instance:    c.cfg.MSSQL.Instance,
		FreeDisk:    "Unknown",
		FdPercent:   0,
		Port:        c.cfg.MSSQL.Port,
		TotalvCpu:   0,
		TotalMemory: 0,
		ConfigPath:  "",
		Database:    c.cfg.MSSQL.Database,
		IsHAEnabled: false,
		HARole:      "UNKNOWN",
		Edition:     "Unknown",
		AlwaysOn:    nil,
	}

	// Fill in cached values if available
	if systemMetrics != nil {
		if ipVal, ok := systemMetrics["ip_address"].(string); ok {
			info.IP = ipVal
		}
		if cpuVal, ok := systemMetrics["cpu_count"]; ok {
			if cpu, ok := cpuVal.(int32); ok {
				info.TotalvCpu = cpu
			}
		}
		if memVal, ok := systemMetrics["total_memory"]; ok {
			if mem, ok := memVal.(int64); ok {
				info.TotalMemory = mem
			}
		}
	}

	// Try to get cached HA state
	haState, hasHAState := c.getHAStateFromCache("mssql_ha_state")
	if hasHAState {
		info.NodeStatus = haState.role
		info.ClusterName = haState.group
		info.IsHAEnabled = (haState.role != "STANDALONE" && haState.role != "")
		info.HARole = haState.role
	}

	// Try to get cached disk usage
	if cachedDisk := c.getCachedDiskUsage(); cachedDisk != nil {
		if freeDisk, ok := cachedDisk["free_disk"].(string); ok {
			info.FreeDisk = freeDisk
		}
		if usagePercent, ok := cachedDisk["usage_percent"]; ok {
			switch v := usagePercent.(type) {
			case int:
				info.FdPercent = int32(v)
			case float64:
				info.FdPercent = int32(v)
			}
		}
	}

	// Get cached config path
	info.ConfigPath = c.getCachedConfigPath()

	// Normalize data
	info.NormalizeData()

	logger.Debug("Returned cached/rate-limited info to prevent overload")
	return info
}

// Global collector instance management functions for thread safety
func init() {
	// Initialize default collector with panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in MSSQL collector init(): %v", r)
			// Try to create a minimal collector as last resort
			collectorMutex.Lock()
			if defaultMSSQLCollector == nil {
				defaultMSSQLCollector = &MSSQLCollector{
					collectionInterval: 25 * time.Second, // Reduced from 30s to prevent timing conflicts
					maxRetries:         3,
					backoffDuration:    5 * time.Second,
					isHealthy:          true,
					lastHealthCheck:    time.Now(),
				}
			}
			collectorMutex.Unlock()
			log.Printf("MSSQL collector init() recovered from panic")
		}
	}()

	// Initialize default collector - will be updated when config is available
	collectorMutex.Lock()
	defaultMSSQLCollector = &MSSQLCollector{
		collectionInterval: 25 * time.Second, // Reduced from 30s to prevent timing conflicts with 30s periodic reporting
		maxRetries:         3,
		backoffDuration:    5 * time.Second,
		isHealthy:          true,
		lastHealthCheck:    time.Now(),
	}
	collectorMutex.Unlock()
	log.Printf("MSSQL default collector initialized in init() with thread safety")

	// DON'T perform startup recovery automatically in init()
	// Let the agent explicitly decide when to start collector based on platform
	// This prevents MSSQL collector from starting in PostgreSQL agents
	log.Printf("MSSQL collector init() completed - startup recovery will be triggered by agent if needed")
}

// EnsureDefaultCollector ensures the default collector is initialized (thread-safe)
func EnsureDefaultCollector() {
	// First quick read-only check
	collectorMutex.RLock()
	if defaultMSSQLCollector != nil {
		logger.Debug("EnsureDefaultCollector: Collector already exists")
		collectorMutex.RUnlock()
		return
	}
	collectorMutex.RUnlock()

	logger.Debug("EnsureDefaultCollector: Collector is nil, acquiring write lock")

	// Need to create new collector
	collectorMutex.Lock()
	defer collectorMutex.Unlock()

	// Double-check in case another goroutine created it
	if defaultMSSQLCollector == nil {
		logger.Warning("EnsureDefaultCollector: Creating new collector - this should only happen once")

		// Try to create with proper recovery
		defer func() {
			if r := recover(); r != nil {
				logger.Error("PANIC in EnsureDefaultCollector: %v", r)
				// Create minimal collector as fallback
				defaultMSSQLCollector = &MSSQLCollector{
					collectionInterval: 25 * time.Second, // Reduced from 30s to prevent timing conflicts
					maxRetries:         3,
					backoffDuration:    5 * time.Second,
					isHealthy:          true,
					lastHealthCheck:    time.Now(),
				}
				logger.Error("EnsureDefaultCollector: Created fallback collector after panic")
			}
		}()

		defaultMSSQLCollector = &MSSQLCollector{
			collectionInterval: 25 * time.Second, // Reduced from 30s to prevent timing conflicts with 30s periodic reporting
			maxRetries:         3,
			backoffDuration:    5 * time.Second,
			isHealthy:          true,
			lastHealthCheck:    time.Now(),
		}
		logger.Info("EnsureDefaultCollector: Successfully created new collector")
	} else {
		logger.Debug("EnsureDefaultCollector: Another goroutine already created collector")
	}
}

// GetDefaultCollectorSafe returns the default collector in a thread-safe manner
func GetDefaultCollectorSafe() *MSSQLCollector {
	// Implement panic recovery
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetDefaultCollectorSafe: %v", r)
		}
	}()

	collectorMutex.RLock()
	defer collectorMutex.RUnlock()

	if defaultMSSQLCollector == nil {
		logger.Error("GetDefaultCollectorSafe: Collector is nil - this should not happen!")
		return nil
	}

	logger.Debug("GetDefaultCollectorSafe: Returning healthy collector")
	return defaultMSSQLCollector
}

// UpdateDefaultMSSQLCollector updates the default collector with proper config (thread-safe)
func UpdateDefaultMSSQLCollector(cfg *config.AgentConfig) {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	defaultMSSQLCollector = NewMSSQLCollector(cfg)
	log.Printf("MSSQL default collector updated with new config (thread-safe)")

	// Perform startup recovery ONLY ONCE to prevent multiple goroutines and race conditions
	// This prevents the request storm issue when agent restarts
	startupRecoveryOnce.Do(func() {
		go func() {
			// Wait a bit for initialization to complete
			time.Sleep(1 * time.Second)

			// Re-acquire the collector safely
			collectorMutex.RLock()
			collector := defaultMSSQLCollector
			collectorMutex.RUnlock()

			if collector != nil {
				// Check if this is a MSSQL agent before running startup recovery
				if cfg.MSSQL.Host == "" {
					log.Printf("MSSQL collector update: No MSSQL config found, skipping startup recovery for non-MSSQL agent")
					return
				}

				log.Printf("MSSQL collector performing ONE-TIME startup recovery after config update...")
				collector.StartupRecovery()
				log.Printf("MSSQL collector one-time startup recovery completed")
			}
		}()
	})
}

// GetDefaultMSSQLCollector returns the default collector instance (thread-safe)
func GetDefaultMSSQLCollector() *MSSQLCollector {
	return GetDefaultCollectorSafe()
}

// measureResponseTime measures MSSQL response time by executing a simple SELECT 1 query
func (c *MSSQLCollector) measureResponseTime(metrics map[string]interface{}) {
	log.Printf("DEBUG: measureResponseTime - Starting response time measurement")

	// Try to get database connection with a short timeout
	db, err := c.GetClient()
	if err != nil {
		log.Printf("DEBUG: measureResponseTime - Database connection failed: %v", err)
		log.Printf("MSSQL bağlantısı kurulamadı, response time ölçülemiyor: %v", err)
		// Set response time to -1 to indicate connection failure
		metrics["response_time_ms"] = float64(-1)
		log.Printf("DEBUG: measureResponseTime - Set response_time_ms = -1 (connection failure)")
		return
	}
	defer func() {
		log.Printf("DEBUG: measureResponseTime - Closing database connection")
		db.Close()
	}()

	log.Printf("DEBUG: measureResponseTime - Database connection successful, starting query execution")

	// Execute simple SELECT 1 query to measure response time with high precision
	queryStart := time.Now()
	var result int
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	log.Printf("DEBUG: measureResponseTime - Executing 'SELECT 1' query with 3 second timeout")
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	duration := time.Since(queryStart)

	log.Printf("DEBUG: measureResponseTime - Query completed in %v (nanoseconds: %d)", duration, duration.Nanoseconds())

	if err != nil {
		log.Printf("DEBUG: measureResponseTime - Query execution failed: %v", err)
		log.Printf("MSSQL response time query failed: %v", err)
		// Set response time to -1 to indicate query failure
		metrics["response_time_ms"] = float64(-1)
		log.Printf("DEBUG: measureResponseTime - Set response_time_ms = -1 (query failure)")
		return
	}

	log.Printf("DEBUG: measureResponseTime - Query returned result: %d", result)

	// Convert to milliseconds for API compatibility (nanoseconds / 1,000,000)
	responseTimeMs := float64(duration.Nanoseconds()) / 1e6

	// Ensure minimum response time to avoid 0.00 values that might be filtered
	// If response time is 0 or extremely small, set to 0.001 ms minimum (1 microsecond)
	if responseTimeMs < 0.001 {
		log.Printf("DEBUG: measureResponseTime - Response time too small (%.6f ms), setting minimum to 0.001 ms", responseTimeMs)
		responseTimeMs = 0.001
	}

	metrics["response_time_ms"] = responseTimeMs

	log.Printf("DEBUG: measureResponseTime - Calculated response time: %.6f ms (nanoseconds: %d)", responseTimeMs, duration.Nanoseconds())
	log.Printf("DEBUG: measureResponseTime - Set metrics[\"response_time_ms\"] = %.6f", responseTimeMs)
	log.Printf("MSSQL response time measured: %.6f ms", responseTimeMs)
}
