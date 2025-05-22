package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb" // MSSQL driver
	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector/utils"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
)

// MSSQLCollector mssql için veri toplama yapısı
type MSSQLCollector struct {
	cfg *config.AgentConfig

	// Cache mechanisms to reduce PowerShell calls
	infoCache      *MSSQLInfo
	infoCacheLock  sync.RWMutex
	lastInfoUpdate time.Time
	infoCacheTTL   time.Duration

	// SQL bağlantılarını yönetmek için
	sqlConnLock   sync.Mutex
	sqlConn       *sql.DB
	lastConnCheck time.Time
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

// Global singleton kolektör
var (
	globalCollector *MSSQLCollector
	collectorMutex  sync.Mutex
)

// NewMSSQLCollector yeni bir MSSQLCollector oluşturur veya mevcut singleton kolektörü döndürür
func NewMSSQLCollector(cfg *config.AgentConfig) *MSSQLCollector {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()

	// Eğer mevcut bir kolektör varsa, bunu yeniden kullan
	if globalCollector != nil {
		logger.Debug("Returning existing MSSQL collector instance")
		return globalCollector
	}

	// Yeni kolektör oluştur
	collector := &MSSQLCollector{
		cfg:          cfg,
		infoCacheTTL: 5 * time.Minute, // Default cache TTL is 5 minutes (was 2 minutes)
	}

	// Windows sistemlerinde PowerShell process izleme mekanizmasını başlat
	if runtime.GOOS == "windows" {
		// Start PowerShell process monitor in the background with higher thresholds (less frequent checks)
		// Check every 30 minutes, allow max 15 processes, clean up processes older than 60 minutes
		utils.StartPowerShellMonitor(30, 15, 60)
		logger.Info("MSSQL Collector: PowerShell process monitoring started with optimized settings")

		// Start Windows performance metrics background collection
		utils.StartBackgroundCollection()
		logger.Info("MSSQL Collector: Background Windows metrics collection started")
	}

	// Global kolektörü güncelle
	globalCollector = collector
	logger.Info("Created new MSSQL collector instance")

	return collector
}

// GetClient returns a SQL Server connection
func (c *MSSQLCollector) GetClient() (*sql.DB, error) {
	c.sqlConnLock.Lock()
	defer c.sqlConnLock.Unlock()

	// Mevcut bağlantıyı kontrol et
	if c.sqlConn != nil {
		// Ping yöntemiyle bağlantının açık olduğunu doğrula
		// Ancak çok sık ping yapmaktan kaçın - 30 saniyede bir maksimum
		if time.Since(c.lastConnCheck) > 30*time.Second {
			pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			pingErr := c.sqlConn.PingContext(pingCtx)
			cancel()

			c.lastConnCheck = time.Now() // Her zaman son kontrol zamanını güncelle

			if pingErr != nil {
				logger.Error("!!!!! Existing SQL connection invalid (ping failed): %v !!!!!", pingErr)
				logger.Error("Bağlantı kapatılıp yeni bağlantı açılacak...")
				// Bağlantıyı yeniden oluştur
				c.sqlConn.Close()
				c.sqlConn = nil
			} else {
				logger.Info("SQL connection is valid (ping successful)")
				return c.sqlConn, nil
			}
		} else {
			// Son kontrol yeterince yakın zamanda yapıldı, bağlantıyı yeniden kullan
			logger.Debug("Reusing existing SQL connection (last checked %v ago)", time.Since(c.lastConnCheck))
			return c.sqlConn, nil
		}
	}

	// Debug: Şu anki connection pool durumunu göster
	logger.Info("SQL Bağlantı havuzu durumu: sqlConn=%v, lastConnCheck=%v",
		c.sqlConn != nil, c.lastConnCheck)

	// Retry mekanizması ekleyelim
	maxRetries := 3
	retryDelay := 1 * time.Second
	var lastError error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("SQL Server bağlantısı yeniden deneniyor (%d/%d)...", attempt+1, maxRetries)
			time.Sleep(retryDelay)
			// Her denemede bekleme süresini arttır
			retryDelay *= 2
		}

		// Bağlantı yok, yeni bağlantı oluştur
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

		// Additional connection parameters for fixing socket issues
		connStr += ";connection timeout=15"

		// Add connection pooling parameters to fix socket exhaustion
		connStr += ";connection pooling=true;max pool size=30;min pool size=3"

		// Add connection reset parameters to help with socket reuse
		connStr += ";connection reset=true;application intent=readwrite"

		// Set TCP keepalive to help with stale connections
		connStr += ";keepalive=60"

		logger.Debug("MSSQL connection string (credentials hidden): %s",
			regexp.MustCompile(`password=([^;])*`).ReplaceAllString(connStr, "password=****"))

		logger.Debug("Creating SQL Server connection to %s...", host)
		startTime := time.Now()

		// Bağlantıyı oluştur
		db, err := sql.Open("sqlserver", connStr)
		if err != nil {
			// Enhanced logging for connection errors
			logger.Error("Failed to open SQL connection after %v: %v",
				time.Since(startTime), err)

			lastError = err
			continue // Retry
		}

		// Bağlantı havuzu ayarları
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(30 * time.Minute) // 30 dakikaya çıkar
		db.SetConnMaxIdleTime(10 * time.Minute) // 10 dakikaya çıkar

		// Bağlantıyı ping ile test et
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pingErr := db.PingContext(ctx)
		cancel()

		if pingErr != nil {
			db.Close() // Bağlantıyı temizle
			logger.Error("SQL connection ping test failed: %v", pingErr)
			lastError = pingErr
			continue // Retry
		}

		// Bağlantı başarılı ise, yapıyı güncelle
		c.sqlConn = db
		c.lastConnCheck = time.Now()

		// Connection successfully established
		logger.Info("SQL Server connection established in %v", time.Since(startTime))
		return db, nil
	}

	// Tüm denemeler başarısız oldu
	return nil, fmt.Errorf("failed to connect to SQL Server after %d attempts: %v", maxRetries, lastError)
}

// GetMSSQLStatus checks if SQL Server service is running by checking if the configured host:port is accessible
func (c *MSSQLCollector) GetMSSQLStatus() string {
	// First try to establish a DB connection - with a short timeout to avoid blocking
	clientCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
		// On Windows, we can try checking the service status in a faster way
		if runtime.GOOS == "windows" {
			// Use sc.exe directly instead of PowerShell
			cmd := exec.Command("sc", "query", "MSSQL$"+c.cfg.MSSQL.Instance)
			if err := cmd.Run(); err == nil {
				return "RUNNING"
			}
		}
		return "UNKNOWN" // Can't determine status via TCP for named instances
	}

	// Try to establish a TCP connection to check if the port is listening
	address := fmt.Sprintf("%s:%s", host, port)
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(clientCtx, "tcp", address)
	if err != nil {
		logger.Warning("MSSQL at %s is not accessible: %v", address, err)
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
		logger.Error("Veritabanı bağlantısı kurulamadı: %v", err)
		return "Unknown", "Unknown"
	}
	defer db.Close()

	var version, edition string
	err = db.QueryRow("SELECT @@VERSION, SERVERPROPERTY('Edition')").Scan(&version, &edition)
	if err != nil {
		logger.Error("Versiyon bilgisi alınamadı: %v", err)
		return "Unknown", "Unknown"
	}

	// Log version değerinin tam içeriğini
	logger.Debug("SQL Server versiyon ham verisi: %s", version)

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
		logger.Warning("Veritabanı bağlantısı kurulamadı: %v", err)

		// Önbellekte HA bilgisi varsa onu kullan
		if hasHAState {
			logger.Info("Servis kapalı ama önbellekte HA durumu mevcut: %s, önbellekteki bilgiler kullanılıyor", haState.role)
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
		logger.Debug("AlwaysOn özelliği etkin değil veya kontrol edilemiyor: %v", err)
		return "STANDALONE", false
	}

	logger.Debug("AlwaysOn özelliği etkin, node rolü tespit ediliyor...")

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
		logger.Warning("Node role could not be determined: %v", err)

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
			logger.Warning("Fallback node role query failed: %v", err)
			return "UNKNOWN", true
		}
	}

	logger.Debug("Node rolü tespit edildi: %s", role)
	return role, true
}

// getHAStateFromCache is a helper to get HA information from alarm cache
func (c *MSSQLCollector) getHAStateFromCache(key string) (struct{ role, group string }, bool) {
	// Boş durumu hazırla
	emptyState := struct{ role, group string }{"", ""}

	// Get cache file path based on OS
	cacheFile := getCacheFilePath()

	// Log attempted cache read
	logger.Debug("MSSQL HA durum önbelleği okunuyor: %s", cacheFile)

	// Check if file exists
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		logger.Debug("MSSQL HA durum önbelleği bulunamadı, ilk çalıştırma olabilir")
		return emptyState, false
	}

	// Read cache file
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		logger.Warning("MSSQL HA durum önbelleği okunamadı: %v", err)
		return emptyState, false
	}

	// Parse cached data from JSON
	var cachedData struct {
		Role        string    `json:"role"`
		Group       string    `json:"group"`
		LastUpdated time.Time `json:"last_updated"`
	}

	if err := json.Unmarshal(data, &cachedData); err != nil {
		logger.Warning("MSSQL HA durum önbelleği ayrıştırılamadı (JSON hatası): %v", err)

		// Try legacy format for backward compatibility
		parts := strings.Split(string(data), "|")
		if len(parts) >= 2 {
			logger.Info("Eski format önbellekte HA bilgisi bulundu: %s, %s", parts[0], parts[1])
			return struct{ role, group string }{parts[0], parts[1]}, true
		}

		return emptyState, false
	}

	// Check if cache is stale (7 days max)
	if time.Since(cachedData.LastUpdated) > 7*24*time.Hour {
		logger.Warning("MSSQL HA durum önbelleği çok eski (7 günden fazla): %s, yeni veri gerekli",
			cachedData.LastUpdated.Format("2006-01-02 15:04:05"))
		return emptyState, false
	}

	logger.Debug("MSSQL HA durum önbelleğinden bilgi alındı: Role=%s, Group=%s, LastUpdated=%s",
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
		logger.Warning("Cache dizini oluşturulamadı: %v, geçici dizin kullanılacak", err)
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
		logger.Error("HA durumu JSON formatına dönüştürülemedi: %v", err)
		return
	}

	// Get cache file path
	cacheFile := getCacheFilePath()

	// Ensure directory exists
	cacheDir := filepath.Dir(cacheFile)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		logger.Error("Cache dizini oluşturulamadı: %v", err)
		return
	}

	// Write cache file
	err = os.WriteFile(cacheFile, jsonData, 0644)
	if err != nil {
		logger.Error("HA durumu önbelleğe kaydedilemedi: %v", err)
	} else {
		logger.Debug("HA durumu başarıyla önbelleğe kaydedildi: Role=%s, Group=%s, Path=%s",
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
		logger.Warning("Veritabanı bağlantısı kurulamadı: %v", err)

		// Önbellekte HA bilgisi varsa onu kullan
		if hasHAState && haState.group != "" {
			logger.Info("SQL Server servis kapalı, önbellekteki cluster adı kullanılıyor: %s", haState.group)
			return haState.group
		}

		logger.Warning("SQL Server kapalı ve önbellekte HA bilgisi bulunamadı")
		return ""
	}
	defer db.Close()

	logger.Debug("AlwaysOn Availability Group ismi tespit ediliyor...")

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
		logger.Warning("İlk metod ile AG ismi tespit edilemedi: %v", err)

		// Alternatif yöntem dene - daha basit sorgu
		err = db.QueryRow(`
			SELECT TOP 1 ag.name 
			FROM sys.availability_groups ag
			JOIN sys.dm_hadr_availability_replica_states ars 
				ON ag.group_id = ars.group_id
		`).Scan(&clusterName)

		if err != nil {
			logger.Warning("İkinci metod ile AG ismi tespit edilemedi: %v", err)

			// Son çare olarak "cluster_name" sistem bilgisini almayı dene
			err = db.QueryRow(`
				SELECT TOP 1 cluster_name 
				FROM sys.dm_hadr_cluster
			`).Scan(&clusterName)

			if err != nil {
				logger.Warning("Cluster name could not be determined: %v", err)
				// Önbellekte bilgi varsa, veritabanından bilgi alınamasa bile önbellekteki bilgiyi geri döndür
				if hasHAState && haState.group != "" {
					logger.Info("Veritabanından bilgi alınamadı, önbellekteki grup bilgisini kullanılıyor: %s", haState.group)
					clusterName = haState.group
				}
			}
		}
	}

	logger.Debug("Tespit edilen Availability Group ismi: %s", clusterName)

	// Bilgileri önbelleğe kaydet
	if nodeRole, isHA := c.GetNodeStatus(); isHA {
		c.saveHAStateToCache(nodeRole, clusterName)
	}

	return clusterName
}

// GetDiskUsage returns disk usage information for the SQL Server data drive
func (c *MSSQLCollector) GetDiskUsage() (string, int) {
	if runtime.GOOS == "windows" {
		// First try the optimized Windows metrics collection approach
		metrics, err := utils.CollectWindowsMetrics()
		if err == nil && metrics != nil && metrics.DiskTotal > 0 {
			// Format disk space in human-readable format
			freeBytes := metrics.DiskFree
			// Calculate percentage used
			usedPercent := metrics.DiskUsage

			// Format the free space string
			freeStr := c.formatBytes(uint64(freeBytes))

			logger.Debug("Disk space obtained from optimized Windows metrics: %s free, %.2f%% used",
				freeStr, usedPercent)

			return freeStr, int(usedPercent)
		}
		logger.Warning("Failed to get disk space from optimized metrics, falling back to alternative method: %v", err)

		// Try the efficient method directly
		total, free, err := utils.GetDiskInfoEfficient()
		if err == nil && total > 0 {
			// Calculate percentage
			used := total - free
			usedPercent := float64(used) / float64(total) * 100.0

			// Format free space
			freeStr := c.formatBytes(uint64(free))

			logger.Debug("Disk space obtained from efficient method: %s free, %.2f%% used",
				freeStr, usedPercent)

			return freeStr, int(usedPercent)
		}
		logger.Warning("Failed to get disk space from efficient method, falling back to legacy method: %v", err)
	}

	// Original implementation as fallback
	var freeDisk string
	var usedPercent int

	// Try to determine SQL Server data directory
	dataPath := c.getMainDataDirectory()
	if dataPath == "" {
		// If we couldn't determine the data directory, just use system drive
		dataPath = "C:"
		if runtime.GOOS != "windows" {
			dataPath = "/"
		}
	}

	// Get the drive letter or mount point
	driveLetter := "C"
	if runtime.GOOS == "windows" {
		if strings.Contains(dataPath, ":") {
			driveLetter = strings.ToUpper(dataPath[:1])
		}
	}

	if runtime.GOOS == "windows" {
		// Get disk space for the drive containing SQL Server data
		output, err := utils.GetSQLServerVolumeDiskSpace(driveLetter, 15)
		if err != nil {
			logger.Error("Failed to get disk space: %v", err)
			return "Unknown", 0
		}

		// Parse the disk information
		type VolumeInfo struct {
			SizeRemaining float64 `json:"SizeRemaining"`
			Size          float64 `json:"Size"`
		}

		var volumeInfo VolumeInfo
		err = json.Unmarshal([]byte(output), &volumeInfo)
		if err != nil {
			logger.Error("Failed to parse disk space information: %v", err)
			return "Unknown", 0
		}

		// Calculate used percentage
		if volumeInfo.Size > 0 {
			usedPercent = int(100 - (100 * volumeInfo.SizeRemaining / volumeInfo.Size))
			freeDisk = c.formatBytes(uint64(volumeInfo.SizeRemaining))
		}
	} else {
		// For Unix-like systems
		cmd := exec.Command("df", "-B1", dataPath)
		output, err := cmd.Output()
		if err != nil {
			logger.Error("Failed to get disk space: %v", err)
			return "Unknown", 0
		}

		lines := strings.Split(string(output), "\n")
		if len(lines) < 2 {
			logger.Error("Unexpected output format from df command")
			return "Unknown", 0
		}

		fields := strings.Fields(lines[1])
		if len(fields) < 5 {
			logger.Error("Unexpected fields count in df command output")
			return "Unknown", 0
		}

		// Parse percentage used from df output
		percentStr := strings.TrimSuffix(fields[4], "%")
		percent, err := strconv.Atoi(percentStr)
		if err != nil {
			logger.Error("Failed to parse disk usage percentage: %v", err)
			return "Unknown", 0
		}
		usedPercent = percent

		// Parse free space
		freeSpace, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			logger.Error("Failed to parse free disk space: %v", err)
			return "Unknown", 0
		}
		freeDisk = c.formatBytes(uint64(freeSpace))
	}

	return freeDisk, usedPercent
}

// getMainDataDirectory attempts to find the SQL Server data directory
func (c *MSSQLCollector) getMainDataDirectory() string {
	if runtime.GOOS == "windows" {
		// First try from registry
		instance := c.cfg.MSSQL.Instance
		regValue := "SQLDataRoot"

		output, err := utils.GetSQLServerRegistryInfo(instance, regValue, 10)
		if err == nil && output != "" {
			return output
		}

		// If we can connect to the database, try to query master.dbo.sysdatabases
		db, err := c.GetClient()
		if err == nil {
			defer db.Close()

			var dataDir string
			err = db.QueryRow("SELECT physical_name FROM sys.master_files WHERE database_id = 1 AND file_id = 1").Scan(&dataDir)
			if err == nil && dataDir != "" {
				// Extract the drive and directory
				return filepath.Dir(dataDir)
			}
		}
	} else {
		// For Linux SQL Server, typical location is /var/opt/mssql/data
		if _, err := os.Stat("/var/opt/mssql/data"); err == nil {
			return "/var/opt/mssql/data"
		}
	}

	return ""
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
		logger.Warning("Veritabanı bağlantısı kurulamadı: %v", err)
		return ""
	}
	defer db.Close()

	var configPath string
	err = db.QueryRow(`
		SELECT SERVERPROPERTY('ErrorLogFileName')
	`).Scan(&configPath)

	if err != nil || configPath == "" {
		logger.Warning("Error log path could not be determined: %v", err)

		// Try to find the SQL Server registry location
		if runtime.GOOS == "windows" {
			instance := c.cfg.MSSQL.Instance
			regPath := "MSSQL"
			if instance != "" {
				regPath = "MSSQL$" + instance
			}

			command := fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name SQLPath", regPath)
			output, err := utils.RunPowerShellCommand(command, 15) // 15 second timeout
			if err == nil {
				rePath := regexp.MustCompile(`SQLPath\s+:\s+(.+)`)
				match := rePath.FindStringSubmatch(output)
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

// GetMSSQLInfo collects SQL Server information with caching
func (c *MSSQLCollector) GetMSSQLInfo() *MSSQLInfo {
	// Check if we have valid cached data
	c.infoCacheLock.RLock()
	if c.infoCache != nil && time.Since(c.lastInfoUpdate) < c.infoCacheTTL {
		// Clone the cached data to prevent modification
		infoCopy := *c.infoCache
		c.infoCacheLock.RUnlock()
		logger.Debug("Using cached MSSQL info (age: %v)", time.Since(c.lastInfoUpdate))
		return &infoCopy
	}
	c.infoCacheLock.RUnlock()

	// Bağlantı hatalarına karşı dayanıklılık için retry mekanizması ekleyelim
	var info *MSSQLInfo
	var bestInfo *MSSQLInfo
	maxRetries := 3 // Arttırıyoruz - daha fazla deneme yapalım
	retryDelay := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("MSSQL bilgisi toplama yeniden deneniyor (%d/%d)...", attempt+1, maxRetries)
			time.Sleep(retryDelay)
			// Her denemede bekleme süresini arttır
			retryDelay *= 2

			// Bağlantı havuzu sorunlarına karşı bağlantıyı temizle
			c.sqlConnLock.Lock()
			if c.sqlConn != nil {
				c.sqlConn.Close()
				c.sqlConn = nil
				logger.Info("Önceki SQL bağlantısı kapatıldı, yeni bağlantı denenecek")
			}
			c.sqlConnLock.Unlock()
		}

		// Collect data
		info = c.collectMSSQLInfo()

		// Eğer bilgilerin kalitesini değerlendirin
		infoQuality := evaluateInfoQuality(info)

		// Eğer bilgiler yeterince iyi ise, daha fazla deneme yapmaya gerek yok
		if infoQuality >= 90 { // Daha yüksek kalite eşiği - özellikle versiyon bilgisini almalıyız
			logger.Info("MSSQL bilgileri yeterli kalitede toplandı (Kalite: %d/100)", infoQuality)
			break
		}

		// Versiyon bilgisi özellikle eksik ise yeniden dene
		if info.Version == "Unknown" || info.Version == "" {
			logger.Warning("Versiyon bilgisi alınamadı, yeniden deneniyor...")
			continue
		}

		// Şu ana kadar toplanan en iyi bilgiyi kaydet
		if bestInfo == nil || evaluateInfoQuality(info) > evaluateInfoQuality(bestInfo) {
			bestInfo = info
		}

		logger.Warning("MSSQL bilgileri eksik toplandı (Kalite: %d/100), yeniden deneniyor...", infoQuality)
	}

	// Eğer tüm denemeler başarısız olduysa, en iyi bilgiyi kullan
	if bestInfo != nil && evaluateInfoQuality(info) < evaluateInfoQuality(bestInfo) {
		info = bestInfo
		logger.Info("En iyi MSSQL bilgileri kullanılıyor (Kalite: %d/100)", evaluateInfoQuality(info))
	}

	// Update the cache
	c.infoCacheLock.Lock()
	c.infoCache = info
	c.lastInfoUpdate = time.Now()
	c.infoCacheLock.Unlock()

	return info
}

// evaluateInfoQuality veri kalitesini değerlendirir (0-100 arası)
func evaluateInfoQuality(info *MSSQLInfo) int {
	if info == nil {
		return 0
	}

	quality := 0

	// Temel bilgiler
	if info.Hostname != "" {
		quality += 10
	}
	if info.IP != "" {
		quality += 10
	}

	// Versiyon bilgisine daha fazla ağırlık verelim
	if info.Version != "" && info.Version != "Unknown" {
		quality += 30 // 20'den 30'a arttırdık - daha kritik bir bilgi
	}

	if info.Status != "" {
		quality += 10
	}

	// HA bilgileri
	if info.NodeStatus != "" && info.NodeStatus != "UNKNOWN" {
		quality += 15
	}
	if info.ClusterName != "" {
		quality += 15
	}

	// Sistem bilgileri
	if info.TotalvCpu > 0 {
		quality += 5 // 10'dan 5'e düşürdük - daha az kritik
	}
	if info.TotalMemory > 0 {
		quality += 5 // 10'dan 5'e düşürdük - daha az kritik
	}

	return quality
}

// collectMSSQLInfo actually collects SQL Server information
func (c *MSSQLCollector) collectMSSQLInfo() *MSSQLInfo {
	// Use logger instead of log.Printf
	logger.Info("MSSQL bilgileri toplamaya başlanıyor...")

	// Use fewer PowerShell calls by collecting data in batches
	var (
		hostname, ip     string
		freeDisk         string
		usagePercent     int
		version, edition string
		nodeStatus       string
		isHAEnabled      bool
		clusterName      string
		totalvCpu        int32
		totalMemory      int64
		configPath       string
		serviceStatus    string
	)

	// Get hostname and IP using Go's standard library when possible
	hostname, _ = os.Hostname()

	if runtime.GOOS == "windows" {
		// Get network information with single PowerShell call for Windows
		ip = c.getLocalIP()
	} else {
		// For other platforms, use Go standard library
		ip = c.getLocalIPGo()
	}

	logger.Debug("Hostname: %s, IP: %s", hostname, ip)

	// Disk kullanımı al - use the optimized metrics when available
	logger.Debug("Disk kullanımı alınıyor...")
	if runtime.GOOS == "windows" {
		// Get metrics from Windows performance collector
		metrics, err := utils.CollectWindowsMetrics()
		if err == nil && metrics != nil {
			usagePercent = int(metrics.DiskUsage)
			freeDisk = formatBytes(float64(metrics.DiskFree))
			logger.Debug("Disk kullanımı metrics: %s boş, %d%% kullanımda", freeDisk, usagePercent)
		} else {
			// Fallback to traditional method
			freeDisk, usagePercent = c.GetDiskUsage()
			logger.Debug("Disk kullanımı fallback: %s boş, %d%% kullanımda", freeDisk, usagePercent)
		}
	} else {
		// Traditional method for non-Windows
		freeDisk, usagePercent = c.GetDiskUsage()
		logger.Debug("Disk kullanımı: %s boş, %d%% kullanımda", freeDisk, usagePercent)
	}

	// Tek bir veritabanı bağlantısı açalım ve tüm sorgularda bunu kullanalım
	logger.Info("Veritabanı bağlantısı açılıyor (MSSQL bilgileri için)...")

	// Retry mekanizması - başlangıçta çok agresif olalım
	var db *sql.DB
	var err error

	// Birkaç kez deneme yapalım - başlangıçta SQL Server hazır olmayabilir
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			waitTime := time.Duration(attempt*2+1) * time.Second
			logger.Info("MSSQL bağlantısı deneme %d - %v bekleniyor...", attempt+1, waitTime)
			time.Sleep(waitTime)
		}

		db, err = c.GetClient()
		if err == nil {
			break
		}
		logger.Warning("Deneme %d - Veritabanı bağlantısı kurulamadı: %v", attempt+1, err)
	}

	if err != nil {
		logger.Warning("Tüm denemeler sonrası veritabanı bağlantısı kurulamadı: %v - Bazı bilgiler alınamayabilir", err)
	} else {
		logger.Info("Veritabanı bağlantısı başarıyla kuruldu")

		// Önemli: İşlem sonunda bağlantıyı kapat
		defer func() {
			// MSSQL bağlantısını kapatma - singleton pattern kullanıyoruz
			// db.Close()
			logger.Debug("Veritabanı bağlantısı korunuyor (singleton pattern)")
		}()

		// Get version and edition
		logger.Debug("SQL Server versiyon ve sürümü alınıyor...")

		// Versiyon bilgisini almak için birkaç deneme yap
		versionRetries := 2
		for vAttempt := 0; vAttempt <= versionRetries; vAttempt++ {
			if vAttempt > 0 {
				logger.Warning("Versiyon bilgisini alma yeniden deneniyor (%d/%d)...", vAttempt, versionRetries)
				time.Sleep(1 * time.Second)

				// Bağlantıyı yeniden kontrol et ve gerekirse yenile
				pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				pingErr := db.PingContext(pingCtx)
				cancel()

				if pingErr != nil {
					logger.Warning("Veritabanı bağlantısı kapanmış: %v, yeniden bağlanmayı deniyorum...", pingErr)

					// Yeni bir bağlantı aç
					newDb, newErr := c.GetClient()
					if newErr == nil {
						// Eski bağlantıyı kapat ve yenisini kullan
						db.Close()
						db = newDb
						logger.Info("Veritabanı bağlantısı başarıyla yenilendi")
					} else {
						logger.Error("Veritabanı bağlantısı yenilenemedi: %v", newErr)
					}
				}
			}

			err = db.QueryRow("SELECT @@VERSION, SERVERPROPERTY('Edition')").Scan(&version, &edition)
			if err == nil {
				// Başarılı olduk, döngüden çık
				break
			}

			logger.Error("Versiyon bilgisi alınamadı (deneme %d/%d): %v", vAttempt+1, versionRetries+1, err)

			// Database is closed hatası mı kontrol et
			if strings.Contains(err.Error(), "database is closed") || strings.Contains(err.Error(), "connection is closed") {
				logger.Warning("Veritabanı bağlantısı kapanmış, yeniden bağlanılacak...")

				// Son denemede değilsek yeniden bağlantı dene
				if vAttempt < versionRetries {
					// Bağlantıyı yenile
					newDb, newErr := c.GetClient()
					if newErr == nil {
						// Eski bağlantıyı kapat ve yenisini kullan
						db.Close()
						db = newDb
						logger.Info("Veritabanı bağlantısı başarıyla yenilendi")
					} else {
						logger.Error("Veritabanı bağlantısı yenilenemedi: %v", newErr)
					}
				}
			}
		}

		if err == nil {
			// Log version değerinin tam içeriğini
			logger.Debug("SQL Server versiyon ham verisi: %s", version)

			// Daha geniş bir regex pattern ile versiyonu algıla
			re := regexp.MustCompile(`Microsoft SQL Server\s+(\d+)(?:\s+\(RTM\))?.*?(\d+\.\d+\.\d+\.\d+)`)
			matches := re.FindStringSubmatch(version)
			if len(matches) > 2 {
				version = matches[2]
			} else {
				// Alternatif regex pattern dene - direkt olarak sürüm numarasını bul
				re = regexp.MustCompile(`(\d+\.\d+\.\d+\.\d+)`)
				matches = re.FindStringSubmatch(version)
				if len(matches) > 1 {
					version = matches[1]
				} else {
					// Eğer bu da bulamazsa, en azından SQL Server sürümünü al (2019, 2022 gibi)
					re = regexp.MustCompile(`Microsoft SQL Server\s+(\d{4})`)
					matches = re.FindStringSubmatch(version)
					if len(matches) > 1 {
						version = matches[1]
					} else {
						version = "Unknown"
					}
				}
			}
		}
		logger.Debug("SQL Server versiyon: %s, sürüm: %s", version, edition)

		// Get HA status
		logger.Debug("High Availability durumu kontrol ediliyor...")
		// Check if AlwaysOn is enabled
		var isHAEnabledInt int
		err = db.QueryRow(`
			SELECT CASE 
				WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 1
				ELSE 0
			END AS IsHAEnabled
		`).Scan(&isHAEnabledInt)

		if err != nil {
			logger.Warning("AlwaysOn özelliği kontrolü başarısız: %v", err)
			isHAEnabled = false
		} else {
			isHAEnabled = (isHAEnabledInt == 1)
		}

		if isHAEnabled {
			logger.Debug("AlwaysOn özelliği etkin, node rolü tespit ediliyor...")

			// If AlwaysOn is enabled, check if this is a primary or secondary replica with a more reliable query
			err = db.QueryRow(`
				SELECT
					CASE WHEN dm_hadr_availability_replica_states.role_desc IS NULL THEN 'UNKNOWN'
						 ELSE dm_hadr_availability_replica_states.role_desc 
					END AS role
				FROM sys.dm_hadr_availability_replica_states 
				JOIN sys.availability_replicas 
					ON dm_hadr_availability_replica_states.replica_id = availability_replicas.replica_id
				WHERE availability_replicas.replica_server_name = @@SERVERNAME
			`).Scan(&nodeStatus)

			if err != nil {
				logger.Warning("Node role could not be determined: %v", err)

				// Fallback için alternatif sorgu
				err = db.QueryRow(`
					SELECT CASE 
						WHEN EXISTS (
							SELECT 1 FROM sys.dm_hadr_availability_replica_states ars
							WHERE ars.role_desc = 'SECONDARY'
						) THEN 'SECONDARY'
						ELSE 'UNKNOWN'
					END
				`).Scan(&nodeStatus)

				if err != nil {
					logger.Warning("Fallback node role query failed: %v", err)
					nodeStatus = "UNKNOWN"
				}
			}

			// Get AlwaysOn Availability Group name
			logger.Debug("AlwaysOn Availability Group ismi tespit ediliyor...")

			// Try to get AG name from availability groups
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
				logger.Warning("İlk metod ile AG ismi tespit edilemedi: %v", err)

				// Alternatif yöntem dene - daha basit sorgu
				err = db.QueryRow(`
					SELECT TOP 1 ag.name 
					FROM sys.availability_groups ag
					JOIN sys.dm_hadr_availability_replica_states ars 
						ON ag.group_id = ars.group_id
				`).Scan(&clusterName)

				if err != nil {
					logger.Warning("İkinci metod ile AG ismi tespit edilemedi: %v", err)

					// Son çare olarak "cluster_name" sistem bilgisini almayı dene
					err = db.QueryRow(`
						SELECT TOP 1 cluster_name 
						FROM sys.dm_hadr_cluster
					`).Scan(&clusterName)

					if err != nil {
						logger.Warning("Cluster name could not be determined: %v", err)
						// Önbellekte bilgi varsa, veritabanından bilgi alınamasa bile önbellekteki bilgiyi geri döndür
						haState, hasHAState := c.getHAStateFromCache("mssql_ha_state")
						if hasHAState && haState.group != "" {
							logger.Info("Veritabanından bilgi alınamadı, önbellekteki grup bilgisini kullanılıyor: %s", haState.group)
							clusterName = haState.group
						}
					}
				}
			}

			// Bilgileri önbelleğe kaydet
			if nodeStatus != "" && clusterName != "" {
				c.saveHAStateToCache(nodeStatus, clusterName)
			}
		} else {
			nodeStatus = "STANDALONE"
			// Önbellekteki bilgileri kontrol et (service down olsa bile)
			haState, hasHAState := c.getHAStateFromCache("mssql_ha_state")
			if hasHAState && haState.group != "" {
				clusterName = haState.group
				logger.Debug("Servis kapalı, önbellekteki cluster adı kullanılıyor: %s", clusterName)
				isHAEnabled = true
				nodeStatus = haState.role
			}
		}
		logger.Debug("HA durumu: %s, HA etkin: %v, Cluster adı: %s", nodeStatus, isHAEnabled, clusterName)

		// Get config path
		logger.Debug("Yapılandırma dosya yolu alınıyor...")
		err = db.QueryRow(`
			SELECT SERVERPROPERTY('ErrorLogFileName')
		`).Scan(&configPath)

		if err != nil || configPath == "" {
			logger.Warning("Error log path could not be determined: %v", err)

			// Try to find the SQL Server registry location
			if runtime.GOOS == "windows" {
				instance := c.cfg.MSSQL.Instance
				regPath := "MSSQL"
				if instance != "" {
					regPath = "MSSQL$" + instance
				}

				command := fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name SQLPath", regPath)
				output, err := utils.RunPowerShellCommand(command, 15) // 15 second timeout
				if err == nil {
					rePath := regexp.MustCompile(`SQLPath\s+:\s+(.+)`)
					match := rePath.FindStringSubmatch(output)
					if len(match) > 1 {
						configPath = filepath.Join(match[1], "MSSQL")
					}
				}
			}
		} else {
			// Return directory containing the error log
			configPath = filepath.Dir(configPath)
		}
		logger.Debug("Yapılandırma yolu: %s", configPath)
	}

	// MSSQL servis durumu - veritabanı kapalı olsa da kontrol et
	logger.Debug("SQL Server servis durumu kontrol ediliyor...")
	serviceStatus = c.GetMSSQLStatus()
	logger.Debug("SQL Server servis durumu: %s", serviceStatus)

	// System information - use optimized metrics collection
	logger.Debug("Sistem bilgileri alınıyor...")
	if runtime.GOOS == "windows" {
		// Try to get from cached metrics first
		metrics, err := utils.CollectWindowsMetrics()
		if err == nil && metrics != nil {
			totalvCpu = metrics.CPUCores
			totalMemory = metrics.MemoryTotal
			logger.Debug("Sistem bilgileri metrics: vCPU: %d, Toplam Bellek: %d bayt", totalvCpu, totalMemory)
		} else {
			// Fallback to traditional methods
			totalvCpu = c.getTotalvCpu()
			totalMemory = c.getTotalMemory()
			logger.Debug("Sistem bilgileri fallback: vCPU: %d, Toplam Bellek: %d bayt", totalvCpu, totalMemory)
		}
	} else {
		// Traditional methods for non-Windows
		totalvCpu = c.getTotalvCpu()
		totalMemory = c.getTotalMemory()
		logger.Debug("vCPU: %d, Toplam Bellek: %d bayt", totalvCpu, totalMemory)
	}

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

	logger.Info("MSSQL bilgileri toplandı: ClusterName=%s, NodeStatus=%s, Version=%s, HAEnabled=%v",
		info.ClusterName, info.NodeStatus, info.Version, info.IsHAEnabled)

	return info
}

// getLocalIPGo returns the local non-loopback IP address using Go standard library
func (c *MSSQLCollector) getLocalIPGo() string {
	// Try to get IPs by checking network interfaces directly
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	// Final fallback using UDP connection trick
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		return localAddr.IP.String()
	}

	return "127.0.0.1"
}

// getLocalIP returns the local non-loopback IP address
func (c *MSSQLCollector) getLocalIP() string {
	if runtime.GOOS == "windows" {
		// Use the optimized Windows utilities
		ip, err := utils.GetNetworkInfo(10) // 10 second timeout
		if err == nil && ip != "" {
			return ip
		}
		logger.Warning("Failed to get IP using optimized method: %v, trying fallback methods", err)
	}

	// Fallback to standard Go method
	return c.getLocalIPGo()
}

// formatBytes converts bytes to a human-readable format
func formatBytes(bytes float64) string {
	const unit = 1024.0
	if bytes < unit {
		return fmt.Sprintf("%.0f B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", bytes/div, "KMGTPE"[exp])
}

// getTotalvCpu returns the total number of vCPUs
func (c *MSSQLCollector) getTotalvCpu() int32 {
	// On Windows systems, use the optimized performance metrics collection
	if runtime.GOOS == "windows" {
		// Get metrics using the optimized Windows performance collector
		metrics, err := utils.CollectWindowsMetrics()
		if err == nil && metrics != nil {
			logger.Debug("CPU cores obtained from optimized Windows metrics: %d", metrics.CPUCores)
			return metrics.CPUCores
		}
		logger.Warning("Failed to get CPU count from optimized metrics, falling back to legacy method: %v", err)
	}

	// Original implementation as fallback
	var cpuCount int32
	if runtime.GOOS == "windows" {
		// For Windows, use Get-CimInstance for better performance
		output, err := utils.RunCimCommand("Win32_ComputerSystem", "NumberOfLogicalProcessors", "", 10)
		if err != nil {
			logger.Error("Failed to get CPU count: %v", err)
			return 0
		}
		count, err := strconv.Atoi(strings.TrimSpace(output))
		if err != nil {
			logger.Error("Failed to parse CPU count: %v", err)
			return 0
		}
		cpuCount = int32(count)
	} else {
		// For non-Windows systems
		count := runtime.NumCPU()
		cpuCount = int32(count)
	}

	return cpuCount
}

// getTotalMemory returns the total amount of RAM in bytes
func (c *MSSQLCollector) getTotalMemory() int64 {
	// On Windows systems, use the optimized performance metrics collection
	if runtime.GOOS == "windows" {
		// Get metrics using the optimized Windows performance collector
		metrics, err := utils.CollectWindowsMetrics()
		if err == nil && metrics != nil && metrics.MemoryTotal > 0 {
			logger.Debug("Total memory obtained from optimized Windows metrics: %d bytes", metrics.MemoryTotal)
			return metrics.MemoryTotal
		}
		logger.Warning("Failed to get memory from optimized metrics, falling back to legacy method: %v", err)

		// Try the efficient method directly
		total, _, err := utils.GetMemoryInfoEfficient()
		if err == nil && total > 0 {
			logger.Debug("Total memory obtained from efficient Windows method: %d bytes", total)
			return total
		}
		logger.Warning("Failed to get memory from efficient method: %v", err)
	}

	// Original implementation as fallback
	var memTotal int64
	if runtime.GOOS == "windows" {
		// For Windows, use Get-CimInstance for better performance
		output, err := utils.RunCimCommand("Win32_ComputerSystem", "TotalPhysicalMemory", "", 10)
		if err != nil {
			logger.Error("Failed to get total memory: %v", err)
			return 0
		}
		total, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
		if err != nil {
			logger.Error("Failed to parse total memory: %v", err)
			return 0
		}
		memTotal = total
	} else {
		// For non-Windows systems, use os/exec to run free command
		cmd := exec.Command("free", "-b")
		output, err := cmd.Output()
		if err != nil {
			logger.Error("Failed to get memory info: %v", err)
			return 0
		}

		lines := strings.Split(string(output), "\n")
		if len(lines) < 2 {
			logger.Error("Unexpected output format from free command")
			return 0
		}

		fields := strings.Fields(lines[1])
		if len(fields) < 2 {
			logger.Error("Unexpected fields count in free command output")
			return 0
		}

		total, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			logger.Error("Failed to parse memory value: %v", err)
			return 0
		}
		memTotal = total
	}

	return memTotal
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
			logger.Error("Sorgu tarama hatası: %v", err)
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
				// logger.Debug("Sistem veritabanı sorgusu atlanıyor: %s", dbName)
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

		logger.Warning("Yavaş sorgu tespit edildi! Süre: %.2f ms (Threshold: %d ms)", durationMs, thresholdMs)
	}

	if err = rows.Err(); err != nil {
		return slowQueries, fmt.Errorf("sorgu sonuçları okunurken hata: %v", err)
	}

	logger.Info("Toplam %d yavaş sorgu bulundu", totalSlowQueries)
	return slowQueries, nil
}

// ExplainMSSQLQuery returns the execution plan for a SQL Server query
func (c *MSSQLCollector) ExplainMSSQLQuery(database, queryStr string) (string, error) {
	// Connect to the database
	db, err := c.GetClient()
	if err != nil {
		logger.Error("MSSQL explain bağlantısı açılamadı: %v", err)
		return "", fmt.Errorf("MSSQL bağlantısı açılamadı: %v", err)
	}
	defer db.Close()

	logger.Debug("ExplainMSSQLQuery başlatılıyor. Veritabanı: %s, Sorgu Boyutu: %d bytes",
		database, len(queryStr))

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to use the specified database
	if database != "" && database != "master" {
		_, err = db.ExecContext(ctx, "USE "+database)
		if err != nil {
			logger.Error("Veritabanı değiştirilemedi %s: %v", database, err)
			return "", fmt.Errorf("veritabanı değiştirilemedi %s: %v", database, err)
		}
	}

	// Get the execution plan
	var planXML string
	err = db.QueryRowContext(ctx, "SET SHOWPLAN_XML ON; "+queryStr+"; SET SHOWPLAN_XML OFF;").Scan(&planXML)

	// If query doesn't return a resultset, try alternative approach
	if err != nil {
		logger.Warning("Execution plan için sorgu başarısız (direct): %v", err)
		logger.Debug("Alternatif execution plan yaklaşımı deneniyor...")

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
			logger.Warning("Temporary procedure silme hatası: %v", err)
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
			logger.Warning("Temporary procedure oluşturma hatası: %v", err)

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
		logger.Warning("Veritabanı bağlantısı kurulamadı: %v", err)
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

			command := fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name SQLPath | Select-Object -ExpandProperty SQLPath", regPath)
			output, err := utils.RunPowerShellCommand(command, 15) // 15 second timeout
			if err == nil {
				// Typical log directory based on SQL Server installation
				logDir = filepath.Join(strings.TrimSpace(output), "Log")
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
				logger.Error("File info error: %v", err)
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
