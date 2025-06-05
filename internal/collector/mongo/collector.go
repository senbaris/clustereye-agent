package mongo

import (
	"bufio"
	"context"
	"encoding/json"
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
	"sync"
	"time"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Global MongoDB collector instance management
var (
	globalMongoCollector *MongoCollector
	mongoCollectorMutex  sync.RWMutex
	mongoCollectorOnce   sync.Once
)

// GetDefaultMongoCollector returns the global MongoDB collector instance
func GetDefaultMongoCollector() *MongoCollector {
	mongoCollectorMutex.RLock()
	defer mongoCollectorMutex.RUnlock()
	return globalMongoCollector
}

// UpdateDefaultMongoCollector creates or updates the global MongoDB collector instance
func UpdateDefaultMongoCollector(cfg *config.AgentConfig) {
	mongoCollectorOnce.Do(func() {
		mongoCollectorMutex.Lock()
		defer mongoCollectorMutex.Unlock()

		log.Printf("Creating global MongoDB collector instance")
		globalMongoCollector = NewMongoCollector(cfg)

		// Perform startup recovery only once
		go func() {
			time.Sleep(2 * time.Second) // Brief delay to avoid startup race conditions
			globalMongoCollector.StartupRecovery()
		}()

		log.Printf("Global MongoDB collector instance created and startup recovery initiated")
	})
}

// MongoCollector mongodb için veri toplama yapısı
type MongoCollector struct {
	cfg                *config.AgentConfig
	lastCollectionTime time.Time
	collectionInterval time.Duration
	maxRetries         int
	backoffDuration    time.Duration
	isHealthy          bool
	lastHealthCheck    time.Time
	startupTime        time.Time     // Track when collector was created
	startupGracePeriod time.Duration // Grace period for startup collections
}

// NewMongoCollector yeni bir MongoCollector oluşturur
func NewMongoCollector(cfg *config.AgentConfig) *MongoCollector {
	return &MongoCollector{
		cfg:                cfg,
		collectionInterval: 30 * time.Second, // Minimum 30 seconds between collections
		maxRetries:         3,
		backoffDuration:    5 * time.Second,
		isHealthy:          true,
		lastHealthCheck:    time.Now(),
		startupTime:        time.Now(),
		startupGracePeriod: 10 * time.Second, // Default to 10 seconds
	}
}

// MongoDB bilgilerini içeren yapı
type MongoInfo struct {
	ClusterName    string
	IP             string
	Hostname       string
	NodeStatus     string
	MongoVersion   string
	Location       string
	MongoStatus    string
	ReplicaSetName string
	ReplicaLagSec  int64
	FreeDisk       string
	FdPercent      int32
	Port           string // MongoDB port bilgisi
	TotalvCpu      int32  // Toplam vCPU sayısı
	TotalMemory    int64  // Toplam RAM miktarı (byte cinsinden)
	ConfigPath     string // MongoDB configuration file path
}

// MongoServiceStatus MongoDB servisinin durumunu ve detaylarını içeren yapı
type MongoServiceStatus struct {
	Status       string // RUNNING, FAIL!, DEGRADED
	IsReplSet    bool   // Replica set mi?
	CurrentState string // PRIMARY, SECONDARY, ARBITER, STANDALONE
	LastState    string // Önceki durum
	ErrorMessage string // Hata mesajı (varsa)
}

// OpenDB MongoDB bağlantısını açar
func (c *MongoCollector) OpenDB() (*mongo.Client, error) {
	// Check rate limiting first
	if c.ShouldSkipCollection() {
		return nil, fmt.Errorf("MongoDB collection rate limited or collector unhealthy")
	}

	// Update collection time
	c.SetCollectionTime()

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in MongoDB OpenDB: %v", r)
			// Mark as unhealthy after panic
			c.isHealthy = false
			c.collectionInterval = 2 * time.Minute
		}
	}()

	// MongoDB URI oluştur
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true&connectTimeoutMS=10000&serverSelectionTimeoutMS=10000&maxPoolSize=2",
		c.cfg.Mongo.User,
		c.cfg.Mongo.Pass,
		c.cfg.Mongo.Host,
		c.cfg.Mongo.Port,
	)

	// Auth bilgileri boşsa, kimlik doğrulama olmadan bağlan
	if !c.cfg.Mongo.Auth {
		uri = fmt.Sprintf("mongodb://%s:%s/?directConnection=true&connectTimeoutMS=10000&serverSelectionTimeoutMS=10000&maxPoolSize=2",
			c.cfg.Mongo.Host, c.cfg.Mongo.Port)
	}

	clientOptions := options.Client().ApplyURI(uri)

	// Set connection pool limits to prevent resource exhaustion
	clientOptions.SetMaxPoolSize(2)                           // Maximum 2 concurrent connections
	clientOptions.SetMinPoolSize(1)                           // Keep minimum 1 connection
	clientOptions.SetMaxConnIdleTime(30 * time.Second)        // Close idle connections after 30 seconds
	clientOptions.SetConnectTimeout(10 * time.Second)         // Connection timeout
	clientOptions.SetServerSelectionTimeout(10 * time.Second) // Server selection timeout

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("MongoDB bağlantısı kurulamadı: %v", err)
	}

	// Test the connection with a shorter timeout
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("MongoDB ping başarısız: %v", err)
	}

	return client, nil
}

// GetMongoStatus checks if MongoDB service is running by checking if the configured host:port is accessible
func (c *MongoCollector) GetMongoStatus() string {
	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetMongoStatus: %v", r)
			// Log detailed stack trace
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger.Error("MongoDB GetMongoStatus STACK TRACE: %s", string(buf[:n]))

			// Mark as unhealthy after panic
			if c != nil {
				c.isHealthy = false
				c.collectionInterval = 2 * time.Minute
			}
		}
	}()

	// Check rate limiting
	if c.ShouldSkipCollection() {
		logger.Debug("MongoDB GetMongoStatus: Rate limited, returning")
		return "RATE_LIMITED"
	}

	// Get MongoDB connection details from config
	host := c.cfg.Mongo.Host
	if host == "" {
		host = "localhost"
	}

	port := c.cfg.Mongo.Port
	if port == "" {
		port = "27017" // default MongoDB port
	}

	// Try to establish a TCP connection to check if the port is listening
	address := fmt.Sprintf("%s:%s", host, port)
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		logger.Warning("MongoDB at %s is not accessible: %v", address, err)
		return "FAIL!"
	}
	if conn != nil {
		conn.Close()
	}

	logger.Debug("MongoDB service at %s is running", address)
	return "RUNNING"
}

// GetMongoVersion MongoDB versiyonunu döndürür
func (c *MongoCollector) GetMongoVersion() string {
	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetMongoVersion: %v", r)
			// Log detailed stack trace
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger.Error("MongoDB GetMongoVersion STACK TRACE: %s", string(buf[:n]))

			// Mark as unhealthy after panic
			if c != nil {
				c.isHealthy = false
				c.collectionInterval = 2 * time.Minute
			}
		}
	}()

	// Check rate limiting
	if c.ShouldSkipCollection() {
		logger.Debug("MongoDB GetMongoVersion: Rate limited, returning")
		return "Rate Limited"
	}

	logger.Debug("MongoDB GetMongoVersion: Getting version info")
	client, err := c.openDBDirect() // Use direct connection for version check
	if err != nil {
		logger.Error("MongoDB GetMongoVersion: Connection failed: %v", err)
		return "Unknown"
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("MongoDB GetMongoVersion: Error closing connection: %v", err)
		}
	}()

	// BuildInfo'yu çalıştır
	logger.Debug("MongoDB GetMongoVersion: Running buildInfo command")
	buildInfo, err := c.runAdminCommand(client, bson.D{{Key: "buildInfo", Value: 1}})
	if err != nil {
		logger.Error("MongoDB GetMongoVersion: buildInfo command failed: %v", err)
		return "Unknown"
	}

	// Versiyon bilgisini al
	version, ok := buildInfo["version"].(string)
	if !ok {
		logger.Error("MongoDB GetMongoVersion: version field is not a string")
		return "Unknown"
	}

	logger.Debug("MongoDB GetMongoVersion: Found version: %s", version)
	return version
}

// GetNodeStatus MongoDB node'un durumunu döndürür (PRIMARY, SECONDARY, ARBITER vs.)
func (c *MongoCollector) GetNodeStatus() string {
	// Comprehensive debug logging
	logger.Debug("MongoDB GetNodeStatus: Starting function")

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in MongoDB GetNodeStatus: %v", r)
			// Log detailed stack trace
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger.Error("MongoDB GetNodeStatus STACK TRACE: %s", string(buf[:n]))

			// Mark as unhealthy after panic
			if c != nil {
				c.isHealthy = false
				c.collectionInterval = 2 * time.Minute
				logger.Error("MongoDB GetNodeStatus: Marked collector as unhealthy after panic")
			}
		}
	}()

	// Check rate limiting
	if c.ShouldSkipCollection() {
		logger.Debug("MongoDB GetNodeStatus: Rate limited, returning")
		return "Rate Limited"
	}
	logger.Debug("MongoDB GetNodeStatus: Rate limiting check passed")

	logger.Debug("MongoDB GetNodeStatus: About to open database connection")
	client, err := c.OpenDB()
	if err != nil {
		logger.Error("MongoDB GetNodeStatus: Database connection failed: %v", err)
		return "Unknown"
	}
	if client == nil {
		logger.Error("MongoDB GetNodeStatus: Database client is nil")
		return "Unknown"
	}
	defer func() {
		logger.Debug("MongoDB GetNodeStatus: Closing database connection")
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("MongoDB GetNodeStatus: Error closing connection: %v", err)
		}
	}()
	logger.Debug("MongoDB GetNodeStatus: Database connection established")

	// İlk olarak serverStatus komutunu çalıştır - bu bize bağlı olduğumuz node'un bilgisini verecek
	logger.Debug("MongoDB GetNodeStatus: Running serverStatus command")
	serverStatus, err := c.runAdminCommand(client, bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		logger.Error("MongoDB GetNodeStatus: serverStatus command failed: %v", err)
		return "Unknown"
	}
	logger.Debug("MongoDB GetNodeStatus: serverStatus command completed")

	// replSetGetStatus komutunu çalıştır
	logger.Debug("MongoDB GetNodeStatus: Running replSetGetStatus command")
	status, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		logger.Warning("MongoDB GetNodeStatus: replSetGetStatus command failed: %v", err)
		logger.Debug("MongoDB GetNodeStatus: Returning STANDALONE")
		return "STANDALONE"
	}
	logger.Debug("MongoDB GetNodeStatus: replSetGetStatus command completed")

	// Şimdiki bağlantının MongoDB instance bilgilerini alıyoruz
	process, ok := serverStatus["process"].(string)
	if !ok {
		logger.Warning("MongoDB GetNodeStatus: process info not found")
		process = "mongod"
	}

	host, ok := serverStatus["host"].(string)
	if !ok {
		logger.Warning("MongoDB GetNodeStatus: host info not found")
		host = "localhost"
	}
	logger.Debug("MongoDB GetNodeStatus: Current host: %s, process: %s", host, process)

	// myState değerini doğrudan almaya çalış
	if replication, ok := status["myState"]; ok {
		state, ok := replication.(int32)
		if ok {
			logger.Debug("MongoDB GetNodeStatus: Found myState: %d", state)
			switch state {
			case 1:
				logger.Debug("MongoDB GetNodeStatus: Returning PRIMARY")
				return "PRIMARY"
			case 2:
				logger.Debug("MongoDB GetNodeStatus: Returning SECONDARY")
				return "SECONDARY"
			case 7:
				logger.Debug("MongoDB GetNodeStatus: Returning ARBITER")
				return "ARBITER"
			default:
				stateStr := fmt.Sprintf("STATE_%d", state)
				logger.Debug("MongoDB GetNodeStatus: Returning %s", stateStr)
				return stateStr
			}
		}
	}

	// myState yoksa members içinde arama yapalım
	logger.Debug("MongoDB GetNodeStatus: myState not found, searching in members")
	members, ok := status["members"].(bson.A)
	if !ok {
		logger.Warning("MongoDB GetNodeStatus: members array not found")
		return "Unknown"
	}

	// Process ID ve host bilgisine göre kendimizi bulalım
	currentHost := fmt.Sprintf("%s", host)
	logger.Debug("MongoDB GetNodeStatus: Searching for current host: %s in members", currentHost)

	// Tüm member'ları dönerek durumu bulalım
	for i, member := range members {
		logger.Debug("MongoDB GetNodeStatus: Checking member %d", i)
		memberDoc, ok := member.(bson.D)
		if !ok {
			logger.Debug("MongoDB GetNodeStatus: Member %d is not a document", i)
			continue
		}
		memberMap := memberDoc.Map()

		// Host bilgisini kontrol et
		memberHost, ok := memberMap["name"].(string)
		if !ok {
			logger.Debug("MongoDB GetNodeStatus: Member %d has no name field", i)
			continue
		}
		logger.Debug("MongoDB GetNodeStatus: Member %d host: %s", i, memberHost)

		// StateStr varsa direkt onu kullan
		if stateStr, ok := memberMap["stateStr"].(string); ok {
			logger.Debug("MongoDB GetNodeStatus: Member %s state: %s", memberHost, stateStr)
		}

		// Kendimizi kontrol et (host name'e göre)
		if strings.Contains(memberHost, currentHost) || strings.Contains(currentHost, memberHost) {
			logger.Debug("MongoDB GetNodeStatus: Found current member: %s", memberHost)

			// StateStr varsa direkt onu kullan
			if stateStr, ok := memberMap["stateStr"].(string); ok {
				logger.Debug("MongoDB GetNodeStatus: Returning stateStr: %s", stateStr)
				return stateStr
			}

			// State değerine göre durum belirle
			state, ok := memberMap["state"].(int32)
			if !ok {
				logger.Debug("MongoDB GetNodeStatus: Member has no state field")
				continue
			}

			logger.Debug("MongoDB GetNodeStatus: Found state: %d", state)
			switch state {
			case 1:
				logger.Debug("MongoDB GetNodeStatus: Returning PRIMARY")
				return "PRIMARY"
			case 2:
				logger.Debug("MongoDB GetNodeStatus: Returning SECONDARY")
				return "SECONDARY"
			case 7:
				logger.Debug("MongoDB GetNodeStatus: Returning ARBITER")
				return "ARBITER"
			default:
				stateStr := fmt.Sprintf("STATE_%d", state)
				logger.Debug("MongoDB GetNodeStatus: Returning %s", stateStr)
				return stateStr
			}
		}
	}

	// Son çare - isMaster komutu ile durumu kontrol et
	logger.Debug("MongoDB GetNodeStatus: Trying isMaster command as fallback")
	isMaster, err := c.runAdminCommand(client, bson.D{{Key: "isMaster", Value: 1}})
	if err == nil {
		if ismaster, ok := isMaster["ismaster"].(bool); ok && ismaster {
			logger.Debug("MongoDB GetNodeStatus: isMaster returned PRIMARY")
			return "PRIMARY"
		}
		if secondary, ok := isMaster["secondary"].(bool); ok && secondary {
			logger.Debug("MongoDB GetNodeStatus: isMaster returned SECONDARY")
			return "SECONDARY"
		}
		if arbiterOnly, ok := isMaster["arbiterOnly"].(bool); ok && arbiterOnly {
			logger.Debug("MongoDB GetNodeStatus: isMaster returned ARBITER")
			return "ARBITER"
		}
	} else {
		logger.Error("MongoDB GetNodeStatus: isMaster command failed: %v", err)
	}

	logger.Debug("MongoDB GetNodeStatus: Returning Unknown")
	return "Unknown"
}

// GetReplicaSetName replica set adını döndürür
func (c *MongoCollector) GetReplicaSetName() string {
	client, err := c.OpenDB()
	if err != nil {
		logger.Error("MongoDB bağlantısı kurulamadı: %v", err)
		return "Unknown"
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("MongoDB bağlantısı kapatılamadı: %v", err)
		}
	}()

	// replSetGetStatus komutunu çalıştır
	status, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		logger.Error("replSetGetStatus komutu çalıştırılamadı: %v", err)
		return "Unknown"
	}

	// Replica set adını al
	replSetName, ok := status["set"].(string)
	if !ok {
		logger.Warning("replica set adı bulunamadı")
		return "Unknown"
	}

	return replSetName
}

// GetReplicationLagSec replication lag'i saniye cinsinden döndürür
func (c *MongoCollector) GetReplicationLagSec() int64 {
	// Eğer PRIMARY ise lag 0'dır
	nodeStatus := c.GetNodeStatus()
	if nodeStatus == "PRIMARY" {
		return 0
	}

	client, err := c.OpenDB()
	if err != nil {
		logger.Error("MongoDB bağlantısı kurulamadı: %v", err)
		return 0
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("MongoDB bağlantısı kapatılamadı: %v", err)
		}
	}()

	// Önce serverStatus ile mevcut node bilgilerini al
	serverStatus, err := c.runAdminCommand(client, bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		logger.Error("serverStatus komutu çalıştırılamadı: %v", err)
		return 0
	}

	host, ok := serverStatus["host"].(string)
	if !ok {
		logger.Warning("Host bilgisi bulunamadı")
		host = "localhost"
	}

	// replSetGetStatus komutunu çalıştır
	status, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		logger.Error("replSetGetStatus komutu çalıştırılamadı: %v", err)
		return 0
	}

	// Member'ları kontrol et
	members, ok := status["members"].(bson.A)
	if !ok {
		logger.Warning("members dizisi bulunamadı")
		return 0
	}

	// Kendi node'umu ve PRIMARY node'u bul
	var myOptimeDate time.Time
	var primaryOptimeDate time.Time
	var foundMe, foundPrimary bool
	currentHost := fmt.Sprintf("%s", host)

	for _, member := range members {
		memberDoc, ok := member.(bson.D)
		if !ok {
			continue
		}
		memberMap := memberDoc.Map()

		// Host bilgisini kontrol et
		memberHost, ok := memberMap["name"].(string)
		if !ok {
			continue
		}

		// State değerini kontrol et
		state, ok := memberMap["state"].(int32)
		if !ok {
			state = 0
		}

		// stateStr da kontrol edebiliriz
		stateStr, hasStateStr := memberMap["stateStr"].(string)

		// optime değerini al
		optime, ok := memberMap["optimeDate"].(time.Time)
		if !ok {
			logger.Warning("Node %s için optimeDate bulunamadı", memberHost)
			continue
		}

		// Kendi node'um mu? (host name'e göre)
		if strings.Contains(memberHost, currentHost) || strings.Contains(currentHost, memberHost) {
			myOptimeDate = optime
			foundMe = true
			logger.Info("Kendi node'umu buldum: %s, optimeDate: %v", memberHost, optime)
		}

		// PRIMARY node mu?
		isPrimary := state == 1
		if hasStateStr {
			isPrimary = isPrimary || stateStr == "PRIMARY"
		}

		if isPrimary {
			primaryOptimeDate = optime
			foundPrimary = true
			logger.Info("PRIMARY node'u buldum: %s, optimeDate: %v", memberHost, optime)
		}
	}

	// Eğer gerekli bilgiler bulunamadıysa 0 döndür
	if !foundMe || !foundPrimary {
		logger.Debug("Lag hesaplanamadı: foundMe=%v, foundPrimary=%v", foundMe, foundPrimary)
		return 0
	}

	// Lag'i hesapla (saniye cinsinden)
	lag := primaryOptimeDate.Sub(myOptimeDate).Seconds()
	if lag < 0 {
		lag = 0 // Negatif lag olamaz
	}

	logger.Debug("Replikasyon lag: %.2f saniye", lag)
	return int64(lag)
}

// GetDiskUsage disk kullanım bilgilerini döndürür: (totalSize, freeSize, usagePercent)
func (c *MongoCollector) GetDiskUsage() (string, string, int) {

	// df komutunu çalıştır
	cmd := exec.Command("df", "-h")
	out, err := cmd.Output()
	if err != nil {
		logger.Warning("Disk bilgileri alınamadı: %v", err)
		return "N/A", "N/A", 0
	}

	// Çıktıyı satırlara böl
	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return "N/A", "N/A", 0
	}

	var maxSize uint64 = 0
	var selectedTotal, selectedFree string
	var selectedUsage int
	var selectedFilesystem, selectedMountPoint string

	// Her satırı işle (başlık satırını atla)
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}

		filesystem := fields[0]
		mountPoint := fields[5]

		// Geçici dosya sistemlerini ve özel bölümleri atla
		if strings.HasPrefix(filesystem, "tmpfs") ||
			strings.HasPrefix(filesystem, "devtmpfs") ||
			strings.HasPrefix(filesystem, "efivarfs") ||
			strings.Contains(mountPoint, "/boot") ||
			strings.Contains(mountPoint, "/run") ||
			strings.Contains(mountPoint, "/dev") {
			logger.Debug("MongoDB GetDiskUsage - Skipping: %s (temporary/special filesystem)", filesystem)
			continue
		}

		// Boyut bilgisini parse et
		size := fields[1]
		free := fields[3]
		usage := strings.TrimSuffix(fields[4], "%")

		// Boyutu byte cinsine çevir
		sizeInBytes, err := c.convertToBytes(size)
		if err != nil {
			logger.Debug("MongoDB GetDiskUsage - convertToBytes error for %s (%s): %v", filesystem, size, err)
			continue
		}

		// En büyük diski veya root dizinini seç
		if sizeInBytes > maxSize || mountPoint == "/" {
			maxSize = sizeInBytes
			selectedTotal = size
			selectedFree = free
			selectedUsage, _ = strconv.Atoi(usage)
			selectedFilesystem = filesystem
			selectedMountPoint = mountPoint
			logger.Debug("MongoDB GetDiskUsage - SELECTED: %s -> %s (Size: %s, Free: %s, Usage: %s%%)",
				selectedFilesystem, selectedMountPoint, selectedTotal, selectedFree, usage)
		}
	}

	if maxSize == 0 {
		logger.Debug("MongoDB GetDiskUsage - Uygun disk bulunamadı, N/A döndürülüyor")
		return "N/A", "N/A", 0
	}

	logger.Debug("MongoDB GetDiskUsage - FINAL RESULT: Filesystem=%s, MountPoint=%s, Total=%s, Free=%s, Usage=%d%%",
		selectedFilesystem, selectedMountPoint, selectedTotal, selectedFree, selectedUsage)
	return selectedTotal, selectedFree, selectedUsage
}

// convertToBytes boyut string'ini (1K, 1M, 1G gibi) byte cinsine çevirir
func (c *MongoCollector) convertToBytes(size string) (uint64, error) {

	size = strings.TrimSpace(size)
	if len(size) == 0 {
		return 0, fmt.Errorf("empty size")
	}

	// Sayısal kısmı ve birimi ayır
	var num float64
	var unit string
	last := size[len(size)-1]
	if last >= '0' && last <= '9' {
		num, _ = strconv.ParseFloat(size, 64)
		unit = "B"
	} else {
		num, _ = strconv.ParseFloat(size[:len(size)-1], 64)
		unit = strings.ToUpper(size[len(size)-1:])
	}

	// Birimi byte cinsine çevir
	multiplier := uint64(1)
	switch unit {
	case "K":
		multiplier = 1024
	case "M":
		multiplier = 1024 * 1024
	case "G":
		multiplier = 1024 * 1024 * 1024
	case "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "P":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024
	}

	result := uint64(num * float64(multiplier))

	return result, nil
}

// getTotalvCpu sistemdeki toplam vCPU sayısını döndürür
func (c *MongoCollector) getTotalvCpu() int32 {
	// UNIX/Linux sistemlerde nproc veya lscpu komutu kullanılabilir
	cmd := exec.Command("sh", "-c", "nproc")
	out, err := cmd.Output()
	if err != nil {
		// nproc çalışmadıysa, lscpu dene
		cmd = exec.Command("sh", "-c", "lscpu | grep 'CPU(s):' | head -n 1 | awk '{print $2}'")
		out, err = cmd.Output()
		if err != nil {
			logger.Warning("MongoDB getTotalvCpu: vCPU sayısı alınamadı: %v", err)
			return int32(1) // Default to 1 instead of 0
		}
	}

	// Çıktıyı int32'ye çevir
	cpuCount, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 32)
	if err != nil {
		logger.Warning("MongoDB getTotalvCpu: vCPU sayısı parse edilemedi: %v", err)
		return int32(1) // Default to 1 instead of 0
	}

	// Ensure result is valid and within reasonable range
	if cpuCount <= 0 {
		logger.Warning("MongoDB getTotalvCpu: Invalid CPU count detected: %d, using default", cpuCount)
		return int32(1)
	}
	if cpuCount > 256 { // Reasonable upper limit
		logger.Warning("MongoDB getTotalvCpu: CPU count seems too high: %d, capping at 256", cpuCount)
		return int32(256)
	}

	return int32(cpuCount)
}

// getTotalMemory sistemdeki toplam RAM miktarını byte cinsinden döndürür
func (c *MongoCollector) getTotalMemory() int64 {
	// Linux sistemlerde /proc/meminfo dosyasından MemTotal değerini okuyabiliriz
	cmd := exec.Command("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'")
	out, err := cmd.Output()
	if err != nil {
		// Alternatif olarak free komutu deneyelim
		cmd = exec.Command("sh", "-c", "free -b | grep 'Mem:' | awk '{print $2}'")
		out, err = cmd.Output()
		if err != nil {
			logger.Warning("MongoDB getTotalMemory: Toplam RAM miktarı alınamadı: %v", err)
			return int64(8 * 1024 * 1024 * 1024) // Default to 8GB
		}
	}

	// Parse as int64 to avoid scientific notation
	memTotal, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		logger.Warning("MongoDB getTotalMemory: Toplam RAM miktarı parse edilemedi: %v", err)
		return int64(8 * 1024 * 1024 * 1024) // Default to 8GB
	}

	// grep MemTotal kullanıldıysa KB cinsinden, bunu byte'a çevir
	if strings.Contains(cmd.String(), "MemTotal") {
		memTotal *= 1024
	}

	// Ensure result is valid and within reasonable range
	if memTotal <= 0 {
		logger.Warning("MongoDB getTotalMemory: Invalid memory size detected: %d, using default 8GB", memTotal)
		return int64(8 * 1024 * 1024 * 1024)
	}

	// Reasonable upper limit check (1TB)
	maxMemory := int64(1024 * 1024 * 1024 * 1024)
	if memTotal > maxMemory {
		logger.Warning("MongoDB getTotalMemory: Memory size seems too high: %d, capping at 1TB", memTotal)
		return maxMemory
	}

	// Log memory value to verify no scientific notation
	logger.Debug("MONGO MEMORY COLLECTED - Value: %d bytes (format check: %d)",
		memTotal, memTotal)

	return memTotal
}

// GetMongoInfo MongoDB bilgilerini toplar
func (c *MongoCollector) GetMongoInfo() *MongoInfo {
	// Check if we should skip collection due to rate limiting or health issues
	if c.ShouldSkipCollection() {
		logger.Debug("MongoDB collection skipped due to rate limiting or health checks")
		return c.getLastKnownGoodInfo() // Return cached info instead
	}

	// Update collection time at the start to prevent concurrent collections
	c.SetCollectionTime()

	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in GetMongoInfo: %v", r)
			// Log call stack for better debugging
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger.Error("MongoDB GetMongoInfo STACK: %s", string(buf[:n]))

			// Mark as unhealthy after panic
			c.isHealthy = false
			c.collectionInterval = 2 * time.Minute // Increase interval significantly
		}
	}()

	logger.Info("MongoDB bilgileri toplanmaya başlanıyor...")

	hostname, _ := os.Hostname()
	ip := c.getLocalIP()
	_, freeDisk, usagePercent := c.GetDiskUsage()

	// Get raw values first and normalize them
	rawTotalvCpu := c.getTotalvCpu()
	rawTotalMemory := c.getTotalMemory()

	// Ensure consistent formatting to prevent false change detection
	// API compares values as strings, so we need to avoid scientific notation
	normalizedCpu := rawTotalvCpu
	if normalizedCpu <= 0 {
		normalizedCpu = 1
	}

	normalizedMemory := rawTotalMemory
	if normalizedMemory < 0 {
		normalizedMemory = 0
	}

	configPath := findMongoConfigFile()

	info := &MongoInfo{
		ClusterName:    c.getConfigValue("Mongo.Cluster"),
		IP:             ip,
		Hostname:       hostname,
		NodeStatus:     c.GetNodeStatus(),
		MongoVersion:   c.GetMongoVersion(),
		Location:       c.getConfigValue("Mongo.Location"),
		MongoStatus:    c.GetMongoStatus(),
		ReplicaSetName: c.GetReplicaSetName(),
		ReplicaLagSec:  c.GetReplicationLagSec(),
		FreeDisk:       freeDisk,
		FdPercent:      int32(usagePercent),
		Port:           c.cfg.Mongo.Port, // MongoDB port bilgisini config'den alıyoruz
		TotalvCpu:      normalizedCpu,    // Use normalized value
		TotalMemory:    normalizedMemory, // Use normalized value
		ConfigPath:     configPath,
	}

	// Enhanced debug logging with format consistency check
	logger.Debug("MONGO VALUES (NORMALIZED) - TotalMemory: %d, TotalvCpu: %d",
		info.TotalMemory, info.TotalvCpu)

	// Additional format verification log
	logger.Debug("MONGO MEMORY FORMAT CHECK - Value: %d, Scientific: %e",
		normalizedMemory, float64(normalizedMemory))

	logger.Info("MongoDB bilgileri başarıyla toplandı - Port: %s, Status: %s, Version: %s, vCPU: %d, Memory: %d, ConfigPath: %s",
		info.Port, info.MongoStatus, info.MongoVersion, info.TotalvCpu, info.TotalMemory, info.ConfigPath)

	return info
}

// getLocalIP yerel IP adresini döndürür
func (c *MongoCollector) getLocalIP() string {
	cmd := exec.Command("sh", "-c", "hostname -I | awk '{print $1}'")
	out, err := cmd.Output()
	if err != nil {
		logger.Warning("MongoDB getLocalIP: IP adresi alınamadı: %v", err)
		return "Unknown"
	}
	return strings.TrimSpace(string(out))
}

// getConfigValue belirtilen yapılandırma değerini döndürür
func (c *MongoCollector) getConfigValue(key string) string {
	// Key'e göre değeri döndür
	switch key {
	case "Mongo.Cluster":
		// Mongo.Cluster şimdilik yapılandırmada yok, replica set adını kullan
		return c.cfg.Mongo.Replset
	case "Mongo.Location":
		// Mongo.Location şimdilik yapılandırmada var
		return c.cfg.Mongo.Location
	default:
		return ""
	}
}

// getFdPercent dosya tanımlayıcı kullanım yüzdesini döndürür
func (c *MongoCollector) getFdPercent() int {
	// Linux sistemlerde /proc/sys/fs/file-nr dosyasından alınabilir
	// MacOS veya diğer sistemlerde farklı bir yöntem gerekebilir
	cmd := exec.Command("sh", "-c", "lsof | wc -l")
	out, err := cmd.Output()
	if err != nil {
		logger.Warning("Dosya tanımlayıcı sayısı alınamadı: %v", err)
		return 0
	}

	fdCount, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		logger.Warning("Dosya tanımlayıcı sayısı dönüştürülemedi: %v", err)
		return 0
	}

	// Varsayılan olarak sistemin maksimum fd sayısı 10240 kabul edildi
	// Gerçek değer için sysctl fs.file-max veya ulimit -n kullanılabilir
	const maxFd = 10240
	fdPercent := (fdCount * 100) / maxFd

	return fdPercent
}

// runAdminCommand admin veritabanında bir komut çalıştırır ve sonucunu döndürür
func (c *MongoCollector) runAdminCommand(client *mongo.Client, command interface{}) (bson.M, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Admin veritabanını seç
	adminDB := client.Database("admin")

	// Komutu çalıştır
	var result bson.M
	err := adminDB.RunCommand(ctx, command).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ToProto MongoInfo'yu proto mesajına dönüştürür
func (m *MongoInfo) ToProto() *pb.MongoInfo {
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

	// Normalize ReplicationLagSec
	normalizedLag := m.ReplicaLagSec
	if normalizedLag < 0 {
		normalizedLag = 0
	}

	proto := &pb.MongoInfo{
		ClusterName:       strings.TrimSpace(m.ClusterName),
		Ip:                strings.TrimSpace(m.IP),
		Hostname:          strings.TrimSpace(m.Hostname),
		NodeStatus:        strings.TrimSpace(m.NodeStatus),
		MongoVersion:      strings.TrimSpace(m.MongoVersion),
		Location:          strings.TrimSpace(m.Location),
		MongoStatus:       strings.TrimSpace(m.MongoStatus),
		ReplicaSetName:    strings.TrimSpace(m.ReplicaSetName),
		ReplicationLagSec: normalizedLag,
		FreeDisk:          strings.TrimSpace(m.FreeDisk),
		FdPercent:         normalizedFdPercent,
		Port:              strings.TrimSpace(m.Port),
		TotalVcpu:         normalizedCpu,
		TotalMemory:       normalizedMemory,
		ConfigPath:        strings.TrimSpace(m.ConfigPath),
	}

	// Enhanced debug logging with format consistency check
	logger.Debug("MONGO PROTOBUF VALUES (NORMALIZED) - TotalMemory: %d, TotalVcpu: %d, FdPercent: %d",
		proto.TotalMemory, proto.TotalVcpu, proto.FdPercent)

	// Additional format verification log
	logger.Debug("MONGO MEMORY FORMAT CHECK - Value: %d, Scientific: %e",
		normalizedMemory, float64(normalizedMemory))

	return proto
}

// FindMongoLogFiles MongoDB log dosyalarını bulur ve listeler
func (c *MongoCollector) FindMongoLogFiles(logPath string) ([]*pb.MongoLogFile, error) {
	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in FindMongoLogFiles: %v", r)
			// Mark as unhealthy after panic
			if c != nil {
				c.isHealthy = false
				c.collectionInterval = 2 * time.Minute
			}
		}
	}()

	// MongoDB çalışıyor mu kontrol et
	isMongoRunning := isMongoDBRunning()
	if !isMongoRunning {
		return nil, fmt.Errorf("MongoDB servisi çalışmıyor, log dosyaları listelenemedi")
	}

	// Eğer logPath belirtilmemişse, varsayılan olarak bilinen lokasyonları kontrol et
	if logPath == "" {
		// MongoDB çalışma parametrelerinden log dizinini bulmaya çalış
		logPathFromProcess := findMongoDBLogPathFromProcess()
		if logPathFromProcess != "" {
			// Log dosyası yolunun var olduğunu kontrol et
			stat, err := os.Stat(logPathFromProcess)
			if err == nil {
				if !stat.IsDir() {
					// Dosyanın bulunduğu dizini belirle
					parentDir := filepath.Dir(logPathFromProcess)
					logger.Debug("MongoDB log dosyası bağlamından log dizini belirlendi: %s", parentDir)
					// Dosya yerine dizini kullan
					logPath = parentDir
				} else {
					// Path zaten bir dizin
					logPath = logPathFromProcess
					logger.Debug("MongoDB log dizini süreç parametrelerinden bulundu: %s", logPath)
				}
			}
		}

		if logPath == "" {
			// Bilinen olası MongoDB log dizinleri
			logDirs := []string{
				"/var/log/mongodb",
				"/var/log/mongo",
				"/opt/mongodb/logs",
				"/data/db/log",
				"/usr/local/var/log/mongodb", // macOS Homebrew
				"/usr/local/mongodb/log",
				"/usr/local/var/mongodb/log",
				"/usr/local/Cellar/mongodb/*/log",                   // macOS Homebrew
				"/opt/homebrew/var/log/mongodb",                     // macOS Homebrew Apple Silicon
				"/opt/homebrew/var/mongodb",                         // macOS Homebrew Apple Silicon
				fmt.Sprintf("/tmp/mongodb-%s.*", os.Getenv("USER")), // Bazı sistemlerde tmp dizini
			}

			// MongoDB konfigürasyon dosyasını bulmayı dene
			configFile := findMongoConfigFile()
			if configFile != "" {
				// Konfigürasyondan log path'i oku
				if path := getLogPathFromConfig(configFile); path != "" {
					// Log path bir dosya ise, dizinini al
					stat, err := os.Stat(path)
					if err == nil && !stat.IsDir() {
						path = filepath.Dir(path)
					}
					logDirs = append([]string{path}, logDirs...)
					logger.Debug("MongoDB log dizini konfigürasyon dosyasından bulundu: %s", path)
				}
			}

			// MongoDB'nin çalıştığı dizini bulmayı dene
			mongoDataDir := findMongoDBDataDir()
			if mongoDataDir != "" {
				// Data dizinindeki log dizinini kontrol et
				logDirs = append([]string{mongoDataDir + "/log", mongoDataDir}, logDirs...)
				logger.Debug("MongoDB veri dizini bulundu, log için kontrol ediliyor: %s/log", mongoDataDir)
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
						logger.Debug("MongoDB log dizini bulundu: %s", logPath)
						break
					}
				}
				if logPath != "" {
					break
				}
			}

			// MacOS'ta /var/log kontrolü
			if logPath == "" {
				// mongodb Process'ini bul ve file descriptors'ı kontrol et
				mongoLogFile := findLogFileFromOpenFD()
				if mongoLogFile != "" {
					// Dosyanın dizinini al
					parentDir := filepath.Dir(mongoLogFile)
					logger.Debug("MongoDB log dosyası bulundu, dizini kontrol edilecek: %s", parentDir)
					// Dizin var mı kontrol et
					if _, err := os.Stat(parentDir); err == nil {
						logPath = parentDir
					} else {
						// Tek dosya döndür
						stat, err := os.Stat(mongoLogFile)
						if err == nil {
							file := &pb.MongoLogFile{
								Name:         filepath.Base(mongoLogFile),
								Path:         mongoLogFile,
								Size:         stat.Size(),
								LastModified: stat.ModTime().Unix(),
							}
							logger.Info("MongoDB log dosyası açık dosya tanımlayıcıları ile bulundu: %s", mongoLogFile)
							return []*pb.MongoLogFile{file}, nil
						}
					}
				}

				if logPath == "" {
					logPath = "/var/log"
					logger.Debug("MongoDB özel log dizini bulunamadı, genel log dizini kontrol ediliyor: %s", logPath)
				}
			}
		}
	}

	// logPath'in var olup olmadığını kontrol et
	info, err := os.Stat(logPath)
	if err != nil {
		return nil, fmt.Errorf("belirtilen log dizini bulunamadı: %v", err)
	}

	// DEBUG: Dizindeki tüm dosyaları listele
	if info.IsDir() {
		logger.Debug(" Dizindeki tüm dosyaları listeliyorum: %s", logPath)
		dirEntries, err := os.ReadDir(logPath)
		if err == nil {
			for _, entry := range dirEntries {
				fileName := entry.Name()
				isArtifact := isMongoDBArtifact(fileName)
				isMatching := isMatchingMongoDBLogName(strings.ToLower(fileName))
				logger.Debug(" Dosya: %s | MongoDB Artifact: %t | Matching Name: %t",
					fileName, isArtifact, isMatching)
			}
		}
	}

	var logFiles []*pb.MongoLogFile

	// Eğer belirtilen path bir dosya ise ve MongoDB log dosyası ise, direkt olarak onu ekle
	if !info.IsDir() {
		fileName := strings.ToLower(filepath.Base(logPath))
		if isMongoDBArtifact(fileName) {
			file := &pb.MongoLogFile{
				Name:         filepath.Base(logPath),
				Path:         logPath,
				Size:         info.Size(),
				LastModified: info.ModTime().Unix(),
			}
			logger.Info("MongoDB tek log dosyası bulundu: %s", logPath)
			return append(logFiles, file), nil
		}
		logger.Debug("Belirtilen dosya MongoDB log dosyası değil, dizini kontrol ediliyor: %s", filepath.Dir(logPath))
		// Dosyanın dizinini dene
		logPath = filepath.Dir(logPath)
		info, err = os.Stat(logPath)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("belirtilen dosya bir MongoDB log dosyası değil ve geçerli bir dizin de değil: %s", logPath)
		}
	}

	// Buraya geldiysek, logPath bir dizindir
	logger.Info("MongoDB log dizini için dosyalar taranıyor: %s", logPath)

	// Dizindeki MongoDB log dosyalarını bul
	entries, err := os.ReadDir(logPath)
	if err != nil {
		return nil, fmt.Errorf("dizin içeriği listelenemedi: %v", err)
	}

	for _, entry := range entries {
		// Sadece dosyaları işle
		if entry.IsDir() {
			continue
		}

		// Dosya ismini kontrol et
		fileName := strings.ToLower(entry.Name())
		if isMongoDBArtifact(fileName) {
			// Dosya bilgilerini al
			fileInfo, err := os.Stat(filepath.Join(logPath, entry.Name()))
			if err != nil {
				logger.Warning("Dosya bilgileri alınamadı: %v", err)
				continue
			}

			file := &pb.MongoLogFile{
				Name:         entry.Name(),
				Path:         filepath.Join(logPath, entry.Name()),
				Size:         fileInfo.Size(),
				LastModified: fileInfo.ModTime().Unix(),
			}
			logFiles = append(logFiles, file)
			logger.Info("MongoDB log dosyası bulundu: %s", file.Path)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("log dosyaları listelenirken hata: %v", err)
	}

	// Hiç log dosyası bulunamadıysa bilgilendirme mesajı döndür
	if len(logFiles) == 0 {
		logger.Warning("Belirtilen dizinde (%s) MongoDB log dosyası bulunamadı", logPath)

		// Son çare olarak, MongoDB process'inin dosya tanımlayıcılarını kontrol et
		logFilePath := checkMongoFileDescriptors()
		if logFilePath != "" {
			info, err := os.Stat(logFilePath)
			if err == nil {
				file := &pb.MongoLogFile{
					Name:         filepath.Base(logFilePath),
					Path:         logFilePath,
					Size:         info.Size(),
					LastModified: info.ModTime().Unix(),
				}
				logger.Info("MongoDB log dosyası file descriptor ile bulundu: %s", logFilePath)
				return []*pb.MongoLogFile{file}, nil
			}
		}

		// MongoDB çalışıyor ama log dosyası bulunamadı
		logger.Warning("MongoDB servisi çalışıyor ama log dosyaları bulunamadı. MongoDB log dosyası konumunu kontrol edin.")
	} else {
		logger.Info("%d adet MongoDB log dosyası bulundu", len(logFiles))
	}

	return logFiles, nil
}

// isMongoDBRunning MongoDB servisinin çalışıp çalışmadığını kontrol eder
func isMongoDBRunning() bool {
	cmd := exec.Command("pgrep", "mongod")
	err := cmd.Run()
	return err == nil
}

// findMongoDBLogPathFromProcess MongoDB process'inden log dosyası yolunu bulmayı dener
func findMongoDBLogPathFromProcess() string {
	// 1. ps ile tüm MongoDB süreçlerini bul
	cmd := exec.Command("sh", "-c", "ps -ef | grep mongod | grep -v grep")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		// --logpath parametresini ara
		if idx := strings.Index(line, "--logpath"); idx != -1 {
			parts := strings.Fields(line[idx:])
			if len(parts) > 0 {
				logpathArg := parts[0]
				if strings.Contains(logpathArg, "=") {
					parts = strings.Split(logpathArg, "=")
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

// findLogFileFromOpenFD açık dosya tanımlayıcılarını kontrol ederek MongoDB log dosyalarını bulmayı dener
func findLogFileFromOpenFD() string {
	// MongoDB PID'sini bul
	cmd := exec.Command("sh", "-c", "pgrep mongod")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	pid := strings.TrimSpace(string(out))
	if pid == "" {
		return ""
	}

	// lsof ile açık dosyaları listele
	cmd = exec.Command("sh", "-c", fmt.Sprintf("lsof -p %s | grep -i -E '(log|mongodb)'", pid))
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
			if strings.Contains(strings.ToLower(filePath), "mongo") && strings.HasSuffix(strings.ToLower(filePath), ".log") {
				return filePath
			}
		}
	}

	return ""
}

// isMongoDBLogFile dosya içeriğini kontrol ederek MongoDB log formatına uygun olup olmadığını belirler
func isMongoDBLogFile(filePath string) bool {
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
		// MongoDB log satırları genellikle tarih formatı ve bazı anahtar kelimeler içerir
		if strings.Contains(line, "MongoDB") ||
			strings.Contains(line, "mongod") ||
			strings.Contains(line, "CONTROL") ||
			strings.Contains(line, "NETWORK") ||
			strings.Contains(line, "COMMAND") ||
			strings.Contains(line, "QUERY") ||
			strings.Contains(line, "REPL") {
			return true
		}
		linesChecked++
	}

	return false
}

// findMongoDBDataDir MongoDB veri dizinini bulmayı dener
func findMongoDBDataDir() string {
	// 1. ps ile MongoDB sürecini bul ve --dbpath parametresini kontrol et
	cmd := exec.Command("sh", "-c", "ps -ef | grep 'mongod' | grep -v 'grep' | grep 'dbpath'")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	// Komut satırı çıktısını kontrol et
	outputLines := strings.Split(string(out), "\n")
	for _, line := range outputLines {
		// --dbpath parametresini ara
		if dbpathIndex := strings.Index(line, "--dbpath"); dbpathIndex != -1 {
			// dbpath değerini çıkar
			dbpathPart := line[dbpathIndex+8:] // "--dbpath" uzunluğu = 8
			dbpathPart = strings.TrimSpace(dbpathPart)

			// Parametreyi ayır
			parts := strings.Fields(dbpathPart)
			if len(parts) > 0 {
				dbPath := parts[0]
				if strings.HasPrefix(dbPath, "=") {
					dbPath = dbPath[1:] // '=' karakterini kaldır
				}

				// DB path'in geçerli olup olmadığını kontrol et
				if _, err := os.Stat(dbPath); err == nil {
					return dbPath
				}
			}
		}
	}

	// 2. Bilinen MongoDB veri dizinlerini kontrol et
	knownDataDirs := []string{
		"/var/lib/mongodb",
		"/var/lib/mongo",
		"/data/db",
		"/usr/local/var/mongodb",
		"/opt/homebrew/var/mongodb", // macOS Homebrew Apple Silicon
	}

	for _, dir := range knownDataDirs {
		if _, err := os.Stat(dir); err == nil {
			return dir
		}
	}

	return ""
}

// checkMongoFileDescriptors MongoDB process'inin açık dosya tanımlayıcılarını kontrol eder
func checkMongoFileDescriptors() string {
	// MongoDB PID'sini bul
	cmd := exec.Command("pgrep", "mongod")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	pid := strings.TrimSpace(string(out))
	if pid == "" {
		return ""
	}

	// MacOS için lsof komutu ile açık dosyaları listele
	cmd = exec.Command("sh", "-c", fmt.Sprintf("lsof -p %s | grep log", pid))
	out, err = cmd.Output()
	if err != nil {
		return ""
	}

	// Çıktıdaki her satırı incele
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if !strings.Contains(line, "log") {
			continue
		}

		// lsof çıktısını parçala
		fields := strings.Fields(line)
		if len(fields) > 8 {
			// 8. alan dosya yolu
			logPath := fields[8]
			if strings.HasSuffix(strings.ToLower(logPath), ".log") {
				return logPath
			}
		}
	}

	return ""
}

// isMatchingMongoDBLogName dosya adının MongoDB log dosyası olduğunu kesin olarak belirler
func isMatchingMongoDBLogName(fileName string) bool {
	// MongoDB ile ilgili kesin isimler
	mongoNames := []string{
		"mongo.log",
		"mongodb.log",
		"mongod.log",
		"mongos.log",
	}

	for _, mongoName := range mongoNames {
		if fileName == mongoName {
			return true
		}
	}

	// mongod1.log, mongod2.log gibi dosyaları kontrol et
	if strings.HasPrefix(fileName, "mongod") && strings.HasSuffix(fileName, ".log") {
		// "mongod" ile başlayan her log dosyasını kabul et
		return true
	}

	// mongo1.log, mongo2.log gibi dosyaları kontrol et
	if strings.HasPrefix(fileName, "mongo") && strings.HasSuffix(fileName, ".log") {
		return true
	}

	return false
}

// isMongoDBArtifact verilen dosya veya dizin adının MongoDB ile ilgili olup olmadığını kontrol eder
func isMongoDBArtifact(name string) bool {
	name = strings.ToLower(name)

	// Özel durum: bazı sistem log dosyalarını hariç tut
	exclusions := []string{
		"system.log",
		"kernel.log",
		"wifi.log",
		"install.log",
		"appfirewall.log",
		"apache",
		"nginx",
		"php",
		"cron.log",
		"asl.log",
		"syslog",
		"kernel_task",
		"security.log",
	}

	for _, exclusion := range exclusions {
		if strings.Contains(name, exclusion) {
			return false
		}
	}

	// MongoDB ile ilgili kesin isimler
	mongoNames := []string{
		"mongo.log",
		"mongodb.log",
		"mongod.log",
		"mongos.log",
	}

	for _, mongoName := range mongoNames {
		if name == mongoName {
			return true
		}
	}

	// "mongod" ile başlayan tüm .log dosyaları
	if strings.HasPrefix(name, "mongod") && strings.HasSuffix(name, ".log") {
		return true
	}

	// "mongo" ile başlayan tüm .log dosyaları
	if strings.HasPrefix(name, "mongo") && strings.HasSuffix(name, ".log") {
		return true
	}

	// Dizin için özel durum
	if !strings.HasSuffix(name, ".log") && (strings.Contains(name, "mongo") || strings.Contains(name, "mongodb")) {
		return true
	}

	return false
}

// findMongoConfigFile MongoDB konfigürasyon dosyasını bulmayı dener
func findMongoConfigFile() string {
	// 1. MongoDB süreç parametrelerinden konfigürasyon dosyasını bul
	cmd := exec.Command("sh", "-c", "ps -ef | grep mongod | grep -v grep | grep -- '--config\\|--f\\|-f' || true")
	out, err := cmd.Output()
	if err == nil && len(out) > 0 {
		lines := strings.Split(string(out), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}

			// --config veya -f veya --f parametresini ara
			configParams := []string{"--config", "--f", "-f"}
			for _, param := range configParams {
				if idx := strings.Index(line, param); idx != -1 {
					parts := strings.Fields(line[idx:])
					if len(parts) > 1 && !strings.HasPrefix(parts[1], "-") {
						configPath := parts[1]
						// "=" işaretini ayır (--config=file.conf durumu için)
						if strings.Contains(parts[0], "=") {
							configParts := strings.Split(parts[0], "=")
							if len(configParts) > 1 {
								configPath = configParts[1]
							}
						}

						// Dosyanın var olup olmadığını kontrol et
						if _, err := os.Stat(configPath); err == nil {
							logger.Info("MongoDB konfigürasyon dosyası süreç parametrelerinden bulundu: %s", configPath)
							return configPath
						}
					}
				}
			}
		}
	}

	// 2. Bilinen olası konfigürasyon dosyalarını kontrol et
	commonConfigPaths := []string{
		"/etc/mongod.conf",
		"/etc/mongodb.conf",
		"/usr/local/etc/mongod.conf",
		"/usr/local/var/mongodb/mongod.conf",
		"/opt/mongodb/conf/mongod.conf",
		"/opt/homebrew/etc/mongod.conf", // macOS Homebrew Apple Silicon
	}

	// Sistemin türüne göre ek yollar ekle
	if runtime.GOOS == "darwin" {
		// macOS için Homebrew kurulumundaki olası yerler
		commonConfigPaths = append(commonConfigPaths,
			"/usr/local/etc/mongod.conf",
			"/opt/homebrew/etc/mongod.conf",
			"/usr/local/Cellar/mongodb/*/etc/mongod.conf",
		)
	} else if runtime.GOOS == "linux" {
		// Linux dağıtımlarına özgü yollar
		commonConfigPaths = append(commonConfigPaths,
			"/etc/mongodb/mongod.conf",
			"/etc/mongod/mongod.conf",
			"/opt/mongodb/etc/mongod.conf",
			"/var/lib/mongodb/mongod.conf",
		)
	}

	// User home dizinini al
	homeDir, err := os.UserHomeDir()
	if err == nil {
		// User-specific konfigürasyon yolları
		userConfigs := []string{
			filepath.Join(homeDir, ".mongodb/mongod.conf"),
			filepath.Join(homeDir, ".config/mongodb/mongod.conf"),
			filepath.Join(homeDir, "mongodb/mongod.conf"),
		}
		commonConfigPaths = append(commonConfigPaths, userConfigs...)
	}

	// Bilinen yolları kontrol et
	for _, path := range commonConfigPaths {
		// Glob pattern desteği için
		matches, err := filepath.Glob(path)
		if err == nil && len(matches) > 0 {
			for _, match := range matches {
				if _, err := os.Stat(match); err == nil {
					logger.Info("MongoDB konfigürasyon dosyası bilinen yoldan bulundu: %s", match)
					return match
				}
			}
		}
	}

	// 3. MongoDB veri dizinini bul ve orada konfigürasyon dosyalarını ara
	dataDir := findMongoDBDataDir()
	if dataDir != "" {
		// Veri dizini ve üst dizininde config dosyalarını ara
		configInDataDir := []string{
			filepath.Join(dataDir, "mongod.conf"),
			filepath.Join(dataDir, "mongodb.conf"),
			filepath.Join(filepath.Dir(dataDir), "conf", "mongod.conf"),
			filepath.Join(filepath.Dir(dataDir), "etc", "mongod.conf"),
		}

		for _, path := range configInDataDir {
			if _, err := os.Stat(path); err == nil {
				logger.Info("MongoDB konfigürasyon dosyası veri dizininde bulundu: %s", path)
				return path
			}
		}
	}

	// 4. Son çare: `mongod --help` çıktısından varsayılan config dosyasını bul
	cmd = exec.Command("mongod", "--help")
	out, err = cmd.Output()
	if err == nil {
		// --help çıktısında varsayılan config dosyası yolunu ara
		helpText := string(out)
		scanner := bufio.NewScanner(strings.NewReader(helpText))
		for scanner.Scan() {
			line := scanner.Text()
			// Config dosyası hakkında bilgi içeren satırı ara
			if strings.Contains(line, "config") && strings.Contains(line, "default") {
				// Satırdaki dosya yollarını çıkar
				re := regexp.MustCompile(`/\w+(?:/\w+)*\.conf`)
				matches := re.FindAllString(line, -1)
				for _, match := range matches {
					if _, err := os.Stat(match); err == nil {
						logger.Info("MongoDB konfigürasyon dosyası --help çıktısından bulundu: %s", match)
						return match
					}
				}
			}
		}
	}

	// Hiçbir şey bulunamadı
	logger.Warning("MongoDB konfigürasyon dosyası bulunamadı")
	return ""
}

// getLogPathFromConfig MongoDB konfigürasyon dosyasından log path'ini çıkarır
func getLogPathFromConfig(configFile string) string {
	// Dosyayı oku
	data, err := os.ReadFile(configFile)
	if err != nil {
		logger.Warning("Konfigürasyon dosyası okunamadı: %v", err)
		return ""
	}

	// Basit bir şekilde "logpath" veya "path" satırını ara
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// YAML formatlı konfigürasyon için
		if strings.HasPrefix(line, "logPath:") || strings.HasPrefix(line, "path:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}

		// INI formatlı konfigürasyon için
		if strings.HasPrefix(line, "logpath=") || strings.HasPrefix(line, "path=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}

	return ""
}

// GetMongoServiceStatus MongoDB servisinin detaylı durumunu kontrol eder
func (c *MongoCollector) GetMongoServiceStatus() *MongoServiceStatus {
	status := &MongoServiceStatus{
		Status:    "FAIL!",
		IsReplSet: false,
	}

	// Önce process kontrolü yap
	cmd := exec.Command("sh", "-c", "pgrep -x mongod")
	out, err := cmd.Output()
	if err != nil || len(out) == 0 {
		status.ErrorMessage = "MongoDB process could not be found!"
		return status
	}

	// MongoDB'ye bağlan
	client, err := c.OpenDB()
	if err != nil {
		status.ErrorMessage = fmt.Sprintf("MongoDB bağlantısı kurulamadı: %v", err)
		return status
	}
	defer client.Disconnect(context.Background())

	// serverStatus kontrolü
	serverStatus, err := c.runAdminCommand(client, bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		status.ErrorMessage = fmt.Sprintf("serverStatus komutu başarısız: %v", err)
		return status
	}

	// Servis durumunu serverStatus'ten kontrol et
	if uptime, ok := serverStatus["uptime"].(int32); ok {
		if uptime <= 0 {
			status.Status = "DEGRADED"
			status.ErrorMessage = "MongoDB uptime sıfır veya negatif"
			return status
		}
	}

	// Bağlantı sayısını kontrol et
	if connections, ok := serverStatus["connections"].(bson.M); ok {
		if current, ok := connections["current"].(int32); ok {
			if current <= 0 {
				status.Status = "DEGRADED"
				status.ErrorMessage = "Aktif bağlantı yok"
				return status
			}
		}
	}

	// replSetGetStatus kontrolü
	replSetStatus, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		// replSetGetStatus hatası standalone sunucu olduğunu gösterebilir
		status.Status = "RUNNING"
		status.CurrentState = "STANDALONE"
		return status
	}

	// Replica set durumunu işaretle
	status.IsReplSet = true

	// myState değerini kontrol et
	if state, ok := replSetStatus["myState"].(int32); ok {
		switch state {
		case 1:
			status.CurrentState = "PRIMARY"
		case 2:
			status.CurrentState = "SECONDARY"
		case 7:
			status.CurrentState = "ARBITER"
		default:
			status.CurrentState = fmt.Sprintf("STATE_%d", state)
		}
	}

	// Servis durumunu belirle
	status.Status = "RUNNING"
	if status.IsReplSet && status.CurrentState == "" {
		status.Status = "DEGRADED"
		status.ErrorMessage = "Node state belirlenemedi"
	}

	return status
}

// CheckForFailover failover durumunu kontrol eder ve gerekirse alarm üretir
func (c *MongoCollector) CheckForFailover(prevStatus, currentStatus *MongoServiceStatus) bool {
	if prevStatus == nil || currentStatus == nil {
		return false
	}

	// Statik durumları karşılaştır (yalnızca durumlar değiştiyse alarm üret)
	if prevStatus.CurrentState == currentStatus.CurrentState &&
		prevStatus.Status == currentStatus.Status {
		// Durum değişmediyse false döndür
		return false
	}

	// Failover durumlarını kontrol et
	isFailover := false

	// 1. PRIMARY -> SECONDARY geçiş (bu node primary iken secondary'ye düşmüş)
	if prevStatus.CurrentState == "PRIMARY" && currentStatus.CurrentState == "SECONDARY" {
		isFailover = true
		logger.Error("ALARM: MongoDB node PRIMARY'den SECONDARY'ye düştü!")
	}

	// 2. SECONDARY -> PRIMARY geçiş (bu node secondary iken primary olmuş)
	if prevStatus.CurrentState == "SECONDARY" && currentStatus.CurrentState == "PRIMARY" {
		isFailover = true
		logger.Error("ALARM: MongoDB node SECONDARY'den PRIMARY'ye yükseldi!")
	}

	// 3. Servis durumu değişiklikleri
	if prevStatus.Status == "RUNNING" && currentStatus.Status != "RUNNING" {
		isFailover = true
		logger.Error("ALARM: MongoDB servis durumu değişti! Önceki: %s, Şimdiki: %s, Hata: %s",
			prevStatus.Status, currentStatus.Status, currentStatus.ErrorMessage)
	}

	return isFailover
}

// PromoteToPrimary MongoDB standby node'unu primary'ye yükseltir
func (c *MongoCollector) PromoteToPrimary(hostname string, port int, replicaSet string) (string, error) {
	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in PromoteToPrimary: %v", r)
			// Mark as unhealthy after panic
			if c != nil {
				c.isHealthy = false
				c.collectionInterval = 2 * time.Minute
			}
		}
	}()

	logger.Info("MongoDB node stepDown başlatılıyor. Hostname: %s, Port: %d, ReplicaSet: %s",
		hostname, port, replicaSet)

	// MongoDB bağlantı URI'sini oluştur
	connectionString := fmt.Sprintf("mongodb://%s:%s@%s:%d/admin",
		c.cfg.Mongo.User, c.cfg.Mongo.Pass, hostname, port)

	// Auth bilgileri boşsa, kimlik doğrulama olmadan bağlan
	if !c.cfg.Mongo.Auth {
		connectionString = fmt.Sprintf("mongodb://%s:%d/admin", hostname, port)
	}

	// MongoDB shell komutu oluştur
	command := fmt.Sprintf(`mongosh "%s" --eval "rs.stepDown(60, 10)"`, connectionString)
	logger.Info("MongoDB RS stepDown komutu çalıştırılıyor")

	// Komutu çalıştır
	cmd := exec.Command("bash", "-c", command)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		logger.Error("MongoDB stepDown komutu çalıştırılırken hata: %v\nÇıktı: %s", err, outputStr)
		// Node primary değilse hata döner
		if strings.Contains(outputStr, "not primary") {
			return "", fmt.Errorf("bu node primary değil, stepDown işlemi yapılamaz: %v", err)
		}
		return "", fmt.Errorf("stepDown başarısız: %v", err)
	}

	logger.Info("MongoDB stepDown komutu başarıyla çalıştırıldı. Çıktı: %s", outputStr)

	// Check status after stepDown (10 second delay)
	logger.Info("Node'un yeni durumunu kontrol etmek için bekleniyor...")
	time.Sleep(10 * time.Second)

	// Check node status after stepDown
	checkCmd := fmt.Sprintf(`mongosh "%s" --eval "rs.status()"`, connectionString)
	cmd = exec.Command("bash", "-c", checkCmd)
	checkOutput, checkErr := cmd.CombinedOutput()

	if checkErr != nil {
		logger.Error("StepDown sonrası durum kontrolü başarısız: %v", checkErr)
		return fmt.Sprintf("MongoDB node stepDown tamamlandı, ancak durum kontrolü başarısız: %s",
			strings.TrimSpace(outputStr)), nil
	}

	checkOutputStr := string(checkOutput)
	return fmt.Sprintf("MongoDB node stepDown başarılı. Çıktı: %s\nYeni Durum: %s",
		strings.TrimSpace(outputStr),
		strings.TrimSpace(checkOutputStr)), nil
}

// FreezeMongoSecondary MongoDB secondary node'larında rs.freeze() komutunu çalıştırır
func (c *MongoCollector) FreezeMongoSecondary(hostname string, port int, replicaSet string, seconds int) (string, error) {
	// Implement panic recovery to prevent crash
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC in FreezeMongoSecondary: %v", r)
			// Mark as unhealthy after panic
			if c != nil {
				c.isHealthy = false
				c.collectionInterval = 2 * time.Minute
			}
		}
	}()

	logger.Info("MongoDB node freeze başlatılıyor. Hostname: %s, Port: %d, ReplicaSet: %s, Seconds: %d",
		hostname, port, replicaSet, seconds)

	// MongoDB bağlantı URI'sini oluştur
	connectionString := fmt.Sprintf("mongodb://%s:%s@%s:%d/admin",
		c.cfg.Mongo.User, c.cfg.Mongo.Pass, hostname, port)

	// Auth bilgileri boşsa, kimlik doğrulama olmadan bağlan
	if !c.cfg.Mongo.Auth {
		connectionString = fmt.Sprintf("mongodb://%s:%d/admin", hostname, port)
	}

	// MongoDB shell komutu oluştur
	command := fmt.Sprintf(`mongosh "%s" --eval "rs.freeze(%d)"`, connectionString, seconds)
	logger.Info("MongoDB RS freeze komutu çalıştırılıyor: %d saniye", seconds)

	// Komutu çalıştır
	cmd := exec.Command("bash", "-c", command)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	if err != nil {
		logger.Error("MongoDB freeze komutu çalıştırılırken hata: %v\nÇıktı: %s", err, outputStr)
		return "", fmt.Errorf("freeze işlemi başarısız: %v", err)
	}

	logger.Info("MongoDB freeze komutu başarıyla çalıştırıldı. Çıktı: %s", outputStr)

	// Check node status after freeze
	checkCmd := fmt.Sprintf(`mongosh "%s" --eval "rs.status()"`, connectionString)
	cmd = exec.Command("bash", "-c", checkCmd)
	checkOutput, checkErr := cmd.CombinedOutput()

	if checkErr != nil {
		logger.Error("Freeze sonrası durum kontrolü başarısız: %v", checkErr)
		return fmt.Sprintf("MongoDB node freeze tamamlandı, ancak durum kontrolü başarısız: %s",
			strings.TrimSpace(outputStr)), nil
	}

	checkOutputStr := string(checkOutput)
	return fmt.Sprintf("MongoDB node freeze başarılı (%d saniye). Çıktı: %s\nYeni Durum: %s",
		seconds,
		strings.TrimSpace(outputStr),
		strings.TrimSpace(checkOutputStr)), nil
}

// GetClient returns a new MongoDB client instance
func (c *MongoCollector) GetClient() (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// MongoDB bağlantı URI'sini oluştur
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s",
		c.cfg.Mongo.User,
		c.cfg.Mongo.Pass,
		c.cfg.Mongo.Host,
		c.cfg.Mongo.Port)

	if c.cfg.Mongo.Replset != "" {
		uri += fmt.Sprintf("/?replicaSet=%s", c.cfg.Mongo.Replset)
	}

	// MongoDB client options
	clientOptions := options.Client().ApplyURI(uri)

	// Yeni bir client oluştur ve bağlan
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("MongoDB client oluşturulamadı: %v", err)
	}

	// Bağlantıyı test et
	err = client.Ping(ctx, nil)
	if err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("MongoDB bağlantısı test edilemedi: %v", err)
	}

	return client, nil
}

// ExplainMongoQuery, MongoDB sorgu planını explain() kullanarak getirir
func (c *MongoCollector) ExplainMongoQuery(database, queryStr string) (string, error) {
	// MongoDB bağlantısını al
	client, err := c.GetClient()
	if err != nil {
		logger.Error("MongoDB explain bağlantısı açılamadı: %v", err)
		return "", fmt.Errorf("MongoDB bağlantısı açılamadı: %v", err)
	}
	defer client.Disconnect(context.Background())

	logger.Info("ExplainMongoQuery başlatılıyor. Veritabanı: %s, Sorgu Boyutu: %d bytes",
		database, len(queryStr))

	// Sorguyu BSON formatına çevir
	var queryDoc bson.D
	if err := bson.UnmarshalExtJSON([]byte(queryStr), true, &queryDoc); err != nil {
		logger.Error("MongoDB sorgusu JSON formatına çevrilemedi: %v", err)
		return "", fmt.Errorf("sorgu JSON formatına çevrilemedi: %v", err)
	}

	logger.Info("MongoDB sorgusu başarıyla parse edildi")

	// Veritabanı bağlantısı
	db := client.Database(database)

	// Sorgu tipi ne olursa olsun, explain komutunu kullan
	explainOpts := bson.D{
		{Key: "explain", Value: queryDoc},
		{Key: "verbosity", Value: "allPlansExecution"},
	}

	var explainResult bson.M
	if err := db.RunCommand(context.Background(), explainOpts).Decode(&explainResult); err != nil {
		// Admin veritabanında tekrar deneyelim
		logger.Error("Explain sorgu hatası: %v, admin veritabanında tekrar deneniyor", err)
		adminDB := client.Database("admin")
		if err := adminDB.RunCommand(context.Background(), explainOpts).Decode(&explainResult); err != nil {
			logger.Error("Admin veritabanında da explain başarısız: %v", err)
			return "", fmt.Errorf("MongoDB sorgu planı alınamadı: %v", err)
		}
	}

	// Sonucu yapılandırılmış JSON formatına dönüştür
	var planParts []string

	// queryPlanner bölümü
	if queryPlanner, exists := explainResult["queryPlanner"]; exists {
		qpBytes, _ := json.MarshalIndent(queryPlanner, "", "  ")
		planParts = append(planParts, "## Query Planner\n"+string(qpBytes))
	}

	// executionStats bölümü
	if execStats, exists := explainResult["executionStats"]; exists {
		esBytes, _ := json.MarshalIndent(execStats, "", "  ")
		planParts = append(planParts, "## Execution Stats\n"+string(esBytes))
	}

	// serverInfo bölümü
	if serverInfo, exists := explainResult["serverInfo"]; exists {
		siBytes, _ := json.MarshalIndent(serverInfo, "", "  ")
		planParts = append(planParts, "## Server Info\n"+string(siBytes))
	}

	// Hiçbir bölüm bulunamadıysa tam sonucu kullan
	if len(planParts) == 0 {
		resultBytes, _ := json.MarshalIndent(explainResult, "", "  ")
		return string(resultBytes), nil
	}

	// Tüm bölümleri birleştir
	return strings.Join(planParts, "\n\n"), nil
}

// CanCollect checks if enough time has passed since last collection to prevent rate limiting
func (c *MongoCollector) CanCollect() bool {
	// Allow collection during startup grace period
	if time.Since(c.startupTime) < c.startupGracePeriod {
		logger.Info("MongoDB collector in startup grace period, allowing collection")
		return true
	}

	if time.Since(c.lastCollectionTime) < c.collectionInterval {
		logger.Debug("MongoDB Rate limiting: Not enough time passed since last collection (min interval: %v)", c.collectionInterval)
		return false
	}
	return true
}

// SetCollectionTime updates the last collection time
func (c *MongoCollector) SetCollectionTime() {
	c.lastCollectionTime = time.Now()
}

// IsHealthy returns the current health state
func (c *MongoCollector) IsHealthy() bool {
	// Check health every 2 minutes
	if time.Since(c.lastHealthCheck) > 2*time.Minute {
		c.checkHealth()
	}
	return c.isHealthy
}

// checkHealth performs a simple health check - INDEPENDENT of rate limiting
func (c *MongoCollector) checkHealth() {
	c.lastHealthCheck = time.Now()

	// CRITICAL: Health check must be independent of rate limiting to allow recovery
	// Don't use c.openDBDirect() if it checks ShouldSkipCollection
	logger.Debug("MongoDB checkHealth: Starting health check")

	// Direct MongoDB connection for health check - bypass all rate limiting
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true&connectTimeoutMS=3000&serverSelectionTimeoutMS=3000&maxPoolSize=1",
		c.cfg.Mongo.User,
		c.cfg.Mongo.Pass,
		c.cfg.Mongo.Host,
		c.cfg.Mongo.Port,
	)

	// Auth bilgileri boşsa, kimlik doğrulama olmadan bağlan
	if !c.cfg.Mongo.Auth {
		uri = fmt.Sprintf("mongodb://%s:%s/?directConnection=true&connectTimeoutMS=3000&serverSelectionTimeoutMS=3000&maxPoolSize=1",
			c.cfg.Mongo.Host, c.cfg.Mongo.Port)
	}

	clientOptions := options.Client().ApplyURI(uri)

	// Minimal connection settings for health check
	clientOptions.SetMaxPoolSize(1)
	clientOptions.SetMinPoolSize(1)
	clientOptions.SetMaxConnIdleTime(5 * time.Second)
	clientOptions.SetConnectTimeout(3 * time.Second)
	clientOptions.SetServerSelectionTimeout(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		c.isHealthy = false
		logger.Warning("MongoDB health check failed - connection error: %v", err)
		c.collectionInterval = 2 * time.Minute
		return
	}

	// Quick test ping with very short timeout
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pingCancel()

	err = client.Ping(pingCtx, readpref.Primary())
	if client != nil {
		client.Disconnect(ctx)
	}

	if err != nil {
		c.isHealthy = false
		logger.Warning("MongoDB health check ping failed - marking collector as unhealthy: %v", err)
		c.collectionInterval = 2 * time.Minute
	} else {
		// RECOVERY: If we were unhealthy, log recovery
		if !c.isHealthy {
			logger.Info("MongoDB collector RECOVERED - marking as healthy")
		}
		c.isHealthy = true
		logger.Debug("MongoDB health check passed - collector is healthy")
		c.collectionInterval = 30 * time.Second // Reset to normal interval
	}
}

// ForceHealthCheck forces an immediate health check - useful for recovery
func (c *MongoCollector) ForceHealthCheck() {
	logger.Info("MongoDB collector forcing health check for recovery")
	c.checkHealth()
}

// ShouldSkipCollection determines if collection should be skipped due to rate limiting or health issues
func (c *MongoCollector) ShouldSkipCollection() bool {
	if !c.CanCollect() {
		return true
	}

	// Check health but allow more frequent recovery attempts
	if !c.IsHealthy() {
		// AGGRESSIVE RECOVERY: Try recovery every 2 minutes instead of 5
		if time.Since(c.lastHealthCheck) > 2*time.Minute {
			logger.Info("MongoDB collector has been unhealthy for 2+ minutes, attempting aggressive recovery...")
			c.ForceHealthCheck()

			// If still unhealthy after forced check, skip collection
			if !c.isHealthy {
				logger.Debug("MongoDB recovery attempt failed, skipping collection")
				return true
			}
			logger.Info("MongoDB collector successfully recovered!")
		} else {
			logger.Debug("Skipping MongoDB collection due to unhealthy state")
			return true
		}
	}

	return false
}

// openDBDirect creates a database connection without rate limiting for health checks and critical operations
func (c *MongoCollector) openDBDirect() (*mongo.Client, error) {
	// MongoDB URI oluştur
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true&connectTimeoutMS=5000&serverSelectionTimeoutMS=5000&maxPoolSize=2",
		c.cfg.Mongo.User,
		c.cfg.Mongo.Pass,
		c.cfg.Mongo.Host,
		c.cfg.Mongo.Port,
	)

	// Auth bilgileri boşsa, kimlik doğrulama olmadan bağlan
	if !c.cfg.Mongo.Auth {
		uri = fmt.Sprintf("mongodb://%s:%s/?directConnection=true&connectTimeoutMS=5000&serverSelectionTimeoutMS=5000&maxPoolSize=2",
			c.cfg.Mongo.Host, c.cfg.Mongo.Port)
	}

	clientOptions := options.Client().ApplyURI(uri)

	// Additional client options for health checks
	clientOptions.SetMaxPoolSize(2)                          // Maximum 2 connections
	clientOptions.SetMinPoolSize(1)                          // Minimum 1 connection
	clientOptions.SetMaxConnIdleTime(10 * time.Second)       // Close idle connections after 10 seconds
	clientOptions.SetConnectTimeout(5 * time.Second)         // Connection timeout
	clientOptions.SetServerSelectionTimeout(5 * time.Second) // Server selection timeout

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("MongoDB direct connection failed: %w", err)
	}

	return client, nil
}

// getLastKnownGoodInfo returns cached information to avoid excessive DB calls
func (c *MongoCollector) getLastKnownGoodInfo() *MongoInfo {
	hostname, _ := os.Hostname()

	// Get raw values and normalize them
	rawTotalvCpu := c.getTotalvCpu()
	rawTotalMemory := c.getTotalMemory()

	normalizedCpu := rawTotalvCpu
	if normalizedCpu <= 0 {
		normalizedCpu = 1
	}

	normalizedMemory := rawTotalMemory
	if normalizedMemory < 0 {
		normalizedMemory = 0
	}

	// Create a basic info structure with cached or default values
	info := &MongoInfo{
		ClusterName:    c.cfg.Mongo.Replset,
		IP:             c.getLocalIP(),
		Hostname:       hostname,
		NodeStatus:     "RATE_LIMITED",
		MongoVersion:   "Rate Limited",
		Location:       c.cfg.Mongo.Location,
		MongoStatus:    "RATE_LIMITED", // Indicate this is a rate-limited response
		ReplicaSetName: c.cfg.Mongo.Replset,
		ReplicaLagSec:  0,
		FreeDisk:       "Unknown",
		FdPercent:      0,
		Port:           c.cfg.Mongo.Port,
		TotalvCpu:      normalizedCpu,    // Use normalized value
		TotalMemory:    normalizedMemory, // Use normalized value
		ConfigPath:     "",
	}

	logger.Debug("Returned cached/rate-limited MongoDB info with normalized values to prevent overload")
	return info
}

// ResetToHealthy forces the collector to healthy state - useful for startup recovery
func (c *MongoCollector) ResetToHealthy() {
	if c != nil {
		c.isHealthy = true
		c.collectionInterval = 30 * time.Second
		c.lastHealthCheck = time.Now()
		c.lastCollectionTime = time.Time{} // Reset to allow immediate collection
		logger.Info("MongoDB collector forcefully reset to healthy state")
	}
}

// StartupRecovery performs recovery checks at agent startup
func (c *MongoCollector) StartupRecovery() {
	logger.Info("MongoDB collector performing startup recovery check...")

	// Reset startup time and extend grace period for recovery
	c.startupTime = time.Now()
	c.startupGracePeriod = 30 * time.Second // Extended grace period for recovery

	// Give it 3 attempts at startup
	for i := 0; i < 3; i++ {
		c.ForceHealthCheck()
		if c.isHealthy {
			logger.Info("MongoDB collector startup recovery successful on attempt %d", i+1)
			return
		}
		logger.Warning("MongoDB collector startup recovery attempt %d failed, retrying...", i+1)
		time.Sleep(2 * time.Second)
	}

	// If all attempts failed, force reset to healthy
	logger.Warning("MongoDB collector startup recovery failed after 3 attempts, forcing healthy state")
	c.ResetToHealthy()
}
