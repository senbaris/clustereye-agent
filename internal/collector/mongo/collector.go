package mongo

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoCollector mongodb için veri toplama yapısı
type MongoCollector struct {
	cfg *config.AgentConfig
}

// NewMongoCollector yeni bir MongoCollector oluşturur
func NewMongoCollector(cfg *config.AgentConfig) *MongoCollector {
	return &MongoCollector{
		cfg: cfg,
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
	// MongoDB URI oluştur
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true",
		c.cfg.Mongo.User,
		c.cfg.Mongo.Pass,
		c.cfg.Mongo.Host, // Sunucunun adresini config'den al, şimdilik localhost
		c.cfg.Mongo.Port,
	)

	// Auth bilgileri boşsa, kimlik doğrulama olmadan bağlan
	if !c.cfg.Mongo.Auth {
		uri = fmt.Sprintf("mongodb://localhost:%s", c.cfg.Mongo.Port)
	}

	clientOptions := options.Client().ApplyURI(uri)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("MongoDB bağlantısı kurulamadı: %v", err)
	}

	// Bağlantıyı kontrol et
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("MongoDB ping başarısız: %v", err)
	}

	return client, nil
}

// GetMongoStatus checks if MongoDB service is running by checking if the configured host:port is accessible
func (c *MongoCollector) GetMongoStatus() string {
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
		log.Printf("MongoDB at %s is not accessible: %v", address, err)
		return "FAIL!"
	}
	if conn != nil {
		conn.Close()
	}

	return "RUNNING"
}

// GetMongoVersion MongoDB versiyonunu döndürür
func (c *MongoCollector) GetMongoVersion() string {
	client, err := c.OpenDB()
	if err != nil {
		log.Printf("MongoDB bağlantısı kurulamadı: %v", err)
		return "Unknown"
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("MongoDB bağlantısı kapatılamadı: %v", err)
		}
	}()

	// BuildInfo'yu çalıştır
	buildInfo, err := c.runAdminCommand(client, bson.D{{Key: "buildInfo", Value: 1}})
	if err != nil {
		log.Printf("BuildInfo komutu çalıştırılamadı: %v", err)
		return "Unknown"
	}

	// Versiyon bilgisini al
	version, ok := buildInfo["version"].(string)
	if !ok {
		log.Printf("Versiyon bilgisi string değil")
		return "Unknown"
	}

	return version
}

// GetNodeStatus MongoDB node'un durumunu döndürür (PRIMARY, SECONDARY, ARBITER vs.)
func (c *MongoCollector) GetNodeStatus() string {
	client, err := c.OpenDB()
	if err != nil {
		log.Printf("MongoDB bağlantısı kurulamadı: %v", err)
		return "Unknown"
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("MongoDB bağlantısı kapatılamadı: %v", err)
		}
	}()

	// İlk olarak serverStatus komutunu çalıştır - bu bize bağlı olduğumuz node'un bilgisini verecek
	serverStatus, err := c.runAdminCommand(client, bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		log.Printf("serverStatus komutu çalıştırılamadı: %v", err)
		return "Unknown"
	}

	// replSetGetStatus komutunu çalıştır
	status, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		log.Printf("replSetGetStatus komutu çalıştırılamadı: %v", err)
		return "STANDALONE"
	}

	// Şimdiki bağlantının MongoDB instance bilgilerini alıyoruz
	process, ok := serverStatus["process"].(string)
	if !ok {
		log.Printf("process bilgisi bulunamadı")
		process = "mongod"
	}

	host, ok := serverStatus["host"].(string)
	if !ok {
		log.Printf("host bilgisi bulunamadı")
		host = "localhost"
	}

	// myState değerini doğrudan almaya çalış
	if replication, ok := status["myState"]; ok {
		state, ok := replication.(int32)
		if ok {
			switch state {
			case 1:
				return "PRIMARY"
			case 2:
				return "SECONDARY"
			case 7:
				return "ARBITER"
			default:
				return fmt.Sprintf("STATE_%d", state)
			}
		}
	}

	// myState yoksa members içinde arama yapalım
	members, ok := status["members"].(bson.A)
	if !ok {
		log.Printf("members dizisi bulunamadı")
		return "Unknown"
	}

	// Process ID ve host bilgisine göre kendimizi bulalım
	currentHost := fmt.Sprintf("%s", host)
	log.Printf("Mevcut MongoDB host: %s, process: %s", currentHost, process)

	// Tüm member'ları dönerek durumu bulalım
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

		// StateStr varsa direkt onu kullan
		if stateStr, ok := memberMap["stateStr"].(string); ok {
			log.Printf("MongoDB node %s durumu: %s", memberHost, stateStr)
		}

		// Kendimizi kontrol et (host name'e göre)
		if strings.Contains(memberHost, currentHost) || strings.Contains(currentHost, memberHost) {
			// StateStr varsa direkt onu kullan
			if stateStr, ok := memberMap["stateStr"].(string); ok {
				return stateStr
			}

			// State değerine göre durum belirle
			state, ok := memberMap["state"].(int32)
			if !ok {
				continue
			}

			switch state {
			case 1:
				return "PRIMARY"
			case 2:
				return "SECONDARY"
			case 7:
				return "ARBITER"
			default:
				return fmt.Sprintf("STATE_%d", state)
			}
		}
	}

	// Son çare - isMaster komutu ile durumu kontrol et
	isMaster, err := c.runAdminCommand(client, bson.D{{Key: "isMaster", Value: 1}})
	if err == nil {
		if ismaster, ok := isMaster["ismaster"].(bool); ok && ismaster {
			return "PRIMARY"
		}
		if secondary, ok := isMaster["secondary"].(bool); ok && secondary {
			return "SECONDARY"
		}
		if arbiterOnly, ok := isMaster["arbiterOnly"].(bool); ok && arbiterOnly {
			return "ARBITER"
		}
	}

	return "Unknown"
}

// GetReplicaSetName replica set adını döndürür
func (c *MongoCollector) GetReplicaSetName() string {
	client, err := c.OpenDB()
	if err != nil {
		log.Printf("MongoDB bağlantısı kurulamadı: %v", err)
		return "Unknown"
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("MongoDB bağlantısı kapatılamadı: %v", err)
		}
	}()

	// replSetGetStatus komutunu çalıştır
	status, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		log.Printf("replSetGetStatus komutu çalıştırılamadı: %v", err)
		return "Unknown"
	}

	// Replica set adını al
	replSetName, ok := status["set"].(string)
	if !ok {
		log.Printf("replica set adı bulunamadı")
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
		log.Printf("MongoDB bağlantısı kurulamadı: %v", err)
		return 0
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("MongoDB bağlantısı kapatılamadı: %v", err)
		}
	}()

	// Önce serverStatus ile mevcut node bilgilerini al
	serverStatus, err := c.runAdminCommand(client, bson.D{{Key: "serverStatus", Value: 1}})
	if err != nil {
		log.Printf("serverStatus komutu çalıştırılamadı: %v", err)
		return 0
	}

	host, ok := serverStatus["host"].(string)
	if !ok {
		log.Printf("Host bilgisi bulunamadı")
		host = "localhost"
	}

	// replSetGetStatus komutunu çalıştır
	status, err := c.runAdminCommand(client, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err != nil {
		log.Printf("replSetGetStatus komutu çalıştırılamadı: %v", err)
		return 0
	}

	// Member'ları kontrol et
	members, ok := status["members"].(bson.A)
	if !ok {
		log.Printf("members dizisi bulunamadı")
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
			log.Printf("Node %s için optimeDate bulunamadı", memberHost)
			continue
		}

		// Kendi node'um mu? (host name'e göre)
		if strings.Contains(memberHost, currentHost) || strings.Contains(currentHost, memberHost) {
			myOptimeDate = optime
			foundMe = true
			log.Printf("Kendi node'umu buldum: %s, optimeDate: %v", memberHost, optime)
		}

		// PRIMARY node mu?
		isPrimary := state == 1
		if hasStateStr {
			isPrimary = isPrimary || stateStr == "PRIMARY"
		}

		if isPrimary {
			primaryOptimeDate = optime
			foundPrimary = true
			log.Printf("PRIMARY node'u buldum: %s, optimeDate: %v", memberHost, optime)
		}
	}

	// Eğer gerekli bilgiler bulunamadıysa 0 döndür
	if !foundMe || !foundPrimary {
		log.Printf("Lag hesaplanamadı: foundMe=%v, foundPrimary=%v", foundMe, foundPrimary)
		return 0
	}

	// Lag'i hesapla (saniye cinsinden)
	lag := primaryOptimeDate.Sub(myOptimeDate).Seconds()
	if lag < 0 {
		lag = 0 // Negatif lag olamaz
	}

	log.Printf("Replikasyon lag: %.2f saniye", lag)
	return int64(lag)
}

// GetDiskUsage disk kullanım bilgilerini döndürür
func (c *MongoCollector) GetDiskUsage() (string, int) {
	// df komutunu çalıştır
	cmd := exec.Command("df", "-h")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("Disk bilgileri alınamadı: %v", err)
		return "N/A", 0
	}

	// Çıktıyı satırlara böl
	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return "N/A", 0
	}

	var maxSize uint64 = 0
	var selectedFree string
	var selectedUsage int

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
			continue
		}

		// Boyut bilgisini parse et
		size := fields[1]
		free := fields[3]
		usage := strings.TrimSuffix(fields[4], "%")

		// Boyutu byte cinsine çevir
		sizeInBytes, err := c.convertToBytes(size)
		if err != nil {
			continue
		}

		// En büyük diski veya root dizinini seç
		if sizeInBytes > maxSize || mountPoint == "/" {
			maxSize = sizeInBytes
			selectedFree = free
			selectedUsage, _ = strconv.Atoi(usage)
		}
	}

	if maxSize == 0 {
		return "N/A", 0
	}

	return selectedFree, selectedUsage
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

	return uint64(num * float64(multiplier)), nil
}

// GetMongoInfo MongoDB bilgilerini toplar
func (c *MongoCollector) GetMongoInfo() *MongoInfo {
	hostname, _ := os.Hostname()
	ip := c.getLocalIP()
	freeDisk, usagePercent := c.GetDiskUsage()

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
	}

	log.Printf("DEBUG: MongoDB bilgileri hazırlandı - Port: %s, Status: %s, Version: %s",
		info.Port, info.MongoStatus, info.MongoVersion)

	return info
}

// getLocalIP yerel IP adresini döndürür
func (c *MongoCollector) getLocalIP() string {
	cmd := exec.Command("sh", "-c", "hostname -I | awk '{print $1}'")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("IP adresi alınamadı: %v", err)
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
		log.Printf("Dosya tanımlayıcı sayısı alınamadı: %v", err)
		return 0
	}

	fdCount, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		log.Printf("Dosya tanımlayıcı sayısı dönüştürülemedi: %v", err)
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
	return &pb.MongoInfo{
		ClusterName:       m.ClusterName,
		Ip:                m.IP,
		Hostname:          m.Hostname,
		NodeStatus:        m.NodeStatus,
		MongoVersion:      m.MongoVersion,
		Location:          m.Location,
		MongoStatus:       m.MongoStatus,
		ReplicaSetName:    m.ReplicaSetName,
		ReplicationLagSec: m.ReplicaLagSec,
		FreeDisk:          m.FreeDisk,
		FdPercent:         m.FdPercent,
		Port:              m.Port,
	}
}

// FindMongoLogFiles MongoDB log dosyalarını bulur ve listeler
func (c *MongoCollector) FindMongoLogFiles(logPath string) ([]*pb.MongoLogFile, error) {
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
					log.Printf("MongoDB log dosyası bağlamından log dizini belirlendi: %s", parentDir)
					// Dosya yerine dizini kullan
					logPath = parentDir
				} else {
					// Path zaten bir dizin
					logPath = logPathFromProcess
					log.Printf("MongoDB log dizini süreç parametrelerinden bulundu: %s", logPath)
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
					log.Printf("MongoDB log dizini konfigürasyon dosyasından bulundu: %s", path)
				}
			}

			// MongoDB'nin çalıştığı dizini bulmayı dene
			mongoDataDir := findMongoDBDataDir()
			if mongoDataDir != "" {
				// Data dizinindeki log dizinini kontrol et
				logDirs = append([]string{mongoDataDir + "/log", mongoDataDir}, logDirs...)
				log.Printf("MongoDB veri dizini bulundu, log için kontrol ediliyor: %s/log", mongoDataDir)
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
						log.Printf("MongoDB log dizini bulundu: %s", logPath)
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
					log.Printf("MongoDB log dosyası bulundu, dizini kontrol edilecek: %s", parentDir)
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
							log.Printf("MongoDB log dosyası açık dosya tanımlayıcıları ile bulundu: %s", mongoLogFile)
							return []*pb.MongoLogFile{file}, nil
						}
					}
				}

				if logPath == "" {
					logPath = "/var/log"
					log.Printf("MongoDB özel log dizini bulunamadı, genel log dizini kontrol ediliyor: %s", logPath)
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
		log.Printf("DEBUG: Dizindeki tüm dosyaları listeliyorum: %s", logPath)
		dirEntries, err := os.ReadDir(logPath)
		if err == nil {
			for _, entry := range dirEntries {
				fileName := entry.Name()
				isArtifact := isMongoDBArtifact(fileName)
				isMatching := isMatchingMongoDBLogName(strings.ToLower(fileName))
				log.Printf("DEBUG: Dosya: %s | MongoDB Artifact: %t | Matching Name: %t",
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
			log.Printf("MongoDB tek log dosyası bulundu: %s", logPath)
			return append(logFiles, file), nil
		}
		log.Printf("Belirtilen dosya MongoDB log dosyası değil, dizini kontrol ediliyor: %s", filepath.Dir(logPath))
		// Dosyanın dizinini dene
		logPath = filepath.Dir(logPath)
		info, err = os.Stat(logPath)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("belirtilen dosya bir MongoDB log dosyası değil ve geçerli bir dizin de değil: %s", logPath)
		}
	}

	// Buraya geldiysek, logPath bir dizindir
	log.Printf("MongoDB log dizini için dosyalar taranıyor: %s", logPath)

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
				log.Printf("Dosya bilgileri alınamadı: %v", err)
				continue
			}

			file := &pb.MongoLogFile{
				Name:         entry.Name(),
				Path:         filepath.Join(logPath, entry.Name()),
				Size:         fileInfo.Size(),
				LastModified: fileInfo.ModTime().Unix(),
			}
			logFiles = append(logFiles, file)
			log.Printf("MongoDB log dosyası bulundu: %s", file.Path)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("log dosyaları listelenirken hata: %v", err)
	}

	// Hiç log dosyası bulunamadıysa bilgilendirme mesajı döndür
	if len(logFiles) == 0 {
		log.Printf("Belirtilen dizinde (%s) MongoDB log dosyası bulunamadı", logPath)

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
				log.Printf("MongoDB log dosyası file descriptor ile bulundu: %s", logFilePath)
				return []*pb.MongoLogFile{file}, nil
			}
		}

		// MongoDB çalışıyor ama log dosyası bulunamadı
		log.Printf("MongoDB servisi çalışıyor ama log dosyaları bulunamadı. MongoDB log dosyası konumunu kontrol edin.")
	} else {
		log.Printf("%d adet MongoDB log dosyası bulundu", len(logFiles))
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
	// MongoDB konfigürasyon dosyası için olası yerler
	configPaths := []string{
		"/etc/mongod.conf",
		"/etc/mongodb.conf",
		"/usr/local/bin/mongod1.conf", // macOS Homebrew
	}

	for _, path := range configPaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

// getLogPathFromConfig MongoDB konfigürasyon dosyasından log path'ini çıkarır
func getLogPathFromConfig(configFile string) string {
	// Dosyayı oku
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Printf("Konfigürasyon dosyası okunamadı: %v", err)
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
		status.ErrorMessage = "MongoDB process bulunamadı"
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
		log.Printf("ALARM: MongoDB node PRIMARY'den SECONDARY'ye düştü!")
	}

	// 2. SECONDARY -> PRIMARY geçiş (bu node secondary iken primary olmuş)
	if prevStatus.CurrentState == "SECONDARY" && currentStatus.CurrentState == "PRIMARY" {
		isFailover = true
		log.Printf("ALARM: MongoDB node SECONDARY'den PRIMARY'ye yükseldi!")
	}

	// 3. Servis durumu değişiklikleri
	if prevStatus.Status == "RUNNING" && currentStatus.Status != "RUNNING" {
		isFailover = true
		log.Printf("ALARM: MongoDB servis durumu değişti! Önceki: %s, Şimdiki: %s, Hata: %s",
			prevStatus.Status, currentStatus.Status, currentStatus.ErrorMessage)
	}

	return isFailover
}
