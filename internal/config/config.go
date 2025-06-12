package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v2"
)

// AgentConfig, Agent için konfigürasyon yapısı
type AgentConfig struct {
	// Temel Bilgiler
	Key  string `yaml:"key"`
	Name string `yaml:"name"`

	// PostgreSQL Bağlantı Bilgileri
	PostgreSQL struct {
		Host            string `yaml:"host"`
		User            string `yaml:"user"`
		Pass            string `yaml:"pass"`
		Port            string `yaml:"port"`
		Cluster         string `yaml:"cluster"`
		Auth            bool   `yaml:"-"` // Auth, dolaylı olarak belirlenir
		Location        string `yaml:"location"`
		ReplicationUser string `yaml:"replication_user"`
		ReplicationPass string `yaml:"replication_password"`
	} `yaml:"postgresql"`

	// MongoDB Bağlantı Bilgileri
	Mongo struct {
		Host     string `yaml:"host"`
		User     string `yaml:"user"`
		Pass     string `yaml:"pass"`
		Port     string `yaml:"port"`
		Replset  string `yaml:"replset"`
		Auth     bool   `yaml:"-"` // Auth, dolaylı olarak belirlenir
		Location string `yaml:"location"`
	} `yaml:"mongo"`

	// MSSQL Bağlantı Bilgileri
	MSSQL struct {
		Host        string `yaml:"host"`
		User        string `yaml:"user"`
		Pass        string `yaml:"pass"`
		Port        string `yaml:"port"`
		Instance    string `yaml:"instance"`
		Database    string `yaml:"database"`
		Auth        bool   `yaml:"-"` // Auth, dolaylı olarak belirlenir
		Location    string `yaml:"location"`
		TrustCert   bool   `yaml:"trust_cert"`
		WindowsAuth bool   `yaml:"windows_auth"`
	} `yaml:"mssql"`

	// GRPC Ayarları
	GRPC struct {
		ServerAddress string `yaml:"server_address"`
	} `yaml:"grpc"`
}

// LoadAgentConfig, agent konfigürasyonunu yükler
func LoadAgentConfig() (*AgentConfig, error) {
	// Konfigürasyon dosyasının yerini belirle
	configPath := getConfigPath("agent.yml")

	// Dosya var mı kontrol et
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Dosya yoksa varsayılan konfigürasyonu oluştur ve kaydet
		return createDefaultAgentConfig(configPath)
	}

	// Dosyayı oku
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("konfigürasyon dosyası okunamadı: %w", err)
	}

	// YAML'ı parse et
	var config AgentConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("konfigürasyon dosyası ayrıştırılamadı: %w", err)
	}

	// Auth değerlerini belirle
	config.PostgreSQL.Auth = config.PostgreSQL.User != "" && config.PostgreSQL.Pass != ""
	config.Mongo.Auth = config.Mongo.User != "" && config.Mongo.Pass != ""
	config.MSSQL.Auth = config.MSSQL.User != "" && config.MSSQL.Pass != "" && !config.MSSQL.WindowsAuth

	return &config, nil
}

// createDefaultAgentConfig, varsayılan agent konfigürasyonunu oluşturur
func createDefaultAgentConfig(configPath string) (*AgentConfig, error) {
	// Varsayılan ayarlar
	config := &AgentConfig{}
	config.Key = "agent_key"
	config.Name = "Agent"
	config.GRPC.ServerAddress = "localhost:50051"

	// PostgreSQL varsayılan ayarları
	config.PostgreSQL.Cluster = "postgres"
	config.PostgreSQL.Port = "5432"

	// MongoDB varsayılan ayarları
	config.Mongo.Replset = "rs0"
	config.Mongo.Port = "27017"

	// MSSQL varsayılan ayarları
	config.MSSQL.Port = "1433"
	config.MSSQL.Database = "master"
	config.MSSQL.TrustCert = true

	// Konfigürasyonu YAML olarak dönüştür
	data, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("varsayılan konfigürasyon oluşturulamadı: %w", err)
	}

	// Dizin yoksa oluştur
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("konfigürasyon dizini oluşturulamadı: %w", err)
	}

	// Dosyayı yaz
	if err := ioutil.WriteFile(configPath, data, 0644); err != nil {
		return nil, fmt.Errorf("konfigürasyon dosyası yazılamadı: %w", err)
	}

	return config, nil
}

// getConfigPath returns the absolute config file path based on OS.
func getConfigPath(filename string) string {
	// 2. Windows: executable dir
	if runtime.GOOS == "windows" {
		log.Printf("Windows OS detected")
		exePath, err := os.Executable()
		log.Printf("exePath: %s", exePath)
		if err == nil {
			exeDir := filepath.Dir(exePath)
			winPath := filepath.Join(exeDir, filename)
			log.Printf("winPath: %s", winPath)
			if _, err := os.Stat(winPath); err == nil {
				log.Printf("Found config near executable: %s", winPath)
				return winPath
			}
			log.Printf("Config not found in: %s", winPath)
		}
	}
	// 1. Current dir
	if _, err := os.Stat(filename); err == nil {
		return filename
	}

	// 3. Linux fallback
	etcPath := filepath.Join("/etc", "clustereye", filename)
	if _, err := os.Stat(etcPath); err == nil {
		log.Printf("Found config in: %s", etcPath)
		return etcPath
	}

	log.Printf("Config not found, fallback to working dir: %s", filename)
	return filename
}
