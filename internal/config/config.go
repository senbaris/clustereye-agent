package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

// AgentConfig, Agent için konfigürasyon yapısı
type AgentConfig struct {
	// Temel Bilgiler
	Key  string `yaml:"key"`
	Name string `yaml:"name"`

	// PostgreSQL Bağlantı Bilgileri
	PostgreSQL struct {
		Host    string `yaml:"host"`
		User    string `yaml:"user"`
		Pass    string `yaml:"pass"`
		Port    string `yaml:"port"`
		Cluster string `yaml:"cluster"`
		Auth    bool   `yaml:"-"` // Auth, dolaylı olarak belirlenir
	} `yaml:"postgresql"`

	// MongoDB Bağlantı Bilgileri
	Mongo struct {
		User    string `yaml:"user"`
		Pass    string `yaml:"pass"`
		Port    string `yaml:"port"`
		Replset string `yaml:"replset"`
		Auth    bool   `yaml:"-"` // Auth, dolaylı olarak belirlenir
	} `yaml:"mongo"`

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

// getConfigPath, konfigürasyon dosyasının tam yolunu döndürür
func getConfigPath(filename string) string {
	// Önce çalışma dizinine bakar, sonra /etc/clustereye dizinine bakar
	if _, err := os.Stat(filename); err == nil {
		return filename
	}

	// /etc/clustereye dizinine bak
	etcPath := filepath.Join("/etc", "clustereye", filename)
	if _, err := os.Stat(etcPath); err == nil {
		return etcPath
	}

	// Dosya bulunamadıysa, çalışma dizinine yaz
	return filename
}
