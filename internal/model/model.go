package model

import (
	"time"
)

// Response API yanıtı için standart yapı
type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

// SystemData agent tarafından toplanan tüm sistem verilerini içerir
type SystemData struct {
	AgentKey   string          `json:"agent_key"`
	AgentName  string          `json:"agent_name"`
	Timestamp  int64           `json:"timestamp"`
	PostgreSQL *PostgreSQLData `json:"postgresql,omitempty"`
	MongoDB    *MongoDBData    `json:"mongodb,omitempty"`
	MSSQL      *MSSQLData      `json:"mssql,omitempty"`
	System     *SystemMetrics  `json:"system"`
}

// PostgreSQLData PostgreSQL veritabanı verilerini içerir
type PostgreSQLData struct {
	Status       string  `json:"status"`
	Connections  int     `json:"connections"`
	Transactions int     `json:"transactions"`
	QueryTime    float64 `json:"query_time"`
	// Diğer PostgreSQL metrikleri
}

// MongoDBData MongoDB veritabanı verilerini içerir
type MongoDBData struct {
	Status       string  `json:"status"`
	Connections  int     `json:"connections"`
	Operations   int     `json:"operations"`
	ResponseTime float64 `json:"response_time"`
	// Diğer MongoDB metrikleri
}

// MSSQLData MSSQL veritabanı verilerini içerir
type MSSQLData struct {
	Status      string `json:"status"`
	Connections int    `json:"connections"`
	// Diğer MSSQL metrikleri
}

// SystemMetrics sistem kaynak kullanım metriklerini içerir
type SystemMetrics struct {
	CPUUsage    float64 `json:"cpu_usage"`
	CPUCores    int32   `json:"cpu_cores"`
	MemoryUsage float64 `json:"memory_usage"`
	DiskUsage   float64 `json:"disk_usage"`
	// Diğer sistem metrikleri
}

// GetCurrentTimestamp şu anki zamanı ISO 8601 formatında döndürür
func GetCurrentTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// Diğer model yapıları buraya eklenebilir
