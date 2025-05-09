package collector

type PostgresInfo struct {
	ClusterName       string
	IP                string
	Hostname          string
	NodeStatus        string
	PGVersion         string
	Location          string
	PGBouncerStatus   string
	PGServiceStatus   string
	FreeDisk          string
	FDPercent         int
	ReplicationLagSec float64
	TotalvCpu         int32  // Toplam vCPU sayısı
	TotalMemory       int64  // Toplam RAM miktarı (byte cinsinden)
	ConfigPath        string // PostgreSQL configuration file path
}

type MSSQLInfo struct {
	ClusterName string
	IP          string
	Hostname    string
	NodeStatus  string // PRIMARY, SECONDARY, STANDALONE
	Version     string
	Location    string
	Status      string // RUNNING, FAIL!, etc.
	Instance    string
	FreeDisk    string
	FDPercent   int
	Port        string // SQL Server port
	TotalvCpu   int32  // Toplam vCPU sayısı
	TotalMemory int64  // Toplam RAM miktarı (byte cinsinden)
	ConfigPath  string // MSSQL configuration path
	Database    string // Database name
	IsHAEnabled bool   // AlwaysOn configuration enabled
	HARole      string // Role in HA topology
	Edition     string // SQL Server edition
}
