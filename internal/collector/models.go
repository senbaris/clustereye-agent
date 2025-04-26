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
