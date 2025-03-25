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
}
