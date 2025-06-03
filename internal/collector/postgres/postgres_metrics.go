package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
)

// PostgreSQLMetricsCollector collects time-series metrics for PostgreSQL
type PostgreSQLMetricsCollector struct {
	cfg       *config.AgentConfig
	collector *PostgresCollector
}

// MetricValue represents a metric value with different types
type MetricValue struct {
	DoubleValue *float64
	IntValue    *int64
	StringValue *string
	BoolValue   *bool
}

// MetricTag represents a key-value tag for metrics
type MetricTag struct {
	Key   string
	Value string
}

// Metric represents a single metric with metadata
type Metric struct {
	Name        string
	Value       MetricValue
	Tags        []MetricTag
	Timestamp   int64 // Unix timestamp in nanoseconds
	Unit        string
	Description string
}

// MetricBatch represents a batch of metrics
type MetricBatch struct {
	AgentID             string
	MetricType          string
	Metrics             []Metric
	CollectionTimestamp int64
	Metadata            map[string]string
}

// NewPostgreSQLMetricsCollector creates a new PostgreSQL metrics collector
func NewPostgreSQLMetricsCollector(cfg *config.AgentConfig) *PostgreSQLMetricsCollector {
	return &PostgreSQLMetricsCollector{
		cfg:       cfg,
		collector: NewPostgresCollector(cfg),
	}
}

// CollectSystemMetrics collects system-level metrics
func (p *PostgreSQLMetricsCollector) CollectSystemMetrics() (*MetricBatch, error) {
	timestamp := time.Now().UnixNano()
	var metrics []Metric

	// Get system metrics from existing collector
	systemMetrics := p.collector.BatchCollectSystemMetrics()

	// Debug logging: Show all collected system metrics
	log.Printf("DEBUG: PostgreSQL CollectSystemMetrics - Raw system metrics collected: %d items", len(systemMetrics))
	for key, value := range systemMetrics {
		log.Printf("DEBUG: PostgreSQL SystemMetric[%s] = %v (type: %T)", key, value, value)
	}

	// CPU Metrics
	if cpuUsage, ok := systemMetrics["cpu_usage"].(float64); ok {
		log.Printf("DEBUG: PostgreSQL - Adding CPU usage metric: %.2f", cpuUsage)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.cpu_usage",
			Value:       MetricValue{DoubleValue: &cpuUsage},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "CPU usage percentage",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - CPU usage metric not found or wrong type in system metrics")
	}

	if cpuCount, ok := systemMetrics["cpu_count"].(int32); ok {
		cores := int64(cpuCount)
		log.Printf("DEBUG: PostgreSQL - Adding CPU cores metric: %d", cores)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.cpu_cores",
			Value:       MetricValue{IntValue: &cores},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of CPU cores",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - CPU count metric not found or wrong type in system metrics")
	}

	// Memory Metrics
	if memUsage, ok := systemMetrics["memory_usage"].(float64); ok {
		log.Printf("DEBUG: PostgreSQL - Adding memory usage metric: %.2f", memUsage)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.memory_usage",
			Value:       MetricValue{DoubleValue: &memUsage},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Memory usage percentage",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - Memory usage metric not found or wrong type in system metrics")
	}

	if totalMem, ok := systemMetrics["total_memory"].(int64); ok {
		log.Printf("DEBUG: PostgreSQL - Adding total memory metric: %d bytes", totalMem)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.total_memory",
			Value:       MetricValue{IntValue: &totalMem},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total system memory",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - Total memory metric not found or wrong type in system metrics")
	}

	if freeMem, ok := systemMetrics["free_memory"].(int64); ok {
		log.Printf("DEBUG: PostgreSQL - Adding free memory metric: %d bytes", freeMem)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.free_memory",
			Value:       MetricValue{IntValue: &freeMem},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free system memory",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - Free memory metric not found or wrong type in system metrics")
	}

	// Disk Metrics
	if totalDisk, ok := systemMetrics["total_disk"].(int64); ok {
		log.Printf("DEBUG: PostgreSQL - Adding total disk metric: %d bytes", totalDisk)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.total_disk",
			Value:       MetricValue{IntValue: &totalDisk},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total disk space",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - Total disk metric not found or wrong type in system metrics")
	}

	if freeDisk, ok := systemMetrics["free_disk"].(int64); ok {
		log.Printf("DEBUG: PostgreSQL - Adding free disk metric: %d bytes", freeDisk)
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.free_disk",
			Value:       MetricValue{IntValue: &freeDisk},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free disk space",
		})
	} else {
		log.Printf("DEBUG: PostgreSQL - Free disk metric not found or wrong type in system metrics")
	}

	// Response Time Metric
	if responseTime, ok := systemMetrics["response_time_ms"].(float64); ok {
		metrics = append(metrics, Metric{
			Name:        "postgresql.system.response_time_ms",
			Value:       MetricValue{DoubleValue: &responseTime},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "milliseconds",
			Description: "PostgreSQL response time for SELECT 1 query in milliseconds",
		})
	} else {
		if _, exists := systemMetrics["response_time_ms"]; exists {
		} else {
			log.Printf("DEBUG: PostgreSQL - response_time_ms key does not exist in systemMetrics")
		}
	}

	return &MetricBatch{
		AgentID:             p.getAgentID(),
		MetricType:          "postgresql_system",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform": "postgresql",
			"os":       runtime.GOOS,
		},
	}, nil
}

// CollectDatabaseMetrics collects database-specific metrics
func (p *PostgreSQLMetricsCollector) CollectDatabaseMetrics() (*MetricBatch, error) {
	timestamp := time.Now().UnixNano()
	var metrics []Metric

	db, err := p.collector.openDB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}
	defer db.Close()

	// Collect various database metrics
	p.collectConnectionMetrics(db, &metrics, timestamp)
	p.collectDatabaseStats(db, &metrics, timestamp)
	p.collectLockMetrics(db, &metrics, timestamp)
	p.collectReplicationMetrics(db, &metrics, timestamp)
	p.collectTableMetrics(db, &metrics, timestamp)
	p.collectIndexMetrics(db, &metrics, timestamp)

	return &MetricBatch{
		AgentID:             p.getAgentID(),
		MetricType:          "postgresql_database",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform": "postgresql",
			"database": "postgres", // PostgreSQL default database
		},
	}, nil
}

// collectConnectionMetrics collects connection-related metrics
func (p *PostgreSQLMetricsCollector) collectConnectionMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			COUNT(*) as total_connections,
			COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
			COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
			COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_transaction
		FROM pg_stat_activity 
		WHERE pid <> pg_backend_pid()
	`

	var totalConn, activeConn, idleConn, idleInTransaction int64
	err := db.QueryRow(query).Scan(&totalConn, &activeConn, &idleConn, &idleInTransaction)
	if err != nil {
		logger.Error("Failed to collect connection metrics: %v", err)
		return
	}

	*metrics = append(*metrics,
		Metric{
			Name:        "postgresql.connections.total",
			Value:       MetricValue{IntValue: &totalConn},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Total number of connections",
		},
		Metric{
			Name:        "postgresql.connections.active",
			Value:       MetricValue{IntValue: &activeConn},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of active connections",
		},
		Metric{
			Name:        "postgresql.connections.idle",
			Value:       MetricValue{IntValue: &idleConn},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of idle connections",
		},
		Metric{
			Name:        "postgresql.connections.idle_in_transaction",
			Value:       MetricValue{IntValue: &idleInTransaction},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of idle in transaction connections",
		},
	)
}

// collectDatabaseStats collects database statistics
func (p *PostgreSQLMetricsCollector) collectDatabaseStats(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			datname,
			numbackends,
			xact_commit,
			xact_rollback,
			blks_read,
			blks_hit,
			tup_returned,
			tup_fetched,
			tup_inserted,
			tup_updated,
			tup_deleted,
			deadlocks,
			temp_files,
			temp_bytes
		FROM pg_stat_database 
		WHERE datname NOT IN ('template0', 'template1')
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect database stats: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var datname string
		var numbackends, xactCommit, xactRollback, blksRead, blksHit int64
		var tupReturned, tupFetched, tupInserted, tupUpdated, tupDeleted int64
		var deadlocks, tempFiles, tempBytes int64

		if err := rows.Scan(&datname, &numbackends, &xactCommit, &xactRollback,
			&blksRead, &blksHit, &tupReturned, &tupFetched, &tupInserted,
			&tupUpdated, &tupDeleted, &deadlocks, &tempFiles, &tempBytes); err != nil {
			continue
		}

		tags := []MetricTag{
			{Key: "host", Value: p.getHostname()},
			{Key: "database", Value: datname},
		}

		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.database.backends",
				Value:       MetricValue{IntValue: &numbackends},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of backends connected to this database",
			},
			Metric{
				Name:        "postgresql.database.xact_commit",
				Value:       MetricValue{IntValue: &xactCommit},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of transactions committed",
			},
			Metric{
				Name:        "postgresql.database.xact_rollback",
				Value:       MetricValue{IntValue: &xactRollback},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of transactions rolled back",
			},
			Metric{
				Name:        "postgresql.database.blks_read",
				Value:       MetricValue{IntValue: &blksRead},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of disk blocks read",
			},
			Metric{
				Name:        "postgresql.database.blks_hit",
				Value:       MetricValue{IntValue: &blksHit},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of buffer hits",
			},
			Metric{
				Name:        "postgresql.database.deadlocks",
				Value:       MetricValue{IntValue: &deadlocks},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of deadlocks detected",
			},
		)
	}
}

// collectLockMetrics collects lock-related metrics
func (p *PostgreSQLMetricsCollector) collectLockMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			mode,
			COUNT(*) as lock_count
		FROM pg_locks 
		WHERE granted = true
		GROUP BY mode
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect lock metrics: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var mode string
		var lockCount int64

		if err := rows.Scan(&mode, &lockCount); err != nil {
			continue
		}

		*metrics = append(*metrics, Metric{
			Name:        "postgresql.locks.granted",
			Value:       MetricValue{IntValue: &lockCount},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}, {Key: "mode", Value: mode}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of granted locks by mode",
		})
	}

	// Get blocking locks
	blockingQuery := `
		SELECT COUNT(*) as blocking_locks
		FROM pg_locks 
		WHERE NOT granted
	`

	var blockingLocks int64
	err = db.QueryRow(blockingQuery).Scan(&blockingLocks)
	if err == nil {
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.locks.waiting",
			Value:       MetricValue{IntValue: &blockingLocks},
			Tags:        []MetricTag{{Key: "host", Value: p.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of waiting locks",
		})
	}
}

// collectReplicationMetrics collects replication-related metrics
func (p *PostgreSQLMetricsCollector) collectReplicationMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	// Check if this is a primary server
	replicationQuery := `
		SELECT 
			client_addr,
			state,
			sent_lsn,
			write_lsn,
			flush_lsn,
			replay_lsn,
			COALESCE(EXTRACT(epoch FROM write_lag), 0) as write_lag_seconds,
			COALESCE(EXTRACT(epoch FROM flush_lag), 0) as flush_lag_seconds,
			COALESCE(EXTRACT(epoch FROM replay_lag), 0) as replay_lag_seconds
		FROM pg_stat_replication
	`

	rows, err := db.Query(replicationQuery)
	if err != nil {
		// Not a primary server or replication not configured
		return
	}
	defer rows.Close()

	for rows.Next() {
		var clientAddr, state, sentLsn, writeLsn, flushLsn, replayLsn string
		var writeLag, flushLag, replayLag float64

		if err := rows.Scan(&clientAddr, &state, &sentLsn, &writeLsn, &flushLsn,
			&replayLsn, &writeLag, &flushLag, &replayLag); err != nil {
			continue
		}

		tags := []MetricTag{
			{Key: "host", Value: p.getHostname()},
			{Key: "client_addr", Value: clientAddr},
			{Key: "state", Value: state},
		}

		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.replication.write_lag",
				Value:       MetricValue{DoubleValue: &writeLag},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "seconds",
				Description: "Write lag in seconds",
			},
			Metric{
				Name:        "postgresql.replication.flush_lag",
				Value:       MetricValue{DoubleValue: &flushLag},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "seconds",
				Description: "Flush lag in seconds",
			},
			Metric{
				Name:        "postgresql.replication.replay_lag",
				Value:       MetricValue{DoubleValue: &replayLag},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "seconds",
				Description: "Replay lag in seconds",
			},
		)
	}
}

// collectTableMetrics collects table-level metrics
func (p *PostgreSQLMetricsCollector) collectTableMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			schemaname,
			tablename,
			seq_scan,
			seq_tup_read,
			idx_scan,
			idx_tup_fetch,
			n_tup_ins,
			n_tup_upd,
			n_tup_del,
			n_live_tup,
			n_dead_tup
		FROM pg_stat_user_tables
		ORDER BY seq_scan + idx_scan DESC
		LIMIT 10
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect table metrics: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var schemaname, tablename string
		var seqScan, seqTupRead, idxScan, idxTupFetch int64
		var nTupIns, nTupUpd, nTupDel, nLiveTup, nDeadTup int64

		if err := rows.Scan(&schemaname, &tablename, &seqScan, &seqTupRead,
			&idxScan, &idxTupFetch, &nTupIns, &nTupUpd, &nTupDel,
			&nLiveTup, &nDeadTup); err != nil {
			continue
		}

		tags := []MetricTag{
			{Key: "host", Value: p.getHostname()},
			{Key: "schema", Value: schemaname},
			{Key: "table", Value: tablename},
		}

		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.table.seq_scan",
				Value:       MetricValue{IntValue: &seqScan},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of sequential scans",
			},
			Metric{
				Name:        "postgresql.table.idx_scan",
				Value:       MetricValue{IntValue: &idxScan},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of index scans",
			},
			Metric{
				Name:        "postgresql.table.live_tuples",
				Value:       MetricValue{IntValue: &nLiveTup},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of live tuples",
			},
			Metric{
				Name:        "postgresql.table.dead_tuples",
				Value:       MetricValue{IntValue: &nDeadTup},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of dead tuples",
			},
		)
	}
}

// collectIndexMetrics collects index-level metrics
func (p *PostgreSQLMetricsCollector) collectIndexMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			schemaname,
			tablename,
			indexname,
			idx_scan,
			idx_tup_read,
			idx_tup_fetch
		FROM pg_stat_user_indexes
		ORDER BY idx_scan DESC
		LIMIT 10
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect index metrics: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var schemaname, tablename, indexname string
		var idxScan, idxTupRead, idxTupFetch int64

		if err := rows.Scan(&schemaname, &tablename, &indexname,
			&idxScan, &idxTupRead, &idxTupFetch); err != nil {
			continue
		}

		tags := []MetricTag{
			{Key: "host", Value: p.getHostname()},
			{Key: "schema", Value: schemaname},
			{Key: "table", Value: tablename},
			{Key: "index", Value: indexname},
		}

		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.index.scans",
				Value:       MetricValue{IntValue: &idxScan},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of index scans",
			},
			Metric{
				Name:        "postgresql.index.tuples_read",
				Value:       MetricValue{IntValue: &idxTupRead},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of index tuples read",
			},
		)
	}
}

// getHostname returns the hostname for tagging
func (p *PostgreSQLMetricsCollector) getHostname() string {
	if p.cfg.PostgreSQL.Host != "" {
		return p.cfg.PostgreSQL.Host
	}
	return "localhost"
}

// getAgentID returns the proper agent ID in the format "agent_<hostname>"
func (p *PostgreSQLMetricsCollector) getAgentID() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	return "agent_" + hostname
}

// CollectAllMetrics collects both system and database metrics
func (p *PostgreSQLMetricsCollector) CollectAllMetrics() ([]*MetricBatch, error) {
	var batches []*MetricBatch

	// Collect system metrics
	systemBatch, err := p.CollectSystemMetrics()
	if err != nil {
		log.Printf("Failed to collect PostgreSQL system metrics: %v", err)
	} else {
		batches = append(batches, systemBatch)
	}

	// Collect database metrics
	dbBatch, err := p.CollectDatabaseMetrics()
	if err != nil {
		log.Printf("Failed to collect PostgreSQL database metrics: %v", err)
	} else {
		batches = append(batches, dbBatch)
	}

	return batches, nil
}
