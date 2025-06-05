package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
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

	// Collect query performance metrics
	p.collectQueryPerformanceMetrics(db, &metrics, timestamp)

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
	tags := []MetricTag{{Key: "host", Value: p.getHostname()}}

	// Basic connection metrics
	query := `
		SELECT 
			COUNT(*) as total_connections,
			COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
			COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
			COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_transaction,
			COUNT(CASE WHEN state = 'idle in transaction (aborted)' THEN 1 END) as idle_in_transaction_aborted,
			COUNT(CASE WHEN state = 'fastpath function call' THEN 1 END) as fastpath_function_call,
			COUNT(CASE WHEN state = 'disabled' THEN 1 END) as disabled_connections,
			AVG(EXTRACT(epoch FROM (now() - backend_start))) as avg_connection_age_seconds
		FROM pg_stat_activity 
		WHERE pid <> pg_backend_pid()
	`

	var totalConn, activeConn, idleConn, idleInTransaction int64
	var idleInTransactionAborted, fastpathFunctionCall, disabledConnections int64
	var avgConnectionAge sql.NullFloat64

	err := db.QueryRow(query).Scan(&totalConn, &activeConn, &idleConn, &idleInTransaction,
		&idleInTransactionAborted, &fastpathFunctionCall, &disabledConnections, &avgConnectionAge)
	if err != nil {
		logger.Error("Failed to collect connection metrics: %v", err)
		return
	}

	*metrics = append(*metrics,
		Metric{
			Name:        "postgresql.connections.total",
			Value:       MetricValue{IntValue: &totalConn},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Total number of connections",
		},
		Metric{
			Name:        "postgresql.connections.active",
			Value:       MetricValue{IntValue: &activeConn},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of active connections",
		},
		Metric{
			Name:        "postgresql.connections.idle",
			Value:       MetricValue{IntValue: &idleConn},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of idle connections",
		},
		Metric{
			Name:        "postgresql.connections.idle_in_transaction",
			Value:       MetricValue{IntValue: &idleInTransaction},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of idle in transaction connections",
		},
		Metric{
			Name:        "postgresql.connections.idle_in_transaction_aborted",
			Value:       MetricValue{IntValue: &idleInTransactionAborted},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of idle in transaction (aborted) connections",
		},
	)

	// Average connection age
	if avgConnectionAge.Valid {
		avgAge := avgConnectionAge.Float64
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.connections.avg_age_seconds",
			Value:       MetricValue{DoubleValue: &avgAge},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "seconds",
			Description: "Average age of connections in seconds",
		})
	}

	// Collect advanced connection pool metrics
	p.collectConnectionPoolAdvancedMetrics(db, metrics, timestamp, tags)

	// Collect connection limits and utilization
	p.collectConnectionLimitsMetrics(db, metrics, timestamp, tags)

	// Collect connection wait and blocking metrics
	p.collectConnectionWaitMetrics(db, metrics, timestamp, tags)
}

// collectConnectionPoolAdvancedMetrics collects advanced connection pool metrics
func (p *PostgreSQLMetricsCollector) collectConnectionPoolAdvancedMetrics(db *sql.DB, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	// Connection age distribution metrics
	ageQuery := `
		SELECT 
			COUNT(CASE WHEN EXTRACT(epoch FROM (now() - backend_start)) < 60 THEN 1 END) as new_connections,
			COUNT(CASE WHEN EXTRACT(epoch FROM (now() - backend_start)) >= 60 AND EXTRACT(epoch FROM (now() - backend_start)) < 300 THEN 1 END) as young_connections,
			COUNT(CASE WHEN EXTRACT(epoch FROM (now() - backend_start)) >= 300 AND EXTRACT(epoch FROM (now() - backend_start)) < 1800 THEN 1 END) as middle_age_connections,
			COUNT(CASE WHEN EXTRACT(epoch FROM (now() - backend_start)) >= 1800 THEN 1 END) as old_connections,
			MAX(EXTRACT(epoch FROM (now() - backend_start))) as oldest_connection_seconds,
			COUNT(CASE WHEN EXTRACT(epoch FROM (now() - query_start)) > 30 AND state = 'active' THEN 1 END) as long_running_queries
		FROM pg_stat_activity 
		WHERE pid <> pg_backend_pid() AND backend_start IS NOT NULL
	`

	var newConn, youngConn, middleAgeConn, oldConn, longRunningQueries int64
	var oldestConnectionSeconds sql.NullFloat64

	err := db.QueryRow(ageQuery).Scan(&newConn, &youngConn, &middleAgeConn, &oldConn, &oldestConnectionSeconds, &longRunningQueries)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to collect connection age metrics: %v", err)
	} else {
		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.connections.new_connections",
				Value:       MetricValue{IntValue: &newConn},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Connections less than 1 minute old",
			},
			Metric{
				Name:        "postgresql.connections.young_connections",
				Value:       MetricValue{IntValue: &youngConn},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Connections 1-5 minutes old",
			},
			Metric{
				Name:        "postgresql.connections.middle_age_connections",
				Value:       MetricValue{IntValue: &middleAgeConn},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Connections 5-30 minutes old",
			},
			Metric{
				Name:        "postgresql.connections.old_connections",
				Value:       MetricValue{IntValue: &oldConn},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Connections older than 30 minutes",
			},
			Metric{
				Name:        "postgresql.connections.long_running_queries",
				Value:       MetricValue{IntValue: &longRunningQueries},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Active queries running longer than 30 seconds",
			},
		)

		if oldestConnectionSeconds.Valid {
			oldestSeconds := oldestConnectionSeconds.Float64
			*metrics = append(*metrics, Metric{
				Name:        "postgresql.connections.oldest_connection_seconds",
				Value:       MetricValue{DoubleValue: &oldestSeconds},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "seconds",
				Description: "Age of oldest connection in seconds",
			})
		}
	}

	// Connection by application name
	appQuery := `
		SELECT 
			COALESCE(application_name, 'unknown') as app_name,
			COUNT(*) as conn_count
		FROM pg_stat_activity 
		WHERE pid <> pg_backend_pid()
		GROUP BY application_name
		ORDER BY conn_count DESC
		LIMIT 10
	`

	rows, err := db.Query(appQuery)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to collect connections by application: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var appName string
		var connCount int64

		if err := rows.Scan(&appName, &connCount); err != nil {
			continue
		}

		appTags := append(tags, MetricTag{Key: "application", Value: appName})
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.connections.by_application",
			Value:       MetricValue{IntValue: &connCount},
			Tags:        appTags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of connections by application name",
		})
	}

	// Connection by database
	dbQuery := `
		SELECT 
			datname,
			COUNT(*) as conn_count
		FROM pg_stat_activity 
		WHERE pid <> pg_backend_pid() AND datname IS NOT NULL
		GROUP BY datname
		ORDER BY conn_count DESC
	`

	dbRows, err := db.Query(dbQuery)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to collect connections by database: %v", err)
		return
	}
	defer dbRows.Close()

	for dbRows.Next() {
		var dbName string
		var connCount int64

		if err := dbRows.Scan(&dbName, &connCount); err != nil {
			continue
		}

		dbTags := append(tags, MetricTag{Key: "database", Value: dbName})
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.connections.by_database",
			Value:       MetricValue{IntValue: &connCount},
			Tags:        dbTags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of connections by database",
		})
	}
}

// collectConnectionLimitsMetrics collects connection limits and utilization metrics
func (p *PostgreSQLMetricsCollector) collectConnectionLimitsMetrics(db *sql.DB, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	// Get max_connections setting
	maxConnQuery := `SELECT setting FROM pg_settings WHERE name = 'max_connections'`
	var maxConnStr string
	err := db.QueryRow(maxConnQuery).Scan(&maxConnStr)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to get max_connections: %v", err)
		return
	}

	maxConnections, err := strconv.ParseInt(maxConnStr, 10, 64)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to parse max_connections: %v", err)
		return
	}

	*metrics = append(*metrics, Metric{
		Name:        "postgresql.connections.max_connections",
		Value:       MetricValue{IntValue: &maxConnections},
		Tags:        tags,
		Timestamp:   timestamp,
		Unit:        "count",
		Description: "Maximum number of connections allowed",
	})

	// Get current connection count for utilization calculation
	currentConnQuery := `SELECT COUNT(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid()`
	var currentConnections int64
	err = db.QueryRow(currentConnQuery).Scan(&currentConnections)
	if err == nil && maxConnections > 0 {
		utilization := (float64(currentConnections) / float64(maxConnections)) * 100
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.connections.utilization_percent",
			Value:       MetricValue{DoubleValue: &utilization},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Connection pool utilization percentage",
		})

		availableConnections := maxConnections - currentConnections
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.connections.available",
			Value:       MetricValue{IntValue: &availableConnections},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of available connections",
		})
	}

	// Get superuser_reserved_connections
	superuserReservedQuery := `SELECT setting FROM pg_settings WHERE name = 'superuser_reserved_connections'`
	var superuserReservedStr string
	err = db.QueryRow(superuserReservedQuery).Scan(&superuserReservedStr)
	if err == nil {
		if superuserReserved, err := strconv.ParseInt(superuserReservedStr, 10, 64); err == nil {
			*metrics = append(*metrics, Metric{
				Name:        "postgresql.connections.superuser_reserved",
				Value:       MetricValue{IntValue: &superuserReserved},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of connections reserved for superusers",
			})

			// Calculate effective max connections for non-superusers
			effectiveMaxConnections := maxConnections - superuserReserved
			*metrics = append(*metrics, Metric{
				Name:        "postgresql.connections.effective_max_connections",
				Value:       MetricValue{IntValue: &effectiveMaxConnections},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Effective max connections for non-superusers",
			})
		}
	}

	// Connection pool efficiency metrics
	totalConnCreatedQuery := `SELECT sum(numbackends) FROM pg_stat_database WHERE datname IS NOT NULL`
	var totalConnCreated sql.NullInt64
	err = db.QueryRow(totalConnCreatedQuery).Scan(&totalConnCreated)
	if err == nil && totalConnCreated.Valid && totalConnCreated.Int64 > 0 {
		connectionReuse := float64(currentConnections) / float64(totalConnCreated.Int64)
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.connections.reuse_ratio",
			Value:       MetricValue{DoubleValue: &connectionReuse},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "ratio",
			Description: "Connection reuse efficiency ratio",
		})
	}
}

// collectConnectionWaitMetrics collects connection wait time and blocking metrics
func (p *PostgreSQLMetricsCollector) collectConnectionWaitMetrics(db *sql.DB, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	// Query for connections waiting on locks
	waitingQuery := `
		SELECT 
			COUNT(*) as waiting_connections,
			COUNT(CASE WHEN wait_event_type = 'Lock' THEN 1 END) as waiting_on_locks,
			COUNT(CASE WHEN wait_event_type = 'LWLock' THEN 1 END) as waiting_on_lwlocks,
			COUNT(CASE WHEN wait_event_type = 'BufferPin' THEN 1 END) as waiting_on_buffer_pin,
			COUNT(CASE WHEN wait_event_type = 'IO' THEN 1 END) as waiting_on_io
		FROM pg_stat_activity 
		WHERE wait_event IS NOT NULL AND pid <> pg_backend_pid()
	`

	var waitingConnections, waitingOnLocks, waitingOnLwlocks, waitingOnBufferPin, waitingOnIO int64
	err := db.QueryRow(waitingQuery).Scan(&waitingConnections, &waitingOnLocks, &waitingOnLwlocks, &waitingOnBufferPin, &waitingOnIO)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to collect wait metrics: %v", err)
		return
	}

	*metrics = append(*metrics,
		Metric{
			Name:        "postgresql.connections.waiting_total",
			Value:       MetricValue{IntValue: &waitingConnections},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Total connections waiting on events",
		},
		Metric{
			Name:        "postgresql.connections.waiting_on_locks",
			Value:       MetricValue{IntValue: &waitingOnLocks},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Connections waiting on locks",
		},
		Metric{
			Name:        "postgresql.connections.waiting_on_lwlocks",
			Value:       MetricValue{IntValue: &waitingOnLwlocks},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Connections waiting on lightweight locks",
		},
		Metric{
			Name:        "postgresql.connections.waiting_on_buffer_pin",
			Value:       MetricValue{IntValue: &waitingOnBufferPin},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Connections waiting on buffer pin",
		},
		Metric{
			Name:        "postgresql.connections.waiting_on_io",
			Value:       MetricValue{IntValue: &waitingOnIO},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Connections waiting on I/O",
		},
	)

	// Query for blocking connections
	blockingQuery := `
		WITH blocking_locks AS (
			SELECT 
				blocked_locks.pid AS blocked_pid,
				blocked_activity.usename AS blocked_user,
				blocking_locks.pid AS blocking_pid,
				blocking_activity.usename AS blocking_user,
				blocked_activity.query AS blocked_statement,
				blocking_activity.query AS current_statement_in_blocking_process
			FROM pg_catalog.pg_locks blocked_locks
			JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
			JOIN pg_catalog.pg_locks blocking_locks 
				ON blocking_locks.locktype = blocked_locks.locktype
				AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
				AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
				AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
				AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
				AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
				AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
				AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
				AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
				AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
				AND blocking_locks.pid != blocked_locks.pid
			JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
			WHERE NOT blocked_locks.granted
		)
		SELECT 
			COUNT(DISTINCT blocked_pid) as blocked_connections,
			COUNT(DISTINCT blocking_pid) as blocking_connections
		FROM blocking_locks
	`

	var blockedConnections, blockingConnections int64
	err = db.QueryRow(blockingQuery).Scan(&blockedConnections, &blockingConnections)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to collect blocking metrics: %v", err)
	} else {
		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.connections.blocked",
				Value:       MetricValue{IntValue: &blockedConnections},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of blocked connections",
			},
			Metric{
				Name:        "postgresql.connections.blocking",
				Value:       MetricValue{IntValue: &blockingConnections},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of connections causing blocks",
			},
		)
	}

	// Connection termination metrics from pg_stat_database
	terminationQuery := `
		SELECT 
			COALESCE(SUM(conflicts), 0) as total_conflicts,
			COALESCE(SUM(temp_files), 0) as temp_files_created,
			COALESCE(SUM(temp_bytes), 0) as temp_bytes_used
		FROM pg_stat_database 
		WHERE datname IS NOT NULL
	`

	var totalConflicts, tempFilesCreated, tempBytesUsed int64
	err = db.QueryRow(terminationQuery).Scan(&totalConflicts, &tempFilesCreated, &tempBytesUsed)
	if err == nil {
		*metrics = append(*metrics,
			Metric{
				Name:        "postgresql.connections.conflicts_total",
				Value:       MetricValue{IntValue: &totalConflicts},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total connection conflicts",
			},
			Metric{
				Name:        "postgresql.connections.temp_files_created",
				Value:       MetricValue{IntValue: &tempFilesCreated},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Temporary files created by connections",
			},
			Metric{
				Name:        "postgresql.connections.temp_bytes_used",
				Value:       MetricValue{IntValue: &tempBytesUsed},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "bytes",
				Description: "Temporary bytes used by connections",
			},
		)
	}
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

		if err := rows.Scan(&schemaname, &tablename, &indexname, &idxScan, &idxTupRead, &idxTupFetch); err != nil {
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
				Description: "Number of index entries read",
			},
			Metric{
				Name:        "postgresql.index.tuples_fetched",
				Value:       MetricValue{IntValue: &idxTupFetch},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of live rows fetched by index scans",
			},
		)
	}
}

// collectQueryPerformanceMetrics collects detailed query performance metrics
func (p *PostgreSQLMetricsCollector) collectQueryPerformanceMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	tags := []MetricTag{
		{Key: "host", Value: p.getHostname()},
	}

	// 1. Try to collect from pg_stat_statements if available
	p.collectPgStatStatementsMetrics(db, metrics, timestamp, tags)

	// 2. Collect current activity metrics (always available)
	p.collectCurrentActivityMetrics(db, metrics, timestamp, tags)

	// 3. Collect database-level query stats
	p.collectDatabaseQueryStats(db, metrics, timestamp, tags)
}

// collectPgStatStatementsMetrics collects metrics from pg_stat_statements extension
func (p *PostgreSQLMetricsCollector) collectPgStatStatementsMetrics(db *sql.DB, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	// Check if pg_stat_statements is available
	checkQuery := `
		SELECT COUNT(*) 
		FROM pg_extension 
		WHERE extname = 'pg_stat_statements'
	`

	var extensionCount int
	err := db.QueryRow(checkQuery).Scan(&extensionCount)
	if err != nil || extensionCount == 0 {
		log.Printf("DEBUG: PostgreSQL - pg_stat_statements extension not available")
		return
	}

	// Get top slow queries and aggregate statistics
	query := `
		SELECT 
			calls,
			total_exec_time,
			mean_exec_time,
			stddev_exec_time,
			rows,
			100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) as hit_percent
		FROM pg_stat_statements 
		WHERE calls > 0
		ORDER BY mean_exec_time DESC
		LIMIT 100
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to query pg_stat_statements: %v", err)
		return
	}
	defer rows.Close()

	var totalCalls int64 = 0
	var totalExecTime float64 = 0
	var slowQueryCount int64 = 0
	var queryTimes []float64
	var totalHitPercent float64 = 0
	var hitPercentCount int64 = 0

	for rows.Next() {
		var calls int64
		var totalTime, meanTime, stddevTime float64
		var rowsReturned int64
		var hitPercent sql.NullFloat64

		if err := rows.Scan(&calls, &totalTime, &meanTime, &stddevTime, &rowsReturned, &hitPercent); err != nil {
			continue
		}

		totalCalls += calls
		totalExecTime += totalTime
		queryTimes = append(queryTimes, meanTime)

		// Count slow queries (>1000ms mean execution time)
		if meanTime > 1000 {
			slowQueryCount++
		}

		// Aggregate hit percentage
		if hitPercent.Valid {
			totalHitPercent += hitPercent.Float64
			hitPercentCount++
		}
	}

	if totalCalls > 0 {
		// 1. Average Query Time
		avgQueryTime := totalExecTime / float64(totalCalls)
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.performance.avg_query_time_ms",
			Value:       MetricValue{DoubleValue: &avgQueryTime},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "milliseconds",
			Description: "Average query execution time",
		})

		// 2. Queries Per Second (approximate)
		qps := float64(totalCalls) / 60.0 // Assuming 1 minute collection interval
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.performance.queries_per_sec",
			Value:       MetricValue{DoubleValue: &qps},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "operations_per_second",
			Description: "Queries per second (approximate)",
		})

		// 3. Slow Queries Count
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.performance.slow_queries_count",
			Value:       MetricValue{IntValue: &slowQueryCount},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of slow queries (>1000ms mean time)",
		})

		// 4. Query Time Percentiles
		if len(queryTimes) > 0 {
			// Sort query times for percentile calculation
			for i := 0; i < len(queryTimes)-1; i++ {
				for j := i + 1; j < len(queryTimes); j++ {
					if queryTimes[i] > queryTimes[j] {
						queryTimes[i], queryTimes[j] = queryTimes[j], queryTimes[i]
					}
				}
			}

			// 95th percentile
			p95Index := int(float64(len(queryTimes)) * 0.95)
			if p95Index >= len(queryTimes) {
				p95Index = len(queryTimes) - 1
			}
			p95Value := queryTimes[p95Index]

			*metrics = append(*metrics, Metric{
				Name:        "postgresql.performance.query_time_p95_ms",
				Value:       MetricValue{DoubleValue: &p95Value},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "milliseconds",
				Description: "95th percentile query execution time",
			})

			// 99th percentile
			p99Index := int(float64(len(queryTimes)) * 0.99)
			if p99Index >= len(queryTimes) {
				p99Index = len(queryTimes) - 1
			}
			p99Value := queryTimes[p99Index]

			*metrics = append(*metrics, Metric{
				Name:        "postgresql.performance.query_time_p99_ms",
				Value:       MetricValue{DoubleValue: &p99Value},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "milliseconds",
				Description: "99th percentile query execution time",
			})
		}

		// 5. Cache Hit Ratio
		if hitPercentCount > 0 {
			avgHitPercent := totalHitPercent / float64(hitPercentCount)
			*metrics = append(*metrics, Metric{
				Name:        "postgresql.performance.cache_hit_ratio",
				Value:       MetricValue{DoubleValue: &avgHitPercent},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "percent",
				Description: "Average cache hit ratio",
			})
		}
	}
}

// collectCurrentActivityMetrics collects metrics from pg_stat_activity
func (p *PostgreSQLMetricsCollector) collectCurrentActivityMetrics(db *sql.DB, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	query := `
		SELECT 
			state,
			COUNT(*) as query_count,
			AVG(EXTRACT(epoch FROM (now() - query_start)) * 1000) as avg_duration_ms
		FROM pg_stat_activity 
		WHERE state IS NOT NULL 
		AND pid <> pg_backend_pid()
		AND query_start IS NOT NULL
		GROUP BY state
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to query pg_stat_activity: %v", err)
		return
	}
	defer rows.Close()

	var totalActiveQueries int64 = 0
	var longRunningQueries int64 = 0

	for rows.Next() {
		var state string
		var queryCount int64
		var avgDuration sql.NullFloat64

		if err := rows.Scan(&state, &queryCount, &avgDuration); err != nil {
			continue
		}

		if state == "active" {
			totalActiveQueries += queryCount

			// Count long-running queries (>30 seconds)
			if avgDuration.Valid && avgDuration.Float64 > 30000 {
				longRunningQueries++
			}
		}

		// Per-state query count
		statesTags := append(tags, MetricTag{Key: "state", Value: state})
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.performance.queries_by_state",
			Value:       MetricValue{IntValue: &queryCount},
			Tags:        statesTags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of queries by state",
		})

		if avgDuration.Valid {
			duration := avgDuration.Float64
			*metrics = append(*metrics, Metric{
				Name:        "postgresql.performance.avg_query_duration_ms",
				Value:       MetricValue{DoubleValue: &duration},
				Tags:        statesTags,
				Timestamp:   timestamp,
				Unit:        "milliseconds",
				Description: "Average query duration by state",
			})
		}
	}

	// Total active queries
	*metrics = append(*metrics, Metric{
		Name:        "postgresql.performance.active_queries_count",
		Value:       MetricValue{IntValue: &totalActiveQueries},
		Tags:        tags,
		Timestamp:   timestamp,
		Unit:        "count",
		Description: "Number of currently active queries",
	})

	// Long-running queries
	*metrics = append(*metrics, Metric{
		Name:        "postgresql.performance.long_running_queries_count",
		Value:       MetricValue{IntValue: &longRunningQueries},
		Tags:        tags,
		Timestamp:   timestamp,
		Unit:        "count",
		Description: "Number of long-running queries (>30s)",
	})
}

// collectDatabaseQueryStats collects database-level query statistics
func (p *PostgreSQLMetricsCollector) collectDatabaseQueryStats(db *sql.DB, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	query := `
		SELECT 
			datname,
			tup_returned,
			tup_fetched,
			tup_inserted,
			tup_updated,
			tup_deleted,
			CASE 
				WHEN tup_returned > 0 THEN (tup_fetched::float / tup_returned::float) * 100 
				ELSE 0 
			END as index_scan_ratio
		FROM pg_stat_database 
		WHERE datname NOT IN ('template0', 'template1')
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("DEBUG: PostgreSQL - Failed to query database stats: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var datname string
		var tupReturned, tupFetched, tupInserted, tupUpdated, tupDeleted int64
		var indexScanRatio float64

		if err := rows.Scan(&datname, &tupReturned, &tupFetched, &tupInserted, &tupUpdated, &tupDeleted, &indexScanRatio); err != nil {
			continue
		}

		dbTags := append(tags, MetricTag{Key: "database", Value: datname})

		// Read/Write ratio
		readOps := tupReturned + tupFetched
		writeOps := tupInserted + tupUpdated + tupDeleted

		if writeOps > 0 {
			readWriteRatio := float64(readOps) / float64(writeOps)
			*metrics = append(*metrics, Metric{
				Name:        "postgresql.performance.read_write_ratio",
				Value:       MetricValue{DoubleValue: &readWriteRatio},
				Tags:        dbTags,
				Timestamp:   timestamp,
				Unit:        "ratio",
				Description: "Read to write operations ratio",
			})
		}

		// Index scan ratio (higher is better - indicates good index usage)
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.performance.index_scan_ratio",
			Value:       MetricValue{DoubleValue: &indexScanRatio},
			Tags:        dbTags,
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Index scan ratio (fetched/returned * 100)",
		})

		// Sequential scan ratio (lower is better)
		seqScanRatio := 100.0 - indexScanRatio
		*metrics = append(*metrics, Metric{
			Name:        "postgresql.performance.seq_scan_ratio",
			Value:       MetricValue{DoubleValue: &seqScanRatio},
			Tags:        dbTags,
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Sequential scan ratio (100 - index_scan_ratio)",
		})
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
