package mssql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
)

// MSSQLMetricsCollector collects time-series metrics for MSSQL
type MSSQLMetricsCollector struct {
	cfg       *config.AgentConfig
	collector *MSSQLCollector
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

// NewMSSQLMetricsCollector creates a new MSSQL metrics collector
func NewMSSQLMetricsCollector(cfg *config.AgentConfig) *MSSQLMetricsCollector {
	return &MSSQLMetricsCollector{
		cfg:       cfg,
		collector: NewMSSQLCollector(cfg),
	}
}

// CollectSystemMetrics collects system-level metrics
func (m *MSSQLMetricsCollector) CollectSystemMetrics() (*MetricBatch, error) {
	timestamp := time.Now().UnixNano()
	var metrics []Metric

	// Get system metrics from existing collector
	systemMetrics := m.collector.BatchCollectSystemMetrics()

	// CPU Metrics
	if cpuUsage, ok := systemMetrics["cpu_usage"].(float64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.cpu_usage",
			Value:       MetricValue{DoubleValue: &cpuUsage},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "CPU usage percentage",
		})
	}

	if cpuCount, ok := systemMetrics["cpu_count"].(int32); ok {
		cores := int64(cpuCount)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.cpu_cores",
			Value:       MetricValue{IntValue: &cores},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of CPU cores",
		})
	}

	// Memory Metrics
	if memUsage, ok := systemMetrics["memory_usage"].(float64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.memory_usage",
			Value:       MetricValue{DoubleValue: &memUsage},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Memory usage percentage",
		})
	}

	if totalMem, ok := systemMetrics["total_memory"].(int64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.total_memory",
			Value:       MetricValue{IntValue: &totalMem},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total system memory",
		})
	}

	if freeMem, ok := systemMetrics["free_memory"].(int64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.free_memory",
			Value:       MetricValue{IntValue: &freeMem},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free system memory",
		})
	}

	// Disk Metrics
	if totalDisk, ok := systemMetrics["total_disk"].(int64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.total_disk",
			Value:       MetricValue{IntValue: &totalDisk},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total disk space",
		})
	}

	if freeDisk, ok := systemMetrics["free_disk"].(int64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.free_disk",
			Value:       MetricValue{IntValue: &freeDisk},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free disk space",
		})
	}

	// Response Time Metric
	if responseTime, ok := systemMetrics["response_time_ms"].(float64); ok {
		metrics = append(metrics, Metric{
			Name:        "mssql.system.response_time_ms",
			Value:       MetricValue{DoubleValue: &responseTime},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "milliseconds",
			Description: "MSSQL response time for SELECT 1 query in milliseconds",
		})
	} else {
		// Check if key exists but with different type
		if _, exists := systemMetrics["response_time_ms"]; exists {
		} else {
			log.Printf("DEBUG: response_time_ms key does not exist in systemMetrics")
		}
	}

	return &MetricBatch{
		AgentID:             m.getAgentID(),
		MetricType:          "mssql_system",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform": "mssql",
			"os":       runtime.GOOS,
		},
	}, nil
}

// CollectDatabaseMetrics collects database-specific metrics
func (m *MSSQLMetricsCollector) CollectDatabaseMetrics() (*MetricBatch, error) {
	// Add panic recovery for database metrics collection
	defer func() {
		if r := recover(); r != nil {
			log.Printf("DEBUG: CollectDatabaseMetrics - PANIC RECOVERED: %v", r)
		}
	}()

	timestamp := time.Now().UnixNano()
	var metrics []Metric

	db, err := m.collector.GetClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}
	defer db.Close()

	// Collect various database metrics
	m.collectConnectionMetrics(db, &metrics, timestamp)
	m.collectPerformanceCounters(db, &metrics, timestamp)
	m.collectTransactionMetrics(db, &metrics, timestamp)
	m.collectBlockingQueries(db, &metrics, timestamp)
	m.collectDeadlockMetrics(db, &metrics, timestamp)
	m.collectWaitStats(db, &metrics, timestamp)
	m.collectDatabaseSizes(db, &metrics, timestamp)

	return &MetricBatch{
		AgentID:             m.getAgentID(),
		MetricType:          "mssql_database",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform": "mssql",
			"database": m.cfg.MSSQL.Database,
		},
	}, nil
}

// collectConnectionMetrics collects connection-related metrics
func (m *MSSQLMetricsCollector) collectConnectionMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			COUNT(*) as total_connections,
			SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as active_connections,
			SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) as idle_connections
		FROM sys.dm_exec_sessions 
		WHERE is_user_process = 1
	`

	var totalConn, activeConn, idleConn int64
	err := db.QueryRow(query).Scan(&totalConn, &activeConn, &idleConn)
	if err != nil {
		logger.Error("Failed to collect connection metrics: %v", err)
		return
	}

	*metrics = append(*metrics,
		Metric{
			Name:        "mssql.connections.total",
			Value:       MetricValue{IntValue: &totalConn},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Total number of connections",
		},
		Metric{
			Name:        "mssql.connections.active",
			Value:       MetricValue{IntValue: &activeConn},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of active connections",
		},
		Metric{
			Name:        "mssql.connections.idle",
			Value:       MetricValue{IntValue: &idleConn},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of idle connections",
		},
	)
}

// collectPerformanceCounters collects SQL Server performance counters
func (m *MSSQLMetricsCollector) collectPerformanceCounters(db *sql.DB, metrics *[]Metric, timestamp int64) {

	query := `
		SELECT 
			counter_name,
			cntr_value,
			cntr_type,
			object_name
		FROM sys.dm_os_performance_counters 
		WHERE object_name LIKE '%:SQL Statistics%' 
		   OR object_name LIKE '%:Buffer Manager%'
		   OR object_name LIKE '%:Memory Manager%'
		   OR object_name LIKE '%:Locks%'
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect performance counters: %v", err)
		return
	}
	defer rows.Close()

	counterCount := 0
	unmappedCount := 0
	for rows.Next() {
		var counterName, objectName string
		var cntrValue, cntrType int64

		if err := rows.Scan(&counterName, &cntrValue, &cntrType, &objectName); err != nil {
			continue
		}

		// Trim whitespace from counter name (SQL Server adds trailing spaces)
		counterName = strings.TrimSpace(counterName)

		// Map counter names to metric names
		metricName := m.mapCounterToMetric(counterName)
		if metricName == "" {
			unmappedCount++
			continue
		}

		*metrics = append(*metrics, Metric{
			Name:        metricName,
			Value:       MetricValue{IntValue: &cntrValue},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        m.getCounterUnit(counterName),
			Description: counterName,
		})
		counterCount++
	}

	log.Printf("DEBUG: collectPerformanceCounters - Collected %d performance counters, unmapped: %d", counterCount, unmappedCount)
}

// collectTransactionMetrics collects transaction-related metrics
func (m *MSSQLMetricsCollector) collectTransactionMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	// Add safety check to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Transaction metrics collection panic recovered: %v", r)
		}
	}()

	// Collect Transaction Performance Counters
	if err := m.collectTransactionPerformanceCounters(db, metrics, timestamp); err != nil {
		logger.Error("Failed to collect transaction performance counters: %v", err)
	}
}

// collectTransactionPerformanceCounters collects transaction performance counters safely
func (m *MSSQLMetricsCollector) collectTransactionPerformanceCounters(db *sql.DB, metrics *[]Metric, timestamp int64) error {
	// Collect transaction counters with SQL Server 2016 specific patterns
	transactionCountersQuery := `
		SELECT 
			counter_name,
			cntr_value,
			ISNULL(instance_name, '') as instance_name,
			object_name
		FROM sys.dm_os_performance_counters 
		WHERE object_name LIKE '%:Transactions%'
		   OR object_name LIKE '%:Databases%'
		   OR object_name LIKE '%:Database Replica%'
		   OR object_name LIKE '%XTP Transactions%'
		   OR object_name LIKE '%XTP Transaction Log%'
	`

	rows, err := db.Query(transactionCountersQuery)
	if err != nil {
		return fmt.Errorf("failed to query transaction performance counters: %w", err)
	}
	defer rows.Close()

	counterCount := 0
	unmappedCount := 0
	for rows.Next() {
		var counterName, instanceName, objectName string
		var cntrValue int64

		if err := rows.Scan(&counterName, &cntrValue, &instanceName, &objectName); err != nil {
			continue
		}

		// Trim whitespace from counter name (SQL Server adds trailing spaces)
		counterName = strings.TrimSpace(counterName)
		instanceName = strings.TrimSpace(instanceName)

		// Map transaction counter names to metric names
		metricName := m.mapTransactionCounterToMetric(counterName)
		if metricName == "" {
			unmappedCount++
			continue
		}

		tags := []MetricTag{{Key: "host", Value: m.getHostname()}}
		if instanceName != "_Total" && instanceName != "" {
			tags = append(tags, MetricTag{Key: "database", Value: instanceName})
		}

		*metrics = append(*metrics, Metric{
			Name:        metricName,
			Value:       MetricValue{IntValue: &cntrValue},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        m.getTransactionCounterUnit(counterName),
			Description: counterName,
		})
		counterCount++
	}

	log.Printf("DEBUG: collectTransactionPerformanceCounters - Collected %d transaction counters, unmapped: %d", counterCount, unmappedCount)
	return nil
}

// collectActiveTransactionsSafe safely collects information about currently active transactions
func (m *MSSQLMetricsCollector) collectActiveTransactionsSafe(db *sql.DB, metrics *[]Metric, timestamp int64) error {
	return m.collectActiveTransactions(db, metrics, timestamp)
}

// collectActiveTransactions collects information about currently active transactions
func (m *MSSQLMetricsCollector) collectActiveTransactions(db *sql.DB, metrics *[]Metric, timestamp int64) error {
	activeTransQuery := `
		SELECT 
			s.session_id,
			s.login_name,
			DB_NAME(s.database_id) as database_name,
			t.transaction_id,
			t.name as transaction_name,
			t.transaction_begin_time,
			DATEDIFF(second, t.transaction_begin_time, GETDATE()) as duration_seconds,
			t.transaction_type,
			t.transaction_state
		FROM sys.dm_tran_active_transactions t
		INNER JOIN sys.dm_tran_session_transactions st ON t.transaction_id = st.transaction_id
		INNER JOIN sys.dm_exec_sessions s ON st.session_id = s.session_id
		WHERE s.is_user_process = 1
	`

	rows, err := db.Query(activeTransQuery)
	if err != nil {
		return fmt.Errorf("failed to collect active transactions: %w", err)
	}
	defer rows.Close()

	var totalActiveTransactions int64 = 0
	var longRunningTransactions int64 = 0
	var maxTransactionDuration int64 = 0
	databaseTransactionCounts := make(map[string]int64)

	for rows.Next() {
		var sessionID, transactionID, transactionType, transactionState int64
		var loginName, databaseName, transactionName sql.NullString
		var transactionBeginTime sql.NullTime
		var durationSeconds int64

		if err := rows.Scan(&sessionID, &loginName, &databaseName, &transactionID, &transactionName,
			&transactionBeginTime, &durationSeconds, &transactionType, &transactionState); err != nil {
			continue
		}

		totalActiveTransactions++

		// Count long-running transactions (>30 seconds)
		if durationSeconds > 30 {
			longRunningTransactions++
		}

		// Track maximum transaction duration
		if durationSeconds > maxTransactionDuration {
			maxTransactionDuration = durationSeconds
		}

		// Count transactions per database
		if databaseName.Valid {
			databaseTransactionCounts[databaseName.String]++
		}
	}

	// Add aggregate metrics
	*metrics = append(*metrics,
		Metric{
			Name:        "mssql.transactions.active_total",
			Value:       MetricValue{IntValue: &totalActiveTransactions},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Total number of active transactions",
		},
		Metric{
			Name:        "mssql.transactions.long_running",
			Value:       MetricValue{IntValue: &longRunningTransactions},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of long-running transactions (>30 seconds)",
		},
		Metric{
			Name:        "mssql.transactions.max_duration_seconds",
			Value:       MetricValue{IntValue: &maxTransactionDuration},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "seconds",
			Description: "Maximum transaction duration in seconds",
		},
	)

	// Add per-database transaction counts
	for dbName, count := range databaseTransactionCounts {
		*metrics = append(*metrics, Metric{
			Name:        "mssql.transactions.active_by_database",
			Value:       MetricValue{IntValue: &count},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "database", Value: dbName}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of active transactions by database",
		})
	}

	return nil
}

// collectBlockingQueries collects information about blocking queries
func (m *MSSQLMetricsCollector) collectBlockingQueries(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT COUNT(*) as blocking_sessions
		FROM sys.dm_exec_requests r
		WHERE r.blocking_session_id > 0
	`

	var blockingSessions int64
	err := db.QueryRow(query).Scan(&blockingSessions)
	if err != nil {
		logger.Error("Failed to collect blocking queries: %v", err)
		return
	}

	*metrics = append(*metrics, Metric{
		Name:        "mssql.blocking.sessions",
		Value:       MetricValue{IntValue: &blockingSessions},
		Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
		Timestamp:   timestamp,
		Unit:        "count",
		Description: "Number of blocked sessions",
	})
}

// collectDeadlockMetrics collects deadlock information
func (m *MSSQLMetricsCollector) collectDeadlockMetrics(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT cntr_value as deadlocks
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Number of Deadlocks/sec'
		AND object_name LIKE '%Locks%'
	`

	var deadlocks int64
	err := db.QueryRow(query).Scan(&deadlocks)
	if err != nil {
		logger.Error("Failed to collect deadlock metrics: %v", err)
		return
	}

	*metrics = append(*metrics, Metric{
		Name:        "mssql.deadlocks.total",
		Value:       MetricValue{IntValue: &deadlocks},
		Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
		Timestamp:   timestamp,
		Unit:        "count",
		Description: "Total number of deadlocks",
	})
}

// collectWaitStats collects wait statistics
func (m *MSSQLMetricsCollector) collectWaitStats(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT TOP 10
			wait_type,
			waiting_tasks_count,
			wait_time_ms,
			signal_wait_time_ms
		FROM sys.dm_os_wait_stats 
		WHERE wait_type NOT IN (
			'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
			'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
			'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
			'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT',
			'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
			'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN'
		)
		ORDER BY wait_time_ms DESC
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect wait stats: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var waitType string
		var waitingTasks, waitTimeMs, signalWaitMs int64

		if err := rows.Scan(&waitType, &waitingTasks, &waitTimeMs, &signalWaitMs); err != nil {
			continue
		}

		*metrics = append(*metrics,
			Metric{
				Name:        "mssql.waits.tasks",
				Value:       MetricValue{IntValue: &waitingTasks},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "wait_type", Value: waitType}},
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of waiting tasks",
			},
			Metric{
				Name:        "mssql.waits.time_ms",
				Value:       MetricValue{IntValue: &waitTimeMs},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "wait_type", Value: waitType}},
				Timestamp:   timestamp,
				Unit:        "milliseconds",
				Description: "Total wait time in milliseconds",
			},
		)
	}
}

// collectDatabaseSizes collects database size information
func (m *MSSQLMetricsCollector) collectDatabaseSizes(db *sql.DB, metrics *[]Metric, timestamp int64) {
	query := `
		SELECT 
			DB_NAME(database_id) as database_name,
			SUM(CASE WHEN type = 0 THEN size END) * 8 * 1024 as data_size_bytes,
			SUM(CASE WHEN type = 1 THEN size END) * 8 * 1024 as log_size_bytes
		FROM sys.master_files
		WHERE database_id > 4  -- Exclude system databases
		GROUP BY database_id
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect database sizes: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		var dataSize, logSize sql.NullInt64

		if err := rows.Scan(&dbName, &dataSize, &logSize); err != nil {
			continue
		}

		if dataSize.Valid {
			*metrics = append(*metrics, Metric{
				Name:        "mssql.database.data_size",
				Value:       MetricValue{IntValue: &dataSize.Int64},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "database", Value: dbName}},
				Timestamp:   timestamp,
				Unit:        "bytes",
				Description: "Database data file size",
			})
		}

		if logSize.Valid {
			*metrics = append(*metrics, Metric{
				Name:        "mssql.database.log_size",
				Value:       MetricValue{IntValue: &logSize.Int64},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "database", Value: dbName}},
				Timestamp:   timestamp,
				Unit:        "bytes",
				Description: "Database log file size",
			})
		}
	}
}

// mapCounterToMetric maps SQL Server counter names to metric names
func (m *MSSQLMetricsCollector) mapCounterToMetric(counterName string) string {
	counterMap := map[string]string{
		"Batch Requests/sec":        "mssql.performance.batch_requests_count",
		"SQL Compilations/sec":      "mssql.performance.compilations_count",
		"SQL Re-Compilations/sec":   "mssql.performance.recompilations_count",
		"Buffer cache hit ratio":    "mssql.performance.buffer_cache_hit_ratio",
		"Page life expectancy":      "mssql.performance.page_life_expectancy",
		"Lazy writes/sec":           "mssql.performance.lazy_writes_count",
		"Page reads/sec":            "mssql.performance.page_reads_count",
		"Page writes/sec":           "mssql.performance.page_writes_count",
		"Lock Requests/sec":         "mssql.performance.lock_requests_count",
		"Lock Timeouts/sec":         "mssql.performance.lock_timeouts_count",
		"Lock Waits/sec":            "mssql.performance.lock_waits_count",
		"Target Server Memory (KB)": "mssql.memory.target_server_memory",
		"Total Server Memory (KB)":  "mssql.memory.total_server_memory",
	}

	return counterMap[counterName]
}

// mapTransactionCounterToMetric maps SQL Server transaction counter names to metric names
func (m *MSSQLMetricsCollector) mapTransactionCounterToMetric(counterName string) string {
	counterMap := map[string]string{
		// Core Transaction Metrics from MSSQL$ONLINE:Transactions
		"Transactions":                     "mssql.transactions.active_total",
		"Snapshot Transactions":            "mssql.transactions.snapshot_active",
		"Update Snapshot Transactions":     "mssql.transactions.update_snapshot_active",
		"NonSnapshot Version Transactions": "mssql.transactions.nonsnapshot_version_active",
		"Longest Transaction Running Time": "mssql.transactions.longest_running_time_seconds",
		"Update conflict ratio":            "mssql.transactions.update_conflict_ratio",
		"Free Space in tempdb (KB)":        "mssql.transactions.tempdb_free_space_kb",
		"Version Generation rate (KB/s)":   "mssql.transactions.version_generation_rate_kb_count",
		"Version Cleanup rate (KB/s)":      "mssql.transactions.version_cleanup_rate_kb_count",
		"Version Store Size (KB)":          "mssql.transactions.version_store_size_kb",
		"Version Store unit count":         "mssql.transactions.version_store_unit_count",
		"Version Store unit creation":      "mssql.transactions.version_store_unit_creation",
		"Version Store unit truncation":    "mssql.transactions.version_store_unit_truncation",

		// Database-Level Transaction Metrics from MSSQL$ONLINE:Databases
		"Active Transactions":      "mssql.transactions.active_by_database",
		"Transactions/sec":         "mssql.transactions.count",
		"Tracked transactions/sec": "mssql.transactions.tracked_count",
		"Write Transactions/sec":   "mssql.transactions.write_count",

		// AlwaysOn/Replication Transaction Metrics from MSSQL$ONLINE:Database Replica
		"Mirrored Write Transactions/sec": "mssql.transactions.mirrored_write_count",
		"Transaction Delay":               "mssql.transactions.replication_delay",

		// In-Memory OLTP Transaction Metrics from SQL Server 2016 XTP
		"Transactions created/sec":            "mssql.transactions.xtp_created_count",
		"Transactions aborted/sec":            "mssql.transactions.xtp_aborted_count",
		"Transactions aborted by user/sec":    "mssql.transactions.xtp_aborted_by_user_count",
		"Transaction validation failures/sec": "mssql.transactions.xtp_validation_failures_count",
		"Read-only transactions prepared/sec": "mssql.transactions.xtp_readonly_prepared_count",
		"Save points created/sec":             "mssql.transactions.xtp_savepoints_created_count",
		"Save point rollbacks/sec":            "mssql.transactions.xtp_savepoint_rollbacks_count",
		"Cascading aborts/sec":                "mssql.transactions.xtp_cascading_aborts_count",
		"Commit dependencies taken/sec":       "mssql.transactions.xtp_commit_dependencies_count",
	}

	return counterMap[counterName]
}

// getCounterUnit returns the appropriate unit for a counter
func (m *MSSQLMetricsCollector) getCounterUnit(counterName string) string {
	// SQL Server /sec counters are cumulative, not rates - let monitoring system calculate rate
	if strings.Contains(counterName, "/sec") {
		return "count" // Changed from "per_second" to "count"
	}
	if strings.Contains(counterName, "ratio") {
		return "percent"
	}
	if strings.Contains(counterName, "(KB)") {
		return "kilobytes"
	}
	if strings.Contains(counterName, "expectancy") {
		return "seconds"
	}
	return "count"
}

// getTransactionCounterUnit returns the appropriate unit for a transaction counter
func (m *MSSQLMetricsCollector) getTransactionCounterUnit(counterName string) string {
	// SQL Server /sec counters are cumulative, not rates - let monitoring system calculate rate
	if strings.Contains(counterName, "/sec") || strings.Contains(counterName, "rate") {
		return "count" // Changed from "per_second" to "count"
	}
	if strings.Contains(counterName, "ratio") {
		return "percent"
	}
	if strings.Contains(counterName, "(KB)") || strings.Contains(counterName, "KB") {
		return "kilobytes"
	}
	if strings.Contains(counterName, "Time") {
		return "seconds"
	}
	return "count"
}

// getHostname returns the hostname for tagging
func (m *MSSQLMetricsCollector) getHostname() string {
	if m.cfg.MSSQL.Host != "" {
		return m.cfg.MSSQL.Host
	}
	return "localhost"
}

// getAgentID returns the proper agent ID in the format "agent_<hostname>"
func (m *MSSQLMetricsCollector) getAgentID() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	return "agent_" + hostname
}

// CollectAllMetrics collects both system and database metrics
func (m *MSSQLMetricsCollector) CollectAllMetrics() ([]*MetricBatch, error) {
	// Add panic recovery for the entire collection process
	defer func() {
		if r := recover(); r != nil {
			log.Printf("DEBUG: CollectAllMetrics - PANIC RECOVERED: %v", r)
		}
	}()

	var batches []*MetricBatch

	// Collect system metrics
	systemBatch, err := m.CollectSystemMetrics()
	if err != nil {
		log.Printf("Failed to collect system metrics: %v", err)
	} else {
		batches = append(batches, systemBatch)
	}

	// Collect database metrics
	dbBatch, err := m.CollectDatabaseMetrics()
	if err != nil {
		log.Printf("Failed to collect database metrics: %v", err)
	} else {
		batches = append(batches, dbBatch)
	}
	return batches, nil
}
