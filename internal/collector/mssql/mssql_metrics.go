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

	// Debug logging: Show all collected system metrics
	log.Printf("DEBUG: CollectSystemMetrics - Raw system metrics collected: %d items", len(systemMetrics))
	for key, value := range systemMetrics {
		log.Printf("DEBUG: SystemMetric[%s] = %v (type: %T)", key, value, value)
	}

	// System metrics collected successfully

	// CPU Metrics
	if cpuUsage, ok := systemMetrics["cpu_usage"].(float64); ok {
		log.Printf("DEBUG: Adding CPU usage metric: %.2f", cpuUsage)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.cpu_usage",
			Value:       MetricValue{DoubleValue: &cpuUsage},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "CPU usage percentage",
		})
	} else {
		log.Printf("DEBUG: CPU usage metric not found or wrong type in system metrics")
	}

	// Fix: Use cpu_count instead of cpu_cores (matches collector output)
	if cpuCount, ok := systemMetrics["cpu_count"].(int32); ok {
		cores := int64(cpuCount)
		log.Printf("DEBUG: Adding CPU cores metric: %d", cores)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.cpu_cores",
			Value:       MetricValue{IntValue: &cores},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of CPU cores",
		})
	} else {
		log.Printf("DEBUG: CPU count metric not found or wrong type in system metrics")
	}

	// Memory Metrics
	if memUsage, ok := systemMetrics["memory_usage"].(float64); ok {
		log.Printf("DEBUG: Adding memory usage metric: %.2f", memUsage)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.memory_usage",
			Value:       MetricValue{DoubleValue: &memUsage},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Memory usage percentage",
		})
	} else {
		log.Printf("DEBUG: Memory usage metric not found or wrong type in system metrics")
	}

	if totalMem, ok := systemMetrics["total_memory"].(int64); ok {
		log.Printf("DEBUG: Adding total memory metric: %d bytes", totalMem)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.total_memory",
			Value:       MetricValue{IntValue: &totalMem},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total system memory",
		})
	} else {
		log.Printf("DEBUG: Total memory metric not found or wrong type in system metrics")
	}

	if freeMem, ok := systemMetrics["free_memory"].(int64); ok {
		log.Printf("DEBUG: Adding free memory metric: %d bytes", freeMem)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.free_memory",
			Value:       MetricValue{IntValue: &freeMem},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free system memory",
		})
	} else {
		log.Printf("DEBUG: Free memory metric not found or wrong type in system metrics")
	}

	// Disk Metrics
	if totalDisk, ok := systemMetrics["total_disk"].(int64); ok {
		log.Printf("DEBUG: Adding total disk metric: %d bytes", totalDisk)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.total_disk",
			Value:       MetricValue{IntValue: &totalDisk},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total disk space",
		})
	} else {
		log.Printf("DEBUG: Total disk metric not found or wrong type in system metrics")
	}

	if freeDisk, ok := systemMetrics["free_disk"].(int64); ok {
		log.Printf("DEBUG: Adding free disk metric: %d bytes", freeDisk)
		metrics = append(metrics, Metric{
			Name:        "mssql.system.free_disk",
			Value:       MetricValue{IntValue: &freeDisk},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free disk space",
		})
	} else {
		log.Printf("DEBUG: Free disk metric not found or wrong type in system metrics")
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
			cntr_type
		FROM sys.dm_os_performance_counters 
		WHERE object_name LIKE '%SQL Statistics%' 
		   OR object_name LIKE '%Buffer Manager%'
		   OR object_name LIKE '%Memory Manager%'
		   OR object_name LIKE '%Locks%'
	`

	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Failed to collect performance counters: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var counterName string
		var cntrValue, cntrType int64

		if err := rows.Scan(&counterName, &cntrValue, &cntrType); err != nil {
			continue
		}

		// Map counter names to metric names
		metricName := m.mapCounterToMetric(counterName)
		if metricName == "" {
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
	}
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
		"Batch Requests/sec":        "mssql.performance.batch_requests_per_sec",
		"SQL Compilations/sec":      "mssql.performance.compilations_per_sec",
		"SQL Re-Compilations/sec":   "mssql.performance.recompilations_per_sec",
		"Buffer cache hit ratio":    "mssql.performance.buffer_cache_hit_ratio",
		"Page life expectancy":      "mssql.performance.page_life_expectancy",
		"Lazy writes/sec":           "mssql.performance.lazy_writes_per_sec",
		"Page reads/sec":            "mssql.performance.page_reads_per_sec",
		"Page writes/sec":           "mssql.performance.page_writes_per_sec",
		"Lock Requests/sec":         "mssql.performance.lock_requests_per_sec",
		"Lock Timeouts/sec":         "mssql.performance.lock_timeouts_per_sec",
		"Lock Waits/sec":            "mssql.performance.lock_waits_per_sec",
		"Target Server Memory (KB)": "mssql.memory.target_server_memory",
		"Total Server Memory (KB)":  "mssql.memory.total_server_memory",
	}

	return counterMap[counterName]
}

// getCounterUnit returns the appropriate unit for a counter
func (m *MSSQLMetricsCollector) getCounterUnit(counterName string) string {
	if strings.Contains(counterName, "/sec") {
		return "per_second"
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
