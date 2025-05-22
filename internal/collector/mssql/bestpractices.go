package mssql

import (
	"database/sql"
	"log"
	"time"
)

// BestPracticesCollector MSSQL best practices için veri toplama yapısı
type BestPracticesCollector struct {
	collector *MSSQLCollector
	results   map[string]interface{}
}

// NewBestPracticesCollector yeni bir BestPracticesCollector oluşturur
func NewBestPracticesCollector(collector *MSSQLCollector) *BestPracticesCollector {
	return &BestPracticesCollector{
		collector: collector,
		results:   make(map[string]interface{}),
	}
}

// CollectAll tüm best practice metriklerini toplar
func (b *BestPracticesCollector) CollectAll() map[string]interface{} {
	b.collectSystemConfiguration()
	b.collectDatabaseHealth()
	b.collectPerformanceMetrics()
	b.collectHighAvailabilityStatus()
	b.collectSecuritySettings()
	b.collectSystemMetrics()
	b.collectTempDBPerformance()
	b.collectMemoryUsage()
	b.collectIOPerformance()

	return b.results
}

// collectSystemConfiguration SQL Server sistem konfigürasyonunu toplar
func (b *BestPracticesCollector) collectSystemConfiguration() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Sistem konfigürasyonu toplanamadı: %v", err)
		return
	}
	defer db.Close()

	// SQL Server sürümü
	var version, edition string
	err = db.QueryRow("SELECT @@VERSION, SERVERPROPERTY('Edition')").Scan(&version, &edition)
	if err == nil {
		b.results["SqlServerVersion"] = version
		b.results["SqlServerEdition"] = edition
	}

	// SQL Server konfigürasyonu
	configs := make(map[string]interface{})
	rows, err := db.Query(`
		SELECT name, value, value_in_use, description 
		FROM sys.configurations 
		WHERE name IN (
			'max degree of parallelism',
			'cost threshold for parallelism',
			'max server memory (MB)',
			'min server memory (MB)',
			'max worker threads',
			'optimize for ad hoc workloads'
		)
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name, description string
			var value, valueInUse int
			if err := rows.Scan(&name, &value, &valueInUse, &description); err == nil {
				configs[name] = map[string]interface{}{
					"value":        value,
					"value_in_use": valueInUse,
					"description":  description,
				}
			}
		}
		b.results["SqlServerConfigurations"] = configs
	}

	// TempDB konfigürasyonu
	tempDbConfig := make(map[string]interface{})
	rows, err = db.Query(`
		SELECT 
			DB_NAME(database_id) AS database_name,
			name,
			physical_name,
			size * 8 / 1024 AS size_mb,
			growth,
			is_percent_growth,
			max_size
		FROM sys.master_files
		WHERE DB_NAME(database_id) = 'tempdb'
	`)

	if err == nil {
		defer rows.Close()
		var tempDbFiles []map[string]interface{}
		for rows.Next() {
			var dbName, name, physicalName string
			var sizeMB, growth, maxSize int
			var isPercentGrowth bool

			if err := rows.Scan(&dbName, &name, &physicalName, &sizeMB, &growth, &isPercentGrowth, &maxSize); err == nil {
				tempDbFiles = append(tempDbFiles, map[string]interface{}{
					"name":              name,
					"physical_name":     physicalName,
					"size_mb":           sizeMB,
					"growth":            growth,
					"is_percent_growth": isPercentGrowth,
					"max_size":          maxSize,
				})
			}
		}
		tempDbConfig["files"] = tempDbFiles

		// TempDB dosya sayısı önerisi (CPU sayısına göre)
		var numCpus int
		err = db.QueryRow("SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE'").Scan(&numCpus)
		if err == nil && numCpus > 0 {
			tempDbConfig["recommended_file_count"] = min(numCpus, 8) // En fazla 8 dosya önerisi
			tempDbConfig["actual_file_count"] = len(tempDbFiles)
		}

		b.results["TempDBConfiguration"] = tempDbConfig
	}
}

// collectDatabaseHealth veritabanı sağlığıyla ilgili metrikleri toplar
func (b *BestPracticesCollector) collectDatabaseHealth() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Veritabanı sağlığı metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	// Veritabanı durumları
	databases := make(map[string]interface{})
	rows, err := db.Query(`
		SELECT 
			name, 
			state_desc,
			recovery_model_desc,
			compatibility_level,
			collation_name,
			create_date,
			is_auto_shrink_on,
			is_auto_create_stats_on,
			is_auto_update_stats_on,
			is_read_only,
			is_published,
			is_subscribed
		FROM sys.databases
		WHERE database_id > 4  -- Sistem veritabanlarını hariç tut
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name, stateDesc, recoveryModel, collation string
			var compatLevel int
			var createDate time.Time
			var isAutoShrink, isAutoCreateStats, isAutoUpdateStats, isReadOnly, isPublished, isSubscribed bool

			if err := rows.Scan(
				&name, &stateDesc, &recoveryModel, &compatLevel, &collation,
				&createDate, &isAutoShrink, &isAutoCreateStats, &isAutoUpdateStats,
				&isReadOnly, &isPublished, &isSubscribed); err == nil {

				databases[name] = map[string]interface{}{
					"state":                   stateDesc,
					"recovery_model":          recoveryModel,
					"compatibility_level":     compatLevel,
					"collation":               collation,
					"create_date":             createDate,
					"is_auto_shrink_on":       isAutoShrink,
					"is_auto_create_stats_on": isAutoCreateStats,
					"is_auto_update_stats_on": isAutoUpdateStats,
					"is_read_only":            isReadOnly,
					"is_published":            isPublished,
					"is_subscribed":           isSubscribed,
				}
			}
		}
		b.results["Databases"] = databases
	}

	// Son DBCC CHECKDB çalıştırma zamanları
	dbccInfo := make(map[string]interface{})
	rows, err = db.Query(`
		SELECT 
			DB_NAME(database_id) AS database_name,
			MAX(last_clean_page_date) AS last_dbcc_checkdb_date,
			DATEDIFF(day, MAX(last_clean_page_date), GETDATE()) AS days_since_last_check
		FROM sys.dm_db_index_usage_stats
		WHERE database_id > 4
		GROUP BY database_id
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var dbName string
			var lastCheckDate time.Time
			var daysSinceLastCheck int

			if err := rows.Scan(&dbName, &lastCheckDate, &daysSinceLastCheck); err == nil {
				dbccInfo[dbName] = map[string]interface{}{
					"last_dbcc_checkdb_date": lastCheckDate,
					"days_since_last_check":  daysSinceLastCheck,
				}
			}
		}
		b.results["DBCCCheckDB"] = dbccInfo
	}

	// Veritabanı dosya bilgileri ve büyüme ayarları
	dbFiles := make(map[string][]map[string]interface{})
	rows, err = db.Query(`
		SELECT 
			DB_NAME(database_id) AS database_name,
			name,
			physical_name,
			type_desc,
			size * 8 / 1024 AS size_mb,
			growth,
			is_percent_growth,
			max_size
		FROM sys.master_files
		WHERE database_id > 4
		ORDER BY database_id, type_desc
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var dbName, name, physicalName, typeDesc string
			var sizeMB, growth, maxSize int
			var isPercentGrowth bool

			if err := rows.Scan(&dbName, &name, &physicalName, &typeDesc, &sizeMB, &growth, &isPercentGrowth, &maxSize); err == nil {
				fileInfo := map[string]interface{}{
					"name":              name,
					"physical_name":     physicalName,
					"type":              typeDesc,
					"size_mb":           sizeMB,
					"growth":            growth,
					"is_percent_growth": isPercentGrowth,
					"max_size":          maxSize,
				}

				if _, exists := dbFiles[dbName]; !exists {
					dbFiles[dbName] = []map[string]interface{}{}
				}
				dbFiles[dbName] = append(dbFiles[dbName], fileInfo)
			}
		}
		b.results["DatabaseFiles"] = dbFiles
	}
}

// collectPerformanceMetrics performans ile ilgili metrikleri toplar
func (b *BestPracticesCollector) collectPerformanceMetrics() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Performans metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	// En yüksek CPU kullanan sorgular
	topCpuQueries := []map[string]interface{}{}
	rows, err := db.Query(`
		SELECT TOP 10
			qs.total_worker_time/qs.execution_count AS avg_cpu_time,
			qs.total_worker_time AS total_cpu_time,
			qs.execution_count,
			qs.total_elapsed_time/qs.execution_count AS avg_elapsed_time,
			qs.max_elapsed_time,
			qs.total_logical_reads/qs.execution_count AS avg_logical_reads,
			qs.max_logical_reads,
			qs.total_physical_reads/qs.execution_count AS avg_physical_reads,
			DB_NAME(st.dbid) AS database_name,
			SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 
				((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) 
				ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1) AS query_text
		FROM sys.dm_exec_query_stats AS qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as st
		WHERE qs.execution_count > 5
		ORDER BY qs.total_worker_time/qs.execution_count DESC
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var avgCpuTime, totalCpuTime, avgElapsedTime, maxElapsedTime, avgLogicalReads, maxLogicalReads, avgPhysicalReads float64
			var executionCount int64
			var dbName, queryText sql.NullString

			if err := rows.Scan(
				&avgCpuTime, &totalCpuTime, &executionCount, &avgElapsedTime, &maxElapsedTime,
				&avgLogicalReads, &maxLogicalReads, &avgPhysicalReads, &dbName, &queryText); err == nil {

				dbNameStr := "unknown"
				if dbName.Valid {
					dbNameStr = dbName.String
				}

				queryTextStr := "unknown"
				if queryText.Valid {
					queryTextStr = queryText.String
				}

				topCpuQueries = append(topCpuQueries, map[string]interface{}{
					"avg_cpu_time_ms":     avgCpuTime / 1000, // Mikrosaniyeden milisaniyeye dönüştür
					"total_cpu_time_ms":   totalCpuTime / 1000,
					"execution_count":     executionCount,
					"avg_elapsed_time_ms": avgElapsedTime / 1000,
					"max_elapsed_time_ms": maxElapsedTime / 1000,
					"avg_logical_reads":   avgLogicalReads,
					"max_logical_reads":   maxLogicalReads,
					"avg_physical_reads":  avgPhysicalReads,
					"database_name":       dbNameStr,
					"query_text":          queryTextStr,
				})
			}
		}
		b.results["TopCPUQueries"] = topCpuQueries
	}

	// Missing indexes
	missingIndexes := []map[string]interface{}{}
	rows, err = db.Query(`
		SELECT TOP 25
			DB_NAME(mid.database_id) AS database_name,
			OBJECT_NAME(mid.object_id, mid.database_id) AS object_name,
			migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) AS improvement_measure,
			'CREATE INDEX missing_index_' + CONVERT (VARCHAR, mid.index_handle) + ' ON ' + mid.statement + 
			' (' + ISNULL (mid.equality_columns, '') + 
			CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN ',' ELSE '' END + 
			ISNULL (mid.inequality_columns, '') + ')' + 
			ISNULL (' INCLUDE (' + mid.included_columns + ')', '') AS create_index_statement,
			migs.user_seeks,
			migs.user_scans,
			migs.avg_total_user_cost,
			migs.avg_user_impact
		FROM sys.dm_db_missing_index_groups mig
		INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
		INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
		ORDER BY improvement_measure DESC
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var dbName, objectName, createIndexStmt sql.NullString
			var improvementMeasure, avgTotalUserCost, avgUserImpact float64
			var userSeeks, userScans int64

			if err := rows.Scan(&dbName, &objectName, &improvementMeasure, &createIndexStmt,
				&userSeeks, &userScans, &avgTotalUserCost, &avgUserImpact); err == nil {

				dbNameStr := "unknown"
				if dbName.Valid {
					dbNameStr = dbName.String
				}

				objectNameStr := "unknown"
				if objectName.Valid {
					objectNameStr = objectName.String
				}

				createIndexStmtStr := ""
				if createIndexStmt.Valid {
					createIndexStmtStr = createIndexStmt.String
				}

				missingIndexes = append(missingIndexes, map[string]interface{}{
					"database_name":          dbNameStr,
					"object_name":            objectNameStr,
					"improvement_measure":    improvementMeasure,
					"create_index_statement": createIndexStmtStr,
					"user_seeks":             userSeeks,
					"user_scans":             userScans,
					"avg_total_user_cost":    avgTotalUserCost,
					"avg_user_impact":        avgUserImpact,
				})
			}
		}
		b.results["MissingIndexes"] = missingIndexes
	}

	// Wait statistics
	waitStats := make(map[string]interface{})
	rows, err = db.Query(`
		SELECT TOP 10
			wait_type,
			wait_time_ms,
			waiting_tasks_count,
			signal_wait_time_ms,
			wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type NOT LIKE '%SLEEP%'
		AND wait_type NOT LIKE 'XE%'
		AND wait_type NOT LIKE 'BROKER%'
		ORDER BY wait_time_ms DESC
	`)

	if err == nil {
		defer rows.Close()
		var waitStatsList []map[string]interface{}
		for rows.Next() {
			var waitType string
			var waitTimeMs, waitingTasksCount, signalWaitTimeMs, resourceWaitTimeMs int64

			if err := rows.Scan(&waitType, &waitTimeMs, &waitingTasksCount,
				&signalWaitTimeMs, &resourceWaitTimeMs); err == nil {
				waitStatsList = append(waitStatsList, map[string]interface{}{
					"wait_type":             waitType,
					"wait_time_ms":          waitTimeMs,
					"waiting_tasks_count":   waitingTasksCount,
					"signal_wait_time_ms":   signalWaitTimeMs,
					"resource_wait_time_ms": resourceWaitTimeMs,
				})
			}
		}
		waitStats["wait_stats"] = waitStatsList
		b.results["WaitStatistics"] = waitStats
	}
}

// collectHighAvailabilityStatus yüksek erişilebilirlik durumunu toplar
func (b *BestPracticesCollector) collectHighAvailabilityStatus() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Yüksek erişilebilirlik metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	// AlwaysOn Availability Group durumu
	haStatus := make(map[string]interface{})
	rows, err := db.Query(`
		SELECT 
			ag.name AS ag_name,
			ar.replica_server_name,
			hars.role_desc, 
			hars.operational_state_desc,
			hars.connected_state_desc,
			harstc.synchronization_health_desc,
			drs.database_name,
			drs.synchronization_state_desc,
			drs.synchronization_health_desc,
			ISNULL(drs.log_send_queue_size, 0) AS log_send_queue_size,
			ISNULL(drs.log_send_rate, 0) AS log_send_rate,
			ISNULL(drs.redo_queue_size, 0) AS redo_queue_size,
			ISNULL(drs.redo_rate, 0) AS redo_rate,
			ISNULL(DATEDIFF(second, drs.last_hardened_time, GETDATE()), 0) AS seconds_behind
		FROM sys.availability_groups AS ag
		JOIN sys.availability_replicas AS ar ON ag.group_id = ar.group_id
		JOIN sys.dm_hadr_availability_replica_states AS hars ON ar.replica_id = hars.replica_id
		JOIN sys.dm_hadr_availability_replica_states AS harstc ON ag.group_id = harstc.group_id 
			AND harstc.role_desc = 'PRIMARY'
		LEFT JOIN sys.dm_hadr_database_replica_states AS drs ON drs.replica_id = hars.replica_id
		ORDER BY ag.name, ar.replica_server_name
	`)

	if err == nil {
		defer rows.Close()
		var agInfo []map[string]interface{}
		for rows.Next() {
			var agName, replicaServer, roleDesc, operationalState, connectedState, agSyncHealth string
			var dbName, syncState, dbSyncHealth sql.NullString
			var logSendQueueSize, logSendRate, redoQueueSize, redoRate, secondsBehind float64

			if err := rows.Scan(
				&agName, &replicaServer, &roleDesc, &operationalState, &connectedState, &agSyncHealth,
				&dbName, &syncState, &dbSyncHealth, &logSendQueueSize, &logSendRate,
				&redoQueueSize, &redoRate, &secondsBehind); err == nil {

				dbNameStr := ""
				if dbName.Valid {
					dbNameStr = dbName.String
				}

				syncStateStr := ""
				if syncState.Valid {
					syncStateStr = syncState.String
				}

				dbSyncHealthStr := ""
				if dbSyncHealth.Valid {
					dbSyncHealthStr = dbSyncHealth.String
				}

				agInfo = append(agInfo, map[string]interface{}{
					"ag_name":                agName,
					"replica_server":         replicaServer,
					"role":                   roleDesc,
					"operational_state":      operationalState,
					"connected_state":        connectedState,
					"ag_sync_health":         agSyncHealth,
					"database_name":          dbNameStr,
					"sync_state":             syncStateStr,
					"db_sync_health":         dbSyncHealthStr,
					"log_send_queue_size_kb": logSendQueueSize,
					"log_send_rate_kb_sec":   logSendRate,
					"redo_queue_size_kb":     redoQueueSize,
					"redo_rate_kb_sec":       redoRate,
					"seconds_behind":         secondsBehind,
				})
			}
		}
		haStatus["availability_groups"] = agInfo
		b.results["HighAvailabilityStatus"] = haStatus
	} else {
		// Sorgu çalışmadıysa AlwaysOn yapısı olmayabilir
		var isHadrEnabled int
		err = db.QueryRow("SELECT ISNULL(SERVERPROPERTY('IsHadrEnabled'), 0)").Scan(&isHadrEnabled)
		if err == nil {
			haStatus["is_hadr_enabled"] = isHadrEnabled != 0
			b.results["HighAvailabilityStatus"] = haStatus
		}
	}
}

// collectSecuritySettings güvenlik ayarlarını toplar
func (b *BestPracticesCollector) collectSecuritySettings() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Güvenlik metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	securitySettings := make(map[string]interface{})

	// SQL Server kimlik doğrulama modu
	var authMode int
	err = db.QueryRow("SELECT SERVERPROPERTY('IsIntegratedSecurityOnly')").Scan(&authMode)
	if err == nil {
		authModeDesc := "Mixed Mode"
		if authMode == 1 {
			authModeDesc = "Windows Authentication Mode"
		}

		securitySettings["auth_mode"] = map[string]interface{}{
			"integrated_security_only": authMode == 1,
			"description":              authModeDesc,
		}
	}

	// Şifreleme durumu
	var tdeEnabled []string
	rows, err := db.Query(`
		SELECT name 
		FROM sys.databases 
		WHERE is_encrypted = 1
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var dbName string
			if err := rows.Scan(&dbName); err == nil {
				tdeEnabled = append(tdeEnabled, dbName)
			}
		}
		securitySettings["transparent_data_encryption"] = map[string]interface{}{
			"enabled_databases": tdeEnabled,
			"count":             len(tdeEnabled),
		}
	}

	// SQL Server administrator olmayan login'ler ile sistem tablolarına erişim
	var sysadminCount, securityAdminCount, serverAdminCount int
	err = db.QueryRow(`
		SELECT 
			COUNT(CASE WHEN IS_SRVROLEMEMBER('sysadmin', name) = 1 THEN 1 ELSE NULL END) as sysadmin_count,
			COUNT(CASE WHEN IS_SRVROLEMEMBER('securityadmin', name) = 1 THEN 1 ELSE NULL END) as securityadmin_count,
			COUNT(CASE WHEN IS_SRVROLEMEMBER('serveradmin', name) = 1 THEN 1 ELSE NULL END) as serveradmin_count
		FROM sys.server_principals 
		WHERE type IN ('U', 'S', 'G') AND is_disabled = 0
	`).Scan(&sysadminCount, &securityAdminCount, &serverAdminCount)

	if err == nil {
		securitySettings["privileged_logins"] = map[string]interface{}{
			"sysadmin_count":      sysadminCount,
			"securityadmin_count": securityAdminCount,
			"serveradmin_count":   serverAdminCount,
		}
	}

	b.results["SecuritySettings"] = securitySettings
}

// collectSystemMetrics genel sistem metriklerini toplar
func (b *BestPracticesCollector) collectSystemMetrics() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Sistem metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	systemMetrics := make(map[string]interface{})

	// Buffer Cache ve Page Life Expectancy bilgileri
	var bufferCacheHitRatio, pageLifeExpectancy float64
	err = db.QueryRow(`
		SELECT 
			(CAST(CAST(cntr_value AS FLOAT) AS FLOAT) / 
			 CAST((SELECT cntr_value FROM sys.dm_os_performance_counters 
				   WHERE object_name LIKE '%Buffer Manager%' 
				   AND counter_name = 'Buffer cache hit ratio base') AS FLOAT)) AS BufferCacheHitRatio,
			(SELECT cntr_value FROM sys.dm_os_performance_counters 
			 WHERE object_name LIKE '%Buffer Manager%' 
			 AND counter_name = 'Page life expectancy') AS PageLifeExpectancy
		FROM sys.dm_os_performance_counters
		WHERE object_name LIKE '%Buffer Manager%'
		AND counter_name = 'Buffer cache hit ratio'
	`).Scan(&bufferCacheHitRatio, &pageLifeExpectancy)

	if err == nil {
		pageLifeExpectancyStatus := "Good"
		if pageLifeExpectancy < 300 {
			pageLifeExpectancyStatus = "Poor"
		}

		systemMetrics["buffer_cache"] = map[string]interface{}{
			"hit_ratio":                   bufferCacheHitRatio * 100, // Yüzde olarak
			"page_life_expectancy":        pageLifeExpectancy,
			"page_life_expectancy_status": pageLifeExpectancyStatus,
		}
	}

	// SQL Agent Job başarı durumları
	agentJobs := make(map[string]interface{})
	rows, err := db.Query(`
		SELECT 
			j.name,
			j.enabled,
			j.date_created,
			j.date_modified,
			ISNULL(last_run.run_date, 0) as last_run_date,
			ISNULL(last_run.run_time, 0) as last_run_time,
			ISNULL(last_run.run_status, 0) as last_run_status,
			ISNULL(last_run.run_duration, 0) as last_run_duration,
			ISNULL(last_run.message, '') as last_run_message
		FROM msdb.dbo.sysjobs j
		LEFT JOIN (
			SELECT job_id, run_date, run_time, run_status, run_duration, message
			FROM msdb.dbo.sysjobhistory 
			WHERE step_id = 0
			AND run_date = (
				SELECT MAX(run_date) 
				FROM msdb.dbo.sysjobhistory 
				WHERE job_id = job_id AND step_id = 0
			)
		) AS last_run ON j.job_id = last_run.job_id
		ORDER BY j.name
	`)

	if err == nil {
		defer rows.Close()
		var jobsList []map[string]interface{}
		var failedJobCount, successJobCount int

		for rows.Next() {
			var name string
			var enabled bool
			var dateCreated, dateModified time.Time
			var lastRunDate, lastRunTime, lastRunStatus, lastRunDuration int
			var lastRunMessage string

			if err := rows.Scan(&name, &enabled, &dateCreated, &dateModified,
				&lastRunDate, &lastRunTime, &lastRunStatus,
				&lastRunDuration, &lastRunMessage); err == nil {

				var lastRunStatusStr string
				if lastRunStatus == 1 {
					lastRunStatusStr = "Succeeded"
					successJobCount++
				} else if lastRunStatus == 0 {
					lastRunStatusStr = "Failed"
					failedJobCount++
				} else {
					lastRunStatusStr = "Unknown"
				}

				jobsList = append(jobsList, map[string]interface{}{
					"name":              name,
					"enabled":           enabled,
					"date_created":      dateCreated,
					"date_modified":     dateModified,
					"last_run_date":     lastRunDate,
					"last_run_time":     lastRunTime,
					"last_run_status":   lastRunStatusStr,
					"last_run_duration": lastRunDuration,
					"last_run_message":  lastRunMessage,
				})
			}
		}

		agentJobs["jobs"] = jobsList
		agentJobs["failed_count"] = failedJobCount
		agentJobs["success_count"] = successJobCount
		agentJobs["total_count"] = len(jobsList)

		systemMetrics["sql_agent_jobs"] = agentJobs
	}

	b.results["SystemMetrics"] = systemMetrics
}

// collectTempDBPerformance TempDB ile ilgili performans sorunlarını analiz eder
func (b *BestPracticesCollector) collectTempDBPerformance() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("TempDB performans analizi yapılamadı: %v", err)
		return
	}
	defer db.Close()

	tempDBMetrics := make(map[string]interface{})

	// TempDB dosya sayısı ve büyüklük uyumsuzluğu kontrolü
	tempDBFiles := make(map[string]interface{})
	rows, err := db.Query(`
		SELECT 
			name,
			physical_name,
			size * 8.0 / 1024 AS size_mb,
			FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024 AS space_used_mb,
			growth,
			is_percent_growth,
			type_desc
		FROM tempdb.sys.database_files
	`)

	if err == nil {
		defer rows.Close()
		var files []map[string]interface{}
		var totalSizeMB float64
		var fileCount int
		var sizesEqual bool = true
		var firstFileSize float64 = -1

		for rows.Next() {
			var name, physicalName, typeDesc string
			var sizeMB, spaceUsedMB float64
			var growth int
			var isPercentGrowth bool

			if err := rows.Scan(&name, &physicalName, &sizeMB, &spaceUsedMB, &growth, &isPercentGrowth, &typeDesc); err == nil {
				if typeDesc == "ROWS" {
					if firstFileSize < 0 {
						firstFileSize = sizeMB
					} else if sizeMB != firstFileSize {
						sizesEqual = false
					}
					fileCount++
					totalSizeMB += sizeMB
				}

				files = append(files, map[string]interface{}{
					"name":              name,
					"physical_name":     physicalName,
					"size_mb":           sizeMB,
					"space_used_mb":     spaceUsedMB,
					"usage_percent":     (spaceUsedMB / sizeMB) * 100,
					"growth":            growth,
					"is_percent_growth": isPercentGrowth,
					"type_desc":         typeDesc,
				})
			}
		}

		// CPU sayısını al
		var cpuCount int
		err = db.QueryRow("SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE'").Scan(&cpuCount)
		if err == nil {
			// TempDB optimize edilmiş mi?
			var recommendedFileCount int
			if cpuCount <= 8 {
				recommendedFileCount = cpuCount
			} else {
				recommendedFileCount = 8
			}

			tempDBFiles["files"] = files
			tempDBFiles["file_count"] = fileCount
			tempDBFiles["total_size_mb"] = totalSizeMB
			tempDBFiles["recommended_file_count"] = recommendedFileCount
			tempDBFiles["is_file_count_optimal"] = fileCount == recommendedFileCount
			tempDBFiles["is_size_equal"] = sizesEqual
			tempDBFiles["optimization_needed"] = !sizesEqual || fileCount != recommendedFileCount
		}

		tempDBMetrics["file_configuration"] = tempDBFiles
	}

	// TempDB contention/çakışma problemleri
	tempDBContention := make(map[string]interface{})
	var totalWaitTimeMs, totalWaitingTasks, maxWaitTimeMs, totalSignalWaitTimeMs int64

	err = db.QueryRow(`
		SELECT
			SUM(wait_time_ms) AS total_wait_time_ms,
			SUM(waiting_tasks_count) AS total_waiting_tasks,
			MAX(wait_time_ms) AS max_wait_time_ms,
			SUM(signal_wait_time_ms) AS total_signal_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type LIKE 'PAGELATCH_EX%' 
		AND (wait_type = 'PAGELATCH_EX' OR wait_type = 'PAGELATCH_SH' OR wait_type LIKE 'PAGELATCH_UP%')
	`).Scan(&totalWaitTimeMs, &totalWaitingTasks, &maxWaitTimeMs, &totalSignalWaitTimeMs)

	if err == nil {
		tempDBContention["total_wait_time_ms"] = totalWaitTimeMs
		tempDBContention["total_waiting_tasks"] = totalWaitingTasks
		tempDBContention["max_wait_time_ms"] = maxWaitTimeMs
		tempDBContention["total_signal_wait_time_ms"] = totalSignalWaitTimeMs

		// TempDB PFS/GAM/SGAM page contention
		var pgaContentionExists int
		err = db.QueryRow(`
			IF EXISTS (
				SELECT * FROM sys.dm_exec_requests r
				CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) s
				WHERE r.wait_type LIKE 'PAGELATCH%'
				AND s.text LIKE '%tempdb%'
			)
			SELECT 1
			ELSE
			SELECT 0
		`).Scan(&pgaContentionExists)

		if err == nil {
			tempDBContention["pfs_gam_contention_exists"] = pgaContentionExists == 1
			tempDBContention["contention_level"] = b.getTempDBContentionLevel(totalWaitTimeMs, totalWaitingTasks)
		}

		tempDBMetrics["contention"] = tempDBContention
	}

	// TempDB en büyük kullanıcılar
	var topConsumers []map[string]interface{}
	rows, err = db.Query(`
		SELECT TOP 10
			t.session_id,
			s.login_name,
			DB_NAME(s.database_id) AS database_name,
			(t.user_objects_alloc_page_count + t.internal_objects_alloc_page_count) * 8 AS allocated_kb,
			(t.user_objects_dealloc_page_count + t.internal_objects_dealloc_page_count) * 8 AS deallocated_kb,
			s.status,
			s.cpu_time,
			s.logical_reads,
			s.total_elapsed_time,
			SUBSTRING(
				(SELECT text FROM sys.dm_exec_sql_text(c.most_recent_sql_handle)), 
				1, 200
			) AS query_text
		FROM sys.dm_db_session_space_usage AS t
		INNER JOIN sys.dm_exec_sessions AS s ON t.session_id = s.session_id
		LEFT JOIN sys.dm_exec_connections AS c ON s.session_id = c.session_id
		WHERE t.session_id > 50
		ORDER BY (t.user_objects_alloc_page_count + t.internal_objects_alloc_page_count) DESC
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var sessionID int
			var loginName, databaseName, status, queryText sql.NullString
			var allocatedKB, deallocatedKB, cpuTime, logicalReads, totalElapsedTime int64

			if err := rows.Scan(&sessionID, &loginName, &databaseName, &allocatedKB, &deallocatedKB,
				&status, &cpuTime, &logicalReads, &totalElapsedTime, &queryText); err == nil {

				consumer := map[string]interface{}{
					"session_id":      sessionID,
					"login_name":      nullStringToString(loginName),
					"database_name":   nullStringToString(databaseName),
					"allocated_kb":    allocatedKB,
					"deallocated_kb":  deallocatedKB,
					"status":          nullStringToString(status),
					"cpu_time_ms":     cpuTime,
					"logical_reads":   logicalReads,
					"elapsed_time_ms": totalElapsedTime,
					"query_text":      nullStringToString(queryText),
				}

				topConsumers = append(topConsumers, consumer)
			}
		}

		tempDBMetrics["top_consumers"] = topConsumers
	}

	b.results["TempDBPerformance"] = tempDBMetrics
}

// getTempDBContentionLevel, TempDB contention seviyesini değerlendirir
func (b *BestPracticesCollector) getTempDBContentionLevel(waitTimeMs, waitingTasks int64) string {
	if waitTimeMs > 30000 && waitingTasks > 50 {
		return "High"
	} else if waitTimeMs > 10000 && waitingTasks > 20 {
		return "Medium"
	} else {
		return "Low"
	}
}

// nullStringToString, sql.NullString'i güvenli bir şekilde string'e dönüştürür
func nullStringToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// min iki sayının küçük olanını döndürür
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetBestPracticesAnalysis tüm metrikleri toplayıp sonuçları döndürür
// Bu AI tarafına veri gönderimi için kullanılabilir
func (b *BestPracticesCollector) GetBestPracticesAnalysis() map[string]interface{} {
	return b.CollectAll()
}

// collectMemoryUsage SQL Server bellek kullanım metriklerini toplar ve analiz eder
func (b *BestPracticesCollector) collectMemoryUsage() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Bellek kullanım metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	memoryMetrics := make(map[string]interface{})

	// Bellek kullanım genel özeti
	memoryOverview := make(map[string]interface{})

	// Geçici değişkenler oluştur
	var physicalMemoryMB, virtualMemoryMB, committedMemoryMB int64
	var committedTargetMB, totalPageFileMB, availablePageFileMB int64

	err = db.QueryRow(`
		SELECT
			physical_memory_kb / 1024 AS physical_memory_mb,
			virtual_memory_kb / 1024 AS virtual_memory_mb,
			committed_kb / 1024 AS committed_memory_mb,
			committed_target_kb / 1024 AS committed_target_mb,
			total_page_file_kb / 1024 AS total_page_file_mb,
			available_page_file_kb / 1024 AS available_page_file_mb
		FROM sys.dm_os_sys_info
	`).Scan(
		&physicalMemoryMB,
		&virtualMemoryMB,
		&committedMemoryMB,
		&committedTargetMB,
		&totalPageFileMB,
		&availablePageFileMB,
	)

	if err == nil {
		// Verileri mape aktar
		memoryOverview["physical_memory_mb"] = physicalMemoryMB
		memoryOverview["virtual_memory_mb"] = virtualMemoryMB
		memoryOverview["committed_memory_mb"] = committedMemoryMB
		memoryOverview["committed_target_mb"] = committedTargetMB
		memoryOverview["total_page_file_mb"] = totalPageFileMB
		memoryOverview["available_page_file_mb"] = availablePageFileMB

		// SQL Server bellek ayarları
		var maxServerMemory, minServerMemory int64
		err = db.QueryRow(`
			SELECT
				(SELECT value_in_use FROM sys.configurations WHERE name = 'max server memory (MB)') AS max_server_memory,
				(SELECT value_in_use FROM sys.configurations WHERE name = 'min server memory (MB)') AS min_server_memory
		`).Scan(&maxServerMemory, &minServerMemory)

		if err == nil {
			memoryOverview["max_server_memory_mb"] = maxServerMemory
			memoryOverview["min_server_memory_mb"] = minServerMemory

			// Doğru bellek ayarları değerlendirmesi
			memoryOverview["recommended_max_memory_mb"] = int64(float64(physicalMemoryMB) * 0.9) // System için %10 boş bırak
			memoryOverview["is_max_memory_optimal"] = maxServerMemory > 0 && maxServerMemory < physicalMemoryMB
		}

		memoryMetrics["overview"] = memoryOverview
	}

	// Buffer pool kullanımı ve page life expectancy
	bufferPoolMetrics := make(map[string]interface{})

	// Geçici değişkenler
	var pageLifeExpectancy, freePages, totalPages, targetPages, databasePages int64

	err = db.QueryRow(`
		SELECT
			(SELECT cntr_value FROM sys.dm_os_performance_counters 
			 WHERE counter_name = 'Page life expectancy' 
			 AND object_name LIKE '%Buffer Manager%') AS page_life_expectancy,
			
			(SELECT cntr_value FROM sys.dm_os_performance_counters 
			 WHERE counter_name = 'Free pages' 
			 AND object_name LIKE '%Buffer Manager%') AS free_pages,
			
			(SELECT cntr_value FROM sys.dm_os_performance_counters 
			 WHERE counter_name = 'Total pages' 
			 AND object_name LIKE '%Buffer Manager%') AS total_pages,
			
			(SELECT cntr_value FROM sys.dm_os_performance_counters 
			 WHERE counter_name = 'Target pages' 
			 AND object_name LIKE '%Buffer Manager%') AS target_pages,
			
			(SELECT cntr_value FROM sys.dm_os_performance_counters 
			 WHERE counter_name = 'Database pages' 
			 AND object_name LIKE '%Buffer Manager%') AS database_pages
	`).Scan(
		&pageLifeExpectancy,
		&freePages,
		&totalPages,
		&targetPages,
		&databasePages,
	)

	if err == nil {
		// Verileri mape aktar
		bufferPoolMetrics["page_life_expectancy"] = pageLifeExpectancy
		bufferPoolMetrics["free_pages"] = freePages
		bufferPoolMetrics["total_pages"] = totalPages
		bufferPoolMetrics["target_pages"] = targetPages
		bufferPoolMetrics["database_pages"] = databasePages

		// Page life expectancy değerlendirmesi (saniye)
		// En az 300 saniye (5 dakika) olması önerilir
		bufferPoolMetrics["ple_status"] = b.getPageLifeExpectancyStatus(pageLifeExpectancy)

		// Bellek kullanım oranları hesapla
		if totalPages > 0 {
			// 8KB sayfalar olarak buffer pool kullanımı
			bufferPoolMetrics["buffer_pool_used_mb"] = (totalPages - freePages) * 8 / 1024
			bufferPoolMetrics["buffer_pool_free_mb"] = freePages * 8 / 1024
			bufferPoolMetrics["buffer_pool_total_mb"] = totalPages * 8 / 1024
			bufferPoolMetrics["buffer_cache_used_percent"] = float64(totalPages-freePages) / float64(totalPages) * 100
		}

		memoryMetrics["buffer_pool"] = bufferPoolMetrics
	}

	// Memory clerks - Bellek kullanım dağılımı
	rows, err := db.Query(`
		SELECT TOP 10
			type,
			name,
			memory_node_id,
			pages_kb / 1024 AS pages_mb,
			virtual_memory_reserved_kb / 1024 AS virtual_memory_reserved_mb,
			virtual_memory_committed_kb / 1024 AS virtual_memory_committed_mb,
			awe_allocated_kb / 1024 AS awe_allocated_mb,
			shared_memory_reserved_kb / 1024 AS shared_memory_reserved_mb,
			shared_memory_committed_kb / 1024 AS shared_memory_committed_mb
		FROM sys.dm_os_memory_clerks
		ORDER BY pages_kb DESC
	`)

	if err == nil {
		defer rows.Close()
		var memoryClerksList []map[string]interface{}

		for rows.Next() {
			var clerkType, clerkName string
			var nodeID int
			var pagesMB, vmReservedMB, vmCommittedMB, aweAllocatedMB, sharedReservedMB, sharedCommittedMB int64

			if err := rows.Scan(&clerkType, &clerkName, &nodeID, &pagesMB, &vmReservedMB,
				&vmCommittedMB, &aweAllocatedMB, &sharedReservedMB, &sharedCommittedMB); err == nil {

				clerk := map[string]interface{}{
					"type":                        clerkType,
					"name":                        clerkName,
					"memory_node_id":              nodeID,
					"pages_mb":                    pagesMB,
					"virtual_memory_reserved_mb":  vmReservedMB,
					"virtual_memory_committed_mb": vmCommittedMB,
					"awe_allocated_mb":            aweAllocatedMB,
					"shared_memory_reserved_mb":   sharedReservedMB,
					"shared_memory_committed_mb":  sharedCommittedMB,
				}

				memoryClerksList = append(memoryClerksList, clerk)
			}
		}

		memoryMetrics["memory_clerks"] = memoryClerksList
	}

	// Memory grants ve memory pressure
	memoryPressure := make(map[string]interface{})

	// Geçici değişkenler
	var waitingTasksCount, totalRequestedMemoryMB, totalGrantedMemoryMB int64
	var maxWaitTimeMS int64
	var avgWaitTimeMS float64

	err = db.QueryRow(`
		SELECT
			COUNT(*) AS waiting_tasks_count,
			ISNULL(SUM(requested_memory_kb)/1024, 0) AS total_requested_memory_mb,
			ISNULL(SUM(granted_memory_kb)/1024, 0) AS total_granted_memory_mb,
			ISNULL(MAX(wait_time_ms), 0) AS max_wait_time_ms,
			ISNULL(AVG(wait_time_ms), 0) AS avg_wait_time_ms
		FROM sys.dm_exec_query_resource_semaphores
	`).Scan(
		&waitingTasksCount,
		&totalRequestedMemoryMB,
		&totalGrantedMemoryMB,
		&maxWaitTimeMS,
		&avgWaitTimeMS,
	)

	if err == nil {
		// Verileri mape aktar
		memoryPressure["waiting_tasks_count"] = waitingTasksCount
		memoryPressure["total_requested_memory_mb"] = totalRequestedMemoryMB
		memoryPressure["total_granted_memory_mb"] = totalGrantedMemoryMB
		memoryPressure["max_wait_time_ms"] = maxWaitTimeMS
		memoryPressure["avg_wait_time_ms"] = avgWaitTimeMS

		// Memory pressure durumunu değerlendir
		if waitingTasksCount > 0 && avgWaitTimeMS > 1000 {
			memoryPressure["pressure_level"] = "High"
		} else if waitingTasksCount > 0 {
			memoryPressure["pressure_level"] = "Medium"
		} else {
			memoryPressure["pressure_level"] = "Low"
		}

		memoryMetrics["memory_pressure"] = memoryPressure
	}

	// En çok bellek kullanan sorgular
	var memoryConsumingQueries []map[string]interface{}
	rows, err = db.Query(`
		SELECT TOP 10
			t.text AS query_text,
			s.plan_handle,
			s.total_worker_time / 1000 AS total_cpu_time_ms,
			s.total_elapsed_time / 1000 AS total_elapsed_time_ms,
			s.total_logical_reads AS total_logical_reads,
			s.total_logical_writes AS total_logical_writes,
			s.total_physical_reads AS total_physical_reads,
			s.execution_count,
			s.total_logical_reads / s.execution_count AS avg_logical_reads,
			s.last_logical_reads,
			s.min_logical_reads,
			s.max_logical_reads,
			s.total_logical_writes / s.execution_count AS avg_logical_writes,
			q.requested_memory_kb / 1024 AS requested_memory_mb,
			q.granted_memory_kb / 1024 AS granted_memory_mb,
			q.ideal_memory_kb / 1024 AS ideal_memory_mb,
			q.required_memory_kb / 1024 AS required_memory_mb
		FROM sys.dm_exec_query_stats s
		CROSS APPLY sys.dm_exec_sql_text(s.sql_handle) t
		CROSS APPLY sys.dm_exec_query_memory_grants q
		WHERE s.plan_handle = q.plan_handle
		ORDER BY q.granted_memory_kb DESC
	`)

	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var queryText sql.NullString
			var planHandle []byte
			var totalCpuTime, totalElapsedTime float64
			var totalLogicalReads, totalLogicalWrites, totalPhysicalReads, executionCount int64
			var avgLogicalReads, lastLogicalReads, minLogicalReads, maxLogicalReads, avgLogicalWrites int64
			var requestedMemoryMB, grantedMemoryMB, idealMemoryMB, requiredMemoryMB int64

			if err := rows.Scan(&queryText, &planHandle, &totalCpuTime, &totalElapsedTime,
				&totalLogicalReads, &totalLogicalWrites, &totalPhysicalReads, &executionCount,
				&avgLogicalReads, &lastLogicalReads, &minLogicalReads, &maxLogicalReads,
				&avgLogicalWrites, &requestedMemoryMB, &grantedMemoryMB, &idealMemoryMB, &requiredMemoryMB); err == nil {

				query := map[string]interface{}{
					"query_text":              nullStringToString(queryText),
					"total_cpu_time_ms":       totalCpuTime,
					"total_elapsed_time_ms":   totalElapsedTime,
					"execution_count":         executionCount,
					"total_logical_reads":     totalLogicalReads,
					"total_logical_writes":    totalLogicalWrites,
					"total_physical_reads":    totalPhysicalReads,
					"avg_logical_reads":       avgLogicalReads,
					"requested_memory_mb":     requestedMemoryMB,
					"granted_memory_mb":       grantedMemoryMB,
					"ideal_memory_mb":         idealMemoryMB,
					"required_memory_mb":      requiredMemoryMB,
					"memory_grant_efficiency": float64(grantedMemoryMB) / float64(idealMemoryMB) * 100,
				}

				memoryConsumingQueries = append(memoryConsumingQueries, query)
			}
		}

		memoryMetrics["memory_consuming_queries"] = memoryConsumingQueries
	}

	b.results["MemoryUsage"] = memoryMetrics
}

// getPageLifeExpectancyStatus, page life expectancy durumunu değerlendirir
func (b *BestPracticesCollector) getPageLifeExpectancyStatus(ple int64) string {
	// Her 4GB RAM için 300 saniye olmalı
	memoryGB := int64(0)

	if serverInfo, ok := b.results["SystemMetrics"].(map[string]interface{}); ok {
		if bufferCache, ok := serverInfo["buffer_cache"].(map[string]interface{}); ok {
			if ple, ok := bufferCache["page_life_expectancy"].(float64); ok {
				if ple < 300 {
					return "Critical"
				} else if ple < 900 {
					return "Warning"
				}
			}
		}
	}

	// Her 4GB için 300 saniye
	threshold := (memoryGB / 4) * 300
	if threshold < 300 {
		threshold = 300 // Minimum 300 saniye
	}

	if ple < threshold {
		return "Critical"
	} else if ple < threshold*2 {
		return "Warning"
	}

	return "Good"
}

// collectIOPerformance SQL Server I/O performans metriklerini toplar ve analiz eder
func (b *BestPracticesCollector) collectIOPerformance() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("I/O performans metrikleri toplanamadı: %v", err)
		return
	}
	defer db.Close()

	ioMetrics := make(map[string]interface{})

	// Veritabanı dosya I/O istatistikleri
	rows, err := db.Query(`
		SELECT
			DB_NAME(vfs.database_id) AS database_name,
			vfs.file_id,
			mf.name AS file_name,
			mf.physical_name,
			mf.type_desc,
			vfs.num_of_reads,
			vfs.num_of_bytes_read,
			vfs.io_stall_read_ms,
			vfs.num_of_writes,
			vfs.num_of_bytes_written,
			vfs.io_stall_write_ms,
			vfs.io_stall,
			vfs.size_on_disk_bytes / 1024 / 1024 AS size_on_disk_mb,
			CASE WHEN vfs.num_of_reads = 0 THEN 0 ELSE vfs.io_stall_read_ms / vfs.num_of_reads END AS avg_read_stall_ms,
			CASE WHEN vfs.num_of_writes = 0 THEN 0 ELSE vfs.io_stall_write_ms / vfs.num_of_writes END AS avg_write_stall_ms
		FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
		JOIN sys.master_files AS mf
			ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
		ORDER BY io_stall DESC
	`)

	if err == nil {
		defer rows.Close()
		var databaseIOStats []map[string]interface{}

		for rows.Next() {
			var dbName, fileName, physicalName, typeDesc sql.NullString
			var fileID, numReads, numWrites int64
			var bytesRead, bytesWritten, readStallMs, writeStallMs, ioStall, sizeOnDiskMB int64
			var avgReadStallMs, avgWriteStallMs int64

			if err := rows.Scan(&dbName, &fileID, &fileName, &physicalName, &typeDesc,
				&numReads, &bytesRead, &readStallMs, &numWrites, &bytesWritten, &writeStallMs,
				&ioStall, &sizeOnDiskMB, &avgReadStallMs, &avgWriteStallMs); err == nil {

				fileStats := map[string]interface{}{
					"database_name":      nullStringToString(dbName),
					"file_id":            fileID,
					"file_name":          nullStringToString(fileName),
					"physical_name":      nullStringToString(physicalName),
					"type_desc":          nullStringToString(typeDesc),
					"num_reads":          numReads,
					"bytes_read_mb":      bytesRead / 1024 / 1024,
					"read_stall_ms":      readStallMs,
					"num_writes":         numWrites,
					"bytes_written_mb":   bytesWritten / 1024 / 1024,
					"write_stall_ms":     writeStallMs,
					"io_stall_ms":        ioStall,
					"size_on_disk_mb":    sizeOnDiskMB,
					"avg_read_stall_ms":  avgReadStallMs,
					"avg_write_stall_ms": avgWriteStallMs,
					"io_performance":     b.evaluateIOPerformance(avgReadStallMs, avgWriteStallMs),
				}

				databaseIOStats = append(databaseIOStats, fileStats)
			}
		}

		ioMetrics["database_files"] = databaseIOStats
	}

	// Pending I/O Requests
	var pendingIORequests []map[string]interface{}
	rows, err = db.Query(`
		SELECT 
			io_pending, 
			io_pending_ms_ticks, 
			scheduler_address 
		FROM sys.dm_io_pending_io_requests
	`)

	if err == nil {
		defer rows.Close()

		for rows.Next() {
			var ioPending int
			var ioPendingMsTicks int64
			var schedulerAddress []byte

			if err := rows.Scan(&ioPending, &ioPendingMsTicks, &schedulerAddress); err == nil {
				pendingIO := map[string]interface{}{
					"io_pending":          ioPending,
					"io_pending_ms_ticks": ioPendingMsTicks,
				}

				pendingIORequests = append(pendingIORequests, pendingIO)
			}
		}

		ioMetrics["pending_io_requests"] = pendingIORequests
		ioMetrics["pending_io_count"] = len(pendingIORequests)
	}

	// Genel I/O istatistikleri (Drive bazında)
	rows, err = db.Query(`
		SELECT
			LEFT(mf.physical_name, 1) AS drive_letter,
			SUM(vfs.num_of_reads) AS num_reads,
			SUM(vfs.num_of_writes) AS num_writes,
			SUM(vfs.num_of_bytes_read) / 1024 / 1024 AS bytes_read_mb,
			SUM(vfs.num_of_bytes_written) / 1024 / 1024 AS bytes_written_mb,
			SUM(vfs.io_stall_read_ms) AS read_stall_ms,
			SUM(vfs.io_stall_write_ms) AS write_stall_ms,
			SUM(vfs.io_stall) AS io_stall_ms,
			CASE WHEN SUM(vfs.num_of_reads) = 0 THEN 0 ELSE SUM(vfs.io_stall_read_ms) / SUM(vfs.num_of_reads) END AS avg_read_stall_ms,
			CASE WHEN SUM(vfs.num_of_writes) = 0 THEN 0 ELSE SUM(vfs.io_stall_write_ms) / SUM(vfs.num_of_writes) END AS avg_write_stall_ms
		FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
		JOIN sys.master_files AS mf
			ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
		GROUP BY LEFT(mf.physical_name, 1)
		ORDER BY SUM(vfs.io_stall) DESC
	`)

	if err == nil {
		defer rows.Close()
		var driveStats []map[string]interface{}

		for rows.Next() {
			var driveLetter string
			var numReads, numWrites, bytesReadMB, bytesWrittenMB int64
			var readStallMs, writeStallMs, ioStallMs, avgReadStallMs, avgWriteStallMs int64

			if err := rows.Scan(&driveLetter, &numReads, &numWrites, &bytesReadMB, &bytesWrittenMB,
				&readStallMs, &writeStallMs, &ioStallMs, &avgReadStallMs, &avgWriteStallMs); err == nil {

				driveStat := map[string]interface{}{
					"drive_letter":       driveLetter,
					"num_reads":          numReads,
					"num_writes":         numWrites,
					"bytes_read_mb":      bytesReadMB,
					"bytes_written_mb":   bytesWrittenMB,
					"read_stall_ms":      readStallMs,
					"write_stall_ms":     writeStallMs,
					"io_stall_ms":        ioStallMs,
					"avg_read_stall_ms":  avgReadStallMs,
					"avg_write_stall_ms": avgWriteStallMs,
					"io_performance":     b.evaluateIOPerformance(avgReadStallMs, avgWriteStallMs),
				}

				driveStats = append(driveStats, driveStat)
			}
		}

		ioMetrics["drive_stats"] = driveStats
	}

	// Checkpoint ve lazy writer istatistikleri
	var bufferManagerStats map[string]interface{}

	var checkpointPages, lazyWrites, pageReads, pageWrites int64

	err = db.QueryRow(`
		SELECT
			(SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Checkpoint pages/sec' AND object_name LIKE '%Buffer Manager%') AS checkpoint_pages,
			(SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Lazy writes/sec' AND object_name LIKE '%Buffer Manager%') AS lazy_writes,
			(SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page reads/sec' AND object_name LIKE '%Buffer Manager%') AS page_reads,
			(SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Page writes/sec' AND object_name LIKE '%Buffer Manager%') AS page_writes
	`).Scan(&checkpointPages, &lazyWrites, &pageReads, &pageWrites)

	if err == nil {
		bufferManagerStats = map[string]interface{}{
			"checkpoint_pages_sec": checkpointPages,
			"lazy_writes_sec":      lazyWrites,
			"page_reads_sec":       pageReads,
			"page_writes_sec":      pageWrites,
		}

		ioMetrics["buffer_manager"] = bufferManagerStats
	}

	// I/O ile ilgili en yüksek bekleme türleri
	var ioWaitStats []map[string]interface{}
	rows, err = db.Query(`
		SELECT 
			wait_type,
			waiting_tasks_count,
			wait_time_ms,
			max_wait_time_ms,
			signal_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type LIKE 'PAGEIOLATCH%'
		OR wait_type LIKE 'IO_COMPLETION%'
		OR wait_type LIKE 'WRITELOG%'
		OR wait_type LIKE 'ASYNC_IO_COMPLETION%'
		OR wait_type = 'IO_QUEUE_LIMIT'
		ORDER BY wait_time_ms DESC
	`)

	if err == nil {
		defer rows.Close()

		for rows.Next() {
			var waitType string
			var waitingTasksCount, waitTimeMs, maxWaitTimeMs, signalWaitTimeMs int64

			if err := rows.Scan(&waitType, &waitingTasksCount, &waitTimeMs, &maxWaitTimeMs, &signalWaitTimeMs); err == nil {
				waitStat := map[string]interface{}{
					"wait_type":             waitType,
					"waiting_tasks_count":   waitingTasksCount,
					"wait_time_ms":          waitTimeMs,
					"max_wait_time_ms":      maxWaitTimeMs,
					"signal_wait_time_ms":   signalWaitTimeMs,
					"resource_wait_time_ms": waitTimeMs - signalWaitTimeMs,
					"avg_wait_time_ms":      getAvgWaitTime(waitTimeMs, waitingTasksCount),
				}

				ioWaitStats = append(ioWaitStats, waitStat)
			}
		}

		ioMetrics["io_wait_stats"] = ioWaitStats
	}

	// I/O Performans Özeti
	ioSummary := make(map[string]interface{})

	// En yüksek gecikme gösteren dosyalar
	var highLatencyFiles []map[string]interface{}
	if files, ok := ioMetrics["database_files"].([]map[string]interface{}); ok {
		for _, file := range files {
			if avgReadMs, ok := file["avg_read_stall_ms"].(int64); ok {
				if avgWriteMs, ok := file["avg_write_stall_ms"].(int64); ok {
					if (avgReadMs > 20 || avgWriteMs > 20) &&
						(file["num_reads"].(int64) > 100 || file["num_writes"].(int64) > 100) {
						highLatencyFiles = append(highLatencyFiles, file)
					}
				}
			}

			// En fazla 5 dosya göster
			if len(highLatencyFiles) >= 5 {
				break
			}
		}

		ioSummary["high_latency_files"] = highLatencyFiles
		ioSummary["high_latency_file_count"] = len(highLatencyFiles)
	}

	ioMetrics["summary"] = ioSummary
	b.results["IOPerformance"] = ioMetrics
}

// evaluateIOPerformance I/O performansını değerlendirir
func (b *BestPracticesCollector) evaluateIOPerformance(avgReadStallMs, avgWriteStallMs int64) string {
	// Microsoft önerilerine göre:
	// <= 10ms: Very good
	// 10-20ms: Good
	// 20-50ms: Poor
	// > 50ms: Critical

	maxStall := avgReadStallMs
	if avgWriteStallMs > maxStall {
		maxStall = avgWriteStallMs
	}

	if maxStall <= 10 {
		return "Very Good"
	} else if maxStall <= 20 {
		return "Good"
	} else if maxStall <= 50 {
		return "Poor"
	} else {
		return "Critical"
	}
}

// getAvgWaitTime ortalama bekleme süresini hesaplar
func getAvgWaitTime(waitTimeMs, waitingTasksCount int64) int64 {
	if waitingTasksCount > 0 {
		return waitTimeMs / waitingTasksCount
	}
	return 0
}
