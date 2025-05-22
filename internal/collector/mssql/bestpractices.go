package mssql

import (
	"database/sql"
	"fmt"
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
	// Sonuçları içeren ana harita
	b.results = make(map[string]interface{})

	// Başlangıç zamanı
	startTime := time.Now()

	// Her bir kategori için ayrı retry mekanizması ve panic koruması uygulayalım
	// Bu şekilde bir kategorideki hata diğerlerinin çalışmasını engellemeyecek
	collectWithRetry(func() {
		b.collectSystemConfiguration()
	}, "SystemConfiguration", 2)

	collectWithRetry(func() {
		b.collectDatabaseHealth()
	}, "DatabaseHealth", 2)

	collectWithRetry(func() {
		b.collectPerformanceMetrics()
	}, "PerformanceMetrics", 2)

	collectWithRetry(func() {
		b.collectHighAvailabilityStatus()
	}, "HighAvailabilityStatus", 2)

	collectWithRetry(func() {
		b.collectSecuritySettings()
	}, "SecuritySettings", 2)

	collectWithRetry(func() {
		b.collectSystemMetrics()
	}, "SystemMetrics", 2)

	collectWithRetry(func() {
		b.collectBackupStatus()
	}, "BackupStatus", 2)

	// TempDB yapılandırması kontrolü - ayrı bir fonksiyon olarak ekliyoruz
	collectWithRetry(func() {
		b.collectTempDBConfiguration()
	}, "TempDBConfiguration", 2)

	// Analiz tamamlandı, toplam süreyi ekle
	duration := time.Since(startTime)
	b.results["analysis_duration_ms"] = duration.Milliseconds()
	b.results["analyzed_categories"] = len(b.results)
	b.results["analysis_timestamp"] = time.Now().Format(time.RFC3339)

	return b.results
}

// collectWithRetry, bir koleksiyon fonksiyonunu belirtilen sayıda tekrar dener
// Panic durumunda recovery sağlar ve logging yapar
func collectWithRetry(collectFunc func(), categoryName string, maxRetries int) {
	// En dışta panic recovery ekliyoruz
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] %s kategorisi panic ile başarısız oldu: %v", categoryName, r)
		}
	}()

	// Belirtilen sayıda deneme yap
	for attempt := 0; attempt < maxRetries; attempt++ {
		// İlk deneme değilse biraz bekle
		if attempt > 0 {
			wait := time.Duration(attempt*2+1) * time.Second
			log.Printf("[INFO] %s kategorisi yeniden deneniyor (%d/%d) - %s bekleniyor...",
				categoryName, attempt+1, maxRetries, wait)
			time.Sleep(wait)
		}

		// Fonksiyonu çağır, panic recovery ile
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[ERROR] %s deneme %d: Panic oluştu: %v",
						categoryName, attempt+1, r)
				}
			}()

			// Fonksiyonu çağır
			collectFunc()
		}()

		// Eğer başarılıysa döngüden çık
		// (Fonksiyon dönebilmişse panic olmamış demektir)
		break
	}
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

// collectBackupStatus yedekleme durumunu kontrol eder ve raporlar
func (b *BestPracticesCollector) collectBackupStatus() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Yedekleme durumu toplanamadı: %v", err)
		return
	}
	defer db.Close()

	backupStatus := make(map[string]interface{})

	// Veritabanı yedekleme durumlarını sorgula
	rows, err := db.Query(`
		SELECT 
			d.name AS database_name,
			ISNULL(MAX(CASE WHEN b.type = 'D' THEN b.backup_finish_date END), '1900-01-01') as last_full_backup,
			ISNULL(MAX(CASE WHEN b.type = 'I' THEN b.backup_finish_date END), '1900-01-01') as last_diff_backup,
			ISNULL(MAX(CASE WHEN b.type = 'L' THEN b.backup_finish_date END), '1900-01-01') as last_log_backup,
			d.recovery_model_desc
		FROM 
			sys.databases d
		LEFT JOIN msdb.dbo.backupset b 
			ON d.name = b.database_name
		WHERE
			d.database_id > 4 -- Sistem veritabanlarını hariç tut
			AND d.state_desc = 'ONLINE' -- Sadece online veritabanları
			AND d.name NOT IN ('tempdb') -- tempdb hariç tut
		GROUP BY 
			d.name, d.recovery_model_desc
		ORDER BY 
			d.name
	`)

	if err != nil {
		log.Printf("Yedekleme sorgusu çalıştırılamadı: %v", err)
		return
	}
	defer rows.Close()

	// Her veritabanı için yedekleme bilgilerini topla
	for rows.Next() {
		var (
			dbName         string
			lastFullBackup time.Time
			lastDiffBackup time.Time
			lastLogBackup  time.Time
			recoveryModel  string
		)

		if err := rows.Scan(&dbName, &lastFullBackup, &lastDiffBackup, &lastLogBackup, &recoveryModel); err != nil {
			log.Printf("Veritabanı yedekleme bilgisi okunamadı: %v", err)
			continue
		}

		// En son alınan yedeklemeyi belirle
		var lastBackupDate time.Time
		var backupType string

		if lastFullBackup.After(time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)) {
			lastBackupDate = lastFullBackup
			backupType = "Full"
		}

		if lastDiffBackup.After(lastBackupDate) {
			lastBackupDate = lastDiffBackup
			backupType = "Differential"
		}

		if lastLogBackup.After(lastBackupDate) && recoveryModel == "FULL" {
			lastBackupDate = lastLogBackup
			backupType = "Log"
		}

		dbBackupInfo := make(map[string]interface{})

		// Eğer hiç yedek yoksa
		if lastBackupDate.Before(time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)) {
			dbBackupInfo["last_backup_date"] = ""
			dbBackupInfo["backup_type"] = "None"
			dbBackupInfo["days_since_last_backup"] = -1
			dbBackupInfo["has_backup"] = false
		} else {
			// RFC3339 formatında tarih - reporter.go bu formatı bekliyor
			dbBackupInfo["last_backup_date"] = lastBackupDate.Format(time.RFC3339)
			dbBackupInfo["backup_type"] = backupType
			dbBackupInfo["days_since_last_backup"] = int(time.Since(lastBackupDate).Hours() / 24)
			dbBackupInfo["has_backup"] = true
		}

		// Recovery model'i de ekle
		dbBackupInfo["recovery_model"] = recoveryModel

		// Full/Diff/Log yedekleme tarihlerini de ayrı ayrı ekle
		if lastFullBackup.After(time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)) {
			dbBackupInfo["last_full_backup"] = lastFullBackup.Format(time.RFC3339)
			dbBackupInfo["days_since_full_backup"] = int(time.Since(lastFullBackup).Hours() / 24)
		} else {
			dbBackupInfo["last_full_backup"] = ""
			dbBackupInfo["days_since_full_backup"] = -1
		}

		if lastDiffBackup.After(time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)) {
			dbBackupInfo["last_diff_backup"] = lastDiffBackup.Format(time.RFC3339)
			dbBackupInfo["days_since_diff_backup"] = int(time.Since(lastDiffBackup).Hours() / 24)
		} else {
			dbBackupInfo["last_diff_backup"] = ""
			dbBackupInfo["days_since_diff_backup"] = -1
		}

		if lastLogBackup.After(time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)) {
			dbBackupInfo["last_log_backup"] = lastLogBackup.Format(time.RFC3339)
			dbBackupInfo["days_since_log_backup"] = int(time.Since(lastLogBackup).Hours() / 24)
		} else {
			dbBackupInfo["last_log_backup"] = ""
			dbBackupInfo["days_since_log_backup"] = -1
		}

		// Bu veritabanının yedekleme bilgilerini map'e ekle
		backupStatus[dbName] = dbBackupInfo
	}

	// Toplam yedekleme durumunu hesapla
	var totalDbs, dbsWithBackup, dbsWithRecentBackup int
	var oldestBackupDays int = -1

	for _, backupInfo := range backupStatus {
		totalDbs++
		if dbInfo, ok := backupInfo.(map[string]interface{}); ok {
			if hasBackup, ok := dbInfo["has_backup"].(bool); ok && hasBackup {
				dbsWithBackup++

				// Son 7 gün içinde yedek alındı mı kontrol et
				if days, ok := dbInfo["days_since_last_backup"].(int); ok {
					if days >= 0 && days <= 7 {
						dbsWithRecentBackup++
					}

					// En eski yedeği bul
					if oldestBackupDays == -1 || days > oldestBackupDays {
						oldestBackupDays = days
					}
				}
			}
		}
	}

	// Özet bilgi
	backupStatus["summary"] = map[string]interface{}{
		"total_databases":              totalDbs,
		"databases_with_backup":        dbsWithBackup,
		"databases_with_recent_backup": dbsWithRecentBackup,
		"oldest_backup_days":           oldestBackupDays,
		"backup_coverage_percent":      float64(dbsWithBackup) / float64(totalDbs) * 100,
	}

	// Tüm sonuçları kaydet
	b.results["BackupStatus"] = backupStatus
}

// collectTempDBConfiguration TempDB yapılandırmasını kontrol eder
func (b *BestPracticesCollector) collectTempDBConfiguration() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("TempDB yapılandırması toplanamadı: %v", err)
		// Hata durumunda bile boş bir yapı oluştur
		b.results["TempDBConfiguration"] = map[string]interface{}{
			"error":  err.Error(),
			"status": "error",
		}
		return
	}
	defer db.Close()

	tempDBConfig := make(map[string]interface{})

	// TempDB dosya sayısı ve CPU sayısı kontrolü
	var cpuCount, tempDBFileCount int
	var totalSizeMB, availableSizeMB float64

	// CPU sayısını al
	err = db.QueryRow(`SELECT cpu_count FROM sys.dm_os_sys_info`).Scan(&cpuCount)
	if err != nil {
		log.Printf("CPU sayısı alınamadı: %v", err)
		tempDBConfig["cpu_count_error"] = err.Error()
		cpuCount = 0
	}
	tempDBConfig["cpu_count"] = cpuCount

	// TempDB dosya sayısını ve boyutlarını al
	rows, err := db.Query(`
		SELECT 
			COUNT(*) as file_count,
			CAST(SUM(size * 8.0 / 1024) AS DECIMAL(18,2)) as total_size_mb,
			CAST(SUM(CASE WHEN max_size = -1 THEN 0 ELSE max_size * 8.0 / 1024 - size * 8.0 / 1024 END) AS DECIMAL(18,2)) as available_size_mb
		FROM sys.master_files
		WHERE database_id = DB_ID('tempdb') AND type_desc = 'ROWS'
	`)

	if err != nil {
		log.Printf("TempDB dosya bilgileri alınamadı: %v", err)
		tempDBConfig["file_info_error"] = err.Error()
	} else {
		defer rows.Close()
		if rows.Next() {
			err := rows.Scan(&tempDBFileCount, &totalSizeMB, &availableSizeMB)
			if err != nil {
				log.Printf("TempDB dosya bilgileri tarama hatası: %v", err)
				tempDBConfig["scan_error"] = err.Error()
			} else {
				// CPU sayısı ile TempDB dosya sayısı kontrolü
				isTempDBOptimal := true
				var recommendation string

				if cpuCount > 0 && tempDBFileCount < cpuCount && cpuCount <= 8 {
					isTempDBOptimal = false
					recommendation = fmt.Sprintf("TempDB dosya sayısı CPU sayısından az. Önerilen: %d dosya", cpuCount)
				} else if cpuCount > 8 && tempDBFileCount < 8 {
					isTempDBOptimal = false
					recommendation = "TempDB dosya sayısı 8'den az. Önerilen: En az 8 dosya"
				}

				tempDBConfig["file_count"] = tempDBFileCount
				tempDBConfig["is_optimal"] = isTempDBOptimal
				if !isTempDBOptimal {
					tempDBConfig["recommendation"] = recommendation
				}
				tempDBConfig["total_size_mb"] = totalSizeMB
				tempDBConfig["available_size_mb"] = availableSizeMB

				log.Printf("TempDB dosya sayısı: %d, CPU sayısı: %d, Optimal: %v",
					tempDBFileCount, cpuCount, isTempDBOptimal)
			}
		} else {
			log.Printf("TempDB dosya bilgileri için satır döndürülmedi")
			tempDBConfig["no_rows"] = true
		}

		if err := rows.Err(); err != nil {
			log.Printf("TempDB dosya bilgileri satır döngüsü hatası: %v", err)
			tempDBConfig["rows_error"] = err.Error()
		}
	}

	// Alternatif TempDB dosya bilgisi sorgusu - daha detaylı bilgi
	rows, err = db.Query(`
		SELECT 
			name,
			physical_name,
			type_desc,
			size * 8 / 1024 AS size_mb,
			growth,
			is_percent_growth,
			max_size
		FROM sys.master_files
		WHERE database_id = DB_ID('tempdb')
	`)

	if err == nil {
		defer rows.Close()
		var tempDbFiles []map[string]interface{}

		for rows.Next() {
			var name, physicalName, typeDesc string
			var sizeMB, growth, maxSize int
			var isPercentGrowth bool

			if err := rows.Scan(&name, &physicalName, &typeDesc, &sizeMB, &growth, &isPercentGrowth, &maxSize); err == nil {
				tempDbFiles = append(tempDbFiles, map[string]interface{}{
					"name":              name,
					"physical_name":     physicalName,
					"type":              typeDesc,
					"size_mb":           sizeMB,
					"growth":            growth,
					"is_percent_growth": isPercentGrowth,
					"max_size":          maxSize,
				})
			}
		}

		if len(tempDbFiles) > 0 {
			tempDBConfig["files"] = tempDbFiles
			log.Printf("TempDB detaylı dosya bilgileri alındı: %d dosya", len(tempDbFiles))
		}
	} else {
		log.Printf("TempDB detaylı dosya bilgileri alınamadı: %v", err)
		tempDBConfig["files_query_error"] = err.Error()
	}

	// TempDB fragmantasyonu kontrolü
	rows, err = db.Query(`
		SELECT 
			name,
			CAST(total_page_count * 8.0 / 1024 AS DECIMAL(18,2)) as size_mb,
			CAST(CAST(free_space_in_bytes AS DECIMAL(18,2)) / 1048576 AS DECIMAL(18,2)) as free_space_mb,
			CAST(CAST(free_space_in_bytes AS DECIMAL(18,2)) / (total_page_count * 8192.0) * 100 AS DECIMAL(18,2)) as free_space_percent
		FROM tempdb.sys.dm_db_file_space_usage
		JOIN tempdb.sys.database_files ON file_id = dm_db_file_space_usage.file_id
	`)

	if err == nil {
		defer rows.Close()
		var fileDetails []map[string]interface{}

		for rows.Next() {
			var name string
			var sizeMB, freeSpaceMB, freeSpacePercent float64

			if err := rows.Scan(&name, &sizeMB, &freeSpaceMB, &freeSpacePercent); err == nil {
				fileDetails = append(fileDetails, map[string]interface{}{
					"name":               name,
					"size_mb":            sizeMB,
					"free_space_mb":      freeSpaceMB,
					"free_space_percent": freeSpacePercent,
				})
			}
		}

		if len(fileDetails) > 0 {
			tempDBConfig["file_details"] = fileDetails
			log.Printf("TempDB dosya kullanım detayları alındı: %d dosya", len(fileDetails))
		}
	} else {
		// Bu sorgu tempdb veritabanına özel erişim gerektirebilir
		// ve bazı durumlarda başarısız olabilir, bu normal
		log.Printf("TempDB dosya kullanım detayları alınamadı: %v", err)
		tempDBConfig["usage_query_error"] = err.Error()

		// Bu durumda basitleştirilmiş bir sorgu deneyelim
		alternativeRows, altErr := db.Query(`
			SELECT 
				DB_NAME(database_id) as db_name,
				name,
				physical_name,
				size * 8 / 1024 as size_mb
			FROM sys.master_files
			WHERE database_id = DB_ID('tempdb')
		`)

		if altErr == nil {
			defer alternativeRows.Close()
			var altFileDetails []map[string]interface{}

			for alternativeRows.Next() {
				var dbName, name, physicalName string
				var sizeMB int

				if scanErr := alternativeRows.Scan(&dbName, &name, &physicalName, &sizeMB); scanErr == nil {
					altFileDetails = append(altFileDetails, map[string]interface{}{
						"db_name":       dbName,
						"name":          name,
						"physical_name": physicalName,
						"size_mb":       sizeMB,
					})
				}
			}

			if len(altFileDetails) > 0 {
				tempDBConfig["basic_file_details"] = altFileDetails
				log.Printf("TempDB temel dosya bilgileri alındı: %d dosya", len(altFileDetails))
			}
		} else {
			log.Printf("TempDB alternatif dosya bilgileri de alınamadı: %v", altErr)
		}
	}

	// Sonuçları kaydet
	b.results["TempDBConfiguration"] = tempDBConfig

	// Verilerin kaydedildiğini onaylamak için log
	log.Printf("TempDB yapılandırması başarıyla toplandı ve sonuçlara eklendi. Alanlar: %d", len(tempDBConfig))
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
