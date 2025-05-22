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

	// TempDB yapılandırması kontrolü
	collectWithRetry(func() {
		b.collectTempDBConfiguration()
	}, "TempDBConfiguration", 2)

	// Yeni eklenen performans analiz kategorileri

	// Index Fragmentation Analizi
	collectWithRetry(func() {
		b.collectIndexFragmentation()
	}, "IndexFragmentation", 2)

	// Wait Stats Analizi
	collectWithRetry(func() {
		b.collectWaitStats()
	}, "WaitStats", 2)

	// Query Store İncelemesi (SQL Server 2016+)
	collectWithRetry(func() {
		b.collectQueryStoreAnalysis()
	}, "QueryStoreAnalysis", 2)

	// Blocking ve Deadlock Analizi
	collectWithRetry(func() {
		b.collectBlockingAnalysis()
	}, "BlockingAnalysis", 2)

	// Memory Pressure Analizi
	collectWithRetry(func() {
		b.collectMemoryPressureAnalysis()
	}, "MemoryPressureAnalysis", 2)

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
			var agName, replicaServer sql.NullString
			var roleDesc, operationalState, connectedState sql.NullString
			var agSyncHealth sql.NullString
			var dbName, syncState, dbSyncHealth sql.NullString
			var logSendQueueSize, logSendRate, redoQueueSize, redoRate, secondsBehind float64

			if err := rows.Scan(
				&agName, &replicaServer, &roleDesc, &operationalState, &connectedState, &agSyncHealth,
				&dbName, &syncState, &dbSyncHealth, &logSendQueueSize, &logSendRate,
				&redoQueueSize, &redoRate, &secondsBehind); err == nil {

				// NULL değerleri güvenli şekilde işle
				agNameStr := ""
				if agName.Valid {
					agNameStr = agName.String
				}

				replicaServerStr := ""
				if replicaServer.Valid {
					replicaServerStr = replicaServer.String
				}

				roleDescStr := ""
				if roleDesc.Valid {
					roleDescStr = roleDesc.String
				}

				operationalStateStr := ""
				if operationalState.Valid {
					operationalStateStr = operationalState.String
				}

				connectedStateStr := ""
				if connectedState.Valid {
					connectedStateStr = connectedState.String
				}

				agSyncHealthStr := ""
				if agSyncHealth.Valid {
					agSyncHealthStr = agSyncHealth.String
				}

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
					"ag_name":                agNameStr,
					"replica_server":         replicaServerStr,
					"role":                   roleDescStr,
					"operational_state":      operationalStateStr,
					"connected_state":        connectedStateStr,
					"ag_sync_health":         agSyncHealthStr,
					"database_name":          dbNameStr,
					"sync_state":             syncStateStr,
					"db_sync_health":         dbSyncHealthStr,
					"log_send_queue_size_kb": logSendQueueSize,
					"log_send_rate_kb_sec":   logSendRate,
					"redo_queue_size_kb":     redoQueueSize,
					"redo_rate_kb_sec":       redoRate,
					"seconds_behind":         secondsBehind,
				})
			} else {
				log.Printf("[ERROR] MSSQL AlwaysOn satırı okunamadı: %v", err)
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
			CAST(unallocated_extent_page_count * 8.0 / 1024 AS DECIMAL(18,2)) as free_space_mb,
			CAST((unallocated_extent_page_count * 100.0) / 
				 CASE WHEN total_page_count = 0 THEN 1 ELSE total_page_count END
				AS DECIMAL(18,2)) as free_space_percent
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

		// Farklı bir SQL Server sürümü için alternatif sorgu deneyelim
		alternativeRows, altErr := db.Query(`
			SELECT 
				name,
				CAST(size * 8.0 / 1024 AS DECIMAL(18,2)) as size_mb,
				CAST(FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024 AS DECIMAL(18,2)) as space_used_mb
			FROM tempdb.sys.database_files
		`)

		if altErr == nil {
			defer alternativeRows.Close()
			var altFileDetails []map[string]interface{}

			for alternativeRows.Next() {
				var name string
				var sizeMB, spaceUsedMB sql.NullFloat64

				if scanErr := alternativeRows.Scan(&name, &sizeMB, &spaceUsedMB); scanErr == nil {
					size := 0.0
					if sizeMB.Valid {
						size = sizeMB.Float64
					}

					used := 0.0
					if spaceUsedMB.Valid {
						used = spaceUsedMB.Float64
					}

					// Boş alan ve yüzde hesapla
					freeMB := size - used
					freePercent := 0.0
					if size > 0 {
						freePercent = (freeMB / size) * 100
					}

					altFileDetails = append(altFileDetails, map[string]interface{}{
						"name":               name,
						"size_mb":            size,
						"used_mb":            used,
						"free_space_mb":      freeMB,
						"free_space_percent": freePercent,
					})
				}
			}

			if len(altFileDetails) > 0 {
				tempDBConfig["file_details"] = altFileDetails
				log.Printf("TempDB alternatif dosya kullanım detayları alındı: %d dosya", len(altFileDetails))
			}
		} else {
			log.Printf("TempDB alternatif dosya kullanım sorgusu da başarısız: %v", altErr)

			// Basit versiyon - sadece dosya bilgilerini al
			basicRows, basicErr := db.Query(`
				SELECT 
					DB_NAME(database_id) as db_name,
					name,
					physical_name,
					size * 8 / 1024 as size_mb
				FROM sys.master_files
				WHERE database_id = DB_ID('tempdb')
			`)

			if basicErr == nil {
				defer basicRows.Close()
				var basicFileDetails []map[string]interface{}

				for basicRows.Next() {
					var dbName, name, physicalName string
					var sizeMB int

					if scanErr := basicRows.Scan(&dbName, &name, &physicalName, &sizeMB); scanErr == nil {
						basicFileDetails = append(basicFileDetails, map[string]interface{}{
							"db_name":       dbName,
							"name":          name,
							"physical_name": physicalName,
							"size_mb":       sizeMB,
						})
					}
				}

				if len(basicFileDetails) > 0 {
					tempDBConfig["basic_file_details"] = basicFileDetails
					log.Printf("TempDB temel dosya bilgileri alındı: %d dosya", len(basicFileDetails))
				}
			} else {
				log.Printf("TempDB alternatif dosya bilgileri de alınamadı: %v", basicErr)
			}
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

// collectIndexFragmentation index fragmentasyonunu ve kullanılmayan indeksleri analiz eder
func (b *BestPracticesCollector) collectIndexFragmentation() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Index fragmentasyon bilgileri toplanamadı: %v", err)
		b.results["IndexFragmentation"] = map[string]interface{}{
			"error":  err.Error(),
			"status": "error",
		}
		return
	}
	defer db.Close()

	// Index fragmentasyon sonuçları
	indexResults := make(map[string]interface{})

	// Fragmentasyona sahip indeksleri al
	log.Printf("Fragmentasyona sahip indeksler analiz ediliyor...")
	rows, err := db.Query(`
		SELECT 
			DB_NAME(ips.database_id) AS database_name,
			OBJECT_NAME(ips.object_id, ips.database_id) AS table_name,
			i.name AS index_name,
			ips.index_type_desc,
			ips.avg_fragmentation_in_percent,
			ips.page_count,
			ips.avg_page_space_used_in_percent,
			CASE 
				WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
				WHEN ips.avg_fragmentation_in_percent BETWEEN 10 AND 30 THEN 'REORGANIZE'
				ELSE 'NONE'
			END AS recommended_action
		FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'LIMITED') ips
		JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
		WHERE ips.avg_fragmentation_in_percent > 10
		AND ips.page_count > 100
		ORDER BY ips.avg_fragmentation_in_percent DESC
	`)

	if err != nil {
		log.Printf("Index fragmentasyon sorgusu başarısız: %v", err)
		indexResults["fragmentation_error"] = err.Error()
	} else {
		defer rows.Close()
		var fragmentedIndexes []map[string]interface{}

		for rows.Next() {
			var dbName, tableName, indexName, indexType, recommendedAction string
			var fragPercent, pageSpaceUsed float64
			var pageCount int64

			if err := rows.Scan(&dbName, &tableName, &indexName, &indexType,
				&fragPercent, &pageCount, &pageSpaceUsed, &recommendedAction); err == nil {

				fragmentedIndexes = append(fragmentedIndexes, map[string]interface{}{
					"database_name":           dbName,
					"table_name":              tableName,
					"index_name":              indexName,
					"index_type":              indexType,
					"fragmentation_percent":   fragPercent,
					"page_count":              pageCount,
					"page_space_used_percent": pageSpaceUsed,
					"recommended_action":      recommendedAction,
				})
			}
		}

		indexResults["fragmented_indexes"] = fragmentedIndexes
		indexResults["fragmented_count"] = len(fragmentedIndexes)

		// Kritik seviyede fragmentasyon tespiti
		var criticalCount int
		for _, idx := range fragmentedIndexes {
			if fragPercent, ok := idx["fragmentation_percent"].(float64); ok && fragPercent > 50 {
				criticalCount++
			}
		}
		indexResults["critical_fragmentation_count"] = criticalCount

		log.Printf("Fragmentasyon analizi tamamlandı: %d index fragmentasyona sahip, %d kritik seviyede",
			len(fragmentedIndexes), criticalCount)
	}

	// Kullanılmayan indeksleri al
	log.Printf("Kullanılmayan indeksler analiz ediliyor...")
	rows, err = db.Query(`
		SELECT 
			DB_NAME(s.database_id) AS database_name,
			OBJECT_NAME(s.object_id, s.database_id) AS table_name,
			i.name AS index_name,
			s.user_seeks + s.user_scans + s.user_lookups AS total_usage,
			s.user_updates AS write_usage,
			i.type_desc AS index_type,
			CASE WHEN p.rows IS NULL THEN 0 ELSE p.rows END AS table_rows,
			CAST(ROUND(((CAST(SUM(a.total_pages) AS float) * 8) / 1024), 2) AS DECIMAL(18,2)) AS index_size_mb
		FROM sys.dm_db_index_usage_stats s
		INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
		INNER JOIN sys.objects o ON i.object_id = o.object_id
		INNER JOIN sys.schemas sc ON o.schema_id = sc.schema_id
		LEFT JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
		WHERE i.type_desc IN ('NONCLUSTERED', 'CLUSTERED')
		AND i.is_primary_key = 0 
		AND i.is_unique_constraint = 0
		AND o.type = 'U' -- User tables only
		AND (s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0)
		AND s.user_updates > 0 -- Index is being maintained but not used
		GROUP BY DB_NAME(s.database_id), OBJECT_NAME(s.object_id, s.database_id), 
			i.name, s.user_seeks + s.user_scans + s.user_lookups, s.user_updates, 
			i.type_desc, p.rows
		ORDER BY s.user_updates DESC, index_size_mb DESC
	`)

	if err != nil {
		log.Printf("Kullanılmayan index sorgusu başarısız: %v", err)
		indexResults["unused_indexes_error"] = err.Error()
	} else {
		defer rows.Close()
		var unusedIndexes []map[string]interface{}
		var totalWastedSpaceMB float64 = 0

		for rows.Next() {
			var dbName, tableName, indexName, indexType string
			var totalUsage, writeUsage, tableRows int64
			var indexSizeMB float64

			if err := rows.Scan(&dbName, &tableName, &indexName, &totalUsage, &writeUsage,
				&indexType, &tableRows, &indexSizeMB); err == nil {

				unusedIndexes = append(unusedIndexes, map[string]interface{}{
					"database_name":    dbName,
					"table_name":       tableName,
					"index_name":       indexName,
					"total_read_usage": totalUsage,
					"write_usage":      writeUsage,
					"index_type":       indexType,
					"table_rows":       tableRows,
					"index_size_mb":    indexSizeMB,
				})

				totalWastedSpaceMB += indexSizeMB
			}
		}

		indexResults["unused_indexes"] = unusedIndexes
		indexResults["unused_count"] = len(unusedIndexes)
		indexResults["wasted_space_mb"] = totalWastedSpaceMB

		log.Printf("Kullanılmayan index analizi tamamlandı: %d index kullanılmıyor, toplam %.2f MB boşa kullanılan alan",
			len(unusedIndexes), totalWastedSpaceMB)
	}

	// Missing indexes (eksik indeksler) bilgisini al
	log.Printf("Eksik indeksler analiz ediliyor...")
	rows, err = db.Query(`
		SELECT TOP 25
			DB_NAME(mid.database_id) AS database_name,
			OBJECT_NAME(mid.object_id, mid.database_id) AS table_name,
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

	if err != nil {
		log.Printf("Eksik index sorgusu başarısız: %v", err)
		indexResults["missing_indexes_error"] = err.Error()
	} else {
		defer rows.Close()
		var missingIndexes []map[string]interface{}

		for rows.Next() {
			var dbName, tableName, createIndexStmt string
			var improvementMeasure, avgTotalUserCost, avgUserImpact float64
			var userSeeks, userScans int64

			if err := rows.Scan(&dbName, &tableName, &improvementMeasure, &createIndexStmt,
				&userSeeks, &userScans, &avgTotalUserCost, &avgUserImpact); err == nil {

				missingIndexes = append(missingIndexes, map[string]interface{}{
					"database_name":          dbName,
					"table_name":             tableName,
					"improvement_measure":    improvementMeasure,
					"create_index_statement": createIndexStmt,
					"user_seeks":             userSeeks,
					"user_scans":             userScans,
					"avg_total_user_cost":    avgTotalUserCost,
					"avg_user_impact":        avgUserImpact,
				})
			}
		}

		indexResults["missing_indexes"] = missingIndexes
		indexResults["missing_count"] = len(missingIndexes)

		log.Printf("Eksik index analizi tamamlandı: %d potansiyel eksik index bulundu", len(missingIndexes))
	}

	// Sonuçları ekle
	b.results["IndexFragmentation"] = indexResults
}

// collectWaitStats sistem genelindeki beklemeleri analiz eder
func (b *BestPracticesCollector) collectWaitStats() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Wait statistics bilgileri toplanamadı: %v", err)
		b.results["WaitStats"] = map[string]interface{}{
			"error":  err.Error(),
			"status": "error",
		}
		return
	}
	defer db.Close()

	// Wait statistics sonuçları
	waitResults := make(map[string]interface{})

	// Top wait statistics
	log.Printf("Wait statistics analiz ediliyor...")
	rows, err := db.Query(`
		SELECT TOP 20
			wait_type,
			wait_time_ms,
			wait_time_ms / 1000.0 / 60 AS wait_time_minutes,
			100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS percentage,
			waiting_tasks_count,
			signal_wait_time_ms,
			signal_wait_time_ms * 1.0 / wait_time_ms AS signal_wait_percent,
			CASE 
				WHEN wait_type LIKE 'LCK%' THEN 'Lock'
				WHEN wait_type LIKE 'PAGEIOLATCH%' THEN 'Buffer I/O'
				WHEN wait_type LIKE 'PAGELATCH%' THEN 'Buffer Latch'
				WHEN wait_type LIKE 'LATCH%' THEN 'Latch'
				WHEN wait_type LIKE 'IO%' THEN 'I/O'
				WHEN wait_type LIKE 'RESOURCE%' THEN 'Resource'
				WHEN wait_type LIKE 'SOS%' THEN 'SOS'
				WHEN wait_type LIKE 'MEMORY%' THEN 'Memory'
				WHEN wait_type LIKE 'CLR%' THEN 'CLR'
				WHEN wait_type LIKE 'THREADPOOL' THEN 'CPU - Worker Thread'
				WHEN wait_type LIKE 'CXPACKET' THEN 'CPU - Parallelism'
				WHEN wait_type LIKE 'WRITELOG' THEN 'Transaction Log'
				WHEN wait_type LIKE 'ASYNC_NETWORK_IO' THEN 'Network I/O'
				ELSE 'Other'
			END AS wait_category
		FROM sys.dm_os_wait_stats
		WHERE wait_type NOT IN (
			-- These wait types are always filtered out
			'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP',
			'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE',
			'CHKPT', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'CLR_SEMAPHORE',
			'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE',
			'DBMIRRORING_CMD', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE',
			'EXECSYNC', 'FSAGENT', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX',
			'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'HADR_LOGCAPTURE_WAIT',
			'HADR_NOTIFICATION_DEQUEUE', 'HADR_TIMER_TASK', 'HADR_WORK_QUEUE',
			'KSOURCE_WAKEUP', 'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE',
			'MEMORY_ALLOCATION_EXT', 'ONDEMAND_TASK_QUEUE',
			'PARALLEL_REDO_DRAIN_WORKER', 'PARALLEL_REDO_LOG_CACHE', 'PARALLEL_REDO_TRAN_LIST',
			'PARALLEL_REDO_WORKER_SYNC', 'PARALLEL_REDO_WORKER_WAIT_WORK',
			'PREEMPTIVE_XE_GETTARGETSTATE', 'PWAIT_ALL_COMPONENTS_INITIALIZED',
			'PWAIT_DIRECTLOGCONSUMER_GETNEXT', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
			'QDS_ASYNC_QUEUE', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
			'QDS_SHUTDOWN_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'RESOURCE_QUEUE',
			'SERVER_IDLE_CHECK', 'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP',
			'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY',
			'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK',
			'SLEEP_TASK', 'SLEEP_TEMPDBSTARTUP', 'SNI_HTTP_ACCEPT',
			'SOS_WORK_DISPATCHER', 'SP_SERVER_DIAGNOSTICS_SLEEP', 'SQLTRACE_BUFFER_FLUSH',
			'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES',
			'WAIT_FOR_RESULTS', 'WAITFOR', 'WAITFOR_TASKSHUTDOWN',
			'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_CKPT_CLOSE',
			'XE_DISPATCHER_JOIN', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'
		)
		AND wait_time_ms > 0 
		ORDER BY wait_time_ms DESC
	`)

	if err != nil {
		log.Printf("Wait statistics sorgusu başarısız: %v", err)
		waitResults["wait_stats_error"] = err.Error()
	} else {
		defer rows.Close()
		var waitStats []map[string]interface{}
		var waitCategories = make(map[string]float64)

		for rows.Next() {
			var waitType, waitCategory string
			var waitTimeMs, waitTimeMinutes, percentage, signalWaitPercent float64
			var waitingTasksCount, signalWaitTimeMs int64

			if err := rows.Scan(&waitType, &waitTimeMs, &waitTimeMinutes, &percentage,
				&waitingTasksCount, &signalWaitTimeMs, &signalWaitPercent, &waitCategory); err == nil {

				waitStats = append(waitStats, map[string]interface{}{
					"wait_type":           waitType,
					"wait_time_ms":        waitTimeMs,
					"wait_time_minutes":   waitTimeMinutes,
					"percentage":          percentage,
					"waiting_tasks_count": waitingTasksCount,
					"signal_wait_time_ms": signalWaitTimeMs,
					"signal_wait_percent": signalWaitPercent,
					"wait_category":       waitCategory,
				})

				// Kategori toplamlarını hesapla
				waitCategories[waitCategory] += percentage
			}
		}

		waitResults["wait_stats"] = waitStats
		waitResults["wait_stats_count"] = len(waitStats)
		waitResults["wait_categories"] = waitCategories

		// En yüksek bekleme kategorilerini tespit et
		if len(waitStats) > 0 {
			topWaitType := waitStats[0]["wait_type"].(string)
			topWaitCategory := waitStats[0]["wait_category"].(string)
			topWaitPercentage := waitStats[0]["percentage"].(float64)

			waitResults["top_wait_type"] = topWaitType
			waitResults["top_wait_category"] = topWaitCategory
			waitResults["top_wait_percentage"] = topWaitPercentage

			// I/O sorunları analizi
			var ioWaitPercentage float64 = 0
			if val, ok := waitCategories["Buffer I/O"]; ok {
				ioWaitPercentage += val
			}
			if val, ok := waitCategories["I/O"]; ok {
				ioWaitPercentage += val
			}

			waitResults["io_wait_percentage"] = ioWaitPercentage
			waitResults["has_io_problems"] = ioWaitPercentage > 20 // %20'den fazla I/O beklemesi varsa sorun var

			// CPU sorunları analizi
			var cpuWaitPercentage float64 = 0
			if val, ok := waitCategories["CPU - Worker Thread"]; ok {
				cpuWaitPercentage += val
			}
			if val, ok := waitCategories["CPU - Parallelism"]; ok {
				cpuWaitPercentage += val
			}

			waitResults["cpu_wait_percentage"] = cpuWaitPercentage
			waitResults["has_cpu_problems"] = cpuWaitPercentage > 15 // %15'den fazla CPU beklemesi varsa sorun var

			// Memory sorunları analizi
			var memoryWaitPercentage float64 = 0
			if val, ok := waitCategories["Memory"]; ok {
				memoryWaitPercentage += val
			}

			waitResults["memory_wait_percentage"] = memoryWaitPercentage
			waitResults["has_memory_problems"] = memoryWaitPercentage > 10 // %10'dan fazla Memory beklemesi varsa sorun var

			// Lock sorunları analizi
			var lockWaitPercentage float64 = 0
			if val, ok := waitCategories["Lock"]; ok {
				lockWaitPercentage += val
			}

			waitResults["lock_wait_percentage"] = lockWaitPercentage
			waitResults["has_lock_problems"] = lockWaitPercentage > 5 // %5'den fazla Lock beklemesi varsa sorun var
		}

		log.Printf("Wait statistics analizi tamamlandı: %d bekleme tipi analiz edildi", len(waitStats))
	}

	// Kritik CXPACKET bekleme analizi (paralellism sorunları)
	rows, err = db.Query(`
		SELECT 
			wait_time_ms,
			waiting_tasks_count,
			wait_time_ms * 1.0 / waiting_tasks_count AS avg_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type = 'CXPACKET'
	`)

	if err != nil {
		log.Printf("CXPACKET analizi başarısız: %v", err)
	} else {
		defer rows.Close()
		if rows.Next() {
			var waitTimeMs, avgWaitTimeMs float64
			var waitingTasksCount int64

			if err := rows.Scan(&waitTimeMs, &waitingTasksCount, &avgWaitTimeMs); err == nil {
				waitResults["cxpacket_wait_time_ms"] = waitTimeMs
				waitResults["cxpacket_waiting_tasks"] = waitingTasksCount
				waitResults["cxpacket_avg_wait_time_ms"] = avgWaitTimeMs

				// Konfigürasyon kontrolü
				var maxDop int
				err = db.QueryRow("SELECT value_in_use FROM sys.configurations WHERE name = 'max degree of parallelism'").Scan(&maxDop)
				if err == nil {
					waitResults["max_dop"] = maxDop

					// CXPACKET beklemeleri yüksekse ve MAXDOP 0 ise (sınırsız) optimizasyon öneri
					if avgWaitTimeMs > 10 && maxDop == 0 {
						waitResults["cxpacket_recommendation"] = "MAXDOP değerini sınırlandırın (CPU sayısının yarısı veya 8 değerinden küçük olanı önerilir)"
					}
				}
			}
		}
	}

	// PAGEIOLATCH beklemeleri (I/O sorunları)
	rows, err = db.Query(`
		SELECT 
			wait_time_ms,
			waiting_tasks_count,
			wait_time_ms * 1.0 / waiting_tasks_count AS avg_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_type LIKE 'PAGEIOLATCH%'
	`)

	if err != nil {
		log.Printf("PAGEIOLATCH analizi başarısız: %v", err)
	} else {
		defer rows.Close()
		var totalPageIOWaitMs float64 = 0
		var totalPageIOTasks int64 = 0

		for rows.Next() {
			var waitTimeMs, avgWaitTimeMs float64
			var waitingTasksCount int64

			if err := rows.Scan(&waitTimeMs, &waitingTasksCount, &avgWaitTimeMs); err == nil {
				totalPageIOWaitMs += waitTimeMs
				totalPageIOTasks += waitingTasksCount
			}
		}

		if totalPageIOTasks > 0 {
			avgPageIOWaitMs := totalPageIOWaitMs / float64(totalPageIOTasks)
			waitResults["pageiolatch_wait_time_ms"] = totalPageIOWaitMs
			waitResults["pageiolatch_waiting_tasks"] = totalPageIOTasks
			waitResults["pageiolatch_avg_wait_time_ms"] = avgPageIOWaitMs

			// I/O sorunları tespiti
			if avgPageIOWaitMs > 20 {
				waitResults["has_io_subsystem_problems"] = true
				waitResults["io_recommendation"] = "Disk I/O performansı iyileştirilmeli, SSD/NVMe disklere geçiş düşünülmeli veya buffer pool artırılmalı"
			}
		}
	}

	// Sonuçları ekle
	b.results["WaitStats"] = waitResults
}

// collectQueryStoreAnalysis Query Store'dan en kötü performans gösteren sorguları analiz eder
func (b *BestPracticesCollector) collectQueryStoreAnalysis() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Query Store bilgileri toplanamadı: %v", err)
		b.results["QueryStoreAnalysis"] = map[string]interface{}{
			"error":  err.Error(),
			"status": "error",
		}
		return
	}
	defer db.Close()

	// Query Store sonuçları
	queryStoreResults := make(map[string]interface{})

	// Query Store etkin olan veritabanlarını kontrol et
	log.Printf("Query Store etkin veritabanları kontrol ediliyor...")
	rows, err := db.Query(`
		SELECT 
			DB_NAME(database_id) AS database_name,
			actual_state_desc,
			desired_state_desc,
			current_storage_size_mb,
			max_storage_size_mb,
			CASE 
				WHEN actual_state_desc = 'OFF' THEN 'NOT_AVAILABLE'
				WHEN actual_state_desc = 'READ_ONLY' THEN 'READ_ONLY'
				ELSE 'AVAILABLE'
			END AS availability_status
		FROM sys.database_query_store_options
		WHERE actual_state_desc IN ('ON', 'READ_ONLY')
	`)

	if err != nil {
		log.Printf("Query Store kontrol sorgusu başarısız: %v", err)
		queryStoreResults["query_store_error"] = err.Error()
		queryStoreResults["is_query_store_available"] = false
	} else {
		defer rows.Close()
		var databases []map[string]interface{}
		var availableDatabases []string
		var totalDatabases int = 0
		var availableCount int = 0

		for rows.Next() {
			var dbName, actualState, desiredState, availabilityStatus string
			var currentStorageSizeMB, maxStorageSizeMB float64

			if err := rows.Scan(&dbName, &actualState, &desiredState, &currentStorageSizeMB, &maxStorageSizeMB, &availabilityStatus); err == nil {
				totalDatabases++

				dbInfo := map[string]interface{}{
					"database_name":           dbName,
					"actual_state":            actualState,
					"desired_state":           desiredState,
					"current_storage_size_mb": currentStorageSizeMB,
					"max_storage_size_mb":     maxStorageSizeMB,
					"availability_status":     availabilityStatus,
				}

				databases = append(databases, dbInfo)

				if availabilityStatus == "AVAILABLE" {
					availableCount++
					availableDatabases = append(availableDatabases, dbName)
				}
			}
		}

		queryStoreResults["query_store_databases"] = databases
		queryStoreResults["query_store_databases_count"] = totalDatabases
		queryStoreResults["available_databases"] = availableDatabases
		queryStoreResults["available_databases_count"] = availableCount
		queryStoreResults["is_query_store_available"] = availableCount > 0

		log.Printf("Query Store kontrol tamamlandı: %d veritabanı, %d kullanılabilir",
			totalDatabases, availableCount)
	}

	// Query Store etkin değilse, analizi sonlandır
	if !queryStoreResults["is_query_store_available"].(bool) {
		log.Printf("Etkin Query Store bulunamadı, analiz yapılamıyor")
		queryStoreResults["recommendation"] = "Query Store özelliğini veritabanlarında etkinleştirerek detaylı sorgu performans analizi yapabilirsiniz"
		b.results["QueryStoreAnalysis"] = queryStoreResults
		return
	}

	// En yüksek CPU kullanan sorgular
	log.Printf("En yüksek CPU kullanan sorgular analiz ediliyor...")
	availableDatabases := queryStoreResults["available_databases"].([]string)
	var topQueries []map[string]interface{}

	for _, dbName := range availableDatabases {
		// Önce veritabanını kullanmaya başla
		useDbCmd := fmt.Sprintf("USE [%s]", dbName)
		_, err := db.Exec(useDbCmd)
		if err != nil {
			log.Printf("Veritabanı değiştirilemedi (%s): %v", dbName, err)
			continue
		}

		// Bu veritabanındaki en yüksek CPU kullanan sorguları al
		queryRows, err := db.Query(`
			SELECT TOP 10
				qt.query_sql_text,
				q.query_id,
				qt.query_text_id,
				rs.count_executions,
				rs.avg_duration / 1000.0 AS avg_duration_ms,
				rs.avg_cpu_time / 1000.0 AS avg_cpu_time_ms,
				rs.avg_logical_io_reads,
				rs.avg_logical_io_writes,
				rs.avg_physical_io_reads,
				rs.avg_clr_time / 1000.0 AS avg_clr_time_ms,
				rs.avg_dop,
				rs.avg_query_max_used_memory,
				rs.avg_rowcount,
				p.plan_id,
				q.object_id,
				OBJECT_NAME(q.object_id) AS object_name,
				rs.last_execution_time
			FROM sys.query_store_query_text AS qt
			JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id
			JOIN sys.query_store_plan AS p ON q.query_id = p.query_id
			JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id
			WHERE rs.last_execution_time > DATEADD(hour, -24, GETUTCDATE())
			ORDER BY rs.avg_cpu_time DESC
		`)

		if err != nil {
			log.Printf("CPU sorguları alınamadı (%s): %v", dbName, err)
			continue
		}

		for queryRows.Next() {
			var querySqlText, objectName string
			var queryId, queryTextId, planId int
			var objectId *int
			var countExecutions, avgDuration, avgCpuTime, avgLogicalReads, avgLogicalWrites, avgPhysicalReads int64
			var avgClrTime, avgDop, avgMemory, avgRowCount float64
			var lastExecutionTime time.Time

			err := queryRows.Scan(
				&querySqlText, &queryId, &queryTextId, &countExecutions,
				&avgDuration, &avgCpuTime, &avgLogicalReads, &avgLogicalWrites,
				&avgPhysicalReads, &avgClrTime, &avgDop, &avgMemory, &avgRowCount,
				&planId, &objectId, &objectName, &lastExecutionTime,
			)

			if err != nil {
				log.Printf("Sorgu satırı okunamadı: %v", err)
				continue
			}

			// Sorgu metnini kısalt (çok uzun olabilir)
			shortText := querySqlText
			if len(shortText) > 200 {
				shortText = shortText[:200] + "..."
			}

			// Sorgu bilgilerini ekle
			topQueries = append(topQueries, map[string]interface{}{
				"database_name":       dbName,
				"query_id":            queryId,
				"query_text_id":       queryTextId,
				"plan_id":             planId,
				"query_text":          shortText,
				"object_name":         objectName,
				"count_executions":    countExecutions,
				"avg_duration_ms":     avgDuration,
				"avg_cpu_time_ms":     avgCpuTime,
				"avg_logical_reads":   avgLogicalReads,
				"avg_logical_writes":  avgLogicalWrites,
				"avg_physical_reads":  avgPhysicalReads,
				"avg_clr_time_ms":     avgClrTime,
				"avg_dop":             avgDop,
				"avg_memory_mb":       avgMemory / 1024.0, // KB to MB
				"avg_rowcount":        avgRowCount,
				"last_execution_time": lastExecutionTime.Format(time.RFC3339),
			})
		}

		queryRows.Close()
	}

	queryStoreResults["top_cpu_queries"] = topQueries
	queryStoreResults["top_cpu_queries_count"] = len(topQueries)

	// En yavaş çalışan sorgular
	log.Printf("En yavaş çalışan sorgular analiz ediliyor...")
	var slowQueries []map[string]interface{}

	for _, dbName := range availableDatabases {
		// Önce veritabanını kullanmaya başla
		useDbCmd := fmt.Sprintf("USE [%s]", dbName)
		_, err := db.Exec(useDbCmd)
		if err != nil {
			log.Printf("Veritabanı değiştirilemedi (%s): %v", dbName, err)
			continue
		}

		// Bu veritabanındaki en yavaş sorguları al
		queryRows, err := db.Query(`
			SELECT TOP 10
				qt.query_sql_text,
				q.query_id,
				qt.query_text_id,
				rs.count_executions,
				rs.avg_duration / 1000.0 AS avg_duration_ms,
				rs.avg_cpu_time / 1000.0 AS avg_cpu_time_ms,
				rs.avg_logical_io_reads,
				rs.avg_logical_io_writes,
				rs.avg_physical_io_reads,
				rs.last_execution_time
			FROM sys.query_store_query_text AS qt
			JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id
			JOIN sys.query_store_plan AS p ON q.query_id = p.query_id
			JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id
			WHERE rs.last_execution_time > DATEADD(hour, -24, GETUTCDATE())
			AND rs.count_executions > 10  -- En az 10 kez çalışmış olmalı
			ORDER BY rs.avg_duration DESC
		`)

		if err != nil {
			log.Printf("Yavaş sorgular alınamadı (%s): %v", dbName, err)
			continue
		}

		for queryRows.Next() {
			var querySqlText string
			var queryId, queryTextId int
			var countExecutions, avgDuration, avgCpuTime, avgLogicalReads, avgLogicalWrites, avgPhysicalReads int64
			var lastExecutionTime time.Time

			err := queryRows.Scan(
				&querySqlText, &queryId, &queryTextId, &countExecutions,
				&avgDuration, &avgCpuTime, &avgLogicalReads, &avgLogicalWrites,
				&avgPhysicalReads, &lastExecutionTime,
			)

			if err != nil {
				log.Printf("Yavaş sorgu satırı okunamadı: %v", err)
				continue
			}

			// Sorgu metnini kısalt (çok uzun olabilir)
			shortText := querySqlText
			if len(shortText) > 200 {
				shortText = shortText[:200] + "..."
			}

			// Sorgu bilgilerini ekle
			slowQueries = append(slowQueries, map[string]interface{}{
				"database_name":       dbName,
				"query_id":            queryId,
				"query_text_id":       queryTextId,
				"query_text":          shortText,
				"count_executions":    countExecutions,
				"avg_duration_ms":     avgDuration,
				"avg_cpu_time_ms":     avgCpuTime,
				"avg_logical_reads":   avgLogicalReads,
				"avg_logical_writes":  avgLogicalWrites,
				"avg_physical_reads":  avgPhysicalReads,
				"last_execution_time": lastExecutionTime.Format(time.RFC3339),
			})
		}

		queryRows.Close()
	}

	queryStoreResults["slow_queries"] = slowQueries
	queryStoreResults["slow_queries_count"] = len(slowQueries)

	// En fazla kaynak kullanan planları analiz et
	var topIoQueries []map[string]interface{}

	for _, dbName := range availableDatabases {
		// Önce veritabanını kullanmaya başla
		useDbCmd := fmt.Sprintf("USE [%s]", dbName)
		_, err := db.Exec(useDbCmd)
		if err != nil {
			continue
		}

		// Bu veritabanındaki en fazla I/O kullanan sorguları al
		queryRows, err := db.Query(`
			SELECT TOP 10
				qt.query_sql_text,
				q.query_id,
				qt.query_text_id,
				rs.count_executions,
				rs.avg_duration / 1000.0 AS avg_duration_ms,
				rs.avg_logical_io_reads,
				rs.avg_physical_io_reads,
				rs.last_execution_time
			FROM sys.query_store_query_text AS qt
			JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id
			JOIN sys.query_store_plan AS p ON q.query_id = p.query_id
			JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id
			WHERE rs.last_execution_time > DATEADD(hour, -24, GETUTCDATE())
			ORDER BY rs.avg_physical_io_reads DESC
		`)

		if err != nil {
			continue
		}

		for queryRows.Next() {
			var querySqlText string
			var queryId, queryTextId int
			var countExecutions, avgDuration, avgLogicalReads, avgPhysicalReads int64
			var lastExecutionTime time.Time

			err := queryRows.Scan(
				&querySqlText, &queryId, &queryTextId, &countExecutions,
				&avgDuration, &avgLogicalReads, &avgPhysicalReads, &lastExecutionTime,
			)

			if err != nil {
				continue
			}

			// Sorgu metnini kısalt (çok uzun olabilir)
			shortText := querySqlText
			if len(shortText) > 200 {
				shortText = shortText[:200] + "..."
			}

			// Sorgu bilgilerini ekle
			topIoQueries = append(topIoQueries, map[string]interface{}{
				"database_name":       dbName,
				"query_id":            queryId,
				"query_text_id":       queryTextId,
				"query_text":          shortText,
				"count_executions":    countExecutions,
				"avg_duration_ms":     avgDuration,
				"avg_logical_reads":   avgLogicalReads,
				"avg_physical_reads":  avgPhysicalReads,
				"last_execution_time": lastExecutionTime.Format(time.RFC3339),
			})
		}

		queryRows.Close()
	}

	queryStoreResults["top_io_queries"] = topIoQueries
	queryStoreResults["top_io_queries_count"] = len(topIoQueries)

	// Sonuçları ekle
	b.results["QueryStoreAnalysis"] = queryStoreResults
}

// collectBlockingAnalysis engelleme (blocking) ve deadlock durumlarını analiz eder
func (b *BestPracticesCollector) collectBlockingAnalysis() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Blocking bilgileri toplanamadı: %v", err)
		b.results["BlockingAnalysis"] = map[string]interface{}{
			"error":  err.Error(),
			"status": "error",
		}
		return
	}
	defer db.Close()

	// Blocking ve deadlock sonuçları
	blockingResults := make(map[string]interface{})

	// Mevcut blocking durumları
	log.Printf("Mevcut blocking durumları analiz ediliyor...")
	rows, err := db.Query(`
		WITH BlockingHierarchy AS (
			SELECT 
				wait.session_id AS blocked_session_id,
				wait.wait_duration_ms,
				wait.wait_type,
				wait.resource_description,
				blocking_session_id,
				wait.resource_address,
				wait.blocking_session_id,
				blockingWait.wait_type AS blocking_wait_type,
				DB_NAME(s1.database_id) AS database_name,
				s1.program_name AS blocked_program,
				s2.program_name AS blocking_program,
				s1.login_name AS blocked_login,
				s2.login_name AS blocking_login,
				blockingSQLText.text AS blocking_sql,
				blockedSQLText.text AS blocked_sql,
				s1.cpu_time AS blocked_cpu_time,
				s1.total_elapsed_time AS blocked_elapsed_time,
				s1.reads AS blocked_reads,
				s1.writes AS blocked_writes,
				s1.logical_reads AS blocked_logical_reads,
				s2.cpu_time AS blocking_cpu_time,
				s2.total_elapsed_time AS blocking_elapsed_time,
				s2.reads AS blocking_reads,
				s2.writes AS blocking_writes,
				s2.logical_reads AS blocking_logical_reads,
				s1.open_transaction_count AS blocked_open_tran_count,
				s2.open_transaction_count AS blocking_open_tran_count
			FROM sys.dm_os_waiting_tasks wait
			LEFT JOIN sys.dm_os_waiting_tasks blockingWait 
				ON wait.blocking_session_id = blockingWait.session_id
			LEFT JOIN sys.dm_exec_sessions s1
				ON wait.session_id = s1.session_id
			LEFT JOIN sys.dm_exec_sessions s2
				ON wait.blocking_session_id = s2.session_id
			OUTER APPLY sys.dm_exec_sql_text(
				(SELECT TOP 1 sql_handle FROM sys.dm_exec_requests WHERE session_id = wait.blocking_session_id)
			) AS blockingSQLText
			OUTER APPLY sys.dm_exec_sql_text(
				(SELECT TOP 1 sql_handle FROM sys.dm_exec_requests WHERE session_id = wait.session_id)
			) AS blockedSQLText
			WHERE wait.blocking_session_id IS NOT NULL 
			AND wait.blocking_session_id <> 0
			AND wait.wait_type <> 'SLEEP_TASK' -- Ignore sleeping tasks
		)
		SELECT * FROM BlockingHierarchy
		ORDER BY wait_duration_ms DESC
	`)

	if err != nil {
		log.Printf("Mevcut blocking sorgusu başarısız: %v", err)
		blockingResults["current_blocking_error"] = err.Error()
	} else {
		defer rows.Close()
		var blockingChains []map[string]interface{}

		for rows.Next() {
			var blockedSessionId, blockingSessionId int
			var waitDurationMs int64
			var waitType, resourceDescription, resourceAddress, blockingWaitType, databaseName string
			var blockedProgram, blockingProgram, blockedLogin, blockingLogin, blockingSql, blockedSql *string
			var blockedCpuTime, blockedElapsedTime, blockedReads, blockedWrites, blockedLogicalReads int64
			var blockingCpuTime, blockingElapsedTime, blockingReads, blockingWrites, blockingLogicalReads int64
			var blockedOpenTranCount, blockingOpenTranCount int

			err := rows.Scan(
				&blockedSessionId, &waitDurationMs, &waitType, &resourceDescription,
				&blockingSessionId, &resourceAddress, &blockingSessionId, &blockingWaitType,
				&databaseName, &blockedProgram, &blockingProgram, &blockedLogin, &blockingLogin,
				&blockingSql, &blockedSql, &blockedCpuTime, &blockedElapsedTime,
				&blockedReads, &blockedWrites, &blockedLogicalReads, &blockingCpuTime,
				&blockingElapsedTime, &blockingReads, &blockingWrites, &blockingLogicalReads,
				&blockedOpenTranCount, &blockingOpenTranCount,
			)

			if err != nil {
				log.Printf("Blocking satırı okunamadı: %v", err)
				continue
			}

			// Sorgu metinlerini kısalt (çok uzun olabilir)
			var blockingSqlShort, blockedSqlShort string

			if blockingSql != nil {
				blockingSqlShort = *blockingSql
				if len(blockingSqlShort) > 200 {
					blockingSqlShort = blockingSqlShort[:200] + "..."
				}
			} else {
				blockingSqlShort = "N/A"
			}

			if blockedSql != nil {
				blockedSqlShort = *blockedSql
				if len(blockedSqlShort) > 200 {
					blockedSqlShort = blockedSqlShort[:200] + "..."
				}
			} else {
				blockedSqlShort = "N/A"
			}

			// Program bilgisini kontrol et
			var blockedProgramStr, blockingProgramStr string

			if blockedProgram != nil {
				blockedProgramStr = *blockedProgram
			} else {
				blockedProgramStr = "N/A"
			}

			if blockingProgram != nil {
				blockingProgramStr = *blockingProgram
			} else {
				blockingProgramStr = "N/A"
			}

			// Login bilgisini kontrol et
			var blockedLoginStr, blockingLoginStr string

			if blockedLogin != nil {
				blockedLoginStr = *blockedLogin
			} else {
				blockedLoginStr = "N/A"
			}

			if blockingLogin != nil {
				blockingLoginStr = *blockingLogin
			} else {
				blockingLoginStr = "N/A"
			}

			// Blocking bilgisini ekle
			blockingChains = append(blockingChains, map[string]interface{}{
				"blocked_session_id":       blockedSessionId,
				"blocking_session_id":      blockingSessionId,
				"wait_duration_ms":         waitDurationMs,
				"wait_duration_sec":        waitDurationMs / 1000,
				"wait_type":                waitType,
				"resource_description":     resourceDescription,
				"database_name":            databaseName,
				"blocked_program":          blockedProgramStr,
				"blocking_program":         blockingProgramStr,
				"blocked_login":            blockedLoginStr,
				"blocking_login":           blockingLoginStr,
				"blocking_sql":             blockingSqlShort,
				"blocked_sql":              blockedSqlShort,
				"blocked_open_tran_count":  blockedOpenTranCount,
				"blocking_open_tran_count": blockingOpenTranCount,
			})
		}

		blockingResults["current_blocking"] = blockingChains
		blockingResults["current_blocking_count"] = len(blockingChains)

		// Uzun süren blocking olup olmadığını tespit et
		var longBlockingCount int
		var criticalBlockingCount int

		for _, chain := range blockingChains {
			waitMs := chain["wait_duration_ms"].(int64)

			if waitMs > 30000 { // 30 saniyeden uzun süren blocking
				longBlockingCount++
			}

			if waitMs > 300000 { // 5 dakikadan uzun süren blocking
				criticalBlockingCount++
			}
		}

		blockingResults["long_blocking_count"] = longBlockingCount
		blockingResults["critical_blocking_count"] = criticalBlockingCount
		blockingResults["has_long_blocking"] = longBlockingCount > 0
		blockingResults["has_critical_blocking"] = criticalBlockingCount > 0

		log.Printf("Mevcut blocking analizi tamamlandı: %d blocking zinciri, %d uzun süreli, %d kritik",
			len(blockingChains), longBlockingCount, criticalBlockingCount)
	}

	// Son deadlock bilgilerini al (gerekli izinler varsa)
	log.Printf("Deadlock izleme bilgileri kontrol ediliyor...")

	// Deadlock izleme etkin mi kontrol et
	var isTraceEnabled int
	err = db.QueryRow(`
		SELECT [status] FROM sys.configurations WHERE [name] = 'blocked process threshold (s)'
	`).Scan(&isTraceEnabled)

	if err != nil {
		log.Printf("Deadlock izleme durumu kontrol edilemedi: %v", err)
		blockingResults["deadlock_trace_status"] = "UNKNOWN"
	} else {
		blockingResults["deadlock_trace_enabled"] = isTraceEnabled == 1

		if isTraceEnabled == 1 {
			blockingResults["deadlock_trace_status"] = "ENABLED"
		} else {
			blockingResults["deadlock_trace_status"] = "DISABLED"
			blockingResults["deadlock_recommendation"] = "Blocking ve deadlock izleme için 'blocked process threshold' değerini yapılandırmanız önerilir"
		}
	}

	// Extended Event'leri kullanarak son deadlock'ları kontrol et
	rows, err = db.Query(`
		SELECT TOP 10
			CAST(xet.target_data as xml) as deadlock_xml,
			xes.name as session_name
		FROM sys.dm_xe_session_targets xet
		JOIN sys.dm_xe_sessions xes ON xes.address = xet.event_session_address
		WHERE xes.name like '%deadlock%' AND xet.target_name = 'ring_buffer'
	`)

	if err != nil {
		log.Printf("Deadlock bilgileri alınamadı: %v", err)
		blockingResults["deadlock_history_error"] = err.Error()
	} else {
		defer rows.Close()
		var deadlockCount int
		var sessionName string

		if rows.Next() {
			var deadlockXml string

			if err := rows.Scan(&deadlockXml, &sessionName); err == nil {
				blockingResults["has_deadlock_tracking"] = true
				blockingResults["deadlock_session"] = sessionName

				// XML uzun olabilir, kısalt
				if len(deadlockXml) > 500 {
					deadlockXml = deadlockXml[:500] + "..."
				}

				blockingResults["deadlock_xml_sample"] = deadlockXml
				deadlockCount++
			}
		} else {
			blockingResults["has_deadlock_tracking"] = false
			blockingResults["deadlock_tracking_recommendation"] = "Deadlock izleme için Extended Event Session kurmanız önerilir"
		}

		blockingResults["deadlock_count"] = deadlockCount
	}

	// Uzun süren işlemleri kontrol et
	log.Printf("Uzun süreli işlemler kontrol ediliyor...")
	rows, err = db.Query(`
		SELECT TOP 20
			r.session_id,
			r.status,
			r.wait_type,
			r.wait_time / 1000.0 as wait_time_sec,
			r.wait_resource,
			r.blocking_session_id,
			r.cpu_time,
			r.total_elapsed_time / 1000.0 as elapsed_time_sec,
			r.reads,
			r.writes,
			r.logical_reads,
			r.row_count,
			SUBSTRING(
				qt.text, 
				(r.statement_start_offset/2)+1, 
				((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1
			) as current_statement,
			qt.text as full_text,
			DB_NAME(r.database_id) as database_name,
			s.login_name,
			s.host_name,
			s.program_name,
			s.last_request_start_time,
			s.login_time,
			s.open_transaction_count
		FROM sys.dm_exec_requests r
		CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) as qt
		JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
		WHERE r.session_id <> @@SPID
		AND r.session_id > 50 -- System sessions hariç
		AND r.wait_type IS NOT NULL
		AND r.wait_type NOT IN ('BROKER_RECEIVE_WAITFOR', 'WAIT_FOR_RESULTS', 'SLEEP_TASK')
		ORDER BY r.total_elapsed_time DESC
	`)

	if err != nil {
		log.Printf("Uzun işlemler sorgusu başarısız: %v", err)
		blockingResults["long_running_queries_error"] = err.Error()
	} else {
		defer rows.Close()
		var longQueries []map[string]interface{}

		for rows.Next() {
			var sessionId, blockingSessionId, cpuTime, reads, writes, logicalReads, rowCount, openTranCount int
			var status, waitType, waitResource, currentStatement, fullText, databaseName, loginName, hostName, programName *string
			var waitTimeSec, elapsedTimeSec float64
			var lastRequestStartTime, loginTime time.Time

			err := rows.Scan(
				&sessionId, &status, &waitType, &waitTimeSec, &waitResource, &blockingSessionId,
				&cpuTime, &elapsedTimeSec, &reads, &writes, &logicalReads, &rowCount,
				&currentStatement, &fullText, &databaseName, &loginName, &hostName, &programName,
				&lastRequestStartTime, &loginTime, &openTranCount,
			)

			if err != nil {
				log.Printf("Uzun işlem satırı okunamadı: %v", err)
				continue
			}

			// Null değerleri işle
			statusStr := "N/A"
			if status != nil {
				statusStr = *status
			}

			waitTypeStr := "N/A"
			if waitType != nil {
				waitTypeStr = *waitType
			}

			waitResourceStr := "N/A"
			if waitResource != nil {
				waitResourceStr = *waitResource
			}

			currentStmtStr := "N/A"
			if currentStatement != nil {
				currentStmtStr = *currentStatement
				if len(currentStmtStr) > 200 {
					currentStmtStr = currentStmtStr[:200] + "..."
				}
			}

			databaseNameStr := "N/A"
			if databaseName != nil {
				databaseNameStr = *databaseName
			}

			loginNameStr := "N/A"
			if loginName != nil {
				loginNameStr = *loginName
			}

			programNameStr := "N/A"
			if programName != nil {
				programNameStr = *programName
			}

			// Uzun işlem bilgilerini ekle
			longQueries = append(longQueries, map[string]interface{}{
				"session_id":              sessionId,
				"status":                  statusStr,
				"wait_type":               waitTypeStr,
				"wait_time_sec":           waitTimeSec,
				"wait_resource":           waitResourceStr,
				"blocking_session_id":     blockingSessionId,
				"elapsed_time_sec":        elapsedTimeSec,
				"cpu_time":                cpuTime,
				"reads":                   reads,
				"writes":                  writes,
				"logical_reads":           logicalReads,
				"row_count":               rowCount,
				"current_statement":       currentStmtStr,
				"database_name":           databaseNameStr,
				"login_name":              loginNameStr,
				"program_name":            programNameStr,
				"last_request_start_time": lastRequestStartTime.Format(time.RFC3339),
				"login_time":              loginTime.Format(time.RFC3339),
				"open_transaction_count":  openTranCount,
			})
		}

		blockingResults["long_running_queries"] = longQueries
		blockingResults["long_running_count"] = len(longQueries)

		log.Printf("Uzun işlemler analizi tamamlandı: %d uzun süreli işlem bulundu", len(longQueries))
	}

	// Sonuçları ekle
	b.results["BlockingAnalysis"] = blockingResults
}

// collectMemoryPressureAnalysis bellek kullanımını analiz eder
func (b *BestPracticesCollector) collectMemoryPressureAnalysis() {
	db, err := b.collector.GetClient()
	if err != nil {
		log.Printf("Memory analizi bilgileri toplanamadı: %v", err)
		b.results["MemoryPressureAnalysis"] = map[string]interface{}{
			"error":  err.Error(),
			"status": "error",
		}
		return
	}
	defer db.Close()

	// Memory analizi sonuçları
	memoryResults := make(map[string]interface{})

	// Sistem bellek kullanımı ve yapılandırması
	log.Printf("Sistem bellek kullanımı ve yapılandırması kontrol ediliyor...")
	var sqlServerMemoryMB, maxServerMemoryMB, minServerMemoryMB, targetServerMemoryMB, totalServerMemoryMB float64
	var memoryModelDesc string
	var totalSystemMemoryMB, availableSystemMemoryMB int64

	err = db.QueryRow(`
		SELECT 
			(physical_memory_in_use_kb / 1024.0) AS sql_server_memory_mb,
			(max_server_memory_kb / 1024.0) AS max_server_memory_mb,
			(min_server_memory_kb / 1024.0) AS min_server_memory_mb,
			(target_server_memory_kb / 1024.0) AS target_server_memory_mb,
			(total_server_memory_kb / 1024.0) AS total_server_memory_mb,
			sql_memory_model_desc
		FROM sys.dm_os_sys_info 
		CROSS JOIN sys.dm_os_process_memory
	`).Scan(&sqlServerMemoryMB, &maxServerMemoryMB, &minServerMemoryMB, &targetServerMemoryMB, &totalServerMemoryMB, &memoryModelDesc)

	if err != nil {
		log.Printf("Bellek yapılandırması bilgileri alınamadı: %v", err)
		memoryResults["memory_config_error"] = err.Error()
	} else {
		memoryResults["sql_server_memory_mb"] = sqlServerMemoryMB
		memoryResults["max_server_memory_mb"] = maxServerMemoryMB
		memoryResults["min_server_memory_mb"] = minServerMemoryMB
		memoryResults["target_server_memory_mb"] = targetServerMemoryMB
		memoryResults["total_server_memory_mb"] = totalServerMemoryMB
		memoryResults["sql_memory_model"] = memoryModelDesc

		// SQL Server tarafından rezerve edilmiş bellek yüzdesi
		if maxServerMemoryMB > 0 && totalServerMemoryMB > 0 {
			reservePercentage := 100.0 * totalServerMemoryMB / maxServerMemoryMB
			memoryResults["memory_reserve_percentage"] = reservePercentage

			// SQL Server hedef belleğine ulaşıp ulaşmadığını kontrol et
			if totalServerMemoryMB >= targetServerMemoryMB*0.95 {
				memoryResults["memory_target_reached"] = true

				// Hedef bellek, max belleğe yakınsa, bellek sınırlaması olabilir
				if targetServerMemoryMB >= maxServerMemoryMB*0.95 {
					memoryResults["has_memory_limitation"] = true
				}
			} else {
				memoryResults["memory_target_reached"] = false
			}
		}
	}

	// Sistem bellek bilgilerini al (Windows sistemlerde)
	err = db.QueryRow(`
		SELECT 
			total_physical_memory_kb / 1024 as total_system_memory_mb,
			available_physical_memory_kb / 1024 as available_system_memory_mb
		FROM sys.dm_os_sys_memory
	`).Scan(&totalSystemMemoryMB, &availableSystemMemoryMB)

	if err != nil {
		log.Printf("Sistem bellek bilgileri alınamadı: %v", err)
		memoryResults["system_memory_error"] = err.Error()
	} else {
		memoryResults["total_system_memory_mb"] = totalSystemMemoryMB
		memoryResults["available_system_memory_mb"] = availableSystemMemoryMB

		// Sistem bellek kullanım yüzdesi hesapla
		systemMemoryUsedPercent := 100.0 * (float64(totalSystemMemoryMB) - float64(availableSystemMemoryMB)) / float64(totalSystemMemoryMB)
		memoryResults["system_memory_used_percent"] = systemMemoryUsedPercent

		// Sistem bellek baskısı kontrolü
		memoryResults["has_system_memory_pressure"] = systemMemoryUsedPercent > 95 // %95'den fazla bellek kullanımı

		// SQL Server bellek yapılandırması kontrolü - sistem belleğinin en fazla %80'i SQL Server'a ayrılmalı
		if maxServerMemoryMB > 0 && totalSystemMemoryMB > 0 {
			sqlMemoryPercent := 100.0 * maxServerMemoryMB / float64(totalSystemMemoryMB)
			memoryResults["sql_memory_percent_of_system"] = sqlMemoryPercent

			if sqlMemoryPercent > 90 {
				memoryResults["has_aggressive_memory_config"] = true
				memoryResults["memory_config_recommendation"] = fmt.Sprintf(
					"SQL Server çok fazla sistem belleğini kullanıyor (%0.1f%%). Max server memory değerini sistem belleğinin %80'ine düşürmeniz önerilir",
					sqlMemoryPercent)
			}

			// Optimal bellek yapılandırması önerisi - bağlam değiştirme maliyetini azaltmak için
			optimalMaxMemoryMB := float64(totalSystemMemoryMB) * 0.8 // Sistem belleğinin %80'i
			memoryResults["recommended_max_memory_mb"] = optimalMaxMemoryMB
		}
	}

	// Buffer pool kullanımı
	log.Printf("Buffer pool kullanımı analiz ediliyor...")
	rows, err := db.Query(`
		SELECT TOP 10
			CASE database_id WHEN 32767 THEN 'ResourceDB' ELSE DB_NAME(database_id) END as database_name,
			COUNT(*) * 8 / 1024 as buffer_size_mb,
			COUNT(*) AS buffer_count,
			COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sys.dm_os_buffer_descriptors) AS buffer_percent
		FROM sys.dm_os_buffer_descriptors
		GROUP BY database_id
		ORDER BY COUNT(*) DESC
	`)

	if err != nil {
		log.Printf("Buffer pool analizi başarısız: %v", err)
		memoryResults["buffer_pool_error"] = err.Error()
	} else {
		defer rows.Close()
		var bufferUsage []map[string]interface{}
		var totalBufferSizeMB float64 = 0

		for rows.Next() {
			var dbName string
			var bufferSizeMB float64
			var bufferCount int64
			var bufferPercent float64

			if err := rows.Scan(&dbName, &bufferSizeMB, &bufferCount, &bufferPercent); err == nil {
				bufferUsage = append(bufferUsage, map[string]interface{}{
					"database_name":  dbName,
					"buffer_size_mb": bufferSizeMB,
					"buffer_count":   bufferCount,
					"buffer_percent": bufferPercent,
				})

				totalBufferSizeMB += bufferSizeMB
			}
		}

		memoryResults["buffer_usage_by_db"] = bufferUsage
		memoryResults["buffer_usage_count"] = len(bufferUsage)
		memoryResults["total_buffer_size_mb"] = totalBufferSizeMB

		log.Printf("Buffer pool analizi tamamlandı: %d veritabanı için toplam %.2f MB",
			len(bufferUsage), totalBufferSizeMB)
	}

	// Cache hit ratio analizi
	log.Printf("Cache hit ratio analiz ediliyor...")
	var bufferCacheHitRatio float64

	err = db.QueryRow(`
		SELECT 
			(a.cntr_value * 1.0 / b.cntr_value) * 100 as buffer_cache_hit_ratio
		FROM sys.dm_os_performance_counters a
		JOIN sys.dm_os_performance_counters b ON 
			a.object_name = b.object_name AND
			b.counter_name = 'Buffer cache hit ratio base' AND
			a.instance_name = b.instance_name
		WHERE a.counter_name = 'Buffer cache hit ratio'
		AND a.cntr_value > 0
	`).Scan(&bufferCacheHitRatio)

	if err != nil {
		log.Printf("Cache hit ratio alınamadı: %v", err)
		memoryResults["cache_hit_ratio_error"] = err.Error()
	} else {
		memoryResults["buffer_cache_hit_ratio"] = bufferCacheHitRatio

		// Cache hit ratio değerlendirmesi
		if bufferCacheHitRatio < 95 {
			memoryResults["has_low_cache_hit_ratio"] = true

			if bufferCacheHitRatio < 90 {
				memoryResults["cache_hit_ratio_status"] = "CRITICAL"
				memoryResults["cache_hit_recommendation"] = "Cache hit ratio çok düşük. SQL Server belleğini artırın veya sorgu desenlerini optimize edin."
			} else {
				memoryResults["cache_hit_ratio_status"] = "WARNING"
				memoryResults["cache_hit_recommendation"] = "Cache hit ratio kabul edilebilir ancak iyileştirilmesi gerekebilir."
			}
		} else {
			memoryResults["cache_hit_ratio_status"] = "GOOD"
		}

		log.Printf("Cache hit ratio: %.2f%% (%s)", bufferCacheHitRatio, memoryResults["cache_hit_ratio_status"])
	}

	// PLE (Page Life Expectancy) - sayfaların bellekte ne kadar süre kaldığını gösterir
	log.Printf("Page Life Expectancy (PLE) kontrol ediliyor...")
	var pageLifeExpectancy int

	err = db.QueryRow(`
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters
		WHERE counter_name = 'Page life expectancy'
		AND object_name LIKE '%Buffer Manager%'
	`).Scan(&pageLifeExpectancy)

	if err != nil {
		log.Printf("PLE bilgisi alınamadı: %v", err)
		memoryResults["ple_error"] = err.Error()
	} else {
		memoryResults["page_life_expectancy"] = pageLifeExpectancy

		// Bellek miktarına göre beklenen minimum PLE değeri hesapla
		// Her 4 GB bellek için 300 saniye (5 dakika) PLE beklenir
		expectedMinimumPLE := 300 // Varsayılan minimum 5 dakika

		if totalServerMemoryMB > 0 {
			memoryGB := totalServerMemoryMB / 1024.0
			expectedMinimumPLE = int(memoryGB * 300.0 / 4.0) // Her 4 GB için 300 saniye

			// Güvenlik için minimum değeri belirle
			if expectedMinimumPLE < 300 {
				expectedMinimumPLE = 300
			}
		}

		memoryResults["expected_minimum_ple"] = expectedMinimumPLE

		// PLE değerlendirmesi
		if pageLifeExpectancy < expectedMinimumPLE {
			memoryResults["has_low_ple"] = true

			if pageLifeExpectancy < expectedMinimumPLE/2 {
				memoryResults["ple_status"] = "CRITICAL"
				memoryResults["ple_recommendation"] = "PLE değeri çok düşük. Bu durum yoğun disk I/O ve bellek baskısına işaret eder. SQL Server belleğini artırın."
			} else {
				memoryResults["ple_status"] = "WARNING"
				memoryResults["ple_recommendation"] = "PLE değeri beklenenden düşük. Bellek ve disk I/O performansı izlenmelidir."
			}
		} else {
			memoryResults["ple_status"] = "GOOD"
		}

		log.Printf("Page Life Expectancy: %d saniye (beklenen minimum: %d saniye) - %s",
			pageLifeExpectancy, expectedMinimumPLE, memoryResults["ple_status"])
	}

	// En fazla bellek kullanan görevleri analiz et
	log.Printf("En fazla bellek kullanan görevler analiz ediliyor...")
	rows, err = db.Query(`
		SELECT TOP 10
			t.session_id,
			s.login_name,
			DB_NAME(t.database_id) as database_name,
			t.memory_usage / 128.0 as memory_usage_mb,
			SUBSTRING(
				qt.text, 
				(t.statement_start_offset/2)+1, 
				((CASE t.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE t.statement_end_offset END - t.statement_start_offset)/2)+1
			) as current_statement,
			t.cpu_time,
			t.total_elapsed_time / 1000.0 as elapsed_time_sec,
			t.reads,
			t.writes,
			t.logical_reads,
			t.status
		FROM sys.dm_exec_requests t
		JOIN sys.dm_exec_sessions s ON t.session_id = s.session_id
		CROSS APPLY sys.dm_exec_sql_text(t.sql_handle) as qt
		WHERE t.session_id <> @@SPID
		AND t.session_id > 50 -- System sessions hariç
		ORDER BY t.memory_usage DESC
	`)

	if err != nil {
		log.Printf("Bellek kullanan görevler sorgusu başarısız: %v", err)
		memoryResults["memory_tasks_error"] = err.Error()
	} else {
		defer rows.Close()
		var memoryTasks []map[string]interface{}

		for rows.Next() {
			var sessionId int
			var loginName, databaseName, currentStatement, status *string
			var memoryUsageMB, elapsedTimeSec float64
			var cpuTime, reads, writes, logicalReads int64

			err := rows.Scan(
				&sessionId, &loginName, &databaseName, &memoryUsageMB, &currentStatement,
				&cpuTime, &elapsedTimeSec, &reads, &writes, &logicalReads, &status,
			)

			if err != nil {
				log.Printf("Bellek görevi satırı okunamadı: %v", err)
				continue
			}

			// Null değerleri işle
			loginNameStr := "N/A"
			if loginName != nil {
				loginNameStr = *loginName
			}

			databaseNameStr := "N/A"
			if databaseName != nil {
				databaseNameStr = *databaseName
			}

			statusStr := "N/A"
			if status != nil {
				statusStr = *status
			}

			currentStmtStr := "N/A"
			if currentStatement != nil {
				currentStmtStr = *currentStatement
				if len(currentStmtStr) > 200 {
					currentStmtStr = currentStmtStr[:200] + "..."
				}
			}

			// Görev bilgilerini ekle
			memoryTasks = append(memoryTasks, map[string]interface{}{
				"session_id":        sessionId,
				"login_name":        loginNameStr,
				"database_name":     databaseNameStr,
				"memory_usage_mb":   memoryUsageMB,
				"current_statement": currentStmtStr,
				"cpu_time":          cpuTime,
				"elapsed_time_sec":  elapsedTimeSec,
				"reads":             reads,
				"writes":            writes,
				"logical_reads":     logicalReads,
				"status":            statusStr,
			})
		}

		memoryResults["memory_tasks"] = memoryTasks
		memoryResults["memory_tasks_count"] = len(memoryTasks)

		log.Printf("Bellek kullanan görevler analizi tamamlandı: %d görev bulundu", len(memoryTasks))
	}

	// Genel değerlendirme ve öneriler
	// Bellek kullanımı sorunları varsa özet öneri oluştur
	var memoryIssues []string

	if memoryResults["has_system_memory_pressure"] == true {
		memoryIssues = append(memoryIssues, "Sistem bellek baskısı tespit edildi")
	}

	if memoryResults["has_aggressive_memory_config"] == true {
		memoryIssues = append(memoryIssues, "SQL Server bellek yapılandırması çok agresif")
	}

	if memoryResults["has_low_cache_hit_ratio"] == true {
		memoryIssues = append(memoryIssues, "Düşük buffer cache hit ratio")
	}

	if memoryResults["has_low_ple"] == true {
		memoryIssues = append(memoryIssues, "Düşük Page Life Expectancy (PLE)")
	}

	memoryResults["memory_issues"] = memoryIssues
	memoryResults["memory_issues_count"] = len(memoryIssues)
	memoryResults["has_memory_issues"] = len(memoryIssues) > 0

	// Sonuçları ekle
	b.results["MemoryPressureAnalysis"] = memoryResults
}
