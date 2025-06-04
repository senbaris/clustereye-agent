package mongo

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/senbaris/clustereye-agent/internal/config"
	"github.com/senbaris/clustereye-agent/internal/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// MongoDBMetricsCollector collects time-series metrics for MongoDB
type MongoDBMetricsCollector struct {
	cfg       *config.AgentConfig
	collector *MongoCollector
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

// NewMongoDBMetricsCollector creates a new MongoDB metrics collector
func NewMongoDBMetricsCollector(cfg *config.AgentConfig) *MongoDBMetricsCollector {
	return &MongoDBMetricsCollector{
		cfg:       cfg,
		collector: NewMongoCollector(cfg),
	}
}

// CollectSystemMetrics collects system-level metrics
func (m *MongoDBMetricsCollector) CollectSystemMetrics() (*MetricBatch, error) {
	timestamp := time.Now().UnixNano()
	var metrics []Metric

	// Get system metrics from existing collector
	systemMetrics := m.collector.BatchCollectSystemMetrics()

	// Debug logging: Show all collected system metrics
	log.Printf("DEBUG: MongoDB CollectSystemMetrics - Raw system metrics collected: %d items", len(systemMetrics))
	for key, value := range systemMetrics {
		log.Printf("DEBUG: MongoDB SystemMetric[%s] = %v (type: %T)", key, value, value)
	}

	// CPU Metrics
	if cpuUsage, ok := systemMetrics["cpu_usage"].(float64); ok {
		log.Printf("DEBUG: MongoDB - Adding CPU usage metric: %.2f", cpuUsage)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.cpu_usage",
			Value:       MetricValue{DoubleValue: &cpuUsage},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "CPU usage percentage",
		})
	} else {
		log.Printf("DEBUG: MongoDB - CPU usage metric not found or wrong type in system metrics")
	}

	if cpuCount, ok := systemMetrics["cpu_count"].(int32); ok {
		cores := int64(cpuCount)
		log.Printf("DEBUG: MongoDB - Adding CPU cores metric: %d", cores)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.cpu_cores",
			Value:       MetricValue{IntValue: &cores},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of CPU cores",
		})
	} else {
		log.Printf("DEBUG: MongoDB - CPU count metric not found or wrong type in system metrics")
	}

	// Memory Metrics
	if memUsage, ok := systemMetrics["memory_usage"].(float64); ok {
		log.Printf("DEBUG: MongoDB - Adding memory usage metric: %.2f", memUsage)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.memory_usage",
			Value:       MetricValue{DoubleValue: &memUsage},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "percent",
			Description: "Memory usage percentage",
		})
	} else {
		log.Printf("DEBUG: MongoDB - Memory usage metric not found or wrong type in system metrics")
	}

	if totalMem, ok := systemMetrics["total_memory"].(int64); ok {
		log.Printf("DEBUG: MongoDB - Adding total memory metric: %d bytes", totalMem)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.total_memory",
			Value:       MetricValue{IntValue: &totalMem},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total system memory",
		})
	} else {
		log.Printf("DEBUG: MongoDB - Total memory metric not found or wrong type in system metrics")
	}

	if freeMem, ok := systemMetrics["free_memory"].(int64); ok {
		log.Printf("DEBUG: MongoDB - Adding free memory metric: %d bytes", freeMem)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.free_memory",
			Value:       MetricValue{IntValue: &freeMem},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free system memory",
		})
	} else {
		log.Printf("DEBUG: MongoDB - Free memory metric not found or wrong type in system metrics")
	}

	// Disk Metrics
	if totalDisk, ok := systemMetrics["total_disk"].(int64); ok {
		log.Printf("DEBUG: MongoDB - Adding total disk metric: %d bytes", totalDisk)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.total_disk",
			Value:       MetricValue{IntValue: &totalDisk},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Total disk space",
		})
	} else {
		log.Printf("DEBUG: MongoDB - Total disk metric not found or wrong type in system metrics")
	}

	if freeDisk, ok := systemMetrics["free_disk"].(int64); ok {
		log.Printf("DEBUG: MongoDB - Adding free disk metric: %d bytes", freeDisk)
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.free_disk",
			Value:       MetricValue{IntValue: &freeDisk},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "bytes",
			Description: "Free disk space",
		})
	} else {
		log.Printf("DEBUG: MongoDB - Free disk metric not found or wrong type in system metrics")
	}

	// Response Time Metric
	if responseTime, ok := systemMetrics["response_time_ms"].(float64); ok {
		metrics = append(metrics, Metric{
			Name:        "mongodb.system.response_time_ms",
			Value:       MetricValue{DoubleValue: &responseTime},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
			Timestamp:   timestamp,
			Unit:        "milliseconds",
			Description: "MongoDB response time for hello() command in milliseconds",
		})
	} else {
		if _, exists := systemMetrics["response_time_ms"]; exists {
		} else {
			log.Printf("DEBUG: MongoDB - response_time_ms key does not exist in systemMetrics")
		}
	}

	return &MetricBatch{
		AgentID:             m.getAgentID(),
		MetricType:          "mongodb_system",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform":    "mongodb",
			"os":          runtime.GOOS,
			"replica_set": m.getReplicaSetName(),
		},
	}, nil
}

// BatchCollectSystemMetrics collects various system metrics including response time
func (c *MongoCollector) BatchCollectSystemMetrics() map[string]interface{} {
	log.Printf("DEBUG: MongoDB BatchCollectSystemMetrics - Starting system metrics collection")

	metrics := make(map[string]interface{})

	// CPU Metrics
	cpuCores := c.getTotalvCpu()
	metrics["cpu_count"] = cpuCores
	log.Printf("DEBUG: MongoDB - CPU cores: %d", cpuCores)

	// Get CPU usage from system
	if cpuUsage, err := c.getCPUUsage(); err == nil {
		metrics["cpu_usage"] = cpuUsage
		log.Printf("DEBUG: MongoDB - CPU usage: %.2f%%", cpuUsage)
	} else {
		log.Printf("DEBUG: MongoDB - Failed to get CPU usage: %v", err)
		metrics["cpu_usage"] = float64(0)
	}

	// Memory Metrics
	totalMemory := c.getTotalMemory()
	metrics["total_memory"] = totalMemory
	log.Printf("DEBUG: MongoDB - Total memory: %d bytes", totalMemory)

	// Get memory usage
	if ramInfo, err := c.getRAMUsage(); err == nil {
		if usedPercent, ok := ramInfo["usage_percent"].(float64); ok {
			metrics["memory_usage"] = usedPercent
			log.Printf("DEBUG: MongoDB - Memory usage: %.2f%%", usedPercent)
		}
		if free, ok := ramInfo["free_mb"].(int64); ok {
			metrics["free_memory"] = free * 1024 * 1024 // Convert MB to bytes
			log.Printf("DEBUG: MongoDB - Free memory: %d bytes", free*1024*1024)
		} else {
			// Calculate free memory from total and used
			if usedPercent, ok := ramInfo["usage_percent"].(float64); ok {
				freeMemory := int64(float64(totalMemory) * (100 - usedPercent) / 100)
				metrics["free_memory"] = freeMemory
				log.Printf("DEBUG: MongoDB - Free memory calculated: %d bytes", freeMemory)
			} else {
				metrics["free_memory"] = int64(0)
			}
		}
	} else {
		log.Printf("DEBUG: MongoDB - Failed to get RAM usage: %v", err)
		metrics["memory_usage"] = float64(0)
		metrics["free_memory"] = int64(0)
	}

	// Disk Metrics
	if diskInfo, err := c.getDiskUsage(); err == nil {
		if total, ok := diskInfo["total_gb"].(int64); ok {
			metrics["total_disk"] = total * 1024 * 1024 * 1024 // Convert GB to bytes
			log.Printf("DEBUG: MongoDB - Total disk: %d bytes", total*1024*1024*1024)
		}

		if free, ok := diskInfo["avail_gb"].(int64); ok {
			metrics["free_disk"] = free * 1024 * 1024 * 1024 // Convert GB to bytes
			log.Printf("DEBUG: MongoDB - Free disk: %d bytes", free*1024*1024*1024)
		}
	} else {
		log.Printf("DEBUG: MongoDB - Failed to get disk usage: %v", err)
		metrics["total_disk"] = int64(0)
		metrics["free_disk"] = int64(0)
	}

	// MongoDB Response Time
	responseTime := c.measureMongoDBResponseTime()
	metrics["response_time_ms"] = responseTime
	log.Printf("DEBUG: MongoDB - Response time: %.6f ms", responseTime)

	// IP Address
	if ip := c.getLocalIP(); ip != "" {
		metrics["ip_address"] = ip
		log.Printf("DEBUG: MongoDB - IP address: %s", ip)
	} else {
		metrics["ip_address"] = "unknown"
	}

	log.Printf("DEBUG: MongoDB BatchCollectSystemMetrics - Collected %d metrics", len(metrics))
	for key, value := range metrics {
		log.Printf("DEBUG: MongoDB Metric[%s] = %v (type: %T)", key, value, value)
	}

	return metrics
}

// measureMongoDBResponseTime measures MongoDB response time with hello() command
func (c *MongoCollector) measureMongoDBResponseTime() float64 {
	start := time.Now()

	// Get database connection
	client, err := c.openDBDirect()
	if err != nil {
		log.Printf("DEBUG: MongoDB response time measurement failed - DB connection error: %v", err)
		return -1.0
	}
	defer client.Disconnect(context.Background())

	// Execute hello command
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adminDB := client.Database("admin")
	var result bson.M
	err = adminDB.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result)

	elapsed := time.Since(start)
	responseTimeMs := float64(elapsed.Nanoseconds()) / 1000000.0

	if err != nil {
		log.Printf("DEBUG: MongoDB response time measurement failed - Command error: %v", err)
		return -1.0
	}

	log.Printf("DEBUG: MongoDB response time measurement successful: %.6f ms", responseTimeMs)
	return responseTimeMs
}

// CollectDatabaseMetrics collects database-specific metrics
func (m *MongoDBMetricsCollector) CollectDatabaseMetrics() (*MetricBatch, error) {
	timestamp := time.Now().UnixNano()
	var metrics []Metric

	client, err := m.collector.openDBDirect()
	if err != nil {
		return nil, fmt.Errorf("failed to get MongoDB connection: %w", err)
	}
	defer client.Disconnect(context.Background())

	// Collect various database metrics
	m.collectConnectionMetrics(client, &metrics, timestamp)
	m.collectOperationMetrics(client, &metrics, timestamp)
	m.collectStorageMetrics(client, &metrics, timestamp)
	m.collectPerformanceMetrics(client, &metrics, timestamp)

	return &MetricBatch{
		AgentID:             m.getAgentID(),
		MetricType:          "mongodb_database",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform":    "mongodb",
			"replica_set": m.getReplicaSetName(),
		},
	}, nil
}

// collectConnectionMetrics collects connection-related metrics
func (m *MongoDBMetricsCollector) collectConnectionMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminDB := client.Database("admin")
	var serverStatus bson.M
	err := adminDB.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		logger.Error("Failed to collect MongoDB server status: %v", err)
		return
	}

	// Connection metrics
	if connections, ok := serverStatus["connections"].(bson.M); ok {
		if current, ok := connections["current"].(int32); ok {
			currentInt := int64(current)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.current",
				Value:       MetricValue{IntValue: &currentInt},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Current number of connections",
			})
		}

		if available, ok := connections["available"].(int32); ok {
			availableInt := int64(available)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.available",
				Value:       MetricValue{IntValue: &availableInt},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Available connections",
			})
		}

		if totalCreated, ok := connections["totalCreated"].(int64); ok {
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.total_created",
				Value:       MetricValue{IntValue: &totalCreated},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total connections created",
			})
		}
	}
}

// collectOperationMetrics collects operation-related metrics
func (m *MongoDBMetricsCollector) collectOperationMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminDB := client.Database("admin")
	var serverStatus bson.M
	err := adminDB.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		logger.Error("Failed to collect MongoDB server status for operations: %v", err)
		return
	}

	// Operation counters
	if opcounters, ok := serverStatus["opcounters"].(bson.M); ok {
		operations := map[string]string{
			"insert":  "Insert operations",
			"query":   "Query operations",
			"update":  "Update operations",
			"delete":  "Delete operations",
			"getmore": "GetMore operations",
			"command": "Command operations",
		}

		for opType, description := range operations {
			if count, ok := opcounters[opType].(int64); ok {
				*metrics = append(*metrics, Metric{
					Name:        fmt.Sprintf("mongodb.operations.%s", opType),
					Value:       MetricValue{IntValue: &count},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
					Timestamp:   timestamp,
					Unit:        "count",
					Description: description,
				})
			}
		}
	}

	// Cursor metrics
	if cursors, ok := serverStatus["cursors"].(bson.M); ok {
		if open, ok := cursors["open"].(bson.M); ok {
			if total, ok := open["total"].(int32); ok {
				totalInt := int64(total)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.cursors.open",
					Value:       MetricValue{IntValue: &totalInt},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Number of open cursors",
				})
			}
		}

		if timedOut, ok := cursors["timedOut"].(int64); ok {
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.cursors.timed_out",
				Value:       MetricValue{IntValue: &timedOut},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of timed out cursors",
			})
		}
	}
}

// collectStorageMetrics collects storage-related metrics
func (m *MongoDBMetricsCollector) collectStorageMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get list of databases
	databases, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		logger.Error("Failed to list databases: %v", err)
		return
	}

	// Collect stats for each database
	for _, dbName := range databases {
		// Skip system databases for detailed metrics
		if dbName == "admin" || dbName == "local" || dbName == "config" {
			continue
		}

		db := client.Database(dbName)
		var dbStats bson.M
		err := db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}, {Key: "scale", Value: 1024 * 1024}}).Decode(&dbStats)
		if err != nil {
			logger.Warning("Failed to get database stats for %s: %v", dbName, err)
			continue
		}

		dbTags := []MetricTag{
			{Key: "host", Value: m.getHostname()},
			{Key: "database", Value: dbName},
			{Key: "replica_set", Value: m.getReplicaSetName()},
		}

		// Data size in MB (scaled)
		if dataSize, ok := dbStats["dataSize"].(float64); ok {
			dataSizeInt := int64(dataSize)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.storage.data_size_mb",
				Value:       MetricValue{IntValue: &dataSizeInt},
				Tags:        dbTags,
				Timestamp:   timestamp,
				Unit:        "megabytes",
				Description: "Database data size in MB",
			})
		}

		// Storage size in MB (scaled)
		if storageSize, ok := dbStats["storageSize"].(float64); ok {
			storageSizeInt := int64(storageSize)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.storage.storage_size_mb",
				Value:       MetricValue{IntValue: &storageSizeInt},
				Tags:        dbTags,
				Timestamp:   timestamp,
				Unit:        "megabytes",
				Description: "Database storage size in MB",
			})
		}

		// Index size in MB (scaled)
		if indexSize, ok := dbStats["indexSize"].(float64); ok {
			indexSizeInt := int64(indexSize)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.storage.index_size_mb",
				Value:       MetricValue{IntValue: &indexSizeInt},
				Tags:        dbTags,
				Timestamp:   timestamp,
				Unit:        "megabytes",
				Description: "Database index size in MB",
			})
		}

		// Collection count
		if collections, ok := dbStats["collections"].(int32); ok {
			collectionsInt := int64(collections)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.storage.collections",
				Value:       MetricValue{IntValue: &collectionsInt},
				Tags:        dbTags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of collections",
			})
		}

		// Index count
		if indexes, ok := dbStats["indexes"].(int32); ok {
			indexesInt := int64(indexes)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.storage.indexes",
				Value:       MetricValue{IntValue: &indexesInt},
				Tags:        dbTags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Number of indexes",
			})
		}
	}
}

// collectPerformanceMetrics collects performance-related metrics
func (m *MongoDBMetricsCollector) collectPerformanceMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminDB := client.Database("admin")
	var serverStatus bson.M
	err := adminDB.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		logger.Error("Failed to collect MongoDB server status for performance: %v", err)
		return
	}

	// WiredTiger cache metrics (if using WiredTiger)
	if wiredTiger, ok := serverStatus["wiredTiger"].(bson.M); ok {
		if cache, ok := wiredTiger["cache"].(bson.M); ok {
			// Cache used bytes
			if bytesInCache, ok := cache["bytes currently in the cache"].(int64); ok {
				cacheUsedMB := bytesInCache / (1024 * 1024)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.performance.cache_used_mb",
					Value:       MetricValue{IntValue: &cacheUsedMB},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
					Timestamp:   timestamp,
					Unit:        "megabytes",
					Description: "WiredTiger cache used in MB",
				})
			}

			// Cache dirty bytes
			if dirtyBytes, ok := cache["tracked dirty bytes in the cache"].(int64); ok {
				cacheDirtyMB := dirtyBytes / (1024 * 1024)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.performance.cache_dirty_mb",
					Value:       MetricValue{IntValue: &cacheDirtyMB},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
					Timestamp:   timestamp,
					Unit:        "megabytes",
					Description: "WiredTiger dirty cache in MB",
				})
			}
		}
	}

	// Global lock metrics
	if globalLock, ok := serverStatus["globalLock"].(bson.M); ok {
		if currentQueue, ok := globalLock["currentQueue"].(bson.M); ok {
			if readers, ok := currentQueue["readers"].(int32); ok {
				readersInt := int64(readers)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.performance.lock_queue_readers",
					Value:       MetricValue{IntValue: &readersInt},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Readers waiting for lock",
				})
			}

			if writers, ok := currentQueue["writers"].(int32); ok {
				writersInt := int64(writers)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.performance.lock_queue_writers",
					Value:       MetricValue{IntValue: &writersInt},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Writers waiting for lock",
				})
			}
		}
	}

	// Extra info metrics
	if extraInfo, ok := serverStatus["extra_info"].(bson.M); ok {
		if pageFaults, ok := extraInfo["page_faults"].(int64); ok {
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.performance.page_faults",
				Value:       MetricValue{IntValue: &pageFaults},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: m.getReplicaSetName()}},
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total page faults",
			})
		}
	}
}

// CollectReplicationMetrics collects replication-specific metrics
func (m *MongoDBMetricsCollector) CollectReplicationMetrics() (*MetricBatch, error) {
	timestamp := time.Now().UnixNano()
	var metrics []Metric

	client, err := m.collector.openDBDirect()
	if err != nil {
		return nil, fmt.Errorf("failed to get MongoDB connection: %w", err)
	}
	defer client.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminDB := client.Database("admin")

	// Get replica set status
	var replSetStatus bson.M
	err = adminDB.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&replSetStatus)
	if err != nil {
		// Not a replica set member or not authorized
		log.Printf("DEBUG: MongoDB not in replica set or not authorized for replSetGetStatus: %v", err)
		return &MetricBatch{
			AgentID:             m.getAgentID(),
			MetricType:          "mongodb_replication",
			Metrics:             metrics,
			CollectionTimestamp: timestamp,
			Metadata: map[string]string{
				"platform":    "mongodb",
				"replica_set": "standalone",
			},
		}, nil
	}

	// Replica set name
	replicaSetName := ""
	if set, ok := replSetStatus["set"].(string); ok {
		replicaSetName = set
	}

	// Members metrics
	if members, ok := replSetStatus["members"].(bson.A); ok {
		totalMembers := int64(len(members))
		healthyMembers := int64(0)

		log.Printf("DEBUG: MongoDB Replication - Found %d members in replica set", totalMembers)

		metrics = append(metrics, Metric{
			Name:        "mongodb.replication.members_total",
			Value:       MetricValue{IntValue: &totalMembers},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Total replica set members",
		})

		for i, member := range members {
			log.Printf("DEBUG: MongoDB Replication - Processing member %d", i)

			if memberMap, ok := member.(bson.M); ok {
				log.Printf("DEBUG: MongoDB Replication - Member %d is valid bson.M", i)

				// Log all fields in memberMap for debugging
				log.Printf("DEBUG: MongoDB Replication - Member %d fields:", i)
				for key, value := range memberMap {
					log.Printf("DEBUG: MongoDB Replication - Member %d field[%s] = %v (type: %T)", i, key, value, value)
				}

				if health, ok := memberMap["health"].(float64); ok && health == 1.0 {
					healthyMembers++
					log.Printf("DEBUG: MongoDB Replication - Member %d is healthy", i)
				} else {
					log.Printf("DEBUG: MongoDB Replication - Member %d health check: health=%v, ok=%v", i, memberMap["health"], ok)
				}

				// Individual member lag metrics
				if state, ok := memberMap["state"].(int32); ok {
					log.Printf("DEBUG: MongoDB Replication - Member %d has state %d", i, state)

					if name, ok := memberMap["name"].(string); ok {
						log.Printf("DEBUG: MongoDB Replication - Member %d name: %s", i, name)

						stateInt := int64(state)
						memberTags := []MetricTag{
							{Key: "host", Value: m.getHostname()},
							{Key: "replica_set", Value: replicaSetName},
							{Key: "member", Value: name},
						}

						metrics = append(metrics, Metric{
							Name:        "mongodb.replication.member_state",
							Value:       MetricValue{IntValue: &stateInt},
							Tags:        memberTags,
							Timestamp:   timestamp,
							Unit:        "state",
							Description: "Replica set member state",
						})

						// Enhanced lag calculation for all members
						log.Printf("DEBUG: MongoDB Replication - Processing member %s with state %d", name, state)

						// Try different types for optimeDate since MongoDB can return different types
						var optimeDate time.Time
						var optimeFound bool

						// Try time.Time first
						if optime, ok := memberMap["optimeDate"].(time.Time); ok {
							optimeDate = optime
							optimeFound = true
							log.Printf("DEBUG: MongoDB Replication - Member %s optimeDate (time.Time): %v", name, optimeDate)
						} else if optime, ok := memberMap["optimeDate"].(primitive.DateTime); ok {
							// Convert primitive.DateTime to time.Time
							optimeDate = optime.Time()
							optimeFound = true
							log.Printf("DEBUG: MongoDB Replication - Member %s optimeDate (primitive.DateTime): %v -> %v", name, optime, optimeDate)
						} else {
							log.Printf("DEBUG: MongoDB Replication - Member %s optimeDate not found or unsupported type: %T", name, memberMap["optimeDate"])
						}

						if optimeFound {
							if primaryOptimeDate, ok := m.getPrimaryOptimeDate(replSetStatus); ok {
								log.Printf("DEBUG: MongoDB Replication - Primary optimeDate: %v", primaryOptimeDate)

								lagMs := primaryOptimeDate.Sub(optimeDate).Milliseconds()

								// Handle negative lag (Secondary ahead of Primary) - this shouldn't happen normally
								// but can occur due to clock skew or MongoDB internal timing
								if lagMs < 0 {
									// If secondary appears ahead, calculate reverse lag and take absolute value
									absLagMs := optimeDate.Sub(primaryOptimeDate).Milliseconds()
									log.Printf("DEBUG: MongoDB Replication - Member %s appears ahead of primary (negative lag: %dms), using absolute value: %dms", name, lagMs, absLagMs)
									lagMs = absLagMs
								}

								lagMsFloat := float64(lagMs)
								log.Printf("DEBUG: MongoDB Replication - Member %s final lag: %d ms", name, lagMs)

								// Include lag for all members (PRIMARY will be 0, SECONDARY will be actual lag)
								if state == 1 { // PRIMARY - lag is 0
									zeroLag := float64(0)
									metrics = append(metrics, Metric{
										Name:        "mongodb.replication.lag_ms_num",
										Value:       MetricValue{DoubleValue: &zeroLag},
										Tags:        memberTags,
										Timestamp:   timestamp,
										Unit:        "milliseconds",
										Description: "Replication lag in milliseconds (PRIMARY=0)",
									})
									log.Printf("DEBUG: MongoDB Replication - Added PRIMARY lag metric (0ms) for %s", name)
								} else if state == 2 { // SECONDARY - always add lag metric (positive value)
									metrics = append(metrics, Metric{
										Name:        "mongodb.replication.lag_ms_num",
										Value:       MetricValue{DoubleValue: &lagMsFloat},
										Tags:        memberTags,
										Timestamp:   timestamp,
										Unit:        "milliseconds",
										Description: "Replication lag in milliseconds",
									})
									log.Printf("DEBUG: MongoDB Replication - Added SECONDARY lag metric (%dms) for %s", lagMs, name)
								} else {
									log.Printf("DEBUG: MongoDB Replication - Skipped lag metric for %s: state=%d (not PRIMARY or SECONDARY)", name, state)
								}
							} else {
								log.Printf("DEBUG: MongoDB Replication - Could not get primary optime for lag calculation")
							}
						} else {
							log.Printf("DEBUG: MongoDB Replication - No optimeDate found for member %s (available fields: %v)", name, func() []string {
								var fields []string
								for k := range memberMap {
									fields = append(fields, k)
								}
								return fields
							}())
						}
					} else {
						log.Printf("DEBUG: MongoDB Replication - Member %d has no name field (available fields: %v)", i, func() []string {
							var fields []string
							for k := range memberMap {
								fields = append(fields, k)
							}
							return fields
						}())
					}
				} else {
					log.Printf("DEBUG: MongoDB Replication - Member %d has no state field or wrong type (value: %v, type: %T)", i, memberMap["state"], memberMap["state"])
				}
			} else {
				log.Printf("DEBUG: MongoDB Replication - Member %d is not a valid bson.M (type: %T)", i, member)
			}
		}

		metrics = append(metrics, Metric{
			Name:        "mongodb.replication.members_healthy",
			Value:       MetricValue{IntValue: &healthyMembers},
			Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Healthy replica set members",
		})
	}

	// Oplog metrics
	var oplogStats bson.M
	localDB := client.Database("local")
	err = localDB.RunCommand(ctx, bson.D{{Key: "collStats", Value: "oplog.rs"}}).Decode(&oplogStats)
	if err == nil {
		if size, ok := oplogStats["size"].(int64); ok {
			oplogSizeMB := size / (1024 * 1024)
			metrics = append(metrics, Metric{
				Name:        "mongodb.replication.oplog_size_mb",
				Value:       MetricValue{IntValue: &oplogSizeMB},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
				Timestamp:   timestamp,
				Unit:        "megabytes",
				Description: "Oplog size in MB",
			})
		}

		if storageSize, ok := oplogStats["storageSize"].(int64); ok {
			oplogStorageMB := storageSize / (1024 * 1024)
			metrics = append(metrics, Metric{
				Name:        "mongodb.replication.oplog_storage_mb",
				Value:       MetricValue{IntValue: &oplogStorageMB},
				Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
				Timestamp:   timestamp,
				Unit:        "megabytes",
				Description: "Oplog storage size in MB",
			})
		}
	}

	return &MetricBatch{
		AgentID:             m.getAgentID(),
		MetricType:          "mongodb_replication",
		Metrics:             metrics,
		CollectionTimestamp: timestamp,
		Metadata: map[string]string{
			"platform":    "mongodb",
			"replica_set": replicaSetName,
		},
	}, nil
}

// getPrimaryOptimeDate finds the primary member's optime date
func (m *MongoDBMetricsCollector) getPrimaryOptimeDate(replSetStatus bson.M) (time.Time, bool) {
	log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate called")

	if members, ok := replSetStatus["members"].(bson.A); ok {
		log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate found %d members", len(members))

		for i, member := range members {
			log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate checking member %d", i)

			if memberMap, ok := member.(bson.M); ok {
				log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate member %d is valid bson.M", i)

				if state, ok := memberMap["state"].(int32); ok {
					log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate member %d has state %d", i, state)

					if state == 1 { // PRIMARY
						log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate found PRIMARY member %d", i)

						// Try different types for optimeDate
						if optimeDate, ok := memberMap["optimeDate"].(time.Time); ok {
							log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate found PRIMARY optimeDate (time.Time): %v", optimeDate)
							return optimeDate, true
						} else if optimeDate, ok := memberMap["optimeDate"].(primitive.DateTime); ok {
							// Convert primitive.DateTime to time.Time
							timeValue := optimeDate.Time()
							log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate found PRIMARY optimeDate (primitive.DateTime): %v -> %v", optimeDate, timeValue)
							return timeValue, true
						} else {
							log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate PRIMARY member %d has unsupported optimeDate type: %T (value: %v)", i, memberMap["optimeDate"], memberMap["optimeDate"])
							log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate PRIMARY member %d available fields: %v", i, func() []string {
								var fields []string
								for k := range memberMap {
									fields = append(fields, k)
								}
								return fields
							}())
						}
					}
				} else {
					log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate member %d has no state field (type: %T, value: %v)", i, memberMap["state"], memberMap["state"])
				}
			} else {
				log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate member %d is not bson.M (type: %T)", i, member)
			}
		}

		log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate no PRIMARY member found with optimeDate")
	} else {
		log.Printf("DEBUG: MongoDB Replication - getPrimaryOptimeDate no members array found in replSetStatus")
	}

	return time.Time{}, false
}

// getHostname returns the hostname for tagging
func (m *MongoDBMetricsCollector) getHostname() string {
	if m.cfg.Mongo.Host != "" {
		return m.cfg.Mongo.Host
	}
	return "localhost"
}

// getReplicaSetName returns the replica set name
func (m *MongoDBMetricsCollector) getReplicaSetName() string {
	if m.collector != nil {
		return m.collector.GetReplicaSetName()
	}
	if m.cfg.Mongo.Replset != "" {
		return m.cfg.Mongo.Replset
	}
	return "standalone"
}

// getAgentID returns the proper agent ID in the format "agent_<hostname>"
func (m *MongoDBMetricsCollector) getAgentID() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	return "agent_" + hostname
}

// CollectAllMetrics collects all types of metrics
func (m *MongoDBMetricsCollector) CollectAllMetrics() ([]*MetricBatch, error) {
	var batches []*MetricBatch

	// Collect system metrics
	systemBatch, err := m.CollectSystemMetrics()
	if err != nil {
		log.Printf("Failed to collect MongoDB system metrics: %v", err)
	} else {
		batches = append(batches, systemBatch)
	}

	// Collect database metrics
	dbBatch, err := m.CollectDatabaseMetrics()
	if err != nil {
		log.Printf("Failed to collect MongoDB database metrics: %v", err)
	} else {
		batches = append(batches, dbBatch)
	}

	// Collect replication metrics
	replBatch, err := m.CollectReplicationMetrics()
	if err != nil {
		log.Printf("Failed to collect MongoDB replication metrics: %v", err)
	} else {
		batches = append(batches, replBatch)
	}

	return batches, nil
}

// Helper methods for MongoCollector system metrics collection

func (c *MongoCollector) getCPUUsage() (float64, error) {
	// Implement CPU usage collection for MongoDB
	// This is a placeholder - you can implement actual CPU usage collection
	return 50.0, nil // Default value
}

func (c *MongoCollector) getRAMUsage() (map[string]interface{}, error) {
	// Implement RAM usage collection for MongoDB
	// This is a placeholder - you can implement actual RAM usage collection
	return map[string]interface{}{
		"usage_percent": 60.0,
		"free_mb":       4096,
	}, nil
}

func (c *MongoCollector) getDiskUsage() (map[string]interface{}, error) {
	// Use the existing GetDiskUsage method which provides real disk information
	freeDiskStr, usagePercent := c.GetDiskUsage()

	if freeDiskStr == "N/A" {
		log.Printf("DEBUG: MongoDB getDiskUsage - No disk info available")
		return map[string]interface{}{
			"total_gb": int64(0),
			"avail_gb": int64(0),
		}, nil
	}

	// Parse free disk size
	freeDiskBytes, err := c.convertToBytes(freeDiskStr)
	if err != nil {
		log.Printf("DEBUG: MongoDB getDiskUsage - Failed to parse free disk: %v", err)
		return map[string]interface{}{
			"total_gb": int64(0),
			"avail_gb": int64(0),
		}, nil
	}

	// Calculate total disk from free disk and usage percentage
	// If usage is 60% and free is 40%, then total = free / 0.4
	freePercent := 100 - usagePercent
	if freePercent <= 0 {
		freePercent = 1 // Prevent division by zero
	}

	totalDiskBytes := (freeDiskBytes * 100) / uint64(freePercent)

	// Convert to GB
	freeDiskGB := int64(freeDiskBytes / (1024 * 1024 * 1024))
	totalDiskGB := int64(totalDiskBytes / (1024 * 1024 * 1024))

	log.Printf("DEBUG: MongoDB getDiskUsage - Real disk info: Free=%s (%dGB), Usage=%d%%, Total=%dGB",
		freeDiskStr, freeDiskGB, usagePercent, totalDiskGB)

	return map[string]interface{}{
		"total_gb": totalDiskGB,
		"avail_gb": freeDiskGB,
	}, nil
}
