package mongo

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
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

	log.Printf("DEBUG: MongoDB collectConnectionMetrics - Starting connection metrics collection")

	adminDB := client.Database("admin")
	var serverStatus bson.M
	err := adminDB.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		logger.Error("Failed to collect MongoDB server status: %v", err)
		return
	}

	tags := []MetricTag{
		{Key: "host", Value: m.getHostname()},
		{Key: "replica_set", Value: m.getReplicaSetName()},
	}

	// Basic connection metrics
	if connections, ok := serverStatus["connections"].(bson.M); ok {
		log.Printf("DEBUG: MongoDB - Found connections in serverStatus: %+v", connections)

		if current, ok := connections["current"].(int32); ok {
			currentInt := int64(current)
			log.Printf("DEBUG: MongoDB - Adding current connections metric: %d", currentInt)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.current",
				Value:       MetricValue{IntValue: &currentInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Current number of connections",
			})
		} else {
			log.Printf("DEBUG: MongoDB - connections.current not found or wrong type: %T", connections["current"])
		}

		if available, ok := connections["available"].(int32); ok {
			availableInt := int64(available)
			log.Printf("DEBUG: MongoDB - Adding available connections metric: %d", availableInt)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.available",
				Value:       MetricValue{IntValue: &availableInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Available connections",
			})

			// Calculate pool utilization percentage
			if current, ok := connections["current"].(int32); ok {
				totalCapacity := int64(current) + availableInt
				if totalCapacity > 0 {
					utilization := (float64(current) / float64(totalCapacity)) * 100
					log.Printf("DEBUG: MongoDB - Adding pool utilization metric: %.2f%%", utilization)
					*metrics = append(*metrics, Metric{
						Name:        "mongodb.connections.pool_utilization",
						Value:       MetricValue{DoubleValue: &utilization},
						Tags:        tags,
						Timestamp:   timestamp,
						Unit:        "percent",
						Description: "Connection pool utilization percentage",
					})
				}
			}
		} else {
			log.Printf("DEBUG: MongoDB - connections.available not found or wrong type: %T", connections["available"])
		}

		if totalCreated, ok := connections["totalCreated"].(int64); ok {
			log.Printf("DEBUG: MongoDB - Adding total created connections metric: %d", totalCreated)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.total_created",
				Value:       MetricValue{IntValue: &totalCreated},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total connections created",
			})
		} else {
			log.Printf("DEBUG: MongoDB - connections.totalCreated not found or wrong type: %T", connections["totalCreated"])
		}

		// Connection pool efficiency metrics
		if current, ok := connections["current"].(int32); ok {
			log.Printf("DEBUG: MongoDB - Current connections for reuse ratio: %d", current)

			// Try different types for totalCreated
			var totalCreated int64 = 0
			var totalCreatedFound bool = false

			if tc, ok := connections["totalCreated"].(int64); ok {
				totalCreated = tc
				totalCreatedFound = true
				log.Printf("DEBUG: MongoDB - totalCreated found as int64: %d", totalCreated)
			} else if tc, ok := connections["totalCreated"].(int32); ok {
				totalCreated = int64(tc)
				totalCreatedFound = true
				log.Printf("DEBUG: MongoDB - totalCreated found as int32: %d", totalCreated)
			} else if tc, ok := connections["totalCreated"].(float64); ok {
				totalCreated = int64(tc)
				totalCreatedFound = true
				log.Printf("DEBUG: MongoDB - totalCreated found as float64: %.0f", tc)
			} else {
				log.Printf("DEBUG: MongoDB - totalCreated not found or unsupported type: %T, value: %v", connections["totalCreated"], connections["totalCreated"])

				// Debug all fields in connections
				log.Printf("DEBUG: MongoDB - All connections fields:")
				for key, value := range connections {
					log.Printf("DEBUG: MongoDB - connections[%s] = %v (type: %T)", key, value, value)
				}
			}

			if totalCreatedFound && totalCreated > 0 {
				connectionReuse := float64(current) / float64(totalCreated)
				log.Printf("DEBUG: MongoDB - Adding connection reuse ratio metric: %.4f (current=%d, totalCreated=%d)", connectionReuse, current, totalCreated)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.reuse_ratio",
					Value:       MetricValue{DoubleValue: &connectionReuse},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "ratio",
					Description: "Connection reuse efficiency ratio",
				})
			} else if totalCreatedFound {
				log.Printf("DEBUG: MongoDB - totalCreated is 0, skipping reuse ratio calculation")
			} else {
				log.Printf("DEBUG: MongoDB - totalCreated not available for reuse ratio calculation")
			}
		} else {
			log.Printf("DEBUG: MongoDB - current connections not available for reuse ratio calculation, type: %T", connections["current"])
		}
	} else {
		log.Printf("DEBUG: MongoDB - connections object not found in serverStatus")
	}

	log.Printf("DEBUG: MongoDB - Calling collectConnectionPoolAdvancedMetrics")
	// Collect advanced connection pool metrics
	m.collectConnectionPoolAdvancedMetrics(client, metrics, timestamp, tags)

	log.Printf("DEBUG: MongoDB - Calling collectConnectionWaitMetrics")
	// Collect connection wait time metrics
	m.collectConnectionWaitMetrics(client, metrics, timestamp, tags)
}

// collectConnectionPoolAdvancedMetrics collects advanced connection pool metrics
func (m *MongoDBMetricsCollector) collectConnectionPoolAdvancedMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	log.Printf("DEBUG: MongoDB collectConnectionPoolAdvancedMetrics - Starting advanced metrics collection")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adminDB := client.Database("admin")

	// Get connection pool stats from serverStatus
	var serverStatus bson.M
	err := adminDB.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		log.Printf("DEBUG: MongoDB - Failed to get serverStatus for pool metrics: %v", err)
		return
	}

	// Check for connection pool specific metrics in serverStatus
	if connectionPool, ok := serverStatus["connectionPool"].(bson.M); ok {
		log.Printf("DEBUG: MongoDB - Found connectionPool in serverStatus: %+v", connectionPool)

		// Pool size metrics
		if totalInUse, ok := connectionPool["totalInUse"].(int32); ok {
			totalInUseInt := int64(totalInUse)
			log.Printf("DEBUG: MongoDB - Adding pool_in_use metric: %d", totalInUseInt)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.pool_in_use",
				Value:       MetricValue{IntValue: &totalInUseInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total connections currently in use",
			})
		} else {
			log.Printf("DEBUG: MongoDB - connectionPool.totalInUse not found or wrong type: %T", connectionPool["totalInUse"])
		}

		if totalAvailable, ok := connectionPool["totalAvailable"].(int32); ok {
			totalAvailableInt := int64(totalAvailable)
			log.Printf("DEBUG: MongoDB - Adding pool_available metric: %d", totalAvailableInt)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.pool_available",
				Value:       MetricValue{IntValue: &totalAvailableInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total available connections in pool",
			})
		} else {
			log.Printf("DEBUG: MongoDB - connectionPool.totalAvailable not found or wrong type: %T", connectionPool["totalAvailable"])
		}

		if totalCreated, ok := connectionPool["totalCreated"].(int32); ok {
			totalCreatedInt := int64(totalCreated)
			log.Printf("DEBUG: MongoDB - Adding pool_total_created metric: %d", totalCreatedInt)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.pool_total_created",
				Value:       MetricValue{IntValue: &totalCreatedInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total connections created by pool",
			})
		} else {
			log.Printf("DEBUG: MongoDB - connectionPool.totalCreated not found or wrong type: %T", connectionPool["totalCreated"])
		}

		if totalRefreshing, ok := connectionPool["totalRefreshing"].(int32); ok {
			totalRefreshingInt := int64(totalRefreshing)
			log.Printf("DEBUG: MongoDB - Adding pool_refreshing metric: %d", totalRefreshingInt)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.pool_refreshing",
				Value:       MetricValue{IntValue: &totalRefreshingInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Connections currently being refreshed",
			})
		} else {
			log.Printf("DEBUG: MongoDB - connectionPool.totalRefreshing not found or wrong type: %T", connectionPool["totalRefreshing"])
		}
	} else {
		log.Printf("DEBUG: MongoDB - connectionPool not found in serverStatus, trying currentOp analysis")
	}

	// Alternative: Get connection info from currentOp
	log.Printf("DEBUG: MongoDB - Getting currentOp for connection analysis")
	cursor, err := adminDB.RunCommandCursor(ctx, bson.D{{Key: "currentOp", Value: 1}})
	if err != nil {
		log.Printf("DEBUG: MongoDB - Failed to get currentOp for connection analysis: %v", err)
		return
	}
	defer cursor.Close(ctx)

	var currentOp bson.M
	if cursor.Next(ctx) {
		if err := cursor.Decode(&currentOp); err != nil {
			log.Printf("DEBUG: MongoDB - Failed to decode currentOp: %v", err)
			return
		}
	} else {
		log.Printf("DEBUG: MongoDB - No data returned from currentOp")
		return
	}

	log.Printf("DEBUG: MongoDB - currentOp keys: %v", func() []string {
		var keys []string
		for k := range currentOp {
			keys = append(keys, k)
		}
		return keys
	}())

	if inprog, ok := currentOp["inprog"].(bson.A); ok {
		log.Printf("DEBUG: MongoDB - Found %d operations in currentOp.inprog", len(inprog))

		var connectionsByClient = make(map[string]int64)
		var longRunningConnections int64 = 0
		var idleConnections int64 = 0

		for i, op := range inprog {
			if opMap, ok := op.(bson.M); ok {
				log.Printf("DEBUG: MongoDB - Processing operation %d, keys: %v", i, func() []string {
					var keys []string
					for k := range opMap {
						keys = append(keys, k)
					}
					return keys
				}())

				// Analyze connection patterns
				if client, ok := opMap["client"].(string); ok {
					connectionsByClient[client]++
					log.Printf("DEBUG: MongoDB - Found client: %s", client)
				} else {
					log.Printf("DEBUG: MongoDB - No client field in operation %d", i)
				}

				// Count long-running connections (>5 minutes)
				if secs, ok := opMap["secs_running"].(int32); ok {
					log.Printf("DEBUG: MongoDB - Operation %d has been running for %d seconds", i, secs)
					if secs > 300 { // 5 minutes
						longRunningConnections++
					}
				} else {
					log.Printf("DEBUG: MongoDB - No secs_running field in operation %d", i)
				}

				// Count idle connections
				if op, ok := opMap["op"].(string); ok {
					log.Printf("DEBUG: MongoDB - Operation %d type: %s", i, op)
					if op == "none" {
						idleConnections++
					}
				} else {
					log.Printf("DEBUG: MongoDB - No op field in operation %d", i)
				}
			} else {
				log.Printf("DEBUG: MongoDB - Operation %d is not a valid bson.M (type: %T)", i, op)
			}
		}

		// Long-running connections metric
		log.Printf("DEBUG: MongoDB - Adding long_running connections metric: %d", longRunningConnections)
		*metrics = append(*metrics, Metric{
			Name:        "mongodb.connections.long_running",
			Value:       MetricValue{IntValue: &longRunningConnections},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Connections running for more than 5 minutes",
		})

		// Idle connections metric
		log.Printf("DEBUG: MongoDB - Adding idle connections metric: %d", idleConnections)
		*metrics = append(*metrics, Metric{
			Name:        "mongodb.connections.idle",
			Value:       MetricValue{IntValue: &idleConnections},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Idle connections",
		})

		// Unique clients count
		uniqueClients := int64(len(connectionsByClient))
		log.Printf("DEBUG: MongoDB - Adding unique_clients metric: %d", uniqueClients)
		*metrics = append(*metrics, Metric{
			Name:        "mongodb.connections.unique_clients",
			Value:       MetricValue{IntValue: &uniqueClients},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of unique client connections",
		})

		// Average connections per client
		if uniqueClients > 0 {
			totalConnections := int64(len(inprog))
			avgConnectionsPerClient := float64(totalConnections) / float64(uniqueClients)
			log.Printf("DEBUG: MongoDB - Adding avg_per_client metric: %.2f", avgConnectionsPerClient)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.avg_per_client",
				Value:       MetricValue{DoubleValue: &avgConnectionsPerClient},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "ratio",
				Description: "Average connections per client",
			})
		} else {
			log.Printf("DEBUG: MongoDB - No unique clients found, skipping avg_per_client metric")
		}
	} else {
		log.Printf("DEBUG: MongoDB - inprog not found in currentOp or wrong type: %T", currentOp["inprog"])
	}

	log.Printf("DEBUG: MongoDB collectConnectionPoolAdvancedMetrics - Completed advanced metrics collection")
}

// collectConnectionWaitMetrics collects connection wait time and blocking metrics
func (m *MongoDBMetricsCollector) collectConnectionWaitMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adminDB := client.Database("admin")

	// Get global lock wait metrics as a proxy for connection contention
	var serverStatus bson.M
	err := adminDB.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus)
	if err != nil {
		return
	}

	if globalLock, ok := serverStatus["globalLock"].(bson.M); ok {
		if currentQueue, ok := globalLock["currentQueue"].(bson.M); ok {
			// Total waiting connections
			if total, ok := currentQueue["total"].(int32); ok {
				totalInt := int64(total)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.waiting_total",
					Value:       MetricValue{IntValue: &totalInt},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Total connections waiting for global lock",
				})
			}

			if readers, ok := currentQueue["readers"].(int32); ok {
				readersInt := int64(readers)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.waiting_readers",
					Value:       MetricValue{IntValue: &readersInt},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Reader connections waiting for lock",
				})
			}

			if writers, ok := currentQueue["writers"].(int32); ok {
				writersInt := int64(writers)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.waiting_writers",
					Value:       MetricValue{IntValue: &writersInt},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Writer connections waiting for lock",
				})
			}
		}

		// Active clients metrics
		if activeClients, ok := globalLock["activeClients"].(bson.M); ok {
			if total, ok := activeClients["total"].(int32); ok {
				totalInt := int64(total)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.active_total",
					Value:       MetricValue{IntValue: &totalInt},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Total active client connections",
				})
			}

			if readers, ok := activeClients["readers"].(int32); ok {
				readersInt := int64(readers)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.active_readers",
					Value:       MetricValue{IntValue: &readersInt},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Active reader connections",
				})
			}

			if writers, ok := activeClients["writers"].(int32); ok {
				writersInt := int64(writers)
				*metrics = append(*metrics, Metric{
					Name:        "mongodb.connections.active_writers",
					Value:       MetricValue{IntValue: &writersInt},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Active writer connections",
				})
			}
		}
	}

	// Connection timeout and error metrics from serverStatus
	if asserts, ok := serverStatus["asserts"].(bson.M); ok {
		if warning, ok := asserts["warning"].(int32); ok {
			warningInt := int64(warning)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.warnings",
				Value:       MetricValue{IntValue: &warningInt},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Connection-related warnings",
			})
		}
	}

	// Network metrics that affect connection performance
	if network, ok := serverStatus["network"].(bson.M); ok {
		if bytesIn, ok := network["bytesIn"].(int64); ok {
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.bytes_in",
				Value:       MetricValue{IntValue: &bytesIn},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "bytes",
				Description: "Total bytes received from connections",
			})
		}

		if bytesOut, ok := network["bytesOut"].(int64); ok {
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.bytes_out",
				Value:       MetricValue{IntValue: &bytesOut},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "bytes",
				Description: "Total bytes sent to connections",
			})
		}

		if numRequests, ok := network["numRequests"].(int64); ok {
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.connections.total_requests",
				Value:       MetricValue{IntValue: &numRequests},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "count",
				Description: "Total network requests received",
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

	// Collect query performance metrics
	m.collectQueryPerformanceMetrics(client, metrics, timestamp, serverStatus)

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

// collectQueryPerformanceMetrics collects detailed query performance metrics
func (m *MongoDBMetricsCollector) collectQueryPerformanceMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64, serverStatus bson.M) {
	tags := []MetricTag{
		{Key: "host", Value: m.getHostname()},
		{Key: "replica_set", Value: m.getReplicaSetName()},
	}

	// 1. Queries Per Second (QPS) from opcounters
	if opcounters, ok := serverStatus["opcounters"].(bson.M); ok {
		var totalOps int64
		for _, opType := range []string{"insert", "query", "update", "delete", "getmore", "command"} {
			if count, ok := opcounters[opType].(int64); ok {
				totalOps += count
			}
		}

		// Calculate QPS (approximate - based on total operations since startup)
		if uptime, ok := serverStatus["uptime"].(int32); ok && uptime > 0 {
			qps := float64(totalOps) / float64(uptime)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.performance.queries_per_sec",
				Value:       MetricValue{DoubleValue: &qps},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "operations_per_second",
				Description: "Queries per second (approximate)",
			})
		}

		// 2. Read/Write Ratio
		readOps := int64(0)
		writeOps := int64(0)

		if query, ok := opcounters["query"].(int64); ok {
			readOps += query
		}
		if getmore, ok := opcounters["getmore"].(int64); ok {
			readOps += getmore
		}

		if insert, ok := opcounters["insert"].(int64); ok {
			writeOps += insert
		}
		if update, ok := opcounters["update"].(int64); ok {
			writeOps += update
		}
		if delete, ok := opcounters["delete"].(int64); ok {
			writeOps += delete
		}

		if writeOps > 0 {
			readWriteRatio := float64(readOps) / float64(writeOps)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.performance.read_write_ratio",
				Value:       MetricValue{DoubleValue: &readWriteRatio},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "ratio",
				Description: "Read to write operations ratio",
			})
		}
	}

	// 3. Current Operations Analysis (for real-time query performance)
	m.collectCurrentOperationsMetrics(client, metrics, timestamp, tags)

	// 4. Profiler Data Analysis (if profiler is enabled)
	m.collectProfilerMetrics(client, metrics, timestamp, tags)
}

// collectCurrentOperationsMetrics analyzes currently running operations
func (m *MongoDBMetricsCollector) collectCurrentOperationsMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adminDB := client.Database("admin")

	// Get current operations
	cursor, err := adminDB.RunCommandCursor(ctx, bson.D{{Key: "currentOp", Value: 1}})
	if err != nil {
		log.Printf("DEBUG: MongoDB - Failed to get current operations: %v", err)
		return
	}
	defer cursor.Close(ctx)

	var currentOp bson.M
	if cursor.Next(ctx) {
		if err := cursor.Decode(&currentOp); err != nil {
			log.Printf("DEBUG: MongoDB - Failed to decode current operations: %v", err)
			return
		}
	}

	if inprog, ok := currentOp["inprog"].(bson.A); ok {
		var slowQueryCount int64 = 0
		var totalQueryTime float64 = 0
		var queryCount int64 = 0
		var queryTimes []float64

		for _, op := range inprog {
			if opMap, ok := op.(bson.M); ok {
				// Check if it's a query operation
				if opType, exists := opMap["op"]; exists {
					if opTypeStr, ok := opType.(string); ok && (opTypeStr == "query" || opTypeStr == "getmore" || opTypeStr == "command") {
						// Get operation duration
						if secs, ok := opMap["secs_running"].(int32); ok {
							duration := float64(secs) * 1000 // Convert to milliseconds
							queryTimes = append(queryTimes, duration)
							totalQueryTime += duration
							queryCount++

							// Count slow queries (>100ms)
							if duration > 100 {
								slowQueryCount++
							}
						}
					}
				}
			}
		}

		// 5. Slow Queries Count
		*metrics = append(*metrics, Metric{
			Name:        "mongodb.performance.slow_queries_count",
			Value:       MetricValue{IntValue: &slowQueryCount},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of currently running slow queries (>100ms)",
		})

		// 6. Average Query Time
		if queryCount > 0 {
			avgQueryTime := totalQueryTime / float64(queryCount)
			*metrics = append(*metrics, Metric{
				Name:        "mongodb.performance.avg_query_time_ms",
				Value:       MetricValue{DoubleValue: &avgQueryTime},
				Tags:        tags,
				Timestamp:   timestamp,
				Unit:        "milliseconds",
				Description: "Average query execution time",
			})

			// 7. Query Time Percentiles
			if len(queryTimes) > 0 {
				// Sort query times for percentile calculation
				sort.Float64s(queryTimes)

				// 95th percentile
				p95Index := int(float64(len(queryTimes)) * 0.95)
				if p95Index >= len(queryTimes) {
					p95Index = len(queryTimes) - 1
				}
				p95Value := queryTimes[p95Index]

				*metrics = append(*metrics, Metric{
					Name:        "mongodb.performance.query_time_p95_ms",
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
					Name:        "mongodb.performance.query_time_p99_ms",
					Value:       MetricValue{DoubleValue: &p99Value},
					Tags:        tags,
					Timestamp:   timestamp,
					Unit:        "milliseconds",
					Description: "99th percentile query execution time",
				})
			}
		}

		// 8. Active Queries Count
		activeQueries := int64(queryCount)
		*metrics = append(*metrics, Metric{
			Name:        "mongodb.performance.active_queries_count",
			Value:       MetricValue{IntValue: &activeQueries},
			Tags:        tags,
			Timestamp:   timestamp,
			Unit:        "count",
			Description: "Number of currently active queries",
		})
	}
}

// collectProfilerMetrics collects metrics from MongoDB profiler (if enabled)
func (m *MongoDBMetricsCollector) collectProfilerMetrics(client *mongo.Client, metrics *[]Metric, timestamp int64, tags []MetricTag) {
	// This will analyze profiler data if profiling is enabled
	// For now, we'll implement a basic version that checks if profiler is enabled

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check each database for profiler status
	databases, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return
	}

	var profiledDatabases int64 = 0
	for _, dbName := range databases {
		if dbName == "admin" || dbName == "local" || dbName == "config" {
			continue
		}

		db := client.Database(dbName)
		var profileStatus bson.M
		err := db.RunCommand(ctx, bson.D{{Key: "profile", Value: -1}}).Decode(&profileStatus)
		if err == nil {
			if level, ok := profileStatus["was"].(int32); ok && level > 0 {
				profiledDatabases++
			}
		}
	}

	// Profiler enabled databases count
	*metrics = append(*metrics, Metric{
		Name:        "mongodb.performance.profiler_enabled_dbs",
		Value:       MetricValue{IntValue: &profiledDatabases},
		Tags:        tags,
		Timestamp:   timestamp,
		Unit:        "count",
		Description: "Number of databases with profiler enabled",
	})
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

	// Oplog metrics - explicitly use local database
	var oplogStats bson.M
	localDB := client.Database("local")

	// Verify local database connection and run collStats on oplog.rs
	log.Printf("DEBUG: MongoDB - Collecting oplog stats from local database")
	err = localDB.RunCommand(ctx, bson.D{{Key: "collStats", Value: "oplog.rs"}}).Decode(&oplogStats)
	if err != nil {
		log.Printf("ERROR: MongoDB - Failed to get oplog stats: %v", err)
	} else {
		log.Printf("DEBUG: MongoDB - Oplog stats collected successfully")

		// Log all available oplog stats fields for debugging
		log.Printf("DEBUG: MongoDB - Oplog stats keys: %v", func() []string {
			var keys []string
			for k := range oplogStats {
				keys = append(keys, k)
			}
			return keys
		}())

		// Check if size field exists and log its type and value
		if sizeVal, exists := oplogStats["size"]; exists {
			log.Printf("DEBUG: MongoDB - Oplog size field found: type=%T, value=%v", sizeVal, sizeVal)

			// Try different numeric types for size
			var oplogSizeMB int64
			var sizeProcessed bool

			if size, ok := sizeVal.(int64); ok {
				oplogSizeMB = size / (1024 * 1024)
				sizeProcessed = true
				log.Printf("DEBUG: MongoDB - Oplog size (int64): %d bytes = %d MB", size, oplogSizeMB)
			} else if size, ok := sizeVal.(int32); ok {
				oplogSizeMB = int64(size) / (1024 * 1024)
				sizeProcessed = true
				log.Printf("DEBUG: MongoDB - Oplog size (int32): %d bytes = %d MB", size, oplogSizeMB)
			} else if size, ok := sizeVal.(float64); ok {
				oplogSizeMB = int64(size) / (1024 * 1024)
				sizeProcessed = true
				log.Printf("DEBUG: MongoDB - Oplog size (float64): %.0f bytes = %d MB", size, oplogSizeMB)
			}

			if sizeProcessed {
				metrics = append(metrics, Metric{
					Name:        "mongodb.replication.oplog_size_mb",
					Value:       MetricValue{IntValue: &oplogSizeMB},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
					Timestamp:   timestamp,
					Unit:        "megabytes",
					Description: "Oplog size in MB",
				})
				log.Printf("DEBUG: MongoDB - Added oplog_size_mb metric: %d MB", oplogSizeMB)
			} else {
				log.Printf("ERROR: MongoDB - Oplog size field has unsupported type: %T", sizeVal)
			}
		} else {
			log.Printf("ERROR: MongoDB - Oplog size field not found in stats")
		}

		// Check if storageSize field exists and log its type and value
		if storageSizeVal, exists := oplogStats["storageSize"]; exists {
			log.Printf("DEBUG: MongoDB - Oplog storageSize field found: type=%T, value=%v", storageSizeVal, storageSizeVal)

			// Try different numeric types for storageSize
			var oplogStorageMB int64
			var storageProcessed bool

			if storageSize, ok := storageSizeVal.(int64); ok {
				oplogStorageMB = storageSize / (1024 * 1024)
				storageProcessed = true
				log.Printf("DEBUG: MongoDB - Oplog storageSize (int64): %d bytes = %d MB", storageSize, oplogStorageMB)
			} else if storageSize, ok := storageSizeVal.(int32); ok {
				oplogStorageMB = int64(storageSize) / (1024 * 1024)
				storageProcessed = true
				log.Printf("DEBUG: MongoDB - Oplog storageSize (int32): %d bytes = %d MB", storageSize, oplogStorageMB)
			} else if storageSize, ok := storageSizeVal.(float64); ok {
				oplogStorageMB = int64(storageSize) / (1024 * 1024)
				storageProcessed = true
				log.Printf("DEBUG: MongoDB - Oplog storageSize (float64): %.0f bytes = %d MB", storageSize, oplogStorageMB)
			}

			if storageProcessed {
				metrics = append(metrics, Metric{
					Name:        "mongodb.replication.oplog_storage_mb",
					Value:       MetricValue{IntValue: &oplogStorageMB},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
					Timestamp:   timestamp,
					Unit:        "megabytes",
					Description: "Oplog storage size in MB",
				})
				log.Printf("DEBUG: MongoDB - Added oplog_storage_mb metric: %d MB", oplogStorageMB)
			} else {
				log.Printf("ERROR: MongoDB - Oplog storageSize field has unsupported type: %T", storageSizeVal)
			}
		} else {
			log.Printf("ERROR: MongoDB - Oplog storageSize field not found in stats")
		}

		// Add additional oplog metrics from your output
		if count, exists := oplogStats["count"]; exists {
			if countVal, ok := count.(int64); ok {
				metrics = append(metrics, Metric{
					Name:        "mongodb.replication.oplog_count",
					Value:       MetricValue{IntValue: &countVal},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Number of documents in oplog",
				})
				log.Printf("DEBUG: MongoDB - Added oplog_count metric: %d", countVal)
			} else if countVal, ok := count.(int32); ok {
				countInt64 := int64(countVal)
				metrics = append(metrics, Metric{
					Name:        "mongodb.replication.oplog_count",
					Value:       MetricValue{IntValue: &countInt64},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
					Timestamp:   timestamp,
					Unit:        "count",
					Description: "Number of documents in oplog",
				})
				log.Printf("DEBUG: MongoDB - Added oplog_count metric: %d", countInt64)
			}
		}

		// Add maxSize metric (capped collection max size)
		if maxSize, exists := oplogStats["maxSize"]; exists {
			log.Printf("DEBUG: MongoDB - Oplog maxSize field found: type=%T, value=%v", maxSize, maxSize)

			var maxSizeMB int64
			var maxSizeProcessed bool

			if size, ok := maxSize.(int64); ok {
				maxSizeMB = size / (1024 * 1024)
				maxSizeProcessed = true
			} else if size, ok := maxSize.(int32); ok {
				maxSizeMB = int64(size) / (1024 * 1024)
				maxSizeProcessed = true
			} else if size, ok := maxSize.(float64); ok {
				maxSizeMB = int64(size) / (1024 * 1024)
				maxSizeProcessed = true
			}

			if maxSizeProcessed {
				metrics = append(metrics, Metric{
					Name:        "mongodb.replication.oplog_max_size_mb",
					Value:       MetricValue{IntValue: &maxSizeMB},
					Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
					Timestamp:   timestamp,
					Unit:        "megabytes",
					Description: "Oplog maximum size in MB",
				})
				log.Printf("DEBUG: MongoDB - Added oplog_max_size_mb metric: %d MB", maxSizeMB)
			}
		}

		// Calculate oplog utilization percentage
		if sizeVal, sizeExists := oplogStats["size"]; sizeExists {
			if maxSizeVal, maxExists := oplogStats["maxSize"]; maxExists {
				var size, maxSize int64
				var utilizationCalculated bool

				// Get size value
				if s, ok := sizeVal.(int64); ok {
					size = s
				} else if s, ok := sizeVal.(int32); ok {
					size = int64(s)
				} else if s, ok := sizeVal.(float64); ok {
					size = int64(s)
				}

				// Get maxSize value
				if ms, ok := maxSizeVal.(int64); ok {
					maxSize = ms
				} else if ms, ok := maxSizeVal.(int32); ok {
					maxSize = int64(ms)
				} else if ms, ok := maxSizeVal.(float64); ok {
					maxSize = int64(ms)
				}

				if size > 0 && maxSize > 0 {
					utilization := (float64(size) / float64(maxSize)) * 100.0
					metrics = append(metrics, Metric{
						Name:        "mongodb.replication.oplog_utilization_percent",
						Value:       MetricValue{DoubleValue: &utilization},
						Tags:        []MetricTag{{Key: "host", Value: m.getHostname()}, {Key: "replica_set", Value: replicaSetName}},
						Timestamp:   timestamp,
						Unit:        "percent",
						Description: "Oplog utilization percentage",
					})
					log.Printf("DEBUG: MongoDB - Added oplog_utilization_percent metric: %.2f%%", utilization)
					utilizationCalculated = true
				}

				if !utilizationCalculated {
					log.Printf("ERROR: MongoDB - Could not calculate oplog utilization: size=%d, maxSize=%d", size, maxSize)
				}
			}
		}

		log.Printf("DEBUG: MongoDB - Total oplog metrics added: checking final metrics count")
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
	// Linux sistemlerde /proc/stat kullan
	if _, err := os.Stat("/proc/stat"); err == nil {
		// İlk ölçüm
		cpu1, err := c.readCPUStat()
		if err != nil {
			log.Printf("MongoDB: İlk CPU ölçümü hatası: %v", err)
			goto AlternativeMethod
		}

		// 500ms bekle (daha uzun süre ile daha doğru ölçüm)
		time.Sleep(500 * time.Millisecond)

		// İkinci ölçüm
		cpu2, err := c.readCPUStat()
		if err != nil {
			log.Printf("MongoDB: İkinci CPU ölçümü hatası: %v", err)
			goto AlternativeMethod
		}

		// Değişimleri hesapla
		userDiff := cpu2.user - cpu1.user
		niceDiff := cpu2.nice - cpu1.nice
		systemDiff := cpu2.system - cpu1.system
		idleDiff := cpu2.idle - cpu1.idle
		iowaitDiff := cpu2.iowait - cpu1.iowait
		irqDiff := cpu2.irq - cpu1.irq
		softirqDiff := cpu2.softirq - cpu1.softirq
		stealDiff := cpu2.steal - cpu1.steal
		totalDiff := cpu2.total - cpu1.total

		log.Printf("DEBUG: MongoDB CPU farkları - User: %d, System: %d, Idle: %d, IOWait: %d, Total: %d",
			userDiff, systemDiff, idleDiff, iowaitDiff, totalDiff)

		if totalDiff == 0 {
			log.Printf("DEBUG: MongoDB - Total diff 0, alternatif yönteme geçiliyor")
			goto AlternativeMethod
		}

		// CPU kullanımını hesapla (user + nice + system + irq + softirq + steal)
		activeDiff := userDiff + niceDiff + systemDiff + irqDiff + softirqDiff + stealDiff
		cpuUsage := (float64(activeDiff) / float64(totalDiff)) * 100

		// Geçerlilik kontrolü
		if cpuUsage < 0 || cpuUsage > 100 {
			log.Printf("DEBUG: MongoDB - Geçersiz CPU kullanımı (%f), alternatif yönteme geçiliyor", cpuUsage)
			goto AlternativeMethod
		}

		log.Printf("DEBUG: MongoDB - Hesaplanan CPU kullanımı: %.2f%%", cpuUsage)
		return cpuUsage, nil
	}

AlternativeMethod:
	// Alternatif yöntem - mpstat kullan (daha doğru sonuçlar için)
	cmd := exec.Command("mpstat", "1", "1")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("DEBUG: MongoDB - mpstat komutu hatası: %v, top deneniyor", err)
		// top dene
		cmd = exec.Command("sh", "-c", "top -bn2 -d 0.5 | grep '^%Cpu' | tail -1 | awk '{print 100-$8}'")
		out, err = cmd.Output()
		if err != nil {
			log.Printf("DEBUG: MongoDB - top komutu hatası: %v, vmstat deneniyor", err)
			// vmstat dene
			cmd = exec.Command("sh", "-c", "vmstat 1 2 | tail -1 | awk '{print 100-$15}'")
			out, err = cmd.Output()
			if err != nil {
				log.Printf("DEBUG: MongoDB - vmstat komutu hatası: %v", err)
				return 0, err
			}
		}
	}

	cpuPercent, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		log.Printf("DEBUG: MongoDB - CPU yüzdesi parse hatası: %v", err)
		return 0, err
	}

	// Geçerlilik kontrolü
	if cpuPercent < 0 {
		cpuPercent = 0
	} else if cpuPercent > 100 {
		cpuPercent = 100
	}

	log.Printf("DEBUG: MongoDB - Alternatif yöntem CPU kullanımı: %.2f%%", cpuPercent)
	return cpuPercent, nil
}

func (c *MongoCollector) getRAMUsage() (map[string]interface{}, error) {
	// Linux sistemlerde /proc/meminfo dosyasını kullan
	if _, err := os.Stat("/proc/meminfo"); err == nil {
		content, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			log.Printf("DEBUG: MongoDB - /proc/meminfo okunamadı: %v", err)
			goto AlternativeMethod
		}

		lines := strings.Split(string(content), "\n")
		var totalMem, freeMem, availableMem, buffers, cached uint64

		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}

			key := strings.TrimSuffix(fields[0], ":")
			valueStr := fields[1]
			value, err := strconv.ParseUint(valueStr, 10, 64)
			if err != nil {
				continue
			}

			// /proc/meminfo değerleri KB cinsinden
			switch key {
			case "MemTotal":
				totalMem = value
			case "MemFree":
				freeMem = value
			case "MemAvailable":
				availableMem = value
			case "Buffers":
				buffers = value
			case "Cached":
				cached = value
			}
		}

		if totalMem == 0 {
			log.Printf("DEBUG: MongoDB - Toplam bellek 0, alternatif yöntem denenecek")
			goto AlternativeMethod
		}

		// Kullanılan belleği hesapla
		// MemAvailable varsa onu kullan, yoksa basit hesaplama
		var usedMem uint64
		if availableMem > 0 {
			usedMem = totalMem - availableMem
		} else {
			// Basit hesaplama: Total - Free - Buffers - Cached
			usedMem = totalMem - freeMem - buffers - cached
		}

		// KB'den MB'ye çevir
		totalMemMB := int64(totalMem / 1024)
		usedMemMB := int64(usedMem / 1024)
		freeMemMB := int64((totalMem - usedMem) / 1024)

		// Kullanım yüzdesini hesapla
		usagePercent := (float64(usedMem) / float64(totalMem)) * 100

		log.Printf("DEBUG: MongoDB RAM - Total: %dMB, Used: %dMB, Free: %dMB, Usage: %.2f%%",
			totalMemMB, usedMemMB, freeMemMB, usagePercent)

		return map[string]interface{}{
			"total_mb":      totalMemMB,
			"used_mb":       usedMemMB,
			"free_mb":       freeMemMB,
			"usage_percent": usagePercent,
		}, nil
	}

AlternativeMethod:
	// Alternatif yöntem - free komutu
	var totalMB, usedMB, freeMB int64
	var err1, err2, err3 error
	var usagePercent float64

	cmd := exec.Command("sh", "-c", "free -m | grep '^Mem'")
	out, err := cmd.Output()
	if err != nil {
		log.Printf("DEBUG: MongoDB - free komutu hatası: %v", err)
		// Varsayılan değerler döndür
		return map[string]interface{}{
			"usage_percent": 60.0,
			"free_mb":       int64(4096),
			"total_mb":      int64(8192),
			"used_mb":       int64(4096),
		}, nil
	}

	// free çıktısını parse et
	// Mem:           7862    2945    2174    1367    2742    3549
	fields := strings.Fields(string(out))
	if len(fields) < 7 {
		log.Printf("DEBUG: MongoDB - free komutu çıktısı geçersiz: %s", string(out))
		goto DefaultValues
	}

	totalMB, err1 = strconv.ParseInt(fields[1], 10, 64)
	usedMB, err2 = strconv.ParseInt(fields[2], 10, 64)
	freeMB, err3 = strconv.ParseInt(fields[3], 10, 64)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Printf("DEBUG: MongoDB - free çıktısı parse edilemedi")
		goto DefaultValues
	}

	if totalMB <= 0 {
		goto DefaultValues
	}

	usagePercent = (float64(usedMB) / float64(totalMB)) * 100

	log.Printf("DEBUG: MongoDB RAM (free cmd) - Total: %dMB, Used: %dMB, Free: %dMB, Usage: %.2f%%",
		totalMB, usedMB, freeMB, usagePercent)

	return map[string]interface{}{
		"total_mb":      totalMB,
		"used_mb":       usedMB,
		"free_mb":       freeMB,
		"usage_percent": usagePercent,
	}, nil

DefaultValues:
	// Varsayılan değerler
	log.Printf("DEBUG: MongoDB - RAM bilgisi alınamadı, varsayılan değerler kullanılıyor")
	return map[string]interface{}{
		"usage_percent": 60.0,
		"free_mb":       int64(4096),
		"total_mb":      int64(8192),
		"used_mb":       int64(4096),
	}, nil
}

func (c *MongoCollector) getDiskUsage() (map[string]interface{}, error) {
	log.Printf("DEBUG: MongoDB mongo_metrics.go - getDiskUsage() called")

	// Use the existing GetDiskUsage method which provides real disk information
	totalDiskStr, freeDiskStr, usagePercent := c.GetDiskUsage()

	log.Printf("DEBUG: MongoDB mongo_metrics.go - GetDiskUsage() returned: total=%s, free=%s, usage=%d%%",
		totalDiskStr, freeDiskStr, usagePercent)

	if freeDiskStr == "N/A" || totalDiskStr == "N/A" {
		return map[string]interface{}{
			"total_gb": int64(0),
			"avail_gb": int64(0),
		}, nil
	}

	// Parse total disk size
	totalDiskBytes, err := c.convertToBytes(totalDiskStr)
	if err != nil {
		log.Printf("DEBUG: MongoDB getDiskUsage - Failed to parse total disk: %v", err)
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

	// Convert to GB
	totalDiskGB := int64(totalDiskBytes / (1024 * 1024 * 1024))
	freeDiskGB := int64(freeDiskBytes / (1024 * 1024 * 1024))

	return map[string]interface{}{
		"total_gb": totalDiskGB,
		"avail_gb": freeDiskGB,
	}, nil
}

// cpuStat represents CPU statistics from /proc/stat
type cpuStat struct {
	user    uint64
	nice    uint64
	system  uint64
	idle    uint64
	iowait  uint64
	irq     uint64
	softirq uint64
	steal   uint64
	total   uint64
}

// readCPUStat reads CPU statistics from /proc/stat
func (c *MongoCollector) readCPUStat() (*cpuStat, error) {
	contents, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "cpu" {
			// En az 8 alan olmalı (cpu, user, nice, system, idle, iowait, irq, softirq)
			if len(fields) < 8 {
				return nil, fmt.Errorf("yetersiz CPU stat alanı")
			}

			stat := &cpuStat{}

			// Değerleri parse et
			values := make([]uint64, len(fields)-1)
			for i := 1; i < len(fields); i++ {
				val, err := strconv.ParseUint(fields[i], 10, 64)
				if err != nil {
					log.Printf("MongoDB: CPU stat parse hatası [%d]: %v", i, err)
					continue
				}
				values[i-1] = val
			}

			// Değerleri ata
			stat.user = values[0]
			stat.nice = values[1]
			stat.system = values[2]
			stat.idle = values[3]
			stat.iowait = values[4]
			stat.irq = values[5]
			stat.softirq = values[6]
			if len(values) > 7 {
				stat.steal = values[7]
			}

			// Toplam CPU zamanını hesapla
			stat.total = stat.user + stat.nice + stat.system + stat.idle +
				stat.iowait + stat.irq + stat.softirq + stat.steal

			log.Printf("DEBUG: MongoDB CPU stat detaylı - User: %d, System: %d, Idle: %d, IOWait: %d, Total: %d",
				stat.user, stat.system, stat.idle, stat.iowait, stat.total)
			return stat, nil
		}
	}
	return nil, fmt.Errorf("CPU stats not found in /proc/stat")
}
