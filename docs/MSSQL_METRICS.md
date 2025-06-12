# MSSQL Metrics Collection System

This document describes the MSSQL metrics collection system that gathers time-series data for analysis in InfluxDB.

## Overview

The MSSQL metrics collection system provides comprehensive monitoring of SQL Server instances, collecting both system-level and database-specific metrics that can be analyzed for performance optimization, capacity planning, and troubleshooting.

## Architecture

The system consists of several components:

1. **MSSQLMetricsCollector** - Core collector that gathers metrics from SQL Server
2. **MetricsSender** - Handles conversion to protobuf and sending to the server
3. **Reporter Integration** - Automatic periodic collection when platform is MSSQL

## Collected Metrics

### System Metrics (`mssql_system`)

- **CPU Metrics**
  - `mssql.system.cpu_usage` - CPU usage percentage
  - `mssql.system.cpu_cores` - Number of CPU cores

- **Memory Metrics**
  - `mssql.system.memory_usage` - Memory usage percentage
  - `mssql.system.total_memory` - Total system memory (bytes)
  - `mssql.system.free_memory` - Free system memory (bytes)

- **Disk Metrics**
  - `mssql.system.total_disk` - Total disk space (bytes)
  - `mssql.system.free_disk` - Free disk space (bytes)

### Database Metrics (`mssql_database`)

- **Connection Metrics**
  - `mssql.connections.total` - Total number of connections
  - `mssql.connections.active` - Number of active connections
  - `mssql.connections.idle` - Number of idle connections

- **Performance Counters**
  - `mssql.performance.batch_requests_per_sec` - Batch requests per second
  - `mssql.performance.compilations_per_sec` - SQL compilations per second
  - `mssql.performance.recompilations_per_sec` - SQL re-compilations per second
  - `mssql.performance.buffer_cache_hit_ratio` - Buffer cache hit ratio
  - `mssql.performance.page_life_expectancy` - Page life expectancy
  - `mssql.performance.lazy_writes_per_sec` - Lazy writes per second
  - `mssql.performance.page_reads_per_sec` - Page reads per second
  - `mssql.performance.page_writes_per_sec` - Page writes per second
  - `mssql.performance.lock_requests_per_sec` - Lock requests per second
  - `mssql.performance.lock_timeouts_per_sec` - Lock timeouts per second
  - `mssql.performance.lock_waits_per_sec` - Lock waits per second

- **Memory Management**
  - `mssql.memory.target_server_memory` - Target server memory (KB)
  - `mssql.memory.total_server_memory` - Total server memory (KB)

- **Blocking and Deadlocks**
  - `mssql.blocking.sessions` - Number of blocked sessions
  - `mssql.deadlocks.total` - Total number of deadlocks

- **Wait Statistics**
  - `mssql.waits.tasks` - Number of waiting tasks (by wait type)
  - `mssql.waits.time_ms` - Total wait time in milliseconds (by wait type)

- **Database Sizes**
  - `mssql.database.data_size` - Database data file size (bytes)
  - `mssql.database.log_size` - Database log file size (bytes)

- **Transaction Metrics**
  - `mssql.transactions.per_sec` - Transactions per second (by database)
  - `mssql.transactions.active_total` - Total number of active transactions
  - `mssql.transactions.active_by_database` - Number of active transactions by database
  - `mssql.transactions.long_running` - Number of long-running transactions (>30 seconds)
  - `mssql.transactions.max_duration_seconds` - Maximum transaction duration in seconds
  - `mssql.transactions.snapshot_active` - Number of active snapshot isolation transactions
  - `mssql.transactions.nonsnapshot_version_active` - Number of active non-snapshot version transactions
  - `mssql.transactions.update_snapshot_active` - Number of active update snapshot transactions
  - `mssql.transactions.longest_running_time_seconds` - Longest transaction running time in seconds
  - `mssql.transactions.update_conflict_ratio` - Update conflict ratio for snapshot isolation
  - `mssql.transactions.version_cleanup_rate_kb_per_sec` - Version cleanup rate (KB/s)
  - `mssql.transactions.version_generation_rate_kb_per_sec` - Version generation rate (KB/s)
  - `mssql.transactions.version_store_size_kb` - Version store size (KB)
  - `mssql.transactions.version_store_unit_count` - Version store unit count
  - `mssql.transactions.tempdb_free_space_kb` - Free space in tempdb (KB)
  - `mssql.transactions.query_executions` - Total query executions by database

## Metric Structure

Each metric includes:

- **Name** - Hierarchical metric name (e.g., `mssql.system.cpu_usage`)
- **Value** - Metric value (double, int64, string, or boolean)
- **Tags** - Key-value pairs for filtering and grouping (e.g., host, database, wait_type)
- **Timestamp** - Unix timestamp in nanoseconds
- **Unit** - Measurement unit (percent, bytes, count, per_second, etc.)
- **Description** - Human-readable description

## Agent Identification

The system uses a consistent agent ID format throughout:

- **Format**: `agent_<hostname>`
- **Example**: `agent_sql-server-01`
- **Purpose**: Uniquely identifies the agent instance sending metrics

This ensures proper correlation with other agent data and consistent identification across all ClusterEye components.

## Configuration

The metrics collection system uses the existing MSSQL configuration from `agent.yml`:

```yaml
mssql:
  host: "localhost"
  port: "1433"
  user: "sa"
  pass: "password"
  database: "master"
  instance: ""
  trust_cert: true
  windows_auth: false
```

## Usage

### Automatic Collection

When the agent is registered with platform "mssql", metrics collection starts automatically:

- Collects metrics every 60 seconds by default
- Sends both system and database metrics
- Handles connection failures gracefully

### Manual Collection

You can also collect metrics manually:

```go
// Create metrics collector
collector := mssql.NewMSSQLMetricsCollector(cfg)

// Collect system metrics
systemBatch, err := collector.CollectSystemMetrics()

// Collect database metrics
dbBatch, err := collector.CollectDatabaseMetrics()

// Collect all metrics
allBatches, err := collector.CollectAllMetrics()
```

### Sending Metrics

```go
// Create metrics sender
sender := NewMetricsSender(cfg, reporter)

// Send MSSQL metrics
err := sender.SendMSSQLMetrics(ctx)

// Start periodic collection
sender.StartPeriodicMetricsCollection(60 * time.Second)
```

## Protobuf Schema

The metrics use the following protobuf schema:

```protobuf
message MetricValue {
  oneof value {
    double double_value = 1;
    int64 int_value = 2;
    string string_value = 3;
    bool bool_value = 4;
  }
}

message MetricTag {
  string key = 1;
  string value = 2;
}

message Metric {
  string name = 1;
  MetricValue value = 2;
  repeated MetricTag tags = 3;
  int64 timestamp = 4;
  string unit = 5;
  string description = 6;
}

message MetricBatch {
  string agent_id = 1;                // Format: "agent_<hostname>"
  string metric_type = 2;             // "mssql_system" or "mssql_database"
  repeated Metric metrics = 3;        // List of metrics
  int64 collection_timestamp = 4;     // Unix timestamp in nanoseconds
  map<string, string> metadata = 5;   // Additional metadata (platform, os, etc.)
}
```

## InfluxDB Integration

The metrics are designed to work seamlessly with InfluxDB:

- **Measurement** - Derived from metric name (e.g., `mssql_system_cpu_usage`)
- **Tags** - Used for indexing and filtering
- **Fields** - Metric values
- **Timestamp** - Time-based indexing

Example InfluxDB queries:

```sql
-- CPU usage over time
SELECT mean("value") FROM "mssql_system_cpu_usage" 
WHERE time >= now() - 1h GROUP BY time(5m)

-- Top wait types
SELECT sum("value") FROM "mssql_waits_time_ms" 
WHERE time >= now() - 1h GROUP BY "wait_type"

-- Database growth
SELECT last("value") FROM "mssql_database_data_size" 
GROUP BY "database"

-- Transaction throughput (TPS) by database
SELECT mean("value") FROM "mssql_transactions_per_sec" 
WHERE time >= now() - 1h GROUP BY time(5m), "database"

-- Long-running transactions alert
SELECT last("value") FROM "mssql_transactions_long_running" 
WHERE time >= now() - 5m AND "value" > 0

-- Active transactions by database
SELECT last("value") FROM "mssql_transactions_active_by_database" 
GROUP BY "database" ORDER BY "value" DESC
```

## Error Handling

The system includes robust error handling:

- Connection failures are logged but don't stop collection
- Individual metric failures don't affect other metrics
- Automatic reconnection for database connections
- Graceful degradation when SQL Server features are unavailable

## Performance Considerations

- Metrics collection is designed to have minimal impact on SQL Server
- Connection pooling limits concurrent connections
- Queries are optimized for performance
- Collection frequency can be adjusted based on requirements

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Check SQL Server connectivity
   - Verify credentials and permissions
   - Ensure SQL Server is running

2. **Missing Metrics**
   - Check SQL Server version compatibility
   - Verify user permissions for system views
   - Review agent logs for specific errors

3. **Performance Impact**
   - Monitor SQL Server performance during collection
   - Adjust collection frequency if needed
   - Review query execution plans

### Logging

Enable debug logging to troubleshoot issues:

```go
log.Printf("Collecting MSSQL metrics...")
log.Printf("Collected %d metrics", len(batch.Metrics))
```

## Future Enhancements

Planned improvements include:

- Additional performance counters
- Custom metric definitions
- Alerting based on metric thresholds
- Historical trend analysis
- Automated performance recommendations 