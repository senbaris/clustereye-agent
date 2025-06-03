package reporter

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector/mssql"
	"github.com/senbaris/clustereye-agent/internal/collector/postgres"
	"github.com/senbaris/clustereye-agent/internal/config"
)

// MetricsSender handles sending metrics to the server
type MetricsSender struct {
	cfg                 *config.AgentConfig
	reporter            *Reporter
	mssqlCollector      *mssql.MSSQLMetricsCollector
	postgresqlCollector *postgres.PostgreSQLMetricsCollector
}

// NewMetricsSender creates a new metrics sender
func NewMetricsSender(cfg *config.AgentConfig, reporter *Reporter) *MetricsSender {
	return &MetricsSender{
		cfg:                 cfg,
		reporter:            reporter,
		mssqlCollector:      mssql.NewMSSQLMetricsCollector(cfg),
		postgresqlCollector: postgres.NewPostgreSQLMetricsCollector(cfg),
	}
}

// getAgentID returns the proper agent ID in the format "agent_<hostname>"
func (ms *MetricsSender) getAgentID() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	return "agent_" + hostname
}

// SendMSSQLMetrics collects and sends MSSQL metrics
func (ms *MetricsSender) SendMSSQLMetrics(ctx context.Context) error {
	log.Printf("DEBUG: SendMSSQLMetrics - Starting metrics collection and sending process")

	// Collect all MSSQL metrics
	metricBatches, err := ms.mssqlCollector.CollectAllMetrics()
	if err != nil {
		log.Printf("DEBUG: SendMSSQLMetrics - Failed to collect metrics: %v", err)
		return fmt.Errorf("failed to collect MSSQL metrics: %w", err)
	}

	log.Printf("DEBUG: SendMSSQLMetrics - Collected %d metric batches", len(metricBatches))

	// Send each batch
	for i, batch := range metricBatches {
		log.Printf("DEBUG: SendMSSQLMetrics - Processing batch %d: Type=%s, AgentID=%s, Metrics=%d",
			i+1, batch.MetricType, batch.AgentID, len(batch.Metrics))

		// Debug log each metric in the batch
		for j, metric := range batch.Metrics {
			log.Printf("DEBUG: Batch[%d] Metric[%d]: Name=%s, Unit=%s, Timestamp=%d",
				i+1, j+1, metric.Name, metric.Unit, metric.Timestamp)

			// Log metric value based on type
			if metric.Value.DoubleValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %.2f (double)", i+1, j+1, *metric.Value.DoubleValue)
			} else if metric.Value.IntValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %d (int)", i+1, j+1, *metric.Value.IntValue)
			} else if metric.Value.StringValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %s (string)", i+1, j+1, *metric.Value.StringValue)
			} else if metric.Value.BoolValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %t (bool)", i+1, j+1, *metric.Value.BoolValue)
			} else {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: null or unknown type", i+1, j+1)
			}
		}

		if err := ms.sendMetricBatch(ctx, batch); err != nil {
			log.Printf("DEBUG: SendMSSQLMetrics - Failed to send batch %d (%s): %v", i+1, batch.MetricType, err)
			log.Printf("Failed to send metric batch %s: %v", batch.MetricType, err)
			continue
		}
		log.Printf("DEBUG: SendMSSQLMetrics - Successfully sent batch %d (%s) with %d metrics", i+1, batch.MetricType, len(batch.Metrics))
		log.Printf("Successfully sent %d metrics for type %s", len(batch.Metrics), batch.MetricType)
	}

	log.Printf("DEBUG: SendMSSQLMetrics - Completed processing all metric batches")
	return nil
}

// sendMetricBatch converts internal metric batch to protobuf and sends it
func (ms *MetricsSender) sendMetricBatch(ctx context.Context, batch *mssql.MetricBatch) error {
	log.Printf("DEBUG: sendMetricBatch - Starting to send batch: Type=%s, Metrics=%d", batch.MetricType, len(batch.Metrics))

	// Convert internal metrics to protobuf
	pbMetrics := make([]*pb.Metric, 0, len(batch.Metrics))

	for i, metric := range batch.Metrics {
		log.Printf("DEBUG: sendMetricBatch - Converting metric %d: %s", i+1, metric.Name)

		pbMetric := &pb.Metric{
			Name:        metric.Name,
			Timestamp:   metric.Timestamp,
			Unit:        metric.Unit,
			Description: metric.Description,
		}

		// Convert metric value
		pbMetric.Value = ms.convertMetricValue(metric.Value)

		// Convert tags
		pbMetric.Tags = make([]*pb.MetricTag, 0, len(metric.Tags))
		for _, tag := range metric.Tags {
			pbMetric.Tags = append(pbMetric.Tags, &pb.MetricTag{
				Key:   tag.Key,
				Value: tag.Value,
			})
		}

		pbMetrics = append(pbMetrics, pbMetric)
	}

	log.Printf("DEBUG: sendMetricBatch - Converted %d metrics to protobuf", len(pbMetrics))

	// Create metric batch
	pbBatch := &pb.MetricBatch{
		AgentId:             batch.AgentID,
		MetricType:          batch.MetricType,
		Metrics:             pbMetrics,
		CollectionTimestamp: batch.CollectionTimestamp,
		Metadata:            batch.Metadata,
	}

	log.Printf("DEBUG: sendMetricBatch - Created protobuf batch: AgentID=%s, Type=%s, Timestamp=%d, Metadata=%v",
		pbBatch.AgentId, pbBatch.MetricType, pbBatch.CollectionTimestamp, pbBatch.Metadata)

	// Create request
	request := &pb.SendMetricsRequest{
		Batch: pbBatch,
	}

	log.Printf("DEBUG: sendMetricBatch - Created gRPC request, preparing to send")

	// Send metrics via gRPC
	if ms.reporter.grpcClient == nil {
		log.Printf("DEBUG: sendMetricBatch - ERROR: gRPC client not available")
		return fmt.Errorf("gRPC client not available")
	}

	client := pb.NewAgentServiceClient(ms.reporter.grpcClient)
	log.Printf("DEBUG: sendMetricBatch - Created gRPC client, sending request")

	// Set timeout for the request
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	response, err := client.SendMetrics(ctx, request)
	if err != nil {
		log.Printf("DEBUG: sendMetricBatch - ERROR: gRPC SendMetrics call failed: %v", err)
		return fmt.Errorf("failed to send metrics: %w", err)
	}

	log.Printf("DEBUG: sendMetricBatch - Received response from server: Status=%s, Message=%s, ProcessedCount=%d",
		response.Status, response.Message, response.ProcessedCount)

	if response.Status != "success" {
		log.Printf("DEBUG: sendMetricBatch - ERROR: Server returned non-success status: %s - %s", response.Status, response.Message)
		return fmt.Errorf("server returned error: %s - %s", response.Status, response.Message)
	}

	log.Printf("Metrics sent successfully: %d processed, status: %s", response.ProcessedCount, response.Message)

	// Log any errors from server
	if len(response.Errors) > 0 {
		log.Printf("DEBUG: sendMetricBatch - Server returned %d errors:", len(response.Errors))
		for i, errMsg := range response.Errors {
			log.Printf("DEBUG: sendMetricBatch - Server error %d: %s", i+1, errMsg)
			log.Printf("Server metric processing error: %s", errMsg)
		}
	} else {
		log.Printf("DEBUG: sendMetricBatch - No errors from server")
	}

	log.Printf("DEBUG: sendMetricBatch - Successfully completed batch sending")
	return nil
}

// convertMetricValue converts internal MetricValue to protobuf MetricValue
func (ms *MetricsSender) convertMetricValue(value mssql.MetricValue) *pb.MetricValue {
	pbValue := &pb.MetricValue{}

	if value.DoubleValue != nil {
		pbValue.Value = &pb.MetricValue_DoubleValue{
			DoubleValue: *value.DoubleValue,
		}
	} else if value.IntValue != nil {
		pbValue.Value = &pb.MetricValue_IntValue{
			IntValue: *value.IntValue,
		}
	} else if value.StringValue != nil {
		pbValue.Value = &pb.MetricValue_StringValue{
			StringValue: *value.StringValue,
		}
	} else if value.BoolValue != nil {
		pbValue.Value = &pb.MetricValue_BoolValue{
			BoolValue: *value.BoolValue,
		}
	}

	return pbValue
}

// SendPostgreSQLMetrics collects and sends PostgreSQL metrics
func (ms *MetricsSender) SendPostgreSQLMetrics(ctx context.Context) error {
	log.Printf("DEBUG: SendPostgreSQLMetrics - Starting metrics collection and sending process")

	// Collect all PostgreSQL metrics
	metricBatches, err := ms.postgresqlCollector.CollectAllMetrics()
	if err != nil {
		log.Printf("DEBUG: SendPostgreSQLMetrics - Failed to collect metrics: %v", err)
		return fmt.Errorf("failed to collect PostgreSQL metrics: %w", err)
	}

	log.Printf("DEBUG: SendPostgreSQLMetrics - Collected %d metric batches", len(metricBatches))

	// Send each batch
	for i, batch := range metricBatches {
		log.Printf("DEBUG: SendPostgreSQLMetrics - Processing batch %d: Type=%s, AgentID=%s, Metrics=%d",
			i+1, batch.MetricType, batch.AgentID, len(batch.Metrics))

		// Debug log each metric in the batch
		for j, metric := range batch.Metrics {
			log.Printf("DEBUG: Batch[%d] Metric[%d]: Name=%s, Unit=%s, Timestamp=%d",
				i+1, j+1, metric.Name, metric.Unit, metric.Timestamp)

			// Log metric value based on type
			if metric.Value.DoubleValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %.2f (double)", i+1, j+1, *metric.Value.DoubleValue)
			} else if metric.Value.IntValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %d (int)", i+1, j+1, *metric.Value.IntValue)
			} else if metric.Value.StringValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %s (string)", i+1, j+1, *metric.Value.StringValue)
			} else if metric.Value.BoolValue != nil {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: %t (bool)", i+1, j+1, *metric.Value.BoolValue)
			} else {
				log.Printf("DEBUG: Batch[%d] Metric[%d] Value: null or unknown type", i+1, j+1)
			}
		}

		if err := ms.sendPostgreSQLMetricBatch(ctx, batch); err != nil {
			log.Printf("DEBUG: SendPostgreSQLMetrics - Failed to send batch %d (%s): %v", i+1, batch.MetricType, err)
			log.Printf("Failed to send PostgreSQL metric batch %s: %v", batch.MetricType, err)
			continue
		}
		log.Printf("DEBUG: SendPostgreSQLMetrics - Successfully sent batch %d (%s) with %d metrics", i+1, batch.MetricType, len(batch.Metrics))
		log.Printf("Successfully sent %d PostgreSQL metrics for type %s", len(batch.Metrics), batch.MetricType)
	}

	log.Printf("DEBUG: SendPostgreSQLMetrics - Completed processing all metric batches")
	return nil
}

// sendPostgreSQLMetricBatch converts internal PostgreSQL metric batch to protobuf and sends it
func (ms *MetricsSender) sendPostgreSQLMetricBatch(ctx context.Context, batch *postgres.MetricBatch) error {
	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Starting to send batch: Type=%s, Metrics=%d", batch.MetricType, len(batch.Metrics))

	// Convert internal metrics to protobuf
	pbMetrics := make([]*pb.Metric, 0, len(batch.Metrics))

	for i, metric := range batch.Metrics {
		log.Printf("DEBUG: sendPostgreSQLMetricBatch - Converting metric %d: %s", i+1, metric.Name)

		pbMetric := &pb.Metric{
			Name:        metric.Name,
			Timestamp:   metric.Timestamp,
			Unit:        metric.Unit,
			Description: metric.Description,
		}

		// Convert metric value
		pbMetric.Value = ms.convertPostgreSQLMetricValue(metric.Value)

		// Convert tags
		pbMetric.Tags = make([]*pb.MetricTag, 0, len(metric.Tags))
		for _, tag := range metric.Tags {
			pbMetric.Tags = append(pbMetric.Tags, &pb.MetricTag{
				Key:   tag.Key,
				Value: tag.Value,
			})
		}

		pbMetrics = append(pbMetrics, pbMetric)
	}

	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Converted %d metrics to protobuf", len(pbMetrics))

	// Create metric batch
	pbBatch := &pb.MetricBatch{
		AgentId:             batch.AgentID,
		MetricType:          batch.MetricType,
		Metrics:             pbMetrics,
		CollectionTimestamp: batch.CollectionTimestamp,
		Metadata:            batch.Metadata,
	}

	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Created protobuf batch: AgentID=%s, Type=%s, Timestamp=%d, Metadata=%v",
		pbBatch.AgentId, pbBatch.MetricType, pbBatch.CollectionTimestamp, pbBatch.Metadata)

	// Create request
	request := &pb.SendMetricsRequest{
		Batch: pbBatch,
	}

	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Created gRPC request, preparing to send")

	// Send metrics via gRPC
	if ms.reporter.grpcClient == nil {
		log.Printf("DEBUG: sendPostgreSQLMetricBatch - ERROR: gRPC client not available")
		return fmt.Errorf("gRPC client not available")
	}

	client := pb.NewAgentServiceClient(ms.reporter.grpcClient)
	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Created gRPC client, sending request")

	// Set timeout for the request
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	response, err := client.SendMetrics(ctx, request)
	if err != nil {
		log.Printf("DEBUG: sendPostgreSQLMetricBatch - ERROR: gRPC SendMetrics call failed: %v", err)
		return fmt.Errorf("failed to send PostgreSQL metrics: %w", err)
	}

	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Received response from server: Status=%s, Message=%s, ProcessedCount=%d",
		response.Status, response.Message, response.ProcessedCount)

	if response.Status != "success" {
		log.Printf("DEBUG: sendPostgreSQLMetricBatch - ERROR: Server returned non-success status: %s - %s", response.Status, response.Message)
		return fmt.Errorf("server returned error: %s - %s", response.Status, response.Message)
	}

	log.Printf("PostgreSQL metrics sent successfully: %d processed, status: %s", response.ProcessedCount, response.Message)

	// Log any errors from server
	if len(response.Errors) > 0 {
		log.Printf("DEBUG: sendPostgreSQLMetricBatch - Server returned %d errors:", len(response.Errors))
		for i, errMsg := range response.Errors {
			log.Printf("DEBUG: sendPostgreSQLMetricBatch - Server error %d: %s", i+1, errMsg)
			log.Printf("Server PostgreSQL metric processing error: %s", errMsg)
		}
	} else {
		log.Printf("DEBUG: sendPostgreSQLMetricBatch - No errors from server")
	}

	log.Printf("DEBUG: sendPostgreSQLMetricBatch - Successfully completed batch sending")
	return nil
}

// convertPostgreSQLMetricValue converts internal PostgreSQL MetricValue to protobuf MetricValue
func (ms *MetricsSender) convertPostgreSQLMetricValue(value postgres.MetricValue) *pb.MetricValue {
	pbValue := &pb.MetricValue{}

	if value.DoubleValue != nil {
		pbValue.Value = &pb.MetricValue_DoubleValue{
			DoubleValue: *value.DoubleValue,
		}
	} else if value.IntValue != nil {
		pbValue.Value = &pb.MetricValue_IntValue{
			IntValue: *value.IntValue,
		}
	} else if value.StringValue != nil {
		pbValue.Value = &pb.MetricValue_StringValue{
			StringValue: *value.StringValue,
		}
	} else if value.BoolValue != nil {
		pbValue.Value = &pb.MetricValue_BoolValue{
			BoolValue: *value.BoolValue,
		}
	}

	return pbValue
}

// StartPeriodicMetricsCollection starts collecting and sending metrics periodically
func (ms *MetricsSender) StartPeriodicMetricsCollection(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

				if err := ms.SendMSSQLMetrics(ctx); err != nil {
					log.Printf("Failed to send periodic MSSQL metrics: %v", err)
				}

				cancel()
			}
		}
	}()

	log.Printf("Started periodic metrics collection with interval: %v", interval)
}

// StartPeriodicPostgreSQLMetricsCollection starts collecting and sending PostgreSQL metrics periodically
func (ms *MetricsSender) StartPeriodicPostgreSQLMetricsCollection(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

				if err := ms.SendPostgreSQLMetrics(ctx); err != nil {
					log.Printf("Failed to send periodic PostgreSQL metrics: %v", err)
				}

				cancel()
			}
		}
	}()

	log.Printf("Started periodic PostgreSQL metrics collection with interval: %v", interval)
}

// HandleCollectMetricsRequest handles server requests to collect specific metrics
func (ms *MetricsSender) HandleCollectMetricsRequest(ctx context.Context, req *pb.CollectMetricsRequest) (*pb.CollectMetricsResponse, error) {
	log.Printf("Received collect metrics request for agent %s, types: %v, interval: %d",
		req.AgentId, req.MetricTypes, req.IntervalSeconds)

	// Validate agent ID
	expectedAgentID := ms.getAgentID()
	if req.AgentId != expectedAgentID {
		return &pb.CollectMetricsResponse{
			Status:  "error",
			Message: fmt.Sprintf("Agent ID mismatch: expected %s, got %s", expectedAgentID, req.AgentId),
		}, nil
	}

	// Process each requested metric type
	for _, metricType := range req.MetricTypes {
		switch metricType {
		case "mssql", "mssql_system", "mssql_database":
			// Start collecting MSSQL metrics
			if req.IntervalSeconds > 0 {
				interval := time.Duration(req.IntervalSeconds) * time.Second
				ms.StartPeriodicMetricsCollection(interval)
			} else {
				// One-time collection
				if err := ms.SendMSSQLMetrics(ctx); err != nil {
					log.Printf("Failed to collect MSSQL metrics: %v", err)
				}
			}
		case "postgresql", "postgresql_system", "postgresql_database", "postgres":
			// Start collecting PostgreSQL metrics
			if req.IntervalSeconds > 0 {
				interval := time.Duration(req.IntervalSeconds) * time.Second
				ms.StartPeriodicPostgreSQLMetricsCollection(interval)
			} else {
				// One-time collection
				if err := ms.SendPostgreSQLMetrics(ctx); err != nil {
					log.Printf("Failed to collect PostgreSQL metrics: %v", err)
				}
			}
		default:
			log.Printf("Unknown metric type requested: %s", metricType)
		}
	}

	return &pb.CollectMetricsResponse{
		Status:  "success",
		Message: "Metrics collection started",
	}, nil
}
