package reporter

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
	"github.com/senbaris/clustereye-agent/internal/collector/mssql"
	"github.com/senbaris/clustereye-agent/internal/config"
)

// MetricsSender handles sending metrics to the server
type MetricsSender struct {
	cfg            *config.AgentConfig
	reporter       *Reporter
	mssqlCollector *mssql.MSSQLMetricsCollector
}

// NewMetricsSender creates a new metrics sender
func NewMetricsSender(cfg *config.AgentConfig, reporter *Reporter) *MetricsSender {
	return &MetricsSender{
		cfg:            cfg,
		reporter:       reporter,
		mssqlCollector: mssql.NewMSSQLMetricsCollector(cfg),
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
	// Collect all MSSQL metrics
	metricBatches, err := ms.mssqlCollector.CollectAllMetrics()
	if err != nil {
		return fmt.Errorf("failed to collect MSSQL metrics: %w", err)
	}

	// Send each batch
	for _, batch := range metricBatches {
		if err := ms.sendMetricBatch(ctx, batch); err != nil {
			log.Printf("Failed to send metric batch %s: %v", batch.MetricType, err)
			continue
		}
		log.Printf("Successfully sent %d metrics for type %s", len(batch.Metrics), batch.MetricType)
	}

	return nil
}

// sendMetricBatch converts internal metric batch to protobuf and sends it
func (ms *MetricsSender) sendMetricBatch(ctx context.Context, batch *mssql.MetricBatch) error {
	// Convert internal metrics to protobuf
	pbMetrics := make([]*pb.Metric, 0, len(batch.Metrics))

	for _, metric := range batch.Metrics {
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

	// Create metric batch
	pbBatch := &pb.MetricBatch{
		AgentId:             batch.AgentID,
		MetricType:          batch.MetricType,
		Metrics:             pbMetrics,
		CollectionTimestamp: batch.CollectionTimestamp,
		Metadata:            batch.Metadata,
	}

	// Create request
	request := &pb.SendMetricsRequest{
		Batch: pbBatch,
	}

	// Send metrics via gRPC
	if ms.reporter.grpcClient == nil {
		return fmt.Errorf("gRPC client not available")
	}

	client := pb.NewAgentServiceClient(ms.reporter.grpcClient)

	// Set timeout for the request
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	response, err := client.SendMetrics(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to send metrics: %w", err)
	}

	if response.Status != "success" {
		return fmt.Errorf("server returned error: %s - %s", response.Status, response.Message)
	}

	log.Printf("Metrics sent successfully: %d processed, status: %s", response.ProcessedCount, response.Message)

	// Log any errors from server
	if len(response.Errors) > 0 {
		for _, errMsg := range response.Errors {
			log.Printf("Server metric processing error: %s", errMsg)
		}
	}

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
		default:
			log.Printf("Unknown metric type requested: %s", metricType)
		}
	}

	return &pb.CollectMetricsResponse{
		Status:  "success",
		Message: "Metrics collection started",
	}, nil
}
