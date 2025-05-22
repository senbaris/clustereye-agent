package utils

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/senbaris/clustereye-agent/internal/logger"
)

// WindowsPerformanceMetrics contains all system metrics for Windows
type WindowsPerformanceMetrics struct {
	CPUUsage       float64
	CPUCores       int32
	MemoryTotal    int64
	MemoryFree     int64
	MemoryUsed     int64
	MemoryUsage    float64
	DiskTotal      int64
	DiskFree       int64
	DiskUsed       int64
	DiskUsage      float64
	LoadAverage1M  float64
	LoadAverage5M  float64
	LoadAverage15M float64
}

// Singleton pattern for cached metrics to reduce PowerShell calls
var (
	cachedMetrics     *WindowsPerformanceMetrics
	cachedMetricsLock sync.RWMutex
	lastMetricsUpdate time.Time
	// Default cache TTL - metrics won't be refreshed more frequently than this
	metricsCacheTTL = 2 * time.Minute
	// Background collection enabled flag
	backgroundEnabled  bool
	stopBackgroundChan chan struct{}
)

// StartBackgroundCollection starts collecting metrics in the background
// to minimize PowerShell process overhead and CPU usage
func StartBackgroundCollection() {
	cachedMetricsLock.Lock()
	defer cachedMetricsLock.Unlock()

	if backgroundEnabled {
		return // Already running
	}

	stopBackgroundChan = make(chan struct{})
	backgroundEnabled = true

	// Start background collection immediately
	go backgroundMetricsCollection()

	logger.Info("Started background collection of Windows performance metrics")
}

// StopBackgroundCollection stops the background metrics collection
func StopBackgroundCollection() {
	cachedMetricsLock.Lock()
	defer cachedMetricsLock.Unlock()

	if !backgroundEnabled {
		return // Not running
	}

	close(stopBackgroundChan)
	backgroundEnabled = false
	logger.Info("Stopped background collection of Windows performance metrics")
}

// backgroundMetricsCollection collects metrics at regular intervals in the background
func backgroundMetricsCollection() {
	ticker := time.NewTicker(metricsCacheTTL)
	defer ticker.Stop()

	// Collect metrics immediately on start
	metrics, err := collectMetricsInternal()
	if err == nil {
		updateCachedMetrics(metrics)
	}

	for {
		select {
		case <-ticker.C:
			metrics, err := collectMetricsInternal()
			if err == nil {
				updateCachedMetrics(metrics)
			} else {
				logger.Warning("Background metrics collection failed: %v", err)
			}
		case <-stopBackgroundChan:
			return
		}
	}
}

// updateCachedMetrics safely updates the cached metrics
func updateCachedMetrics(metrics *WindowsPerformanceMetrics) {
	cachedMetricsLock.Lock()
	defer cachedMetricsLock.Unlock()

	cachedMetrics = metrics
	lastMetricsUpdate = time.Now()

	logger.Debug("Updated cached Windows metrics: CPU=%.2f%%, Memory=%.2f%%, Disk=%.2f%%",
		metrics.CPUUsage, metrics.MemoryUsage, metrics.DiskUsage)
}

// CollectWindowsMetrics collects all Windows metrics in a single operation
// to reduce the number of PowerShell processes, using cache when possible
func CollectWindowsMetrics() (*WindowsPerformanceMetrics, error) {
	// Check if background collection is enabled
	if !backgroundEnabled {
		// Start background collection if not already running
		StartBackgroundCollection()
	}

	// Check if we have valid cached metrics
	cachedMetricsLock.RLock()
	if cachedMetrics != nil && time.Since(lastMetricsUpdate) < metricsCacheTTL {
		// Return a copy of cached metrics to prevent modification
		metricsCopy := *cachedMetrics
		cachedMetricsLock.RUnlock()
		logger.Debug("Using cached Windows metrics (age: %v)", time.Since(lastMetricsUpdate))
		return &metricsCopy, nil
	}
	cachedMetricsLock.RUnlock()

	// No valid cache, collect metrics (this might still update the cache in background)
	return collectMetricsInternal()
}

// collectMetricsInternal performs the actual metric collection without using cache
func collectMetricsInternal() (*WindowsPerformanceMetrics, error) {
	// Create a metrics struct to populate
	metrics := &WindowsPerformanceMetrics{}
	startTime := time.Now()

	// Create a single PowerShell script that collects all metrics at once
	// This significantly reduces the number of PowerShell processes
	metricsScript := `
# More efficient PowerShell metric collection - avoid WMI where possible
$metrics = @{}

# Use typeperf instead of WMI for CPU metrics - much faster
$cpuLoad = (typeperf -sc 1 "\Processor(_Total)\% Processor Time" | Select-String -Pattern "[0-9\.]+")[0].ToString().Split(",")[1].Trim('"')
$cpuCores = (Get-CimInstance -ClassName Win32_ComputerSystem -Property NumberOfLogicalProcessors).NumberOfLogicalProcessors
$metrics.CPUUsage = [float]$cpuLoad
$metrics.CPUCores = $cpuCores

# Memory metrics using pure CIM (faster than WMI)
$os = Get-CimInstance -ClassName Win32_OperatingSystem -Property TotalVisibleMemorySize,FreePhysicalMemory
$metrics.MemoryTotal = $os.TotalVisibleMemorySize * 1024L  # Convert from KB to bytes
$metrics.MemoryFree = $os.FreePhysicalMemory * 1024L       # Convert from KB to bytes
$metrics.MemoryUsed = $metrics.MemoryTotal - $metrics.MemoryFree
$metrics.MemoryUsage = [math]::Round(($metrics.MemoryUsed / $metrics.MemoryTotal) * 100, 2)

# Disk metrics using CIM (system drive)
$systemDrive = $env:SystemDrive.TrimEnd(':')
$disk = Get-CimInstance -ClassName Win32_LogicalDisk -Filter "DeviceID='$($env:SystemDrive)'"
$metrics.DiskTotal = $disk.Size
$metrics.DiskFree = $disk.FreeSpace
$metrics.DiskUsed = $metrics.DiskTotal - $metrics.DiskFree
$metrics.DiskUsage = [math]::Round(($metrics.DiskUsed / $metrics.DiskTotal) * 100, 2)

# System load - use processor queue length from performance counter
$systemInfo = Get-CimInstance -ClassName Win32_PerfFormattedData_PerfOS_System -Property ProcessorQueueLength
$cpuQueue = $systemInfo.ProcessorQueueLength
$metrics.LoadAverage1M = $cpuQueue
$metrics.LoadAverage5M = $cpuQueue 
$metrics.LoadAverage15M = $cpuQueue

# Convert to JSON and output
ConvertTo-Json $metrics
`

	// First check if command is already cached
	cacheKey := "windows_metrics"
	psResultMutex.RLock()
	cachedRes, exists := psResultCache[cacheKey]
	psResultMutex.RUnlock()

	var outputStr string
	var err error

	if exists && time.Since(cachedRes.timestamp) < metricsCacheTTL {
		logger.Debug("Using cached PowerShell metrics result")
		outputStr = cachedRes.output
		err = cachedRes.err
	} else {
		// Execute the combined script just once - use exec.Command directly instead of RunPowerShellCommand
		// to avoid mutex locks/contentions when multiple parts of the system request metrics simultaneously
		cmd := exec.Command("powershell", "-NoProfile", "-NonInteractive", "-Command", metricsScript)
		output, cmdErr := cmd.Output()
		if cmdErr != nil {
			logger.Error("Failed to collect Windows metrics: %v", cmdErr)
			return nil, cmdErr
		}

		outputStr = string(output)
		err = cmdErr

		// Cache the result
		result := cachedResult{
			output:    outputStr,
			timestamp: time.Now(),
			err:       err,
		}

		psResultMutex.Lock()
		psResultCache[cacheKey] = result
		psResultMutex.Unlock()
	}

	// Parse the JSON output
	if err != nil {
		return nil, err
	}

	// Extract CPU usage
	if strings.Contains(outputStr, "CPUUsage") {
		cpuStr := getBetween(outputStr, "\"CPUUsage\":", ",")
		if cpuVal, err := strconv.ParseFloat(strings.TrimSpace(cpuStr), 64); err == nil {
			metrics.CPUUsage = cpuVal
		}
	}

	// Extract CPU cores
	if strings.Contains(outputStr, "CPUCores") {
		coresStr := getBetween(outputStr, "\"CPUCores\":", ",")
		if coresVal, err := strconv.ParseInt(strings.TrimSpace(coresStr), 10, 32); err == nil {
			metrics.CPUCores = int32(coresVal)
		}
	}

	// Extract memory metrics
	if strings.Contains(outputStr, "MemoryTotal") {
		memTotalStr := getBetween(outputStr, "\"MemoryTotal\":", ",")
		if memTotalVal, err := strconv.ParseInt(strings.TrimSpace(memTotalStr), 10, 64); err == nil {
			metrics.MemoryTotal = memTotalVal
		}
	}

	if strings.Contains(outputStr, "MemoryFree") {
		memFreeStr := getBetween(outputStr, "\"MemoryFree\":", ",")
		if memFreeVal, err := strconv.ParseInt(strings.TrimSpace(memFreeStr), 10, 64); err == nil {
			metrics.MemoryFree = memFreeVal
		}
	}

	if strings.Contains(outputStr, "MemoryUsage") {
		memUsageStr := getBetween(outputStr, "\"MemoryUsage\":", ",")
		if memUsageVal, err := strconv.ParseFloat(strings.TrimSpace(memUsageStr), 64); err == nil {
			metrics.MemoryUsage = memUsageVal
		}
	}

	// Extract disk metrics
	if strings.Contains(outputStr, "DiskTotal") {
		diskTotalStr := getBetween(outputStr, "\"DiskTotal\":", ",")
		if diskTotalVal, err := strconv.ParseInt(strings.TrimSpace(diskTotalStr), 10, 64); err == nil {
			metrics.DiskTotal = diskTotalVal
		}
	}

	if strings.Contains(outputStr, "DiskFree") {
		diskFreeStr := getBetween(outputStr, "\"DiskFree\":", ",")
		if diskFreeVal, err := strconv.ParseInt(strings.TrimSpace(diskFreeStr), 10, 64); err == nil {
			metrics.DiskFree = diskFreeVal
		}
	}

	if strings.Contains(outputStr, "DiskUsage") {
		diskUsageStr := getBetween(outputStr, "\"DiskUsage\":", ",")
		if diskUsageVal, err := strconv.ParseFloat(strings.TrimSpace(diskUsageStr), 64); err == nil {
			metrics.DiskUsage = diskUsageVal
		}
	}

	// Extract load averages
	if strings.Contains(outputStr, "LoadAverage1M") {
		load1Str := getBetween(outputStr, "\"LoadAverage1M\":", ",")
		if load1Val, err := strconv.ParseFloat(strings.TrimSpace(load1Str), 64); err == nil {
			metrics.LoadAverage1M = load1Val
		}
	}

	if strings.Contains(outputStr, "LoadAverage5M") {
		load5Str := getBetween(outputStr, "\"LoadAverage5M\":", ",")
		if load5Val, err := strconv.ParseFloat(strings.TrimSpace(load5Str), 64); err == nil {
			metrics.LoadAverage5M = load5Val
		}
	}

	if strings.Contains(outputStr, "LoadAverage15M") {
		load15Str := getBetween(outputStr, "\"LoadAverage15M\":", "}")
		if load15Val, err := strconv.ParseFloat(strings.TrimSpace(load15Str), 64); err == nil {
			metrics.LoadAverage15M = load15Val
		}
	}

	logger.Debug("Windows metrics collected in %v", time.Since(startTime))

	// Update cache with fresh metrics
	updateCachedMetrics(metrics)

	return metrics, nil
}

// Helper function to extract values from JSON string
func getBetween(s string, start string, end string) string {
	startIdx := strings.Index(s, start)
	if startIdx == -1 {
		return ""
	}

	startIdx += len(start)
	endIdx := strings.Index(s[startIdx:], end)
	if endIdx == -1 {
		return s[startIdx:]
	}

	return s[startIdx : startIdx+endIdx]
}

// PerformanceMeasure measures execution time of a function and logs it
func PerformanceMeasure(functionName string) func() {
	start := time.Now()
	return func() {
		logger.Debug("Execution time for %s: %v", functionName, time.Since(start))
	}
}

// GetCPUUsageEfficient gets CPU usage information using a more efficient method
// with caching to reduce PowerShell calls
func GetCPUUsageEfficient() (float64, error) {
	defer PerformanceMeasure("GetCPUUsageEfficient")()

	// Use the cached metrics if available
	metrics, err := CollectWindowsMetrics()
	if err == nil && metrics != nil {
		return metrics.CPUUsage, nil
	}

	// Fallback to direct typeperf call if CollectWindowsMetrics fails
	cmd := exec.Command("typeperf", "-sc", "1", "\\Processor(_Total)\\% Processor Time")
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	// Parse typeperf output (CSV format)
	lines := strings.Split(string(output), "\n")
	if len(lines) < 3 {
		return 0, fmt.Errorf("unexpected typeperf output format")
	}

	// Parse the value from the CSV line
	fields := strings.Split(lines[2], ",")
	if len(fields) < 2 {
		return 0, fmt.Errorf("cannot parse typeperf value")
	}

	// Remove quotes and convert to float
	value := strings.Trim(fields[1], "\"")
	cpuUsage, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}

	return cpuUsage, nil
}

// GetMemoryInfoEfficient gets memory information using a more efficient method
// with caching to reduce PowerShell calls
func GetMemoryInfoEfficient() (total int64, free int64, err error) {
	defer PerformanceMeasure("GetMemoryInfoEfficient")()

	// Use the cached metrics if available
	metrics, err := CollectWindowsMetrics()
	if err == nil && metrics != nil && metrics.MemoryTotal > 0 {
		return metrics.MemoryTotal, metrics.MemoryFree, nil
	}

	// Fallback to wmic as a last resort
	cmd := exec.Command("wmic", "OS", "get", "TotalVisibleMemorySize,FreePhysicalMemory", "/Value")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}

	// Parse wmic output
	outputStr := string(output)
	totalStr := ""
	freeStr := ""

	// Extract TotalVisibleMemorySize
	if idx := strings.Index(outputStr, "TotalVisibleMemorySize="); idx != -1 {
		totalPart := outputStr[idx+len("TotalVisibleMemorySize="):]
		endIdx := strings.Index(totalPart, "\r\n")
		if endIdx != -1 {
			totalStr = totalPart[:endIdx]
		}
	}

	// Extract FreePhysicalMemory
	if idx := strings.Index(outputStr, "FreePhysicalMemory="); idx != -1 {
		freePart := outputStr[idx+len("FreePhysicalMemory="):]
		endIdx := strings.Index(freePart, "\r\n")
		if endIdx != -1 {
			freeStr = freePart[:endIdx]
		}
	}

	// Convert to int64 (values are in KB)
	if totalStr != "" {
		if totalVal, err := strconv.ParseInt(totalStr, 10, 64); err == nil {
			total = totalVal * 1024 // Convert KB to bytes
		}
	}

	if freeStr != "" {
		if freeVal, err := strconv.ParseInt(freeStr, 10, 64); err == nil {
			free = freeVal * 1024 // Convert KB to bytes
		}
	}

	if total == 0 || free == 0 {
		return 0, 0, fmt.Errorf("failed to parse memory information")
	}

	return total, free, nil
}

// GetDiskInfoEfficient gets disk information using a more efficient method
// with caching to reduce PowerShell calls
func GetDiskInfoEfficient() (total int64, free int64, err error) {
	defer PerformanceMeasure("GetDiskInfoEfficient")()

	// Use the cached metrics if available
	metrics, err := CollectWindowsMetrics()
	if err == nil && metrics != nil && metrics.DiskTotal > 0 {
		return metrics.DiskTotal, metrics.DiskFree, nil
	}

	// Get system drive (usually C:)
	systemDrive := "C:"

	// Fallback to wmic as a last resort
	cmd := exec.Command("wmic", "LogicalDisk", "Where", fmt.Sprintf("DeviceID='%s'", systemDrive), "get", "Size,FreeSpace", "/Value")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}

	// Parse wmic output
	outputStr := string(output)
	totalStr := ""
	freeStr := ""

	// Extract Size
	if idx := strings.Index(outputStr, "Size="); idx != -1 {
		totalPart := outputStr[idx+len("Size="):]
		endIdx := strings.Index(totalPart, "\r\n")
		if endIdx != -1 {
			totalStr = totalPart[:endIdx]
		}
	}

	// Extract FreeSpace
	if idx := strings.Index(outputStr, "FreeSpace="); idx != -1 {
		freePart := outputStr[idx+len("FreeSpace="):]
		endIdx := strings.Index(freePart, "\r\n")
		if endIdx != -1 {
			freeStr = freePart[:endIdx]
		}
	}

	// Convert to int64
	if totalStr != "" {
		if totalVal, err := strconv.ParseInt(totalStr, 10, 64); err == nil {
			total = totalVal
		}
	}

	if freeStr != "" {
		if freeVal, err := strconv.ParseInt(freeStr, 10, 64); err == nil {
			free = freeVal
		}
	}

	if total == 0 || free == 0 {
		return 0, 0, fmt.Errorf("failed to parse disk information")
	}

	return total, free, nil
}
