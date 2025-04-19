package mongo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
)

// Regular expressions for parsing MongoDB log entries
var (
	// Example log line: 2024-02-21T10:15:30.123+00:00 I  NETWORK  [conn123] connection accepted from 127.0.0.1:12345
	logEntryRegex = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{2}:\d{2})\s+(\w)\s+(\w+)\s*(?:\[([^\]]+)\])?\s+(.+)$`)

	// Additional regex patterns for extracting specific fields
	durationRegex    = regexp.MustCompile(`(\d+)ms`)
	commandRegex     = regexp.MustCompile(`command:\s+(\{[^}]+\})`)
	namespaceRegex   = regexp.MustCompile(`(?:ns|namespace):\s+([^\s,]+)`)
	dbNameRegex      = regexp.MustCompile(`db:\s+([^\s,]+)`)
	planSummaryRegex = regexp.MustCompile(`planSummary:\s+([^\n]+)`)

	// Check if a line is a JSON log entry
	jsonLogRegex = regexp.MustCompile(`^\s*\{.*\}\s*$`)
)

// MongoDB JSON log structure
type MongoJSONLog struct {
	Timestamp struct {
		Date string `json:"$date"`
	} `json:"t"`
	Severity   string `json:"s"`
	Component  string `json:"c"`
	Context    string `json:"ctx"`
	Message    string `json:"msg"`
	Attributes struct {
		Type           string `json:"type"`
		Namespace      string `json:"ns"`
		DbName         string `json:"$db"`
		Command        any    `json:"command"`
		WorkingMillis  int64  `json:"workingMillis"`
		DurationMillis int64  `json:"durationMillis"`
		PlanSummary    string `json:"planSummary"`
	} `json:"attr"`
}

// AnalyzeMongoLogs analyzes MongoDB log files and returns the analysis results
func (c *MongoCollector) AnalyzeMongoLog(req *pb.MongoLogAnalyzeRequest) (*pb.MongoLogAnalyzeResponse, error) {
	log.Printf("====== MONGO COLLECTOR: LOG ANALİZİ BAŞLIYOR ======")
	log.Printf("MongoDB log analizi başlatılıyor: Log Dosyası=%s, Eşik=%d ms",
		req.LogFilePath, req.SlowQueryThresholdMs)
	startTime := time.Now()

	// Dosya varlığını kontrol et
	if _, err := os.Stat(req.LogFilePath); os.IsNotExist(err) {
		log.Printf("HATA: Log dosyası bulunamadı: %s", req.LogFilePath)
		return nil, fmt.Errorf("log dosyası bulunamadı: %s", req.LogFilePath)
	}
	log.Printf("Dosya varlığı doğrulandı: %s", req.LogFilePath)

	// Dosya boyutunu al ve logla
	fileInfo, err := os.Stat(req.LogFilePath)
	if err != nil {
		log.Printf("HATA: Dosya bilgisi alınamadı: %v", err)
		return nil, fmt.Errorf("dosya bilgisi alınamadı: %v", err)
	}

	fileSize := fileInfo.Size()
	fileSizeMB := float64(fileSize) / 1024 / 1024
	log.Printf(">>>>> Log dosyası boyutu: %.2f MB", fileSizeMB)

	// Büyük dosyalar için optimizasyon
	var maxLines int64 = 0
	if fileSizeMB > 100 {
		log.Printf(">>>>> Büyük log dosyası tespit edildi (%.2f MB > 100 MB). Okuma optimizasyonu yapılıyor...", fileSizeMB)
		// 100MB'dan büyük dosyalar için son 50,000 satırı analiz et
		maxLines = 50000
		log.Printf(">>>>> En son %d satır analiz edilecek", maxLines)
	} else if fileSizeMB > 50 {
		// 50MB'dan büyük dosyalar için son 20,000 satırı analiz et
		maxLines = 20000
		log.Printf(">>>>> En son %d satır analiz edilecek", maxLines)
	}

	// Analiz işlemini gerçekleştir
	log.Printf(">>>>> Log analizi başlıyor...")

	// Open the log file
	log.Printf(">>>>> MongoDB log dosyası analiz ediliyor: %s (Eşik: %d ms)", req.LogFilePath, req.SlowQueryThresholdMs)

	f, err := os.Open(req.LogFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}
	defer f.Close()

	// Initialize response
	response := &pb.MongoLogAnalyzeResponse{
		LogEntries: make([]*pb.MongoLogEntry, 0),
	}

	// Count for debugging
	totalLines := int64(0)
	validLogEntries := 0
	slowQueries := 0

	// Detect log format (assume JSON format for MongoDB 5+ and legacy for older versions)
	isJSONFormat := false
	mongoVersion := c.GetMongoVersion()

	// Check mongo version to determine log format
	if strings.HasPrefix(mongoVersion, "6.") || strings.HasPrefix(mongoVersion, "7.") || strings.HasPrefix(mongoVersion, "8.") {
		log.Printf("Detected modern MongoDB version %s, using JSON log parser", mongoVersion)
		isJSONFormat = true
	} else {
		// Peek at the first line to detect format
		scanner := bufio.NewScanner(f)
		if scanner.Scan() {
			line := scanner.Text()
			if jsonLogRegex.MatchString(line) {
				log.Printf("Detected JSON formatted logs")
				isJSONFormat = true
			} else {
				log.Printf("Detected legacy formatted logs")
			}
			// Rewind file for processing
			f.Seek(0, 0)
		}
	}

	// Process log file - use optimal strategy based on file size
	var lines []string
	var scanErr error

	if maxLines > 0 {
		// Büyük dosya için sadece son satırları oku
		log.Printf(">>>>> Büyük dosya için sadece son %d satır okunuyor...", maxLines)
		lines, scanErr = readLastLines(req.LogFilePath, maxLines)
		if scanErr != nil {
			log.Printf("HATA: Son satırlar okunurken hata: %v, normal okuma deneniyor", scanErr)
			// Fallback to normal reading if readLastLines fails
			f.Seek(0, 0)
			scanner := bufio.NewScanner(f)
			for scanner.Scan() && totalLines < maxLines {
				lines = append(lines, scanner.Text())
				totalLines++
			}
			if scanErr := scanner.Err(); scanErr != nil {
				log.Printf("HATA: Log dosyası okuma hatası: %v", scanErr)
				return nil, fmt.Errorf("error reading log file: %v", scanErr)
			}
		} else {
			totalLines = int64(len(lines))
			log.Printf(">>>>> Son %d satır başarıyla okundu", totalLines)
		}
	} else {
		// Normal dosya boyutu için tüm dosyayı oku
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
			totalLines++
		}
		if scanErr := scanner.Err(); scanErr != nil {
			log.Printf("HATA: Log dosyası okuma hatası: %v", scanErr)
			return nil, fmt.Errorf("error reading log file: %v", scanErr)
		}
	}

	log.Printf(">>>>> Toplam %d satır okundu, şimdi analiz ediliyor...", totalLines)

	// Store all valid entries to use if no matching entries are found
	var allValidEntries []*pb.MongoLogEntry
	var last24HoursEntries []*pb.MongoLogEntry

	// 24 saat öncesini hesapla
	now := time.Now()
	yesterday := now.Add(-24 * time.Hour)
	log.Printf(">>>>> Son 24 saat için tarih filtresi: %s", yesterday.Format(time.RFC3339))

	// Process each line
	for _, line := range lines {
		// Print sample lines for debugging (first 3 lines)
		if validLogEntries <= 3 {
			log.Printf("Sample log line: %s", trimString(line, 100))
		}

		// Parse based on detected format
		var entry *pb.MongoLogEntry
		var parseErr error

		if isJSONFormat && jsonLogRegex.MatchString(line) {
			entry, parseErr = parseJSONLogEntry(line)
		} else {
			entry, parseErr = parseLogEntry(line)
		}

		if parseErr != nil {
			continue
		}

		// Skip if entry is nil (not a valid log entry)
		if entry == nil {
			continue
		}

		validLogEntries++

		// Store valid entries for potential fallback
		allValidEntries = append(allValidEntries, entry)

		// 24 saat kontrolü yap ve filtreye uyanları tut
		logTime := time.Unix(entry.Timestamp, 0)
		if logTime.After(yesterday) {
			last24HoursEntries = append(last24HoursEntries, entry)
		}

		// Either include all entries if threshold is 0, or only slow queries
		if req.SlowQueryThresholdMs == 0 || entry.DurationMillis > req.SlowQueryThresholdMs {
			if entry.DurationMillis > req.SlowQueryThresholdMs {
				slowQueries++
				// Log all slow queries that match our threshold
				log.Printf("!!!! YAVAŞ SORGU #%d: [%s] %s (%d ms)",
					slowQueries, entry.Component, trimString(entry.Message, 50),
					entry.DurationMillis)
			}
			response.LogEntries = append(response.LogEntries, entry)
		}
	}

	// If SlowQueryThresholdMs is set but no slow queries found, return last 24 hours entries
	if req.SlowQueryThresholdMs > 0 && len(response.LogEntries) == 0 {
		log.Printf(">>>>> Log dosyasında %d ms eşiğini aşan sorgu bulunamadı, son 24 saatin kayıtları ekleniyor", req.SlowQueryThresholdMs)

		if len(last24HoursEntries) > 0 {
			response.LogEntries = last24HoursEntries
			log.Printf(">>>>> Son 24 saate ait %d adet log kaydı eklendi", len(last24HoursEntries))
		} else if len(allValidEntries) > 0 {
			// Son 24 saate ait log bulunamadıysa ve geçerli girdiler varsa, en son 20 kaydı ekle
			sampleCount := 20
			if len(allValidEntries) < sampleCount {
				sampleCount = len(allValidEntries)
			}

			startIdx := len(allValidEntries) - sampleCount
			if startIdx < 0 {
				startIdx = 0
			}

			response.LogEntries = allValidEntries[startIdx:]
			log.Printf(">>>>> Son 24 saate ait kayıt bulunamadığı için son %d adet kayıt eklendi", len(response.LogEntries))
		} else {
			// Hiç geçerli log girişi bulunamazsa, ham log satırlarını normal şekilde ekle
			log.Printf(">>>>> Geçerli log girişi bulunamadı, ham log satırları ekleniyor")

			timestamp := time.Now().Unix()
			for i, line := range lines {
				if i >= 20 { // En fazla 20 ham satır ekle
					break
				}

				entry := &pb.MongoLogEntry{
					Timestamp:      timestamp,
					Severity:       "I",
					Component:      "COMMAND",
					Context:        "",
					Message:        line,
					DurationMillis: 0,
				}
				response.LogEntries = append(response.LogEntries, entry)

				if i < 3 {
					log.Printf(">>>>> Örnek ham log satırı #%d: %s", i+1, trimString(line, 100))
				}
			}
			log.Printf(">>>>> Ham log satırlarından %d adet eklendi", len(response.LogEntries))
		}
	}

	log.Printf(">>>>> MongoDB log analizi istatistikleri: Toplam satır: %d, Geçerli girdiler: %d, Yavaş sorgular: %d, Döndürülen girdiler: %d",
		totalLines, validLogEntries, slowQueries, len(response.LogEntries))

	// Log the first 5 entries we're sending to the server
	log.Printf(">>>>> Sunucuya gönderilen ilk girişler (maksimum 5):")
	for i, entry := range response.LogEntries {
		if i >= 5 {
			break
		}

		log.Printf("  Girdi #%d: %s %s [%s] (%dms) - %s",
			i+1,
			time.Unix(entry.Timestamp, 0).Format("2006-01-02T15:04:05"),
			entry.Severity,
			entry.Component,
			entry.DurationMillis,
			trimString(entry.Message, 80))
	}

	// If we have more than 5 entries, show a summary
	if len(response.LogEntries) > 5 {
		log.Printf(">>>>> ... ve %d girdi daha", len(response.LogEntries)-5)
	}

	elapsedTime := time.Since(startTime)
	log.Printf("====== MONGO LOG ANALİZİ TAMAMLANDI (Süre: %v) ======", elapsedTime)

	return response, nil
}

// Helper function to trim strings for logging
func trimString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// parseJSONLogEntry parses a MongoDB JSON format log entry
func parseJSONLogEntry(line string) (*pb.MongoLogEntry, error) {
	var jsonLog MongoJSONLog

	if err := json.Unmarshal([]byte(line), &jsonLog); err != nil {
		return nil, fmt.Errorf("failed to parse JSON log: %v", err)
	}

	// Debug'ı sadece slow query için yap
	isSlowQuery := jsonLog.Message == "Slow query"

	// Sadece yavaş sorgular için debug et
	if isSlowQuery && jsonLog.Attributes.DurationMillis > 100 {
		log.Printf("!!!! JSON YAVAŞ SORGU: bileşen=%s, süre=%d ms, ns=%s",
			jsonLog.Component, jsonLog.Attributes.DurationMillis, jsonLog.Attributes.Namespace)
	}

	// Parse timestamp
	var timestamp int64
	if jsonLog.Timestamp.Date != "" {
		// Try to parse the timestamp
		t, err := time.Parse(time.RFC3339, jsonLog.Timestamp.Date)
		if err != nil {
			// Hata logunu kaldır, sadece gerçekten kritik olanlarda log at
			timestamp = time.Now().Unix()
		} else {
			timestamp = t.Unix()
		}
	} else {
		timestamp = time.Now().Unix()
	}

	// Create protobuf entry
	entry := &pb.MongoLogEntry{
		Timestamp:      timestamp,
		Severity:       jsonLog.Severity,
		Component:      jsonLog.Component,
		Context:        jsonLog.Context,
		Message:        jsonLog.Message,
		DurationMillis: jsonLog.Attributes.DurationMillis,
		PlanSummary:    jsonLog.Attributes.PlanSummary,
		DbName:         jsonLog.Attributes.DbName,
		Namespace:      jsonLog.Attributes.Namespace,
	}

	// Handle the command field
	if jsonLog.Attributes.Command != nil {
		cmdJson, err := json.Marshal(jsonLog.Attributes.Command)
		if err == nil {
			entry.Command = string(cmdJson)
		}
	}

	return entry, nil
}

// parseLogEntry parses a single MongoDB log entry line (legacy format)
func parseLogEntry(line string) (*pb.MongoLogEntry, error) {
	matches := logEntryRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, nil // Not a valid log entry line
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, matches[1])
	if err != nil {
		log.Printf("Error parsing timestamp: %v", err)
		return nil, err
	}

	// Extract fields from regex matches
	severity := matches[2]
	component := matches[3]
	context := matches[4]
	message := matches[5]

	// Create log entry
	entry := &pb.MongoLogEntry{
		Timestamp: timestamp.Unix(),
		Severity:  severity,
		Component: component,
		Context:   context,
		Message:   message,
	}

	// Extract additional fields from message
	if dur := durationRegex.FindStringSubmatch(message); dur != nil {
		if duration, err := strconv.ParseInt(dur[1], 10, 64); err == nil {
			entry.DurationMillis = duration
		}
	}

	if cmd := commandRegex.FindStringSubmatch(message); cmd != nil {
		entry.Command = cmd[1]
	}

	if ns := namespaceRegex.FindStringSubmatch(message); ns != nil {
		entry.Namespace = ns[1]
	}

	if db := dbNameRegex.FindStringSubmatch(message); db != nil {
		entry.DbName = db[1]
	}

	if plan := planSummaryRegex.FindStringSubmatch(message); plan != nil {
		entry.PlanSummary = plan[1]
	}

	return entry, nil
}

// readLastLines reads the last N lines from a file
func readLastLines(filePath string, n int64) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Approximate the buffer size we might need
	// This is a heuristic - assume average line length of 200 bytes
	approximateSize := n * 200
	if approximateSize > stat.Size() {
		approximateSize = stat.Size()
	}

	// Start reading from the end of the file
	pos := stat.Size() - approximateSize
	if pos < 0 {
		pos = 0
	}

	// Create buffered reader
	_, err = file.Seek(pos, 0)
	if err != nil {
		return nil, err
	}

	// Read all lines
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	// If error occurred during scanning
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// If we have more lines than requested, truncate
	if int64(len(lines)) > n {
		lines = lines[len(lines)-int(n):]
	}

	return lines, nil
}
