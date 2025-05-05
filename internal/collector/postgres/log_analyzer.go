package postgres

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	pb "github.com/sefaphlvn/clustereye-test/pkg/agent"
)

// Regular expressions for parsing PostgreSQL log entries
var (
	// Main log line pattern - matches format:
	// 2025-04-19 13:08:29.513 +03 [99940] LOG:  starting PostgreSQL 15.12 ...
	logLineRegex = regexp.MustCompile(`^([\d-]+ [\d:.]+(?:\.\d+)?\s+(?:\+|\-)\d+)\s+\[(\d+)\]\s+(\w+):\s+(.*)$`)

	// Duration pattern - more flexible to match various formats
	// Matches:
	// - duration: 4.550 ms
	// - [duration: 123.45 ms]
	// - execute <unnamed>: SELECT ... [duration: 123.45 ms]
	durationRegex = regexp.MustCompile(`(?:.*?)duration:\s*([\d.]+)\s*ms`)

	// Connection info pattern
	// connection authorized: user=ossec_user database=ossecdb
	connectionRegex = regexp.MustCompile(`user=(\S+)\s+database=(\S+)`)

	// Statement type patterns
	statementStartRegex = regexp.MustCompile(`(?i)^(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP|TRUNCATE|BEGIN|COMMIT|ROLLBACK)`)
	statementEndRegex   = regexp.MustCompile(`;\s*$`)
)

// LogBuffer represents a buffer for collecting multi-line log entries
type LogBuffer struct {
	timestamp   time.Time
	processId   string
	logLevel    string
	message     []string
	query       []string
	duration    int64
	userName    string
	database    string
	isStatement bool
	isMultiLine bool
}

// Reset clears the buffer for reuse
func (b *LogBuffer) Reset() {
	b.timestamp = time.Time{}
	b.processId = ""
	b.logLevel = ""
	b.message = b.message[:0]
	b.query = b.query[:0]
	b.duration = 0
	b.userName = ""
	b.database = ""
	b.isStatement = false
	b.isMultiLine = false
}

// ToLogEntry converts the buffer to a PostgresLogEntry
func (b *LogBuffer) ToLogEntry() *pb.PostgresLogEntry {
	entry := &pb.PostgresLogEntry{
		Timestamp:     b.timestamp.Unix(),
		ProcessId:     b.processId,
		LogLevel:      b.logLevel,
		Message:       strings.Join(b.message, "\n"),
		InternalQuery: strings.Join(b.query, "\n"),
		DurationMs:    b.duration,
		UserName:      b.userName,
		Database:      b.database,
	}
	return entry
}

// AnalyzePostgresLog analyzes a PostgreSQL log file and returns relevant entries
func AnalyzePostgresLog(logFilePath string, slowQueryThresholdMs int64) (*pb.PostgresLogAnalyzeResponse, error) {
	file, err := os.Open(logFilePath)
	if err != nil {
		return nil, fmt.Errorf("log dosyası açılamadı: %v", err)
	}
	defer file.Close()

	var logEntries []*pb.PostgresLogEntry        // Tüm geçerli girdiler
	var last24HourEntries []*pb.PostgresLogEntry // Son 24 saatlik loglar
	scanner := bufio.NewScanner(file)
	totalLines := 0
	validEntries := 0
	slowQueries := 0
	multiLineQueries := 0

	// Son 24 saatin timestamp'ini hesapla
	last24Hours := time.Now().Add(-24 * time.Hour).Unix()

	log.Printf("PostgreSQL log analizi başlıyor: %s (Eşik: %d ms)", logFilePath, slowQueryThresholdMs)

	// Create a buffer for multi-line entries
	buffer := &LogBuffer{
		message: make([]string, 0, 10),
		query:   make([]string, 0, 10),
	}

	for scanner.Scan() {
		totalLines++
		line := scanner.Text()

		// Try to parse as a new log line
		if matches := logLineRegex.FindStringSubmatch(line); matches != nil {
			// If we have a previous entry in the buffer, process it
			if buffer.processId != "" {
				entry := processBufferedEntry(buffer)
				if entry != nil {
					validEntries++
					processEntry(entry, &logEntries, &last24HourEntries, last24Hours, slowQueryThresholdMs, &slowQueries)
				}
				buffer.Reset()
			}

			// Parse the new line
			if !parseLogLine(line, matches, buffer) {
				log.Printf("Yeni log satırı ayrıştırılamadı: %s", line)
				continue
			}
		} else {
			// This might be a continuation of a multi-line entry
			if buffer.processId != "" {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				// Check if this is a continuation of a SQL statement
				if buffer.isStatement {
					buffer.query = append(buffer.query, line)
					buffer.isMultiLine = true
					if statementEndRegex.MatchString(line) {
						multiLineQueries++
						log.Printf("Çok satırlı sorgu tamamlandı (%d satır)", len(buffer.query))
					}
				} else {
					buffer.message = append(buffer.message, line)
				}
			}
		}
	}

	// Process the last entry in the buffer
	if buffer.processId != "" {
		entry := processBufferedEntry(buffer)
		if entry != nil {
			validEntries++
			processEntry(entry, &logEntries, &last24HourEntries, last24Hours, slowQueryThresholdMs, &slowQueries)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("log dosyası okuma hatası: %v", err)
	}

	log.Printf("PostgreSQL log analizi tamamlandı: Toplam satır=%d, Geçerli girdiler=%d, Yavaş sorgular=%d, Çok satırlı sorgular=%d",
		totalLines, validEntries, slowQueries, multiLineQueries)

	return &pb.PostgresLogAnalyzeResponse{
		LogEntries: logEntries, // Tüm geçerli girdileri döndür
	}, nil
}

// parseLogLine parses a log line into the buffer
func parseLogLine(line string, matches []string, buffer *LogBuffer) bool {
	// Parse timestamp
	timestampStr := strings.TrimSpace(matches[1])

	// Convert +03 format to +0300 format for proper parsing
	if strings.Contains(timestampStr, " +03") {
		timestampStr = strings.Replace(timestampStr, " +03", " +0300", 1)
	} else if strings.Contains(timestampStr, " -03") {
		timestampStr = strings.Replace(timestampStr, " -03", " -0300", 1)
	}

	var timestamp time.Time
	var err error

	// Try parsing with explicit timezone offset
	timestamp, err = time.Parse("2006-01-02 15:04:05.000 -0700", timestampStr)
	if err != nil {
		// Try without milliseconds
		timestamp, err = time.Parse("2006-01-02 15:04:05 -0700", timestampStr)
		if err != nil {
			log.Printf("Timestamp ayrıştırma hatası (%s): %v", timestampStr, err)
			return false
		}
	}

	// Fill the buffer
	buffer.timestamp = timestamp
	buffer.processId = matches[2]
	buffer.logLevel = matches[3]
	message := strings.TrimSpace(matches[4])
	buffer.message = append(buffer.message, message)

	// Check if this is a statement
	if strings.Contains(message, "statement:") {
		parts := strings.SplitN(message, "statement:", 2)
		if len(parts) > 1 {
			query := strings.TrimSpace(parts[1])
			buffer.query = append(buffer.query, query)
			buffer.isStatement = true

			// Check if this might be a multi-line statement
			if statementStartRegex.MatchString(query) && !statementEndRegex.MatchString(query) {
				buffer.isMultiLine = true
			}
		}
	}

	// Parse duration if present
	if durMatch := durationRegex.FindStringSubmatch(message); durMatch != nil {
		duration, err := strconv.ParseFloat(durMatch[1], 64)
		if err == nil {
			buffer.duration = int64(duration)
			log.Printf("Duration bulundu: %d ms", buffer.duration)
		}
	}

	// Parse connection info if present
	if connMatch := connectionRegex.FindStringSubmatch(message); connMatch != nil {
		buffer.userName = connMatch[1]
		buffer.database = connMatch[2]
	}

	return true
}

// processBufferedEntry converts a buffer to a log entry and performs final processing
func processBufferedEntry(buffer *LogBuffer) *pb.PostgresLogEntry {
	if buffer.isMultiLine {
		// For multi-line queries, ensure we have a complete statement
		lastQuery := buffer.query[len(buffer.query)-1]
		if !statementEndRegex.MatchString(lastQuery) {
			log.Printf("Eksik sonlandırılmış çok satırlı sorgu: %s", strings.Join(buffer.query, "\n"))
		}
	}
	return buffer.ToLogEntry()
}

// processEntry handles a single log entry and updates the relevant collections
func processEntry(entry *pb.PostgresLogEntry, logEntries, last24HourEntries *[]*pb.PostgresLogEntry, last24Hours int64, slowQueryThresholdMs int64, slowQueries *int) {
	// Tüm geçerli girdileri logEntries'e ekle
	*logEntries = append(*logEntries, entry)

	// Son 24 saatlik logları ayrı bir slice'da tut
	if entry.Timestamp >= last24Hours {
		*last24HourEntries = append(*last24HourEntries, entry)
		log.Printf("Son 24 saat logu bulundu: %s", entry.Message)
	}

	// Yavaş sorguları say
	if entry.DurationMs > 0 && entry.DurationMs >= slowQueryThresholdMs {
		*slowQueries++
		log.Printf("Yavaş sorgu bulundu (%d ms): %s", entry.DurationMs, entry.InternalQuery)
	}
}
