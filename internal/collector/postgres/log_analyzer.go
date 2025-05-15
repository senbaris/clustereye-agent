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

	// Alternative format without timezone
	// 2025-04-19 13:08:29.513 [99940] LOG:  starting PostgreSQL 15.12 ...
	logLineAltRegex = regexp.MustCompile(`^([\d-]+ [\d:.]+(?:\.\d+)?)\s+\[(\d+)\]\s+(\w+):\s+(.*)$`)

	// Syslog format
	// May 15 10:49:45 hostname postgresql[12345]: LOG:  message
	syslogRegex = regexp.MustCompile(`^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}).*?(?:postgresql|postgres).*?\[(\d+)\](?::|.*?:)\s+(\w+):\s+(.*)$`)

	// Generic syslog format with more flexible hostname/service matching
	// May 15 10:49:45 ip-10-30-100-249 clustereye-agent-linux-amd64[3543442]: 2025/05/15 10:49:45 PostgreSQL log...
	genericSyslogRegex = regexp.MustCompile(`^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+\S+\s+\S+\[\d+\]:\s+(?:\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+)?(.+)$`)

	// CSV format (if PostgreSQL is configured to log in CSV)
	// 2025-04-19 13:08:29.513,99940,LOG,starting PostgreSQL 15.12 ...
	csvRegex = regexp.MustCompile(`^([\d-]+ [\d:.]+(?:\.\d+)?),(\d+),(\w+),(.*)$`)

	// Fallback pattern - matches anything with LOG: or ERROR: in it
	// This is used as a last resort to find any PostgreSQL log lines
	fallbackRegex = regexp.MustCompile(`(?:LOG|ERROR|FATAL|PANIC|WARNING|NOTICE|INFO|DEBUG):\s+(.*)$`)

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
	var slowQueryEntries []*pb.PostgresLogEntry  // Yavaş sorgu eşiğini geçen girdiler
	var last24HourEntries []*pb.PostgresLogEntry // Son 24 saatlik loglar
	scanner := bufio.NewScanner(file)
	totalLines := 0
	validEntries := 0
	slowQueries := 0
	multiLineQueries := 0
	unparsedLines := 0
	formatStats := map[string]int{
		"standard": 0,
		"notz":     0,
		"syslog":   0,
		"generic":  0,
		"csv":      0,
		"fallback": 0,
		"unknown":  0,
	}

	// Log dosyası formatı hakkında teşhis bilgisi için ilk birkaç satırı logla
	var sampleLines []string

	// Son 24 saatin timestamp'ini hesapla
	last24Hours := time.Now().Add(-24 * time.Hour).Unix()

	log.Printf("PostgreSQL log analizi başlıyor: %s (Eşik: %d ms)", logFilePath, slowQueryThresholdMs)

	// Create a buffer for multi-line entries
	buffer := &LogBuffer{
		message: make([]string, 0, 10),
		query:   make([]string, 0, 10),
	}

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		totalLines++
		line := scanner.Text()

		// İlk 5 satırı örnek olarak topla
		if len(sampleLines) < 5 {
			sampleLines = append(sampleLines, line)
		}

		// Try to parse as a new log line - try all formats
		var matches []string
		var formatMatched string

		// Try the original timezone format
		if matchesStd := logLineRegex.FindStringSubmatch(line); matchesStd != nil {
			matches = matchesStd
			formatMatched = "standard"
			formatStats["standard"]++
		} else if matchesAlt := logLineAltRegex.FindStringSubmatch(line); matchesAlt != nil {
			// Try alternative format without timezone
			matches = matchesAlt
			formatMatched = "notz"
			formatStats["notz"]++
		} else if matchesSyslog := syslogRegex.FindStringSubmatch(line); matchesSyslog != nil {
			// Try syslog format
			matches = matchesSyslog
			formatMatched = "syslog"
			formatStats["syslog"]++
		} else if matchesGeneric := genericSyslogRegex.FindStringSubmatch(line); matchesGeneric != nil {
			// Try generic syslog format - process differently since the pattern is different
			// We'll use current time for timestamps and extract any LOG/ERROR info from the content
			matches = make([]string, 5)
			matches[0] = line              // Full line
			matches[1] = matchesGeneric[1] // Timestamp
			matches[2] = "0"               // Process ID (placeholder)

			// Try to extract log level from the content
			content := matchesGeneric[2]
			var logLevel string
			var message string

			// Check for common PostgreSQL log prefixes
			if logMatch := fallbackRegex.FindStringSubmatch(content); logMatch != nil {
				// Found a log pattern
				for _, level := range []string{"LOG", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG", "FATAL", "PANIC"} {
					if strings.Contains(content, level+":") {
						logLevel = level
						// Extract the message part after the level
						parts := strings.SplitN(content, level+":", 2)
						if len(parts) > 1 {
							message = strings.TrimSpace(parts[1])
						} else {
							message = content // Fallback to full content
						}
						break
					}
				}
			}

			if logLevel == "" {
				// No log level found, use INFO as default
				logLevel = "INFO"
				message = content
			}

			matches[3] = logLevel
			matches[4] = message

			formatMatched = "generic"
			formatStats["generic"]++
		} else if matchesCsv := csvRegex.FindStringSubmatch(line); matchesCsv != nil {
			// Try CSV format
			matches = matchesCsv
			formatMatched = "csv"
			formatStats["csv"]++
		} else if fallbackMatch := fallbackRegex.FindStringSubmatch(line); fallbackMatch != nil {
			// Fallback - try to extract anything that looks like a PostgreSQL log line
			// This won't have proper timestamp and PID, but at least we'll get the message
			matches = make([]string, 5)
			matches[0] = line                                     // Full line
			matches[1] = time.Now().Format("2006-01-02 15:04:05") // Current time
			matches[2] = "0"                                      // Process ID (placeholder)

			// Try to detect log level
			var logLevel string
			for _, level := range []string{"LOG", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG", "FATAL", "PANIC"} {
				if strings.Contains(line, level+":") {
					logLevel = level
					break
				}
			}
			if logLevel == "" {
				logLevel = "INFO" // Default if not found
			}

			matches[3] = logLevel
			matches[4] = fallbackMatch[1] // Message after LOG:/ERROR: etc.

			formatMatched = "fallback"
			formatStats["fallback"]++
		} else {
			// Not a log line start - might be continuation or invalid line
			formatStats["unknown"]++
		}

		if matches != nil {
			// If we have a previous entry in the buffer, process it
			if buffer.processId != "" {
				entry := processBufferedEntry(buffer)
				if entry != nil {
					validEntries++
					processEntry(entry, &logEntries, &last24HourEntries, &slowQueryEntries, last24Hours, slowQueryThresholdMs, &slowQueries)
				}
				buffer.Reset()
			}

			// Parse the new line
			if !parseLogLine(line, matches, buffer) {
				log.Printf("Yeni log satırı ayrıştırılamadı: %s (format: %s, satır: %d)", line, formatMatched, lineNumber)
				unparsedLines++
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
			} else {
				// Couldn't parse line and no active buffer
				unparsedLines++
				if unparsedLines <= 10 {
					log.Printf("Tanımlanamayan log satırı (#%d): %s", unparsedLines, line)
				}
			}
		}
	}

	// Process the last entry in the buffer
	if buffer.processId != "" {
		entry := processBufferedEntry(buffer)
		if entry != nil {
			validEntries++
			processEntry(entry, &logEntries, &last24HourEntries, &slowQueryEntries, last24Hours, slowQueryThresholdMs, &slowQueries)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("log dosyası okuma hatası: %v", err)
	}

	// Log file format diagnostic information
	log.Printf("PostgreSQL log formatı istatistikleri: Standart=%d, TimezoneYok=%d, Syslog=%d, Generic=%d, CSV=%d, Fallback=%d, Bilinmeyen=%d",
		formatStats["standard"], formatStats["notz"], formatStats["syslog"], formatStats["generic"],
		formatStats["csv"], formatStats["fallback"], formatStats["unknown"])

	if len(sampleLines) > 0 {
		log.Printf("Log dosyası format örnekleri (ilk %d satır):", len(sampleLines))
		for i, sample := range sampleLines {
			log.Printf("  Satır %d: %s", i+1, sample)
		}
	}

	log.Printf("PostgreSQL log analizi tamamlandı: Toplam satır=%d, Geçerli girdiler=%d, Yavaş sorgular=%d, Çok satırlı sorgular=%d, Ayrıştırılamayan=%d",
		totalLines, validEntries, slowQueries, multiLineQueries, unparsedLines)

	// Yanıt olarak hangi log girdilerini döndüreceğimize karar ver
	var resultEntries []*pb.PostgresLogEntry

	// Eğer yavaş sorgu eşiği belirlenmişse ve yavaş sorgular varsa, onları döndür
	if slowQueryThresholdMs > 0 && len(slowQueryEntries) > 0 {
		resultEntries = slowQueryEntries
		log.Printf("Yavaş sorgu eşiğini (%d ms) geçen %d adet girdi döndürülüyor", slowQueryThresholdMs, len(resultEntries))
	} else if slowQueryThresholdMs > 0 && len(slowQueryEntries) == 0 {
		// Yavaş sorgu eşiği belirlenmişse ama yavaş sorgu yoksa, son 24 saatlik logları döndür
		if len(last24HourEntries) > 0 {
			resultEntries = last24HourEntries
			log.Printf("Yavaş sorgu bulunamadı, son 24 saate ait %d adet log kaydı döndürülüyor", len(resultEntries))
		} else {
			// Son 24 saate ait log yoksa, tüm loglardan örnek bir grup döndür (en fazla 100 girdi)
			maxSample := 100
			if len(logEntries) > maxSample {
				// Son 100 girdiyi al
				resultEntries = logEntries[len(logEntries)-maxSample:]
			} else {
				resultEntries = logEntries
			}
			log.Printf("Yavaş sorgu ve son 24 saatlik log bulunamadı, örnek %d adet log kaydı döndürülüyor", len(resultEntries))
		}
	} else {
		// Yavaş sorgu eşiği belirtilmemişse, tüm geçerli girdileri döndür
		resultEntries = logEntries
		log.Printf("Tüm geçerli girdiler döndürülüyor (%d adet)", len(resultEntries))
	}

	// Eğer hiç geçerli kayıt bulunamadıysa, raw satırlardan en az bir örnek döndür
	if len(resultEntries) == 0 && totalLines > 0 {
		log.Printf("Hiç geçerli log girişi bulunamadı, ilk satırlar ham veri olarak döndürülüyor")

		// Dosyayı baştan açıp ilk birkaç satırı oku
		file.Seek(0, 0)
		scanner = bufio.NewScanner(file)

		// Şu anki zaman için varsayılan timestamp
		currentTime := time.Now().Unix()

		// En fazla 20 satır oku
		sampleCount := 0
		for scanner.Scan() && sampleCount < 20 {
			line := scanner.Text()
			if len(strings.TrimSpace(line)) > 0 {
				// Her satırı ham şekilde bir log girdisi olarak ekle
				entry := &pb.PostgresLogEntry{
					Timestamp: currentTime,
					LogLevel:  "INFO",
					Message:   fmt.Sprintf("RAW_LOG: %s", line),
				}
				resultEntries = append(resultEntries, entry)
				sampleCount++
			}
		}

		log.Printf("Ham veri olarak %d satır döndürülüyor", len(resultEntries))
	}

	return &pb.PostgresLogAnalyzeResponse{
		LogEntries: resultEntries,
	}, nil
}

// parseLogLine parses a log line into the buffer
func parseLogLine(line string, matches []string, buffer *LogBuffer) bool {
	// Parse timestamp
	timestampStr := strings.TrimSpace(matches[1])

	var timestamp time.Time
	var err error

	// Try different timestamp formats based on which regex matched
	if strings.Contains(timestampStr, "+") || strings.Contains(timestampStr, "-") && strings.Contains(timestampStr, ":") {
		// Format with timezone: 2025-04-19 13:08:29.513 +03
		// Convert +03 format to +0300 format for proper parsing
		if strings.Contains(timestampStr, " +03") {
			timestampStr = strings.Replace(timestampStr, " +03", " +0300", 1)
		} else if strings.Contains(timestampStr, " -03") {
			timestampStr = strings.Replace(timestampStr, " -03", " -0300", 1)
		}

		// Try parsing with explicit timezone offset
		timestamp, err = time.Parse("2006-01-02 15:04:05.000 -0700", timestampStr)
		if err != nil {
			// Try without milliseconds
			timestamp, err = time.Parse("2006-01-02 15:04:05 -0700", timestampStr)
		}
	} else if strings.Contains(timestampStr, "-") && strings.Contains(timestampStr, ":") {
		// Format without timezone: 2025-04-19 13:08:29.513
		timestamp, err = time.Parse("2006-01-02 15:04:05.000", timestampStr)
		if err != nil {
			// Try without milliseconds
			timestamp, err = time.Parse("2006-01-02 15:04:05", timestampStr)
		}
	} else {
		// Syslog format: May 15 10:49:45
		currentYear := time.Now().Year()
		timestampWithYear := fmt.Sprintf("%d %s", currentYear, timestampStr)
		timestamp, err = time.Parse("2006 Jan 2 15:04:05", timestampWithYear)

		if err != nil {
			// Try alternative syslog formats
			formats := []string{
				"2006 Jan 2 15:04:05",
				"2006 Jan _2 15:04:05",
				"Jan 2 15:04:05 2006",
				"Jan _2 15:04:05 2006",
			}

			for _, format := range formats {
				timestamp, err = time.Parse(format, timestampWithYear)
				if err == nil {
					break
				}
			}
		}
	}

	if err != nil {
		log.Printf("Timestamp ayrıştırma hatası (%s): %v", timestampStr, err)
		return false
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
	} else if strings.Contains(message, "execute <unnamed>:") {
		// Alternative format for statement logs
		parts := strings.SplitN(message, "execute <unnamed>:", 2)
		if len(parts) > 1 {
			query := strings.TrimSpace(parts[1])
			buffer.query = append(buffer.query, query)
			buffer.isStatement = true

			// Check if this might be a multi-line statement
			if statementStartRegex.MatchString(query) && !statementEndRegex.MatchString(query) {
				buffer.isMultiLine = true
			}
		}
	} else if strings.Contains(message, "execute ") && strings.Contains(message, ": ") {
		// Another format: "execute S_1: SELECT..."
		parts := strings.SplitN(message, ": ", 2)
		if len(parts) > 1 && statementStartRegex.MatchString(parts[1]) {
			query := strings.TrimSpace(parts[1])
			buffer.query = append(buffer.query, query)
			buffer.isStatement = true

			// Check if this might be a multi-line statement
			if !statementEndRegex.MatchString(query) {
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
func processEntry(entry *pb.PostgresLogEntry, logEntries, last24HourEntries, slowQueryEntries *[]*pb.PostgresLogEntry, last24Hours int64, slowQueryThresholdMs int64, slowQueries *int) {
	// Tüm geçerli girdileri logEntries'e ekle
	*logEntries = append(*logEntries, entry)

	// Son 24 saatlik logları ayrı bir slice'da tut
	if entry.Timestamp >= last24Hours {
		*last24HourEntries = append(*last24HourEntries, entry)
		log.Printf("Son 24 saat logu bulundu: %s", entry.Message)
	}

	// Yavaş sorguları ayrı bir slice'da tut
	if entry.DurationMs > 0 && entry.DurationMs >= slowQueryThresholdMs {
		*slowQueries++
		*slowQueryEntries = append(*slowQueryEntries, entry)
		log.Printf("Yavaş sorgu bulundu (%d ms): %s", entry.DurationMs, entry.InternalQuery)
	}
}
