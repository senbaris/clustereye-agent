package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Log levels
const (
	LevelDebug = iota
	LevelInfo
	LevelWarning
	LevelError
	LevelFatal
)

var levelNames = map[int]string{
	LevelDebug:   "DEBUG",
	LevelInfo:    "INFO",
	LevelWarning: "WARNING",
	LevelError:   "ERROR",
	LevelFatal:   "FATAL",
}

// Global logger instance
var (
	stdLogger  *Logger
	loggerOnce sync.Once
	logLevel   = LevelWarning // Default to WARNING level
)

// Initialize logger package with desired log level
func init() {
	// Create and configure the standard logger
	stdLogger = NewLogger(os.Stderr, logLevel)

	// Override standard log output with our filtered writer
	log.SetOutput(&logWriter{level: logLevel})

	// Set standard log flags
	log.SetFlags(log.LstdFlags)
}

// Logger is a custom logger that supports log levels
type Logger struct {
	mu         sync.Mutex
	out        io.Writer
	level      int
	prefix     string
	timeFormat string
}

// NewLogger creates a new logger with the specified writer and log level
func NewLogger(out io.Writer, level int) *Logger {
	return &Logger{
		out:        out,
		level:      level,
		timeFormat: "2006/01/02 15:04:05",
	}
}

// SetOutput sets the output destination for the logger
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = w
}

// SetLevel sets the minimum log level to display
func (l *Logger) SetLevel(level int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current log level
func (l *Logger) GetLevel() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// SetPrefix sets the logger prefix
func (l *Logger) SetPrefix(prefix string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prefix = prefix
}

// log is the internal logging function
func (l *Logger) log(level int, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Skip if this message's level is lower than the current level
	if level < l.level {
		return
	}

	// Format the message with level name
	levelName, ok := levelNames[level]
	if !ok {
		levelName = "UNKNOWN"
	}

	// Format time
	timeStr := time.Now().Format(l.timeFormat)

	// Create the final message
	var message string
	if len(args) > 0 {
		message = fmt.Sprintf(format, args...)
	} else {
		message = format
	}

	// Format the complete log line with timestamp, level, and message
	logLine := fmt.Sprintf("%s [%s] %s%s\n", timeStr, levelName, l.prefix, message)

	// Write to output
	l.out.Write([]byte(logLine))

	// If this is a fatal log, exit after logging
	if level == LevelFatal {
		os.Exit(1)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(LevelDebug, format, args...)
}

// Info logs an info message
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(LevelInfo, format, args...)
}

// Warning logs a warning message
func (l *Logger) Warning(format string, args ...interface{}) {
	l.log(LevelWarning, format, args...)
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(LevelError, format, args...)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(LevelFatal, format, args...)
}

// Debug logs a debug message
func Debug(format string, args ...interface{}) {
	stdLogger.Debug(format, args...)
}

// Info logs an info message
func Info(format string, args ...interface{}) {
	stdLogger.Info(format, args...)
}

// Warning logs a warning message
func Warning(format string, args ...interface{}) {
	stdLogger.Warning(format, args...)
}

// Error logs an error message
func Error(format string, args ...interface{}) {
	stdLogger.Error(format, args...)
}

// Fatal logs a fatal message and exits
func Fatal(format string, args ...interface{}) {
	stdLogger.Fatal(format, args...)
}

// Printf prints a message at INFO level (for drop-in replacement of log.Printf)
func Printf(format string, args ...interface{}) {
	stdLogger.Info(format, args...)
}

// Println prints a message at INFO level (for drop-in replacement of log.Println)
func Println(v ...interface{}) {
	s := fmt.Sprintln(v...)
	stdLogger.Info(s)
}

// Fatalf prints a message at FATAL level and exits (for drop-in replacement of log.Fatalf)
func Fatalf(format string, args ...interface{}) {
	stdLogger.Fatal(format, args...)
}

// SetLevel sets the global log level
func SetLevel(level int) {
	stdLogger.SetLevel(level)
	logLevel = level // Update package-level variable for new writers
}

// GetLevel returns the global log level
func GetLevel() int {
	return stdLogger.GetLevel()
}

// ParseLevel converts a level string to its integer value
func ParseLevel(level string) int {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return LevelDebug
	case "INFO":
		return LevelInfo
	case "WARNING", "WARN":
		return LevelWarning
	case "ERROR":
		return LevelError
	case "FATAL":
		return LevelFatal
	default:
		return LevelInfo // Default to INFO
	}
}

// LevelToString converts a level integer to its string representation
func LevelToString(level int) string {
	if name, ok := levelNames[level]; ok {
		return name
	}
	return "UNKNOWN"
}

// logWriter implements io.Writer and filters standard log messages
type logWriter struct {
	level int
}

// Write implements io.Writer for processing standard log package output
func (w *logWriter) Write(p []byte) (n int, err error) {
	message := string(p)

	// Try to detect the log level from the message content
	detectedLevel := detectLogLevel(message)

	// Skip messages below our threshold
	if detectedLevel < w.level {
		return len(p), nil // Pretend we wrote it
	}

	// Write to stderr
	return os.Stderr.Write(p)
}

// detectLogLevel examines a log message to guess its level
func detectLogLevel(message string) int {
	// Check for explicit level markers
	if strings.Contains(message, "[DEBUG]") {
		return LevelDebug
	} else if strings.Contains(message, "[INFO]") {
		return LevelInfo
	} else if strings.Contains(message, "[WARNING]") || strings.Contains(message, "[WARN]") {
		return LevelWarning
	} else if strings.Contains(message, "[ERROR]") {
		return LevelError
	} else if strings.Contains(message, "[FATAL]") {
		return LevelFatal
	}

	// Check for error-like messages
	if strings.Contains(message, "başarısız") ||
		strings.Contains(message, "hata") ||
		strings.Contains(message, "hatası") ||
		strings.Contains(message, "failed") ||
		strings.Contains(message, "error") ||
		strings.Contains(message, "invalid") ||
		strings.Contains(message, "bulunamadı") ||
		strings.Contains(message, "not found") ||
		strings.Contains(message, "alamadı") {
		return LevelError
	}

	// Check for warning-like messages
	if strings.Contains(message, "uyarı") ||
		strings.Contains(message, "dikkat") ||
		strings.Contains(message, "warning") {
		return LevelWarning
	}

	// Check for info-like messages
	if strings.Contains(message, "başarıyla") ||
		strings.Contains(message, "raporlandı") ||
		strings.Contains(message, "bilgileri toplanıyor") ||
		strings.Contains(message, "alınıyor") ||
		strings.Contains(message, "başlatılıyor") ||
		strings.Contains(message, "kontrolü başlıyor") ||
		strings.Contains(message, "kontrol ediliyor") ||
		strings.Contains(message, "toplandı") ||
		strings.Contains(message, "starting") ||
		strings.Contains(message, "successfully") ||
		strings.Contains(message, "completed") {
		return LevelInfo
	}

	// Default to INFO for unrecognized messages
	return LevelInfo
}

// SetOutput sets the output destination for the logger
func SetOutput(w io.Writer) {
	stdLogger.SetOutput(w)
}
