package postgres

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/senbaris/clustereye-agent/internal/config"
)

// FailoverState failover işleminin durumunu takip eder
type FailoverState struct {
	JobID              string            `json:"job_id"`
	StartTime          time.Time         `json:"start_time"`
	CurrentStep        string            `json:"current_step"`
	CompletedSteps     []string          `json:"completed_steps"`
	BackupPaths        map[string]string `json:"backup_paths"`    // step -> backup_path
	OriginalConfig     map[string]string `json:"original_config"` // config_file -> original_content
	OriginalNodeStatus string            `json:"original_node_status"`
	OriginalMasterHost string            `json:"original_master_host"`
	OriginalMasterIP   string            `json:"original_master_ip"`
	OriginalMasterPort int               `json:"original_master_port"`
	NewMasterHost      string            `json:"new_master_host"`
	NewMasterIP        string            `json:"new_master_ip"`
	NewMasterPort      int               `json:"new_master_port"`
	DataDirectory      string            `json:"data_directory"`
	PostgreSQLVersion  string            `json:"postgresql_version"`
	ReplicationUser    string            `json:"replication_user"`
	CanRollback        bool              `json:"can_rollback"`
	RollbackReason     string            `json:"rollback_reason,omitempty"`
	LastError          string            `json:"last_error,omitempty"`
}

// PostgreSQLFailoverManager PostgreSQL failover işlemlerini yönetir
type PostgreSQLFailoverManager struct {
	cfg           *config.AgentConfig
	currentState  *FailoverState
	stateFilePath string
}

// NewPostgreSQLFailoverManager yeni bir failover manager oluşturur
func NewPostgreSQLFailoverManager(cfg *config.AgentConfig) *PostgreSQLFailoverManager {
	stateDir := "/tmp/clustereye-failover"
	os.MkdirAll(stateDir, 0755)

	return &PostgreSQLFailoverManager{
		cfg:           cfg,
		stateFilePath: filepath.Join(stateDir, "failover-state.json"),
	}
}

// SaveState failover durumunu dosyaya kaydeder
func (fm *PostgreSQLFailoverManager) SaveState() error {
	if fm.currentState == nil {
		return nil
	}

	data, err := json.MarshalIndent(fm.currentState, "", "  ")
	if err != nil {
		return fmt.Errorf("state serialization failed: %v", err)
	}

	err = os.WriteFile(fm.stateFilePath, data, 0600)
	if err != nil {
		return fmt.Errorf("state file write failed: %v", err)
	}

	log.Printf("Failover state saved: step=%s, can_rollback=%v", fm.currentState.CurrentStep, fm.currentState.CanRollback)
	return nil
}

// LoadState failover durumunu dosyadan yükler
func (fm *PostgreSQLFailoverManager) LoadState() error {
	if _, err := os.Stat(fm.stateFilePath); os.IsNotExist(err) {
		return nil // State file yok, normal
	}

	data, err := os.ReadFile(fm.stateFilePath)
	if err != nil {
		return fmt.Errorf("state file read failed: %v", err)
	}

	fm.currentState = &FailoverState{}
	err = json.Unmarshal(data, fm.currentState)
	if err != nil {
		return fmt.Errorf("state deserialization failed: %v", err)
	}

	log.Printf("Failover state loaded: job_id=%s, step=%s, can_rollback=%v",
		fm.currentState.JobID, fm.currentState.CurrentStep, fm.currentState.CanRollback)
	return nil
}

// ClearState failover durumunu temizler
func (fm *PostgreSQLFailoverManager) ClearState() error {
	fm.currentState = nil
	if _, err := os.Stat(fm.stateFilePath); err == nil {
		return os.Remove(fm.stateFilePath)
	}
	return nil
}

// InitializeFailoverState yeni bir failover işlemi için state başlatır
func (fm *PostgreSQLFailoverManager) InitializeFailoverState(jobID, dataDir, newMasterHost, newMasterIP string, newMasterPort int) error {
	// Mevcut durumu kaydet
	originalNodeStatus := GetNodeStatus()
	pgVersion := GetPGVersion()

	fm.currentState = &FailoverState{
		JobID:              jobID,
		StartTime:          time.Now(),
		CurrentStep:        "initialized",
		CompletedSteps:     []string{},
		BackupPaths:        make(map[string]string),
		OriginalConfig:     make(map[string]string),
		OriginalNodeStatus: originalNodeStatus,
		NewMasterHost:      newMasterHost,
		NewMasterIP:        newMasterIP,
		NewMasterPort:      newMasterPort,
		DataDirectory:      dataDir,
		PostgreSQLVersion:  pgVersion,
		CanRollback:        true,
	}

	// Replication credentials'ı al
	replUser := fm.cfg.PostgreSQL.ReplicationUser
	if replUser == "" {
		replUser = fm.cfg.PostgreSQL.User
	}
	fm.currentState.ReplicationUser = replUser

	return fm.SaveState()
}

// InitializePromotionState promotion işlemi için state başlatır
func (fm *PostgreSQLFailoverManager) InitializePromotionState(jobID, dataDir string) error {
	originalNodeStatus := GetNodeStatus()
	pgVersion := GetPGVersion()

	fm.currentState = &FailoverState{
		JobID:              jobID,
		StartTime:          time.Now(),
		CurrentStep:        "promotion_initialized",
		CompletedSteps:     []string{},
		BackupPaths:        make(map[string]string),
		OriginalConfig:     make(map[string]string),
		OriginalNodeStatus: originalNodeStatus,
		DataDirectory:      dataDir,
		PostgreSQLVersion:  pgVersion,
		CanRollback:        true,
	}

	return fm.SaveState()
}

// MarkStepCompleted bir adımı tamamlandı olarak işaretler
func (fm *PostgreSQLFailoverManager) MarkStepCompleted(step string) error {
	if fm.currentState == nil {
		return fmt.Errorf("no active failover state")
	}

	fm.currentState.CompletedSteps = append(fm.currentState.CompletedSteps, step)
	fm.currentState.CurrentStep = step

	// Kritik adımlardan sonra rollback imkansız hale gelir
	criticalSteps := []string{"data_directory_cleaned", "pg_basebackup_completed", "promotion_completed"}
	for _, criticalStep := range criticalSteps {
		if step == criticalStep {
			fm.currentState.CanRollback = false
			fm.currentState.RollbackReason = fmt.Sprintf("Critical step completed: %s", step)
			break
		}
	}

	return fm.SaveState()
}

// AddBackupPath bir backup yolunu state'e ekler
func (fm *PostgreSQLFailoverManager) AddBackupPath(step, backupPath string) error {
	if fm.currentState == nil {
		return fmt.Errorf("no active failover state")
	}

	fm.currentState.BackupPaths[step] = backupPath
	return fm.SaveState()
}

// SaveOriginalConfig orijinal konfigürasyonu saklar
func (fm *PostgreSQLFailoverManager) SaveOriginalConfig(configFile string) error {
	if fm.currentState == nil {
		return fmt.Errorf("no active failover state")
	}

	content, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %v", configFile, err)
	}

	fm.currentState.OriginalConfig[configFile] = string(content)
	return fm.SaveState()
}

// SetError bir hata durumunu state'e kaydeder
func (fm *PostgreSQLFailoverManager) SetError(err error) {
	if fm.currentState != nil {
		fm.currentState.LastError = err.Error()
		fm.SaveState()
	}
}

// CanRollback rollback yapılıp yapılamayacağını kontrol eder
func (fm *PostgreSQLFailoverManager) CanRollback() (bool, string) {
	if fm.currentState == nil {
		return false, "No active failover state"
	}

	if !fm.currentState.CanRollback {
		return false, fm.currentState.RollbackReason
	}

	// Zaman kontrolü - 1 saatten eski işlemler için rollback riskli
	if time.Since(fm.currentState.StartTime) > time.Hour {
		return false, "Failover too old (>1 hour), rollback risky"
	}

	return true, ""
}

// RollbackFailover failover işlemini geri alır
func (fm *PostgreSQLFailoverManager) RollbackFailover(logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	// State yükle
	if err := fm.LoadState(); err != nil {
		return fmt.Errorf("failed to load failover state: %v", err)
	}

	if fm.currentState == nil {
		return fmt.Errorf("no failover state found")
	}

	canRollback, reason := fm.CanRollback()
	if !canRollback {
		return fmt.Errorf("rollback not possible: %s", reason)
	}

	logMessage(fmt.Sprintf("Starting rollback for job %s (step: %s)", fm.currentState.JobID, fm.currentState.CurrentStep))

	// Rollback adımlarını ters sırada uygula
	completedSteps := fm.currentState.CompletedSteps
	for i := len(completedSteps) - 1; i >= 0; i-- {
		step := completedSteps[i]
		logMessage(fmt.Sprintf("Rolling back step: %s", step))

		switch step {
		case "postgresql_stopped":
			if err := fm.rollbackPostgreSQLStop(logger); err != nil {
				logMessage(fmt.Sprintf("Failed to rollback PostgreSQL stop: %v", err))
				// Continue with other rollback steps
			}

		case "config_backup_created":
			if err := fm.rollbackConfigChanges(logger); err != nil {
				logMessage(fmt.Sprintf("Failed to rollback config changes: %v", err))
			}

		case "data_directory_backed_up":
			if err := fm.rollbackDataDirectoryChanges(logger); err != nil {
				logMessage(fmt.Sprintf("Failed to rollback data directory: %v", err))
			}

		case "standby_config_created":
			if err := fm.rollbackStandbyConfig(logger); err != nil {
				logMessage(fmt.Sprintf("Failed to rollback standby config: %v", err))
			}

		default:
			logMessage(fmt.Sprintf("No rollback action defined for step: %s", step))
		}
	}

	// PostgreSQL'i orijinal durumda başlat
	if err := fm.restoreOriginalState(logger); err != nil {
		logMessage(fmt.Sprintf("Failed to restore original state: %v", err))
		return err
	}

	// State'i temizle
	if err := fm.ClearState(); err != nil {
		logMessage(fmt.Sprintf("Failed to clear state: %v", err))
	}

	logMessage("Rollback completed successfully")
	return nil
}

// rollbackPostgreSQLStop PostgreSQL'i yeniden başlatır
func (fm *PostgreSQLFailoverManager) rollbackPostgreSQLStop(logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	logMessage("Rolling back PostgreSQL stop - starting PostgreSQL")
	return fm.StartPostgreSQLService(fm.currentState.PostgreSQLVersion)
}

// rollbackConfigChanges konfigürasyon değişikliklerini geri alır
func (fm *PostgreSQLFailoverManager) rollbackConfigChanges(logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	for configFile, originalContent := range fm.currentState.OriginalConfig {
		logMessage(fmt.Sprintf("Restoring original config: %s", configFile))

		err := os.WriteFile(configFile, []byte(originalContent), 0600)
		if err != nil {
			return fmt.Errorf("failed to restore config %s: %v", configFile, err)
		}

		logMessage(fmt.Sprintf("Config restored: %s", configFile))
	}

	return nil
}

// rollbackDataDirectoryChanges data directory değişikliklerini geri alır
func (fm *PostgreSQLFailoverManager) rollbackDataDirectoryChanges(logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	backupPath, exists := fm.currentState.BackupPaths["data_directory_backed_up"]
	if !exists {
		logMessage("No data directory backup found, skipping restore")
		return nil
	}

	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", backupPath)
	}

	dataDir := fm.currentState.DataDirectory
	logMessage(fmt.Sprintf("Restoring data directory from backup: %s -> %s", backupPath, dataDir))

	// Mevcut data directory'yi temizle
	cmd := exec.Command("sudo", "-u", "postgres", "find", dataDir, "-mindepth", "1", "-delete")
	if output, err := cmd.CombinedOutput(); err != nil {
		logMessage(fmt.Sprintf("Warning: failed to clean data directory: %v - %s", err, string(output)))
	}

	// Backup'tan restore et
	cmd = exec.Command("sudo", "-u", "postgres", "cp", "-r", backupPath+"/.", dataDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to restore data directory: %v - %s", err, string(output))
	}

	logMessage("Data directory restored successfully")
	return nil
}

// rollbackStandbyConfig standby konfigürasyonunu kaldırır
func (fm *PostgreSQLFailoverManager) rollbackStandbyConfig(logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	dataDir := fm.currentState.DataDirectory

	// standby.signal dosyasını kaldır
	standbySignalPath := filepath.Join(dataDir, "standby.signal")
	if _, err := os.Stat(standbySignalPath); err == nil {
		if err := os.Remove(standbySignalPath); err != nil {
			logMessage(fmt.Sprintf("Warning: failed to remove standby.signal: %v", err))
		} else {
			logMessage("standby.signal removed")
		}
	}

	// recovery.signal dosyasını kaldır
	recoverySignalPath := filepath.Join(dataDir, "recovery.signal")
	if _, err := os.Stat(recoverySignalPath); err == nil {
		if err := os.Remove(recoverySignalPath); err != nil {
			logMessage(fmt.Sprintf("Warning: failed to remove recovery.signal: %v", err))
		} else {
			logMessage("recovery.signal removed")
		}
	}

	return nil
}

// restoreOriginalState orijinal PostgreSQL durumunu restore eder
func (fm *PostgreSQLFailoverManager) restoreOriginalState(logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	logMessage("Restoring original PostgreSQL state")

	// PostgreSQL'i başlat
	if err := fm.StartPostgreSQLService(fm.currentState.PostgreSQLVersion); err != nil {
		return fmt.Errorf("failed to start PostgreSQL: %v", err)
	}

	// Node durumunu kontrol et
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		time.Sleep(2 * time.Second)
		currentStatus := GetNodeStatus()

		logMessage(fmt.Sprintf("Current node status: %s (expected: %s)", currentStatus, fm.currentState.OriginalNodeStatus))

		if currentStatus == fm.currentState.OriginalNodeStatus {
			logMessage("Original node status restored successfully")
			return nil
		}
	}

	return fmt.Errorf("failed to restore original node status within timeout")
}

// GetRollbackInfo mevcut rollback durumu hakkında bilgi verir
func (fm *PostgreSQLFailoverManager) GetRollbackInfo() (*FailoverState, error) {
	if err := fm.LoadState(); err != nil {
		return nil, err
	}
	return fm.currentState, nil
}

// ConvertToSlave master node'unu slave'e dönüştürür (hostname ile - DNS çözümlemesi yapar)
func (fm *PostgreSQLFailoverManager) ConvertToSlave(dataDir, newMasterHost string, newMasterPort int, replUser, replPassword, pgVersion string) error {
	// Hostname'i IP adresine çevir
	masterIP, err := fm.resolveHostnameToIPWithError(newMasterHost)
	if err != nil {
		return fmt.Errorf("hostname IP'ye çevrilemedi (%s): %v", newMasterHost, err)
	}
	log.Printf("Master->Slave dönüşüm başlatılıyor: %s -> %s:%d (IP: %s, version: %s)", dataDir, newMasterHost, newMasterPort, masterIP, pgVersion)
	log.Printf("DEBUG: ConvertToSlave adım 1 - Hostname çözümlendi: %s -> %s", newMasterHost, masterIP)

	// IP adresi ile devam et
	return fm.ConvertToSlaveWithIP(dataDir, masterIP, newMasterPort, replUser, replPassword, pgVersion)
}

// Logger interface for process logging
type Logger interface {
	LogMessage(message string)
	LogError(message string, err error)
}

// ConvertToSlaveWithIP master node'unu slave'e dönüştürür (direkt IP ile - DNS çözümlemesi yapmaz)
func (fm *PostgreSQLFailoverManager) ConvertToSlaveWithIP(dataDir, masterIP string, newMasterPort int, replUser, replPassword, pgVersion string) error {
	return fm.ConvertToSlaveWithIPAndLogger(dataDir, masterIP, newMasterPort, replUser, replPassword, pgVersion, nil)
}

// ConvertToSlaveWithIPAndLogger master node'unu slave'e dönüştürür (logger ile)
func (fm *PostgreSQLFailoverManager) ConvertToSlaveWithIPAndLogger(dataDir, masterIP string, newMasterPort int, replUser, replPassword, pgVersion string, logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	// Failover state'i başlat
	jobID := fmt.Sprintf("convert_to_slave_%d", time.Now().Unix())
	if err := fm.InitializeFailoverState(jobID, dataDir, "", masterIP, newMasterPort); err != nil {
		logMessage(fmt.Sprintf("Failed to initialize failover state: %v", err))
	}

	// Otomatik rollback wrapper - TÜM HATA DURUMLARI İÇİN
	var rollbackPerformed bool = false
	defer func() {
		if r := recover(); r != nil {
			logMessage(fmt.Sprintf("Panic during conversion: %v", r))
			if canRollback, _ := fm.CanRollback(); canRollback && !rollbackPerformed {
				logMessage("Attempting automatic rollback due to panic")
				if rollbackErr := fm.RollbackFailover(logger); rollbackErr != nil {
					logMessage(fmt.Sprintf("Automatic rollback failed: %v", rollbackErr))
				} else {
					logMessage("Automatic rollback completed successfully")
				}
				rollbackPerformed = true
			}
		}
	}()

	// Otomatik rollback fonksiyonu - normal hatalar için
	performAutoRollback := func(reason string, originalErr error) error {
		if rollbackPerformed {
			return originalErr // Zaten rollback yapılmış
		}

		canRollback, rollbackReason := fm.CanRollback()
		if !canRollback {
			logMessage(fmt.Sprintf("Automatic rollback not possible: %s", rollbackReason))
			return originalErr
		}

		logMessage(fmt.Sprintf("Performing automatic rollback due to: %s", reason))
		if rollbackErr := fm.RollbackFailover(logger); rollbackErr != nil {
			logMessage(fmt.Sprintf("Automatic rollback failed: %v", rollbackErr))
			return fmt.Errorf("original error: %v; rollback also failed: %v", originalErr, rollbackErr)
		}

		logMessage("Automatic rollback completed successfully")
		rollbackPerformed = true
		return fmt.Errorf("operation failed (%s) but automatic rollback completed: %v", reason, originalErr)
	}

	logMessage(fmt.Sprintf("Master->Slave dönüşüm başlatılıyor (IP ile): %s -> %s:%d (version: %s)", dataDir, masterIP, newMasterPort, pgVersion))
	logMessage(fmt.Sprintf("DEBUG: ConvertToSlaveWithIP adım 1 - IP adresi direkt kullanılıyor: %s", masterIP))

	// PostgreSQL'i durdur
	logMessage("DEBUG: ConvertToSlaveWithIP adım 1.5 - PostgreSQL durduruluyor")
	if err := fm.StopPostgreSQLService(pgVersion); err != nil {
		fm.SetError(err)
		return performAutoRollback("PostgreSQL stop failed", fmt.Errorf("PostgreSQL durdurulamadı: %v", err))
	}
	fm.MarkStepCompleted("postgresql_stopped")

	// Eski data directory'yi backup al ve temizle
	logMessage("DEBUG: ConvertToSlaveWithIP adım 2 - Data directory backup/temizleme başlatılıyor")
	err := fm.backupAndCleanDataDirectoryWithLogger(dataDir, logger)
	if err != nil {
		fm.SetError(err)
		return performAutoRollback("Data directory backup/cleanup failed", fmt.Errorf("data directory backup ve temizleme başarısız: %v", err))
	}
	logMessage("DEBUG: ConvertToSlaveWithIP adım 2 - Data directory backup/temizleme tamamlandı")

	// pg_basebackup ile fresh backup al (-R parametresi ile standby konfigürasyonu otomatik oluşturulur)
	logMessage("DEBUG: ConvertToSlaveWithIP adım 3 - pg_basebackup başlatılıyor")
	err = fm.performBaseBackupWithLogger(masterIP, newMasterPort, replUser, replPassword, dataDir, logger)
	if err != nil {
		fm.SetError(err)
		return performAutoRollback("pg_basebackup failed", fmt.Errorf("pg_basebackup başarısız: %v", err))
	}
	fm.MarkStepCompleted("pg_basebackup_completed")
	logMessage("DEBUG: ConvertToSlaveWithIP adım 3 - pg_basebackup tamamlandı")

	logMessage("pg_basebackup -R parametresi ile standby konfigürasyonu otomatik oluşturuldu (standby.signal ve postgresql.auto.conf)")

	// PostgreSQL'i standby modunda başlat (pg_ctl ile)
	logMessage("DEBUG: ConvertToSlaveWithIP adım 4 - PostgreSQL standby modunda başlatılıyor")
	err = fm.startPostgreSQLAsStandbyWithLogger(dataDir, pgVersion, logger)
	if err != nil {
		fm.SetError(err)
		return performAutoRollback("PostgreSQL standby start failed", fmt.Errorf("PostgreSQL standby modunda başlatılamadı: %v", err))
	}
	logMessage("DEBUG: ConvertToSlaveWithIP adım 4 - PostgreSQL standby modunda başlatıldı")

	// Başarılı tamamlama - state'i temizle
	fm.ClearState()
	logMessage("Master->Slave conversion completed successfully - no rollback needed")
	return nil
}

// StopPostgreSQLService PostgreSQL servisini durdurur
func (fm *PostgreSQLFailoverManager) StopPostgreSQLService(pgVersion string) error {
	log.Printf("PostgreSQL servisi durduruluyor (version: %s)", pgVersion)

	// PostgreSQL version'ını parse et
	majorVersionInt, err := fm.parsePGVersion(pgVersion)
	if err != nil {
		log.Printf("PostgreSQL version parse edilemedi: %v, varsayılan 15 kullanılacak", err)
		majorVersionInt = 15
	}

	// Cluster-aware servis adlarını dene
	serviceNames := []string{
		fmt.Sprintf("postgresql@%d-main", majorVersionInt), // Ubuntu cluster: postgresql@15-main
		"postgresql", // Genel: postgresql
		fmt.Sprintf("postgresql-%d", majorVersionInt), // RHEL/CentOS: postgresql-15
		"postgresql.service",                          // Açık service adı
	}

	// Systemctl ile cluster-aware durdurma deneyi
	for _, serviceName := range serviceNames {
		log.Printf("Systemctl ile PostgreSQL durduruluyor: %s", serviceName)
		cmd := exec.Command("systemctl", "stop", serviceName)
		output, err := cmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi systemctl ile durduruldu: %s", serviceName)
			return nil
		}
		log.Printf("DEBUG: %s servisi systemctl ile durdurulamadı: %v - Çıktı: %s", serviceName, err, string(output))
	}

	// pg_ctl ile durdurma deneyi
	log.Printf("Systemctl başarısız, pg_ctl ile deneniyor...")
	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd != "" {
		// Önce graceful shutdown dene
		cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "stop", "-m", "fast")
		output, err := cmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi pg_ctl ile durduruldu (fast mode)")
			return nil
		}
		log.Printf("pg_ctl fast shutdown başarısız: %v - Çıktı: %s", err, string(output))

		// Force shutdown dene
		cmd = exec.Command("sudo", "-u", "postgres", pgCtlCmd, "stop", "-m", "immediate")
		output, err = cmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi pg_ctl ile durduruldu (immediate mode)")
			return nil
		}
		log.Printf("pg_ctl immediate shutdown başarısız: %v - Çıktı: %s", err, string(output))
	} else {
		log.Printf("pg_ctl komutu bulunamadı")
	}

	// Service komutu ile durdurma deneyi (eski sistemler için)
	for _, serviceName := range serviceNames {
		log.Printf("Service komutu ile PostgreSQL durduruluyor: %s", serviceName)
		serviceCmd := exec.Command("service", serviceName, "stop")
		output, err := serviceCmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi service komutu ile durduruldu: %s", serviceName)
			return nil
		}
		log.Printf("DEBUG: %s servisi service komutu ile durdurulamadı: %v - Çıktı: %s", serviceName, err, string(output))
	}

	return fmt.Errorf("PostgreSQL servisi durdurulamadı - tüm yöntemler başarısız (systemctl, pg_ctl, service)")
}

// StartPostgreSQLService PostgreSQL servisini başlatır
func (fm *PostgreSQLFailoverManager) StartPostgreSQLService(pgVersion string) error {
	log.Printf("PostgreSQL servisi başlatılıyor (version: %s)", pgVersion)

	// PostgreSQL version'ını parse et
	majorVersionInt, err := fm.parsePGVersion(pgVersion)
	if err != nil {
		log.Printf("PostgreSQL version parse edilemedi: %v, varsayılan 15 kullanılacak", err)
		majorVersionInt = 15
	}

	// Cluster-aware servis adlarını dene
	serviceNames := []string{
		fmt.Sprintf("postgresql@%d-main", majorVersionInt), // Ubuntu cluster: postgresql@15-main
		"postgresql", // Genel: postgresql
		fmt.Sprintf("postgresql-%d", majorVersionInt), // RHEL/CentOS: postgresql-15
		"postgresql.service",                          // Açık service adı
	}

	// Systemctl ile cluster-aware başlatma deneyi
	for _, serviceName := range serviceNames {
		log.Printf("Systemctl ile PostgreSQL başlatılıyor: %s", serviceName)
		cmd := exec.Command("systemctl", "start", serviceName)
		output, err := cmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi systemctl ile başlatıldı: %s", serviceName)
			return nil
		}
		log.Printf("DEBUG: %s servisi systemctl ile başlatılamadı: %v - Çıktı: %s", serviceName, err, string(output))
	}

	// pg_ctl ile başlatma deneyi
	log.Printf("Systemctl başarısız, pg_ctl ile deneniyor...")
	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd != "" {
		// Data directory'yi bul
		dataDir, err := GetDataDirectory()
		if err != nil {
			log.Printf("Data directory bulunamadı: %v", err)
			dataDir = "/var/lib/postgresql/data" // Fallback
		}

		// pg_ctl ile başlat
		cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "start", "-D", dataDir)
		output, err := cmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi pg_ctl ile başlatıldı: %s", string(output))
			return nil
		}
		log.Printf("pg_ctl ile başlatma başarısız: %v - Çıktı: %s", err, string(output))
	} else {
		log.Printf("pg_ctl komutu bulunamadı")
	}

	// Service komutu ile başlatma deneyi (eski sistemler için)
	for _, serviceName := range serviceNames {
		log.Printf("Service komutu ile PostgreSQL başlatılıyor: %s", serviceName)
		serviceCmd := exec.Command("service", serviceName, "start")
		output, err := serviceCmd.CombinedOutput()
		if err == nil {
			log.Printf("PostgreSQL servisi service komutu ile başlatıldı: %s", serviceName)
			return nil
		}
		log.Printf("DEBUG: %s servisi service komutu ile başlatılamadı: %v - Çıktı: %s", serviceName, err, string(output))
	}

	return fmt.Errorf("PostgreSQL servisi başlatılamadı - tüm yöntemler başarısız (systemctl, pg_ctl, service)")
}

// PromoteToMaster standby node'unu master'a yükseltir
func (fm *PostgreSQLFailoverManager) PromoteToMaster(dataDir string) error {
	return fm.PromoteToMasterWithLogger(dataDir, nil)
}

// PromoteToMasterWithLogger standby node'unu master'a yükseltir (logger ile + otomatik rollback)
func (fm *PostgreSQLFailoverManager) PromoteToMasterWithLogger(dataDir string, logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	logMessage(fmt.Sprintf("PostgreSQL promotion başlatılıyor: %s", dataDir))

	// Promotion state'i başlat
	jobID := fmt.Sprintf("promotion_%d", time.Now().Unix())
	if err := fm.InitializePromotionState(jobID, dataDir); err != nil {
		logMessage(fmt.Sprintf("Failed to initialize promotion state: %v", err))
	}

	// Otomatik rollback wrapper - TÜM HATA DURUMLARI İÇİN
	var rollbackPerformed bool = false
	defer func() {
		if r := recover(); r != nil {
			logMessage(fmt.Sprintf("Panic during promotion: %v", r))
			if canRollback, _ := fm.CanRollback(); canRollback && !rollbackPerformed {
				logMessage("Attempting automatic rollback due to panic")
				if rollbackErr := fm.RollbackFailover(logger); rollbackErr != nil {
					logMessage(fmt.Sprintf("Automatic rollback failed: %v", rollbackErr))
				} else {
					logMessage("Automatic rollback completed successfully")
				}
				rollbackPerformed = true
			}
		}
	}()

	// Otomatik rollback fonksiyonu - normal hatalar için
	performAutoRollback := func(reason string, originalErr error) error {
		if rollbackPerformed {
			return originalErr // Zaten rollback yapılmış
		}

		canRollback, rollbackReason := fm.CanRollback()
		if !canRollback {
			logMessage(fmt.Sprintf("Automatic rollback not possible: %s", rollbackReason))
			return originalErr
		}

		logMessage(fmt.Sprintf("Performing automatic rollback due to: %s", reason))
		if rollbackErr := fm.RollbackFailover(logger); rollbackErr != nil {
			logMessage(fmt.Sprintf("Automatic rollback failed: %v", rollbackErr))
			return fmt.Errorf("original error: %v; rollback also failed: %v", originalErr, rollbackErr)
		}

		logMessage("Automatic rollback completed successfully")
		rollbackPerformed = true
		return fmt.Errorf("promotion failed (%s) but automatic rollback completed: %v", reason, originalErr)
	}

	// PostgreSQL version al
	pgVersion := GetPGVersion()
	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd == "" {
		fm.SetError(fmt.Errorf("pg_ctl komutu bulunamadı"))
		return performAutoRollback("pg_ctl command not found", fmt.Errorf("pg_ctl komutu bulunamadı"))
	}

	// pg_ctl promote komutu ile yükselt
	logMessage(fmt.Sprintf("pg_ctl promote komutu çalıştırılıyor: %s", pgCtlCmd))
	cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "promote", "-D", dataDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fm.SetError(err)
		return performAutoRollback("pg_ctl promote failed", fmt.Errorf("promotion başarısız: %v - Çıktı: %s", err, string(output)))
	}

	// Başarılı tamamlama - state'i temizle
	fm.ClearState()
	logMessage(fmt.Sprintf("PostgreSQL promotion başarılı: %s", string(output)))
	logMessage("Promotion completed successfully - no rollback needed")
	return nil
}

// CheckReplicationSlots replication slot'larının varlığını kontrol eder
func (fm *PostgreSQLFailoverManager) CheckReplicationSlots() ([]string, error) {
	db, err := OpenDB()
	if err != nil {
		return nil, fmt.Errorf("veritabanı bağlantısı açılamadı: %v", err)
	}
	defer db.Close()

	query := `SELECT slot_name FROM pg_replication_slots WHERE active = true`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("replication slot sorgusu başarısız: %v", err)
	}
	defer rows.Close()

	var slots []string
	for rows.Next() {
		var slotName string
		if err := rows.Scan(&slotName); err != nil {
			continue
		}
		slots = append(slots, slotName)
	}

	return slots, nil
}

// resolveHostnameToIP hostname'i IP adresine çevirir
func (fm *PostgreSQLFailoverManager) resolveHostnameToIP(hostname string) string {
	log.Printf("DEBUG: resolveHostnameToIP çağrıldı - hostname: %s", hostname)

	// Eğer zaten IP adresi ise, olduğu gibi döndür
	if net.ParseIP(hostname) != nil {
		log.Printf("Hostname zaten IP adresi: %s", hostname)
		return hostname
	}

	// Hostname'i IP'ye çevir
	log.Printf("DEBUG: net.LookupIP çağrılıyor - hostname: %s", hostname)
	ips, err := net.LookupIP(hostname)
	if err != nil {
		log.Printf("HATA: Hostname çözümlenemedi (%s), bu durumda pg_basebackup başarısız olacak: %v", hostname, err)
		// DNS çözümlemesi başarısız olduğunda hostname döndürme, hata ver
		log.Printf("KRITIK: DNS çözümlemesi başarısız, pg_basebackup çalışmayacak")
		return hostname // Hala hostname döndürüyoruz ama bu sorunlu
	}

	log.Printf("DEBUG: net.LookupIP başarılı - %d IP adresi bulundu", len(ips))

	// İlk IPv4 adresini kullan
	for i, ip := range ips {
		log.Printf("DEBUG: IP %d: %s (IPv4: %v)", i, ip.String(), ip.To4() != nil)
		if ipv4 := ip.To4(); ipv4 != nil {
			log.Printf("Hostname çözümlendi: %s -> %s", hostname, ipv4.String())
			return ipv4.String()
		}
	}

	// IPv4 bulunamadıysa, ilk IP'yi kullan
	if len(ips) > 0 {
		log.Printf("IPv4 bulunamadı, ilk IP kullanılıyor: %s -> %s", hostname, ips[0].String())
		return ips[0].String()
	}

	log.Printf("HATA: IP çözümlenemedi, hostname olarak kullanılacak ama pg_basebackup başarısız olacak: %s", hostname)
	return hostname
}

// resolveHostnameToIPWithError hostname'i IP adresine çevirir ve hata döndürür
func (fm *PostgreSQLFailoverManager) resolveHostnameToIPWithError(hostname string) (string, error) {
	log.Printf("DEBUG: resolveHostnameToIPWithError çağrıldı - hostname: %s", hostname)

	// Eğer zaten IP adresi ise, olduğu gibi döndür
	if net.ParseIP(hostname) != nil {
		log.Printf("Hostname zaten IP adresi: %s", hostname)
		return hostname, nil
	}

	// Hostname'i IP'ye çevir
	log.Printf("DEBUG: net.LookupIP çağrılıyor - hostname: %s", hostname)
	ips, err := net.LookupIP(hostname)
	if err != nil {
		return "", fmt.Errorf("DNS çözümlemesi başarısız: %v", err)
	}

	log.Printf("DEBUG: net.LookupIP başarılı - %d IP adresi bulundu", len(ips))

	// İlk IPv4 adresini kullan
	for i, ip := range ips {
		log.Printf("DEBUG: IP %d: %s (IPv4: %v)", i, ip.String(), ip.To4() != nil)
		if ipv4 := ip.To4(); ipv4 != nil {
			log.Printf("Hostname çözümlendi: %s -> %s", hostname, ipv4.String())
			return ipv4.String(), nil
		}
	}

	// IPv4 bulunamadıysa, ilk IP'yi kullan
	if len(ips) > 0 {
		log.Printf("IPv4 bulunamadı, ilk IP kullanılıyor: %s -> %s", hostname, ips[0].String())
		return ips[0].String(), nil
	}

	return "", fmt.Errorf("hiçbir IP adresi bulunamadı")
}

// createStandbyConfigurationWithVersion standby konfigürasyon dosyalarını oluşturur
func (fm *PostgreSQLFailoverManager) createStandbyConfigurationWithVersion(dataDir, masterIP string, masterPort int, replUser, replPassword, pgVersion string) error {
	log.Printf("Standby konfigürasyonu oluşturuluyor: %s -> %s:%d (version: %s)", dataDir, masterIP, masterPort, pgVersion)

	// PostgreSQL version'ını parse et
	majorVersionInt, err := fm.parsePGVersion(pgVersion)
	if err != nil {
		return fmt.Errorf("PostgreSQL version parse edilemedi: %v", err)
	}

	if majorVersionInt >= 12 {
		// PostgreSQL 12+ için postgresql.auto.conf ve standby.signal
		postgresqlAutoConfPath := filepath.Join(dataDir, "postgresql.auto.conf")
		standbySignalPath := filepath.Join(dataDir, "standby.signal")

		// postgresql.auto.conf'u güncelle
		err = fm.updatePostgreSQLAutoConf(postgresqlAutoConfPath, masterIP, masterPort, replUser, replPassword)
		if err != nil {
			return fmt.Errorf("postgresql.auto.conf güncellenemedi: %v", err)
		}

		// standby.signal dosyasını oluştur
		err = fm.createStandbySignal(standbySignalPath)
		if err != nil {
			return fmt.Errorf("standby.signal oluşturulamadı: %v", err)
		}
	} else {
		// PostgreSQL 11 ve öncesi için recovery.conf
		recoveryConfPath := filepath.Join(dataDir, "recovery.conf")
		err = fm.createRecoveryConf(recoveryConfPath, masterIP, masterPort, replUser, replPassword)
		if err != nil {
			return fmt.Errorf("recovery.conf oluşturulamadı: %v", err)
		}
	}

	return nil
}

// updatePostgreSQLAutoConf postgresql.auto.conf dosyasını günceller
func (fm *PostgreSQLFailoverManager) updatePostgreSQLAutoConf(confPath, masterIP string, masterPort int, replUser, replPassword string) error {
	log.Printf("postgresql.auto.conf dosyası güncelleniyor: %s", confPath)

	// Mevcut dosyayı oku
	content, err := os.ReadFile(confPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("postgresql.auto.conf okunamadı: %v", err)
	}

	// Primary connection info
	primaryConnInfo := fmt.Sprintf("primary_conninfo = 'host=%s port=%d user=%s password=%s application_name=standby'",
		masterIP, masterPort, replUser, replPassword)

	// Mevcut primary_conninfo satırlarını kaldır
	lines := strings.Split(string(content), "\n")
	var newLines []string
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		// primary_conninfo ile başlayan satırları atla
		if !strings.HasPrefix(trimmedLine, "primary_conninfo") && !strings.HasPrefix(trimmedLine, "#primary_conninfo") {
			newLines = append(newLines, line)
		}
	}

	// Yeni primary_conninfo'yu ekle
	newLines = append(newLines, "")
	newLines = append(newLines, "# Standby configuration added by ClusterEye")
	newLines = append(newLines, primaryConnInfo)

	// Dosyayı yaz
	newContent := strings.Join(newLines, "\n")
	err = os.WriteFile(confPath, []byte(newContent), 0600)
	if err != nil {
		return fmt.Errorf("postgresql.auto.conf yazılamadı: %v", err)
	}

	log.Printf("postgresql.auto.conf başarıyla güncellendi")
	return nil
}

// createStandbySignal standby.signal dosyasını oluşturur
func (fm *PostgreSQLFailoverManager) createStandbySignal(signalPath string) error {
	log.Printf("standby.signal dosyası oluşturuluyor: %s", signalPath)

	// Boş dosya oluştur
	file, err := os.Create(signalPath)
	if err != nil {
		return fmt.Errorf("standby.signal oluşturulamadı: %v", err)
	}
	file.Close()

	log.Printf("standby.signal başarıyla oluşturuldu")
	return nil
}

// createRecoveryConf recovery.conf dosyası oluşturur (PostgreSQL 11 ve öncesi)
func (fm *PostgreSQLFailoverManager) createRecoveryConf(recoveryPath, masterIP string, masterPort int, replUser, replPassword string) error {
	log.Printf("recovery.conf dosyası oluşturuluyor: %s", recoveryPath)

	// Recovery.conf içeriği (streaming replication için)
	recoveryContent := fmt.Sprintf(`# Recovery configuration created by ClusterEye (streaming replication)
standby_mode = 'on'
primary_conninfo = 'host=%s port=%d user=%s password=%s application_name=standby'
trigger_file = '%s/promote.trigger'
`,
		masterIP, masterPort, replUser, replPassword, filepath.Dir(recoveryPath))

	// Dosyayı oluştur
	err := os.WriteFile(recoveryPath, []byte(recoveryContent), 0600)
	if err != nil {
		return fmt.Errorf("recovery.conf oluşturulamadı: %v", err)
	}

	log.Printf("recovery.conf başarıyla oluşturuldu (streaming replication için)")
	return nil
}

// startPostgreSQLAsStandby PostgreSQL'i standby modunda başlatır
func (fm *PostgreSQLFailoverManager) startPostgreSQLAsStandby(dataDir, pgVersion string) error {
	return fm.startPostgreSQLAsStandbyWithLogger(dataDir, pgVersion, nil)
}

// startPostgreSQLAsStandbyWithLogger PostgreSQL'i standby modunda başlatır (logger ile)
func (fm *PostgreSQLFailoverManager) startPostgreSQLAsStandbyWithLogger(dataDir, pgVersion string, logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	logMessage(fmt.Sprintf("PostgreSQL standby modunda başlatılıyor: %s", dataDir))

	// PostgreSQL version'ını parse et
	majorVersionInt, err := fm.parsePGVersion(pgVersion)
	if err != nil {
		logMessage(fmt.Sprintf("PostgreSQL version parse edilemedi: %v, varsayılan 15 kullanılacak", err))
		majorVersionInt = 15
	}

	// Cluster-aware servis adlarını dene
	serviceNames := []string{
		fmt.Sprintf("postgresql@%d-main", majorVersionInt), // Ubuntu cluster: postgresql@15-main
		"postgresql", // Genel: postgresql
		fmt.Sprintf("postgresql-%d", majorVersionInt), // RHEL/CentOS: postgresql-15
		"postgresql.service",                          // Açık service adı
	}

	// Systemctl ile cluster-aware başlatma deneyi
	for _, serviceName := range serviceNames {
		logMessage(fmt.Sprintf("Systemctl ile PostgreSQL başlatılmaya çalışılıyor: %s", serviceName))
		cmd := exec.Command("systemctl", "start", serviceName)
		output, err := cmd.CombinedOutput()
		if err == nil {
			logMessage(fmt.Sprintf("PostgreSQL servisi systemctl ile başlatıldı: %s", serviceName))
			return nil
		}
		logMessage(fmt.Sprintf("DEBUG: %s servisi systemctl ile başlatılamadı: %v - Çıktı: %s", serviceName, err, string(output)))
	}

	// Systemctl başarısız olduysa pg_ctl ile dene
	logMessage("Systemctl başarısız, pg_ctl ile deneniyor...")
	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd != "" {
		// pg_ctl ile başlat
		cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "start", "-D", dataDir)
		output, err := cmd.CombinedOutput()
		if err == nil {
			logMessage(fmt.Sprintf("PostgreSQL standby modunda pg_ctl ile başarıyla başlatıldı: %s", string(output)))
			return nil
		}
		logMessage(fmt.Sprintf("pg_ctl ile başlatma başarısız: %v - Çıktı: %s", err, string(output)))
	} else {
		logMessage("pg_ctl komutu bulunamadı")
	}

	// Service komutu ile başlatma deneyi (eski sistemler için)
	for _, serviceName := range serviceNames {
		logMessage(fmt.Sprintf("Service komutu ile PostgreSQL başlatılıyor: %s", serviceName))
		serviceCmd := exec.Command("service", serviceName, "start")
		output, err := serviceCmd.CombinedOutput()
		if err == nil {
			logMessage(fmt.Sprintf("PostgreSQL servisi service komutu ile başlatıldı: %s", serviceName))
			return nil
		}
		logMessage(fmt.Sprintf("DEBUG: %s servisi service komutu ile başlatılamadı: %v - Çıktı: %s", serviceName, err, string(output)))
	}

	return fmt.Errorf("PostgreSQL standby modunda başlatılamadı - tüm yöntemler başarısız (systemctl, pg_ctl, service)")
}

// findPgCtlCommand pg_ctl komutunu bulur
func (fm *PostgreSQLFailoverManager) findPgCtlCommand(pgVersion string) string {
	// pg_ctl komutunu bul - birkaç olası yolu dene
	pgCtlPaths := []string{
		"pg_ctl",                           // Normal PATH'te varsa
		"/usr/lib/postgresql/*/bin/pg_ctl", // Debian/Ubuntu
		"/usr/pgsql-*/bin/pg_ctl",          // RHEL/CentOS
		"/usr/local/bin/pg_ctl",            // Homebrew/MacOS
		"/opt/PostgreSQL/*/bin/pg_ctl",     // EnterpriseDB
		"/var/lib/postgresql/*/bin/pg_ctl", // Custom
		"/usr/bin/pg_ctl",                  // Alternative
		"/bin/pg_ctl",                      // Alternative
		fmt.Sprintf("/usr/lib/postgresql/%s/bin/pg_ctl", pgVersion), // Sürüm belirterek
	}

	for _, path := range pgCtlPaths {
		// Glob pattern'leri genişlet
		if strings.Contains(path, "*") {
			matches, err := filepath.Glob(path)
			if err == nil && len(matches) > 0 {
				// En son sürümü seç
				return matches[len(matches)-1]
			}
		} else {
			// Normal dosya kontrolü
			_, err := exec.LookPath(path)
			if err == nil {
				return path
			}
		}
	}

	return ""
}

// parsePGVersion PostgreSQL version string'ini parse eder
func (fm *PostgreSQLFailoverManager) parsePGVersion(pgVersion string) (int, error) {
	// "PostgreSQL 14.2" gibi formatları parse et
	re := regexp.MustCompile(`(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(pgVersion)
	if len(matches) >= 2 {
		majorVersion, err := strconv.Atoi(matches[1])
		if err != nil {
			return 0, fmt.Errorf("major version parse edilemedi: %v", err)
		}
		return majorVersion, nil
	}

	// Sadece sayı varsa ("14" gibi)
	if majorVersion, err := strconv.Atoi(strings.TrimSpace(pgVersion)); err == nil {
		return majorVersion, nil
	}

	return 0, fmt.Errorf("PostgreSQL version formatı tanınmıyor: %s", pgVersion)
}

// backupAndCleanDataDirectory eski data directory'yi backup alır ve temizler
func (fm *PostgreSQLFailoverManager) backupAndCleanDataDirectory(dataDir string) error {
	return fm.backupAndCleanDataDirectoryWithLogger(dataDir, nil)
}

// backupAndCleanDataDirectoryWithLogger eski data directory'yi backup alır ve temizler (logger ile)
func (fm *PostgreSQLFailoverManager) backupAndCleanDataDirectoryWithLogger(dataDir string, logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	logMessage(fmt.Sprintf("Data directory backup alınıyor ve temizleniyor: %s", dataDir))

	// Backup directory oluştur
	backupDir := fmt.Sprintf("%s_backup_%d", dataDir, time.Now().Unix())

	// Data directory'nin var olup olmadığını kontrol et
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		logMessage(fmt.Sprintf("Data directory zaten yok: %s", dataDir))
		return nil
	}

	// Backup al
	logMessage(fmt.Sprintf("Data directory backup alınıyor: %s -> %s", dataDir, backupDir))
	cmd := exec.Command("sudo", "-u", "postgres", "cp", "-r", dataDir, backupDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logMessage(fmt.Sprintf("Backup alma başarısız: %v - Çıktı: %s", err, string(output)))
		// Backup başarısız olsa da devam et, sadece uyar
	} else {
		logMessage(fmt.Sprintf("Data directory backup başarıyla alındı: %s", backupDir))
		// Backup path'ini state'e kaydet
		fm.AddBackupPath("data_directory_backed_up", backupDir)
	}

	// Data directory içeriğini temizle (dizini silme, sadece içeriği temizle)
	logMessage(fmt.Sprintf("Data directory içeriği temizleniyor: %s", dataDir))
	cmd = exec.Command("sudo", "-u", "postgres", "find", dataDir, "-mindepth", "1", "-delete")
	output, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("data directory temizleme başarısız: %v - Çıktı: %s", err, string(output))
	}

	logMessage("Data directory başarıyla temizlendi")
	fm.MarkStepCompleted("data_directory_backed_up")
	return nil
}

// performBaseBackup pg_basebackup ile fresh backup alır
func (fm *PostgreSQLFailoverManager) performBaseBackup(masterIP string, masterPort int, replUser, replPassword, dataDir string) error {
	return fm.performBaseBackupWithLogger(masterIP, masterPort, replUser, replPassword, dataDir, nil)
}

// performBaseBackupWithLogger pg_basebackup ile fresh backup alır (logger ile)
func (fm *PostgreSQLFailoverManager) performBaseBackupWithLogger(masterIP string, masterPort int, replUser, replPassword, dataDir string, logger Logger) error {
	logMessage := func(msg string) {
		log.Printf(msg)
		if logger != nil {
			logger.LogMessage(msg)
		}
	}

	logMessage(fmt.Sprintf("pg_basebackup başlatılıyor: %s:%d -> %s", masterIP, masterPort, dataDir))

	// pg_basebackup komutunu oluştur
	// PGPASSWORD=replicator_password pg_basebackup -h <new_primary_ip> -D /var/lib/postgresql/15/main -U replicator -Fp -Xs -P
	basebackupCmd := fmt.Sprintf("PGPASSWORD=%s pg_basebackup -h %s -p %d -D %s -U %s -Fp -Xs -P -R",
		replPassword, masterIP, masterPort, dataDir, replUser)

	logMessage(fmt.Sprintf("pg_basebackup komutu çalıştırılıyor: PGPASSWORD=*** pg_basebackup -h %s -p %d -D %s -U %s -Fp -Xs -P -R", masterIP, masterPort, dataDir, replUser))
	logMessage(fmt.Sprintf("DEBUG: Master IP: %s, Port: %d, User: %s, DataDir: %s", masterIP, masterPort, replUser, dataDir))

	// Komutu postgres kullanıcısı olarak çalıştır
	cmd := exec.Command("sudo", "-u", "postgres", "bash", "-c", basebackupCmd)

	// Çıktıyı yakala
	output, err := cmd.CombinedOutput()
	if err != nil {
		logMessage(fmt.Sprintf("pg_basebackup hatası: %v", err))
		logMessage(fmt.Sprintf("pg_basebackup çıktısı: %s", string(output)))
		return fmt.Errorf("pg_basebackup başarısız: %v - Çıktı: %s", err, string(output))
	}

	logMessage(fmt.Sprintf("pg_basebackup çıktısı: %s", string(output)))
	logMessage("pg_basebackup başarıyla tamamlandı")

	// Data directory ownership'ini postgres kullanıcısına ayarla
	logMessage(fmt.Sprintf("Data directory ownership düzeltiliyor: %s", dataDir))
	chownCmd := exec.Command("sudo", "chown", "-R", "postgres:postgres", dataDir)
	chownOutput, chownErr := chownCmd.CombinedOutput()
	if chownErr != nil {
		logMessage(fmt.Sprintf("UYARI: Ownership düzeltme başarısız: %v - Çıktı: %s", chownErr, string(chownOutput)))
		// Ownership hatası kritik değil, uyarı olarak devam et
	} else {
		logMessage("Data directory ownership başarıyla düzeltildi")
	}

	// Data directory permissions'ını ayarla (700 - sadece postgres kullanıcısı erişebilir)
	logMessage(fmt.Sprintf("Data directory permissions düzeltiliyor: %s", dataDir))
	chmodCmd := exec.Command("sudo", "chmod", "700", dataDir)
	chmodOutput, chmodErr := chmodCmd.CombinedOutput()
	if chmodErr != nil {
		logMessage(fmt.Sprintf("UYARI: Permissions düzeltme başarısız: %v - Çıktı: %s", chmodErr, string(chmodOutput)))
		// Permissions hatası kritik değil, uyarı olarak devam et
	} else {
		logMessage("Data directory permissions başarıyla düzeltildi (700)")
	}

	return nil
}

// ReconfigureSlaveToNewMaster mevcut slave'i yeni master'a yönlendirir
func (fm *PostgreSQLFailoverManager) ReconfigureSlaveToNewMaster(dataDir, newMasterIP string, newMasterPort int, replUser, replPassword, pgVersion string) error {
	log.Printf("Slave reconfiguration başlatılıyor: yeni master %s:%d", newMasterIP, newMasterPort)

	// PostgreSQL version'ını parse et
	majorVersionInt, err := fm.parsePGVersion(pgVersion)
	if err != nil {
		return fmt.Errorf("PostgreSQL version parse edilemedi: %v", err)
	}

	if majorVersionInt >= 12 {
		// PostgreSQL 12+ için postgresql.auto.conf güncelle
		postgresqlAutoConfPath := filepath.Join(dataDir, "postgresql.auto.conf")
		log.Printf("PostgreSQL 12+ için postgresql.auto.conf güncelleniyor: %s", postgresqlAutoConfPath)
		err = fm.updatePostgreSQLAutoConf(postgresqlAutoConfPath, newMasterIP, newMasterPort, replUser, replPassword)
		if err != nil {
			return fmt.Errorf("postgresql.auto.conf güncellenemedi: %v", err)
		}
	} else {
		// PostgreSQL 11 ve öncesi için recovery.conf güncelle
		recoveryConfPath := filepath.Join(dataDir, "recovery.conf")
		log.Printf("PostgreSQL 11- için recovery.conf güncelleniyor: %s", recoveryConfPath)
		err = fm.createRecoveryConf(recoveryConfPath, newMasterIP, newMasterPort, replUser, replPassword)
		if err != nil {
			return fmt.Errorf("recovery.conf güncellenemedi: %v", err)
		}
	}

	log.Printf("Slave başarıyla yeni master'a yönlendirildi: %s:%d", newMasterIP, newMasterPort)
	return nil
}
