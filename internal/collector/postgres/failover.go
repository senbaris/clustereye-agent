package postgres

import (
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

// PostgreSQLFailoverManager PostgreSQL failover işlemlerini yönetir
type PostgreSQLFailoverManager struct {
	cfg *config.AgentConfig
}

// NewPostgreSQLFailoverManager yeni bir failover manager oluşturur
func NewPostgreSQLFailoverManager(cfg *config.AgentConfig) *PostgreSQLFailoverManager {
	return &PostgreSQLFailoverManager{
		cfg: cfg,
	}
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

// ConvertToSlaveWithIP master node'unu slave'e dönüştürür (direkt IP ile - DNS çözümlemesi yapmaz)
func (fm *PostgreSQLFailoverManager) ConvertToSlaveWithIP(dataDir, masterIP string, newMasterPort int, replUser, replPassword, pgVersion string) error {
	log.Printf("Master->Slave dönüşüm başlatılıyor (IP ile): %s -> %s:%d (version: %s)", dataDir, masterIP, newMasterPort, pgVersion)
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 1 - IP adresi direkt kullanılıyor: %s", masterIP)

	// Eski data directory'yi backup al ve temizle
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 2 - Data directory backup/temizleme başlatılıyor")
	err := fm.backupAndCleanDataDirectory(dataDir)
	if err != nil {
		return fmt.Errorf("data directory backup ve temizleme başarısız: %v", err)
	}
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 2 - Data directory backup/temizleme tamamlandı")

	// pg_basebackup ile fresh backup al (-R parametresi ile standby konfigürasyonu otomatik oluşturulur)
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 3 - pg_basebackup başlatılıyor")
	err = fm.performBaseBackup(masterIP, newMasterPort, replUser, replPassword, dataDir)
	if err != nil {
		return fmt.Errorf("pg_basebackup başarısız: %v", err)
	}
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 3 - pg_basebackup tamamlandı")

	log.Printf("pg_basebackup -R parametresi ile standby konfigürasyonu otomatik oluşturuldu (standby.signal ve postgresql.auto.conf)")

	// PostgreSQL'i standby modunda başlat (pg_ctl ile)
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 4 - PostgreSQL standby modunda başlatılıyor")
	err = fm.startPostgreSQLAsStandby(dataDir, pgVersion)
	if err != nil {
		return fmt.Errorf("PostgreSQL standby modunda başlatılamadı: %v", err)
	}
	log.Printf("DEBUG: ConvertToSlaveWithIP adım 4 - PostgreSQL standby modunda başlatıldı")

	return nil
}

// StopPostgreSQLService PostgreSQL servisini durdurur
func (fm *PostgreSQLFailoverManager) StopPostgreSQLService(pgVersion string) error {
	log.Printf("PostgreSQL servisi durduruluyor (version: %s)", pgVersion)

	// pg_ctl ile durdur
	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd == "" {
		return fmt.Errorf("pg_ctl komutu bulunamadı")
	}

	// Önce graceful shutdown dene
	cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "stop", "-m", "fast")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Graceful shutdown başarısız: %v - Çıktı: %s", err, string(output))
		// Force shutdown dene
		cmd = exec.Command("sudo", "-u", "postgres", pgCtlCmd, "stop", "-m", "immediate")
		output, err = cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("PostgreSQL durdurulamadı: %v - Çıktı: %s", err, string(output))
		}
	}

	log.Printf("PostgreSQL başarıyla durduruldu")
	return nil
}

// PromoteToMaster standby node'unu master'a yükseltir
func (fm *PostgreSQLFailoverManager) PromoteToMaster(dataDir string) error {
	log.Printf("PostgreSQL promotion başlatılıyor: %s", dataDir)

	// PostgreSQL version al
	pgVersion := GetPGVersion()
	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd == "" {
		return fmt.Errorf("pg_ctl komutu bulunamadı")
	}

	// pg_ctl promote komutu ile yükselt
	cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "promote", "-D", dataDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("promotion başarısız: %v - Çıktı: %s", err, string(output))
	}

	log.Printf("PostgreSQL promotion başarılı: %s", string(output))
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
	log.Printf("PostgreSQL standby modunda başlatılıyor: %s", dataDir)

	pgCtlCmd := fm.findPgCtlCommand(pgVersion)
	if pgCtlCmd == "" {
		return fmt.Errorf("pg_ctl komutu bulunamadı")
	}

	// pg_ctl ile başlat
	cmd := exec.Command("sudo", "-u", "postgres", pgCtlCmd, "start", "-D", dataDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("PostgreSQL standby modunda başlatılamadı: %v - Çıktı: %s", err, string(output))
	}

	log.Printf("PostgreSQL standby modunda başarıyla başlatıldı: %s", string(output))
	return nil
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
	log.Printf("Data directory backup alınıyor ve temizleniyor: %s", dataDir)

	// Backup directory oluştur
	backupDir := fmt.Sprintf("%s_backup_%d", dataDir, time.Now().Unix())

	// Data directory'nin var olup olmadığını kontrol et
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		log.Printf("Data directory zaten yok: %s", dataDir)
		return nil
	}

	// Backup al
	log.Printf("Data directory backup alınıyor: %s -> %s", dataDir, backupDir)
	cmd := exec.Command("sudo", "-u", "postgres", "cp", "-r", dataDir, backupDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Backup alma başarısız: %v - Çıktı: %s", err, string(output))
		// Backup başarısız olsa da devam et, sadece uyar
	} else {
		log.Printf("Data directory backup başarıyla alındı: %s", backupDir)
	}

	// Data directory içeriğini temizle (dizini silme, sadece içeriği temizle)
	log.Printf("Data directory içeriği temizleniyor: %s", dataDir)
	cmd = exec.Command("sudo", "-u", "postgres", "find", dataDir, "-mindepth", "1", "-delete")
	output, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("data directory temizleme başarısız: %v - Çıktı: %s", err, string(output))
	}

	log.Printf("Data directory başarıyla temizlendi")
	return nil
}

// performBaseBackup pg_basebackup ile fresh backup alır
func (fm *PostgreSQLFailoverManager) performBaseBackup(masterIP string, masterPort int, replUser, replPassword, dataDir string) error {
	log.Printf("pg_basebackup başlatılıyor: %s:%d -> %s", masterIP, masterPort, dataDir)

	// pg_basebackup komutunu oluştur
	// PGPASSWORD=replicator_password pg_basebackup -h <new_primary_ip> -D /var/lib/postgresql/15/main -U replicator -Fp -Xs -P
	basebackupCmd := fmt.Sprintf("PGPASSWORD=%s pg_basebackup -h %s -p %d -D %s -U %s -Fp -Xs -P -R",
		replPassword, masterIP, masterPort, dataDir, replUser)

	log.Printf("pg_basebackup komutu çalıştırılıyor: %s", basebackupCmd)
	log.Printf("DEBUG: Master IP: %s, Port: %d, User: %s, DataDir: %s", masterIP, masterPort, replUser, dataDir)

	// Komutu postgres kullanıcısı olarak çalıştır
	cmd := exec.Command("sudo", "-u", "postgres", "bash", "-c", basebackupCmd)

	// Çıktıyı yakala
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("pg_basebackup hatası: %v", err)
		log.Printf("pg_basebackup çıktısı: %s", string(output))
		return fmt.Errorf("pg_basebackup başarısız: %v - Çıktı: %s", err, string(output))
	}

	log.Printf("pg_basebackup çıktısı: %s", string(output))

	log.Printf("pg_basebackup başarıyla tamamlandı")

	// Data directory ownership'ini postgres kullanıcısına ayarla
	log.Printf("Data directory ownership düzeltiliyor: %s", dataDir)
	chownCmd := exec.Command("sudo", "chown", "-R", "postgres:postgres", dataDir)
	chownOutput, chownErr := chownCmd.CombinedOutput()
	if chownErr != nil {
		log.Printf("UYARI: Ownership düzeltme başarısız: %v - Çıktı: %s", chownErr, string(chownOutput))
		// Ownership hatası kritik değil, uyarı olarak devam et
	} else {
		log.Printf("Data directory ownership başarıyla düzeltildi")
	}

	// Data directory permissions'ını ayarla (700 - sadece postgres kullanıcısı erişebilir)
	log.Printf("Data directory permissions düzeltiliyor: %s", dataDir)
	chmodCmd := exec.Command("sudo", "chmod", "700", dataDir)
	chmodOutput, chmodErr := chmodCmd.CombinedOutput()
	if chmodErr != nil {
		log.Printf("UYARI: Permissions düzeltme başarısız: %v - Çıktı: %s", chmodErr, string(chmodOutput))
		// Permissions hatası kritik değil, uyarı olarak devam et
	} else {
		log.Printf("Data directory permissions başarıyla düzeltildi (700)")
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
