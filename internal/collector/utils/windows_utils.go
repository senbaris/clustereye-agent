package utils

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/senbaris/clustereye-agent/internal/logger"
)

var (
	// PowerShell singleton instance management
	psMutex  sync.Mutex
	psCache  map[string]*exec.Cmd
	psActive bool

	// PowerShell result cache to avoid repeated identical calls
	psResultCache   map[string]cachedResult
	psResultMutex   sync.RWMutex
	maxCacheEntries = 100 // Önbellek boyutunu sınırla
)

// cachedResult yapısı, önbelleklenen PowerShell komut sonuçlarını saklar
type cachedResult struct {
	output    string
	timestamp time.Time
	err       error
}

func init() {
	psCache = make(map[string]*exec.Cmd)
	psResultCache = make(map[string]cachedResult)
}

// RunPowerShellCommand executes a PowerShell command efficiently with timeout
// and proper resource cleanup
func RunPowerShellCommand(command string, timeoutSeconds int) (string, error) {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 10 // Default timeout
	}

	// Check if command is in cache and still valid (30 second TTL for most commands)
	cacheKey := fmt.Sprintf("%s_%d", command, timeoutSeconds)
	psResultMutex.RLock()
	cachedRes, exists := psResultCache[cacheKey]
	psResultMutex.RUnlock()

	// Önbellek cache süresi - daha önce 5 saniyeydi, şimdi 30 saniye
	if exists && time.Since(cachedRes.timestamp) < 30*time.Second {
		logger.Debug("Using cached PowerShell result for command: %s", shortenCommand(command))
		return cachedRes.output, cachedRes.err
	}

	// Limit concurrent PowerShell executions
	psMutex.Lock()

	// Track active PowerShell processes and limit to 5 max
	psCount := len(psCache)
	if psCount >= 5 {
		// Remove oldest commands
		for cmdKey := range psCache {
			delete(psCache, cmdKey)
			if len(psCache) < 5 {
				break
			}
		}
	}

	psMutex.Unlock()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	// Create PowerShell command - using -NoProfile and -NonInteractive to improve performance
	cmd := exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-Command", command)

	// Execute and get output
	output, err := cmd.Output()

	// Cache the result regardless of success/failure
	result := cachedResult{
		output:    strings.TrimSpace(string(output)),
		timestamp: time.Now(),
		err:       err,
	}

	// Update cache with mutex protection
	psResultMutex.Lock()
	// Clean cache if too many entries
	if len(psResultCache) >= maxCacheEntries {
		// Clear half the entries
		count := 0
		for k := range psResultCache {
			delete(psResultCache, k)
			count++
			if count >= maxCacheEntries/2 {
				break
			}
		}
	}
	psResultCache[cacheKey] = result
	psResultMutex.Unlock()

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			logger.Error("PowerShell command timed out after %d seconds: %s", timeoutSeconds, shortenCommand(command))
			return "", fmt.Errorf("command timed out after %d seconds", timeoutSeconds)
		}
		return "", err
	}

	return result.output, nil
}

// shortenCommand trims a command for logging purposes
func shortenCommand(cmd string) string {
	if len(cmd) > 50 {
		return cmd[:47] + "..."
	}
	return cmd
}

// RunBatchCommands runs multiple PowerShell commands in a single PowerShell session
// This is much more efficient than running multiple separate PowerShell processes
func RunBatchCommands(commands []string, timeoutSeconds int) ([]string, []error) {
	if len(commands) == 0 {
		return nil, nil
	}

	if timeoutSeconds <= 0 {
		timeoutSeconds = 20 // Default longer timeout for batch commands
	}

	// Combine commands with output markers for parsing later
	var combinedCmd strings.Builder
	for i, cmd := range commands {
		combinedCmd.WriteString(fmt.Sprintf("Write-Output '===CMD_START_%d==='\n", i))
		combinedCmd.WriteString(cmd)
		combinedCmd.WriteString(fmt.Sprintf("\nWrite-Output '===CMD_END_%d==='\n", i))
	}

	// Run the combined command
	output, err := RunPowerShellCommand(combinedCmd.String(), timeoutSeconds)
	if err != nil {
		// If overall command failed, return error for all commands
		errors := make([]error, len(commands))
		for i := range errors {
			errors[i] = err
		}
		return nil, errors
	}

	// Parse individual command results
	results := make([]string, len(commands))
	errors := make([]error, len(commands))

	for i := range commands {
		startMarker := fmt.Sprintf("===CMD_START_%d===", i)
		endMarker := fmt.Sprintf("===CMD_END_%d===", i)

		startIdx := strings.Index(output, startMarker)
		if startIdx == -1 {
			errors[i] = fmt.Errorf("command %d result not found", i)
			continue
		}

		// Move past the marker
		startIdx += len(startMarker)

		endIdx := strings.Index(output[startIdx:], endMarker)
		if endIdx == -1 {
			errors[i] = fmt.Errorf("command %d end marker not found", i)
			continue
		}

		// Extract result
		cmdResult := strings.TrimSpace(output[startIdx : startIdx+endIdx])
		results[i] = cmdResult
	}

	return results, errors
}

// RunWMICommand runs a WMI command efficiently
func RunWMICommand(wmiClass, property, filter string, timeoutSeconds int) (string, error) {
	var command string

	// Build query based on whether we have a filter
	if filter != "" {
		command = fmt.Sprintf("Get-WmiObject -Class %s -Filter \"%s\" | Select-Object -ExpandProperty %s",
			wmiClass, filter, property)
	} else {
		command = fmt.Sprintf("Get-WmiObject -Class %s | Select-Object -ExpandProperty %s",
			wmiClass, property)
	}

	return RunPowerShellCommand(command, timeoutSeconds)
}

// RunCimCommand runs a CIM command efficiently (newer alternative to WMI)
func RunCimCommand(cimClass, property, filter string, timeoutSeconds int) (string, error) {
	var command string

	// Build query based on whether we have a filter
	if filter != "" {
		command = fmt.Sprintf("Get-CimInstance -ClassName %s -Filter \"%s\" | Select-Object -ExpandProperty %s",
			cimClass, filter, property)
	} else {
		command = fmt.Sprintf("Get-CimInstance -ClassName %s | Select-Object -ExpandProperty %s",
			cimClass, property)
	}

	return RunPowerShellCommand(command, timeoutSeconds)
}

// GetDiskInfo gets disk information using PowerShell
func GetDiskInfo(timeoutSeconds int) (string, error) {
	command := "Get-Volume | Where-Object {$_.DriveLetter} | " +
		"Select-Object DriveLetter, @{Name='UsedPercent';Expression={100 - (100 * $_.SizeRemaining / $_.Size)}}, " +
		"@{Name='Size';Expression={$_.Size}}, @{Name='FreeSpace';Expression={$_.SizeRemaining}} | ConvertTo-Json"

	return RunPowerShellCommand(command, timeoutSeconds)
}

// GetCPUUsage gets CPU usage information
func GetCPUUsage(timeoutSeconds int) (string, error) {
	// Try more efficient methods first
	command := "Get-CimInstance Win32_Processor | Measure-Object -Property LoadPercentage -Average | " +
		"Select-Object -ExpandProperty Average"

	return RunPowerShellCommand(command, timeoutSeconds)
}

// GetMemoryInfo gets memory information
func GetMemoryInfo(timeoutSeconds int) (string, error) {
	command := "Get-CimInstance Win32_OperatingSystem | " +
		"Select-Object TotalVisibleMemorySize, FreePhysicalMemory | ConvertTo-Json"

	return RunPowerShellCommand(command, timeoutSeconds)
}

// GetNetworkInfo gets network information
func GetNetworkInfo(timeoutSeconds int) (string, error) {
	command := "Get-NetIPAddress -AddressFamily IPv4 | " +
		"Where-Object { $_.IPAddress -notlike '127.0.0.*' -and $_.IPAddress -notlike '169.254.*' } | " +
		"Select-Object -First 1 -ExpandProperty IPAddress"

	return RunPowerShellCommand(command, timeoutSeconds)
}

// GetSQLServerRegistryInfo gets SQL Server registry information
func GetSQLServerRegistryInfo(instance string, regValue string, timeoutSeconds int) (string, error) {
	regPath := "MSSQL"
	if instance != "" {
		regPath = "MSSQL$" + instance
	}

	command := fmt.Sprintf("Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Microsoft SQL Server\\*\\%s\\Setup' -Name %s | Select-Object -ExpandProperty %s",
		regPath, regValue, regValue)

	return RunPowerShellCommand(command, timeoutSeconds)
}

// GetSQLServerVolumeDiskSpace gets disk space for a specific volume
func GetSQLServerVolumeDiskSpace(driveLetter string, timeoutSeconds int) (string, error) {
	command := fmt.Sprintf("Get-Volume -DriveLetter %s | Select-Object SizeRemaining,Size | ConvertTo-Json", driveLetter)
	return RunPowerShellCommand(command, timeoutSeconds)
}

// CheckRunningPowerShellProcesses lists current PowerShell processes (useful for diagnostics)
func CheckRunningPowerShellProcesses() (int, error) {
	command := "Get-Process -Name powershell | Measure-Object | Select-Object -ExpandProperty Count"
	output, err := RunPowerShellCommand(command, 10)
	if err != nil {
		return 0, err
	}

	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return 0, err
	}

	return count, nil
}

// KillOrphanedPowerShellProcesses kills PowerShell processes over a certain age
func KillOrphanedPowerShellProcesses(olderThanMinutes int) (int, error) {
	if olderThanMinutes <= 0 {
		olderThanMinutes = 30 // Default to 30 minutes
	}

	// Find and kill PowerShell processes that have been running longer than the specified time
	command := fmt.Sprintf("Get-Process -Name powershell | Where-Object {$_.StartTime -lt (Get-Date).AddMinutes(-%d)} | Stop-Process -Force -PassThru | Measure-Object | Select-Object -ExpandProperty Count",
		olderThanMinutes)

	output, err := RunPowerShellCommand(command, 20) // Give it extra time since it's doing process management
	if err != nil {
		return 0, err
	}

	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return 0, err
	}

	return count, nil
}

// StartPowerShellMonitor starts a background monitor for PowerShell processes
// Returns a stop function that can be called to stop the monitoring
func StartPowerShellMonitor(checkIntervalMinutes, maxAllowedProcesses, cleanupOlderThanMinutes int) func() {
	if checkIntervalMinutes <= 0 {
		checkIntervalMinutes = 15 // Default to checking every 15 minutes
	}

	if maxAllowedProcesses <= 0 {
		maxAllowedProcesses = 20 // Default to 20 processes
	}

	if cleanupOlderThanMinutes <= 0 {
		cleanupOlderThanMinutes = 30 // Default to 30 minutes
	}

	stopCh := make(chan struct{})
	ticker := time.NewTicker(time.Duration(checkIntervalMinutes) * time.Minute)

	go func() {
		logger.Info("PowerShell process monitor started - checking every %d minutes", checkIntervalMinutes)

		for {
			select {
			case <-ticker.C:
				// Check PowerShell process count
				count, err := CheckRunningPowerShellProcesses()
				if err != nil {
					logger.Error("Error checking PowerShell processes: %v", err)
					continue
				}

				logger.Debug("Current PowerShell process count: %d", count)

				// If too many processes, clean up older ones
				if count > maxAllowedProcesses {
					logger.Warning("High number of PowerShell processes detected: %d (threshold: %d)",
						count, maxAllowedProcesses)

					killed, err := KillOrphanedPowerShellProcesses(cleanupOlderThanMinutes)
					if err != nil {
						logger.Error("Error cleaning up PowerShell processes: %v", err)
					} else if killed > 0 {
						logger.Info("Cleaned up %d orphaned PowerShell processes", killed)
					}
				}

			case <-stopCh:
				ticker.Stop()
				logger.Info("PowerShell process monitor stopped")
				return
			}
		}
	}()

	// Return a function that can be called to stop the monitoring
	return func() {
		close(stopCh)
	}
}

// OptimizeWindowsTCPSettings configures Windows TCP/IP stack for better connection handling
// This addresses the "Only one usage of each socket address" error by modifying registry settings
func OptimizeWindowsTCPSettings() error {
	if runtime.GOOS != "windows" {
		return nil // Only works on Windows
	}

	logger.Info("Optimizing Windows TCP/IP settings for better connection handling...")

	// First check if we have admin rights by testing a harmless registry read operation
	testCmd := `Get-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -ErrorAction SilentlyContinue`
	_, testErr := RunPowerShellCommandAsAdmin(testCmd, 5)

	// If we couldn't run the test command as admin, log and create diagnostic info, but don't treat as error
	if testErr != nil {
		logger.Warning("No administrator rights to modify Windows TCP settings. Skipping TCP optimizations.")

		// Just create the diagnostic file with instructions for manual changes
		diagnosis := `
MSSQL Socket Optimization Recommendations:

To fix the "Only one usage of each socket address" error, run these commands as Administrator in PowerShell:

# TIME_WAIT süresini 240 saniyeden 30 saniyeye düşür
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "TcpTimedWaitDelay" -Value 30 -Type DWord

# Kullanılabilir maksimum port sayısını artır
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "MaxUserPort" -Value 65534 -Type DWord

# Dinamik port aralığını genişlet
netsh int ipv4 set dynamicport tcp start=10000 num=55534

Alternatively, you can modify these settings using Windows Registry Editor:
1. Run regedit
2. Navigate to HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters
3. Add/modify:
   - TcpTimedWaitDelay (DWORD): 30
   - MaxUserPort (DWORD): 65534
4. Then run netsh command from Admin Command Prompt
`

		// Write to a file for easy access
		diagFile := filepath.Join(os.TempDir(), "mssql_socket_optimization.txt")
		if err := os.WriteFile(diagFile, []byte(diagnosis), 0644); err != nil {
			logger.Error("Failed to write diagnostic file: %v", err)
		} else {
			logger.Info("TCP optimization instructions written to: %s", diagFile)
		}

		// Run diagnostics to get current TCP settings
		diagnostics := DiagnoseTCPSocketIssue()
		logger.Debug("Current system TCP diagnostics: %v", diagnostics)

		// This is not a failure case - we'll just continue with application level mitigations
		return nil
	}

	// We have admin rights, proceed with optimizations

	// 1. Increase TcpTimedWaitDelay (reduce TIME_WAIT socket timeout from 240 to 30 seconds)
	timewaitCmd := `Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "TcpTimedWaitDelay" -Value 30 -Type DWord`

	// 2. Enable TCPTimedWaitDelay settings
	reuseTCPCmd := `Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "MaxUserPort" -Value 65534 -Type DWord`

	// 3. Increase dynamic port range
	portRangeCmd := `netsh int ipv4 set dynamicport tcp start=10000 num=55534`

	// Execute commands with proper error handling
	cmds := []struct {
		name    string
		command string
	}{
		{"TcpTimedWaitDelay", timewaitCmd},
		{"MaxUserPort", reuseTCPCmd},
		{"Dynamic Port Range", portRangeCmd},
	}

	var errors []string

	for _, cmd := range cmds {
		logger.Info("Applying TCP optimization: %s", cmd.name)

		// Run with admin check
		output, err := RunPowerShellCommandAsAdmin(cmd.command, 15)
		if err != nil {
			logger.Warning("Failed to set %s: %v - %s", cmd.name, err, output)
			errors = append(errors, fmt.Sprintf("%s: %v", cmd.name, err))
			continue
		}

		logger.Info("Successfully applied TCP optimization: %s", cmd.name)
	}

	if len(errors) > 0 {
		logger.Warning("Some TCP optimizations failed (may require direct registry edit): %s", strings.Join(errors, ", "))

		// Create diagnostic information and recommendations
		diagnosis := fmt.Sprintf(`
MSSQL Socket Optimization Recommendations:

Some Windows registry settings could not be updated automatically. 
To manually fix "Only one usage of each socket address" issues, run these commands as Administrator:

%s

%s

%s

Alternatively, you can manually change these settings in Windows Registry using regedit.
`, timewaitCmd, reuseTCPCmd, portRangeCmd)

		// Write to a file for easy access
		diagFile := filepath.Join(os.TempDir(), "mssql_socket_optimization.txt")
		if err := os.WriteFile(diagFile, []byte(diagnosis), 0644); err != nil {
			logger.Error("Failed to write diagnostic file: %v", err)
		} else {
			logger.Info("TCP optimization instructions written to: %s", diagFile)
		}
	}

	return nil
}

// RunPowerShellCommandAsAdmin runs a PowerShell command with elevated privileges if possible
func RunPowerShellCommandAsAdmin(command string, timeoutSeconds int) (string, error) {
	// Admin yetki denemesi yapıldığını takip etmek için statik değişken
	static := struct {
		adminFailed bool
		sync.Once
	}{}

	// Eğer daha önce admin yetkisi başarısız olduysa, direkt normal yönteme geç
	if static.adminFailed {
		return RunPowerShellCommand(command, timeoutSeconds)
	}

	// Create a temporary script file
	tempFile, err := os.CreateTemp("", "admin_ps_*.ps1")
	if err != nil {
		// Hata olursa normal yönteme geç
		static.adminFailed = true
		return RunPowerShellCommand(command, timeoutSeconds)
	}
	defer os.Remove(tempFile.Name()) // Clean up

	// Write command to the script
	if _, err := tempFile.WriteString(command); err != nil {
		// Hata olursa normal yönteme geç
		static.adminFailed = true
		return RunPowerShellCommand(command, timeoutSeconds)
	}
	tempFile.Close()

	// Try to run with elevated privileges - shorter timeout to fail faster
	elevatedCmd := fmt.Sprintf(
		"powershell -Command \"Start-Process powershell -Verb RunAs -ArgumentList '-ExecutionPolicy Bypass -File \"%s\"' -Wait\"",
		tempFile.Name())

	// Create context with reduced timeout for admin attempt
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) // Daha kısa timeout
	defer cancel()

	// Try running with elevation
	cmd := exec.CommandContext(ctx, "cmd", "/C", elevatedCmd)
	output, err := cmd.CombinedOutput()

	// If elevation failed or was denied, try running normally
	if err != nil {
		logger.Warning("Failed to run as admin, switching to non-elevated mode: %v", err)
		// Statik değişkeni güncelle - bir kez başarısız olduysa, sonraki çağrılarda direkt normal mod kullan
		static.Once.Do(func() {
			static.adminFailed = true
			logger.Info("Disabling admin elevation attempts for future PowerShell calls")
		})
		return RunPowerShellCommand(command, timeoutSeconds)
	}

	return string(output), nil
}

// DiagnoseTCPSocketIssue runs diagnostics for TCP socket issues
func DiagnoseTCPSocketIssue() string {
	if runtime.GOOS != "windows" {
		return "TCP socket diagnostics only available on Windows"
	}

	// Get active TCP connections
	tcpConnsCommand := "Get-NetTCPConnection | Where-Object {$_.LocalPort -eq 1433 -or $_.RemotePort -eq 1433} | Format-Table -AutoSize"
	tcpConns, _ := RunPowerShellCommand(tcpConnsCommand, 15)

	// Get socket statistics
	socketStatsCommand := "netstat -ano | findstr 1433"
	socketStats, _ := RunPowerShellCommand(socketStatsCommand, 15)

	// Get dynamic port range
	portRangeCommand := "netsh int ipv4 show dynamicport tcp"
	portRange, _ := RunPowerShellCommand(portRangeCommand, 15)

	// Get current registry settings
	tcpParamsCommand := "Get-ItemProperty -Path 'HKLM:\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters' | Select-Object TcpTimedWaitDelay, MaxUserPort | Format-List"
	tcpParams, _ := RunPowerShellCommand(tcpParamsCommand, 15)

	// Count TIME_WAIT sockets
	timeWaitCommand := "netstat -ano | findstr TIME_WAIT | Measure-Object -Line | Select-Object -ExpandProperty Lines"
	timeWaitCount, _ := RunPowerShellCommand(timeWaitCommand, 15)

	// Format results
	diagnostics := fmt.Sprintf(`
==== MSSQL TCP Socket Diagnostics ====

-- TIME_WAIT Sockets Count --
%s

-- Dynamic Port Range --
%s

-- TCP Parameters --
%s

-- SQL Server TCP Connections --
%s

-- Socket Statistics --
%s

-- Recommendations --
1. If TIME_WAIT count is high, Windows is holding too many closed connections
2. Ensure TcpTimedWaitDelay is set to 30 (seconds) instead of default 240
3. Ensure MaxUserPort is set to at least 65534
4. Verify dynamic port range is sufficiently large
5. Restart the agent service if this issue persists
`,
		strings.TrimSpace(timeWaitCount),
		strings.TrimSpace(portRange),
		strings.TrimSpace(tcpParams),
		strings.TrimSpace(tcpConns),
		strings.TrimSpace(socketStats))

	// Write to a file
	diagFile := filepath.Join(os.TempDir(), "mssql_socket_diagnostics.txt")
	if err := os.WriteFile(diagFile, []byte(diagnostics), 0644); err == nil {
		logger.Info("TCP socket diagnostics written to: %s", diagFile)
	}

	return diagnostics
}

// ConfigureGlobalSQLConnection creates and maintains a single global SQL connection
// to avoid creating too many connections
var (
	globalSQLConn     *sql.DB
	globalSQLConnMux  sync.Mutex
	globalConnCreated time.Time
	lastPingTime      time.Time
	// Ping yapılan son bağlantının hash değeri - değişmiş mi kontrol etmek için
	lastConnHash string
)

// GenerateConnHash bağlantı dizesi için basit bir hash üretir
func GenerateConnHash(connString string) string {
	// Basit bir hash oluştur - bağlantı dizesinin ilk 50 karakterini al
	if len(connString) > 50 {
		connString = connString[:50]
	}
	return connString
}

// GetGlobalSQLConnection returns a shared SQL connection with the specified parameters
func GetGlobalSQLConnection(connString string, maxIdleTime, maxLifetime time.Duration) (*sql.DB, error) {
	globalSQLConnMux.Lock()
	defer globalSQLConnMux.Unlock()

	// Bağlantı hash değerini oluştur
	connHash := GenerateConnHash(connString)

	// Eğer geçerli bir bağlantı varsa ve bağlantı dizesi değişmemişse
	if globalSQLConn != nil && lastConnHash == connHash {
		// Son ping zamanını kontrol et - son 30 saniyede ping yapıldıysa tekrar ping yapma
		if time.Since(lastPingTime) < 30*time.Second {
			logger.Debug("Reusing global SQL connection (last ping was %v ago)", time.Since(lastPingTime))
			return globalSQLConn, nil
		}

		// Ping ile bağlantıyı test et
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pingErr := globalSQLConn.PingContext(pingCtx)
		cancel()

		// Her ping denemesinde ping zamanını güncelle
		lastPingTime = time.Now()

		if pingErr == nil {
			// Bağlantı hala açık, bunu kullan
			return globalSQLConn, nil
		}

		logger.Warning("Global SQL connection ping failed: %v", pingErr)
		// Bağlantıyı kapat ve yeniden oluştur
		globalSQLConn.Close()
		globalSQLConn = nil
	} else if globalSQLConn != nil && lastConnHash != connHash {
		// Bağlantı dizesi değişmiş, eski bağlantıyı kapat
		logger.Info("SQL connection string changed, creating new connection")
		globalSQLConn.Close()
		globalSQLConn = nil
	}

	// Yeni veritabanı bağlantısı oluştur
	logger.Debug("Creating new global SQL connection")
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQL connection: %w", err)
	}

	// Bağlantı havuzunu yapılandır - daha yüksek limitler kullan
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(maxLifetime)
	db.SetConnMaxIdleTime(maxIdleTime)

	// Bağlantıyı test et
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	pingErr := db.PingContext(pingCtx)
	cancel()

	// Ping zamanını güncelle
	lastPingTime = time.Now()

	if pingErr != nil {
		db.Close()
		return nil, fmt.Errorf("SQL connection test failed: %w", pingErr)
	}

	// Bağlantı başarılı olduğunda güncelle
	globalSQLConn = db
	globalConnCreated = time.Now()
	lastConnHash = connHash

	logger.Info("Created new global SQL connection successfully")
	return globalSQLConn, nil
}
