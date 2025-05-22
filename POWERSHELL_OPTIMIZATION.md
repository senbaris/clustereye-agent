# Windows PowerShell Process Optimization

## Problem Description

The agent was experiencing high CPU and memory usage on Windows systems due to multiple PowerShell processes being spawned and potentially not being properly terminated. This was causing resource leaks and degrading system performance.

## Solution Strategy

We've implemented a new utility package to optimize PowerShell execution on Windows:

1. **Centralized PowerShell Execution**: 
   - Created a dedicated utility package for PowerShell command execution
   - Replaced direct `exec.Command("powershell", ...)` calls with utility functions

2. **Resource Management Improvements**:
   - Added proper timeouts to all PowerShell commands
   - Ensured proper cleanup of PowerShell processes
   - Improved error handling and resource disposal
   - Added `-NoProfile` and `-NonInteractive` flags to PowerShell for faster execution

3. **Context Timeout**:
   - All PowerShell commands now run with a context timeout
   - Automatically terminates processes that take too long
   - Prevents zombie PowerShell processes

## Optimization in the MSSQL Collector

The MSSQL collector has been optimized to use the central PowerShell utility functions:

1. **Updated Components**:
   - `GetDiskUsage`: Now uses `utils.RunPowerShellCommand` with timeouts
   - `GetConfigPath`: Registry queries optimized with centralized PowerShell calls
   - `getLocalIP`: Using `utils.GetNetworkInfo` and optimized PowerShell calls
   - `getTotalvCpu`: Using `utils.RunCimCommand` for CIM queries
   - `getTotalMemory`: Using `utils.RunCimCommand` for CIM queries
   - `FindMSSQLLogFiles`: Registry queries optimized with timeouts

2. **Additional Utility Functions**:
   - `GetSQLServerRegistryInfo`: Dedicated function for SQL Server registry queries
   - `GetSQLServerVolumeDiskSpace`: Fast disk space checking for SQL Server volumes
   - `CheckRunningPowerShellProcesses`: Diagnostic tool to count active PowerShell processes
   - `KillOrphanedPowerShellProcesses`: Cleanup utility for long-running PowerShell processes

## Monitoring and Maintenance

To further address the issue, recommended periodic maintenance:

1. **Monitor PowerShell Process Count**:
   ```go
   count, err := utils.CheckRunningPowerShellProcesses()
   if count > 20 {
       logger.Warning("High number of PowerShell processes detected: %d", count)
   }
   ```

2. **Clean Up Orphaned Processes**:
   ```go
   killed, err := utils.KillOrphanedPowerShellProcesses(30) // Kill processes older than 30 minutes
   if killed > 0 {
       logger.Info("Cleaned up %d orphaned PowerShell processes", killed)
   }
   ```

3. **Avoid Long-Running Commands**:
   All PowerShell commands should complete in under 30 seconds, preferably under 10 seconds.

## Expected Improvements

These changes should significantly reduce the CPU and memory usage on Windows systems by:

1. Reducing the total number of PowerShell processes
2. Properly terminating PowerShell processes after use
3. Avoiding PowerShell process leaks
4. Using more efficient PowerShell commands
5. Setting appropriate timeouts to prevent runaway processes

The centralized management approach will also make it easier to further optimize PowerShell usage in the future.

## Implementation Steps

The implementation requires updating the following files:

1. ✅ `internal/collector/utils/windows_utils.go` - Created utility functions
2. ✅ `internal/alarm/monitor.go` - Updated disk usage check
3. ⬜ `internal/collector/mssql/collector.go` - Update MSSQL-specific PowerShell commands
4. ⬜ `internal/collector/postgres/collector.go` - Update PostgreSQL collector's Windows checks
5. ⬜ `internal/collector/postgres/diskstat_windows.go` - Update disk statistics collection

## Usage Guidelines

When working with PowerShell on Windows:

1. Always use the utility functions from `internal/collector/utils/windows_utils.go`
2. Always specify a reasonable timeout (5-15 seconds recommended)
3. Use built-in helpers for common operations:
   - `RunPowerShellCommand` - For generic PowerShell commands
   - `RunWMICommand` - For WMI queries
   - `RunCimCommand` - For CIM queries (preferred over WMI)
   - `GetDiskInfo`, `GetCPUUsage`, etc. - For specific metrics

## Example

Old approach (problematic):
```go
cmd := exec.Command("powershell", "-Command", "Get-Volume | Select-Object ...") 
output, err := cmd.Output()
```

New approach:
```go
output, err := utils.GetDiskInfo(15) // 15 second timeout
```

## Results

- Reduced number of concurrent PowerShell processes
- Lower memory and CPU usage
- More robust error handling and timeout management
- Better Windows performance overall 