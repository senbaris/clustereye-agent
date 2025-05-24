//go:build !windows
// +build !windows

package mssql

// Windows dışındaki platformlar için
// getWindowsPhysicalIP, getWindowsCPUCount ve getWindowsMemory metodlarının
// gereksinimleri karşılamak için stub implementasyonları

// getWindowsPhysicalIP placeholder implementation for non-Windows platforms
func (c *MSSQLCollector) getWindowsPhysicalIP() string {
	return ""
}

// getWindowsCPUCount placeholder implementation for non-Windows platforms
func (c *MSSQLCollector) getWindowsCPUCount() int32 {
	return 0
}

// getWindowsMemory placeholder implementation for non-Windows platforms
func (c *MSSQLCollector) getWindowsMemory() int64 {
	return 0
}
