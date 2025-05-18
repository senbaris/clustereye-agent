//go:build !windows
// +build !windows

package main

// setupWindowsEventLog is a no-op on non-Windows platforms
func setupWindowsEventLog() bool {
	// Non-Windows sistemlerde çalışmıyor, dosya loguna dönülmeli
	return false
}
