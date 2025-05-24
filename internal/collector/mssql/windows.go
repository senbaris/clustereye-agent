//go:build windows
// +build windows

package mssql

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/StackExchange/wmi"
	"golang.org/x/sys/windows"
)

// getWindowsPhysicalIP attempts to get the physical node's IP address using direct Windows APIs
func (c *MSSQLCollector) getWindowsPhysicalIP() string {
	// First try direct WMI query instead of PowerShell
	var adapters []struct {
		NetConnectionID string
		Name            string
		Description     string
		MACAddress      string
		PhysicalAdapter bool
		NetEnabled      bool
		AdapterType     string
	}

	var configs []struct {
		IPAddress        []string
		IPEnabled        bool
		MACAddress       string
		DefaultIPGateway []string
		DHCPEnabled      bool
	}

	// Query physical network adapters
	q := wmi.CreateQuery(&adapters, "WHERE PhysicalAdapter=TRUE AND NetEnabled=TRUE")
	err := wmi.Query(q, &adapters)
	if err == nil && len(adapters) > 0 {
		// Get adapter configurations for the physical adapters
		var macAddresses []string
		for _, adapter := range adapters {
			// Skip adapters that look like virtual or cluster adapters
			desc := strings.ToLower(adapter.Description)
			if strings.Contains(desc, "virtual") ||
				strings.Contains(desc, "cluster") ||
				strings.Contains(desc, "team") ||
				strings.Contains(desc, "vmware") ||
				strings.Contains(desc, "hyper-v") {
				continue
			}

			if adapter.MACAddress != "" {
				macAddresses = append(macAddresses, adapter.MACAddress)
			}
		}

		// Only proceed if we found physical adapters
		if len(macAddresses) > 0 {
			// Build WHERE clause for MAC addresses
			macFilter := "WHERE "
			for i, mac := range macAddresses {
				if i > 0 {
					macFilter += " OR "
				}
				macFilter += fmt.Sprintf("MACAddress='%s'", mac)
			}
			macFilter += " AND IPEnabled=TRUE"

			// Query configurations for those adapters
			q = wmi.CreateQuery(&configs, macFilter)
			err = wmi.Query(q, &configs)

			if err == nil {
				var validIPs []string

				// Process configurations to find valid IPs
				for _, config := range configs {
					// Only consider configurations with gateway (connected to network)
					if len(config.DefaultIPGateway) > 0 {
						for _, ip := range config.IPAddress {
							// Only IPv4 addresses
							if net.ParseIP(ip) != nil && !strings.Contains(ip, ":") &&
								ip != "127.0.0.1" && !strings.HasPrefix(ip, "169.254.") {
								validIPs = append(validIPs, ip)
							}
						}
					}
				}

				// Sort IPs by preferred ranges
				sort.Slice(validIPs, func(i, j int) bool {
					if strings.HasPrefix(validIPs[i], "192.168.") &&
						!strings.HasPrefix(validIPs[j], "192.168.") {
						return true
					}
					if strings.HasPrefix(validIPs[i], "10.") &&
						!strings.HasPrefix(validIPs[j], "10.") &&
						!strings.HasPrefix(validIPs[j], "192.168.") {
						return true
					}
					return false
				})

				if len(validIPs) > 0 {
					log.Printf("Found IP using direct WMI query: %s", validIPs[0])
					return validIPs[0]
				}
			}
		}
	}

	// Fallback to Windows DNS API if WMI failed
	var hostName [windows.MAX_COMPUTERNAME_LENGTH + 1]uint16
	var size uint32 = windows.MAX_COMPUTERNAME_LENGTH + 1
	err = windows.GetComputerName(&hostName[0], &size)
	if err == nil {
		computerName := windows.UTF16ToString(hostName[:size])

		// Use net.LookupIP which uses native Windows APIs under the hood
		ips, err := net.LookupIP(computerName)
		if err == nil {
			var validIPs []string
			for _, ip := range ips {
				ipStr := ip.String()
				if ip.To4() != nil && ipStr != "127.0.0.1" && !strings.HasPrefix(ipStr, "169.254.") {
					validIPs = append(validIPs, ipStr)
				}
			}

			// Sort IPs by preferred ranges
			sort.Slice(validIPs, func(i, j int) bool {
				if strings.HasPrefix(validIPs[i], "192.168.") &&
					!strings.HasPrefix(validIPs[j], "192.168.") {
					return true
				}
				if strings.HasPrefix(validIPs[i], "10.") &&
					!strings.HasPrefix(validIPs[j], "10.") &&
					!strings.HasPrefix(validIPs[j], "192.168.") {
					return true
				}
				return false
			})

			if len(validIPs) > 0 {
				log.Printf("Found IP using Windows DNS API: %s", validIPs[0])
				return validIPs[0]
			}
		}
	}

	// If all else fails, fall back to ipconfig but parse with Go
	cmd := exec.Command("ipconfig", "/all")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil {
		// Parse output with Go code instead of PowerShell
		output := string(out)

		// Extract IPv4 addresses from ipconfig output
		ipv4Regex := regexp.MustCompile(`IPv4 Address[^:]*:\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`)
		matches := ipv4Regex.FindAllStringSubmatch(output, -1)

		var validIPs []string
		for _, match := range matches {
			if len(match) >= 2 {
				ip := match[1]
				if ip != "127.0.0.1" && !strings.HasPrefix(ip, "169.254.") {
					validIPs = append(validIPs, ip)
				}
			}
		}

		// Sort IPs by preferred ranges
		sort.Slice(validIPs, func(i, j int) bool {
			if strings.HasPrefix(validIPs[i], "192.168.") &&
				!strings.HasPrefix(validIPs[j], "192.168.") {
				return true
			}
			if strings.HasPrefix(validIPs[i], "10.") &&
				!strings.HasPrefix(validIPs[j], "10.") &&
				!strings.HasPrefix(validIPs[j], "192.168.") {
				return true
			}
			return false
		})

		if len(validIPs) > 0 {
			log.Printf("Found IP using ipconfig parsing: %s", validIPs[0])
			return validIPs[0]
		}
	}

	log.Printf("Could not find physical node IP using direct methods")
	return ""
}

// getWindowsCPUCount returns the total number of vCPUs using direct WMI queries
func (c *MSSQLCollector) getWindowsCPUCount() int32 {
	// Use direct WMI query instead of PowerShell
	var computerSystems []struct {
		NumberOfLogicalProcessors uint32
		TotalPhysicalMemory       uint64
	}

	q := wmi.CreateQuery(&computerSystems, "")
	err := wmi.Query(q, &computerSystems)

	if err == nil && len(computerSystems) > 0 {
		cpuCount := int32(computerSystems[0].NumberOfLogicalProcessors)
		if cpuCount > 0 {
			// Cache the result
			c.cacheCPUCount(cpuCount)
			log.Printf("Got CPU count via WMI: %d", cpuCount)
			return cpuCount
		}
	} else {
		log.Printf("WMI CPU query failed: %v, falling back to OS command", err)
	}

	// Fallback to direct command if WMI fails
	cmd := exec.Command("wmic", "cpu", "get", "NumberOfLogicalProcessors", "/value")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil {
		// Parse the output
		cpuStr := strings.TrimSpace(string(out))
		parts := strings.Split(cpuStr, "=")
		if len(parts) == 2 {
			count, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err == nil && count > 0 {
				cpuCount := int32(count)
				// Cache the result
				c.cacheCPUCount(cpuCount)
				log.Printf("Got CPU count via wmic command: %d", cpuCount)
				return cpuCount
			}
		}
	}

	return 0 // Fallback to runtime.NumCPU() in caller
}

// getWindowsMemory returns the total memory in bytes using direct WMI queries
func (c *MSSQLCollector) getWindowsMemory() int64 {
	// Use direct WMI query instead of PowerShell
	var computerSystems []struct {
		NumberOfLogicalProcessors uint32
		TotalPhysicalMemory       uint64
	}

	q := wmi.CreateQuery(&computerSystems, "")
	err := wmi.Query(q, &computerSystems)

	if err == nil && len(computerSystems) > 0 {
		totalMemory := int64(computerSystems[0].TotalPhysicalMemory)
		if totalMemory > 0 {
			// Cache the result
			c.cacheMemory(totalMemory)
			log.Printf("Got total memory via WMI: %d bytes", totalMemory)
			return totalMemory
		}
	} else {
		log.Printf("WMI memory query failed: %v, falling back to OS command", err)
	}

	// Alternative method using Win32_OperatingSystem (sometimes more reliable)
	var operatingSystems []struct {
		FreePhysicalMemory     uint64
		TotalVisibleMemorySize uint64
	}

	q = wmi.CreateQuery(&operatingSystems, "")
	err = wmi.Query(q, &operatingSystems)

	if err == nil && len(operatingSystems) > 0 {
		// TotalVisibleMemorySize is in KB, convert to bytes
		totalMemory := int64(operatingSystems[0].TotalVisibleMemorySize) * 1024
		if totalMemory > 0 {
			// Cache the result
			c.cacheMemory(totalMemory)
			log.Printf("Got total memory via Win32_OperatingSystem: %d bytes", totalMemory)
			return totalMemory
		}
	}

	// Fallback to wmic command
	cmd := exec.Command("wmic", "computersystem", "get", "TotalPhysicalMemory", "/value")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmdWithTimeout := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	out, err := cmdWithTimeout.Output()
	if err == nil {
		// Parse the output
		memStr := strings.TrimSpace(string(out))
		parts := strings.Split(memStr, "=")
		if len(parts) == 2 {
			mem, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err == nil && mem > 0 {
				totalMemory := mem
				// Cache the result
				c.cacheMemory(totalMemory)
				log.Printf("Got total memory via wmic command: %d bytes", totalMemory)
				return totalMemory
			}
		}
	}

	return 0 // Fallback to default in caller
}
