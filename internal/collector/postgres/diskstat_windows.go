//go:build windows
// +build windows

package postgres

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// GetDiskUsage Windows sistemleri için disk kullanım bilgilerini alır
func GetDiskUsage() (string, int) {
	// PostgreSQL veri dizinini bul
	dataDir, err := getDataDirectoryFromConfig()
	if err != nil {
		log.Printf("PostgreSQL veri dizini bulunamadı: %v", err)
		return "N/A", 0
	}

	// Windows'ta sürücü harfini al
	driveLetter := filepath.VolumeName(dataDir)
	if driveLetter == "" {
		log.Printf("Disk sürücü harfi belirlenemedi: %s", dataDir)
		return "N/A", 0
	}

	// PowerShell komutu çalıştır
	cmd := exec.Command("powershell", "-Command",
		fmt.Sprintf("Get-Volume -DriveLetter %s | Select-Object SizeRemaining,Size | ConvertTo-Json",
			driveLetter[0:1])) // C: -> C

	out, err := cmd.Output()
	if err != nil {
		log.Printf("Disk kullanım bilgileri alınamadı: %v", err)
		return "N/A", 0
	}

	// PowerShell çıktısını işle
	output := string(out)
	reFree := strings.Index(output, `"SizeRemaining"`)
	reTotal := strings.Index(output, `"Size"`)

	if reFree == -1 || reTotal == -1 {
		log.Printf("PowerShell çıktısı işlenemedi: %s", output)
		return "N/A", 0
	}

	// Değerleri çıkartmaya çalış
	var freeBytes, totalBytes uint64

	// SizeRemaining değerini çıkar
	freeStr := output[reFree+len(`"SizeRemaining"`) : reTotal]
	freeStr = strings.TrimSpace(freeStr)
	freeStr = strings.Trim(freeStr, ":, \t\r\n")
	if val, err := strconv.ParseUint(freeStr, 10, 64); err == nil {
		freeBytes = val
	}

	// Size değerini çıkar
	totalStr := output[reTotal+len(`"Size"`) : len(output)]
	totalStr = strings.TrimSpace(totalStr)
	totalStr = strings.Trim(totalStr, ":, \t\r\n}")
	if val, err := strconv.ParseUint(totalStr, 10, 64); err == nil {
		totalBytes = val
	}

	if totalBytes == 0 {
		return "N/A", 0
	}

	// Kullanım yüzdesini hesapla
	usedBytes := totalBytes - freeBytes
	percent := int((float64(usedBytes) / float64(totalBytes)) * 100)

	// Boş alanı okunabilir formata çevir
	freeDisk := convertSize(freeBytes)

	return freeDisk, percent
}
