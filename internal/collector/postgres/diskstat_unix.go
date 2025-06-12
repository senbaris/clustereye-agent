//go:build !windows
// +build !windows

package postgres

import (
	"log"
	"syscall"
)

// GetDiskUsage Unix/Linux sistemleri için disk kullanım bilgilerini alır
func GetDiskUsage() (string, int) {
	// PostgreSQL veri dizinini bul
	dataDir, err := getDataDirectoryFromConfig()
	if err != nil {
		log.Printf("PostgreSQL veri dizini bulunamadı: %v", err)
		return "N/A", 0
	}

	// Disk kullanım bilgilerini al
	var stat syscall.Statfs_t
	err = syscall.Statfs(dataDir, &stat)
	if err != nil {
		log.Printf("Disk kullanım bilgileri alınamadı: %v", err)
		return "N/A", 0
	}

	// Boş alanı hesapla
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	totalBytes := stat.Blocks * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes

	// Kullanım yüzdesini hesapla (Used Disk Percent)
	percent := int((float64(usedBytes) / float64(totalBytes)) * 100)

	// Boş alanı okunabilir formata çevir
	freeDisk := convertSize(freeBytes)

	return freeDisk, percent
}
