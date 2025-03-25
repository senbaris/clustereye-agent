package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"runtime"
)

// GenerateRandomID rastgele bir ID oluşturur
func GenerateRandomID(length int) (string, error) {
	bytes := make([]byte, length/2)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// GetLocalIP, yerel IP'yi almak için yardımcı fonksiyon
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

// GetPlatformInfo işletim sistemi ve mimari bilgisini döndürür
func GetPlatformInfo() string {
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

// Diğer yardımcı fonksiyonlar buraya eklenebilir
