#!/bin/bash

# Renk kodları
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Varsayılan değerler
RELEASE_URL="https://github.com/senbaris/clustereye-agent/releases/download/v1.0.1"
INSTALL_DIR="/opt/clustereye"
SERVICE_NAME="clustereye-agent"

# Kullanım bilgisi
usage() {
    echo -e "${YELLOW}ClusterEye Agent Kurulum Scripti${NC}"
    echo -e "Kullanım: $0 -p <platform> [-k <license-key>] [-d <install-dir>]"
    echo -e "\nParametreler:"
    echo -e "  -p  Platform (zorunlu): 'postgres' veya 'mongo'"
    echo -e "  -k  Lisans anahtarı (opsiyonel)"
    echo -e "  -d  Kurulum dizini (varsayılan: /opt/clustereye)"
    echo -e "  -h  Bu yardım mesajını göster"
    exit 1
}

# Parametreleri parse et
while getopts "p:k:d:h" opt; do
    case $opt in
        p) PLATFORM="$OPTARG";;
        k) LICENSE_KEY="$OPTARG";;
        d) INSTALL_DIR="$OPTARG";;
        h) usage;;
        ?) usage;;
    esac
done

# Platform parametresini kontrol et
if [ -z "$PLATFORM" ] || [[ ! "$PLATFORM" =~ ^(postgres|mongo)$ ]]; then
    echo -e "${RED}Hata: Geçerli bir platform belirtilmedi (postgres veya mongo)${NC}"
    usage
fi

# Root yetkisi kontrol
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Bu script root yetkileri ile çalıştırılmalıdır.${NC}"
    echo -e "Lütfen 'sudo $0' şeklinde çalıştırın."
    exit 1
fi

echo -e "${YELLOW}ClusterEye Agent kurulumu başlatılıyor...${NC}"
echo "Platform: $PLATFORM"
echo "Kurulum dizini: $INSTALL_DIR"

# Sistem mimarisini belirle
ARCH=$(uname -m)
case $ARCH in
    x86_64)
        BINARY_ARCH="amd64"
        ;;
    aarch64)
        BINARY_ARCH="arm64"
        ;;
    *)
        echo -e "${RED}Desteklenmeyen sistem mimarisi: $ARCH${NC}"
        exit 1
        ;;
esac

# Kurulum dizinini oluştur
echo -e "\n${YELLOW}1. Kurulum dizini oluşturuluyor...${NC}"
mkdir -p "$INSTALL_DIR"

# Binary'i indir
echo -e "\n${YELLOW}2. Agent binary dosyası indiriliyor...${NC}"
BINARY_NAME="clustereye-agent-linux-$BINARY_ARCH"
curl -L -o "$INSTALL_DIR/$BINARY_NAME" "$RELEASE_URL/$BINARY_NAME"

if [ $? -ne 0 ]; then
    echo -e "${RED}Binary indirme hatası!${NC}"
    exit 1
fi

# Çalıştırma yetkisi ver
chmod +x "$INSTALL_DIR/$BINARY_NAME"

# Örnek config dosyasını indir
echo -e "\n${YELLOW}3. Yapılandırma dosyası indiriliyor...${NC}"
curl -L -o "$INSTALL_DIR/agent.yml" "$RELEASE_URL/agent.yml.example"

if [ $? -ne 0 ]; then
    echo -e "${RED}Yapılandırma dosyası indirme hatası!${NC}"
    exit 1
fi

# Lisans anahtarı varsa config dosyasını güncelle
if [ ! -z "$LICENSE_KEY" ]; then
    echo -e "\n${YELLOW}4. Lisans anahtarı yapılandırılıyor...${NC}"
    sed -i "s/YOUR-LICENSE-KEY/$LICENSE_KEY/" "$INSTALL_DIR/agent.yml"
fi

# Systemd service dosyası oluştur
echo -e "\n${YELLOW}5. Systemd service dosyası oluşturuluyor...${NC}"
cat > "/etc/systemd/system/$SERVICE_NAME.service" << EOF
[Unit]
Description=ClusterEye Agent Service
After=network.target

[Service]
Type=simple
User=root
ExecStart=$INSTALL_DIR/$BINARY_NAME -platform $PLATFORM
WorkingDirectory=$INSTALL_DIR
Restart=always
RestartSec=5
Environment=PATH=/usr/bin:/bin:/usr/local/bin

[Install]
WantedBy=multi-user.target
EOF

# Systemd'yi yenile
systemctl daemon-reload

echo -e "\n${GREEN}Kurulum tamamlandı!${NC}"
echo -e "\n${YELLOW}Önemli:${NC}"
echo "1. Yapılandırma dosyasını düzenleyin:"
echo "   sudo nano $INSTALL_DIR/agent.yml"
echo -e "\n2. Servisi başlatın:"
echo "   sudo systemctl start $SERVICE_NAME"
echo "   sudo systemctl enable $SERVICE_NAME"
echo -e "\n3. Servis durumunu kontrol edin:"
echo "   sudo systemctl status $SERVICE_NAME"
echo -e "\n4. Logları izleyin:"
echo "   sudo journalctl -u $SERVICE_NAME -f" 