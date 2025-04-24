#!/bin/bash

# Renk kodları
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parametreleri kontrol et
if [ "$#" -lt 3 ]; then
    echo -e "${RED}Kullanım: $0 <sunucu_ip> <kullanıcı> <platform>${NC}"
    echo -e "Platform: 'postgres' veya 'mongo' olmalıdır"
    exit 1
fi

SERVER_IP=$1
USER=$2
PLATFORM=$3

echo -e "${YELLOW}ClusterEye Agent Deployment Script${NC}"
echo "Sunucu: $SERVER_IP"
echo "Kullanıcı: $USER"
echo "Platform: $PLATFORM"

# Binary oluştur
echo -e "\n${YELLOW}1. Binary oluşturuluyor...${NC}"
mkdir -p build
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o build/clustereye-agent-linux-amd64 cmd/agent/main.go

if [ $? -ne 0 ]; then
    echo -e "${RED}Binary oluşturma hatası!${NC}"
    exit 1
fi

# Sunucuda dizin oluştur
echo -e "\n${YELLOW}2. Sunucuda dizin oluşturuluyor...${NC}"
ssh $USER@$SERVER_IP "sudo mkdir -p /opt/clustereye && sudo chown $USER:$USER /opt/clustereye"

# Dosyaları transfer et
echo -e "\n${YELLOW}3. Dosyalar transfer ediliyor...${NC}"
scp build/clustereye-agent-linux-amd64 $USER@$SERVER_IP:/opt/clustereye/
scp cmd/agent/agent.yml.example $USER@$SERVER_IP:/opt/clustereye/agent.yml

# Yetkileri ayarla
echo -e "\n${YELLOW}4. Yetkiler ayarlanıyor...${NC}"
ssh $USER@$SERVER_IP "chmod +x /opt/clustereye/clustereye-agent-linux-amd64"

# Systemd service dosyası oluştur
echo -e "\n${YELLOW}5. Systemd service dosyası oluşturuluyor...${NC}"
cat << EOF | ssh $USER@$SERVER_IP "sudo tee /etc/systemd/system/clustereye-agent.service"
[Unit]
Description=ClusterEye Agent Service
After=network.target

[Service]
Type=simple
User=$USER
ExecStart=/opt/clustereye/clustereye-agent-linux-amd64 -platform $PLATFORM
WorkingDirectory=/opt/clustereye
Restart=always
RestartSec=5
Environment=PATH=/usr/bin:/bin:/usr/local/bin

[Install]
WantedBy=multi-user.target
EOF

echo -e "\n${YELLOW}6. Systemd service yeniden yükleniyor...${NC}"
ssh $USER@$SERVER_IP "sudo systemctl daemon-reload"

echo -e "\n${GREEN}Deployment tamamlandı!${NC}"
echo -e "${YELLOW}Önemli:${NC}"
echo "1. /opt/clustereye/agent.yml dosyasını düzenleyin:"
echo "   ssh $USER@$SERVER_IP 'nano /opt/clustereye/agent.yml'"
echo "2. Servisi başlatın:"
echo "   ssh $USER@$SERVER_IP 'sudo systemctl start clustereye-agent'"
echo "3. Servis durumunu kontrol edin:"
echo "   ssh $USER@$SERVER_IP 'sudo systemctl status clustereye-agent'"
echo "4. Logları izleyin:"
echo "   ssh $USER@$SERVER_IP 'sudo journalctl -u clustereye-agent -f'" 