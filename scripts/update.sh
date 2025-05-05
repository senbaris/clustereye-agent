#!/bin/bash

# Renk kodları
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Sabit değişkenler
INSTALL_DIR="/opt/clustereye"
BINARY_NAME="clustereye-agent-linux-amd64"
CONFIG_FILE="agent.yml"

# Log fonksiyonu
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

# Sistem kontrolü
check_system() {
    # Root kontrolü
    if [ "$EUID" -ne 0 ]; then
        error "Bu script root yetkisi gerektirir. Lütfen 'sudo' ile çalıştırın."
        exit 1
    fi

    # curl kontrolü
    if ! command -v curl &> /dev/null; then
        error "curl yüklü değil. Lütfen yükleyin: sudo apt-get install curl"
        exit 1
    fi

    # systemd kontrolü
    if ! command -v systemctl &> /dev/null; then
        error "systemd bulunamadı. Bu script systemd kullanan sistemler için tasarlanmıştır."
        exit 1
    fi

    # Kurulum dizini kontrolü
    if [ ! -d "$INSTALL_DIR" ]; then
        error "Kurulum dizini ($INSTALL_DIR) bulunamadı."
        exit 1
    fi
}

# Mevcut versiyon kontrolü
get_current_version() {
    if [ -f "${INSTALL_DIR}/${BINARY_NAME}" ]; then
        current_version=$("${INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null || echo "unknown")
        echo "$current_version"
    else
        echo "not_installed"
    fi
}

# GitHub'dan en son sürümü kontrol et
get_latest_version() {
    latest_version=$(curl -s https://api.github.com/repos/senbaris/clustereye-agent/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    if [ -z "$latest_version" ]; then
        error "En son sürüm bilgisi alınamadı."
        exit 1
    fi
    echo "$latest_version"
}

# Yeni versiyonu indir
download_new_version() {
    local version=$1
    local arch=$(uname -m)
    local binary_name
    local tmp_dir
    local target_path
    local download_url
    
    # Mimariye göre binary adını belirle
    case "$arch" in
        x86_64)
            binary_name="clustereye-agent-linux-amd64"
            ;;
        aarch64)
            binary_name="clustereye-agent-linux-arm64"
            ;;
        *)
            error "Desteklenmeyen mimari: $arch"
            exit 1
            ;;
    esac

    # Geçici dizin oluştur
    tmp_dir=$(mktemp -d)
    target_path="${tmp_dir}/${binary_name}"
    download_url="https://github.com/senbaris/clustereye-agent/releases/download/${version}/${binary_name}"
    
    # Binary'yi sessizce indir
    if ! curl -s -L -o "$target_path" "$download_url"; then
        rm -rf "$tmp_dir"
        error "Binary indirilemedi"
        exit 1
    fi

    # Dosyanın varlığını kontrol et
    if [ ! -f "$target_path" ]; then
        rm -rf "$tmp_dir"
        error "Binary dosyası oluşturulamadı"
        exit 1
    fi

    # Önce path'i yazdır, sonra logları
    echo "$target_path"
    log "Yeni versiyon indiriliyor: $version..."
    log "Debug: Geçici dizin: $tmp_dir"
    log "Debug: İndirme URL: $download_url"
    log "Debug: Hedef dosya: $target_path"
    log "Debug: Binary başarıyla indirildi"
}

# Servisi güncelle
update_service() {
    local binary_path=$1
    local tmp_dir=$(dirname "$binary_path")

    log "Debug: Güncellenecek binary: $binary_path"
    log "Debug: Geçici dizin: $tmp_dir"

    # Binary'nin varlığını kontrol et
    if [ ! -f "$binary_path" ]; then
        error "Binary dosyası bulunamadı: $binary_path"
        exit 1
    fi

    log "Servis durduruluyor..."
    systemctl stop clustereye-agent

    log "Mevcut kurulumu yedekleme..."
    timestamp=$(date +%Y%m%d_%H%M%S)
    backup_dir="${INSTALL_DIR}_backup_${timestamp}"
    
    log "Debug: Yedek dizini: $backup_dir"
    
    if ! cp -r "$INSTALL_DIR" "$backup_dir"; then
        error "Yedekleme yapılamadı"
        systemctl start clustereye-agent
        rm -rf "$tmp_dir"
        exit 1
    fi

    log "Yeni binary yükleniyor..."
    log "Debug: Kaynak: $binary_path"
    log "Debug: Hedef: ${INSTALL_DIR}/${BINARY_NAME}"
    
    if ! cp "$binary_path" "${INSTALL_DIR}/${BINARY_NAME}"; then
        error "Yeni binary yüklenemedi. Eski versiyona geri dönülüyor..."
        rm -rf "${INSTALL_DIR}"
        mv "$backup_dir" "$INSTALL_DIR"
        systemctl start clustereye-agent
        rm -rf "$tmp_dir"
        exit 1
    fi
    
    chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

    log "Servis başlatılıyor..."
    systemctl start clustereye-agent

    # Servis durumunu kontrol et
    if ! systemctl is-active --quiet clustereye-agent; then
        error "Servis başlatılamadı. Eski versiyona geri dönülüyor..."
        rm -rf "${INSTALL_DIR}"
        mv "$backup_dir" "$INSTALL_DIR"
        systemctl start clustereye-agent
        rm -rf "$tmp_dir"
        exit 1
    fi

    # Başarılı güncelleme
    log "Yedek temizleniyor..."
    rm -rf "$backup_dir"
    rm -rf "$tmp_dir"
}

# Ana güncelleme fonksiyonu
do_update() {
    log "Güncelleme kontrol ediliyor..."
    
    current_version=$(get_current_version)
    latest_version=$(get_latest_version)

    log "Mevcut versiyon: $current_version"
    log "En son versiyon: $latest_version"

    if [ "$current_version" = "$latest_version" ]; then
        log "Agent zaten güncel."
        exit 0
    fi

    # Konfigürasyon dosyasını kontrol et
    if [ ! -f "${INSTALL_DIR}/${CONFIG_FILE}" ]; then
        error "Konfigürasyon dosyası (${CONFIG_FILE}) bulunamadı."
        exit 1
    fi

    # Yeni versiyonu indir
    binary_path=$(download_new_version "$latest_version" | head -n1)
    
    if [ ! -f "$binary_path" ]; then
        error "Binary dosyası bulunamadı: $binary_path"
        exit 1
    fi
    
    # Servisi güncelle
    update_service "$binary_path"

    log "Güncelleme başarıyla tamamlandı. Yeni versiyon: $latest_version"
}

# Ana program
main() {
    check_system
    do_update
}

main 