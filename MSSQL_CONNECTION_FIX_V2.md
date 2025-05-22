# MSSQL Bağlantı Sorunu - Gelişmiş Çözüm (v2)

## Yaşanan Sorun ve Temel Nedenler

`"Only one usage of each socket address (protocol/network address/port) is normally permitted"`

Bu hata, Windows işletim sisteminin TCP/IP yığınında sıkça karşılaşılan bir sorundan kaynaklanır ve temel olarak TCP soketlerinin tükendiğini gösterir.

### Teknik Nedenleri

1. **TIME_WAIT Soketi Birikimi**: Windows'ta varsayılan olarak bir TCP bağlantısı kapatıldıktan sonra soket 240 saniye boyunca TIME_WAIT durumunda kalır ve yeniden kullanılamaz.

2. **Dinamik Port Sınırlaması**: Windows varsayılan olarak istemci tarafı bağlantıları için sınırlı bir dinamik port aralığı kullanır (genellikle ~16,000 port).

3. **Bağlantı Havuzu Sorunları**: Veritabanı bağlantı havuzu düzgün yapılandırılmadığında her istek yeni bir bağlantı oluşturabilir.

## Yapılan Kapsamlı Düzeltmeler

Bu soruna yönelik üç katmanlı bir çözüm uyguladık:

### 1. Uygulama Seviyesi İyileştirmeler

- **Bağlantı Havuzu Optimizasyonu**: Daha etkili bağlantı yönetimi için bağlantı parametreleri güncellendi.
- **Global Bağlantı Havuzu**: Tüm agent için tek bir bağlantı havuzu kullanılarak çok sayıda bağlantı oluşturulması engellendi.
- **Bağlantı Yaşam Döngüsü Yönetimi**: Bağlantıların maksimum ömrü ve boşta kalma süreleri optimize edildi.

### 2. İşletim Sistemi Seviyesi İyileştirmeler

İşletim sistemi seviyesinde otomatik olarak uygulanan düzeltmeler:

```powershell
# TIME_WAIT süresini 240 saniyeden 30 saniyeye düşür
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "TcpTimedWaitDelay" -Value 30 -Type DWord

# Kullanılabilir maksimum port sayısını artır
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "MaxUserPort" -Value 65534 -Type DWord

# Dinamik port aralığını genişlet (10000-65534)
netsh int ipv4 set dynamicport tcp start=10000 num=55534
```

### 3. İleri Düzey Tanı ve Teşhis Araçları

- **TCP Soket Diagnostiği**: Sistem durumunu analiz ederek sorunun kaynağını belirleme.
- **TIME_WAIT Sayacı**: Sistemdeki TIME_WAIT durumunda olan soketlerin sayısını izleme.
- **Bağlantı İstatistikleri**: MSSQL bağlantılarının durumu ve sayısı hakkında bilgi toplama.

## Sorun Devam Ediyorsa Yapılacaklar

1. **Agent Servisini Yeniden Başlatın**: Bu, tüm açık TCP soketlerini temizler.

2. **Windows Yönetici Haklarıyla Komutları Çalıştırın**: Eğer agent otomatik olarak registry ayarlarını değiştiremediyse, şu PowerShell komutlarını yönetici olarak çalıştırın:

   ```powershell
   Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "TcpTimedWaitDelay" -Value 30 -Type DWord
   Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" -Name "MaxUserPort" -Value 65534 -Type DWord
   netsh int ipv4 set dynamicport tcp start=10000 num=55534
   ```

3. **Soket Durumunu Kontrol Edin**: TIME_WAIT durumunda çok sayıda soket olup olmadığını görmek için şu komutu çalıştırın:

   ```powershell
   netstat -ano | findstr TIME_WAIT | Measure-Object -Line
   ```

4. **SQL Server Bağlantılarını İzleyin**:

   ```powershell
   netstat -ano | findstr 1433
   ```

5. **Windows Güncellemelerini Kontrol Edin**: Bazı Windows güncellemeleri TCP/IP yığınında sorunlara neden olabilir.

## Teknik Detaylar

Bu yeni düzeltme, bağlantı havuzu optimizasyonlarına ek olarak, işletim sistemi seviyesinde TCP/IP yığınını yapılandırmakta ve sistemdeki tüm uygulamalar için soket yönetimini iyileştirmektedir.

Özellikle, global bağlantı havuzu sayesinde her istek için yeni bağlantı açmak yerine, aynı bağlantılar yeniden kullanılmaktadır. Bu, hem soket tüketimini azaltmakta hem de bağlantı kurma maliyetini düşürmektedir. 