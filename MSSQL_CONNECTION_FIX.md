# MSSQL Bağlantı Sorunu ve Çözümü

## Yaşanan Sorun

`"Only one usage of each socket address (protocol/network address/port) is normally permitted"`

Bu hata, TCP soketlerinin yanlış kullanımından kaynaklanan bir Windows ağ yığını hatasıdır. Aşağıdaki durumlarda ortaya çıkabilir:

1. Çok fazla TCP bağlantısı açılıp düzgün kapatılmadığında
2. TIME_WAIT durumundaki soketler yeniden kullanılmaya çalışıldığında 
3. Bağlantı havuzu (connection pool) doğru yönetilmediğinde
4. Yüksek frekanslı bağlantı açma/kapama işlemleri yapıldığında

## Yapılan Düzeltmeler

### 1. Bağlantı Havuzu (Connection Pool) Optimizasyonu

SQL Server bağlantı string'ine eklenen parametreler:

```
connection pooling=true;   // Bağlantı havuzunu etkinleştir
max pool size=50;          // Maksimum 50 bağlantı
min pool size=5;           // Minimum 5 bağlantı hazır tut
connection reset=true;     // Bağlantıları sıfırla
application intent=readwrite; // Okuma/yazma niyeti belirt
keepalive=30;              // 30 saniye TCP keepalive
```

### 2. Go'nun sql.DB Havuz Yönetiminin Yapılandırılması

```go
db.SetMaxOpenConns(50)              // Maksimum 50 açık bağlantı
db.SetMaxIdleConns(10)              // Maksimum 10 boşta bağlantı
db.SetConnMaxLifetime(30 * time.Minute) // Bağlantı ömrü en fazla 30 dakika
db.SetConnMaxIdleTime(5 * time.Minute)  // Boşta kalma süresi en fazla 5 dakika
```

### 3. Bağlantı Hata Toleransı İyileştirmeleri

- MSSQL Health Check için yeniden deneme mekanizması eklendi (3 deneme)
- Artan bekleme süreleri ile exponential backoff uygulandı
- Hata mesajları zenginleştirildi ve tanılamaya yönelik detaylar eklendi
- Bağlantı sorunlarına karşı timeout mekanizması eklendi
- Best Practices Analysis için timeout ve panic recovery eklendi

## Öneriler

1. Sorun tekrarlarsa agent servisini yeniden başlatın
2. Windows'ta aktif TCP bağlantılarını kontrol edin:
   ```
   netstat -ano | findstr 1433
   ```
3. TIME_WAIT durumundaki soketleri kontrol edin:
   ```
   netstat -ano | findstr TIME_WAIT
   ```
4. Sorunun devam etmesi durumunda agent log dosyalarını inceleyin

## Teknik Detaylar

Windows'ta TIME_WAIT soketi sorunu, çok sayıda kısa süreli bağlantı açılması durumunda oluşabilir. 
Varsayılan olarak, Windows'ta TIME_WAIT durumu 240 saniye (4 dakika) sürer ve bu süre içinde 
aynı soket adresi (IP:port) tekrar kullanılamaz. Yüksek hacimli bağlantılarda Windows'un 
ephemeral port havuzu (dinamik port aralığı) tükenebilir.

Bu düzeltme, bağlantı havuzunu optimize ederek aynı TCP bağlantılarının yeniden kullanılmasını 
sağlar ve böylece yeni soket oluşturma ihtiyacını azaltır. 