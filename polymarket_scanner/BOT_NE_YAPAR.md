# BOT_NE_YAPAR.md  SCANNER

## 1) Gorev
Scanner, tum botlara ortak kullanilan market snapshot'ini uretir ve surekli guncel tutar.

## 2) Girdiler
- Polymarket/Gamma/CLOB veri kaynaklari
- API response'lari (markets, fiyatlar, spread)

## 3) Ciktilar
- `runtime/snapshots/polymarket_shared_snapshot.json`
  - `ts`
  - `markets`
  - `mids`
  - `spreads`
- Scanner loglari

## 4) Neden kritik?
- Diger tum botlar bu snapshot'i tuketiyor.
- Scanner stale olursa diger botlar teknik olarak ACTIVE olsa da karar kalitesi duser.

## 5) scanner_manager ne yapiyor?
- Scanner processini izler
- Donma/stale veriyi tespit eder
- Gerekirse yeniden baslatir
- Kritik scanner alarmlarini Telegram'a yollar

## 6) Hangi durumlarda alarm?
- Snapshot cok eskiyse (stale)
- Scanner process duserse
- API baglanti hatalari artarsa

## 7) Hizli kontrol
1. Snapshot dosyasi var mi?
2. `ts` su anki zamana yakin mi?
3. `markets/mids/spreads` bos mu dolu mu?
4. scanner_manager process aktif mi?

## 8) Fail-safe
- Tek-instance lock
- Manager restart davranisi
- Hata loglama + alert

## 9) Sistemdeki rolu
- Scanner = veri omurgasi.
- Scanner sagliksizsa trade botlarinin karari sagliksiz olur.

## Neye gore isleme girer / neye gore islemden cikar?

### Isleme giris (Entry)
- Snapshot verisi gecerliyse (`ts` stale degil) aday marketler degerlendirilir.
- Botun kendi strateji kosullari saglanmali:
  - momentum/drift esigi
  - spread limiti
  - fiyat bandi uygunlugu
  - cooldown engeli olmamasi
  - `MAX_OPEN_POSITIONS` ve pozisyon boyutu limitleri
- `ALLOW_NEW_ENTRIES=1` ise yeni pozisyon acilir; degilse sadece mevcut pozisyon yonetilir.

### Islemden cikis (Exit)
- Asagidaki kosullardan biri tetiklenince pozisyon kapanir:
  - TP/Trailing stop
  - Volatility stop veya fixed SL
  - Time-stop (pozisyon suresi doldu)
  - Bot/stratejiye ozel expiry/slot kurali (ozellikle 5MIN)
- Kapanislar DB'de reason/pnl ile kayda gecer.

### Kritik not
- Scanner ve Reporter trade acmaz/kapatmaz:
  - Scanner sadece veri uretir (snapshot)
  - Reporter sadece raporlar
- Trade giris/cikis kararini trade botlari verir: Core/Fast/Pair/Sports/5MIN.
