# BOT_NE_YAPAR.md  5MIN BOT

## 1) Gorev
5MIN bot, kisa vadeli (5 dakikalik) kripto up/down marketlerde hizli paper trade avciligi yapar.

## 2) Girdiler
- Ortak snapshot (ozellikle 5min market slug/token)
- Kendi DB (diger botlardan farkli sema)

## 3) Ciktilar
- `paper_trades.db` (5min'e ozel tablo yapisi)
- `CURRENT_BALANCE.txt`
- `bot.log`

> OPEN/CLOSE Telegram bildirimi yok.

## 4) Diger botlardan farki
- Slot/zaman penceresi mantigi agirlikli
- `coin`, `market_slug`, `slot_ts` ile calisir
- Expiry temelli kapanis davranisi belirgin

## 5) Islem acma mantigi
- Snapshot'ta o anki 5min marketlerini bulur
- Momentum benzeri sinyal ile OPEN kaydi olusturur

## 6) Islem kapama mantigi
- Slot suresi dolunca veya kural tetiklenince CLOSE eder
- PnL DB'ye yazar

## 7) Risk
- Kendi max open limiti
- Pozisyon boyutu
- Equity dosyasi ile kasa takibi

## 8) Izlenebilirlik
- CLOSED sayisi yuksek olabilir (slot yapisi geregi)
- Realized/Unrealized ayrimi raporlamada ozel ele alinmali

## 9) Hizli kontrol
1. 5MIN ACTIVE mi?
2. OPEN/CLOSED sayisi mantikli mi?
3. `CURRENT_BALANCE.txt` ve DB PnL tutarli mi?

## 10) Rol
- Tum stackte en hizli reaksiyon veren, kisa slot odakli bottur.

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
