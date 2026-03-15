# 5MIN Incident - 2026-03-14 - Live Close Failed After Expiry

## Ozet

Bu incident, `5MIN` live botta acilan bir pozisyonun `principal take` sonrasi kalan runner parcasinin
slot bittikten sonra kapatilamamasiyla ilgilidir.

Gorunen semptom:

- Telegram:
  - `CLOB API hatasi (market_sell(...)): not enough balance / allowance`
  - `LIVE CLOSE BASARISIZ`
  - `KILL SWITCH AKTIF`
- Bot log:
  - ayni pozisyon icin art arda `CLOSE failed ... MARKET_SELL: tum retry'lar basarisiz`

Bu hata ilk bakista wallet access veya allowance problemi gibi gorunuyordu.
Asil problem farkliydi.

## Semptomun Teknik Gorunumu

Incident sirasinda su akis oldu:

1. Live `OPEN` basariyla gonderildi.
2. Fiyat hedefe geldigi icin `REDUCE` basariyla calisti.
3. Pozisyonun bir kismi kapandi ve ana para geri alindi.
4. Kalan runner parca slot bitimine kadar elde tutuldu.
5. Slot bittikten sonra bot kalan parcayi `market_sell` ile kapatmaya calisti.
6. CLOB order tekrar tekrar basarisiz oldu.
7. Safety layer bunu live close failure olarak gorup kill switch acti.

## Root Cause

Root cause su:

- Pozisyonun kalan runner parcasi, market **expiry** sonrasina tasindi.
- Expiry sonrasinda ilgili token icin artik aktif bir **CLOB orderbook** yoktu.
- Bot eski mantikta bu durumu ayirt edemiyordu ve kalan tokeni hala satilabilir saniyordu.
- Sonuc olarak ayni expired token icin tekrar tekrar `market_sell` deneniyordu.

Yapilan dogrulama:

- Conditional token balance vardi.
- Conditional allowance da vardi.
- Buna ragmen public CLOB endpoint:
  - `/book`
  - `/midpoint`
  - `/price`
  icin `404 No orderbook exists for the requested token id` dondu.

Yani problem:

- `wallet access yok`
- `approval yok`

degildi.

Problem:

- **tokenin artik tradable orderbook'u olmamasi** idi.

## Neden "not enough balance / allowance" Gorundu?

SDK veya API tarafindaki hata mesaji, gercek root cause'u tam acik yansitmadi.

Bizim vakada:

- elde hala conditional token vardi
- ama satisa uygun orderbook yoktu
- bot da bunu ayirt etmeden `market_sell` retry loop'una girdi

Bu nedenle operator tarafinda hata ilk bakista `balance/allowance` problemi gibi gorundu.

## Uygulanan Cozum

### 1. No-orderbook detection eklendi

Dosya:

- `common/clob_client.py`

Eklenen mantik:

- `token_has_orderbook(token_id)` helper'i eklendi
- `midpoint` endpoint ile tokenin aktif orderbook'u var mi kontrol ediliyor
- `404 No orderbook exists` donerse `False` kabul ediliyor

### 2. Live close path sertlestirildi

Dosya:

- `common/execution.py`

Degisiklik:

- live `market_sell` basarisiz olursa,
- eger tokenin orderbook'u yoksa,
- bu durum artik dogrudan "fatal close failure" gibi ele alinmiyor

Boylece her no-orderbook durumu kill switch'e gitmiyor.

### 3. Yeni durum eklendi: `PENDING_SETTLEMENT`

Dosya:

- `polymarket_paper_bot_5min/polymarket_paper_bot.py`

Degisiklik:

- expiry sonrasi orderbook'u olmayan open pozisyon,
  artik `CLOSED` ya da `OPEN` gibi davranmiyor
- yeni bir state'e aliniyor:
  - `PENDING_SETTLEMENT`

Bu state'in amaci:

- sonsuz `close retry` loop'unu kesmek
- log spam'ini durdurmak
- sistemi tekrar yeni trade'lere hazir hale getirmek
- elde kalan unresolved tokeni operatora gorunur kılmak

### 4. Equity hesabi duzeltildi

Dosya:

- `polymarket_paper_bot_5min/polymarket_paper_bot.py`

Degisiklik:

- `PENDING_SETTLEMENT` durumundaki pozisyonlarin `realized_pnl_usd` katkisi
  equity hesabina dahil edildi

Bu sayede dashboard/local balance gorunumu tamamen bozulmadi.

### 5. Dashboard gorunurlugu eklendi

Dosya:

- `control/dashboard/server.py`

Degisiklik:

- `Pending` sayaci eklendi
- artik UI tarafinda:
  - `Open`
  - `Pending`
  - `Closed`
  ayrimi gorulebiliyor

### 6. Son 15 saniye exit davranisi degistirildi

Dosya:

- `polymarket_paper_bot_5min/polymarket_paper_bot.py`

Ek hardening:

- slotun son `15s` icinde bot artik partial `REDUCE` birakmiyor
- bu fazda dogrudan **full close** oncelikli

Amac:

- expiry'ye runner tasima olasiligini azaltmak
- ayni incident'in tekrar olma ihtimalini dusurmek

## Operasyonel Olarak Ne Yapildi?

Incident sonrasi su recovery adimlari uygulandi:

1. Acik pozisyon state'i incelendi
2. Conditional token balance ve allowance dogrulandi
3. Expired token icin orderbook olmadigi teyit edildi
4. Worker yeni kodla tekrar baslatildi
5. Stuck pozisyon `PENDING_SETTLEMENT` state'ine alindi
6. Kill switch temizlendi
7. Dashboard yeni state'i gosterecek sekilde yeniden yuklendi

## Incident Sonrasi Beklenen Davranis

Yeni davranis su:

- pozisyon expiry olmadan kapanabiliyorsa normal `CLOSE`
- expiry sonrasi orderbook yoksa:
  - artik sonsuz retry yok
  - pozisyon `PENDING_SETTLEMENT`
  - kill switch gereksiz yere acilmiyor
- son 15 saniyede:
  - partial runner birakmak yerine full close deneniyor

## Kalan Gercek Risk

Bu duzeltme su problemi cozer:

- expired/no-orderbook token icin sonsuz close retry

Ama sunu tamamen ortadan kaldirmaz:

- slot bittiginde markette likidite aniden kaybolursa,
  kalan position settlement veya resolution beklemek zorunda kalabilir

Yani:

- problem "runtime bug + state handling" tarafinda cozuldu
- ama market microstructure dogasi geregi expiry sonrasinda
  her tokenin aninda satilabilir kalacagi garanti degil

## Kisa Sonuc

Problem neydi:

- expiry sonrasi orderbook'u olmayan runner token icin botun tekrar tekrar `market_sell` denemesi

Ne uygulandi:

- no-orderbook detection
- `PENDING_SETTLEMENT` state
- dashboard pending count
- last 15s full close kuralı

Nasil cozuldu:

- retry loop kesildi
- kill switch temizlendi
- sistem tekrar calisir hale geldi
- unresolved kalan parca operator tarafinda gorunur ve takip edilebilir oldu
