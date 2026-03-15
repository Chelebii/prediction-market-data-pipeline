# BTC5M Scanner Gap Analysis and Upgrade Plan

## 1. Amac
Bu dokumanin amaci, mevcut scanner dosyasini:

- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py)

mevcut haliyle analiz edip, dataset planina gore:
- nelerin dogru oldugunu,
- nelerin eksik oldugunu,
- nelerin scanner icinde kalmasi gerektigini,
- nelerin ayri component olmasi gerektigini
netlestirmektir.

Bu dokumanin sonucu:
- scanner'a degisiklik gerekecek
- ama tum dataset sistemi scanner'in icine yigilmamali

---

## 2. Mevcut scanner'da dogru olan kisimlar

Asagidaki kisimlar korunmali:

### 2.1 CLOB-only fiyat mantigi dogru
Scanner fallback fiyat kullanmiyor.
Bu dogru.
Dataset kalitesi icin de bu korunmali.

Ilgili kisim:
- `btc_5min_clob_scanner.py` baslangic aciklamasi

### 2.2 Current-slot-first secimi dogru
Scanner once current slot'u tercih ediyor, next slot'u ancak sona yakin yayinliyor.
Bu trading davranisi icin mantikli.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L171)

### 2.3 Validation kati saglam
Mevcut scanner:
- BUY/SELL price cekiyor
- midpoint cekiyor
- spread kontrol ediyor
- midpoint deviation kontrol ediyor
- complement gap kontrol ediyor
- liquidity filtreliyor

Bu kisim dataset icin de degerli.

Ilgili kisimlar:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L270)
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L316)

### 2.4 Stable publish gate dogru
Tek scan'de gelen temiz quote'u hemen publish etmiyor.
Bu da iyi.
Transient noise'u azaltir.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L445)

### 2.5 Atomic snapshot write dogru
Bot tarafi icin JSON snapshot dosyasini atomik yazmasi dogru.
Bu korunmali.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L391)

### 2.6 No-data alert mantigi faydali
Operational olarak scanner'in fresh snapshot uretemedigini anlamak icin gerekli.
Bu da korunmali.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L484)

Sonuc:
Mevcut scanner'in "live bot icin temiz snapshot uretme" gorevi genel olarak dogru.
Sorun, bunun dataset collector ihtiyacini tek basina karsilamiyor olmasi.

---

## 3. Dataset planina gore scanner'daki eksikler

## 3.1 En buyuk eksik: scanner sadece publish ediyor, collect etmiyor
Mevcut scanner'in ana cikisi:
- tek bir JSON snapshot dosyasi
- log dosyasi

Ama dataset icin gerekli olan:
- tum observation'lar
- reject edilen adaylar
- warmup state'leri
- lifecycle event'ler
- raw DB kayitlari

Mevcut haliyle bunlar yok.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L402)

Etki:
- secim yanliligi olusur
- sadece "gecmis temiz quote" saklanir
- invalid state'ler kaybolur

## 3.2 Reject ve warmup durumlari structured degil
Scanner `WARMUP` ve `SKIP` logluyor ama veri tabanina yazmiyor.

Ilgili kisimlar:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L447)
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L478)

Bu dataset acisindan yetersiz.
Cunku strateji icin en onemli sorulardan biri su:
- hangi marketler neden elendi?

## 3.3 Snapshot semasi dataset icin dar
Mevcut payload'ta:
- `slot_end_ts` yok
- `seconds_to_resolution` yok
- `publish_reason` structured degil
- `reject_reason` yok
- `quote_stable_pass_count` yok
- `complement_gap` explicit alan olarak yok
- `orderbook_exists_yes/no` yok
- `market_status` yok
- `collected_ts` ve `written_ts` ayrimi yok

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L335)

## 3.4 Stale kontrolu pratikte etkisiz
Su an scanner `payload["ts"]` degerini kendisi yaziyor, sonra hemen onun yasini kontrol ediyor.
Bu kontrol gercek source staleness'i olcmuyor.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L460)

Bu nedenle:
- `MAX_BOOK_AGE_SEC` dataset acisindan guvenilir bir kalite olcusu degil
- sadece local processing age kontrolu yapiyor

## 3.5 Orderbook depth yok
Scanner `/book` endpoint'inden sadece best bid/ask ve size cekiyor.
Ilk 3-5 level depth yok.

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L200)

Etki:
- slippage simulasyonu zayif kalir
- execution realism eksik olur

## 3.6 Reference price entegrasyonu yok
Dataset planinda BTC reference tick zorunlu.
Scanner bunu hic toplamiyor.

Etki:
- market hareketi ile underlying hareketi ayni timeline'da baglanamiyor
- feature engineering zayif kaliyor

## 3.7 Lifecycle state capture yok
Scanner sadece aktif/publish edilebilir marketle ilgileniyor.
Su durumlari explicit state olarak tutmuyor:
- `DISCOVERED`
- `PUBLISHED`
- `REJECTED`
- `EXPIRED`
- `PENDING_SETTLEMENT`
- `RESOLVED`
- `CANCELLED`

Ilgili kisim:
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L122)
- [btc_5min_clob_scanner.py](C:/Users/mavia/.openclaw/workspace-mavi-x/xPolymarketBots/polymarket_scanner/btc_5min_clob_scanner.py#L171)

Etki:
- expiry sonrasi ne oldugu gorulmez
- no-orderbook state dataset'e yansimaz

## 3.8 Resolution ingestion yok
Scanner resmi market sonucunu toplamiyor.

Etki:
- label pipeline icin resmi source yok
- dataset supervised training icin eksik kaliyor

## 3.9 Audit / collector run kaydi yok
Mevcut scanner:
- snapshot sayisi
- reject sayisi
- run config hash
- duplicate count
- gap metrics
gibi bilgileri structured tutmuyor.

Etki:
- veri kalitesi olculemez
- run bazli karsilastirma zorlasir

---

## 4. Hangi gereksinimler scanner icinde olmali, hangileri ayri olmali?

Bu kisim kritik.
Tum dataset sorumlulugunu scanner'a yuklemek dogru degil.

## 4.1 Scanner icinde kalmasi gerekenler

Scanner icinde kalmali:
- market discovery
- YES/NO quote fetch
- top-of-book validation
- complement/liquidity checks
- stable pass logic
- bot icin JSON snapshot write
- raw DB snapshot write
- rejected candidate write
- orderbook depth summary write
- lifecycle event write
- lightweight run metrics

## 4.2 Scanner disina alinmasi gerekenler

Ayri component olmali:
- official resolution collector
- BTC reference collector
- audit script
- feature ETL
- label ETL
- decision dataset builder

Neden?
Scanner'in ana gorevi:
- hizli ve guvenilir observation

Scanner'in ana gorevi olmamali:
- ETL
- label generation
- training dataset build

Bu ayrim korunmazsa scanner hem karmasiklasir hem de live snapshot publish riske girer.

---

## 5. Mevcut scanner icin degismesi gerekenler

## 5.1 Non-breaking kural
En onemli kural:

**Mevcut bot snapshot JSON contract'i bozulmamali.**

Yani:
- `write_snapshot(...)` korunmali
- botun kullandigi temel alanlar degismemeli
- yeni alan eklenebilir ama eski alanlar kirilmamali

## 5.2 Scanner dual-output olmali
Scanner tek output yerine iki output uretmeli:

1. Bot icin:
- mevcut JSON snapshot

2. Dataset icin:
- SQLite raw write

Bu en dogru gecis modeli.

## 5.3 Her candidate observation saklanmali
Sadece valid publish degil:
- warmup pass
- reject
- token missing
- cross validation fail
- market not publishable
gibi durumlar da structured kaydedilmeli.

Bu noktada yeni kavram:
- `candidate observation`

Her scan'de her aday market icin bir observation row olusmali.

## 5.4 Snapshot row ile event row ayrilmali
Tek tabloya her seyi yigmak yerine:
- `btc5m_snapshots`
- `btc5m_lifecycle_events`
ayri olmali.

Bu scanner icin de daha temiz olur.

## 5.5 Depth aggregation eklenmeli
`fetch_book(...)` sadece best price degil, ilk 3 ve ilk 5 level notional toplamini da hesaplamali.

Bu degisiklik scanner icinde yapilmali.
Cunku `/book` response zaten scanner'in elinde.

## 5.6 Explicit market_status alani eklenmeli
Snapshot payload ve DB row icinde su explicit olmali:
- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`

Bu alanlar yoksa dataset path-dependent execution hatalarini anlayamaz.

## 5.7 Local latency olculmeli
Source timestamp yoksa bile scanner su alanlari yazmali:
- request start ts
- request end ts
- local fetch latency ms
- snapshot write ts

Bu minimum kalite altyapisidir.

## 5.8 Stale check yeniden tasarlanmali
Mevcut stale check kaldirilmamali ama dogru hale getirilmeli.

Yeni kural:
- snapshot file freshness ayridir
- source data staleness ayridir

Scanner icin uygulanabilir minimum:
- fetch latency olc
- scan cycle duration olc
- previous publish age olc
- `source_ts` yoksa `source_ts=NULL` ve `staleness_unknown` flag yaz

## 5.9 Resolution scanner'a degil, sidecar'a gitmeli
Resmi outcome toplama isi scanner icinde olmasin.
Ayri collector/script olmali.

Bu hem operasyonel hem mantiksal olarak daha dogru.

---

## 6. Onerilen upgrade fazlari

## Faz 0 - Refactor without behavior change
Hedef:
Mevcut scanner davranisini bozmadan ic yapisini hazirlamak.

Yapilacaklar:
- DB helper module ekle
- snapshot row mapper fonksiyonu ekle
- candidate result object yapisi ekle
- line-level log reason'larini structured hale getir

Teslimat:
- behavior ayni
- sadece kod tabani upgrade'e hazir

## Faz 1 - Raw DB write
Hedef:
Valid snapshot'lari SQLite'a da yazmak.

Yapilacaklar:
- `btc5m_markets` upsert
- `btc5m_snapshots` insert
- `collector_runs` start/finish

Teslimat:
- JSON snapshot devam eder
- DB side write baslar

## Faz 2 - Rejected observation capture
Hedef:
Secim yanliligini azaltmak.

Yapilacaklar:
- warmup observation kaydi
- reject_reason kaydi
- token missing / cross fail / price fail ayrimi
- `btc5m_lifecycle_events` tablosuna `REJECTED`, `PUBLISHED`

Teslimat:
- dataset'te neden elendigini gorebilir hale geliriz

## Faz 3 - Depth and latency
Hedef:
Execution realism eklemek.

Yapilacaklar:
- depth 3 / depth 5 aggregation
- local request latency
- scan duration
- write duration

Teslimat:
- slippage simulation icin minimum veri hazir olur

## Faz 4 - Explicit state and no-orderbook support
Hedef:
Lifecycle state'i netlestirmek.

Yapilacaklar:
- `market_status`
- `orderbook_exists_yes/no`
- `first_seen_ts`
- `last_seen_ts`
- `last_orderbook_seen_ts`

Teslimat:
- expiry/no-orderbook transition'lari gorulebilir olur

## Faz 5 - Reference and resolution integration
Hedef:
Scanner ile dataset ekosistemini baglamak.

Burada iki yol var:

Scanner'a direkt koyma:
- reference collector ayri script/process
- resolution collector ayri script/process

Scanner'in gorevi:
- sadece raw market observation

Teslimat:
- scanner daha guvenli kalir
- dataset gereksinimi tamamlanir

## Faz 6 - Audit readiness
Hedef:
Collector'in ne kadar iyi veri topladigini sayisal gormek.

Yapilacaklar:
- per-slot expected vs actual snapshot count
- duplicate snapshot audit
- missing reference audit
- missing resolution audit

Teslimat:
- scanner verisinin ise yarar olup olmadigi olculebilir

---

## 7. Scanner icin ozel task list

1. `common/btc5m_dataset_db.py` modulunu ekle.
2. Scanner'a config ile acilan DB write path ekle.
3. `build_snapshot(...)` yanina `build_snapshot_row(...)` ekle.
4. `scan_once()` icinde her candidate icin structured result objesi uret.
5. Valid observation'lari `btc5m_snapshots` tablosuna yaz.
6. Reject ve warmup observation'lari da yaz.
7. `fetch_book(...)` icinde depth aggregation ekle.
8. `market_status` ve `orderbook_exists_*` alanlarini explicit yaz.
9. Local latency metric'lerini ekle.
10. JSON snapshot contract'ini bozmadan backward-compatible tut.

---

## 8. Riskler ve dikkat edilmesi gerekenler

### 8.1 En buyuk risk: live botu bozmak
Scanner su an bot icin kritik.
Bu nedenle tum degisiklikler:
- additive olmali
- backward-compatible olmali
- JSON snapshot path'ini bozmamali

### 8.2 DB write scanner'i block etmemeli
SQLite write yuzunden scan loop gecikmemeli.

Tavsiye:
- basit transaction
- kisa insert path
- hata olursa logla ama publish'i durdurma

### 8.3 Scanner'a fazla sorumluluk yuklenmemeli
Reference feed ve resolution logic scanner'in icine tam gomulurse:
- debugging zorlasir
- outage riski artar

Bu nedenle ayri collector daha dogru.

---

## 9. Son karar
Evet, mevcut 5MIN BTC scanner'da dataset planina gore degismesi gereken seyler var.

Ama sonuc su:

- mevcut validation/publish mantigi korunmali
- scanner `snapshot publisher + raw collector` haline getirilmeli
- reference, resolution, audit ve ETL scanner disinda tutulmali

Yani scanner tamamen yeniden yazilmayacak.
Dogru yol:
- mevcut scanner'i kirpmadan genisletmek
- dataset icin structured raw write eklemek
- geri kalan pipeline'i ayri componentlerle kurmak

---

**Bagli dokumanlar:**
- [Backtest_Data_Collection_Plan.md](C:/Users/mavia/.openclaw/workspace-mavi-x/PROJECT_MANAGEMENT/Historical_Data_and_Backtesting/Strategy/Backtest_Data_Collection_Plan.md)
- [BTC5M_Dataset_Implementation_Spec.md](C:/Users/mavia/.openclaw/workspace-mavi-x/PROJECT_MANAGEMENT/Historical_Data_and_Backtesting/Strategy/BTC5M_Dataset_Implementation_Spec.md)
- [BTC5M_Dataset_Architecture_Diagram.md](C:/Users/mavia/.openclaw/workspace-mavi-x/PROJECT_MANAGEMENT/Historical_Data_and_Backtesting/Strategy/BTC5M_Dataset_Architecture_Diagram.md)
