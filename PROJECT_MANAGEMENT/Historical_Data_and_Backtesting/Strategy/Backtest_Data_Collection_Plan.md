# BTC 5MIN Up/Down Backtest Veri Toplama ve Dataset Plani

## 1. Amac
Amac, Polymarket BTC 5min up/down marketleri icin:
- backtest-ready,
- execution-aware,
- ML-ready,
- sonra istersek LLM-friendly
bir veri seti kurmaktir.

Bu veri seti su 4 soruya cevap verebilmelidir:
1. Bu anda hangi market tradable idi?
2. Hangi fiyattan gercekci sekilde girip cikabilirdim?
3. Market nasil resolve oldu?
4. Bu snapshot/decision noktasi model egitimi icin guvenilir mi?

Bu 4 sorudan birine cevap veremeyen veri seti eksik kabul edilmelidir.

---

## 2. Net Odak
Sadece su kapsam var:
- BTC 5min up/down marketleri
- raw market data collection
- label generation
- backtest ve strategy research
- ileride supervised ML / ranking / policy learning

Simdilik kapsam disi:
- ETH / SOL / diger coinler
- diger market tipleri
- genel scanner platformu
- ilk gunden distributed architecture
- ilk gunden LLM fine-tuning

---

## 3. Mevcut planda dogru olanlar
Asagidaki yonler korunmali:
- tek hedefe odaklanma: BTC 5min
- 2-5 saniye snapshot resolution fikri
- bid/ask + size toplama zorunlulugu
- BTC reference price katmani
- label / outcome katmani
- SQLite ile baslama
- quality kontrol ve audit fikri

Ama bunlar tek basina yeterli degil.
Ozellikle label tanimi, lifecycle state'leri ve execution gercekligi daha netlesmeli.

---

## 4. Kesin eklenmesi gereken kritik maddeler

### 4.1 Resolution rule metadata zorunlu
Sadece `resolved_outcome=Up/Down` yetmez.

Her market icin su alanlar kaydedilmeli:
- `resolution_source`
- `resolution_rule_text`
- `resolution_rule_version`
- `slot_start_reference_price`
- `slot_end_reference_price`
- `slot_start_reference_ts`
- `slot_end_reference_ts`
- `resolved_ts`
- `settled_ts`
- `market_resolution_status` -> `ACTIVE`, `EXPIRED`, `PENDING_SETTLEMENT`, `RESOLVED`, `CANCELLED`

Neden?
BTC reference olarak Binance toplamak faydali, ama label'i Binance'tan tahmin etmek tehlikelidir.
Label kaynagi marketin kendi resmi resolution bilgisinden gelmelidir.

### 4.2 Tum slotlar toplanmali, sadece trade edilenler degil
Dataset sadece botun trade actigi anlari toplarsa secim yanliligi olusur.

Bu yuzden:
- her aktif slot kaydedilmeli
- trade sinyali olmayan snapshot'lar da kaydedilmeli
- invalid / stale / reject snapshot'lar ayri flag ile tutulmali

Bu, hem gercek backtest hem de ML icin zorunludur.

### 4.3 Market lifecycle ve orderbook state'leri eklenmeli
Runtime incident'lerde goruldugu gibi, expiry sonrasi orderbook kaybolabilir.
Bu nedenle veri modeli sadece fiyat degil, market state de tasimalidir.

Zorunlu alanlar:
- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`
- `last_orderbook_seen_ts`
- `first_seen_ts`
- `last_seen_ts`
- `publish_reason`
- `reject_reason`

Backtest engine su farki anlayabilmeli:
- quote var
- quote yok
- market expired
- settlement bekleniyor
- market resolved

### 4.4 Execution icin depth / marketability verisi eklenmeli
Sadece top-of-book bazen yetmez.
En azindan su alanlar eklenmeli:
- `best_bid_yes`, `best_ask_yes`, `best_bid_no`, `best_ask_no`
- `best_bid_size_yes`, `best_ask_size_yes`, `best_bid_size_no`, `best_ask_size_no`
- `depth_3_levels_yes_bid_notional`
- `depth_3_levels_yes_ask_notional`
- `depth_3_levels_no_bid_notional`
- `depth_3_levels_no_ask_notional`
- `depth_5_levels_*` veya alternatif olarak `depth_within_1c`, `depth_within_2c`, `depth_within_5c`

Eger full orderbook pahaliysa, en azindan aggregated depth tutulmali.
Slippage simulasyonu icin bu kritik.

### 4.5 Source timestamp ve latency alanlari eklenmeli
Collector clock ile source clock ayni sey degildir.

Bu alanlar eklenmeli:
- `collected_ts_utc`
- `written_ts_utc`
- `source_ts_utc` (varsa)
- `collector_latency_ms`
- `reference_latency_ms`
- `snapshot_age_ms`

Boylece stale data analizi sayisal hale gelir.

### 4.6 Raw ve derived layer ayirilmali
Feature'lari toplama aninda yazmak pratik olabilir ama canonical dataset raw olmali.

Dogru ayrim:
- `raw layer`: market, snapshot, orderbook, reference, resolution
- `derived layer`: features, labels, decision rows

Boylece:
- feature formula degistiginde raw veri bozulmaz
- ayni ham veriden yeni experiment uretilebilir
- audit ve reproducibility korunur

### 4.7 Decision dataset tablosu eklenmeli
Tek basina raw snapshot tablosu model egitimi icin yeterli degildir.
Ek olarak bir "decision row" dataset lazim.

Her satir:
- bir market
- bir timestamp
- bir karar anidir

Alanlar:
- `market_id`
- `decision_ts`
- `seconds_to_resolution`
- raw quote summary
- reference summary
- feature set
- label set
- quality flags
- `is_trainable`

Bu tablo sonradan supervised ML, ranking model ve policy test icin ana tablo olur.

### 4.8 Time-based split ve leakage kurali eklenmeli
ML egitimi icin simdiden kural yazilmali:
- random split YASAK
- split sadece zamana gore
- ayni slotun verisi train ve test'e birlikte dusmemeli
- label'a yakin gelecek bilgisi feature'a sizmamali

Bu kural plana acik yazilmali.

### 4.9 Quality SLO / kabul kriterleri eklenmeli
Sadece "rapor olsun" demek yetmez.
Kabul kriterleri sayisal olmali.

Ornek:
- `slot_coverage_ratio >= 0.90`
- `max_snapshot_gap_sec <= 10`
- `reference_sync_gap_sec <= 1`
- `missing_resolution_count = 0`
- `invalid_book_ratio < 0.20`
- `duplicate_snapshot_ratio < 0.01`

Bu degerler sonra degisebilir ama planda threshold mantigi kesin olmali.

### 4.10 LLM notu dogru konumlanmali
Bu veri setinin ilk hedefi LLM olmamali.
Ilk hedef:
- backtest
- tabular/time-series ML

LLM sonraki asamada su sekilde kullanilabilir:
- snapshot window -> textual market summary
- anomaly explanation
- strategy diary / run analysis

Yani dataset semasi LLM'e gore degil, numeric research'e gore kurulmalidir.

---

## 5. Cikarilmasi veya ikinci plana atilmasi gerekenler

### 5.1 Collector icinde fazla feature hesaplama
Asagidaki seyler collector MVP'de zorunlu degil:
- RSI benzeri kompleks indicator'lar
- regime classifier
- cok sayida handcrafted feature

Bunlar offline ETL katmanina alinmali.
Collector tarafinda sadece ucuz ve debug icin kritik metrikler tutulabilir:
- `complement_gap`
- `price_mid_gap`
- `quote_stable_pass_count`

### 5.2 "Sadece resolved_outcome yeter" yaklasimi
Bu yaklasim cikarilmali.
Yerine su gelmeli:
- resmi label bilgisi
- start/end reference price
- resolution timestamps
- lifecycle status

### 5.3 "Sadece top-of-book yeter" varsayimi
Bu da cikarilmali veya zayif varsayim olarak isaretlenmeli.
Minimum seviye top-of-book olsa da, orta vadede depth summary zorunlu.

### 5.4 Erken model egitimi
Ilk gunden model egitimi hedeflenmemeli.
Dogru sira:
1. raw collection
2. audit
3. label generation
4. backtest
5. baseline ML
6. gerekirse LLM

---

## 6. Onerilen veri mimarisi

## 6.1 Raw layer tablolari

### `btc5m_markets`
Her slot/market icin metadata.

Zorunlu kolonlar:
- `market_id`
- `market_slug`
- `question`
- `slot_start_ts`
- `slot_end_ts`
- `yes_token_id`
- `no_token_id`
- `tick_size`
- `min_order_size`
- `resolution_source`
- `resolution_rule_text`
- `created_at`
- `first_seen_ts`
- `last_seen_ts`
- `market_resolution_status`

### `btc5m_snapshots`
Asil zaman serisi tablo.

Zorunlu kolonlar:
- `snapshot_id`
- `market_id`
- `collected_ts_utc`
- `written_ts_utc`
- `seconds_to_resolution`
- `best_bid_yes`
- `best_ask_yes`
- `best_bid_no`
- `best_ask_no`
- `mid_yes`
- `mid_no`
- `spread_yes`
- `spread_no`
- `best_bid_size_yes`
- `best_ask_size_yes`
- `best_bid_size_no`
- `best_ask_size_no`
- `liquidity_market`
- `complement_gap_mid`
- `complement_gap_cross`
- `book_valid`
- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`
- `reject_reason`
- `source_name`
- `collector_latency_ms`

### `btc5m_orderbook_depth`
Execution simulasyonu icin ozet derinlik tablosu.

Zorunlu kolonlar:
- `market_id`
- `collected_ts_utc`
- `yes_bid_depth_3`
- `yes_ask_depth_3`
- `no_bid_depth_3`
- `no_ask_depth_3`
- `yes_bid_depth_5`
- `yes_ask_depth_5`
- `no_bid_depth_5`
- `no_ask_depth_5`

Alternatif:
full depth yerine `within_1c`, `within_2c`, `within_5c` notional alanlari tutulabilir.

### `btc5m_reference_ticks`
BTC reference time series.

Zorunlu kolonlar:
- `ts_utc`
- `source_name`
- `btc_price`
- `btc_bid`
- `btc_ask`
- `btc_mark_price`
- `btc_index_price`
- `volume_1s`
- `latency_ms`

Ek olarak 1m OHLCV tablosu veya gorunum:
- `btc_1m_ohlcv`

### `btc5m_resolution`
Market sonuc ve resmi label tablosu.

Zorunlu kolonlar:
- `market_id`
- `slot_start_reference_price`
- `slot_end_reference_price`
- `resolved_outcome`
- `resolved_yes_price`
- `resolved_no_price`
- `resolved_ts`
- `settled_ts`
- `resolution_source`
- `market_resolution_status`
- `label_quality_flag`

### `btc5m_lifecycle_events`
Market state gecisleri.

Ornek event'ler:
- `DISCOVERED`
- `PUBLISHED`
- `REJECTED`
- `EXPIRED`
- `PENDING_SETTLEMENT`
- `RESOLVED`

Kolonlar:
- `event_id`
- `market_id`
- `event_ts`
- `event_type`
- `reason`
- `meta_json`

### `collector_runs`
Ingestion ve audit icin.

Kolonlar:
- `run_id`
- `started_ts`
- `ended_ts`
- `collector_version`
- `config_hash`
- `snapshot_count`
- `error_count`

### `quality_audits`
Gunluk / slot bazli kalite raporu.

Kolonlar:
- `audit_id`
- `audit_date`
- `market_id`
- `expected_snapshot_count`
- `actual_snapshot_count`
- `slot_coverage_ratio`
- `max_gap_sec`
- `invalid_book_ratio`
- `missing_reference_ratio`
- `missing_resolution_flag`
- `notes`

---

## 6.2 Derived layer tablolari

### `btc5m_features`
Raw veriden yeniden uretilebilir feature katmani.

Ornek kolonlar:
- `market_id`
- `ts_utc`
- `seconds_to_resolution`
- `return_15s`
- `return_30s`
- `return_60s`
- `return_120s`
- `volatility_30s`
- `volatility_60s`
- `volatility_180s`
- `microprice_yes`
- `microprice_no`
- `order_imbalance_yes`
- `order_imbalance_no`
- `complement_gap`
- `distance_to_0_5`
- `distance_to_recent_high`
- `distance_to_recent_low`
- `quote_stability_score`

### `btc5m_labels`
Tek bir market sonucundan fazlasini tutan label katmani.

Ornek kolonlar:
- `market_id`
- `decision_ts`
- `terminal_outcome`
- `mtm_return_if_buy_yes_hold_to_resolution`
- `mtm_return_if_buy_no_hold_to_resolution`
- `best_exit_yes_before_expiry`
- `best_exit_no_before_expiry`
- `would_hit_tp_5c`
- `would_hit_tp_10c`
- `would_hit_sl_5c`
- `time_to_best_yes`
- `time_to_best_no`
- `label_horizon_sec`

### `btc5m_decision_dataset`
Model egitimi ve research icin ana tablo.

Her satir:
- `market_id`
- `decision_ts`
- feature set
- quality set
- label set
- `is_trainable`
- `split_bucket`

Bu tablo raw'dan tekrar uretilebilir olmalidir.

---

## 7. Toplama frekansi ve kapsam

### Onerilen frekans
- market snapshot: her `2-3 saniye`
- reference tick: her `1 saniye`
- orderbook depth summary: her `2-5 saniye`
- resolution: market kapaninca ve settlement tamamlaninca

### Neden?
5 dakikalik markette son 30-90 saniye microstructure cok onemlidir.
60 saniyelik kayit cozunurlugu yetersizdir.

### Kapsam kurali
Bir market icin veri toplama:
- `DISCOVERED` aninda baslamali
- `RESOLVED` veya `CANCELLED` olana kadar surmeli

Sadece aktif quote olan anlari degil:
- quote yokken de state kaydi olmali
- rejection reason kaydi olmali

---

## 8. Label uretim kurali
En kritik konu budur.

Label uretimi su prensiple yapilmali:
- `resolved_outcome` resmi market sonucundan gelsin
- `slot_start_reference_price` ve `slot_end_reference_price` ayri saklansin
- `final_btc_price` tek basina label yerine gecmesin

Neden?
Cunku son fiyat bilgisi tek basina:
- hangi source'tan geldigi belli degilse
- hangi ana ait oldugu net degilse
- market rule ile birebir uyusmuyorsa
yanlis label uretebilir.

Bu yuzden label tablosunda:
- source
- timestamp
- quality flag
- status
ayri tutulmali.

---

## 9. Kalite kontrolleri
Asagidaki kontroller zorunlu:
- her slotta beklenen snapshot sayisi vs gercek snapshot sayisi
- duplicate snapshot var mi?
- max gap kac saniye?
- stale snapshot orani kac?
- invalid book orani kac?
- complement gap bozuklugu kac?
- orderbook yok ama market aktif gorunuyor mu?
- reference tick eksigi var mi?
- resolved ama label yok market var mi?
- cancelled / ambiguous market var mi?

Asagidaki alarm/SLO mantigi eklenmeli:
- `slot_coverage_ratio < threshold`
- `missing_resolution_count > 0`
- `max_gap_sec > threshold`
- `invalid_book_ratio > threshold`
- `reference_sync_gap_sec > threshold`

---

## 10. Backtest icin zorunlu simulasyon varsayimlari
Veri toplama plani ile birlikte simulasyon modeli de net yazilmali.

Engine en az su kurallari desteklemeli:
- fee modeli
- spread crossing
- order size vs available depth
- partial fill
- slippage
- expiry once force-exit
- expiry sonrasi no-orderbook durumu
- pending settlement state'i
- same-slot max trade count
- cooldown
- time-stop
- stop-loss / take-profit / trailing

Ozellikle:
`EXPIRED` ve `PENDING_SETTLEMENT` ayrimi simulasyonda acik olmali.

---

## 11. ML ve LLM icin veri hazirligi

### ML icin
Oncelik su olmali:
- tabular baseline
- time-series baseline
- ranking / classification

Hazirlik kurallari:
- random split kullanma
- train/validation/test zamana gore ayrilsin
- ayni slot iki split'e dusmesin
- feature computation label leakage olusturmasin

### LLM icin
LLM dusunulebilir ama ikinci asama olarak.
Gerekirse sonradan su formatlar uretilir:
- `window_summary_text`
- `market_context_text`
- `anomaly_report_text`

Ama ana dataset numeric ve structured kalmalidir.

---

## 12. Onerilen faz plani

### Faz 1 - Raw collector
Hedef:
BTC 5min marketleri icin raw ve immutable veri toplamak.

Teslimatlar:
- `btc5m_markets`
- `btc5m_snapshots`
- `btc5m_reference_ticks`
- `btc5m_resolution`
- `btc5m_lifecycle_events`

### Faz 2 - Quality / audit
Hedef:
Verinin kullanilabilir oldugunu ispatlamak.

Teslimatlar:
- `quality_audits`
- audit script
- stale / gap / missing label raporu

### Faz 3 - Execution realism
Hedef:
Backtest'i fill gercekligine yaklastirmak.

Teslimatlar:
- `btc5m_orderbook_depth`
- slippage model girdileri
- orderbook/state edge-case loglari

### Faz 4 - Feature + label ETL
Hedef:
Research ve modelleme icin derived katman uretmek.

Teslimatlar:
- `btc5m_features`
- `btc5m_labels`
- `btc5m_decision_dataset`

### Faz 5 - Backtest + baseline ML
Hedef:
Ayni dataset ile strateji karsilastirmasi yapmak.

Teslimatlar:
- parameterized backtest engine
- baseline strategy comparison
- baseline ML benchmark

---

## 13. Ilk uygulanacak minimum kapsam (gercek MVP)
Su alanlar olmadan dataset'e "ise yarar" demek zor:

### Minimum market metadata
- `market_id`
- `market_slug`
- `question`
- `slot_start_ts`
- `slot_end_ts`
- `yes_token_id`
- `no_token_id`

### Minimum snapshot
- `collected_ts_utc`
- `market_id`
- `seconds_to_resolution`
- `best_bid_yes`
- `best_ask_yes`
- `best_bid_no`
- `best_ask_no`
- `mid_yes`
- `mid_no`
- `spread_yes`
- `spread_no`
- `best_bid_size_yes`
- `best_ask_size_yes`
- `best_bid_size_no`
- `best_ask_size_no`
- `liquidity_market`
- `book_valid`
- `market_status`
- `orderbook_exists_yes`
- `orderbook_exists_no`

### Minimum reference
- `ts_utc`
- `source_name`
- `btc_price`

### Minimum label
- `resolved_outcome`
- `slot_start_reference_price`
- `slot_end_reference_price`
- `resolved_ts`
- `market_resolution_status`

Eger bu 4 grup eksikse, veri seti hem backtest hem ML icin zayif kalir.

---

## 14. Son karar
Bu proje icin dogru hedef:

**BTC 5min up/down marketleri icin raw + quality-scored + resolution-safe bir dataset kurmak.**

Bu dataset:
1. strategy backtest
2. execution simulation
3. supervised ML
4. daha sonra gerekirse LLM destekli research
icin uygun olmali.

Ana ilke:
- raw veri immutable olsun
- label resmi source'tan gelsin
- lifecycle state kaybi olmasin
- sadece trade edilen anlar degil, tum slotlar toplansin

---

**Durum:** Revize edildi
**Odak:** BTC 5min up/down dataset
**Son Guncelleme:** 2026-03-14
