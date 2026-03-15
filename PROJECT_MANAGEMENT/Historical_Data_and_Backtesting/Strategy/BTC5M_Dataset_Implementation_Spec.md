# BTC5M Dataset Implementation Spec

## 1. Amac
Bu dokuman, [Backtest_Data_Collection_Plan.md](C:/Users/mavia/.openclaw/workspace-mavi-x/PROJECT_MANAGEMENT/Historical_Data_and_Backtesting/Strategy/Backtest_Data_Collection_Plan.md) icindeki dataset hedefini uygulanabilir hale getirir.

Bu dokumanin kapsami:
- concrete SQLite schema
- collector implementation task list
- label ETL checklist
- validation ve acceptance kriterleri

Odak:
- sadece BTC 5min up/down
- raw-first dataset
- backtest ve ML icin reusable pipeline

---

## 2. Tasarim prensipleri

### 2.1 Raw immutable olmali
Collector tarafinda yazilan raw tablolar sonradan mutate edilmemeli.
Hatali veri duzeltilecekse:
- yeni audit kaydi acilmali
- yeni derived table uretilmeli
- raw tablo overwrite edilmemeli

### 2.2 Official resolution label'dan ayrilmamali
`resolved_outcome` ve settlement durumu resmi market sonucundan gelmeli.
Reference exchange price sadece:
- feature
- debug
- reconciliation
icin kullanilmali.

### 2.3 State kaybi olmamali
Dataset sadece "iyi quote" anlarini degil, state gecislerini de tutmali:
- discovered
- publishable
- rejected
- expired
- pending settlement
- resolved

### 2.4 Derived her zaman raw'dan tekrar uretilebilir olmali
Feature ve label tablolari disposable kabul edilmeli.

---

## 3. Onerilen dosya/module dagilimi

Asagidaki dagilim mevcut repo yapisina uygundur:

- `polymarket_scanner/btc_5min_clob_scanner.py`
  Collector main loop. Discovery, quote fetch, validation, DB write trigger.

- `common/btc5m_dataset_db.py`
  SQLite connect, schema migration, insert helpers, upsert helpers.

- `common/btc5m_reference_feed.py`
  BTC reference tick fetch ve normalize helper.

- `scripts/btc5m_build_labels.py`
  Resolution tablosundan label generation.

- `scripts/btc5m_build_features.py`
  Raw snapshot + reference veriden feature generation.

- `scripts/btc5m_build_decision_dataset.py`
  Feature + label join ederek final research dataset uretir.

- `scripts/btc5m_audit_dataset.py`
  Coverage, gap, missing label, invalid ratio audit.

Bu ayrim scanner kodunu sade tutar ve ETL mantigini runtime collector'dan ayirir.

---

## 4. SQLite schema

## 4.1 DB dosya onerisi

Tek DB ile basla:

```text
runtime/data/btc5m_dataset.db
```

WAL acik olmali.
Collector ve offline ETL ayni anda okuyabilmeli.

## 4.2 PRAGMA ayarlari

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -20000;
```

## 4.3 Raw layer DDL

### `btc5m_markets`

```sql
CREATE TABLE IF NOT EXISTS btc5m_markets (
    market_id TEXT PRIMARY KEY,
    market_slug TEXT NOT NULL UNIQUE,
    question TEXT NOT NULL,
    slot_start_ts INTEGER NOT NULL,
    slot_end_ts INTEGER NOT NULL,
    yes_token_id TEXT NOT NULL,
    no_token_id TEXT NOT NULL,
    tick_size REAL,
    min_order_size REAL,
    resolution_source TEXT,
    resolution_rule_text TEXT,
    resolution_rule_version TEXT,
    first_seen_ts INTEGER NOT NULL,
    last_seen_ts INTEGER NOT NULL,
    created_at_ts INTEGER NOT NULL,
    market_resolution_status TEXT NOT NULL DEFAULT 'ACTIVE',
    resolved_outcome TEXT,
    resolved_yes_price REAL,
    resolved_no_price REAL,
    resolved_ts INTEGER,
    settled_ts INTEGER,
    slot_start_reference_price REAL,
    slot_end_reference_price REAL,
    slot_start_reference_ts INTEGER,
    slot_end_reference_ts INTEGER,
    label_quality_flag TEXT,
    notes TEXT
);
```

Indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_btc5m_markets_slot_start_ts ON btc5m_markets(slot_start_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_markets_slot_end_ts ON btc5m_markets(slot_end_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_markets_status ON btc5m_markets(market_resolution_status);
```

### `btc5m_snapshots`

```sql
CREATE TABLE IF NOT EXISTS btc5m_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    collected_ts INTEGER NOT NULL,
    written_ts INTEGER NOT NULL,
    source_ts INTEGER,
    seconds_to_resolution INTEGER NOT NULL,
    best_bid_yes REAL,
    best_ask_yes REAL,
    best_bid_no REAL,
    best_ask_no REAL,
    mid_yes REAL,
    mid_no REAL,
    spread_yes REAL,
    spread_no REAL,
    best_bid_size_yes REAL,
    best_ask_size_yes REAL,
    best_bid_size_no REAL,
    best_ask_size_no REAL,
    liquidity_market REAL,
    tick_size REAL,
    min_order_size REAL,
    complement_gap_mid REAL,
    complement_gap_cross REAL,
    price_mid_gap_yes_buy REAL,
    price_mid_gap_yes_sell REAL,
    price_mid_gap_no_buy REAL,
    price_mid_gap_no_sell REAL,
    quote_stable_pass_count INTEGER,
    book_valid INTEGER NOT NULL,
    market_status TEXT NOT NULL,
    orderbook_exists_yes INTEGER NOT NULL,
    orderbook_exists_no INTEGER NOT NULL,
    publish_reason TEXT,
    reject_reason TEXT,
    source_name TEXT NOT NULL,
    collector_latency_ms INTEGER,
    reference_sync_gap_ms INTEGER,
    snapshot_age_ms INTEGER,
    meta_json TEXT,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_snapshots_market_ts
    ON btc5m_snapshots(market_id, collected_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_collected_ts
    ON btc5m_snapshots(collected_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_market_status
    ON btc5m_snapshots(market_status);
CREATE INDEX IF NOT EXISTS idx_btc5m_snapshots_book_valid
    ON btc5m_snapshots(book_valid);
```

### `btc5m_orderbook_depth`

```sql
CREATE TABLE IF NOT EXISTS btc5m_orderbook_depth (
    depth_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    collected_ts INTEGER NOT NULL,
    yes_bid_depth_3 REAL,
    yes_ask_depth_3 REAL,
    no_bid_depth_3 REAL,
    no_ask_depth_3 REAL,
    yes_bid_depth_5 REAL,
    yes_ask_depth_5 REAL,
    no_bid_depth_5 REAL,
    no_ask_depth_5 REAL,
    yes_bid_depth_within_1c REAL,
    yes_ask_depth_within_1c REAL,
    no_bid_depth_within_1c REAL,
    no_ask_depth_within_1c REAL,
    yes_bid_depth_within_2c REAL,
    yes_ask_depth_within_2c REAL,
    no_bid_depth_within_2c REAL,
    no_ask_depth_within_2c REAL,
    yes_bid_depth_within_5c REAL,
    yes_ask_depth_within_5c REAL,
    no_bid_depth_within_5c REAL,
    no_ask_depth_within_5c REAL,
    source_name TEXT NOT NULL,
    meta_json TEXT,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_orderbook_depth_market_ts
    ON btc5m_orderbook_depth(market_id, collected_ts);
```

### `btc5m_reference_ticks`

```sql
CREATE TABLE IF NOT EXISTS btc5m_reference_ticks (
    ref_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc INTEGER NOT NULL,
    source_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    btc_price REAL NOT NULL,
    btc_bid REAL,
    btc_ask REAL,
    btc_mark_price REAL,
    btc_index_price REAL,
    volume_1s REAL,
    latency_ms INTEGER,
    meta_json TEXT
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_reference_ticks_source_ts
    ON btc5m_reference_ticks(source_name, symbol, ts_utc);
CREATE INDEX IF NOT EXISTS idx_btc5m_reference_ticks_ts
    ON btc5m_reference_ticks(ts_utc);
```

### `btc5m_reference_1m_ohlcv`

```sql
CREATE TABLE IF NOT EXISTS btc5m_reference_1m_ohlcv (
    candle_ts INTEGER PRIMARY KEY,
    source_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    trade_count INTEGER,
    meta_json TEXT
);
```

### `btc5m_lifecycle_events`

```sql
CREATE TABLE IF NOT EXISTS btc5m_lifecycle_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    event_ts INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT,
    meta_json TEXT,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_btc5m_lifecycle_market_ts
    ON btc5m_lifecycle_events(market_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_btc5m_lifecycle_event_type
    ON btc5m_lifecycle_events(event_type);
```

### `collector_runs`

```sql
CREATE TABLE IF NOT EXISTS collector_runs (
    run_id TEXT PRIMARY KEY,
    started_ts INTEGER NOT NULL,
    ended_ts INTEGER,
    collector_name TEXT NOT NULL,
    collector_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    snapshot_count INTEGER NOT NULL DEFAULT 0,
    market_count INTEGER NOT NULL DEFAULT 0,
    reference_tick_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    meta_json TEXT
);
```

### `quality_audits`

```sql
CREATE TABLE IF NOT EXISTS quality_audits (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_ts INTEGER NOT NULL,
    audit_date TEXT NOT NULL,
    market_id TEXT,
    run_id TEXT,
    expected_snapshot_count INTEGER,
    actual_snapshot_count INTEGER,
    slot_coverage_ratio REAL,
    max_gap_sec REAL,
    invalid_book_ratio REAL,
    duplicate_snapshot_ratio REAL,
    missing_reference_ratio REAL,
    missing_resolution_flag INTEGER NOT NULL DEFAULT 0,
    reference_sync_gap_sec REAL,
    audit_status TEXT NOT NULL,
    notes TEXT
);
```

## 4.4 Derived layer DDL

### `btc5m_features`

```sql
CREATE TABLE IF NOT EXISTS btc5m_features (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    ts_utc INTEGER NOT NULL,
    seconds_to_resolution INTEGER NOT NULL,
    return_15s REAL,
    return_30s REAL,
    return_60s REAL,
    return_120s REAL,
    volatility_30s REAL,
    volatility_60s REAL,
    volatility_180s REAL,
    microprice_yes REAL,
    microprice_no REAL,
    order_imbalance_yes REAL,
    order_imbalance_no REAL,
    complement_gap REAL,
    spread_sum REAL,
    depth_ratio_yes REAL,
    depth_ratio_no REAL,
    quote_stability_score REAL,
    feature_version TEXT NOT NULL,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_features_market_ts_version
    ON btc5m_features(market_id, ts_utc, feature_version);
```

### `btc5m_labels`

```sql
CREATE TABLE IF NOT EXISTS btc5m_labels (
    label_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    decision_ts INTEGER NOT NULL,
    label_horizon_sec INTEGER NOT NULL,
    terminal_outcome TEXT NOT NULL,
    resolved_yes_price REAL,
    resolved_no_price REAL,
    mtm_return_if_buy_yes_hold_to_resolution REAL,
    mtm_return_if_buy_no_hold_to_resolution REAL,
    best_exit_yes_before_expiry REAL,
    best_exit_no_before_expiry REAL,
    would_hit_tp_5c INTEGER,
    would_hit_tp_10c INTEGER,
    would_hit_sl_5c INTEGER,
    would_hit_sl_10c INTEGER,
    time_to_best_yes_sec REAL,
    time_to_best_no_sec REAL,
    label_quality_flag TEXT NOT NULL,
    label_version TEXT NOT NULL,
    FOREIGN KEY (market_id) REFERENCES btc5m_markets(market_id)
);
```

Indexes:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_btc5m_labels_market_ts_version
    ON btc5m_labels(market_id, decision_ts, label_horizon_sec, label_version);
```

### `btc5m_decision_dataset`

```sql
CREATE TABLE IF NOT EXISTS btc5m_decision_dataset (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    decision_ts INTEGER NOT NULL,
    seconds_to_resolution INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    mid_yes REAL,
    mid_no REAL,
    spread_yes REAL,
    spread_no REAL,
    btc_price REAL,
    quote_stability_score REAL,
    terminal_outcome TEXT,
    target_yes_hold REAL,
    target_no_hold REAL,
    label_quality_flag TEXT,
    is_trainable INTEGER NOT NULL,
    split_bucket TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    UNIQUE (market_id, decision_ts, dataset_version)
);
```

---

## 5. Schema notlari

### 5.1 Integer timestamp standardi
Tum runtime timestamp alanlari Unix epoch second olarak saklansin.
Eger ms gerekiyorsa:
- ayri `*_ms` alanlari eklenebilir
- ama ana join key second bazli kalsin

### 5.2 Bool alan standardi
SQLite icin:
- `0 = false`
- `1 = true`

### 5.3 Versioned ETL
Asagidaki derived alanlar version tasimali:
- `feature_version`
- `label_version`
- `dataset_version`

Bu sayede formula degisikligi izlenebilir olur.

---

## 6. Collector task list

## 6.1 Faz 1 - DB temel katmani

### Task 1
Yeni modul ekle:
- `common/btc5m_dataset_db.py`

Icerik:
- `connect_db(db_path)`
- `migrate_schema(conn)`
- `upsert_market(...)`
- `insert_snapshot(...)`
- `insert_orderbook_depth(...)`
- `insert_reference_tick(...)`
- `insert_lifecycle_event(...)`
- `start_collector_run(...)`
- `finish_collector_run(...)`

Definition of done:
- schema tek fonksiyonla olusuyor
- ikinci kez calisinca bozulmuyor
- WAL aktif

### Task 2
DB yolu `.env` ile configurable olsun.

Yeni env:
- `BTC5M_DATASET_DB_PATH`

Default:
- `runtime/data/btc5m_dataset.db`

## 6.2 Faz 2 - Scanner raw write

### Task 3
`polymarket_scanner/btc_5min_clob_scanner.py` icinde market metadata write ekle.

Yapilacaklar:
- market discover edilince `btc5m_markets` upsert
- `DISCOVERED` lifecycle event yaz
- current slot / next slot secim sebebi loglanabilir meta olarak sakla

### Task 4
Her valid scan sonunda snapshot write ekle.

Yapilacaklar:
- mevcut `build_snapshot(...)` payload'undan DB row map et
- `collected_ts` ve `written_ts` ayir
- `publish_reason` ve `reject_reason` tut
- `book_valid`, `complement_gap`, `price_mid_gap` sakla

### Task 5
Reject edilen adaylari da kaydet.

Bu kritik.
Sadece publish edilen snapshot degil:
- rejected candidate row
- reject reason
- state event
de saklanmali.

Minimum yol:
- `btc5m_snapshots` tablosuna `book_valid=0` ve `reject_reason`
- ek olarak `btc5m_lifecycle_events` tablosuna `REJECTED`

### Task 6
Orderbook depth summary ekle.

Mevcut scanner su an top level cekiyor.
Gelistirme:
- `/book` response icinden ilk 3 ve ilk 5 level notional toplamlarini hesapla
- tabloya yaz

Not:
Full raw orderbook saklamak MVP icin sart degil.

## 6.3 Faz 3 - Reference feed

### Task 7
Yeni helper modul ekle:
- `common/btc5m_reference_feed.py`

Yapilacaklar:
- Binance veya secilen source icin tick fetch
- normalize field names
- latency olc
- 1 second cadence write

### Task 8
Reference collector loop ekle.

Uygulama secenekleri:
- scanner icinde lightweight loop
- veya ayri process

Tavsiye:
- ilk asamada scanner icinde yap
- sonra gerekirse ayir

### Task 9
Snapshot ile en yakin reference tick eslestirme kuralini sabitle.

Kural:
- snapshot ts ile reference ts farki `<= 1 sec`
- fark buyukse `reference_sync_gap_ms` yaz
- kalite flag dusur

## 6.4 Faz 4 - Lifecycle ve resolution

### Task 10
Market status gecislerini explicit hale getir.

Status/event list:
- `DISCOVERED`
- `PUBLISHED`
- `REJECTED`
- `EXPIRED`
- `PENDING_SETTLEMENT`
- `RESOLVED`
- `CANCELLED`

### Task 11
Resolution collector ekle.

Yapilacaklar:
- market kapaninca resmi sonuc kontrol et
- resolution fields `btc5m_markets` ve gerekirse ayri resolution row'a yaz
- `RESOLVED` lifecycle event ekle

### Task 12
Expiry sonrasi no-orderbook durumunu state olarak yakala.

Neden?
Runtime incident'te bu durum gercek.
Backtest tarafi bunu gormek zorunda.

Yapilacaklar:
- `orderbook_exists_yes/no`
- `last_orderbook_seen_ts`
- `PENDING_SETTLEMENT` event

## 6.5 Faz 5 - Audit script

### Task 13
Yeni script ekle:
- `scripts/btc5m_audit_dataset.py`

Kontroller:
- slot coverage
- duplicate row
- missing reference
- missing resolution
- invalid ratio
- max snapshot gap

### Task 14
Gunluk audit raporu tabloya ve stdout'a yazilsin.

MVP:
- console summary
- `quality_audits` insert

---

## 7. Label ETL checklist

## 7.1 Label ETL girdileri
Label ETL su tablolari kullanir:
- `btc5m_markets`
- `btc5m_snapshots`
- `btc5m_reference_ticks`
- `btc5m_lifecycle_events`

## 7.2 Label ETL adimlari

### Step 1
Sadece terminal durumu net marketleri sec.

Dahil:
- `RESOLVED`

Disla:
- `ACTIVE`
- `PENDING_SETTLEMENT`
- `CANCELLED` unless ayri label strategy tanimliysa

### Step 2
Her market icin official terminal fields'i dogrula.

Kontrol et:
- `resolved_outcome`
- `resolved_yes_price`
- `resolved_no_price`
- `resolved_ts`

Eksikse:
- `label_quality_flag = MISSING_OFFICIAL_RESOLUTION`
- market train set disi

### Step 3
Decision timestamp candidate'larini sec.

Oneri:
- her snapshot bir candidate olabilir
- ama son 3 saniye veya stale snapshot'lar opsiyonel olarak dislanabilir

Pragmatic MVP:
- `seconds_to_resolution >= 5`
- `book_valid = 1`
- reference sync uygun

### Step 4
Her decision row icin entry price kuralini sabitle.

Tavsiye:
- YES long icin `best_ask_yes`
- NO long icin `best_ask_no`

Hold-to-resolution return:
- YES: `resolved_yes_price - entry_price`
- NO: `resolved_no_price - entry_price`

### Step 5
Path-dependent label'lari hesapla.

Ornek:
- `best_exit_yes_before_expiry`
- `best_exit_no_before_expiry`
- `would_hit_tp_5c`
- `would_hit_sl_5c`
- `time_to_best_yes_sec`

Bu hesapta:
- sadece decision ts sonrasi snapshot'lar kullanilmali
- expiry sonrasi no-orderbook varsa path buna gore kesilmeli veya state olarak islenmeli

### Step 6
Leakage kontrolu uygula.

Kurallar:
- feature hesaplama decision ts sonrasini kullanamaz
- ayni slotun farkli satirlari farkli split'e gidemez
- label hesaplama future kullanabilir, feature kullanamaz

### Step 7
Trainability flag belirle.

`is_trainable = 1` kosullari:
- official resolution mevcut
- snapshot valid
- reference sync kabul edilebilir
- duplicate degil
- stale degil
- cancelled degil

Aksi halde:
- `is_trainable = 0`
- reason notu veya quality flag yaz

### Step 8
Versionli label output uret.

Output:
- `btc5m_labels`
- `btc5m_decision_dataset`

Versionlar:
- `label_version`
- `dataset_version`

---

## 8. Feature ETL checklist

### Minimum feature set
Ilk versiyonda asagidakiler yeter:
- `return_15s`
- `return_30s`
- `return_60s`
- `volatility_30s`
- `volatility_60s`
- `spread_sum`
- `complement_gap`
- `order_imbalance_yes`
- `order_imbalance_no`
- `depth_ratio_yes`
- `depth_ratio_no`
- `quote_stability_score`
- `seconds_to_resolution`

### Feature kurallari
- sadece gecmise bak
- forward fill limitli olsun
- missing reference varsa flag yaz
- exact formula versionlansin

---

## 9. Acceptance kriterleri

Collector ve dataset "kullanilabilir" sayilmasi icin:

- `slot_coverage_ratio >= 0.90`
- `max_snapshot_gap_sec <= 10`
- `missing_resolution_count = 0` for completed markets
- `duplicate_snapshot_ratio < 0.01`
- `reference_sync_gap_sec <= 1` median
- `invalid_book_ratio < 0.20`
- `trainable_row_ratio >= 0.70` after first stable data window

Bu threshold'lar sonradan optimize edilir ama ilk raporlama icin yeterli.

---

## 10. Hemen uygulanacak coding order

1. `common/btc5m_dataset_db.py` olustur.
2. Scanner'a `btc5m_markets` ve `btc5m_snapshots` write ekle.
3. Reject/invalid adaylari da kaydet.
4. Reference tick writer ekle.
5. Lifecycle event write ekle.
6. Audit script yaz.
7. Resolution ingestion yaz.
8. Feature ETL yaz.
9. Label ETL yaz.
10. Decision dataset builder yaz.

Bu sira korunursa:
- once veri kaybi engellenir
- sonra quality gorulur
- sonra model/backtest tarafina gecilir

---

## 11. Kisa teknik not
MVP icin en buyuk hata su olur:

Sadece bot snapshot JSON'unu arsivleyip dataset saymak.

Bu yetersizdir cunku:
- rejected candidate yok
- lifecycle state yok
- reference sync kaniti zayif
- resolution metadata eksik
- ETL reproducibility dusuk

Dogru MVP:
- raw DB write
- audit
- sonra ETL

---

**Durum:** Implementation spec hazir
**Bagli plan:** [Backtest_Data_Collection_Plan.md](C:/Users/mavia/.openclaw/workspace-mavi-x/PROJECT_MANAGEMENT/Historical_Data_and_Backtesting/Strategy/Backtest_Data_Collection_Plan.md)
**Son guncelleme:** 2026-03-14
