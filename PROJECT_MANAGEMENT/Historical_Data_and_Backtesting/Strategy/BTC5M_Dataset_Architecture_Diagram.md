# BTC5M Dataset Architecture Diagram

## 1. Buyuk resim
Asagidaki diyagram, BTC 5min up/down dataset akisini uctan uca gosterir.

```mermaid
flowchart LR
    A["Gamma Events API\nmarket discovery"] --> B["BTC5M Scanner\ncandidate selection"]
    C["Polymarket CLOB\nbook / price / midpoint"] --> B
    D["BTC Reference Feed\nBinance or chosen source"] --> E["Reference Collector"]
    B --> F["Validation Layer\nspread / complement / liquidity / stale checks"]
    E --> G["Raw SQLite DB"]
    F --> G
    H["Resolution Collector\nofficial market outcome"] --> G

    G --> I["Audit Layer\ncoverage / gap / duplicate / missing label"]
    G --> J["Feature ETL"]
    G --> K["Label ETL"]

    J --> L["Decision Dataset Builder"]
    K --> L
    I --> L

    L --> M["Backtest Engine"]
    L --> N["Baseline ML"]
    G --> O["Execution Simulator"]
    L --> P["Later: LLM Research Layer"]
```

Bu akisin anlami:
- Scanner marketi bulur ve quote toplar.
- Reference collector BTC tarafini toplar.
- Resolution collector resmi sonucu toplar.
- Hepsi once raw DB'ye gider.
- Derived katman sonra uretilir.
- Backtest ve ML raw yerine dogrudan decision dataset + raw execution katmanini kullanir.

---

## 2. Sistem nasil calisacak?

```mermaid
flowchart TD
    A["1. Discover market"] --> B["2. Fetch YES/NO quotes"]
    B --> C["3. Fetch /book depth"]
    C --> D["4. Validate snapshot"]
    D --> E{"Valid mi?"}
    E -- "Yes" --> F["Write valid snapshot"]
    E -- "No" --> G["Write rejected snapshot + reject_reason"]
    F --> H["Write lifecycle event: PUBLISHED"]
    G --> I["Write lifecycle event: REJECTED"]
    F --> J["Match nearest BTC reference tick"]
    J --> K["Wait for market expiry/resolution"]
    K --> L["Write official resolution"]
    L --> M["Run audit"]
    M --> N["Build features"]
    N --> O["Build labels"]
    O --> P["Build decision dataset"]
    P --> Q["Backtest / ML / research"]
```

Buradaki en kritik nokta:
- invalid veya reject olan data cope atilmiyor
- onlar da dataset'in parcasi oluyor
- cunku gercek dunya execution kalitesi ve signal reliability ancak boyle anlasilir

---

## 3. Raw ve derived ayrimi

```mermaid
flowchart LR
    subgraph Raw["Raw Layer"]
        R1["btc5m_markets"]
        R2["btc5m_snapshots"]
        R3["btc5m_orderbook_depth"]
        R4["btc5m_reference_ticks"]
        R5["btc5m_lifecycle_events"]
        R6["collector_runs"]
        R7["quality_audits"]
    end

    subgraph Derived["Derived Layer"]
        D1["btc5m_features"]
        D2["btc5m_labels"]
        D3["btc5m_decision_dataset"]
    end

    R1 --> D2
    R2 --> D1
    R2 --> D2
    R3 --> D1
    R4 --> D1
    R5 --> D2
    R7 --> D3
    D1 --> D3
    D2 --> D3
```

Mantik su:
- raw tablolar gercegin arsivi
- derived tablolar experiment urunu
- feature veya label formulu degisirse raw'a dokunmadan yeniden uretiriz

---

## 4. Tablo iliskileri

```mermaid
erDiagram
    btc5m_markets ||--o{ btc5m_snapshots : has
    btc5m_markets ||--o{ btc5m_orderbook_depth : has
    btc5m_markets ||--o{ btc5m_lifecycle_events : has
    btc5m_markets ||--o{ btc5m_features : has
    btc5m_markets ||--o{ btc5m_labels : has
    btc5m_markets ||--o{ btc5m_decision_dataset : has
    collector_runs ||--o{ quality_audits : produces

    btc5m_markets {
        text market_id PK
        text market_slug
        integer slot_start_ts
        integer slot_end_ts
        text market_resolution_status
        text resolved_outcome
    }

    btc5m_snapshots {
        integer snapshot_id PK
        text market_id FK
        integer collected_ts
        integer seconds_to_resolution
        real best_bid_yes
        real best_ask_yes
        real best_bid_no
        real best_ask_no
        integer book_valid
        text reject_reason
    }

    btc5m_orderbook_depth {
        integer depth_id PK
        text market_id FK
        integer collected_ts
        real yes_bid_depth_3
        real yes_ask_depth_3
        real no_bid_depth_3
        real no_ask_depth_3
    }

    btc5m_reference_ticks {
        integer ref_id PK
        integer ts_utc
        text source_name
        real btc_price
    }

    btc5m_lifecycle_events {
        integer event_id PK
        text market_id FK
        integer event_ts
        text event_type
        text reason
    }

    btc5m_features {
        integer feature_id PK
        text market_id FK
        integer ts_utc
        text feature_version
    }

    btc5m_labels {
        integer label_id PK
        text market_id FK
        integer decision_ts
        text terminal_outcome
        text label_version
    }

    btc5m_decision_dataset {
        integer row_id PK
        text market_id FK
        integer decision_ts
        integer is_trainable
        text split_bucket
        text dataset_version
    }
```

Not:
- `btc5m_reference_ticks` markete direkt FK ile bagli degil.
- join, zaman uzerinden yapiliyor.

---

## 5. Market lifecycle
Bu kisim zihinde net oturmali cunku backtest davranisi buradan cikacak.

```mermaid
stateDiagram-v2
    [*] --> DISCOVERED
    DISCOVERED --> PUBLISHED: valid snapshot
    DISCOVERED --> REJECTED: invalid quote
    REJECTED --> PUBLISHED: next valid pass
    PUBLISHED --> PUBLISHED: more valid snapshots
    PUBLISHED --> REJECTED: temporary invalid state
    PUBLISHED --> EXPIRED: slot end reached
    REJECTED --> EXPIRED: slot end reached
    EXPIRED --> PENDING_SETTLEMENT: no orderbook / waiting settle
    EXPIRED --> RESOLVED: official result ready
    PENDING_SETTLEMENT --> RESOLVED: settlement completed
    DISCOVERED --> CANCELLED: market invalidated
    PUBLISHED --> CANCELLED: market invalidated
```

Bu state machine neden gerekli?
- cunku "trade acilabilir market" ile "sadece resolve bekleyen market" ayni sey degil
- incident'te gordugumuz no-orderbook durumu backtest'e aynen yansitilmali

---

## 6. Label nasil ureyecek?

```mermaid
flowchart TD
    A["Resolved market"] --> B["Load all snapshots for market"]
    B --> C["Filter decision candidates"]
    C --> D{"Trainable mi?"}
    D -- "No" --> E["Write label_quality_flag + is_trainable=0"]
    D -- "Yes" --> F["Entry price from ask side"]
    F --> G["Compute hold-to-resolution target"]
    G --> H["Compute path-dependent labels\nTP / SL / best-exit / time-to-best"]
    H --> I["Join feature row"]
    I --> J["Assign time-based split"]
    J --> K["Write btc5m_labels"]
    K --> L["Write btc5m_decision_dataset"]
```

Kritik kural:
- feature sadece gecmise bakar
- label gelecegi kullanabilir
- train/test split market-slot bazli olur

---

## 7. Backtest motoru bu datayi nasil kullanacak?

```mermaid
flowchart LR
    A["Decision Dataset"] --> B["Strategy Logic\nentry filters / thresholds"]
    B --> C["Execution Simulator"]
    D["Raw Snapshots"] --> C
    E["Orderbook Depth"] --> C
    F["Lifecycle Events"] --> C
    G["Resolution"] --> C
    C --> H["Trade Log"]
    H --> I["PnL Metrics"]
    H --> J["Risk Metrics"]
    H --> K["Compare strategies"]
```

Backtest sadece label'a bakip "dogru tahmin etti mi" demeyecek.
Su sorulara da cevap verecek:
- gercekten fill olabilir miydi?
- spread maliyeti neydi?
- expiry once cikis mumkun muydu?
- no-orderbook durumunda ne olurdu?

---

## 8. Uygulama sirasi

```mermaid
flowchart TD
    A["Step 1\nDB schema + migration"] --> B["Step 2\nScanner raw writes"]
    B --> C["Step 3\nReject snapshot capture"]
    C --> D["Step 4\nReference ticks"]
    D --> E["Step 5\nLifecycle + resolution"]
    E --> F["Step 6\nAudit script"]
    F --> G["Step 7\nFeature ETL"]
    G --> H["Step 8\nLabel ETL"]
    H --> I["Step 9\nDecision dataset"]
    I --> J["Step 10\nBacktest engine"]
```

Bu sira neden dogru?
- once veri kaybi durur
- sonra veri kalitesi olculur
- sonra research katmani insa edilir

---

## 9. Kisa ozet
Bu sistemde yapacagimiz sey aslinda 3 katmanli:

1. Collection
   Scanner + reference + resolution -> raw DB

2. Data engineering
   Audit + feature ETL + label ETL -> derived dataset

3. Research
   Backtest + ML + sonra gerekirse LLM

En onemli ilke:
- once dogru veri
- sonra dogru label
- sonra strateji

---

**Bagli dokumanlar:**
- [Backtest_Data_Collection_Plan.md](Backtest_Data_Collection_Plan.md)
- [BTC5M_Dataset_Implementation_Spec.md](BTC5M_Dataset_Implementation_Spec.md)
