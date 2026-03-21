# BTC5M Dataset Architecture Diagram

## 1. Big Picture

The diagram below shows the end-to-end BTC 5-minute up/down dataset flow.

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

What this flow means:

- The scanner discovers markets and collects quotes.
- The reference collector captures the BTC side.
- The resolution collector captures the official outcome.
- All of them first write to the raw database.
- The derived layer is produced afterward.
- Backtesting and ML use the decision dataset plus the raw execution layer directly, rather than consuming raw tables blindly.

---

## 2. How Will the System Work?

```mermaid
flowchart TD
    A["1. Discover market"] --> B["2. Fetch YES/NO quotes"]
    B --> C["3. Fetch /book depth"]
    C --> D["4. Validate snapshot"]
    D --> E{"Valid?"}
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

The most critical point here is:

- invalid or rejected data is not thrown away
- it also becomes part of the dataset
- because real-world execution quality and signal reliability can only be understood this way

---

## 3. Raw vs. Derived Separation

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

The logic is:

- raw tables are the archive of reality
- derived tables are experiment products
- if the feature or label formula changes, we regenerate without touching raw data

---

## 4. Table Relationships

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

Note:

- `btc5m_reference_ticks` is not directly linked to a market via a foreign key.
- The join is done through time.

---

## 5. Market Lifecycle

This part must be mentally clear because backtest behavior will come from here.

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

Why is this state machine necessary?

- because a "tradable market" and a "market only waiting for resolution" are not the same thing
- the no-orderbook condition we saw in incidents must be reflected exactly in backtesting

---

## 6. How Will Labels Be Produced?

```mermaid
flowchart TD
    A["Resolved market"] --> B["Load all snapshots for market"]
    B --> C["Filter decision candidates"]
    C --> D{"Trainable?"}
    D -- "No" --> E["Write label_quality_flag + is_trainable=0"]
    D -- "Yes" --> F["Entry price from ask side"]
    F --> G["Compute hold-to-resolution target"]
    G --> H["Compute path-dependent labels\nTP / SL / best-exit / time-to-best"]
    H --> I["Join feature row"]
    I --> J["Assign time-based split"]
    J --> K["Write btc5m_labels"]
    K --> L["Write btc5m_decision_dataset"]
```

Critical rule:

- features only look backward
- labels may use future information
- train/test splitting is market-slot-based

---

## 7. How Will the Backtest Engine Use This Data?

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

The backtest will not simply look at the label and ask, "Did it predict correctly?"
It must also answer:

- could it really have been filled?
- what was the spread cost?
- was it possible to exit before expiry?
- what would happen in a no-orderbook condition?

---

## 8. Implementation Order

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

Why is this order correct?

- first, data loss is stopped
- then data quality is measured
- then the research layer is built

---

## 9. Short Summary

What we are actually building in this system is a three-layer structure:

1. Collection
   Scanner + reference + resolution -> raw DB

2. Data engineering
   Audit + feature ETL + label ETL -> derived dataset

3. Research
   Backtest + ML + later, if needed, LLM

The most important principle:

- first correct data
- then correct labels
- then strategy

---

**Related documents:**
- [Backtest_Data_Collection_Plan.md](../strategy/Backtest_Data_Collection_Plan.md)
- [BTC5M_Dataset_Implementation_Spec.md](../strategy/BTC5M_Dataset_Implementation_Spec.md)
