# Swiss Economic Monitor

End-to-end data engineering pipeline on Microsoft Fabric — tracking Swiss economic indicators (unemployment, CPI, SNB policy rate) across regions and time.

Built as a portfolio project targeting the Swiss data engineering market.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│  OFS Unemployment BIT (CSV)  │  SNB CPI (API)  │  SNB Rate │
└──────────────────┬──────────────────┬───────────────┬───────┘
                   │                  │               │
                   ▼                  ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                           │
│         Raw ingestion — append-only, RunID tagged           │
│   bronze_unemployment  │  bronze_cpi  │  bronze_policy_rate │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                           │
│      Cleaned, typed, filtered — rejection logging           │
│   silver_unemployment  │  silver_cpi  │  silver_policy_rate │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                            │
│             Star schema — BI-ready, KPI-serving             │
│  DimDate │ DimRegion │ FactUnemployment │ FactMacroIndicators│
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                       POWER BI                              │
│         Swiss Economic Dashboard — regional + macro         │
└─────────────────────────────────────────────────────────────┘
```

---

## Stack

| Layer | Tool |
|-------|------|
| Ingestion | Python (requests, pandas) |
| Transformation | Python / PySpark (Fabric) |
| Storage | Parquet / Delta Lake (OneLake) |
| Orchestration | Microsoft Fabric Pipelines |
| Testing | pytest (23 tests) |
| CI/CD | GitHub Actions |
| BI | Power BI |
| Version control | Git / GitHub (feature → develop → main) |

---

## Data Sources

| Source | Format | Frequency | Grain |
|--------|--------|-----------|-------|
| OFS Unemployment BIT | CSV | Monthly | 7 Swiss regions |
| SNB CPI (plkopr) | API JSON | Monthly | National |
| SNB Policy Rate (snboffzisa) | API JSON | Monthly | National |

---

## Project Structure

```
swiss-economic-monitor/
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI pipeline
├── ingestion/
│   ├── bronze_ofs_chomage.py   # OFS unemployment ingestion
│   └── bronze_bns_api.py       # Generic SNB API ingestion
├── transforms/
│   ├── silver/
│   │   ├── silver_unemployment.py
│   │   ├── silver_cpi.py
│   │   └── silver_policy_rate.py
│   └── gold/
│       └── gold_layer.py
├── notebooks/
│   ├── 01_bronze_exploration.ipynb   # Source profiling
│   ├── 02_bronze_ingestion.ipynb     # Ingestion dev
│   ├── 03_silver_transformation.ipynb # Transformation dev
│   └── 04_gold_layer.ipynb           # Gold layer dev
├── tests/
│   ├── test_silver_unemployment.py   # 8 tests
│   ├── test_silver_cpi.py            # 7 tests
│   └── test_silver_policy_rate.py    # 8 tests
├── fabric/
│   └── pipelines/                    # Fabric pipeline definitions
├── docs/
│   ├── architecture.md
│   └── data_sources.md
├── requirements.txt
└── README.md
```

---

## Running the Pipeline Locally

### Prerequisites
```bash
pip install pandas pyarrow requests pytest
```

### Full pipeline
```bash
# Bronze ingestion
python ingestion/bronze_ofs_chomage.py
python ingestion/bronze_bns_api.py

# Silver transformations
python transforms/silver/silver_unemployment.py
python transforms/silver/silver_cpi.py
python transforms/silver/silver_policy_rate.py

# Gold layer
python transforms/gold/gold_layer.py
```

### Run tests
```bash
python -m pytest tests/ -v
```

---

## Key Architecture Decisions

**Medallion architecture (Bronze/Silver/Gold)**
Bronze is append-only and never modified — it is the replayability guarantee. If anything breaks downstream, the full pipeline can be replayed from Bronze.

**Two fact tables in Gold**
Unemployment data is regional (7 NUTS2 regions), while CPI and SNB policy rate are national. Mixing different geographic grains in a single fact table is a modelling anti-pattern. Two facts share a common DimDate for cross-indicator analysis in Power BI.

**SNB rate reconstruction**
The SNB adopted a single policy rate in June 2019. Prior to that, monetary policy was expressed as a LIBOR target range. The Silver layer reconstructs a continuous series: midpoint(lower + upper LIBOR bounds) for 2010–2019, direct SNB policy rate from 2019 onwards. The `_rate_source` column preserves lineage.

**Rejection logging**
No rows are silently dropped. Every filtered row is written to a `_rejected.parquet` file with a `_rejection_reason` column. This enables auditability and debugging without re-ingesting from source.

**RunID + timestamp on every Bronze row**
Every ingestion run is tagged with a UUID (`_run_id`) and UTC timestamp (`_ingested_at`). This allows pinpointing exactly which run produced a given row — critical for debugging and compliance.

---

## CI/CD

GitHub Actions runs on every PR targeting `develop` or `main`:
1. Installs dependencies
2. Runs full Bronze ingestion (live API calls)
3. Runs Silver transformations
4. Runs 23 pytest tests across all Silver layers

A PR cannot be merged if the CI pipeline fails.

---

## License

MIT
