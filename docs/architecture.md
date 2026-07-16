# Architecture Documentation

## Pipeline Overview

The pipeline follows the medallion architecture pattern (Bronze → Silver → Gold), a standard approach for lakehouse environments such as Microsoft Fabric and Databricks.

Each layer has a single responsibility:
- **Bronze**: raw ingestion, never modified
- **Silver**: cleaning, typing, filtering, quality gates
- **Gold**: dimensional model, BI-ready

---

## Bronze Layer

### Principles
- Append-only: existing rows are never updated or deleted
- No transformation of any kind applied to source data
- Every row tagged with run metadata for full lineage

### Metadata columns added at ingestion
| Column | Description |
|--------|-------------|
| `_run_id` | UUID v4 identifying the ingestion run |
| `_ingested_at` | UTC timestamp of ingestion |
| `_source` | Human-readable source label |
| `_source_url` | Exact URL used to fetch the data |

### Why UUID for RunID and not a timestamp?
A timestamp is not guaranteed to be unique — two concurrent runs starting in the same second would share the same ID. UUID v4 generates a 128-bit random identifier with collision probability of ~1 in 5×10³⁶. This is the standard used by Airflow, dbt, and Fabric for run identification.

### Files
| File | Source | Rows | Format |
|------|--------|------|--------|
| `bronze_unemployment.parquet` | OFS CSV | 9,888 | Parquet |
| `bronze_cpi.parquet` | SNB API JSON | 2,518 | Parquet |
| `bronze_policy_rate.parquet` | SNB API JSON | 2,710 | Parquet |

---

## Silver Layer

### Principles
- No rows silently dropped — all filtered rows written to `_rejected.parquet` with `_rejection_reason`
- Bilingual columns (FR/DE) reduced to FR only
- Date formats normalised to `YYYY-MM`
- Bronze lineage columns preserved

### Unemployment (OFS)
Filters applied in order:
1. `FREQ = M` — monthly only (file contains M/Q/Y mixed)
2. `PERIOD >= 2010-M01` — pre-2010 data has no values (all null)
3. `GENDER_FR = Total` — aggregate only, no sex breakdown
4. `INDICATORS_FR = NUTS2` — regional level (7 regions), national aggregate dropped

Date parsing: `2010-M01` → `2010-01` (remove `M`)

| Output | Rows |
|--------|------|
| `silver_unemployment.parquet` | 1,365 |
| `silver_unemployment_rejected.parquet` | 8,523 |

### CPI (SNB)
Filters applied:
1. `date >= 2010-01` — align with other sources

Two series preserved:
- `LD2010100`: CPI index (base December 2025 = 100)
- `VVP`: year-on-year inflation rate (%)

| Output | Rows |
|--------|------|
| `silver_cpi.parquet` | 394 |
| `silver_cpi_rejected.parquet` | 2,124 |

### Policy Rate (SNB)
The SNB cube `snboffzisa` contains 10 timeseries covering multiple central banks (Fed, ECB, Bank of England, Bank of Japan). Only Swiss series are retained.

**Rate reconstruction — why two sources?**
The SNB adopted a single policy rate in June 2019. Prior to that, monetary policy was expressed as a LIBOR 3-month CHF target range (lower and upper bounds). To produce a continuous series from 2010:

- **2010-01 to 2019-05**: `(LIBOR_lower + LIBOR_upper) / 2` — midpoint of target range
- **2019-06 to present**: SNB policy rate directly

The `_rate_source` column (`LIBOR_midpoint` or `SNB_policy_rate`) preserves the origin of each value for auditability.

| Output | Rows |
|--------|------|
| `silver_policy_rate.parquet` | 197 |
| `silver_policy_rate_rejected.parquet` | 2,160 |

---

## Gold Layer

### Dimensional Model

```
DimDate                    DimRegion
────────────────           ──────────────────────
date_id (PK)  ◄──┐    ┌─► region_id (PK)
period             │    │   region_name
year               │    │   latitude
month              │    │   longitude
quarter            │    │   main_cities
year_month_label   │    │
                   │    │
         ┌─────────┘    └──────────┐
         │                         │
FactUnemployment          FactMacroIndicators
────────────────          ───────────────────
date_id (FK)              date_id (FK)
region_id (FK)            period
unemployment_rate         cpi_index
_run_id                   cpi_yoy
_ingested_at              swiss_policy_rate
                          _rate_source
```

### Why two fact tables?

Unemployment data has regional granularity (7 NUTS2 regions × month), while CPI and SNB policy rate are national indicators (1 value per month). Mixing different geographic grains in a single fact table is a modelling anti-pattern (Kimball). Two fact tables share DimDate for cross-indicator analysis in Power BI.

### DimDate
Surrogate key `date_id` uses format `YYYYMM` (e.g. `202301`) rather than auto-increment. This makes the key self-describing — a value of `201001` immediately communicates "January 2010" without joining to the dimension.

### DimRegion
GPS centroid coordinates included to enable Power BI map visuals. The 7 regions correspond to Swiss NUTS2 classification.

| Region | Main cities |
|--------|-------------|
| Région lémanique | Geneva, Lausanne, Sion |
| Espace Mittelland | Bern, Fribourg, Neuchâtel |
| Suisse du Nord-Ouest | Basel, Aarau, Soleure |
| Zurich | Zurich, Winterthur |
| Suisse orientale | St-Gallen, Chur, Schaffhausen |
| Suisse centrale | Lucerne, Zug, Altdorf |
| Tessin | Lugano, Bellinzona, Locarno |

### Gold table summary
| Table | Rows | Grain |
|-------|------|-------|
| `dim_date.parquet` | 197 | 1 row per month |
| `dim_region.parquet` | 7 | 1 row per region |
| `fact_unemployment.parquet` | 1,365 | 1 row per region × month |
| `fact_macro_indicators.parquet` | 197 | 1 row per month |

---

## CI/CD Pipeline

GitHub Actions workflow triggers on every PR targeting `develop` or `main`.

Steps:
1. Checkout code
2. Set up Python 3.12
3. Install dependencies (pandas, pyarrow, requests, pytest)
4. Run Bronze ingestion (live API + CSV calls)
5. Run Silver transformations
6. Run 23 pytest tests

A failed CI pipeline blocks the PR merge. This guarantees that no broken transformation reaches `develop` or `main`.

### Test coverage
| Test file | Tests | What is verified |
|-----------|-------|-----------------|
| `test_silver_unemployment.py` | 8 | Row counts, columns, nulls, types, period format |
| `test_silver_cpi.py` | 7 | Row counts, columns, nulls, types, period format |
| `test_silver_policy_rate.py` | 8 | Row counts, columns, nulls, rate source values, 2019 transition |
