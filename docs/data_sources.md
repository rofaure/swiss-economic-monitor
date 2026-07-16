# Data Sources

## Overview

| Source | Provider | Format | Frequency | Coverage |
|--------|----------|--------|-----------|----------|
| Unemployment BIT | OFS (Swiss Federal Statistical Office) | CSV | Monthly | 2010–present |
| Consumer Price Index | SNB (Swiss National Bank) | API JSON | Monthly | 1921–present (filtered 2010+) |
| SNB Policy Rate | SNB (Swiss National Bank) | API JSON | Monthly | 2000–present (filtered 2010+) |

All sources are publicly available, require no authentication, and are updated regularly by official Swiss federal institutions.

---

## Source 1 — OFS Unemployment BIT

**Dataset:** Taux de chômage au sens du BIT selon le sexe et les grandes régions

**URL:** `https://dam-api.bfs.admin.ch/hub/api/dam/assets/36519062/master`

**Provider:** Office fédéral de la statistique (OFS)

**Last updated:** May 2026

**License:** CC BY (opendata.swiss)

### Raw schema
| Column | Type | Description |
|--------|------|-------------|
| `INDICATORS_HRCHY` | string | Hierarchy level (P, P_1 … P_7) |
| `INDICATORS_FR` | string | Indicator type: TOTAL or NUTS2 |
| `INDICATORS_DE` | string | German equivalent — dropped in Silver |
| `GENDER_FR` | string | Total / Hommes / Femmes |
| `GENDER_DE` | string | German equivalent — dropped in Silver |
| `DETAILS_FR` | string | Region name (French) |
| `DETAILS_DE` | string | German equivalent — dropped in Silver |
| `PERIOD` | string | Format: `YYYY-MXX` (e.g. `2010-M01`) |
| `FREQ` | string | M (monthly) / Q (quarterly) / Y (yearly) |
| `MEASURE_FR` | string | Measure description — dropped in Silver |
| `MEASURE_DE` | string | German equivalent — dropped in Silver |
| `VALUE` | float | Unemployment rate (%) |
| `STATUS` | string | Data status code |
| `STATUS_1` | string | Additional status — dropped in Silver |

### Profiling findings
- 9,888 rows × 14 columns
- File mixes three frequencies (M/Q/Y) — Silver filters to M only
- Values null for all periods before 2010-M01 (96 months × 24 rows = 2,304 null rows)
- Three gender breakdowns: Total, Hommes, Femmes — Silver keeps Total only
- Two indicator levels: TOTAL (national aggregate) and NUTS2 (7 regions) — Silver keeps NUTS2 only
- Date format non-standard: `2010-M01` → parsed to `2010-01` in Silver

### Silver filters applied
| Filter | Reason |
|--------|--------|
| `FREQ = M` | Remove quarterly and annual rows |
| `PERIOD >= 2010-M01` | Remove pre-2010 null rows |
| `GENDER_FR = Total` | Aggregate only |
| `INDICATORS_FR = NUTS2` | Regional breakdown only |

### Regions (NUTS2)
| Code | Region | Main cities |
|------|--------|-------------|
| P_1 | Région lémanique | Geneva, Lausanne, Sion |
| P_2 | Espace Mittelland | Bern, Fribourg, Neuchâtel |
| P_3 | Suisse du Nord-Ouest | Basel, Aarau, Soleure |
| P_4 | Zurich | Zurich, Winterthur |
| P_5 | Suisse orientale | St-Gallen, Chur, Schaffhausen |
| P_6 | Suisse centrale | Lucerne, Zug, Altdorf |
| P_7 | Tessin | Lugano, Bellinzona, Locarno |

---

## Source 2 — SNB Consumer Price Index (CPI)

**Cube:** `plkopr`

**Base URL:** `https://data.snb.ch/api/cube/plkopr/data/json/fr`

**Provider:** Swiss National Bank (SNB / BNS)

**Frequency:** Monthly

**Coverage:** January 1921 → present (filtered to 2010+ in Silver)

### API response structure
```json
{
  "timeseries": [
    {
      "header": [{ "dimItem": "Indice suisse – Décembre 2025 = 100" }],
      "metadata": { "key": "EPB@SNB.plkopr{LD2010100}", "frequency": "P1M" },
      "values": [{ "date": "1921-01", "value": 19.59 }, ...]
    },
    {
      "header": [{ "dimItem": "Variation en % par rapport au mois correspondant de l'année précédente" }],
      "metadata": { "key": "EPB@SNB.plkopr{VVP}", "frequency": "P1M" },
      "values": [{ "date": "1922-01", "value": -15.07 }, ...]
    }
  ]
}
```

### Timeseries
| Key | Description | Gold column |
|-----|-------------|-------------|
| `LD2010100` | CPI index, base December 2025 = 100 | `cpi_index` |
| `VVP` | Year-on-year inflation rate (%) | `cpi_yoy` |

### Profiling findings
- 2 timeseries, 2,518 rows total after flatten
- Zero nulls
- Date format `YYYY-MM` — no parsing needed
- Data from 1921 — filtered to 2010+ in Silver (1,056 rows rejected)

---

## Source 3 — SNB Policy Rate

**Cube:** `snboffzisa`

**Base URL:** `https://data.snb.ch/api/cube/snboffzisa/data/json/fr`

**Provider:** Swiss National Bank (SNB / BNS)

**Frequency:** Monthly (end of month)

**Coverage:** January 2000 → present (filtered to 2010+ in Silver)

### Timeseries in cube (10 total)
| Series | Coverage | Used |
|--------|----------|------|
| Suisse - Taux directeur de la BNS | 2019-06 → present | ✓ |
| Suisse - BNS - LIBOR range lower | 2000-01 → 2019-05 | ✓ (midpoint) |
| Suisse - BNS - LIBOR range upper | 2000-01 → 2019-05 | ✓ (midpoint) |
| États-Unis - Fed range lower/upper | 2000-01 → present | ✗ |
| Zone euro/BCE (3 series) | 2000-01 → present | ✗ |
| Royaume-Uni - BoE | 2000-01 → present | ✗ |
| Japon - BoJ | 2000-01 → present | ✗ |

### Rate reconstruction

The SNB switched from a LIBOR target range to a single policy rate in June 2019. To produce a continuous Swiss rate series from 2010:

```
2010-01 → 2019-05 : (LIBOR_lower + LIBOR_upper) / 2   [_rate_source = LIBOR_midpoint]
2019-06 → present : SNB policy rate directly             [_rate_source = SNB_policy_rate]
```

The `_rate_source` column in Silver and Gold preserves the origin of each value.

### Transition continuity check
At the 2019-06 boundary, both series produce -0.75% — confirming a seamless reconstruction with no discontinuity.

### Profiling findings
- 10 timeseries, 2,710 rows total after flatten
- Zero nulls on Swiss series
- Date format `YYYY-MM` — consistent with CPI source
- 7 non-Swiss series rejected in Silver (2,160 rows)
