# Hydrometric Data Framework — reach.io Reference

> This document summarises how `reach.io` implements the F&W Imported Hydrometric Data Framework v1.2.
> For the authoritative framework, refer to the full document held in the team governance repository.

---

## The Medallion Architecture

All hydrometric data in the flode ecosystem passes through three quality tiers. `reach.io` handles ingestion to **Bronze** and provides schema enforcement for **Silver** promotion.

```
Supplier data
     │
     ▼
┌─────────┐   read-only archive; provenance-labelled
│ BRONZE  │   exactly as received; no corrections
└────┬────┘
     │  QC process (Stage 1–4)
     ▼
┌─────────┐   quality-flagged; corrections documented
│ SILVER  │   primary working dataset for modellers
└────┬────┘
     │  Owner approval
     ▼
┌─────────┐   formally approved analysis-ready products
│  GOLD   │   calibration inputs, FFA, ML training sets
└─────────┘
```

**Key principle:** Each tier is a preserved, immutable state. Data is never overwritten — moving to the next tier creates a new dataset. The previous tier is always retained.

---

## Bronze Tier — `reach.io` responsibilities

`reach.io` owns the Bronze ingestion pipeline. Its role is to:

1. Pull or read supplier data (HDE API, WISKI API, bulk files)
2. Assign a unique **dataset ID** using the standard format
3. Write a **provenance record** to the Hydrometric Data Register
4. Store data in the Bronze layer as **Parquet** — read-only after ingestion
5. Notify the Steward that a new Bronze dataset has been ingested

### Dataset ID format

```
[SupplierCode]_[SiteID]_[DataType]_[YYYYMMDD]

Examples:
  EA_39001_Q_20260115      ← EA flow gauge
  NRFA_39001_Q_20240301    ← NRFA flow archive
  MO_235096_P_20240101     ← Met Office rainfall
```

**Supplier codes:** `EA`, `NRFA`, `MO`, `CEH` — agree others with Steward before first use.

**Data type codes:**

| Code | Meaning |
|---|---|
| `Q` | Flow (discharge) |
| `H` | Stage (water level) |
| `P` | Rainfall |
| `SM` | Soil moisture |
| `SWE` | Snow water equivalent |

---

## Bronze Parquet Schema

`reach.io` enforces this 6-column mandatory schema via `apply_bronze_schema()`:

| Column | Type | Description |
|---|---|---|
| `timestamp` | `timestamp[ns, UTC]` | Observation datetime in UTC. Convert supplier local times at ingestion. |
| `value` | `float64` | Observed value exactly as received. Do not round or convert units. |
| `supplier_flag` | `string` | Quality code as provided by supplier. `NULL` if none provided. |
| `dataset_id` | `string` | Bronze dataset ID. Stored in every row — join key to the register. |
| `site_id` | `string` | Supplier's site identifier, preserved exactly as received. |
| `data_type` | `string` | Framework code: Q / H / P / SM / SWE |

Optional columns (include if provided by supplier):

| Column | Type | Description |
|---|---|---|
| `raw_value_str` | `string` | Raw string before parsing (retain if supplier uses special markers, e.g. `">"`) |
| `uncertainty` | `float64` | Measurement uncertainty if stated |
| `sensor_id` | `string` | Instrument ID where provided |

> **Do not add columns beyond those listed.** Bronze is a faithful record of what was received. Derived fields belong in Silver.

---

## Silver Parquet Schema

Silver is produced outside `reach.io` by the QC pipeline, but `reach.io` outputs are the input. Silver retains all Bronze columns and adds:

| Column | Type | Description |
|---|---|---|
| `qc_flag` | `int32` | Team quality flag (see table below) |
| `qc_value` | `float64` | Value to use for analysis. Equal to `value` for Good data; estimated value for flag=2; `NULL` for flags 4/5. Never overwrite `value`. |
| `estimation_method` | `string` | How `qc_value` was estimated for flag=2 rows. `NULL` otherwise. |
| `rating_curve_version` | `string` | Rating curve version applied to derive flow from stage. |
| `extrapolated` | `bool` | `TRUE` where stage is outside the valid rating range. |
| `silver_dataset_id` | `string` | Silver dataset ID, stored in every row. |
| `qc_version` | `int32` | Version of the Silver QC (1, 2, 3...) |

### Quality flags

| Flag | Label | Meaning | Use in modelling |
|---|---|---|---|
| 1 | Good | Passed all checks; expert review confirms no concerns | Use without restriction |
| 2 | Estimated | Value estimated using a documented method | Use with caution; document in run records |
| 3 | Suspect | Anomalous but not confirmed wrong; retained for user judgement | Exclude by default |
| 4 | Rejected | Confirmed erroneous | Do not use |
| 5 | No data | Genuinely missing; no basis for estimation | Do not use |
| 6 | Below detection | Below instrument detection limit | Use as threshold information only |

---

## Gold Tier — what `reach.io` produces

`reach.io` does not create Gold datasets directly — that requires Steward review and Owner approval. However, `reach.io` outputs (Bronze Parquet) are the upstream source for all Gold products.

**Only Gold-tier data may be used as input to Tier 1 model runs or ML model training promoted to operational use.**

### Gold ML training sets

A Gold ML training set is Silver data at native 15-minute resolution with static catchment attributes attached. Required static columns:

| Column | Type | Description |
|---|---|---|
| `catchment_area_km2` | `float64` | Catchment area in km² |
| `bfi` | `float64` | Base Flow Index (0–1) |
| `mean_elevation_m` | `float64` | Mean elevation in metres AOD |
| `dominant_soil_type` | `string` | HOST classification (e.g. `HOST_1`) |
| `land_cover_class` | `string` | Dominant land cover (e.g. `improved_grassland`) |
| `gauge_easting` | `float64` | OSGB36 easting |
| `gauge_northing` | `float64` | OSGB36 northing |

---

## Parquet Storage Structure

```
hydrometric/
├── bronze/
│   ├── EA/
│   │   ├── Q/
│   │   │   └── 2024/
│   │   │       ├── EA_39001_Q_20240101.parquet
│   │   │       └── EA_39001_Q_20240101_raw.csv   ← original supplier file kept
│   │   └── H/
│   └── NRFA/
├── silver/
│   ├── Q/
│   │   └── 39001/
│   │       ├── EA_39001_Q_20240101_SILVER_v1.parquet
│   │       └── EA_39001_Q_20240101_SILVER_v2.parquet   ← v1 kept
│   └── P/
└── gold/
    ├── Q/
    │   ├── calibration/
    │   └── FFA/
    └── P/
```

> Previous Silver and Gold versions are **never deleted or renamed**. Superseded datasets keep their version number in the filename and have their status updated in the register.

---

## Hydrometric Data Register

`write_provenance_record()` appends entries to the central Hydrometric Data Register — an Excel workbook or Delta table. Key fields written at Bronze ingestion:

| Field | Description |
|---|---|
| `dataset_id` | Bronze dataset ID |
| `tier` | Bronze |
| `supplier` | Name of supplying organisation |
| `site_identifier` | Gauge ID or station name as received |
| `data_type` | Q / H / P / SM / SWE |
| `time_period` | Start and end date |
| `temporal_resolution` | e.g. 15 min, daily |
| `units` | As stated by supplier |
| `date_received` | Date data arrived |
| `received_by` | Custodian name |
| `method_of_receipt` | API pull / email / portal download / FTP |
| `supplier_quality_flags` | Supplier flag codes and their meaning |
| `file_path` | Location in data store |
| `known_limitations` | Limitations stated by supplier |

---

## Bronze Rules — What `reach.io` Must Never Do

- ❌ Modify `value` after ingestion
- ❌ Delete or overwrite Bronze files
- ❌ Ingest data without a completed provenance record
- ❌ Use Bronze data directly as model input (Silver or Gold required)
- ❌ Share Bronze data externally without Owner approval and licence check

---

## Key Functions

| Function | Tier | Role |
|---|---|---|
| `download_hydrology()` | Bronze | Main entry point — pull data from HDE / WISKI / bulk file |
| `apply_bronze_schema()` | Bronze | Enforce mandatory 6-column schema |
| `make_dataset_id()` | Bronze | Generate standardised dataset ID |
| `write_provenance_record()` | Bronze | Append to Hydrometric Data Register |
| `run_backfill()` | Bronze | Parallelised historical ingest across all gauges |
| `run_incremental()` | Bronze | High-watermark append-only sync |
| `setup_hydro_store()` | Bronze | Create Bronze directory tree |
| `bronze_path()` | Bronze | Construct standardised file paths |
