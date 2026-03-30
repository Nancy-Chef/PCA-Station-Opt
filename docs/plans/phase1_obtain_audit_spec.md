# Phase 1 Artifact 4: Obtain-Stage Audit Specification

## Purpose
This document specifies exactly what `01_obtain.py` must produce.  It defines every
required audit output, the checks that generate them, and the acceptance criteria that
determine whether the Obtain stage has succeeded.  No implementation code is written
here; this spec is the contract that Phase 2 coding must satisfy.

---

## Audit Output Files

All audit outputs are written to `data/scrubbed/obtain_audit/`.  This folder must be
created by `01_obtain.py` if it does not exist.

| Output File | Format | Description |
|---|---|---|
| `file_inventory.csv` | CSV | One row per discovered file with metadata |
| `schema_families.csv` | CSV | Schema family assignment per file |
| `coverage_matrix.csv` | CSV | Station × year × month presence grid |
| `duplicate_candidates.csv` | CSV | Files sharing (station_id, year, month) |
| `contamination_flags.csv` | CSV | Files where folder-station ≠ filename-station |
| `anomaly_log.csv` | CSV | All filename anomalies, format warnings, and unresolved issues |
| `obtain_summary.txt` | plain text | Human-readable audit summary for review |

---

## File Inventory (`file_inventory.csv`)

One row per file discovered under `data/raw/`.

Required columns:

| Column | Type | Description |
|---|---|---|
| `file_path` | string | Absolute path to the file |
| `folder_station_id` | string | Station assigned from folder path |
| `filename_prefix` | string | Detected filename prefix before normalisation |
| `filename_station_id` | string or null | Station derived from filename prefix (null if not recognised) |
| `year_folder` | integer or null | Year extracted from parent/grandparent folder |
| `month_raw` | string or null | Raw month token from filename (e.g. `Sept`) |
| `month_canonical` | integer or null | Normalised month (1–12) |
| `file_extension` | string | e.g. `.csv`, `.xle`, `.xlsx` |
| `schema_family` | string | `A`, `B`, `C`, `D`, or `UNKNOWN` |
| `row_count` | integer or null | Number of data rows (excluding header) if CSV |
| `header_preview` | string | First-row header (truncated to 200 chars) |
| `status` | string | `OK`, `ANOMALY`, `CONTAMINATION_CANDIDATE`, `EXCLUDED`, `DOWNLOAD_REQUIRED` |
| `status_note` | string | Free-text explanation for any non-OK status |

---

## Schema Families (`schema_families.csv`)

Aggregated view of schema family detection across all CSV files.

Required columns:

| Column | Type | Description |
|---|---|---|
| `station_id` | string | |
| `year` | integer | |
| `month` | integer | |
| `schema_family` | string | `A`, `B`, `C`, `D`, `UNKNOWN` |
| `file_path` | string | Source file |
| `column_count` | integer | Total number of columns in header |
| `has_temperature` | bool | |
| `has_rh` | bool | |
| `has_dew_point` | bool | |
| `has_rain` | bool | |
| `has_wind_speed` | bool | |
| `has_wind_dir` | bool | |
| `has_solar` | bool | |
| `has_marine_sensors` | bool | Any of water pressure/level/flow/temperature present |
| `has_duplicate_columns` | bool | Any column name appears more than once |
| `timestamp_offset_observed` | string or null | First UTC offset token found in Time column (e.g. `-0400`) |

---

## Coverage Matrix (`coverage_matrix.csv`)

Station × year × month presence grid.

Required columns:

| Column | Type | Description |
|---|---|---|
| `station_id` | string | |
| `year` | integer | |
| `month` | integer | 1–12 |
| `file_count` | integer | Number of CSV files for this station-year-month |
| `status` | string | `PRESENT`, `MISSING`, `EXCESS` (>1 file = excess) |

Coverage expectations per station:
- `CAV`: 12 months per year for 2022 (Oct–Dec only), 2023, 2024, 2025 (through available)
- `GRE`: 12 months per year for 2022 (Oct–Dec only), 2023, 2024, 2025 — **known gap: 2023–2025 folders appeared empty in workspace snapshot; verify at runtime**
- `NRW`: monthly HOBOlink CSVs from 2023 onward
- `SBW`: monthly HOBOlink CSVs from 2023 onward; 2022 has only Solinst files
- `TRW`: monthly HOBOlink CSVs from 2023 onward; no 2022 data
- `STA`: `DOWNLOAD_REQUIRED` for all months until acquisition runs

---

## Duplicate Candidates (`duplicate_candidates.csv`)

Lists any (station_id, year, month) combination that has more than one CSV file.

Required columns:

| Column | Type | Description |
|---|---|---|
| `station_id` | string | |
| `year` | integer | |
| `month` | integer | |
| `file_count` | integer | |
| `file_paths` | string | Pipe-separated list of all matching file paths |
| `row_counts` | string | Pipe-separated row counts in same order as file_paths |

---

## Contamination Flags (`contamination_flags.csv`)

Lists files where the folder-derived station id does not match the filename-derived
station id.

Required columns:

| Column | Type | Description |
|---|---|---|
| `file_path` | string | |
| `folder_station_id` | string | Station from folder |
| `filename_station_id` | string | Station from filename prefix |
| `year_folder` | integer | |
| `month_canonical` | integer | |
| `recommended_action` | string | `MOVE_TO_CORRECT_FOLDER` or `VERIFY_MANUALLY` |

Known case to detect at runtime:
`data/raw/North Rustico Wharf/2025/PEINP_TR_WeatherStn_Feb2025.csv`
→ folder=`NRW`, filename=`TRW`, recommended_action=`MOVE_TO_CORRECT_FOLDER`

---

## Anomaly Log (`anomaly_log.csv`)

All other issues that do not fit into contamination or duplicate categories.

Required columns:

| Column | Type | Description |
|---|---|---|
| `file_path` | string | |
| `anomaly_type` | string | Code from the anomaly-type vocabulary below |
| `detail` | string | Specific description of the issue |

Anomaly-type vocabulary:

| `anomaly_type` | Meaning |
|---|---|
| `FILENAME_TRUNCATED` | Filename year or month token is incomplete (e.g. `Jul202`) |
| `MONTH_NAME_NONSTANDARD` | Month token uses non-standard spelling (e.g. `Sept`, `July`) — normalised but logged |
| `DUPLICATE_COLUMNS` | Header row contains the same column name more than once |
| `EMPTY_FILE` | CSV exists but has zero data rows after header |
| `SCHEMA_UNKNOWN` | Could not classify into any known schema family |
| `NO_TIMESTAMP_OFFSET` | Time column present but no UTC offset token detected |
| `EXTENSION_NOT_CSV` | Non-CSV file found in a data year folder (e.g. `.xle`, `.xlsx`) |
| `STANHOPE_NOT_DOWNLOADED` | Stanhope hourly file not present; acquisition required |
| `SOLINST_FORMAT` | Stanley Bridge 2022 Solinst file; excluded from pipeline |
| `MISSING_MONTH` | Expected month file absent for a station-year combination |

---

## Obtain Summary (`obtain_summary.txt`)

A plain-text report written last, after all CSV outputs are generated.  Minimum content:

```
OBTAIN STAGE AUDIT SUMMARY
Generated: <timestamp UTC>

FILES DISCOVERED
  Total files found:         <n>
  CSV files (loadable):      <n>
  Non-CSV files (excluded):  <n>
  Files with OK status:      <n>
  Files with anomalies:      <n>
  Contamination candidates:  <n>
  Duplicate candidates:      <n>

COVERAGE BY STATION
  CAV  : <year range>, <n> months present, <n> months missing
  GRE  : <year range>, <n> months present, <n> months missing
  NRW  : <year range>, <n> months present, <n> months missing
  SBW  : <year range>, <n> months present, <n> months missing
         Note: 2022 = Solinst format (excluded from PCA/FWI)
  TRW  : <year range>, <n> months present, <n> months missing
  STA  : DOWNLOAD REQUIRED — no hourly observations present

SCHEMA FAMILIES DETECTED
  Family A (HOBOlink atmospheric):        <n> files
  Family B (HOBOlink marine+atmospheric): <n> files
  Family C (Solinst water level):         <n> files
  Family D (ECCC hourly):                 <n> files
  UNKNOWN: <n> files

ANOMALIES
  <list each anomaly_type with count>

CONTAMINATION FLAGS
  <list each flagged file>

ACTION REQUIRED BEFORE PHASE 2
  1. Resolve <n> contamination candidate(s).
  2. Review <n> duplicate candidate(s).
  3. Run acquisition step for Stanhope (--acquire-stanhope flag).
  4. Review <n> UNKNOWN schema file(s) manually.
  5. Decide handling of Stanley Bridge 2022 Solinst files (currently excluded).
```

---

## Acceptance Criteria for `01_obtain.py`

The Obtain stage is considered complete and Phase 2 may begin when ALL of the following
are true:

1. `file_inventory.csv` exists and contains one row for every file under `data/raw/`
   (recursive walk).
2. `coverage_matrix.csv` contains an entry for every (station, year, month) combination
   from 2022-01 through 2025-12 for all six stations.
3. `contamination_flags.csv` has been reviewed by the analyst and flagged files have been
   either moved or explicitly excluded.
4. No file has `schema_family = UNKNOWN` without a corresponding anomaly log entry.
5. `obtain_summary.txt` exists and the "ACTION REQUIRED" section is empty (all items
   resolved) or every unresolved item has a documented deferral reason.
6. Stanhope download has either completed successfully OR the deferral is explicitly
   documented with a reason.

---

## Timestamp Validation Check (performed inside Obtain)

For every CSV file with schema family A or B, Obtain must sample the first 10 data rows
and verify:

- The Time column value matches the regex `\d{2}:\d{2}:\d{2} [+-]\d{4}`.
- The UTC offset token is in the set `{-0400, -0300}`.  Log any other offset value as
  an anomaly.
- Obtain does NOT parse or convert timestamps.  It only validates format.

For schema C (Solinst), Obtain logs the file as `SOLINST_FORMAT` and records that no
UTC offset was found.

---

## What Obtain Must NOT Do

- Must not write any transformed or cleaned data to `data/scrubbed/` data tables.
- Must not resample, merge, or join files.
- Must not calculate FWI or PCA inputs.
- Must not delete or rename any raw files.
