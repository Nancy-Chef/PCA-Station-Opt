# Phase 1 Artifact 2: Station Registry and File-Discovery Rules

## Purpose
This document is the authoritative registry of all Parks Canada and ECCC stations in
scope.  It defines canonical identifiers, accepted folder names, known filename prefix
variants, schema family assignments, year coverage, and all known anomalies that the
Obtain stage must handle.

---

## Station Registry Table

| `station_id` | `station_name` | Root Folder (under `data/raw/`) | Known Filename Prefixes | Schema Family | Years Present | FWI Station |
|---|---|---|---|---|---|---|
| `CAV` | Cavendish | `Cavendish/` | `PEINP_Cav_WeatherStn_` | A | 2022–2025 | yes |
| `GRE` | Greenwich | `Greenwich/` | `PEINP_GR_WeatherStn_` | A | 2022–2025 | yes |
| `NRW` | North Rustico Wharf | `North Rustico Wharf/` | `PEINP_NR_WeatherStn_` | B | 2023–2025 | no |
| `SBW` | Stanley Bridge Wharf | `Stanley Bridge Wharf/` | `PEINP_SB_WeatherStn_`, `PEINP_STB_WeatherStn_`, `Stanley Bridge_*` | B (2023+), C (2022) | 2022–2025 | no |
| `TRW` | Tracadie Wharf | `Tracadie Wharf/` | `PEINP_TR_WeatherStn_` | B | 2023–2025 | no |
| `STA` | Stanhope ECCC | `ECCC Stanhope Weather Station/` (metadata only; hourly data TBD) | `Stanhope_Hourly_*` or ECCC bulk CSV | D | 2022–2025 (download required) | benchmark |

---

## File-Discovery Rules

### Rule 1 — Directory Walk
Obtain must recursively walk each station root folder.  Only `.csv` files are loaded for
data ingestion.  `.xle`, `.R`, `.txt`, `.xlsx`, and other extensions are inventoried but
not parsed as data.

### Rule 2 — Station Identity Assignment
Station identity is determined **first by folder path**, not filename.  Example:
a file named `PEINP_TR_WeatherStn_Feb2025.csv` found inside
`North Rustico Wharf/2025/` is assigned to `NRW` (North Rustico) for folder-based routing
but is simultaneously flagged as a **cross-station contamination candidate** because its
filename prefix (`PEINP_TR_`) maps to `TRW` (Tracadie).

**Contamination rule:** When folder-derived station_id ≠ filename-derived station_id,
log the file in the anomaly inventory with status `CONTAMINATION_CANDIDATE` and do not
process it until a human resolves the conflict.

### Rule 3 — Filename Prefix-to-Station Mapping

| Filename Prefix | Mapped station_id |
|---|---|
| `PEINP_Cav_` | `CAV` |
| `PEINP_GR_` | `GRE` |
| `PEINP_NR_` | `NRW` |
| `PEINP_SB_` | `SBW` |
| `PEINP_STB_` | `SBW` (2024+ variant; same station) |
| `PEINP_TR_` | `TRW` |
| `Stanley Bridge_` | `SBW` (2022 Solinst format) |

### Rule 4 — Year Folder Extraction
Year is taken from the immediate parent folder name if it is a 4-digit integer (e.g.
`2023/`).  If a season subfolder exists (e.g. `Fall/`, `Spring/`, `Winter/`), the year is
taken from the grandparent folder.  Files in season subfolders that are not `.csv` (e.g.
`.xlsx`) are inventoried but excluded from data loading.

### Rule 5 — Month Extraction from Filename
Month is extracted from the filename stem using a canonical normalisation map.  The
following raw month tokens have been observed and must be normalised:

| Observed Token | Canonical Month | Notes |
|---|---|---|
| `Jan` | 01 | |
| `Feb` | 02 | |
| `Mar` | 03 | |
| `Apr` | 04 | |
| `April` | 04 | seen in Cavendish 2025 |
| `May` | 05 | |
| `Jun` | 06 | |
| `June` | 06 | seen in Cavendish 2025, Tracadie 2023 |
| `Jul` | 07 | |
| `July` | 07 | seen in Cavendish 2025, Tracadie 2023 |
| `Aug` | 08 | |
| `Sep` | 09 | |
| `Sept` | 09 | seen in Cavendish 2025, Stanley Bridge 2023 |
| `Oct` | 10 | |
| `Nov` | 11 | |
| `Dec` | 12 | |

### Rule 6 — Schema Family Classification
Schema family is assigned at load time by examining the first header row:

- Family A: header contains `S-THB` or `S-THC` in a Temperature column **and** an RH
  column **and** a Rain column. No marine sensors.
- Family B: header contains one or more of `Water Pressure`, `Water Level`,
  `Water Temperature`, `Barometric Pressure` alongside atmospheric sensors.
- Family C: header matches `Serial_number:` in the first 5 rows (Solinst logger).
- Family D (Stanhope): header contains `Temp (°C)` and `Rel Hum (%)` without HOBOlink
  sensor ID patterns.

### Rule 7 — Duplicate File Detection
Files are candidates for duplication if they share the same (station_id, year, month)
tuple.  The Obtain audit must list all such candidates and report their row counts for
manual review.

### Rule 8 — Truncated Filename Detection
Filenames that do not match the expected pattern
`PEINP_<prefix>_WeatherStn_<Month><YYYY>.csv` (after normalisation) must be logged with
status `FILENAME_ANOMALY`.

Known observed case: `PEINP_SB_WeatherStn_Jul202.csv` in `Stanley Bridge Wharf/2023/` —
the year token is truncated to `202` instead of `2023`.

---

## Station-Specific Anomalies

### CAV — Cavendish
- Station name embedded in HOBOlink header is spelled `"Cavenish Green Gables"` (missing
  `d`). This is a data-source artefact; assign canonical name `Cavendish` regardless.
- 5-minute recording interval (unlike 2-min at other stations).
- Timezone offset is `-0400` (AST) in winter files and `-0300` (ADT) in summer files.
  Both are expected; the offset is embedded per-row in the Time column.
- Both km/h and m/s wind columns present; m/s columns observed empty in Jan 2023.

### GRE — Greenwich
- 2-minute interval, but battery-only rows appear at 5-minute sub-intervals.  Battery
  rows have all sensor columns empty except `Battery`.
- Dec 2022 file has many rows where RH, Dew Point, and Temperature columns are empty
  (only wind and rain observed). Humidity sensor may not have been installed yet.
- Column order is not identical to Cavendish even though the schema family is A.
- Duplicated wind column names observed in Dec 2022 header.
- Two Temperature columns (`S-THC` and `S-TMB`) may both be present.

### NRW — North Rustico Wharf
- Contains an `xlsx` file in a season subfolder:
  `2023/Fall/Parks Canada UPEI Station Data April 1 to September 30 2023.xlsx`.
  This file must be inventoried but is excluded from automated CSV loading.
- Two wind-sensor IDs appear in some files (sensor replacement or dual installation).
- Timestamps observed with `-0300` offset (ADT); confirms summer deployment.
- No data in 2022 folder (folder exists but is empty or absent).
- Marine sensor columns dominate the header; atmospheric columns are sparser.

### SBW — Stanley Bridge Wharf
- **2022 only:** Solinst XLE logger format (schema C).
  - Files named `Stanley Bridge_2022-07-10_2022-10-02.csv` (date-range, not month-based).
  - Contains `LEVEL` (water level, m) and `TEMPERATURE` (°C) only.
  - Time format: `M/D/YYYY,HH:MM:SS am/pm` — no UTC offset, local time unknown.
  - These files contribute NO atmospheric variables and should be excluded from PCA and
    FWI processing.  Retain in inventory for completeness.
  - `.xle` versions of the same date ranges also exist; do not load.
- **2023:** Prefix `PEINP_SB_WeatherStn_`.
  Station name in header: `Stanley Bridge Harbour`.
  No RH column observed (only `Temperature S-TMB`).
  Filename anomaly: `PEINP_SB_WeatherStn_Jul202.csv` (truncated year).
  Month anomaly: `PEINP_SB_WeatherStn_Sept2023.csv` (Sept vs Sep).
- **2024+:** Prefix changed to `PEINP_STB_WeatherStn_`.  Same station, same schema B.
- RH is absent across all inspected SBW HOBOlink files.  Flag as a known data gap.

### TRW — Tracadie Wharf
- Month name inconsistencies in 2023: `July2023`, `June2023` (vs `Jul`, `Jun`).
- Tracadie file (`PEINP_TR_WeatherStn_Feb2025.csv`) found inside
  `North Rustico Wharf/2025/` — confirmed cross-station contamination.  Must be flagged
  as `CONTAMINATION_CANDIDATE` and not processed from that path.
- No data in 2022 folder (station not operational or no files deposited).

### STA — Stanhope ECCC
- The folder `ECCC Stanhope Weather Station/` contains only metadata and an R script.
  No hourly observations are present in the repository.
- Acquisition is required before this station can contribute to PCA or serve as benchmark.
  See `phase1_stanhope_pathway.md` for the approved acquisition plan.

---

## Missing-Month Detection Rules

After building the file inventory, Obtain must report the following expected coverage
gaps (some are structural, not data errors):

| station_id | Years with Known Gaps | Known Reason |
|---|---|---|
| `GRE` | 2023, 2024, 2025 | Folders exist but appear empty in workspace listing |
| `NRW` | 2022 | Station not yet deployed |
| `TRW` | 2022 | Station not yet deployed |
| `SBW` | 2022 (monthly HOBOlink) | Only Solinst logger files present |
| `STA` | 2022–2025 | No files downloaded yet |

For all other station-year combinations, Obtain must flag any month between January and
December not represented by at least one CSV file.
