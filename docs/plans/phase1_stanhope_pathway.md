# Phase 1 Artifact 3: Stanhope ECCC Acquisition Pathway

## Decision
**Approved approach: Python-native download via ECCC bulk data HTTP API.**

The repository R script (`ECCC_Weather data bulk upload_Rcode.R`) uses the `weathercan`
R package to download Stanhope data and writes it to an institutional network drive.
That workflow depends on R, a specific network path, and a package that is not in this
project's Python stack.  For reproducibility within this Python-only project, a
Python-native acquisition step is the approved path.

---

## Evidence Supporting This Decision

| Evidence Item | Source |
|---|---|
| ECCC station metadata confirms station name "STANHOPE", Climate Identifier `8300590` | `en_climate_hourly_metadata_PE_8300590.txt` |
| R script confirms ECCC station_id for `weathercan` download is `6545` | `ECCC_Weather data bulk upload_Rcode.R` |
| The bulk download URL pattern is publicly documented by ECCC | ECCC Climate Data website |
| No Stanhope hourly CSV files exist anywhere in the repository | Workspace inspection |
| Project contract explicitly forbids notebook workflows | `.github/copilot-instructions.md` |
| `requirements.txt` contains no R-bridge or subprocess wrapper | `requirements.txt` |

---

## ECCC Bulk Download URL Pattern

ECCC provides hourly data as CSV via a public GET endpoint.  The pattern confirmed from
ECCC Climate Data Online documentation:

```
https://climate.weather.gc.ca/climate_data/bulk_data_e.html
    ?format=csv
    &stationID=8300590
    &Year=YYYY
    &Month=MM
    &timeframe=1
    &submit=Download+Data
```

- `stationID=8300590` — ECCC Climate Identifier from metadata file
- `timeframe=1` — hourly interval
- One request per year-month combination
- Response is a UTF-8 CSV with a multi-row header block before the data rows

---

## Approved Implementation Approach for Phase 2

Phase 2 (`01_obtain.py`) must include an optional `--acquire-stanhope` mode that:

1. Iterates over year-month combinations from 2022-01 to 2025-12.
2. Issues an HTTP GET request per month using the URL pattern above.
3. Writes each response to
   `data/raw/ECCC Stanhope Weather Station/hourly/<YYYY>/Stanhope_Hourly_<YYYY>_<MM>.csv`.
4. Skips months already downloaded (idempotent).
5. Logs HTTP errors and retries once before marking a month as `DOWNLOAD_FAILED`.
6. Does NOT parse or transform the downloaded files.  Transformation is Scrub's
   responsibility.

**Dependency to add to `requirements.txt` in Phase 2:** `requests` (already a transitive
dependency of many packages but should be listed explicitly).

---

## Folder Structure for Downloaded Files

```
data/raw/ECCC Stanhope Weather Station/
    hourly/
        2022/
            Stanhope_Hourly_2022_01.csv
            Stanhope_Hourly_2022_02.csv
            ...
        2023/
            ...
        2024/
            ...
        2025/
            ...
    ECCC_Weather data bulk upload_Rcode.R   (kept as historical reference)
    en_climate_hourly_metadata_PE_8300590.txt
```

---

## ECCC Hourly CSV Structure

Based on the ECCC Climate Data Online format for hourly records, the downloaded CSV will
contain the following columns relevant to the canonical schema (full column set
contains additional fields that are mapped to auxiliary or discarded):

| ECCC Column Name | Unit | Canonical Mapping | Notes |
|---|---|---|---|
| `Date/Time (LST)` | string | `timestamp_local_raw` | LST = UTC-4 always |
| `Temp (°C)` | °C | `air_temp_c` | |
| `Rel Hum (%)` | % | `relative_humidity_pct` | |
| `Dew Point Temp (°C)` | °C | `dew_point_c` | |
| `Wind Dir (10s deg)` | 10° units | `wind_dir_deg` | Multiply by 10 |
| `Wind Spd (km/h)` | km/h | `wind_speed_kmh` | |
| `Wind Gust (km/h)` | km/h | `wind_gust_kmh` | Not always populated |
| `Stn Press (kPa)` | kPa | `barometric_pressure_kpa` | Auxiliary |
| `Total Rain (mm)` | mm | `precip_mm` | Use `Total Precip (mm)` if this absent |
| `Visibility (km)` | km | discarded | Not in canonical schema |
| `Weather` | categorical | auxiliary | Retain as annotation if desired |

**Quality flags:** ECCC uses flag columns (e.g. `Temp Flag`) with values `E` (estimated),
`M` (missing), `NA` (not available).  During Scrub, rows where temperature flag is `M`
should set `air_temp_c` to null.

---

## Timestamp Conversion Rule for Stanhope

ECCC LST for Prince Edward Island (Atlantic Standard Time) is UTC−4 year-round.
The metadata file states: *"If Local Standard Time (LST) was selected, add 1 hour to
adjust for Daylight Saving Time where and when it is observed."*

This means ECCC records do NOT shift the timestamp for DST.  The LST column is always
UTC−4, regardless of time of year.

**Scrub rule:** `timestamp_utc = parse(timestamp_local_raw) + timedelta(hours=4)`

This is different from HOBOlink files where the offset is embedded per-row and
automatically accounts for DST (e.g. `-0300` in summer, `-0400` in winter).

---

## Minimum Stanhope Fields Required for PCA Benchmark Join

The following canonical fields must be successfully populated from Stanhope to use it
as a benchmark in the five-station PCA:

- `timestamp_utc` (join key)
- `air_temp_c`
- `relative_humidity_pct`
- `dew_point_c`
- `wind_speed_kmh`
- `wind_dir_deg`
- `solar_wm2` — **NOTE**: ECCC hourly data does not include solar radiation.  This is an
  accepted limitation.  Stanhope will participate in PCA components that do not require
  solar radiation, and `solar_wm2` will be null for all Stanhope rows.
- `precip_mm`

---

## Deferred Decision

**Solar radiation gap at Stanhope:** The ECCC hourly product does not include solar
radiation.  The impact on PCA depends on how many components weight solar heavily.
This decision is deferred to the Explore stage, which will quantify whether Stanhope
can still serve as a meaningful benchmark without it, or whether solar must be excluded
from the inter-station comparison variables.

---

## Alternative (Rejected): Manual External File Placement

A simpler approach would be to instruct the user to manually download Stanhope files
and place them in a documented folder.  This was rejected because:

- It breaks the reproducibility requirement (no automated re-run path).
- The project contract requires OSEMN pipeline scripts, not manual steps.
- The ECCC bulk endpoint is stable and public, making automation low-risk.

The R-based pathway is retained in the repository as historical documentation but is
not part of the active pipeline.
