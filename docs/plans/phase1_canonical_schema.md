# Phase 1 Artifact 1: Canonical Variable Model

## Purpose
This document defines the single authoritative field set that all downstream scrub, PCA,
and FWI logic must use.  Every field below was verified against at least one real raw file
before being included.

---

## Core Analytical Fields

| Canonical Field | Unit | Raw Source Column Patterns | Analytical Use | Null Behaviour | Hourly Aggregation Rule |
|---|---|---|---|---|---|
| `timestamp_local_raw` | string | `Date` + `Time` columns (space-joined) | retained only | NOT NULL | keep first value in window |
| `timestamp_utc` | datetime (UTC) | Parsed from `Date` + `Time` + embedded offset (e.g. `-0400`, `-0300`) | both | NOT NULL (key) | — |
| `station_id` | string (code) | Assigned from station registry; never from file content | both | NOT NULL (key) | — |
| `station_name` | string | Assigned from station registry | retained | NOT NULL | — |
| `air_temp_c` | °C | `Temperature.*S-T[HM][BC].*` (prefer S-THC > S-THB > S-TMB) | both | allow null | mean |
| `relative_humidity_pct` | % | `RH \(S-T.*\)` | both | allow null | mean |
| `dew_point_c` | °C | `Dew Point.*` | PCA | allow null | mean |
| `precip_mm` | mm | `Rain \(S-RGB.*\),mm` (event-bucket rain gauge) | FWI | allow null | sum |
| `wind_speed_kmh` | km/h | `Average [Ww]ind [Ss]peed.*Km/h` (prefer km/h over m/s column) | both | allow null | mean |
| `wind_gust_kmh` | km/h | `Wind [Gg]ust.*[Ss]peed.*Km/h` | both | allow null | max |
| `wind_dir_deg` | ° | `Wind Direction.*°` | PCA | allow null | circular mean (resultant vector) |
| `solar_wm2` | W/m² | `Solar Radiation.*W/m²` | PCA | allow null | mean |
| `battery_v` | V | `Battery.*V` | retained | allow null | mean |
| `source_schema` | string (code) | Derived during Obtain; see schema family table below | retained | NOT NULL | first value in window |

---

## Auxiliary Marine Fields (retained, not in PCA/FWI core)

These columns appear in North Rustico, Stanley Bridge, and Tracadie files.  They must be
preserved in the scrubbed dataset under these canonical names but must NOT be included in
the shared PCA feature matrix or FWI input table.

| Canonical Field | Unit | Raw Source Column Patterns |
|---|---|---|
| `water_pressure_kpa` | kPa | `Water Pressure.*kPa` |
| `diff_pressure_kpa` | kPa | `Diff Pressure.*kPa` |
| `water_flow_ls` | l/s | `Water Flow.*l/s` |
| `water_level_m` | m | `Water Level.*meters` |
| `water_temp_c` | °C | `Water Temperature.*°C` |
| `barometric_pressure_kpa` | kPa | `Barometric Pressure.*kPa` |
| `accumulated_rain_mm` | mm | `Accumulated Rain.*mm` |

---

## Observed Source Wind Duplication Issue

All HOBOlink files export both Km/h and m/s wind columns from the same sensor.  Observed
pattern across Cavendish, Greenwich, North Rustico, Stanley Bridge, and Tracadie files:

- The `*Km/h` columns are consistently populated.
- The `m/s` columns (`Wind Speed (S-WCF…),m/s` and `Gust Speed (S-WCF…),m/s`) are
  systematically empty or sparse in all inspected files.

**Scrub rule (Phase 2):** Use the Km/h columns as the authoritative source for
`wind_speed_kmh` and `wind_gust_kmh`.  Drop the m/s duplicate columns after confirming
they are empty.  Log any file where m/s columns contain non-null values.

---

## Observed Duplicate Column Issue (Greenwich and Others)

The Greenwich Dec 2022 header contains duplicated column names for wind speed and gust
speed.  This is a HOBOlink export artefact when sensors are reconfigured.  During Obtain,
duplicated column names must be detected and flagged.  During Scrub (Phase 2), the first
non-null value among duplicates will be used.

---

## Temperature Sensor Priority

Multiple temperature sensors may appear in a single file:

| Sensor Code | Description | Priority |
|---|---|---|
| `S-THC` | Temperature/RH combo (most complete) | 1st choice |
| `S-THB` | Temperature/RH combo (older model) | 2nd choice |
| `S-TMB` | Temperature only (marine/bare sensor) | 3rd choice (use when no THC/THB) |

**Rule:** Prefer the sensor that also provides RH.  If a file has both S-THC and S-TMB,
use S-THC for `air_temp_c` and `relative_humidity_pct`.  Document the choice per station
and year in the scrub log.

---

## Schema Family Codes

| Code | Description | Observed Stations | Notes |
|---|---|---|---|
| `A` | HOBOlink RX3000 atmospheric | Cavendish, Greenwich | 5-min (Cav) or 2-min (GR); full temp+RH+wind+solar |
| `B` | HOBOlink RX3000 marine+atmospheric | North Rustico, Tracadie, Stanley Bridge 2023+ | 2-min; includes water sensors; some lack RH |
| `C` | Solinst XLE water-level logger | Stanley Bridge 2022 only | No UTC offset; 12-hr time; level+temperature only; NOT usable for PCA or FWI |
| `D` | ECCC hourly bulk download | Stanhope | LST timestamps (no daylight offset); separate column set |

---

## Stanley Bridge Atmospheric Coverage Warning

Inspected Stanley Bridge 2023 (`PEINP_SB_WeatherStn_Aug2023.csv`) shows:

- **No RH column** — only `Temperature (S-TMB)` for atmospheric sensing
- Wind direction and rain are present
- No dew point column
- Barometric and water sensors present

**Impact on PCA:** Stanley Bridge will contribute `air_temp_c`, `wind_speed_kmh`,
`wind_gust_kmh`, `wind_dir_deg`, `solar_wm2`, `precip_mm`, and `barometric_pressure_kpa`
but **cannot contribute `relative_humidity_pct` or `dew_point_c`**.  This limits its role
in PCA components that depend on humidity.  The Explore stage must quantify this gap
before finalising the five-station PCA variable selection.

---

## Stanhope ECCC Field Mapping

The ECCC hourly bulk CSV uses different column names.  Mapping to the canonical schema:

| ECCC Column | Canonical Field | Notes |
|---|---|---|
| `Temp (°C)` | `air_temp_c` | |
| `Rel Hum (%)` | `relative_humidity_pct` | |
| `Dew Point Temp (°C)` | `dew_point_c` | |
| `Total Rain (mm)` | `precip_mm` | Use `Total Precip` if rain not available separately |
| `Wind Spd (km/h)` | `wind_speed_kmh` | |
| `Wind Gust (km/h)` | `wind_gust_kmh` | not always present in hourly |
| `Wind Dir (10s deg)` | `wind_dir_deg` | multiply by 10 to get degrees |
| `Stn Press (kPa)` | `barometric_pressure_kpa` | auxiliary |
| `Date/Time (LST)` | `timestamp_local_raw` | LST = UTC-4 year-round; add 1 hr during DST |

**UTC conversion for Stanhope:** ECCC LST for PEI is UTC-4 (AST).  The metadata file
states: "If Local Standard Time (LST) was selected, add 1 hour to adjust for Daylight
Saving Time where and when it is observed."  The bulk download endpoint defaults to LST.
Conversion rule: `timestamp_utc = timestamp_local_raw + timedelta(hours=4)` for all rows
(AST is always UTC-4; ECCC records do not observe DST in the LST column).
