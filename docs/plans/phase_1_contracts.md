# Phase 1 Data Contracts

This document is the authoritative Phase 1 handoff package. It contains every decision, contract, and acceptance rule that the Obtain, Scrub, and Explore implementation stages must follow. Nothing in this document should be changed during implementation without a formal revision that records the reason and cascades the update to all downstream artifacts.

---

## 1. Phase 1 Charter

### Approved Scope

Build the analytical contract that governs the first three OSEMN stages. The goal is weather-only feature preparation for later PCA-style ordination readiness and Fire Weather Index readiness. True redundancy analysis is blocked pending a future response matrix. Final FWI calculation is explicitly out of scope for these three stages.

### Locked Decisions

| # | Decision | Value |
|---|---|---|
| 1 | Initial FWI-core candidate stations | Cavendish, Greenwich |
| 2 | Timezone normalization policy | Parse embedded UTC offset from Time field when present; use Atlantic Time (-0300 summer / -0400 winter) only for legacy files without explicit offsets |
| 3 | Canonical wind speed unit | m/s |
| 4 | Minimum sub-hourly observations for FWI-usable hour | 3 observations per required variable within the hour |
| 5 | Wind direction retention | Retained in the cleaned outputs for exploratory plotting; not transformed to vector components in Phase 1 |
| 6 | Stanley Bridge Wharf scope | Supplemental only; excluded from the FWI-ready atmospheric core |
| 7 | North Rustico Wharf and Tracadie Wharf scope | Provisional supplemental; upgrade to atmospheric core blocked until full-season profiling confirms all four FWI variables are consistently present |
| 8 | Imputation policy | No unsupported imputation in Phase 1; stations missing required FWI variables are excluded from the core scope rather than filled |

### Explicitly Deferred Decisions

- Final FWI calculation rules (deferred to a later modeling phase)
- True RDA using a response matrix (deferred until response data is available)
- Broad multi-gap interpolation strategy (deferred until Explore-stage profiling quantifies gap sizes)
- Whether North Rustico and Tracadie are promoted to atmospheric-core status (deferred until full-season column profiling in Phase 2)
- Whether wind direction will be transformed for ordination modeling (deferred until Explore confirms modeling approach)

---

## 2. Station Metadata Contract

Every implementation artifact that references a station must use the station code from this table. Parser assignment and FWI-core eligibility are authoritative here.

| station_code | station_name | station_family | parser_family | station_type | fwi_core_status | timezone_policy | notes |
|---|---|---|---|---|---|---|---|
| CAV | Cavendish | land_weather | hobolink_modern | weather_focused | atmospheric_core_candidate | embedded_offset | Has duplicate wind columns: m/s (canonical) and km/h (excluded via crosswalk). Both wind channels map to the same physical sensor (S-WCF 21107892); km/h columns dropped after validation. |
| GR | Greenwich | land_weather | hobolink_modern | weather_focused | atmospheric_core_candidate | embedded_offset | Has no m/s wind columns; km/h columns require unit conversion (÷ 3.6) to produce canonical wind_speed_mps and wind_gust_mps. RH column present but sparse — confirm temporal density during profiling. Irregular sub-hourly sampling (2–10+ min gaps). |
| NRW | North Rustico Wharf | wharf_mixed | hobolink_modern | mixed_marine | provisional_supplemental | embedded_offset (-0300) | Has RH (S-THC 21648581-2). Has extensive duplicate wind columns across two sensor IDs (21113174 and 21135217); canonical wind source to be determined during Phase 2 profiling by temporal completeness. Extensive marine sensors present (water pressure, water level below wharf, water level CGVD28, water flow, water temperature, accumulated rain, barometric pressure). Upgrade to atmospheric_core_candidate requires full-season confirmation of all four FWI variables. |
| TW | Tracadie Wharf | wharf_mixed | hobolink_modern | mixed_marine | provisional_supplemental | embedded_offset (-0300) | No RH column observed in any sampled file. Duplicate wind columns across two sensor IDs (21038454 and 21038470); one has m/s (21038470-1), one has km/h only (21038454-1). Two rain gauge columns (21038368-1 and 20698368-1) — primary gauge to be confirmed during profiling. Marine sensors present (water pressure, diff pressure, water temperature, water level, barometric pressure). FWI-core upgrade blocked until RH column is confirmed. |
| SBW | Stanley Bridge Wharf | wharf_legacy | hobolink_legacy | legacy_only | supplemental_only | atlantic_time_fallback | CSV file (Stanley Bridge_2022-07-10_2022-10-02.csv) contains only metadata preamble (serial number, project ID, location, unit declarations). Actual data is in XLE files (XML-based HOBOware format). Only records Level (m) and Temperature (°C). No wind, precipitation, or humidity. Coverage period July–November 2022 only. Cannot compute FWI. |

### Expected Weather Variables by Station

| station_code | temperature_c | relative_humidity_pct | wind_speed_mps | precipitation_mm | wind_gust_mps | wind_direction_deg | solar_radiation_wm2 | dew_point_c | barometric_pressure_kpa |
|---|---|---|---|---|---|---|---|---|---|
| CAV | yes | yes | yes (m/s direct) | yes | yes (m/s direct) | yes | yes | yes | no |
| GR | yes | yes (sparse) | yes (km/h → convert) | yes | yes (km/h → convert) | yes | yes | no | no |
| NRW | yes | yes | yes (km/h → convert, 2 sensors — confirm primary) | yes | yes (km/h → convert) | yes | yes | yes | yes (supplemental) |
| TW | yes | no | yes (m/s sensor 21038470-1; km/h sensor 21038454-1) | yes (2 gauges — confirm primary) | yes | yes | yes | no | yes (supplemental) |
| SBW | yes (XLE) | no | no | no | no | no | no | no | no |

---

## 3. Predictor Dataset Contract

Every row in the cleaned atmospheric dataset must contain these fields. The provenance and join-key fields are non-negotiable because the Explore stage and any future response-data join depend on them.

### Required Provenance and Join-Key Fields

| field_name | type | description | example |
|---|---|---|---|
| station_code | string | Short code from station metadata table | `"CAV"` |
| station_name | string | Full display name | `"Cavendish"` |
| station_family | string | Family from station metadata table | `"land_weather"` |
| station_type | string | Type from station metadata table | `"weather_focused"` |
| source_file | string | Relative path of the raw file that produced this row | `"data/raw/Cavendish/2023/PEINP_Cav_WeatherStn_Jan2023.csv"` |
| parser_type | string | Parser family that loaded the file | `"hobolink_modern"` |
| datetime_local_raw | string | Original raw timestamp string from the file, before any parsing | `"01/01/2023,00:00:00 -0400"` |
| datetime_utc | datetime | UTC-normalized timestamp after offset parsing and conversion | `2023-01-01 04:00:00 UTC` |
| year | int | Calendar year derived from datetime_utc | `2023` |
| month | int | Calendar month derived from datetime_utc (1–12) | `1` |
| season | string | Season derived from month: winter (Dec–Feb), spring (Mar–May), summer (Jun–Aug), fall (Sep–Nov) | `"winter"` |

### Reserved Atmospheric Fields

These are the field names the Scrub stage will produce. Fields marked optional may be null where sensors are absent.

| field_name | unit | required for FWI | optional | notes |
|---|---|---|---|---|
| temperature_c | °C | yes | no | |
| relative_humidity_pct | % | yes | no | |
| wind_speed_mps | m/s | yes | no | Converted from km/h where m/s not directly available |
| precipitation_mm | mm | yes | no | Interval sum, not cumulative |
| wind_gust_mps | m/s | no | yes | Converted from km/h where necessary |
| wind_direction_deg | ° | no | yes | Retained for exploratory plotting; not transformed in Phase 1 |
| solar_radiation_wm2 | W/m² | no | yes | |
| dew_point_c | °C | no | yes | Derived from sensor at Cavendish and North Rustico |
| barometric_pressure_kpa | kPa | no | yes | Supplemental; wharf stations only |
| battery_voltage_v | V | no | yes | Engineering signal; retained for data-quality audit only |

### Required Quality Fields

These fields are added during the Scrub stage resampling step. They must be present in the hourly cleaned output.

| field_name | type | description |
|---|---|---|
| obs_count_in_hour | int | Number of raw sub-hourly records that contributed to this hourly aggregation |
| fwi_usable | bool | True when all four required FWI variables have obs_count_in_hour >= 3 |
| quality_flag | string | `"good"` / `"sparse"` / `"missing_rh"` / `"missing_wind"` / `"missing_precip"` / `"marine_only"` |

---

## 4. Canonical Variable and Unit Policy

The crosswalk built in Phase 2 must map every raw header to the canonical name in this table. No raw header should reach the cleaned dataset without a crosswalk entry.

### Canonical Field Reference

| canonical_name | canonical_unit | fwi_required | destination | raw_header_pattern | conversion_rule |
|---|---|---|---|---|---|
| temperature_c | °C | yes | master_atmospheric | `Temperature (S-TH...` or `Temperature (S-TM...` | none — raw unit is already °C |
| relative_humidity_pct | % | yes | master_atmospheric | `RH (S-TH...` | none |
| wind_speed_mps | m/s | yes | master_atmospheric | `Wind Speed (S-WCF... m/s` — or `Average wind speed (S-WCF... Km/h` where m/s is absent | If source is km/h: `value / 3.6`; document conversion in crosswalk |
| precipitation_mm | mm | yes | master_atmospheric | `Rain (S-RGB...` | none — raw unit is already mm; strip cumulative column if both present |
| wind_gust_mps | m/s | no | master_atmospheric | `Gust Speed (S-WCF... m/s` or `Wind gust speed (S-WCF... Km/h` | If source is km/h: `value / 3.6` |
| wind_direction_deg | ° | no | master_atmospheric | `Wind Direction (S-WCF...` | none |
| solar_radiation_wm2 | W/m² | no | master_atmospheric | `Solar Radiation (S-LIB...` | none |
| dew_point_c | °C | no | master_atmospheric | `Dew Point (S-THB... or S-THC...` | none |
| barometric_pressure_kpa | kPa | no | supplemental | `Barometric Pressure (M-BP...` | none |
| battery_voltage_v | V | no | master_atmospheric | `Battery (RX3000 BATTERY...` | none |
| water_pressure_kpa | kPa | no | supplemental | `Water Pressure (M-WP...` | none |
| diff_pressure_kpa | kPa | no | supplemental | `Diff Pressure (M-DP...` | none |
| water_temperature_c | °C | no | supplemental | `Water Temperature (M-WT...` | none |
| water_level_m | m | no | supplemental | `Water Level (M-WL...` (primary: top-of-wharf datum) | none |
| water_level_cgvd28_m | m | no | supplemental | `Water Level ... CGVD28` | none |
| water_flow_ls | l/s | no | supplemental | `Water Flow (M-WF...` | none |
| accumulated_rain_mm | mm | no | supplemental | `Accumulated Rain (C-ACC-RAIN...` | none — distinct from interval rain |

### Duplicate-Column Resolution Rules

| station_code | situation | rule |
|---|---|---|
| CAV | Two wind speed columns for the same sensor (21107892-1): `Wind Speed (m/s)` and `Average wind speed (Km/h)` | Keep m/s column as canonical `wind_speed_mps`. Mark km/h column as EXCLUDED in crosswalk. |
| CAV | Two gust columns for the same sensor (21107892-2): `Gust Speed (m/s)` and `Wind gust speed (Km/h)` | Keep m/s column as canonical `wind_gust_mps`. Mark km/h column as EXCLUDED. |
| GR | Only km/h wind columns (sensor 21135207) — no m/s equivalent | Convert `Average wind speed (Km/h)` to `wind_speed_mps` using `value / 3.6`. Document conversion. |
| NRW | Two wind sensor IDs present (21113174 and 21135217), each with m/s and km/h variants | Phase 2 profiling task: compare temporal completeness of sensor 21113174 vs. 21135217 and designate the more complete m/s column as canonical. Assign other as EXCLUDED or supplemental. |
| TW | Two wind sensor IDs (21038454 — km/h only; 21038470 — has m/s) | Use sensor 21038470 m/s column as canonical `wind_speed_mps`. Mark 21038454 km/h as EXCLUDED. |
| TW | Two rain gauge columns (21038368-1 and 20698368-1) | Phase 2 profiling task: compare coverage and designate the primary gauge. Mark duplicate as excluded. |
| NRW | Two temperature sensors (S-THC 21648581-1 and S-TMB 21038284-1) | Use S-THC column (same sensor as RH) as canonical `temperature_c`. Mark S-TMB as supplemental temperature for audit. |

---

## 5. Timestamp and Parser Policy

### Parser Family Definitions

| parser_family | applies_to | file_clues |
|---|---|---|
| hobolink_modern | CAV, GR, NRW, TW | First row is a comma-separated header line with verbose sensor-ID column names. Time field contains embedded UTC offset (e.g., `00:00:00 -0400`). |
| hobolink_legacy | SBW | First rows are plain metadata lines (serial number, project ID, location, unit headers). Data is in XLE files (XML). CSV file is metadata only. |

### Timestamp Rules — hobolink_modern

1. The `Date` field holds the date in `MM/DD/YYYY` format.
2. The `Time` field holds the time in `HH:MM:SS OFFSET` format where OFFSET is a signed four-digit UTC offset (e.g., `-0400`).
3. Combine `Date` and `Time` as a single string before parsing: `"01/01/2023 00:00:00 -0400"`.
4. Parse using a timezone-aware method (e.g., `pd.to_datetime(..., utc=True)` after stripping and appending the offset string as a UTC offset).
5. Preserve the raw combined string in the `datetime_local_raw` field before conversion.
6. Convert to UTC and store in `datetime_utc`.
7. The embedded offset is authoritative. Do not override it with a seasonal assumption.

### Timestamp Rules — hobolink_legacy (Stanley Bridge)

1. The CSV file is not a data file. Skip it for atmospheric ingestion. Log it in the audit report as format = metadata_only.
2. Actual data is in XLE files. XLE is the HOBOware XML export format. Parse the XML structure to extract the data log section.
3. Stanley Bridge XLE timestamps use 12-hour format (`HH:MM:SS am/pm`) with no explicit UTC offset in the data.
4. Assume Atlantic Time for all Stanley Bridge records. Apply seasonal offset: UTC-4 (ADT, summer) for records before the first Sunday in November; UTC-5 (AST, winter) for records on or after that date.
5. Preserve the raw timestamp string in `datetime_local_raw` before UTC conversion.
6. Store the converted UTC result in `datetime_utc`.

### Audit Columns Required for Every Loaded Row

| column_name | purpose |
|---|---|
| datetime_local_raw | Reproducibility audit; confirms original file content before any transformation |
| source_file | Provenance; allows per-file debugging |
| parser_type | Identifies which parser branch produced the row |

### Edge Cases

| scenario | handling rule |
|---|---|
| DST transition hour — clock moves back, creating an ambiguous local hour | The embedded offset resolves this; both hours will carry distinct offsets (-0300 vs. -0400) and map to distinct UTC timestamps. No special handling needed for modern files. |
| Duplicate UTC timestamps within a station after UTC conversion | Log the duplicates in the Obtain-stage audit report. Do not silently drop either record. Flag them with quality_flag = "duplicate_utc". |
| Gaps longer than 2 hours within a station-year file | Log in the audit report. Do not fill gaps at the Obtain stage. |
| Missing Date or Time field in a row | Drop that row and log a count in the per-file audit. |

---

## 6. FWI Input Contract and Fallback Rules

### Required Hourly Variables

| canonical_name | aggregation rule | FWI role |
|---|---|---|
| temperature_c | mean of sub-hourly observations in the hour | Dry bulb temperature input |
| relative_humidity_pct | mean of sub-hourly observations in the hour | Fine Fuel Moisture Code input |
| wind_speed_mps | mean of sub-hourly observations in the hour | Initial Spread Index input |
| precipitation_mm | sum of sub-hourly observations in the hour | Duff Moisture Code input |

### Hourly Usability Rule

An hourly row is marked `fwi_usable = True` only when all four required variables each have `obs_count_in_hour >= 3`. If any required variable falls below this threshold the row is marked `fwi_usable = False` and receives an appropriate `quality_flag`.

### Fallback Rules for Missing Variables

| scenario | fallback | rationale |
|---|---|---|
| Station permanently lacks a required variable (e.g., Tracadie has no RH) | Exclude station from FWI-ready atmospheric core. Retain all available data in supplemental output. | Imputation across an entirely absent sensor channel is not scientifically defensible. |
| Station has a required variable but below the hourly coverage threshold in a specific hour | Set fwi_usable = False for that hour. Do not impute. | Flags low-confidence records without discarding them; FWI calculation step can apply a stricter threshold if needed. |
| Wind speed available only as km/h column (Greenwich) | Convert using `value / 3.6` and document in crosswalk. Use converted m/s value as `wind_speed_mps`. | Unit conversion is lossless and deterministic; it is not imputation. |
| Precipitation column shows a gap (NaN) within an otherwise valid hour | Treat the gap as unknown. Do not substitute zero. Mark the hour `fwi_usable = False` if gap contributes to obs_count < 3. | Zero precipitation is a specific meteorological state, not a safe fill for missing data. |
| Two rain gauges present at one station (Tracadie) | Use primary gauge determined by Phase 2 profiling. Document the secondary gauge in the Obtain-stage audit report. | Avoids double-counting accumulation. |

### FWI Scope by Station

| station_code | fwi_core_eligible | reason |
|---|---|---|
| CAV | yes | All four required variables confirmed present |
| GR | yes | All four required variables present; wind_speed_mps requires km/h conversion |
| NRW | not yet confirmed | RH confirmed present in May 2023 but full-season coverage not yet profiled |
| TW | no | RH absent in all sampled files; FWI-core blocked |
| SBW | no | Wind, RH, and precipitation all absent; legacy format |

### Optional Variables for Exploratory Work

These are retained in the cleaned dataset for Explore-stage analysis but are not required for the hourly FWI calculation.

- `wind_gust_mps`
- `wind_direction_deg`
- `solar_radiation_wm2`
- `dew_point_c`
- `battery_voltage_v`

---

## 7. Master-versus-Supplemental Output Contract

### Output Layers

| layer | filename pattern | contents | row unit | stations included |
|---|---|---|---|---|
| Master atmospheric | `data/scrubbed/master_atmospheric_hourly.csv` | All canonical atmospheric fields plus all quality and provenance fields; one row per station-hour | one row per station-hour | CAV and GR initially; NRW and TW promoted if profiling confirms FWI readiness |
| Station-level atmospheric | `data/scrubbed/{station_code}_atmospheric_hourly.csv` | Same schema as master filtered to one station | one row per station-hour | One file per station included in master |
| Supplemental marine | `data/scrubbed/{station_code}_supplemental.csv` | Marine and wharf variables (water pressure, water level, water temperature, water flow, accumulated rain, barometric pressure) joined to station-hour key | one row per station-hour where marine variables exist | NRW, TW, SBW (where data is available) |

### Master Atmospheric Table Schema

| column | type | notes |
|---|---|---|
| station_code | string | From station metadata |
| station_name | string | From station metadata |
| station_family | string | From station metadata |
| station_type | string | From station metadata |
| source_file | string | Relative path to raw source |
| parser_type | string | Parser family |
| datetime_local_raw | string | Original raw timestamp before parsing |
| datetime_utc | datetime | UTC-normalized hourly timestamp (floored to hour after resampling) |
| year | int | |
| month | int | 1–12 |
| season | string | winter / spring / summer / fall |
| temperature_c | float | Mean of sub-hourly observations |
| relative_humidity_pct | float | Mean of sub-hourly observations |
| wind_speed_mps | float | Mean of sub-hourly observations |
| wind_gust_mps | float | Mean of sub-hourly observations; null if absent |
| wind_direction_deg | float | Mean of sub-hourly observations; null if absent |
| precipitation_mm | float | Sum of sub-hourly observations |
| solar_radiation_wm2 | float | Mean; null if absent |
| dew_point_c | float | Mean; null if absent |
| battery_voltage_v | float | Mean; null if absent |
| obs_count_in_hour | int | Count of raw rows that contributed to this hourly row |
| fwi_usable | bool | True when all four required variables have obs_count >= 3 |
| quality_flag | string | See FWI Input Contract section |

### Supplemental Table Schema

| column | type | notes |
|---|---|---|
| station_code | string | Join key back to master table |
| datetime_utc | datetime | UTC hourly timestamp — join key |
| barometric_pressure_kpa | float | Mean of sub-hourly observations; null if absent |
| water_pressure_kpa | float | Mean; null if absent |
| diff_pressure_kpa | float | Mean; null if absent |
| water_temperature_c | float | Mean; null if absent |
| water_level_m | float | Mean (top-of-wharf datum); null if absent |
| water_level_cgvd28_m | float | Mean (CGVD28 datum); null if absent |
| water_flow_ls | float | Mean; null if absent |
| accumulated_rain_mm | float | Last observation in hour (cumulative counter); null if absent |

### Variable Assignment to Tables

| raw header category | destination |
|---|---|
| Temperature, RH, Dew Point | master_atmospheric |
| Wind Speed, Wind Gust, Wind Direction | master_atmospheric |
| Rain (interval) | master_atmospheric |
| Solar Radiation | master_atmospheric |
| Battery Voltage | master_atmospheric |
| Water Pressure, Diff Pressure | supplemental |
| Water Temperature | supplemental |
| Water Level (any datum) | supplemental |
| Water Flow | supplemental |
| Accumulated Rain (cumulative counter) | supplemental |
| Barometric Pressure | supplemental |
| Quality flags (obs_count, fwi_usable, quality_flag) | master_atmospheric |
| Duplicate km/h wind columns after m/s canonical selected | EXCLUDED — logged in crosswalk only |

---

## 8. Phase 1 Verification Checklist

Use this checklist to confirm Phase 1 is complete and implementation can begin without reopening contract decisions.

### Section A: Charter and Scope

- [ ] The charter records the four locked planning decisions: station scope, timezone policy, canonical wind unit, and hourly coverage threshold.
- [ ] The deferred-decisions list explains why each item is deferred and what would trigger it being resolved.

### Section B: Station Metadata

- [ ] Every station (CAV, GR, NRW, TW, SBW) has exactly one entry in the station metadata table.
- [ ] Every station has one parser_family assignment.
- [ ] Every station has an unambiguous fwi_core_status: `atmospheric_core_candidate`, `provisional_supplemental`, or `supplemental_only`.
- [ ] Notes for GR document the km/h-only wind situation and state the conversion rule.
- [ ] Notes for NRW acknowledge the dual-sensor-ID situation and state that primary sensor selection is a Phase 2 profiling task.
- [ ] Notes for SBW state the XLE-format and metadata-only CSV finding.

### Section C: Predictor Dataset Contract

- [ ] All four FWI-required variables are named in the contract schema.
- [ ] Every provenance and join-key field (station_code, source_file, datetime_local_raw, datetime_utc, year, month, season) is present in the contract.
- [ ] All three quality fields (obs_count_in_hour, fwi_usable, quality_flag) are defined.

### Section D: Canonical Variable and Unit Policy

- [ ] Every variable that appears in the expected-weather-variables rows of the station metadata table has a canonical_name entry in the policy.
- [ ] Every canonical_name has exactly one canonical_unit.
- [ ] The duplicate-column resolution rules cover CAV, GR, NRW, and TW.
- [ ] The rule for converting km/h to m/s states the formula (`value / 3.6`).

### Section E: Timestamp and Parser Policy

- [ ] Parser rules cover both hobolink_modern and hobolink_legacy families.
- [ ] The rule for combining Date and Time fields is explicit.
- [ ] The rule for UTC conversion from the embedded offset is explicit.
- [ ] The Stanley Bridge 12-hour format and Atlantic Time fallback assumption are documented.
- [ ] The audit column list includes datetime_local_raw, source_file, and parser_type.
- [ ] Edge cases cover DST transition, duplicate UTC timestamps, and missing Date/Time fields.

### Section F: FWI Input Contract

- [ ] All four required hourly variables are listed with their aggregation rules.
- [ ] The hourly usability rule states `obs_count_in_hour >= 3` for each required variable.
- [ ] The fallback for permanently absent variables (e.g., Tracadie no RH) is documented as a scope exclusion rather than imputation.
- [ ] The fallback for km/h wind is documented as a unit conversion rather than imputation.
- [ ] The FWI scope table shows which stations are eligible, which are blocked, and why.

### Section G: Master-versus-Supplemental Assignment

- [ ] The master atmospheric table schema is fully specified.
- [ ] The supplemental table schema is fully specified.
- [ ] Every variable category from the raw data has an explicit destination: master, supplemental, or excluded.
- [ ] Duplicate wind columns are explicitly listed as excluded after canonical selection.

---

## 9. Implementation Handoff Note

Phase 1 is complete when all checklist items in Section 8 are confirmed. The following work can start immediately after Phase 1 sign-off.

### Phase 2 Obtain Stage — Ready to Start

1. **Raw file manifest**: Inventory every file under `data/raw/` using the station metadata table to assign station_code, station_family, and parser_family automatically. Record station, year, filename, file size, row count, date range, parser assignment, and anomaly notes per file.

2. **Column crosswalk construction**: For each unique raw header found across all files, map it to a canonical_name using the canonical variable and unit policy. Record destination (master, supplemental, or excluded) and any conversion rule.

3. **Parser branch design**: Write two parser branches using the timestamp policy as the implementation specification. The hobolink_modern branch handles CAV, GR, NRW, TW. The hobolink_legacy branch handles SBW XLE files.

4. **Timestamp validation on representative files**: Run the timestamp policy rules on at least one file from each station and confirm that UTC timestamps are strictly monotonic within each station-year grouping.

5. **Parallel profiling tasks**: Build the manifest and profile raw columns in parallel since neither depends on the other. Finalize the complete crosswalk before any cleaning logic is written.

### Still Out of Scope After Phase 1

| task | why deferred |
|---|---|
| Final FWI calculation | Requires Phase 3 scrubbed hourly outputs and daily noon-observation selection |
| Response-matrix joining for true RDA | No response dataset exists yet |
| Full imputation strategy | Deferred until Explore quantifies gap sizes and FWI specialist guidance reviewed |
| North Rustico and Tracadie promotion to atmospheric core | Requires full-season column profiling in Phase 2 to confirm FWI-required variable availability |
| Wind direction vector transformation | Deferred until Explore confirms whether modeling or only visualization is needed |

### Files to Create in Phase 2

| artifact | target path |
|---|---|
| Raw file manifest | `data/scrubbed/manifest.csv` |
| Column crosswalk | `data/scrubbed/column_crosswalk.csv` |
| Obtain script | `src/01_obtain.py` |
| Obtain audit report | `data/scrubbed/obtain_audit_report.md` |

---

*Phase 1 completed: 2026-03-25. Contracts grounded in raw header sampling from Cavendish Jan 2023, Greenwich Jan 2023, North Rustico Wharf May 2023, Tracadie Wharf Aug 2023, and Stanley Bridge Wharf 2022 CSV/XLE file inventory.*
