## Phase 2 Summary – Scrub and Normalize

### What was done

Phase 2 is complete.  The following source files were created:

- `src/02_scrub.py` – orchestration entry point; runnable from the command line
- `src/scrub_utils.py` – helper module covering parser-family readers, timestamp
  validation and UTC normalization, variable mapping, unit harmonization, quality
  flag assignment, hourly regularization, daily aggregation, and artifact writers

Running `python src/02_scrub.py` from the workspace root produces five artifacts
in `data/scrubbed/`:

| Artifact | Rows | Size |
|----------|------|------|
| `phase2_hourly.csv`       | 2 015 901 | 866 MB |
| `phase2_daily.csv`        |    84 100 |  36 MB |
| `phase2_schema_audit.csv` |     2 902 | 0.5 MB |
| `phase2_completeness.csv` |       255 | <1 MB  |
| `phase2_ts_audit.csv`     |         0 | —      |

---

### Registry filtering and deferred formats

Phase 2 reads `data/scrubbed/phase1_registry.csv` and processes only rows where
`supported = True` and `parser_family` is one of the two supported families.

| Category | Count |
|----------|-------|
| Files processed (PEINP-HOBOlink + ECCC-LST) | 220 |
| Files skipped (errors) | 0 |
| Deferred – `special-case-csv-metadata` (SBW 2022) | 1 |
| Deferred – `special-case-xle` (SBW 2022 .xle) | 4 |
| Deferred – `xlsx` (seasonal exports) | 6 |
| Deferred – `unknown` (.docx reference link) | 1 |

All 12 deferred files are explicitly logged in the terminal summary on every run;
none are silently ignored.

---

### Station time spans (UTC)

| Code | Station | Start (UTC) | End (UTC) |
|------|---------|-------------|-----------|
| CAV  | Cavendish | 2022-10-12 | 2025-11-01 |
| GRE  | Greenwich | 2022-10-25 | 2026-01-01 |
| NRW  | North Rustico Wharf | 2023-04-14 | 2026-01-01 |
| SBW  | Stanley Bridge Wharf | 2023-07-25 | 2026-01-01 |
| STA  | ECCC Stanhope | 2022-01-01 | 2026-01-01 |
| TRW  | Tracadie Wharf | 2023-06-29 | 2026-01-01 |

The longest common overlap across all six stations runs from approximately
**2023-07-25 to 2025-11-01**.  Phase 4 should use this window for the primary
PCA and benchmarking matrix, and use the longer station-specific histories for
sensitivity checks only.

---

### Canonical long-form schema

Every row in the hourly and daily CSVs represents one station, one UTC timestamp,
and one standardized variable.  Required output columns:

`station_code`, `station_name`, `parser_family`, `source_file`,
`timestamp_utc`, `variable_name_std`, `value`, `unit_std`,
`quality_flag_source`, `quality_flag_scrub`, `imputation_flag`, `resample_level`

Optional provenance columns retained for debugging: `raw_column_name`,
`tz_token`, `schema_variant`, `known_issue_tag`, `timestamp_local_raw`.

---

### Standardized variables produced

24 standardized variable names were mapped across both parser families:

**Atmospheric (all or most stations)**

| Variable | Unit |
|----------|------|
| `air_temperature_c`     | °C    |
| `dew_point_c`           | °C    |
| `relative_humidity_pct` | %     |
| `wind_speed_kmh`        | km/h  |
| `wind_gust_kmh`         | km/h  |
| `wind_speed_ms`         | m/s   |
| `wind_gust_ms`          | m/s   |
| `wind_direction_deg`    | °     |
| `wind_direction_10s_deg`| 10s°  |
| `solar_radiation_wm2`   | W/m²  |
| `precipitation_mm`      | mm    |
| `accumulated_rain_mm`   | mm    |
| `pressure_kpa`          | kPa   |
| `visibility_km`         | km    |
| `humidex`               | —     |
| `wind_chill`            | —     |
| `weather_desc`          | —     |

**Marine-dominant (wharf stations)**

| Variable | Unit |
|----------|------|
| `water_temperature_c` | °C   |
| `water_level_m`       | m    |
| `water_pressure_kpa`  | kPa  |
| `diff_pressure_kpa`   | kPa  |
| `water_flow_ls`       | l/s  |

**Auxiliary / diagnostic**

| Variable | Notes |
|----------|-------|
| `aux_air_temperature_c` | Greenwich S-TMB sensor, retained alongside primary S-THC |
| `battery_v`             | Logger battery voltage |

**Variable coverage per station (hourly)**

| Station | Distinct variables |
|---------|-------------------|
| CAV | 12 |
| GRE | 13 |
| NRW | 18 |
| SBW | 16 |
| STA | 11 |
| TRW | 15 |

---

### Timestamp handling and UTC normalization

**PEINP-HOBOlink:** The UTC offset is embedded as a suffix in the `Time` column
(e.g., `00:00:00 -0300`).  Phase 2 trusts the per-file `tz_token` recorded in the
phase-1 registry exactly as observed, including the `-0300` values for CAV and TRW.
This means CAV and TRW timestamps are treated as Atlantic Daylight Time rather than
Atlantic Standard Time for the affected files.  This is a known open question;
phase 3 or phase 4 should validate these offsets against an independent source if
the UTC alignment matters for benchmarking precision.

**ECCC-LST:** The `Date/Time (LST)` column contains no offset string.  A fixed
UTC-4 offset is applied (Atlantic Standard Time), as documented in phase 1 and
confirmed consistent across all 48 ECCC files.

Original `Date`, `Time`, and combined local-timestamp strings are preserved in the
`timestamp_local_raw` provenance column for every row.  The timestamp audit
artifact (`phase2_ts_audit.csv`) recorded zero errors and zero warnings across all
220 files.

---

### Variable mapping decisions

**PEINP sensor-ID headers:** Raw column names embed hardware sensor IDs and
location suffixes (e.g.,
`Temperature (S-THB 21114839:20824084-1),°C,Cavenish Green Gables`).  The mapping
layer strips sensor-ID parenthesized tokens and matches on the plain-English base
name plus a unit or sensor-family hint to resolve competing columns.

**Duplicate wind-speed columns:** PEINP files often carry both a km/h and an m/s
variant for wind speed and gust speed.  The km/h variant is mapped to the primary
standardized name (`wind_speed_kmh`, `wind_gust_kmh`); the m/s variant is mapped to
the secondary names (`wind_speed_ms`, `wind_gust_ms`) and retained rather than
dropped.

**Greenwich temperature duplicate:** Greenwich files contain both an S-THC family
sensor (primary atmospheric air temperature) and an S-TMB sensor.  The S-THC sensor
is mapped to `air_temperature_c` (priority 1); the S-TMB sensor is mapped to
`aux_air_temperature_c` (auxiliary).  Both are present in the output.  If the two
sensors diverge materially, phase 3 diagnostics will reveal this.

**Schema audit:** 2 858 columns were mapped and 44 were deliberately demoted to
auxiliary status (lower-priority duplicates).  Zero columns were silently ignored;
all unmapped columns are recorded in `phase2_schema_audit.csv`.

---

### Missing-data policy and quality flags

Gap-aware imputation was applied after UTC normalization and variable standardization,
at hourly resolution only.

| Rule | Threshold |
|------|-----------|
| Short-gap linear interpolation | ≤ 2 consecutive missing hourly steps |
| Long-gap exclusion | > 2 consecutive missing hourly steps |
| Variables never interpolated | `precipitation_mm`, `accumulated_rain_mm`, `water_level_m`, `water_pressure_kpa`, `diff_pressure_kpa`, `water_flow_ls` |

The `quality_flag_scrub` vocabulary used in both outputs:

| Flag | Meaning |
|------|---------|
| `native_ok` | Value present and passed all checks |
| `source_flagged` | ECCC source flag column was non-empty |
| `range_failed` | Value outside physical plausibility bounds (set to NaN) |
| `duplicate_resolved` | Aggregated from sub-hourly duplicates |
| `interpolated_short_gap` | Filled by linear interpolation (≤ 2 h gap) |
| `excluded_long_gap` | Gap too large; value remains NaN |

---

### Completeness summary (hourly, per-variable average by station)

| Station | Mean % complete | Mean interpolated / variable | Mean excluded-long-gap / variable |
|---------|----------------|------------------------------|-----------------------------------|
| CAV     | 78.7 %  |  0.1 | 4 740 |
| GRE     | 69.6 %  |  1.4 | 3 469 |
| NRW     | 81.4 %  |  0.7 | 4 425 |
| SBW     | 71.0 %  |  0.0 | 6 200 |
| STA     | 57.4 %  | 87.7 | 15 061 |
| TRW     | 78.7 %  |  0.3 | 2 346 |

**Note on STA completeness:** ECCC Stanhope's apparent 57 % average is driven by
derived or auxiliary variables (`humidex`, `wind_chill`, `weather_desc`,
`visibility_km`) that are sparsely populated in the raw ECCC exports.  Core
atmospheric variables (temperature, humidity, pressure, wind) have substantially
higher completeness.  Phase 3 should compute per-variable completeness for STA to
separate core from auxiliary before benchmarking.

No whole stations were dropped in phase 2.  All stations are retained with
completeness flags so phase 3 diagnostics and phase 4 modeling can determine
fitness for use.

---

### Daily aggregation rules

Daily products are derived from the hourly UTC-normalized data, not directly from
raw files.  Variable-specific rules applied:

| Rule | Variables |
|------|-----------|
| `mean` | `air_temperature_c`, `dew_point_c`, `relative_humidity_pct`, `wind_speed_kmh`, `wind_speed_ms`, `wind_direction_deg`, `wind_direction_10s_deg`, `solar_radiation_wm2`, `pressure_kpa`, `visibility_km`, `humidex`, `water_temperature_c`, `water_level_m`, `water_pressure_kpa`, `diff_pressure_kpa`, `water_flow_ls`, `battery_v`, `aux_air_temperature_c` |
| `sum` | `precipitation_mm` |
| `max` | `wind_gust_kmh`, `wind_gust_ms` |
| `max` (end-of-day cumulative) | `accumulated_rain_mm` |
| `min` | `wind_chill` |

FWI moisture-code calculations (FFMC, DMC, DC) are deferred to phase 4.  Both
Cavendish and Greenwich daily series are present in `phase2_daily.csv` and are
ready for that workflow.

---

### Key decisions and assumptions

1. **Registry-driven processing:** Phase 2 is fully driven by
   `data/scrubbed/phase1_registry.csv`.  No file paths are hard-coded.
2. **Trust the recorded `tz_token`:** PEINP UTC conversion uses the per-file
   `tz_token` as observed in phase 1, including the ambiguous `-0300` values for
   CAV and TRW files.  No inference based on calendar date or station location was
   applied to override these tokens.
3. **Long-form canonical schema:** The primary scrubbed artifacts are in long form
   (one row per station × timestamp × variable).  Wide analysis-ready tables should
   be derived from these artifacts in phase 3 or phase 4 rather than maintained in
   parallel.
4. **Conservative imputation:** Only ≤ 2-hour gaps in continuous atmospheric
   variables are interpolated.  All other missing data is flagged as
   `excluded_long_gap` and left as NaN.  No station or variable was dropped; all
   completeness information is preserved for downstream decision-making.
5. **PEINP sub-hourly data resampled to hourly via aggregation:** Raw PEINP files
   log at 10-minute or 15-minute intervals; the `resample("h")` step aggregates
   these to hourly using variable-appropriate methods (mean, max, or sum), which
   simultaneously eliminates sub-hourly duplicates.

---

### Verification performed

1. All 220 supported registry files were processed with 0 read errors and 0 skipped.
2. The 12 deferred files (xlsx, xle, metadata CSV, docx) were explicitly logged
   and excluded from processing.
3. Timestamp parsing produced zero audit errors and zero warnings across all files.
4. Total raw long-form rows before regularization: **49 284 467**.
5. Total hourly long-form rows after regularization: **2 015 901**.
6. Total daily long-form rows: **84 100**.
7. Schema audit confirmed 2 858 column-to-variable mappings with 44 deliberate
   auxiliary demotions and zero unmapped columns silently discarded.
8. Hourly and daily output schemas were inspected; all required columns are present
   and the `timestamp_utc` field is in ISO UTC format.
9. Python syntax checks passed on both `scrub_utils.py` and `02_scrub.py` before
   the pipeline run.

---

### Artifacts produced

| Artifact | Location | Description |
|----------|----------|-------------|
| Hourly scrubbed data | `data/scrubbed/phase2_hourly.csv` | 2 015 901 rows × 17 columns; long-form; hourly UTC |
| Daily scrubbed data | `data/scrubbed/phase2_daily.csv` | 84 100 rows × 17 columns; long-form; daily UTC |
| Schema mapping audit | `data/scrubbed/phase2_schema_audit.csv` | 2 902 rows; one row per column-mapping decision per source file |
| Completeness report | `data/scrubbed/phase2_completeness.csv` | 255 rows; raw / hourly / daily completeness per station-variable |
| Timestamp audit | `data/scrubbed/phase2_ts_audit.csv` | Empty (zero timestamp issues detected) |

---

### Open questions and next steps for Phase 3

- Verify whether the `-0300` UTC offset for CAV and TRW files is correct (ADT)
  or a logger misconfiguration.  Compare overlapping periods with STA as a sanity
  check during phase 3 diagnostics.
- Examine per-variable completeness for ECCC Stanhope to separate core atmospheric
  variables (expected high completeness) from auxiliary derived variables (expected
  sparse).
- Assess whether the Greenwich S-THC and S-TMB temperature sensors (`air_temperature_c`
  vs `aux_air_temperature_c`) diverge materially; if not, the auxiliary variable can
  be dropped in the phase 4 analysis matrix.
- Phase 3 (`03_explore.py`) should consume `phase2_hourly.csv` and
  `phase2_completeness.csv` as its primary inputs and produce distribution plots,
  completeness heatmaps, time-step regularity checks, and station-alignment
  diagnostics before any modeling begins.
- Phase 4 should derive wide analysis-ready tables from the long-form hourly
  artifact rather than asking phase 2 to maintain a separate schema.
