## Plan: Phase 2 Scrub and Normalize

Build phase 2 as a registry-driven scrub pipeline that reads only the 220 supported CSV files identified in phase 1, parses each file by parser family, converts timestamps to UTC using the per-file `tz_token` recorded in the registry, standardizes heterogeneous weather variables into one long-form schema, applies conservative gap-aware missing-data rules, and emits hourly and daily long-form products plus audit artifacts for later exploration, PCA benchmarking, and FWI work.

**Steps**
1. Confirm the phase-2 contract and input filter.
   - Read `data/scrubbed/phase1_registry.csv` and restrict processing to rows where `supported` is true and `parse_status` is success-ready.
   - Treat `.xlsx`, `.xle`, the Stanley Bridge 2022 metadata CSV, and the ECCC `.docx` reference file as explicitly excluded from phase 2 scope.
   - Use `phase1_summary.json` only as a cross-check for counts and deferred-file reporting, not as a primary processing input.

2. Define the phase-2 module layout. Depends on step 1.
   - Create one thin orchestration entrypoint in `src/` for the scrub phase.
   - Create one helper module in `src/` for parser-family readers, timestamp normalization, schema mapping, quality flags, imputation, resampling, and artifact writing.
   - Reuse phase-1 constants and metadata where practical instead of duplicating station code maps, month logic, or encoding fallback behavior.

3. Establish the canonical normalized schema. Depends on step 2.
   - Keep the primary scrubbed dataset in long form with one row per station, UTC timestamp, and standardized variable.
   - Define required identity and provenance fields: `station_code`, `station_name`, `parser_family`, `source_file`, `source_row_group` or ingest batch id, `timestamp_local_raw`, `timestamp_utc`, `variable_name_std`, `value`, `unit_std`, `quality_flag_source`, `quality_flag_scrub`, `imputation_flag`, `resample_level`.
   - Define optional raw-trace fields for debugging and reproducibility: `raw_column_name`, `tz_token`, `schema_variant`, `known_issue_tag`.
   - Separate the standardized variable dictionary into atmospheric variables used downstream across stations versus marine-dominant variables retained as auxiliary diagnostics.

4. Build parser-family readers around the established phase-1 schema and header findings. Depends on step 3.
   - `PEINP-HOBOlink` reader:
     - Read comma-delimited files with the same encoding fallback chain used in phase 1.
     - Strip surrounding whitespace from headers and collapse internal repeated spaces only where needed to match known Tracadie anomalies.
     - Preserve raw source headers before normalization.
     - Combine `Date` and `Time` into a timezone-aware local timestamp using the per-file `tz_token` from the registry.
   - `ECCC-LST` reader:
     - Read the single `Date/Time (LST)` field, relying on `skip_blank_lines` behavior for blank-row files.
     - Apply the fixed UTC-4 interpretation for LST exactly as documented in phase 1.
     - Preserve ECCC measurement flag columns for later scrub quality mapping.
   - Reject any file whose observed timestamp pattern does not match the parser family recorded in the registry and log it to a phase-2 audit artifact instead of silently coercing it.

5. Implement timestamp validation and UTC normalization. Depends on step 4.
   - Trust the per-file `tz_token` recorded in the phase-1 registry for PEINP files, including files with `-0300`.
   - Parse timestamps into a local timezone-aware column and derive `timestamp_utc` as the canonical processing time field.
   - Preserve the original `Date`, `Time`, and combined local timestamp strings for traceability.
   - Run per-file validation checks before continuing: monotonicity within file, duplicate timestamp counts, parse failure counts, and offset consistency against the registry metadata.
   - Send files with timestamp parse failures or severe duplication to an audit table; continue processing remaining files unless the failure rate breaches a planned threshold.

6. Design the variable-mapping layer from raw headers to standardized fields. Depends on steps 4 and 5.
   - Create an explicit mapping table or mapping functions keyed by parser family and header patterns.
   - For PEINP files, normalize sensor-labeled headers into stable variables such as `air_temperature_c`, `relative_humidity_pct`, `dew_point_c`, `wind_speed_kmh`, `wind_gust_kmh`, `precipitation_mm`, `solar_radiation_wm2`, `water_temperature_c`, `level_m`, `salinity_ppt`, `conductivity_us_cm`, and `pressure_kpa` or `pressure_hpa` as available.
   - Resolve known duplicates intentionally rather than by first-match behavior. Example: prefer one wind-speed unit consistently and convert secondary unit variants only if needed.
   - For Greenwich temperature duplicates, define one primary atmospheric air-temperature source for downstream analysis and retain the alternate sensor as an auxiliary variable or provenance note.
   - For ECCC files, map measurement columns and source flags into the same standardized variable vocabulary.
   - Record every unmapped raw column in a schema-audit artifact so the normalization step is transparent and extensible.

7. Harmonize units and attach scrub-level quality flags. Depends on step 6.
   - Convert units only where required to support one standard variable dictionary across families.
   - Preserve the original unit in provenance fields when conversion occurs.
   - Translate ECCC source flag columns and any obvious PEINP sentinel or impossible values into a common `quality_flag_scrub` vocabulary such as `native_ok`, `source_flagged`, `range_failed`, `duplicate_resolved`, `interpolated_short_gap`, `excluded_long_gap`.
   - Apply range checks for core atmospheric variables using `standards/data_handling.md` guidance and clearly document any chosen thresholds in the code and phase summary.

8. Apply conservative missing-data policy and long-gap handling. Depends on step 7.
   - Perform missingness handling after variable standardization and UTC alignment, not on the raw wide inputs.
   - Use a short-gap interpolation rule only for atmospheric variables that are continuous and appropriate for interpolation.
   - Recommended default policy:
     - Interpolate gaps up to 2 consecutive hourly steps after hourly regularization.
     - Do not interpolate precipitation totals or marine level measurements unless explicitly justified later.
     - Mark longer gaps as `excluded_long_gap` and keep them missing in the primary scrubbed products.
   - Compute station-variable completeness metrics before and after imputation and emit them as a dedicated quality artifact.
   - Do not drop whole stations in phase 2; instead, retain sparse series with completeness flags so phase 3 and phase 4 can decide fitness for use.

9. Regularize to hourly cadence before resampling outputs. Depends on step 8.
   - For each station and standardized variable, sort by `timestamp_utc` and regularize to an hourly index covering the observed station span.
   - Deduplicate coincident timestamps using an explicit policy by variable type, such as mean for continuous measures and max for gust-like measures, while logging affected groups.
   - Only after hourly regularization should short-gap interpolation be applied.
   - Store whether each hourly value is native, aggregated-from-subhourly, deduplicated, or interpolated.

10. Produce the main phase-2 outputs. Depends on step 9.
   - Emit one hourly long-form scrubbed dataset in `data/scrubbed/` as the primary exploration and benchmarking input.
   - Emit one daily long-form scrubbed dataset in `data/scrubbed/` as the base for later FWI preparation and daily diagnostics.
   - Emit at least two audit artifacts in `data/scrubbed/`: a schema-mapping or unmapped-column report and a data-quality or completeness report.
   - Keep output naming deterministic and phase-specific so reruns overwrite cleanly and downstream phases can target stable filenames.

11. Define daily aggregation rules explicitly for downstream FWI compatibility. Depends on step 10.
   - Generate daily products from the hourly UTC-normalized data, not directly from raw files.
   - For atmospheric variables, set variable-specific daily aggregation rules and document them clearly: mean for temperature and humidity where appropriate, sum for precipitation, max for gust-like variables, and a designated end-of-day or observation-hour selection where later FWI logic requires it.
   - Preserve station coverage beyond Cavendish and Greenwich in the daily artifact, while noting that only Cavendish and Greenwich feed the planned moisture-code workflow.
   - Do not calculate FFMC, DMC, or DC in phase 2.

12. Build validation and audit reporting into the pipeline. Depends on steps 4 through 11.
   - Compare processed file counts against the 220 supported-registry rows.
   - Validate row counts, null counts, duplicate timestamp counts, and coverage by station-variable before and after normalization.
   - Spot-check representative files from PEINP and ECCC families to confirm `timestamp_utc` values and mapped variables align with the phase-1 findings.
   - Produce a compact terminal summary plus persistent audit outputs so later phases do not need to rediscover schema issues.

13. Document phase completion. Depends on successful end-to-end dry run.
   - Write `docs/plans/phase2_summary.md` after implementation describing what was done, why the long-form design was chosen, how `tz_token` was applied, which variables were standardized, what interpolation thresholds were used, and which source formats remain deferred.
   - Ensure the summary includes any decisions made during implementation that affect phase 3 exploration or phase 4 modeling assumptions.

**Relevant Files**
- `docs/plans/doc.md` - Master scope and phase sequencing.
- `docs/plans/phase1_summary.md` - Verified phase-1 findings, supported-file counts, and timestamp anomalies.
- `docs/plans/phase1_implementation.md` - Earlier obtain-phase structure and artifact expectations.
- `data/scrubbed/phase1_registry.csv` - Authoritative per-file processing registry for phase 2.
- `data/scrubbed/phase1_summary.json` - Cross-check counts and deferred follow-up files.
- `src/01_obtain.py` - Orchestration and CLI pattern to mirror.
- `src/obtain_utils.py` - Reusable station metadata, parser-family, and encoding patterns.
- `standards/python.md` - Naming, modularity, and comment expectations.
- `standards/data_handling.md` - Data-cleaning, missing-data, normalization, and resampling guidance.
- `standards/git.md` - Version-control conventions.

**Verification**
1. Confirm the phase-2 input set equals the 220 supported rows from `phase1_registry.csv` and that excluded formats are reported, not silently ignored.
2. Validate timestamp parsing and UTC conversion on at least one representative file from Cavendish, Greenwich, Tracadie, and ECCC Stanhope, including preservation of source-local timestamp fields.
3. Compare pre- and post-standardization variable coverage by station and ensure every retained raw column is either mapped or explicitly reported as unmapped.
4. Compare row counts, null counts, and duplicate timestamp counts before and after hourly regularization and short-gap interpolation.
5. Confirm interpolation is never applied to disallowed variables such as precipitation totals unless the implementation deliberately documents an exception.
6. Inspect the hourly and daily long-form outputs to verify stable schema, deterministic sorting, and clear quality and imputation flags.
7. Run a Python syntax or compile check on the created scrub-phase source files.

**Decisions**
- Process only the 220 supported CSV files from phase 1 in phase 2.
- Trust the phase-1 registry `tz_token` per file for PEINP UTC conversion, including suspect `-0300` values; do not override with inferred Atlantic timezone rules in this phase.
- Use long-form hourly and daily scrubbed outputs as the base artifact design.
- Keep unsupported formats and the Stanley Bridge 2022 metadata case explicitly deferred and documented.
- Do not perform PCA, Stanhope benchmarking metrics, or FWI calculations in phase 2.

**Further Considerations**
1. If implementation reveals that Greenwich’s duplicate temperature sensors diverge materially, preserve both in the long-form output but mark only one as the preferred atmospheric source for downstream wide-table construction.
2. If hourly regularization exposes widespread duplicate timestamps within a parser family, add a dedicated duplicate-resolution audit artifact rather than burying that logic in the main output.
3. If phase 3 later needs wide analysis tables, derive them from the long-form scrubbed artifact instead of making phase 2 maintain two canonical schemas.