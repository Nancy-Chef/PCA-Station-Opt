## Plan: Phase 3 Exploration Diagnostics

Use the Phase 2 scrubbed artifacts as the sole analytical input layer for a diagnostic-only exploration phase that quantifies station-variable fitness for Phase 4. The recommended approach is to make hourly diagnostics the primary path, use daily diagnostics as a secondary validation path for FWI-relevant series, and emit a small set of reproducible tables and figures that answer three questions clearly: which variables are usable across stations, where missingness or quality flags make comparisons unsafe, and what caveats Phase 4 must carry forward.

**Steps**
1. Phase 3 inputs and scope confirmation.
   - Read `data/scrubbed/phase2_hourly.csv` as the primary exploration dataset and `data/scrubbed/phase2_daily.csv` as the secondary daily-check dataset.
   - Reuse `data/scrubbed/phase2_completeness.csv`, `data/scrubbed/phase2_schema_audit.csv`, and `data/scrubbed/phase2_ts_audit.csv` as already-computed audit baselines instead of recomputing their logic.
   - Keep Phase 3 strictly diagnostic: no PCA fitting, no Stanhope benchmark metrics, no FWI moisture-code calculation, and no new feature engineering beyond summary diagnostics.
   - Primary analytical window should be the confirmed common-overlap period `2023-07-25` to `2025-11-01`, with station-full-history sensitivity outputs kept separate.

2. Build the Phase 3 module structure. Depends on step 1.
   - Implement a thin orchestration entry point in `src/03_explore.py` following the same CLI and orchestration pattern used in `src/02_scrub.py`.
   - Implement reusable plotting and summary helpers in `src/explore_utils.py` rather than placing analysis logic directly in the entry script.
   - Reuse Phase 2 schema and flag vocabulary directly from `src/scrub_utils.py` rather than redefining variable lists, quality flags, or aggregation assumptions.

3. Validate the Phase 2 handoff contract before analysis. Depends on step 2.
   - Check that the expected long-form columns are present in both scrubbed datasets: `station_code`, `station_name`, `parser_family`, `source_file`, `timestamp_utc`, `variable_name_std`, `value`, `unit_std`, `quality_flag_source`, `quality_flag_scrub`, `imputation_flag`, `resample_level`, plus available provenance fields such as `timestamp_local_raw`, `raw_column_name`, `tz_token`, `schema_variant`, and `known_issue_tag`.
   - Confirm that `phase2_ts_audit.csv` is empty and treat any future non-empty rows as a hard warning in the Phase 3 summary.
   - Verify that quality flags match the existing Phase 2 vocabulary from `src/scrub_utils.py`: `native_ok`, `source_flagged`, `range_failed`, `duplicate_resolved`, `interpolated_short_gap`, `excluded_long_gap`.
   - Produce a compact ingestion summary that records row counts, distinct stations, distinct variables, station date ranges, and the common-overlap subset size.

4. Produce tabular diagnostics that answer Phase 4 readiness. Depends on step 3.
   - Generate a Phase 3 station-variable fitness table from hourly data with one row per `station_code` x `variable_name_std` containing at minimum: total observations, valid observations, percent completeness, percent flagged, percent interpolated, percent long-gap excluded, first timestamp, last timestamp, longest consecutive valid run, longest consecutive missing run, and whether the series exists in the common-overlap window.
   - Generate a common-overlap-only fitness table using the primary comparison window so Phase 4 does not rely on inflated full-history coverage.
   - Generate a station-level summary table with variable counts, median completeness across core atmospheric variables, median completeness across marine variables, and a recommended status such as `primary`, `secondary`, or `diagnostic-only`.
   - Generate a variable-level summary table showing station coverage counts, shared-availability across the five primary stations, and preliminary inclusion status for the common reduced feature set.
   - Generate a quality-flag distribution table from hourly data, broken down by station and variable, so Phase 4 can distinguish native coverage from interpolated or excluded coverage.
   - Generate a daily-series diagnostic table for Cavendish and Greenwich focused on `air_temperature_c`, `relative_humidity_pct`, `wind_speed_kmh` or the retained wind equivalent, and `precipitation_mm` to confirm daily FWI-relevant coverage without calculating moisture codes.

5. Produce missingness and cadence diagnostics. Depends on step 4. Parallel with step 6.
   - Build completeness heatmaps for `station_code` x `variable_name_std`, with separate panels for full-history and common-overlap coverage.
   - Build station availability timelines showing when each station-variable pair is active across the study period.
   - Build gap-structure summaries that quantify gap frequency and gap-length distributions, with explicit emphasis on long gaps that remained excluded after Phase 2.
   - Build cadence checks that confirm each hourly series is regular after scrubbing and identify any residual off-grid timestamps or duplicated `(station_code, variable_name_std, timestamp_utc)` rows.
   - Include a targeted timestamp spot-check output for CAV and TRW using `timestamp_local_raw`, `tz_token`, and `timestamp_utc` to document the inherited `-0300` offset issue as a caution for Phase 4 benchmarking.

6. Produce distribution and sensor-consistency diagnostics. Depends on step 4. Parallel with step 5.
   - Generate station-by-variable distribution plots for the core atmospheric variables expected to matter in Phase 4, using quality-filtered hourly values and separate common-overlap views where practical.
   - Generate outlier summary tables using simple robust diagnostics already supported by the stack, such as IQR or quantile-based extremes, and report counts rather than removing data in Phase 3.
   - Compare Greenwich `air_temperature_c` against `aux_air_temperature_c` over overlapping timestamps to quantify sensor agreement, drift, and whether the auxiliary series should remain diagnostic-only.
   - Compare duplicated wind-unit variables where both `wind_speed_kmh` and `wind_speed_ms` or gust equivalents exist to confirm whether the secondary unit columns carry independent signal or can be ignored in Phase 4 selection.
   - Summarize station-specific variable distributions that make a variable unsuitable for cross-station modeling, such as near-zero variance, persistent sparsity, or non-overlapping temporal support.

7. Produce explicit Phase 4 handoff recommendations. Depends on steps 5 and 6.
   - Classify each variable into one of three buckets: `candidate-core`, `candidate-secondary`, or `exclude-from-primary` based on shared station coverage, common-overlap completeness, flag burden, and interpretability.
   - Classify each station-variable pair into `usable`, `usable-with-caveat`, or `not-recommended` with the reason attached.
   - Recommend the exact subset of variables that appear viable for the common reduced feature set, but stop short of reshaping the matrix or fitting PCA.
   - Record unresolved cautions that Phase 4 must respect, including CAV and TRW offset validation, Greenwich dual-temperature drift if observed, sparse marine-variable handling, and any Stanhope auxiliary-variable sparsity.

8. Write stable Phase 3 artifacts. Depends on steps 4 through 7.
   - Write summary CSVs to `data/scrubbed/` with deterministic names such as `phase3_station_variable_fitness.csv`, `phase3_station_summary.csv`, `phase3_variable_summary.csv`, `phase3_quality_flags.csv`, and `phase3_daily_fwi_readiness.csv`.
   - Write figures to `outputs/figures/` with phase-specific names for completeness heatmaps, availability timelines, gap diagnostics, atmospheric distribution plots, and Greenwich sensor-comparison plots.
   - Keep artifact naming phase-prefixed and rerunnable so later phases can consume fixed filenames without manual renaming.

9. Document the phase completion and decision trail. Depends on step 8.
   - Write `docs/plans/phase3_summary.md` summarizing what diagnostics were produced, what they showed, why certain station-variable pairs were recommended or rejected, and which caveats remain open for Phase 4.
   - Ensure the summary explicitly distinguishes between conclusions derived from full-history data and conclusions derived from the common-overlap window.
   - Include a short section listing deliberate exclusions from Phase 3 scope so Phase 4 inherits a clean boundary.

**Relevant Files**
- `docs/plans/doc.md` - Governing scope and Phase 3 boundary.
- `docs/plans/phase2_summary.md` - Verified Phase 2 handoff facts and caveats.
- `docs/plans/phase2_implementation.md` - Implementation assumptions worth preserving.
- `src/03_explore.py` - New orchestration entry point for Phase 3.
- `src/explore_utils.py` - New helper module for summaries, plots, and diagnostics.
- `src/02_scrub.py` - Reuse the CLI and orchestration structure and file-loading pattern.
- `src/scrub_utils.py` - Reuse the canonical variable names, quality flags, `MAX_INTERP_GAP_HOURS`, `NO_INTERP_VARS`, `DAILY_AGG_RULES`, and writer conventions; relevant functions include `regularize_to_hourly`, `aggregate_to_daily`, and `build_completeness_report`.
- `data/scrubbed/phase2_hourly.csv` - Primary Phase 3 input.
- `data/scrubbed/phase2_daily.csv` - Secondary daily diagnostics input.
- `data/scrubbed/phase2_completeness.csv` - Reuse for validation and raw-vs-scrub comparison.
- `data/scrubbed/phase2_schema_audit.csv` - Reuse for variable provenance and duplicate-column context.
- `data/scrubbed/phase2_ts_audit.csv` - Timestamp-validation baseline.
- `outputs/figures` - Target location for Phase 3 figures.
- `standards/python.md` - Style, modularity, and reproducibility guidance.
- `standards/data_handling.md` - Plotting and analysis-tool guidance.

**Verification**
1. Confirm that both Phase 2 datasets load with the expected columns and that station and variable counts match the Phase 2 summary.
2. Reconcile full-history completeness derived from `phase2_hourly.csv` with `phase2_completeness.csv` for a spot-check sample of stations and variables.
3. Verify there are no residual duplicate hourly keys on `(station_code, variable_name_std, timestamp_utc)` after filtering the hourly artifact.
4. Confirm common-overlap diagnostics use only `2023-07-25` through `2025-11-01` and do not silently mix full-history values into the main readiness tables.
5. Validate that Greenwich `air_temperature_c` and `aux_air_temperature_c` comparisons are computed only on overlapping timestamps and reported separately from cross-station summaries.
6. Spot-check CAV and TRW timezone provenance by comparing `timestamp_local_raw`, `tz_token`, and `timestamp_utc` in the Phase 3 caution output.
7. Run a Python syntax or compile check on `src/03_explore.py` and `src/explore_utils.py` once implemented.
8. Review generated figures and CSVs to confirm they answer Phase 4 selection questions directly rather than duplicating raw Phase 2 audits.

**Decisions**
- Hourly diagnostics are the primary Phase 3 product because Phase 4 PCA and benchmarking depend on hourly alignment.
- Daily diagnostics are included only to assess FWI-input readiness for Cavendish and Greenwich, not to calculate moisture codes.
- The common-overlap window is the default basis for Phase 4-readiness recommendations; full-history outputs are sensitivity context only.
- Tracadie Wharf remains secondary in the recommendation tables unless you explicitly expand the primary modeling scope to all six stations.
- Phase 3 should recommend variable and station suitability explicitly, but must not create the final PCA matrix or benchmark outputs.

**Further Considerations**
1. If runtime or memory becomes limiting on the hourly scrubbed artifact, implement chunked summaries or station-by-station processing in Phase 3 rather than shrinking diagnostic scope.
2. If station-level plots become too dense across all 24 variables, prioritize core atmospheric variables for the main figure set and write marine diagnostics as tables plus a smaller companion figure set.
3. If the inherited CAV and TRW `-0300` offsets materially affect overlap alignment with Stanhope in spot checks, record that as a Phase 4 blocker rather than trying to reinterpret timestamps inside Phase 3.