## Phase 3 Summary – Exploration Diagnostics

### What was done

Phase 3 is complete. The following source files were created:

- `src/03_explore.py` – orchestration entry point; runnable from the command line
- `src/explore_utils.py` – helper module covering handoff validation, fitness tables, gap and cadence checks, sensor comparisons, figure generation, and Phase 4 recommendation logic

Running `python src/03_explore.py` from the workspace root reads the Phase 2 scrubbed products and produces twelve diagnostic CSV outputs in `data/scrubbed/` plus seven exploratory figures in `outputs/figures/`.

### Inputs used

Phase 3 uses the Phase 2 handoff artifacts as its analytical base:

- `data/scrubbed/phase2_hourly.csv`
- `data/scrubbed/phase2_daily.csv`
- `data/scrubbed/phase2_completeness.csv`
- `data/scrubbed/phase2_schema_audit.csv`
- `data/scrubbed/phase2_ts_audit.csv`

The primary diagnostic window is the confirmed common overlap across stations: **2023-07-25 to 2025-11-01**.

### Outputs produced

| Artifact | Rows | Purpose |
|----------|------|---------|
| `phase3_station_variable_fitness.csv` | 85 | Full-history per station-variable fitness table |
| `phase3_station_variable_fitness_overlap.csv` | 83 | Common-overlap fitness table |
| `phase3_station_summary.csv` | 6 | Station-level suitability summary |
| `phase3_variable_summary.csv` | 24 | Variable-level coverage and inclusion summary |
| `phase3_quality_flags.csv` | 115 | Quality-flag distribution by station and variable |
| `phase3_daily_fwi_readiness.csv` | 8 | Cavendish/Greenwich daily FWI-input readiness check |
| `phase3_cadence_check.csv` | 85 | Off-grid and duplicate timestamp diagnostics |
| `phase3_gap_summary.csv` | 85 | Gap-length distribution summary |
| `phase3_outlier_summary.csv` | 74 | IQR-based outlier counts for native_ok values |
| `phase3_tz_spot_check.csv` | 0 | CAV/TRW timezone provenance spot-check |
| `phase3_gre_sensor_comparison.csv` | 3 | Greenwich primary vs auxiliary air-temperature comparison |
| `phase3_phase4_recommendations.csv` | 83 | Usability classifications for Phase 4 |

### Figures produced

All figures were written to `outputs/figures/`:

- `phase3_completeness_heatmap_fullhistory.png`
- `phase3_completeness_heatmap_overlap.png`
- `phase3_availability_timeline.png`
- `phase3_distributions_core_atmo.png`
- `phase3_distributions_marine.png`
- `phase3_gap_heatmap.png`
- `phase3_gre_sensor_comparison.png`

### Key diagnostics

The Phase 2 handoff validated cleanly:

- Hourly rows loaded: 2,015,901
- Daily rows loaded: 84,100
- Stations present: 6
- Variables present: 24
- Common-overlap rows in hourly data: 1,572,748
- Timestamp audit rows: 0 parseable rows, consistent with the Phase 2 zero-issue result

Cadence diagnostics also completed cleanly:

- Off-grid hourly series: 0
- Duplicate hourly series: 0

Station-level summary from the full-history fitness table classified all six stations as `primary` on median atmospheric completeness:

- CAV: 93.1%
- GRE: 92.0%
- NRW: 99.0%
- SBW: 87.6%
- STA: 98.6%
- TRW: 91.9%

The variable-level recommendation step identified 9 `candidate-core` variables for the common reduced feature set:

- `air_temperature_c`
- `dew_point_c`
- `relative_humidity_pct`
- `wind_speed_kmh`
- `wind_gust_kmh`
- `wind_direction_deg`
- `solar_radiation_wm2`
- `precipitation_mm`
- `accumulated_rain_mm`

The Phase 4 recommendation table classified the remaining station-variable pairs as either `usable-with-caveat` or `not-recommended` based on completeness, gap burden, category, and overlap-window presence.

### Notes and caveats

1. `phase2_hourly.csv` does not retain `timestamp_local_raw` or `tz_token`, so the CAV/TRW timezone spot-check cannot be reconstructed from the hourly artifact alone. The Phase 3 run logged this and emitted an empty `phase3_tz_spot_check.csv`.
2. Greenwich primary versus auxiliary air-temperature comparison found only 3 overlapping hours in the hourly scrubbed output. The result is useful as a cautionary diagnostic, but the auxiliary sensor series is too sparse for a strong agreement assessment.
3. Phase 3 stayed strictly diagnostic. It did not fit PCA, compute Stanhope benchmark metrics, or calculate FWI moisture codes.
4. No off-grid or duplicate hourly timestamps were found, so cadence regularity appears stable after Phase 2 scrubbing.

### Verification performed

1. The Phase 2 hourly and daily artifacts loaded successfully and matched the expected station and variable counts.
2. The common-overlap diagnostics were computed using the Phase 2 overlap window and not mixed with full-history metrics.
3. The hourly cadence check confirmed zero off-grid timestamps and zero duplicate keys.
4. The figure outputs and CSV outputs were confirmed on disk after the run.
5. Python syntax checks passed for `src/03_explore.py` and `src/explore_utils.py` before execution.

### Next steps (Phase 4 inputs)

Phase 4 (`04_model.py` and supporting utilities) should:

- Use the common-overlap hourly window as the primary PCA and benchmarking frame
- Build the common reduced feature set from the `candidate-core` variables identified here
- Treat CAV/TRW timezone alignment as a cautionary item if UTC precision matters for benchmarking
- Use the Greenwich sensor comparison only as a diagnostic check, not as a second modeling input unless future data support improves
- Carry forward the Phase 3 recommendation table when selecting variables for Stanhope benchmarking and uncertainty analysis