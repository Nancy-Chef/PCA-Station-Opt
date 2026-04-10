"""
03_explore.py – Phase 3: Exploration Diagnostics
=================================================
Diagnostic-only exploration of the Phase 2 scrubbed artifacts.  Produces
a suite of summary tables and figures that quantify station-variable fitness
for downstream PCA, Stanhope benchmarking, and FWI moisture-code work.

SCOPE BOUNDARY  No PCA fitting, no Stanhope benchmark metrics, no FWI
                calculation, and no new feature engineering are performed
                in this script.  All outputs are diagnostic.

Inputs (from data/scrubbed/)
------------------------------
phase2_hourly.csv       Primary long-form hourly scrubbed data
phase2_daily.csv        Daily aggregates (used for FWI-readiness check)
phase2_completeness.csv Baseline completeness reference from Phase 2
phase2_ts_audit.csv     Timestamp audit baseline (expected empty)

Outputs (data/scrubbed/)
--------------------------
phase3_station_variable_fitness.csv       Full-history per-(station,variable)
phase3_station_variable_fitness_overlap.csv  Common-overlap window only
phase3_station_summary.csv                Station-level rollup
phase3_variable_summary.csv               Variable-level rollup
phase3_quality_flags.csv                  Flag-distribution table
phase3_daily_fwi_readiness.csv            CAV/GRE daily FWI-input coverage
phase3_cadence_check.csv                  Off-grid and duplicate checks
phase3_gap_summary.csv                    Gap-length distribution per series
phase3_outlier_summary.csv                IQR-based outlier counts (no removal)
phase3_tz_spot_check.csv                  CAV/TRW timezone provenance table
phase3_gre_sensor_comparison.csv          GRE primary vs auxiliary temperature
phase3_phase4_recommendations.csv         Usability classifications for Phase 4

Outputs (outputs/figures/)
---------------------------
phase3_completeness_heatmap_fullhistory.png
phase3_completeness_heatmap_overlap.png
phase3_availability_timeline.png
phase3_distributions_core_atmo.png
phase3_distributions_marine.png
phase3_gap_heatmap.png
phase3_gre_sensor_comparison.png

Usage
-----
  python src/03_explore.py

All outputs overwrite prior runs; reruns are idempotent.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

ROOT          = Path(__file__).resolve().parent.parent
SCRUBBED_DIR  = ROOT / "data" / "scrubbed"
FIGURES_DIR   = ROOT / "outputs" / "figures"

# Phase 2 inputs
HOURLY_IN       = SCRUBBED_DIR / "phase2_hourly.csv"
DAILY_IN        = SCRUBBED_DIR / "phase2_daily.csv"
COMPLETENESS_IN = SCRUBBED_DIR / "phase2_completeness.csv"
TS_AUDIT_IN     = SCRUBBED_DIR / "phase2_ts_audit.csv"

# Phase 3 CSV outputs
FITNESS_OUT         = SCRUBBED_DIR / "phase3_station_variable_fitness.csv"
FITNESS_OV_OUT      = SCRUBBED_DIR / "phase3_station_variable_fitness_overlap.csv"
STATION_SUM_OUT     = SCRUBBED_DIR / "phase3_station_summary.csv"
VARIABLE_SUM_OUT    = SCRUBBED_DIR / "phase3_variable_summary.csv"
FLAG_DIST_OUT       = SCRUBBED_DIR / "phase3_quality_flags.csv"
FWI_READY_OUT       = SCRUBBED_DIR / "phase3_daily_fwi_readiness.csv"
CADENCE_OUT         = SCRUBBED_DIR / "phase3_cadence_check.csv"
GAP_OUT             = SCRUBBED_DIR / "phase3_gap_summary.csv"
OUTLIER_OUT         = SCRUBBED_DIR / "phase3_outlier_summary.csv"
TZ_SPOT_OUT         = SCRUBBED_DIR / "phase3_tz_spot_check.csv"
GRE_SENSOR_OUT      = SCRUBBED_DIR / "phase3_gre_sensor_comparison.csv"
RECOMMEND_OUT       = SCRUBBED_DIR / "phase3_phase4_recommendations.csv"

# Phase 3 figure outputs
FIG_HEAT_FULL   = FIGURES_DIR / "phase3_completeness_heatmap_fullhistory.png"
FIG_HEAT_OV     = FIGURES_DIR / "phase3_completeness_heatmap_overlap.png"
FIG_TIMELINE    = FIGURES_DIR / "phase3_availability_timeline.png"
FIG_DIST_ATMO   = FIGURES_DIR / "phase3_distributions_core_atmo.png"
FIG_DIST_MARINE = FIGURES_DIR / "phase3_distributions_marine.png"
FIG_GAP         = FIGURES_DIR / "phase3_gap_heatmap.png"
FIG_GRE         = FIGURES_DIR / "phase3_gre_sensor_comparison.png"

# Extend sys.path so sibling src/ modules can be imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from explore_utils import (  # noqa: E402
    ALL_STATIONS,
    CORE_ATMO_VARS,
    MARINE_VARS,
    OVERLAP_END,
    OVERLAP_START,
    PRIMARY_STATIONS,
    build_fwi_readiness,
    build_fitness_table,
    build_flag_distribution,
    build_gap_summary,
    build_outlier_summary,
    build_phase4_recommendations,
    build_station_summary,
    build_tz_spot_check,
    build_variable_summary,
    check_cadence,
    compare_gre_sensors,
    plot_availability_timeline,
    plot_completeness_heatmap,
    plot_distribution_grid,
    plot_gap_heatmap,
    plot_gre_sensor_comparison,
    summarise_sensor_comparison,
    validate_handoff,
    write_csv,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _load_hourly() -> pd.DataFrame:
    """
    Load phase2_hourly.csv with the timestamp column parsed to UTC datetime.
    The file is large (~866 MB). We read it with low_memory=False to avoid
    mixed-type warnings on the long-form value columns.
    """
    log.info("Loading hourly data from %s", HOURLY_IN)
    df = pd.read_csv(HOURLY_IN, low_memory=False)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    log.info("  Hourly rows loaded: %d", len(df))
    return df


def _load_daily() -> pd.DataFrame:
    """Load phase2_daily.csv with the timestamp column parsed to UTC datetime."""
    log.info("Loading daily data from %s", DAILY_IN)
    df = pd.read_csv(DAILY_IN, low_memory=False)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    log.info("  Daily rows loaded: %d", len(df))
    return df


def _check_ts_audit() -> int:
    """
    Check phase2_ts_audit.csv row count and warn if it is non-empty.
    Returns the number of audit rows found.  A zero-byte or header-only file
    (i.e. the expected Phase 2 outcome of zero issues) returns 0.
    """
    if not TS_AUDIT_IN.exists():
        log.warning("phase2_ts_audit.csv not found at %s", TS_AUDIT_IN)
        return -1
    # Guard against completely empty file (zero bytes) which pandas cannot parse.
    if TS_AUDIT_IN.stat().st_size == 0:
        log.info("phase2_ts_audit.csv is empty (zero bytes – zero timestamp issues).")
        return 0
    try:
        df = pd.read_csv(TS_AUDIT_IN)
    except pd.errors.EmptyDataError:
        log.info("phase2_ts_audit.csv has no parseable rows (zero timestamp issues).")
        return 0
    n = len(df)
    if n > 0:
        log.warning(
            "phase2_ts_audit.csv contains %d rows – Phase 2 timestamp issues present. "
            "Review before accepting Phase 3 results.",
            n,
        )
    else:
        log.info("phase2_ts_audit.csv is empty (zero timestamp issues).")
    return n


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Execute the full Phase 3 exploration pipeline:

    Step 1 – Validate Phase 2 handoff contract.
    Step 2 – Build tabular fitness and readiness diagnostics.
    Step 3 – Build missingness, cadence, and gap diagnostics.
    Step 4 – Build distribution, outlier, and sensor diagnostics.
    Step 5 – Build Phase 4 handoff recommendations.
    Step 6 – Produce all figures.
    Step 7 – Write all CSV outputs.
    Step 8 – Print terminal summary.
    """

    # ------------------------------------------------------------------
    # Step 1: Validate Phase 2 handoff contract
    # ------------------------------------------------------------------
    log.info("=== Step 1: Validating Phase 2 handoff ===")
    ts_audit_rows = _check_ts_audit()

    hourly_df = _load_hourly()
    hourly_summary = validate_handoff(hourly_df, "hourly")

    daily_df = _load_daily()
    daily_summary = validate_handoff(daily_df, "daily")

    # Warn if any primary stations are missing
    present_stations = set(hourly_summary["distinct_stations"])
    missing_stations = set(ALL_STATIONS) - present_stations
    if missing_stations:
        log.warning("Stations expected but not found in hourly data: %s", missing_stations)

    # ------------------------------------------------------------------
    # Step 2: Tabular fitness and readiness diagnostics
    # ------------------------------------------------------------------
    log.info("=== Step 2: Building fitness tables ===")

    # Full-history fitness table
    log.info("  Building full-history fitness table ...")
    fitness_full = build_fitness_table(hourly_df, overlap_only=False)

    # Common-overlap fitness table
    log.info("  Building common-overlap fitness table ...")
    fitness_ov = build_fitness_table(hourly_df, overlap_only=True)

    # Station-level summary (based on full-history)
    log.info("  Building station summary ...")
    station_summary = build_station_summary(fitness_full)

    # Variable-level summary (based on full-history, filtered to overlap stations)
    log.info("  Building variable summary ...")
    variable_summary = build_variable_summary(fitness_full)

    # Quality-flag distribution
    log.info("  Building flag distribution ...")
    flag_dist = build_flag_distribution(hourly_df)

    # Daily FWI-readiness (Cavendish and Greenwich only)
    log.info("  Building FWI-readiness table ...")
    fwi_readiness = build_fwi_readiness(daily_df)

    # ------------------------------------------------------------------
    # Step 3: Missingness, cadence, and gap diagnostics
    # ------------------------------------------------------------------
    log.info("=== Step 3: Cadence and gap diagnostics ===")

    log.info("  Checking hourly cadence (off-grid timestamps and duplicates) ...")
    cadence = check_cadence(hourly_df)

    log.info("  Building gap-length summary ...")
    gap_summary = build_gap_summary(hourly_df)

    # CAV / TRW timezone spot-check
    log.info("  Building CAV/TRW timezone spot-check ...")
    tz_spot = build_tz_spot_check(hourly_df, n_samples=15)

    # ------------------------------------------------------------------
    # Step 4: Distribution, outlier, and sensor diagnostics
    # ------------------------------------------------------------------
    log.info("=== Step 4: Distribution and sensor diagnostics ===")

    log.info("  Building outlier summary (IQR, native_ok rows only) ...")
    outlier_summary = build_outlier_summary(hourly_df, iqr_multiplier=3.0)

    log.info("  Comparing GRE primary vs auxiliary temperature sensors ...")
    gre_comparison = compare_gre_sensors(hourly_df)
    gre_stats = summarise_sensor_comparison(gre_comparison)
    if gre_stats:
        log.info(
            "  GRE sensor stats: n=%d, mean_diff=%.3f°C, std=%.3f°C, "
            "pct_within_0.5°C=%.1f%%",
            gre_stats["n_overlap_hours"],
            gre_stats["mean_diff_c"],
            gre_stats["std_diff_c"],
            gre_stats["pct_within_0_5c"],
        )

    # ------------------------------------------------------------------
    # Step 5: Phase 4 handoff recommendations
    # ------------------------------------------------------------------
    log.info("=== Step 5: Building Phase 4 recommendations ===")
    # Use the overlap-window fitness table as the basis for recommendations
    # so they reflect realistic Phase 4 analysis conditions.
    recommendations = build_phase4_recommendations(
        fitness_df=fitness_ov,
        variable_summary_df=variable_summary,
        gap_df=gap_summary,
    )

    # ------------------------------------------------------------------
    # Step 6: Produce figures
    # ------------------------------------------------------------------
    log.info("=== Step 6: Generating figures ===")

    log.info("  Completeness heatmap – full history ...")
    plot_completeness_heatmap(fitness_full, FIG_HEAT_FULL, title_suffix="(full history)")

    log.info("  Completeness heatmap – common overlap ...")
    plot_completeness_heatmap(fitness_ov, FIG_HEAT_OV,
                               title_suffix=f"({OVERLAP_START.date()} – {OVERLAP_END.date()})")

    log.info("  Availability timeline ...")
    plot_availability_timeline(hourly_df, FIG_TIMELINE)

    log.info("  Distribution grid – core atmospheric variables ...")
    plot_distribution_grid(
        hourly_df,
        variables=CORE_ATMO_VARS,
        out_path=FIG_DIST_ATMO,
        title="Core Atmospheric Variable Distributions (common-overlap, native_ok)",
        overlap_only=True,
    )

    log.info("  Distribution grid – marine variables ...")
    plot_distribution_grid(
        hourly_df,
        variables=MARINE_VARS,
        out_path=FIG_DIST_MARINE,
        title="Marine Variable Distributions (common-overlap, native_ok)",
        overlap_only=True,
    )

    log.info("  Gap heatmap ...")
    plot_gap_heatmap(gap_summary, FIG_GAP)

    log.info("  GRE sensor comparison figure ...")
    plot_gre_sensor_comparison(gre_comparison, FIG_GRE)

    # ------------------------------------------------------------------
    # Step 7: Write CSV outputs
    # ------------------------------------------------------------------
    log.info("=== Step 7: Writing CSV outputs ===")
    write_csv(fitness_full,     FITNESS_OUT)
    write_csv(fitness_ov,       FITNESS_OV_OUT)
    write_csv(station_summary,  STATION_SUM_OUT)
    write_csv(variable_summary, VARIABLE_SUM_OUT)
    write_csv(flag_dist,        FLAG_DIST_OUT)
    write_csv(fwi_readiness,    FWI_READY_OUT)
    write_csv(cadence,          CADENCE_OUT)
    write_csv(gap_summary,      GAP_OUT)
    write_csv(outlier_summary,  OUTLIER_OUT)
    write_csv(tz_spot,          TZ_SPOT_OUT)
    write_csv(gre_comparison,   GRE_SENSOR_OUT)
    write_csv(recommendations,  RECOMMEND_OUT)

    # ------------------------------------------------------------------
    # Step 8: Terminal summary
    # ------------------------------------------------------------------
    _print_summary(
        hourly_summary=hourly_summary,
        daily_summary=daily_summary,
        ts_audit_rows=ts_audit_rows,
        fitness_full=fitness_full,
        fitness_ov=fitness_ov,
        station_summary=station_summary,
        variable_summary=variable_summary,
        cadence=cadence,
        gre_stats=gre_stats,
        recommendations=recommendations,
    )


# ---------------------------------------------------------------------------
# Terminal summary printer
# ---------------------------------------------------------------------------

def _print_summary(
    hourly_summary: dict,
    daily_summary: dict,
    ts_audit_rows: int,
    fitness_full: pd.DataFrame,
    fitness_ov: pd.DataFrame,
    station_summary: pd.DataFrame,
    variable_summary: pd.DataFrame,
    cadence: pd.DataFrame,
    gre_stats: dict,
    recommendations: pd.DataFrame,
) -> None:
    """Print a concise terminal summary analogous to the Phase 2 pattern."""

    print("\n" + "=" * 65)
    print("Phase 3 Exploration Summary")
    print("=" * 65)

    print(f"\n  Phase 2 handoff")
    print(f"    Hourly rows loaded:       {hourly_summary['total_rows']:>12,}")
    print(f"    Daily rows loaded:        {daily_summary['total_rows']:>12,}")
    print(f"    Common-overlap rows:      {hourly_summary.get('overlap_rows', 0):>12,}")
    print(f"    Stations (hourly):        {hourly_summary['n_stations']:>12}")
    print(f"    Variables (hourly):       {hourly_summary['n_variables']:>12}")
    print(f"    TS audit rows (expected 0):{ts_audit_rows:>11}")

    print(f"\n  Station-variable fitness (full history)")
    print(f"    Series with >=70% complete: "
          f"{int((fitness_full['pct_complete'] >= 70).sum()):>6}")
    print(f"    Series with <50% complete:  "
          f"{int((fitness_full['pct_complete'] < 50).sum()):>6}")

    print(f"\n  Station-variable fitness (common-overlap {OVERLAP_START.date()} – {OVERLAP_END.date()})")
    print(f"    Series with >=70% complete: "
          f"{int((fitness_ov['pct_complete'] >= 70).sum()):>6}")
    print(f"    Series with <50% complete:  "
          f"{int((fitness_ov['pct_complete'] < 50).sum()):>6}")

    if not station_summary.empty:
        print(f"\n  Station recommended status (median atmo. completeness, full history):")
        for _, row in station_summary.sort_values("station_code").iterrows():
            print(
                f"    {row['station_code']:6s}  {row['median_pct_complete_atmo']:5.1f}%  "
                f"{row['recommended_status']}"
            )

    if not variable_summary.empty:
        core = variable_summary[
            variable_summary["preliminary_inclusion_status"] == "candidate-core"
        ]
        print(f"\n  Candidate-core variables: {len(core)}")
        for v in core["variable_name_std"]:
            print(f"    {v}")

    # Cadence issues
    n_offgrid = int((cadence["n_off_grid"] > 0).sum()) if not cadence.empty else 0
    n_dupes   = int((cadence["n_duplicates"] > 0).sum()) if not cadence.empty else 0
    print(f"\n  Cadence issues")
    print(f"    Series with off-grid timestamps: {n_offgrid}")
    print(f"    Series with duplicate timestamps: {n_dupes}")

    if gre_stats:
        print(f"\n  GRE sensor comparison (primary vs auxiliary air temp)")
        print(f"    Overlapping hours:   {gre_stats['n_overlap_hours']:>8,}")
        print(f"    Mean diff (P – A):   {gre_stats['mean_diff_c']:>8.3f} °C")
        print(f"    Std diff:            {gre_stats['std_diff_c']:>8.3f} °C")
        print(f"    Within ±0.5 °C:      {gre_stats['pct_within_0_5c']:>7.1f} %")

    if not recommendations.empty:
        rec_counts = recommendations["recommendation"].value_counts()
        print(f"\n  Phase 4 readiness recommendations")
        for label in ["usable", "usable-with-caveat", "not-recommended"]:
            print(f"    {label:<22s}: {rec_counts.get(label, 0):>4}")

    print(f"\n  Outputs written to data/scrubbed/ and outputs/figures/")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
