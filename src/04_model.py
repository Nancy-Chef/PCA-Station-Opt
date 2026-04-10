"""
04_model.py – Phase 4: Modeling, Benchmarking, FWI, and Uncertainty
====================================================================
Orchestration entry point for the Phase 4 analysis pipeline.

All analysis logic lives in model_utils.py.  This script follows the same
thin-orchestration pattern used in 03_explore.py and 02_scrub.py.

Inputs (from data/scrubbed/)
------------------------------
phase2_hourly.csv                        UTC-normalized long-form hourly data
phase3_phase4_recommendations.csv        Usability gate for station-variable pairs
phase3_station_variable_fitness_overlap.csv   Overlap-window fitness metrics
phase3_daily_fwi_readiness.csv           CAV/GRE daily FWI-input readiness
phase3_station_summary.csv               Station-level suitability summary

Outputs (data/scrubbed/)
--------------------------
phase4_matrix_audit.csv             Retained/dropped columns in the PCA matrix
phase4_pca_loadings.csv             PCA component loadings  (feature × PC)
phase4_pca_scores_summary.csv       Mean/std scores per component   (diagnostic)
phase4_pca_explained_variance.csv   Variance explained by each component
phase4_pca_station_contributions.csv Mean-squared loading per station
phase4_pca_loo.csv                  Leave-one-out variance loss per station
phase4_benchmark_metrics.csv        Per station-variable RMSE/r/MAE/bias vs STA
phase4_benchmark_ci.csv             Bootstrap 95% CI for RMSE and r
phase4_fwi_codes_all.csv            Daily FFMC/DMC/DC for CAV, GRE, and STA
phase4_fwi_validation.csv           FWI validation outcomes vs STA / ECCC_REF
phase4_removal_risk_bootstrap.csv   Bootstrap loss distribution summary per station
phase4_removal_risk_kde.csv         KDE risk summary per station
phase4_network_recommendations.csv  Final network-optimization recommendations

Outputs (outputs/figures/)
---------------------------
phase4_scree.png
phase4_pca_biplot.png
phase4_benchmark_heatmap.png
phase4_removal_risk_kde.png
phase4_loo_bar.png
phase4_fwi_timeseries.png
phase4_fwi_validation.png

Usage
-----
  python src/04_model.py

All outputs overwrite prior runs; reruns are idempotent.
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

ROOT          = Path(__file__).resolve().parent.parent
SCRUBBED_DIR  = ROOT / "data" / "scrubbed"
FIGURES_DIR   = ROOT / "outputs" / "figures"

# Phase 2 / Phase 3 inputs
HOURLY_IN           = SCRUBBED_DIR / "phase2_hourly.csv"
RECS_IN             = SCRUBBED_DIR / "phase3_phase4_recommendations.csv"
FITNESS_OVERLAP_IN  = SCRUBBED_DIR / "phase3_station_variable_fitness_overlap.csv"
FWI_READY_IN        = SCRUBBED_DIR / "phase3_daily_fwi_readiness.csv"
STATION_SUM_IN      = SCRUBBED_DIR / "phase3_station_summary.csv"

# Phase 4 CSV outputs
MATRIX_AUDIT_OUT    = SCRUBBED_DIR / "phase4_matrix_audit.csv"
LOADINGS_OUT        = SCRUBBED_DIR / "phase4_pca_loadings.csv"
SCORES_SUM_OUT      = SCRUBBED_DIR / "phase4_pca_scores_summary.csv"
EXP_VAR_OUT         = SCRUBBED_DIR / "phase4_pca_explained_variance.csv"
STN_CONTRIB_OUT     = SCRUBBED_DIR / "phase4_pca_station_contributions.csv"
LOO_OUT             = SCRUBBED_DIR / "phase4_pca_loo.csv"
BENCHMARK_OUT       = SCRUBBED_DIR / "phase4_benchmark_metrics.csv"
BENCH_CI_OUT        = SCRUBBED_DIR / "phase4_benchmark_ci.csv"
FWI_CODES_OUT       = SCRUBBED_DIR / "phase4_fwi_codes_all.csv"
FWI_VALID_OUT       = SCRUBBED_DIR / "phase4_fwi_validation.csv"
RISK_BOOT_OUT       = SCRUBBED_DIR / "phase4_removal_risk_bootstrap.csv"
RISK_KDE_OUT        = SCRUBBED_DIR / "phase4_removal_risk_kde.csv"
RECOMMEND_OUT       = SCRUBBED_DIR / "phase4_network_recommendations.csv"

# Phase 4 figure outputs
FIG_SCREE       = FIGURES_DIR / "phase4_scree.png"
FIG_BIPLOT      = FIGURES_DIR / "phase4_pca_biplot.png"
FIG_BENCH_HEAT  = FIGURES_DIR / "phase4_benchmark_heatmap.png"
FIG_RISK_KDE    = FIGURES_DIR / "phase4_removal_risk_kde.png"
FIG_LOO_BAR     = FIGURES_DIR / "phase4_loo_bar.png"
FIG_FWI_TS      = FIGURES_DIR / "phase4_fwi_timeseries.png"
FIG_FWI_VALID   = FIGURES_DIR / "phase4_fwi_validation.png"

# Ensure src/ is on the path so model_utils can be imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from model_utils import (  # noqa: E402
    PARK_STATIONS,
    FWI_STATIONS,
    OVERLAP_START,
    OVERLAP_END,
    BOOTSTRAP_N,
    add_wind_direction_deg_sta,
    assemble_hourly_matrix,
    bootstrap_benchmark_ci,
    bootstrap_station_removal_risk,
    build_benchmark_pairs,
    calc_daily_fwi,
    compute_benchmark_metrics,
    compute_station_contributions,
    fetch_eccc_fwi_reference,
    fit_kde_risk_summary,
    load_approved_pairs,
    plot_benchmark_heatmap,
    plot_fwi_timeseries,
    plot_fwi_validation,
    plot_loo_bar,
    plot_pca_biplot,
    plot_removal_risk,
    plot_scree,
    run_loo_pca,
    run_pca,
    standardize_matrix,
    synthesize_recommendations,
    validate_phase4_handoff,
    validate_fwi_codes,
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
    Load phase2_hourly.csv with the UTC timestamp parsed and quality flags
    present.  The file is large; low_memory=False suppresses dtype warnings.
    """
    log.info("Loading hourly data from %s", HOURLY_IN)
    df = pd.read_csv(HOURLY_IN, low_memory=False)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    log.info("  Rows loaded: %d", len(df))
    return df


def _load_phase3_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the three Phase 3 tables used as selection gates in Phase 4."""
    log.info("Loading Phase 3 input tables.")
    recs_df      = pd.read_csv(RECS_IN)
    fwi_ready_df = pd.read_csv(FWI_READY_IN)
    station_sum  = pd.read_csv(STATION_SUM_IN)
    log.info(
        "  Recommendations: %d rows  |  FWI readiness: %d rows  |  "
        "Station summary: %d rows",
        len(recs_df), len(fwi_ready_df), len(station_sum),
    )
    return recs_df, fwi_ready_df, station_sum


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Execute the full Phase 4 modeling pipeline.

    Step 1  –  Validate Phase 3 handoff contract.
    Step 2  –  Assemble the analysis-ready hourly matrix.
    Step 3  –  Fit PCA and leave-one-station-out sensitivity.
    Step 4  –  Benchmark all Park stations against Stanhope.
    Step 5  –  Calculate daily FWI moisture codes for CAV, GRE, and STA.
    Step 6  –  Fetch ECCC reference data and validate FWI codes.
    Step 7  –  Bootstrap station-removal risk and fit KDE summaries.
    Step 8  –  Synthesize network-optimization recommendations.
    Step 9  –  Produce all figures.
    Step 10 –  Write all CSV outputs and print terminal summary.
    """

    # ------------------------------------------------------------------
    # Step 1: Validate Phase 3 handoff
    # ------------------------------------------------------------------
    log.info("=== Step 1: Validating Phase 3 handoff ===")
    hourly_df = _load_hourly()
    recs_df, fwi_ready_df, station_sum = _load_phase3_inputs()

    handoff_summary = validate_phase4_handoff(hourly_df, recs_df, fwi_ready_df)
    if handoff_summary["warnings"]:
        log.warning(
            "Phase 3 handoff issued %d warning(s). "
            "Review before interpreting outputs.",
            len(handoff_summary["warnings"]),
        )

    # ------------------------------------------------------------------
    # Step 2: Assemble the analysis matrix
    # ------------------------------------------------------------------
    log.info("=== Step 2: Assembling PCA matrix ===")
    approved_pairs = load_approved_pairs(recs_df)

    # Derive STA wind_direction_deg before benchmarking (not used for PCA).
    hourly_bench = add_wind_direction_deg_sta(hourly_df)

    matrix_df, coverage_report = assemble_hourly_matrix(
        hourly_df,
        approved_pairs,
        park_stations=PARK_STATIONS,
    )
    scaled_df, scaler = standardize_matrix(matrix_df)

    log.info(
        "  Matrix shape: %d timestamps × %d features",
        scaled_df.shape[0], scaled_df.shape[1],
    )
    coverage_report = coverage_report.reset_index()
    coverage_report.columns = ["station_code", "variable_name_std", "coverage", "retained"]

    # ------------------------------------------------------------------
    # Step 3: PCA and leave-one-station-out sensitivity
    # ------------------------------------------------------------------
    log.info("=== Step 3: PCA redundancy analysis ===")
    pca_result = run_pca(scaled_df)

    station_contrib = compute_station_contributions(
        pca_result["loadings"],
        n_pcs=min(3, pca_result["n_components"]),
    )

    log.info("Running leave-one-station-out sensitivity ...")
    loo_df = run_loo_pca(scaled_df, park_stations=PARK_STATIONS)

    log.info("  LOO results:\n%s", loo_df.to_string(index=False))

    # Scores summary (mean and std per component for reporting)
    scores_summary = pca_result["scores"].agg(["mean", "std"]).T.reset_index()
    scores_summary.columns = ["component", "mean_score", "std_score"]

    explained_var_df = pd.DataFrame({
        "component":    [f"PC{i+1}" for i in range(pca_result["n_components"])],
        "explained_var_ratio": pca_result["explained_var"],
        "cumulative_var_ratio": pca_result["cum_var"],
    })

    # ------------------------------------------------------------------
    # Step 4: Stanhope benchmark
    # ------------------------------------------------------------------
    log.info("=== Step 4: Stanhope benchmarking ===")
    bench_pairs  = build_benchmark_pairs(hourly_bench, approved_pairs)
    benchmark_df = compute_benchmark_metrics(bench_pairs)

    log.info("  Benchmark pairs found: %d", len(bench_pairs))
    if benchmark_df.empty:
        log.warning("No benchmark pairs computed; check overlap and STA coverage.")
    else:
        log.info(
            "  Mean RMSE across all pairs: %.3f",
            benchmark_df["rmse"].mean(),
        )

    log.info("Running bootstrap confidence intervals for benchmarks ...")
    bench_ci_df = bootstrap_benchmark_ci(bench_pairs, n_resamples=BOOTSTRAP_N)

    # ------------------------------------------------------------------
    # Step 5: Daily FWI moisture codes for CAV, GRE, and STA
    # ------------------------------------------------------------------
    log.info("=== Step 5: Daily FWI moisture codes ===")
    fwi_dict: dict = {}
    for station in FWI_STATIONS:
        log.info("  Computing FWI for %s ...", station)
        fwi_dict[station] = calc_daily_fwi(hourly_df, station)

    # Concatenate all FWI results into a single table for output
    fwi_all_df = pd.concat(fwi_dict.values(), ignore_index=True)
    log.info(
        "  FWI codes computed: %d total station-days",
        len(fwi_all_df),
    )

    # ------------------------------------------------------------------
    # Step 6: ECCC reference FWI fetch and validation
    # ------------------------------------------------------------------
    log.info("=== Step 6: ECCC reference FWI and validation ===")
    start_year = OVERLAP_START.year
    end_year   = OVERLAP_END.year

    log.info(
        "Attempting to fetch ECCC CDO reference data for %d–%d ...",
        start_year, end_year,
    )
    eccc_ref_fwi = fetch_eccc_fwi_reference(
        year_start=start_year,
        year_end=end_year,
    )

    if eccc_ref_fwi is not None:
        log.info(
            "  ECCC reference FWI retrieved: %d days.", len(eccc_ref_fwi)
        )
    else:
        log.info(
            "  ECCC CDO reference unavailable. "
            "FWI validation will use STA-computed values only."
        )

    fwi_validation_df = validate_fwi_codes(
        computed_dict=fwi_dict,
        reference_df=eccc_ref_fwi,
    )
    log.info("  Validation outcomes:\n%s",
             fwi_validation_df[["station","reference","fwi_code","rmse","outcome"]]
             .to_string(index=False))

    # ------------------------------------------------------------------
    # Step 7: Bootstrap station-removal risk and KDE
    # ------------------------------------------------------------------
    log.info("=== Step 7: Bootstrap uncertainty and KDE risk ===")
    log.info("Running %d bootstrap resamples (this may take a few minutes) ...", BOOTSTRAP_N)
    removal_losses = bootstrap_station_removal_risk(
        scaled_df,
        park_stations=PARK_STATIONS,
        n_resamples=BOOTSTRAP_N,
    )

    kde_summary = fit_kde_risk_summary(removal_losses, threshold=0.05)
    log.info("  KDE risk summary:\n%s", kde_summary.to_string(index=False))

    # Turn the per-station loss arrays into a tidy table for CSV output
    risk_boot_records = []
    for station, losses in removal_losses.items():
        for b, loss in enumerate(losses):
            risk_boot_records.append({"station": station, "replicate": b, "loss": float(loss)})
    risk_boot_df = pd.DataFrame(risk_boot_records)

    # ------------------------------------------------------------------
    # Step 8: Network recommendations
    # ------------------------------------------------------------------
    log.info("=== Step 8: Network-optimization recommendations ===")
    recommendations_df = synthesize_recommendations(
        loo_df=loo_df,
        benchmark_df=benchmark_df,
        kde_summary=kde_summary,
        fwi_validation=fwi_validation_df if not fwi_validation_df.empty else None,
    )
    log.info("  Recommendations:\n%s",
             recommendations_df[["station","recommendation","reason"]]
             .to_string(index=False))

    # ------------------------------------------------------------------
    # Step 9: Figures
    # ------------------------------------------------------------------
    log.info("=== Step 9: Generating figures ===")
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    plot_scree(pca_result, FIG_SCREE)

    if pca_result["n_components"] >= 2:
        plot_pca_biplot(pca_result, FIG_BIPLOT)
    else:
        log.info("  Fewer than 2 PCs; skipping biplot.")

    if not benchmark_df.empty:
        plot_benchmark_heatmap(benchmark_df, FIG_BENCH_HEAT)

    plot_removal_risk(removal_losses, kde_summary, FIG_RISK_KDE, threshold=0.05)
    plot_loo_bar(loo_df, FIG_LOO_BAR)

    if fwi_dict:
        plot_fwi_timeseries(fwi_dict, FIG_FWI_TS)

    if not fwi_validation_df.empty:
        plot_fwi_validation(fwi_validation_df, FIG_FWI_VALID)

    # ------------------------------------------------------------------
    # Step 10: Write all CSV outputs and terminal summary
    # ------------------------------------------------------------------
    log.info("=== Step 10: Writing outputs ===")

    write_csv(coverage_report,      MATRIX_AUDIT_OUT)
    write_csv(pca_result["loadings"], LOADINGS_OUT)
    write_csv(scores_summary,        SCORES_SUM_OUT)
    write_csv(explained_var_df,      EXP_VAR_OUT)
    write_csv(station_contrib,       STN_CONTRIB_OUT)
    write_csv(loo_df,                LOO_OUT)
    write_csv(benchmark_df,          BENCHMARK_OUT)
    write_csv(bench_ci_df,           BENCH_CI_OUT)
    write_csv(fwi_all_df,            FWI_CODES_OUT)
    write_csv(fwi_validation_df,     FWI_VALID_OUT)
    write_csv(risk_boot_df,          RISK_BOOT_OUT)
    write_csv(kde_summary,           RISK_KDE_OUT)
    write_csv(recommendations_df,    RECOMMEND_OUT)

    # ------------------------------------------------------------------
    # Terminal summary
    # ------------------------------------------------------------------
    log.info("")
    log.info("========================================")
    log.info("Phase 4 complete – summary")
    log.info("========================================")
    log.info("PCA matrix:        %d timestamps × %d features",
             scaled_df.shape[0], scaled_df.shape[1])
    log.info("PCA components:    %d (explain %.1f%% variance)",
             pca_result["n_components"], pca_result["cum_var"][-1] * 100)
    log.info("Benchmark pairs:   %d station-variable combinations", len(bench_pairs))
    if not benchmark_df.empty:
        log.info("  Mean RMSE vs STA: %.3f", benchmark_df["rmse"].mean())
    log.info("FWI station-days:  %d  (CAV: %d  GRE: %d  STA: %d)",
             len(fwi_all_df),
             len(fwi_dict.get("CAV", [])),
             len(fwi_dict.get("GRE", [])),
             len(fwi_dict.get("STA", [])),
             )
    log.info("FWI validation rows: %d", len(fwi_validation_df))
    log.info("Bootstrap resamples: %d  |  block size: %d h", BOOTSTRAP_N, 168)
    log.info("Network recommendations:")
    for _, row in recommendations_df.iterrows():
        log.info("  %-5s  →  %s", row["station"], row["recommendation"])
    log.info("All outputs written to %s and %s.", SCRUBBED_DIR, FIGURES_DIR)


if __name__ == "__main__":
    main()
