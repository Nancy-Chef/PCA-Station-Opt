"""
model_utils.py – Phase 4: Modeling, Benchmarking, FWI, and Uncertainty Helpers
===============================================================================
All heavy-lifting for the Phase 4 pipeline lives here so that 04_model.py
remains a thin orchestration entry point.  Functions are stateless and accept
plain Python / pandas types for easy isolated testing.

Responsibilities
----------------
1.  Input loading and Phase 3 handoff validation
2.  Analysis-ready hourly matrix assembly and standardization
3.  PCA-based redundancy analysis (Park stations only)
4.  Leave-one-station-out PCA sensitivity
5.  ECCC Stanhope hourly benchmarking with bootstrap confidence intervals
6.  Daily FWI moisture-code calculations (FFMC, DMC, DC)
7.  ECCC reference data fetch and reference FWI computation
8.  FWI validation against computed reference
9.  Block-bootstrap station-removal risk estimation
10. KDE-based risk distribution fitting and tail-probability summaries
11. Network-optimization recommendation synthesis
12. All Phase 4 figure generation
13. Deterministic CSV output writers

FWI implementation
------------------
Implements the Canadian Forest Fire Weather Index System formulas from
Van Wagner (1987, CFS Tech. Report 35) and CFFDRS (Lawson & Armitage 2008).
Day-length adjustors are interpolated for PEI (~46.5°N).
Observation convention: 16:00 UTC = ≈ noon AST / 13:00 ADT for PEI.
"""

import logging
import warnings
from io import StringIO
from math import exp, log
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns
from scipy.stats import gaussian_kde
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# Re-use Phase 2 quality-flag vocabulary so Phase 4 does not redefine it.
from scrub_utils import SCRUB_FLAG_OK  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Constants
# ---------------------------------------------------------------------------

# Stations participating in the Parks Canada network (PCA + benchmarking).
PARK_STATIONS = ["CAV", "GRE", "NRW", "SBW", "TRW"]

# Reference station used exclusively for benchmarking (not in PCA removal set).
BENCHMARK_REF = "STA"

# Stations for which daily FWI moisture codes are calculated.
FWI_STATIONS = ["CAV", "GRE", "STA"]  # STA = reference

# Common overlap window (confirmed in Phase 3).
OVERLAP_START = pd.Timestamp("2023-07-25", tz="UTC")
OVERLAP_END   = pd.Timestamp("2025-11-01", tz="UTC")

# Phase 3 candidate-core variables; accumulated_rain_mm excluded from PCA
# because it is cumulative (highly correlated with precipitation_mm).
PCA_CANDIDATE_VARS = [
    "air_temperature_c",
    "dew_point_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "wind_direction_deg",
    "solar_radiation_wm2",
    "precipitation_mm",
    "pressure_kpa",
]

# Benchmark variables that have good STA coverage in the overlap window.
# STA has wind_direction_10s_deg (converted to deg below).
BENCHMARK_VARS = [
    "air_temperature_c",
    "dew_point_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "precipitation_mm",
]

# Daily variables required for FWI moisture-code calculations.
FWI_REQUIRED_VARS = [
    "air_temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "precipitation_mm",
]

# Hourly UTC observation time used as the daily noon observation for FWI.
FWI_OBS_HOUR_UTC = 16

# FWI default restart values (start of fire season or after long gap).
FFMC_DEFAULT = 85.0
DMC_DEFAULT  =  6.0
DC_DEFAULT   = 15.0

# Gaps larger than this trigger an FWI code restart.
RESTART_GAP_DAYS = 30

# Fire-season start month (inclusive); codes reset on March 1 each year.
FIRE_SEASON_START_MONTH = 3

# Day-length adjustors for DMC (Le) at 46.5°N interpolated from CFFDRS tables.
# Index 0 = January … 11 = December.
DMC_LE = [6.5, 7.5, 9.0, 12.8, 13.9, 13.9, 12.4, 10.9, 9.4, 8.0, 7.0, 6.0]

# Day-length adjustors for DC (Lf) at 45°N from Van Wagner (1987) Table 3.
# Index 0 = January … 11 = December.
DC_LF = [-1.6, -1.6, -1.6, 0.9, 3.8, 5.8, 6.4, 5.0, 2.4, 0.4, -1.6, -1.6]

# Minimum column coverage required to include a station-variable in the PCA
# matrix (proportion of non-NaN values over the overlap window).
PCA_MIN_COV = 0.70

# Bootstrap and uncertainty parameters.
BOOTSTRAP_N    = 1000
BLOCK_SIZE_H   = 168   # 1 week of hourly observations (preserves autocorrelation)

# Moderate FWI validation tolerance levels (RMSE thresholds for pass/review/fail).
FWI_TOL = {"FFMC": 5.0, "DMC": 10.0, "DC": 20.0}

# ECCC Climate Data Online reference station for PEI (Charlottetown A).
ECCC_REF_STATION_ID = 50620
ECCC_CDO_URL = (
    "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
    "?format=csv&stationID={sid}&Year={year}&Month={month}"
    "&timeframe=1"   # 1 = hourly
)


# ---------------------------------------------------------------------------
# 2. Input loading and Phase 3 handoff validation
# ---------------------------------------------------------------------------

def validate_phase4_handoff(
    hourly_df: pd.DataFrame,
    recs_df: pd.DataFrame,
    fwi_ready_df: pd.DataFrame,
) -> dict:
    """
    Verify that the Phase 3 artifacts meet the minimum contract for Phase 4.

    Checks:
    - Hourly data contains required columns and the overlap window.
    - Recommendation table contains required columns and expected usability values.
    - FWI readiness table contains CAV and GRE for the four FWI inputs.

    Returns a summary dict with pass/warn flags and row counts.
    """
    result: dict = {"warnings": []}

    # --- Hourly artifact checks ---
    req_cols = ["station_code", "variable_name_std", "value", "timestamp_utc",
                "quality_flag_scrub"]
    missing = [c for c in req_cols if c not in hourly_df.columns]
    if missing:
        result["warnings"].append(f"hourly missing columns: {missing}")

    overlap_mask = (
        (hourly_df["timestamp_utc"] >= OVERLAP_START)
        & (hourly_df["timestamp_utc"] <= OVERLAP_END)
    )
    result["hourly_total_rows"]   = len(hourly_df)
    result["hourly_overlap_rows"] = int(overlap_mask.sum())
    if result["hourly_overlap_rows"] == 0:
        result["warnings"].append("No hourly rows in overlap window.")

    # --- Recommendation table checks ---
    req_rec_cols = ["station_code", "variable_name_std", "recommendation"]
    missing_rec = [c for c in req_rec_cols if c not in recs_df.columns]
    if missing_rec:
        result["warnings"].append(f"recommendations missing columns: {missing_rec}")

    usable_count = recs_df["recommendation"].isin(
        ["usable", "usable-with-caveat"]
    ).sum()
    result["approved_pairs_count"] = int(usable_count)

    # --- FWI readiness checks ---
    for station in ["CAV", "GRE"]:
        for var in FWI_REQUIRED_VARS:
            row = fwi_ready_df[
                (fwi_ready_df["station_code"] == station)
                & (fwi_ready_df["variable_name_std"] == var)
            ]
            if row.empty:
                result["warnings"].append(
                    f"FWI readiness missing: {station}/{var}"
                )

    if result["warnings"]:
        for w in result["warnings"]:
            logger.warning("[Phase4 handoff] %s", w)
    else:
        logger.info("[Phase4 handoff] All checks passed.")

    logger.info(
        "  Hourly rows: %d  |  overlap rows: %d  |  approved pairs: %d",
        result["hourly_total_rows"],
        result["hourly_overlap_rows"],
        result["approved_pairs_count"],
    )
    return result


# ---------------------------------------------------------------------------
# 3. Analysis-ready hourly matrix assembly
# ---------------------------------------------------------------------------

def load_approved_pairs(recs_df: pd.DataFrame) -> set:
    """
    Build the set of (station_code, variable_name_std) pairs approved for
    primary analysis.  Includes 'usable' and 'usable-with-caveat' pairs.
    """
    approved = recs_df[
        recs_df["recommendation"].isin(["usable", "usable-with-caveat"])
    ][["station_code", "variable_name_std"]]
    pair_set = set(map(tuple, approved.values))
    logger.info("Approved station-variable pairs: %d", len(pair_set))
    return pair_set


def add_wind_direction_deg_sta(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert STA's wind_direction_10s_deg to wind_direction_deg by multiplying
    by 10, and add the result as new rows so the standard variable name is
    available for benchmarking.  Operates non-destructively on a copy.
    """
    sta_10s = hourly_df[
        (hourly_df["station_code"] == BENCHMARK_REF)
        & (hourly_df["variable_name_std"] == "wind_direction_10s_deg")
    ].copy()

    if sta_10s.empty:
        logger.warning(
            "STA wind_direction_10s_deg not found; wind_direction_deg will be "
            "absent for STA in benchmarking."
        )
        return hourly_df

    sta_10s["variable_name_std"] = "wind_direction_deg"
    sta_10s["value"] = sta_10s["value"] * 10.0
    sta_10s["unit_std"] = "°"

    out = pd.concat([hourly_df, sta_10s], ignore_index=True)
    logger.info(
        "STA wind_direction_deg derived from wind_direction_10s_deg (%d rows).",
        len(sta_10s),
    )
    return out


def assemble_hourly_matrix(
    hourly_df: pd.DataFrame,
    approved_pairs: set,
    park_stations: list[str] | None = None,
    min_cov: float = PCA_MIN_COV,
) -> pd.DataFrame:
    """
    Build the wide-form matrix required for PCA and LOO analysis.

    Workflow
    --------
    1. Filter to the common overlap window.
    2. Keep only Park-station rows (not STA) for approved candidate-core vars.
    3. Pivot to wide form:  index = timestamp_utc,
       columns = MultiIndex (station_code, variable_name_std).
    4. Drop columns with coverage < min_cov.
    5. Fill remaining NaN with column mean (mean imputation, documented).

    Returns (wide_df, coverage_report).
    """
    if park_stations is None:
        park_stations = PARK_STATIONS

    # Step 1 – overlap window + native_ok quality filter
    df = hourly_df[
        (hourly_df["timestamp_utc"] >= OVERLAP_START)
        & (hourly_df["timestamp_utc"] <= OVERLAP_END)
        & (hourly_df["station_code"].isin(park_stations))
        & (hourly_df["variable_name_std"].isin(PCA_CANDIDATE_VARS))
    ].copy()

    # Step 2 – keep only approved pairs
    df["_pair"] = list(zip(df["station_code"], df["variable_name_std"]))
    df = df[df["_pair"].isin(approved_pairs)].drop(columns=["_pair"])

    # Step 3 – pivot
    pivot = df.pivot_table(
        index="timestamp_utc",
        columns=["station_code", "variable_name_std"],
        values="value",
        aggfunc="first",
    )
    pivot.sort_index(inplace=True)

    # Step 4 – coverage filter
    cov = pivot.notna().mean()
    cols_low = cov[cov < min_cov].index.tolist()
    if cols_low:
        logger.info(
            "Dropping %d columns with coverage < %.0f%%: %s",
            len(cols_low),
            min_cov * 100,
            [(s, v) for s, v in cols_low],
        )
    pivot.drop(columns=cols_low, inplace=True, errors="ignore")

    # Step 5 – mean imputation for residual NaN
    remaining_nan = pivot.isna().sum().sum()
    if remaining_nan > 0:
        pivot.fillna(pivot.mean(), inplace=True)
        logger.info(
            "Filled %d residual NaN cells with column mean (mean imputation).",
            remaining_nan,
        )

    coverage_report = pd.DataFrame(
        {"coverage": cov, "retained": ~cov.index.isin(cols_low)}
    )
    logger.info(
        "PCA matrix assembled: %d timestamps × %d features",
        len(pivot),
        len(pivot.columns),
    )
    return pivot, coverage_report


def standardize_matrix(matrix_df: pd.DataFrame) -> tuple:
    """
    Z-score standardize the wide matrix for PCA.

    Returns (scaled_df, scaler) where scaled_df has the same
    index and MultiIndex columns as matrix_df.
    """
    scaler = StandardScaler()
    scaled_vals = scaler.fit_transform(matrix_df.values)
    scaled_df = pd.DataFrame(
        scaled_vals,
        index=matrix_df.index,
        columns=matrix_df.columns,
    )
    return scaled_df, scaler


# ---------------------------------------------------------------------------
# 4. PCA-based redundancy analysis
# ---------------------------------------------------------------------------

def run_pca(scaled_df: pd.DataFrame, n_components: int | None = None) -> dict:
    """
    Fit PCA on the standardized Park-station matrix.

    Returns a dict with:
        pca          : fitted sklearn PCA object
        scores       : DataFrame (timestamps × components)
        loadings     : DataFrame (features × components)
        explained_var: array of explained variance ratios
        cum_var      : array of cumulative explained variance
        n_components : number of components retained
    """
    if n_components is None:
        # Retain enough components to explain 95% of variance.
        pca_full = PCA().fit(scaled_df.values)
        cum = np.cumsum(pca_full.explained_variance_ratio_)
        n_components = int(np.searchsorted(cum, 0.95)) + 1
        n_components = min(n_components, scaled_df.shape[1])

    pca = PCA(n_components=n_components, random_state=42)
    scores_vals = pca.fit_transform(scaled_df.values)

    comp_names = [f"PC{i+1}" for i in range(n_components)]

    scores = pd.DataFrame(scores_vals, index=scaled_df.index, columns=comp_names)
    loadings = pd.DataFrame(
        pca.components_.T,
        index=scaled_df.columns,
        columns=comp_names,
    )
    loadings.index.names = ["station_code", "variable_name_std"]
    loadings = loadings.reset_index()

    logger.info(
        "PCA fitted: %d components explain %.1f%% of variance.",
        n_components,
        pca.explained_variance_ratio_.sum() * 100,
    )

    return {
        "pca":            pca,
        "scores":         scores,
        "loadings":       loadings,
        "explained_var":  pca.explained_variance_ratio_,
        "cum_var":        np.cumsum(pca.explained_variance_ratio_),
        "n_components":   n_components,
    }


def compute_station_contributions(
    loadings_df: pd.DataFrame,
    n_pcs: int = 3,
) -> pd.DataFrame:
    """
    Compute the mean squared loading of each station across the top n_pcs
    principal components as a proxy for its contribution to the PCA solution.

    Higher values mean the station's variables collectively drive more variance
    in the retained components.
    """
    pc_cols = [f"PC{i+1}" for i in range(n_pcs)]
    avail = [c for c in pc_cols if c in loadings_df.columns]
    contrib = (
        loadings_df.groupby("station_code")[avail]
        .apply(lambda g: (g**2).mean())
        .mean(axis=1)
        .rename("mean_sq_loading")
        .reset_index()
    )
    contrib = contrib.sort_values("mean_sq_loading", ascending=False)
    return contrib


def run_loo_pca(
    scaled_df: pd.DataFrame,
    park_stations: list[str] | None = None,
) -> pd.DataFrame:
    """
    Leave-one-station-out PCA sensitivity analysis.

    For each Park station, remove its columns, refit PCA retaining the same
    number of components as the full model, and compute:
        - var_explained_full     : total variance explained by full model (%)
        - var_explained_reduced  : total variance explained without station (%)
        - abs_var_loss           : absolute loss in explained variance (%)
        - rel_var_loss           : relative loss as fraction of full-model variance (%)
        - pc1_share_full         : PC1 share in full model (%)
        - pc1_share_reduced      : PC1 share without station (%)

    Returns a DataFrame with one row per station.
    """
    if park_stations is None:
        park_stations = PARK_STATIONS

    # Full-model baseline – use absolute eigenvalues for cross-matrix comparability.
    # explained_variance_ gives eigenvalues in variance units (not ratios), so the
    # comparison stays anchored to the full-model scale when we reduce the feature set.
    pca_full_obj = PCA(random_state=42).fit(scaled_df.values)
    cum_ratio = np.cumsum(pca_full_obj.explained_variance_ratio_)
    n_comp = min(int(np.searchsorted(cum_ratio, 0.95)) + 1, scaled_df.shape[1])

    full_captured = pca_full_obj.explained_variance_[:n_comp].sum()  # abs variance units
    full_total    = pca_full_obj.explained_variance_.sum()            # = n_features (stdized)
    full_var_pct  = float(full_captured / full_total * 100)
    full_pc1_abs  = float(pca_full_obj.explained_variance_[0])

    records = []
    for station in park_stations:
        # Drop all columns belonging to this station
        reduced = scaled_df.drop(
            columns=[c for c in scaled_df.columns if c[0] == station],
            errors="ignore",
        )
        if reduced.empty or reduced.shape[1] < 2:
            logger.warning(
                "LOO: not enough columns to refit PCA without %s; skipping.",
                station,
            )
            continue

        n_comp_r = min(n_comp, reduced.shape[1])
        pca_r = PCA(n_components=n_comp_r, random_state=42).fit(reduced.values)

        # Absolute variance captured by reduced model's top components
        red_captured = pca_r.explained_variance_.sum()
        abs_loss     = float(full_captured - red_captured)      # abs variance units
        rel_loss_pct = float(abs_loss / full_captured * 100)    # positive = loss
        red_pc1_abs  = float(pca_r.explained_variance_[0])

        records.append({
            "station":                   station,
            "full_captured_variance":    round(full_captured, 3),
            "reduced_captured_variance": round(red_captured, 3),
            "abs_var_loss":              round(abs_loss, 3),
            "rel_var_loss_pct":          round(rel_loss_pct, 3),
            "full_var_pct_explained":    round(full_var_pct, 2),
            "pc1_eigenvalue_full":       round(full_pc1_abs, 3),
            "pc1_eigenvalue_reduced":    round(red_pc1_abs, 3),
        })

    loo_df = pd.DataFrame(records).sort_values("abs_var_loss", ascending=False)
    return loo_df


# ---------------------------------------------------------------------------
# 5. Stanhope benchmark
# ---------------------------------------------------------------------------

def build_benchmark_pairs(
    hourly_df: pd.DataFrame,
    approved_pairs: set,
    bench_vars: list[str] | None = None,
) -> dict:
    """
    Build aligned hourly UTC series pairing each Park station against STA for
    each benchmark variable.

    Returns a dict keyed by (station, variable) with DataFrames containing
    columns ['timestamp_utc', 'park_value', 'sta_value'].

    Only timestamps where BOTH the Park station and STA have non-NaN,
    native_ok values are retained.
    """
    if bench_vars is None:
        bench_vars = BENCHMARK_VARS

    # Filter to overlap window and native quality
    df = hourly_df[
        (hourly_df["timestamp_utc"] >= OVERLAP_START)
        & (hourly_df["timestamp_utc"] <= OVERLAP_END)
        & (hourly_df["quality_flag_scrub"] == SCRUB_FLAG_OK)
    ].copy()

    pairs: dict = {}
    for var in bench_vars:
        sta_ser = df[
            (df["station_code"] == BENCHMARK_REF)
            & (df["variable_name_std"] == var)
        ].set_index("timestamp_utc")["value"]

        if sta_ser.empty:
            logger.warning("STA: no native_ok data for %s – skipping benchmark.", var)
            continue

        for station in PARK_STATIONS:
            if (station, var) not in approved_pairs:
                continue

            park_ser = df[
                (df["station_code"] == station)
                & (df["variable_name_std"] == var)
            ].set_index("timestamp_utc")["value"]

            if park_ser.empty:
                logger.warning(
                    "%s/%s: no data in overlap; skipping benchmark.", station, var
                )
                continue

            aligned = pd.concat(
                [park_ser.rename("park"), sta_ser.rename("sta")],
                axis=1,
                join="inner",
            ).dropna()

            if len(aligned) < 24:
                logger.warning(
                    "%s/%s: only %d shared timestamps; benchmark unreliable.",
                    station, var, len(aligned),
                )
                continue

            pairs[(station, var)] = aligned
            logger.info(
                "  Benchmark pair %s/%s: %d aligned hours.",
                station, var, len(aligned),
            )

    return pairs


def compute_benchmark_metrics(pairs: dict) -> pd.DataFrame:
    """
    Compute per station-variable benchmark metrics against STA.

    Metrics: n_pairs, pearson_r, rmse_c (for temperature) or rmse,
             mae, bias (park_mean - sta_mean), park_mean, sta_mean.
    """
    records = []
    for (station, variable), aligned in pairs.items():
        park = aligned["park"].values
        sta  = aligned["sta"].values

        corr = np.corrcoef(park, sta)[0, 1] if len(park) > 1 else np.nan
        rmse = float(np.sqrt(np.mean((park - sta) ** 2)))
        mae  = float(np.mean(np.abs(park - sta)))
        bias = float(park.mean() - sta.mean())

        records.append({
            "station":      station,
            "variable":     variable,
            "n_pairs":      len(aligned),
            "pearson_r":    round(float(corr), 4),
            "rmse":         round(rmse, 4),
            "mae":          round(mae, 4),
            "bias":         round(bias, 4),
            "park_mean":    round(float(park.mean()), 3),
            "sta_mean":     round(float(sta.mean()), 3),
        })

    return pd.DataFrame(records).sort_values(["station", "variable"])


def bootstrap_benchmark_ci(
    pairs: dict,
    n_resamples: int = BOOTSTRAP_N,
    block_size: int = BLOCK_SIZE_H,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Compute bootstrap 95% CI for RMSE and Pearson-r for each benchmark pair
    using block resampling to preserve temporal autocorrelation.

    Returns a DataFrame with columns:
        station, variable, rmse_lower, rmse_upper, r_lower, r_upper.
    """
    rng = np.random.default_rng(seed)
    records = []

    for (station, variable), aligned in pairs.items():
        n = len(aligned)
        park = aligned["park"].values
        sta  = aligned["sta"].values

        # Build block bootstrap sample indices
        block_starts = np.arange(0, n, block_size)
        rmse_boot = np.empty(n_resamples)
        r_boot    = np.empty(n_resamples)

        for b in range(n_resamples):
            chosen = rng.choice(block_starts, size=len(block_starts), replace=True)
            idx = np.concatenate([
                np.arange(s, min(s + block_size, n)) for s in chosen
            ])[:n]
            p_s = park[idx]
            s_s = sta[idx]
            rmse_boot[b] = np.sqrt(np.mean((p_s - s_s) ** 2))
            if len(p_s) > 1:
                r_boot[b] = np.corrcoef(p_s, s_s)[0, 1]
            else:
                r_boot[b] = np.nan

        records.append({
            "station":    station,
            "variable":   variable,
            "rmse_ci_lo": round(float(np.nanpercentile(rmse_boot, 2.5)), 4),
            "rmse_ci_hi": round(float(np.nanpercentile(rmse_boot, 97.5)), 4),
            "r_ci_lo":    round(float(np.nanpercentile(r_boot, 2.5)), 4),
            "r_ci_hi":    round(float(np.nanpercentile(r_boot, 97.5)), 4),
        })

    return pd.DataFrame(records).sort_values(["station", "variable"])


# ---------------------------------------------------------------------------
# 6. FWI moisture-code calculations
# ---------------------------------------------------------------------------

def _ffmc_step(F_prev: float, T: float, RH: float, W: float, rain: float) -> float:
    """
    Compute one daily FFMC step using Van Wagner (1987) formulas.

    Parameters
    ----------
    F_prev : Previous day's FFMC (0–101).
    T      : Temperature at noon local time (°C).
    RH     : Relative humidity at noon (%).
    W      : Wind speed at noon (km/h).
    rain   : 24-hour rainfall (mm).
    """
    # Clamp inputs to physical bounds
    RH = max(0.0, min(100.0, RH))
    W  = max(0.0, W)
    T  = max(-50.0, min(60.0, T))

    # Convert previous FFMC to fuel moisture content (%)
    mo = 147.2 * (101.0 - F_prev) / (59.5 + F_prev)

    # Rainfall correction
    if rain > 0.5:
        rf = rain - 0.5
        base = 42.5 * rf * exp(-100.0 / (251.0 - mo)) * (1.0 - exp(-6.93 / rf))
        if mo <= 150.0:
            mr = mo + base
        else:
            mr = mo + base + 0.0015 * (mo - 150.0) ** 2 * rf ** 0.5
        mo = min(mr, 250.0)

    # Equilibrium moisture contents for drying (Ed) and wetting (Ew)
    Ed = (0.942 * RH ** 0.679
          + 11.0 * exp((RH - 100.0) / 10.0)
          + 0.18 * (21.1 - T) * (1.0 - exp(-0.115 * RH)))

    Ew = (0.618 * RH ** 0.753
          + 10.0 * exp((RH - 100.0) / 10.0)
          + 0.18 * (21.1 - T) * (1.0 - exp(-0.115 * RH)))

    if mo > Ed:
        # Drying phase
        ko = (0.424 * (1.0 - (RH / 100.0) ** 1.7)
               + 0.0694 * W ** 0.5 * (1.0 - (RH / 100.0) ** 8))
        kd = ko * 0.463 * exp(0.0365 * T)
        m = Ed + (mo - Ed) * 10.0 ** (-kd)
    elif mo < Ew:
        # Wetting phase
        kl = (0.424 * (1.0 - ((100.0 - RH) / 100.0) ** 1.7)
               + 0.0694 * W ** 0.5 * (1.0 - ((100.0 - RH) / 100.0) ** 8))
        kw = kl * 0.463 * exp(0.0365 * T)
        m = Ew - (Ew - mo) * 10.0 ** (-kw)
    else:
        m = mo

    # Convert moisture back to FFMC scale and clamp
    F = 59.5 * (250.0 - m) / (147.2 + m)
    return max(0.0, min(101.0, F))


def _dmc_step(D_prev: float, T: float, RH: float, rain: float, month: int) -> float:
    """
    Compute one daily DMC step.

    Parameters
    ----------
    D_prev : Previous day's DMC (>= 0).
    T      : Temperature at noon (°C).
    RH     : Relative humidity at noon (%).
    rain   : 24-hour rainfall (mm).
    month  : Calendar month (1–12).
    """
    T  = max(-1.1, T)    # lower floor per CFFDRS
    RH = max(0.0, min(100.0, RH))

    Le = DMC_LE[month - 1]
    K  = 1.894 * (T + 1.1) * (100.0 - RH) * Le * 1e-4

    if rain > 1.5:
        re = 0.92 * rain - 1.27
        mo = 20.0 + exp(5.6348 - D_prev / 43.43)

        if D_prev <= 33.0:
            b = 100.0 / (0.5 + 0.3 * D_prev)
        elif D_prev <= 65.0:
            b = 14.0 - 1.3 * log(max(D_prev, 1e-9))
        else:
            b = 6.2 * log(max(D_prev, 1e-9)) - 17.2

        mr = mo + 1000.0 * re / (48.77 + b * re)
        pr = 244.72 - 43.43 * log(max(mr - 20.0, 1e-9))
        D = max(pr, 0.0) + K
    else:
        D = D_prev + K

    return max(D, 0.0)


def _dc_step(Dc_prev: float, T: float, rain: float, month: int) -> float:
    """
    Compute one daily DC step.

    Parameters
    ----------
    Dc_prev : Previous day's DC (>= 0).
    T       : Temperature at noon (°C).
    rain    : 24-hour rainfall (mm).
    month   : Calendar month (1–12).
    """
    T = max(-2.8, T)     # lower floor per CFFDRS
    Lf = DC_LF[month - 1]
    V  = max(0.0, 0.36 * (T + 2.8) + Lf)

    if rain > 2.8:
        rd = 0.83 * rain - 1.27
        Qo = 800.0 * exp(-Dc_prev / 400.0)
        Qr = Qo + 3.937 * rd
        Dr = 400.0 * log(800.0 / max(Qr, 1e-9))
        Dc = max(Dr, 0.0) + V / 2.0
    else:
        Dc = Dc_prev + V / 2.0

    return max(Dc, 0.0)


def calc_daily_fwi(
    hourly_df: pd.DataFrame,
    station: str,
) -> pd.DataFrame:
    """
    Calculate daily FFMC, DMC, and DC for one station using the 16:00 UTC
    noon observation convention.

    Temperature, RH, and wind speed are taken from the hourly entry at
    16:00 UTC each day.  Precipitation is the daily total (sum of all hourly
    values on that UTC date).

    Season restarts at March 1 each year and after any gap > RESTART_GAP_DAYS.

    Returns a DataFrame with columns:
        date, FFMC, DMC, DC, T_noon, RH_noon, W_noon, precip_daily,
        restart_flag, data_flag ('complete' | 'partial' | 'missing').
    """
    df = hourly_df[
        (hourly_df["station_code"] == station)
        & (hourly_df["variable_name_std"].isin(FWI_REQUIRED_VARS))
    ].copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["date"] = df["timestamp_utc"].dt.date

    # Extract noon observations at FWI_OBS_HOUR_UTC (16:00 UTC)
    noon_df = df[df["timestamp_utc"].dt.hour == FWI_OBS_HOUR_UTC]
    noon_wide = noon_df.pivot_table(
        index="date",
        columns="variable_name_std",
        values="value",
        aggfunc="first",
    )

    # Daily precipitation sum (all hours, not just noon)
    precip_daily = (
        df[df["variable_name_std"] == "precipitation_mm"]
        .groupby("date")["value"]
        .sum()
    )

    # Build a complete date range over the station record
    all_dates = pd.date_range(
        start=pd.Timestamp(df["date"].min()),
        end=pd.Timestamp(df["date"].max()),
        freq="D",
    ).date

    records = []
    F = FFMC_DEFAULT
    D = DMC_DEFAULT
    Dc = DC_DEFAULT
    prev_date = None

    for dt in all_dates:
        month = dt.month

        # Annual fire-season restart on March 1
        if dt.month == FIRE_SEASON_START_MONTH and dt.day == 1:
            F, D, Dc = FFMC_DEFAULT, DMC_DEFAULT, DC_DEFAULT
            restart = True
        else:
            restart = False

        # Gap-triggered restart
        if prev_date is not None:
            gap_days = (pd.Timestamp(dt) - pd.Timestamp(prev_date)).days
            if gap_days > RESTART_GAP_DAYS:
                F, D, Dc = FFMC_DEFAULT, DMC_DEFAULT, DC_DEFAULT
                restart = True
                logger.info(
                    "%s: FWI restart on %s due to %d-day gap.", station, dt, gap_days
                )

        # Retrieve noon observations for this day
        T_noon  = noon_wide.at[dt, "air_temperature_c"]     if dt in noon_wide.index else np.nan
        RH_noon = noon_wide.at[dt, "relative_humidity_pct"] if dt in noon_wide.index else np.nan
        W_noon  = noon_wide.at[dt, "wind_speed_kmh"]        if dt in noon_wide.index else np.nan
        precip  = precip_daily.get(dt, np.nan)
        precip  = 0.0 if np.isnan(precip) else precip

        # Classify data completeness for the day
        if all(not np.isnan(x) for x in [T_noon, RH_noon, W_noon]):
            data_flag = "complete"
        elif any(not np.isnan(x) for x in [T_noon, RH_noon, W_noon]):
            data_flag = "partial"
        else:
            data_flag = "missing"

        if data_flag != "missing":
            # For partial rows, substitute missing values with defaults
            T_noon  = T_noon  if not np.isnan(T_noon)  else 10.0
            RH_noon = RH_noon if not np.isnan(RH_noon) else 50.0
            W_noon  = W_noon  if not np.isnan(W_noon)  else 10.0

            F  = _ffmc_step(F,  T_noon, RH_noon, W_noon, precip)
            D  = _dmc_step(D,   T_noon, RH_noon, precip, month)
            Dc = _dc_step(Dc,   T_noon, precip, month)

        records.append({
            "date":         dt,
            "FFMC":         round(F,  2) if data_flag != "missing" else np.nan,
            "DMC":          round(D,  2) if data_flag != "missing" else np.nan,
            "DC":           round(Dc, 2) if data_flag != "missing" else np.nan,
            "T_noon_utc16": round(T_noon,  2) if data_flag == "complete" else np.nan,
            "RH_noon_utc16": round(RH_noon, 2) if data_flag == "complete" else np.nan,
            "W_noon_utc16": round(W_noon,  2) if data_flag == "complete" else np.nan,
            "precip_daily_mm": round(precip, 3),
            "restart_flag": restart,
            "data_flag":    data_flag,
        })

        prev_date = dt

    fwi_df = pd.DataFrame(records)
    fwi_df["station"] = station
    logger.info(
        "%s FWI codes computed: %d days  |  complete: %d  |  partial: %d  |  missing: %d",
        station,
        len(fwi_df),
        (fwi_df["data_flag"] == "complete").sum(),
        (fwi_df["data_flag"] == "partial").sum(),
        (fwi_df["data_flag"] == "missing").sum(),
    )
    return fwi_df


# ---------------------------------------------------------------------------
# 7. ECCC reference data fetch and reference FWI
# ---------------------------------------------------------------------------

def fetch_eccc_hourly_month(
    station_id: int,
    year: int,
    month: int,
    session: requests.Session,
    timeout: int = 30,
) -> pd.DataFrame | None:
    """
    Download one month of ECCC Climate Data Online hourly data and return
    a raw DataFrame.  Returns None on HTTP error or parse failure (logs a
    warning) so the caller can skip gracefully.
    """
    url = ECCC_CDO_URL.format(sid=station_id, year=year, month=month)
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("ECCC CDO fetch failed (%s/%s): %s", year, month, exc)
        return None

    try:
        # ECCC CDO files for hourly data use a standard CSV format.
        # Skip preamble lines by finding the header row containing 'Date/Time'.
        text_lines = resp.text.splitlines()
        header_line = next(
            (i for i, l in enumerate(text_lines) if "Date/Time" in l), None
        )
        if header_line is None:
            logger.warning("ECCC CDO: no header row found for %s/%s.", year, month)
            return None
        csv_text = "\n".join(text_lines[header_line:])
        df = pd.read_csv(StringIO(csv_text), low_memory=False)
        return df
    except Exception as exc:
        logger.warning("ECCC CDO parse error (%s/%s): %s", year, month, exc)
        return None


def fetch_eccc_fwi_reference(
    station_id: int = ECCC_REF_STATION_ID,
    year_start: int = 2023,
    year_end: int = 2025,
    obs_hour_lst: int = 12,
) -> pd.DataFrame | None:
    """
    Download ECCC hourly data for the reference station, extract noon LST
    observations, and compute reference FFMC, DMC, and DC using the same
    formulas used for CAV/GRE.

    Charlottetown A (stationID=50620) is used by default as the nearest
    long-record official ECCC climate station to the PEINP sites.

    Returns a DataFrame with columns [date, FFMC, DMC, DC, station='ECCC_REF']
    or None if the download is unavailable.

    Note: This provides ECCC-station-computed reference FWI values using the
    same formulas as CAV/GRE, not independently published pre-computed FWI
    tables.  The comparison reflects spatial meteorological differences between
    the Parks Canada sites and the ECCC reference station.
    """
    months = [
        (y, m)
        for y in range(year_start, year_end + 1)
        for m in range(1, 13)
    ]

    all_frames = []
    with requests.Session() as session:
        for year, month in months:
            df = fetch_eccc_hourly_month(station_id, year, month, session)
            if df is not None:
                all_frames.append(df)

    if not all_frames:
        logger.warning(
            "ECCC CDO: no data retrieved for station %s; "
            "FWI external validation will be skipped.",
            station_id,
        )
        return None

    raw = pd.concat(all_frames, ignore_index=True)

    # Identify the key columns: ECCC hourly CSV column names vary slightly.
    col_map = {c.strip(): c for c in raw.columns}
    date_col = col_map.get("Date/Time (LST)") or col_map.get("Date/Time")
    temp_col = col_map.get("Temp (°C)") or col_map.get("Temp (\u00b0C)")
    rh_col   = col_map.get("Rel Hum (%)")
    wind_col = col_map.get("Wind Spd (km/h)")
    precip_col = col_map.get("Precip. Amount (mm)")

    missing_key_cols = [
        n for n, c in [
            ("date", date_col), ("temp", temp_col),
            ("rh", rh_col), ("wind", wind_col),
        ] if c is None
    ]
    if missing_key_cols:
        logger.warning(
            "ECCC CDO: missing expected columns %s; skipping reference FWI.",
            missing_key_cols,
        )
        return None

    raw["_dt"] = pd.to_datetime(raw[date_col], errors="coerce")
    raw = raw.dropna(subset=["_dt"])
    raw["_hour"] = raw["_dt"].dt.hour
    raw["_date"] = raw["_dt"].dt.date
    raw["_T"]    = pd.to_numeric(raw[temp_col], errors="coerce")
    raw["_RH"]   = pd.to_numeric(raw[rh_col],   errors="coerce")
    raw["_W"]    = pd.to_numeric(raw[wind_col],  errors="coerce")
    if precip_col:
        raw["_P"] = pd.to_numeric(raw[precip_col], errors="coerce").fillna(0.0)
    else:
        raw["_P"] = 0.0

    # Extract noon LST observations
    noon = raw[raw["_hour"] == obs_hour_lst].copy()

    # Daily precipitation total
    precip_daily = raw.groupby("_date")["_P"].sum()

    # Build a synthetic "hourly_df" compatible dict for calc_daily_fwi.
    # We'll compute FWI directly here rather than calling calc_daily_fwi again.
    noon = noon.set_index("_date")

    all_dates = sorted(set(raw["_date"]))
    records = []
    F = FFMC_DEFAULT
    D = DMC_DEFAULT
    Dc = DC_DEFAULT
    prev_date = None

    for dt in all_dates:
        month = dt.month
        if dt.month == FIRE_SEASON_START_MONTH and dt.day == 1:
            F, D, Dc = FFMC_DEFAULT, DMC_DEFAULT, DC_DEFAULT

        if prev_date is not None:
            gap = (pd.Timestamp(dt) - pd.Timestamp(prev_date)).days
            if gap > RESTART_GAP_DAYS:
                F, D, Dc = FFMC_DEFAULT, DMC_DEFAULT, DC_DEFAULT

        T_n  = noon.at[dt, "_T"]  if dt in noon.index else np.nan
        RH_n = noon.at[dt, "_RH"] if dt in noon.index else np.nan
        W_n  = noon.at[dt, "_W"]  if dt in noon.index else np.nan
        P    = float(precip_daily.get(dt, 0.0))

        if all(not np.isnan(x) for x in [T_n, RH_n, W_n]):
            F  = _ffmc_step(F,  T_n, RH_n, W_n, P)
            D  = _dmc_step(D,   T_n, RH_n, P, month)
            Dc = _dc_step(Dc,   T_n, P, month)
            records.append({"date": dt, "FFMC": round(F, 2),
                             "DMC": round(D, 2), "DC": round(Dc, 2),
                             "station": "ECCC_REF"})
        prev_date = dt

    ref_fwi = pd.DataFrame(records)
    logger.info(
        "ECCC reference FWI computed: %d days from station %s.",
        len(ref_fwi), station_id,
    )
    return ref_fwi


# ---------------------------------------------------------------------------
# 8. FWI validation
# ---------------------------------------------------------------------------

def validate_fwi_codes(
    computed_dict: dict,
    reference_df: pd.DataFrame | None,
    tol: dict | None = None,
) -> pd.DataFrame:
    """
    Validate computed FWI codes for CAV/GRE against:
    (a) STA-computed FWI (spatial reference from the Stanhope ECCC station
        processed through the same pipeline).
    (b) ECCC CDO–computed reference FWI if available.

    The 'tol' dict may override default RMSE thresholds for pass/review/fail.

    Returns a validation summary DataFrame with columns:
        station, reference, code, n_days, rmse, mae, bias,
        rmse_threshold, outcome ('pass' | 'review' | 'fail'),
        season_coverage_pct.
    """
    if tol is None:
        tol = FWI_TOL

    codes = ["FFMC", "DMC", "DC"]
    records = []

    # Build reference comparison sources
    refs: list[tuple[str, pd.DataFrame]] = []
    if "STA" in computed_dict:
        refs.append(("STA", computed_dict["STA"]))
    if reference_df is not None:
        refs.append(("ECCC_REF", reference_df))

    for station in ["CAV", "GRE"]:
        if station not in computed_dict:
            continue
        comp = computed_dict[station][
            computed_dict[station]["data_flag"] == "complete"
        ][["date", "FFMC", "DMC", "DC"]].set_index("date")

        for ref_label, ref_df in refs:
            if isinstance(ref_df, pd.DataFrame) and "date" in ref_df.columns:
                ref = ref_df[["date", "FFMC", "DMC", "DC"]].dropna(
                    subset=["FFMC", "DMC", "DC"]
                ).set_index("date")
            else:
                continue

            # Align on shared dates
            shared = comp.index.intersection(ref.index)
            if len(shared) < 10:
                logger.warning(
                    "FWI validation %s vs %s: only %d shared days; skipping.",
                    station, ref_label, len(shared),
                )
                continue

            for code in codes:
                c_vals = comp.loc[shared, code].values
                r_vals = ref.loc[shared, code].values
                rmse_ = float(np.sqrt(np.mean((c_vals - r_vals) ** 2)))
                mae_  = float(np.mean(np.abs(c_vals - r_vals)))
                bias_ = float(c_vals.mean() - r_vals.mean())
                thresh = tol.get(code, 999)
                outcome = (
                    "pass"   if rmse_ <= thresh else
                    "review" if rmse_ <= thresh * 1.5 else
                    "fail"
                )
                records.append({
                    "station":             station,
                    "reference":           ref_label,
                    "fwi_code":            code,
                    "n_shared_days":       len(shared),
                    "rmse":                round(rmse_, 3),
                    "mae":                 round(mae_,  3),
                    "bias":                round(bias_, 3),
                    "rmse_threshold":      thresh,
                    "outcome":             outcome,
                })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 9. Block-bootstrap station-removal risk
# ---------------------------------------------------------------------------

def _pca_var_loss(scaled_df: pd.DataFrame, remove_station: str, n_comp: int) -> float:
    """
    Compute the fraction of absolute variance captured by the top n_comp PCA
    components that is lost when one station's features are removed.

    Uses ABSOLUTE eigenvalues (explained_variance_) rather than ratios so the
    comparison is anchored to the full-model total and yields positive loss
    values regardless of the feature-count change.

    Returns a value in [0, 1] (fraction of full-model captured variance lost).
    """
    pca_full = PCA(n_components=n_comp, random_state=42).fit(scaled_df.values)
    full_captured = pca_full.explained_variance_.sum()   # absolute units

    reduced = scaled_df.drop(
        columns=[c for c in scaled_df.columns if c[0] == remove_station],
        errors="ignore",
    )
    if reduced.shape[1] < 2:
        return 1.0  # degenerate: treating full loss

    n_comp_r = min(n_comp, reduced.shape[1])
    red_captured = PCA(n_components=n_comp_r, random_state=42).fit(
        reduced.values
    ).explained_variance_.sum()   # absolute units

    # Loss as fraction of the full-model captured variance
    return max(0.0, (full_captured - red_captured) / full_captured)


def bootstrap_station_removal_risk(
    scaled_df: pd.DataFrame,
    park_stations: list[str] | None = None,
    n_resamples: int = BOOTSTRAP_N,
    block_size: int = BLOCK_SIZE_H,
    seed: int = 42,
) -> dict:
    """
    Estimate the distribution of PCA variance loss when each station is
    removed, using block-bootstrap resampling of the time axis.

    Uses the number of components required to explain 95% variance in the
    full model as the fixed reference dimensionality across all resamples.

    Returns a dict keyed by station_code with arrays of loss-fraction values.
    """
    if park_stations is None:
        park_stations = PARK_STATIONS

    rng = np.random.default_rng(seed)
    n = len(scaled_df)

    # Fix the reference number of components from the full dataset
    pca_full = PCA(random_state=42).fit(scaled_df.values)
    cum = np.cumsum(pca_full.explained_variance_ratio_)
    n_comp = min(int(np.searchsorted(cum, 0.95)) + 1, scaled_df.shape[1])

    logger.info(
        "Bootstrap removal risk: n=%d, blocks=%d, n_resamples=%d, n_comp=%d",
        n, block_size, n_resamples, n_comp,
    )

    block_starts = np.arange(0, n, block_size)
    results: dict = {s: np.empty(n_resamples) for s in park_stations}

    for b in range(n_resamples):
        chosen = rng.choice(block_starts, size=len(block_starts), replace=True)
        idx = np.concatenate([
            np.arange(s, min(s + block_size, n)) for s in chosen
        ])[:n]
        boot_df = scaled_df.iloc[idx].reset_index(drop=True)

        # Fit full-model PCA once per resample; share across all station removals.
        pca_b = PCA(n_components=n_comp, random_state=42).fit(boot_df.values)
        full_captured = pca_b.explained_variance_.sum()   # absolute variance units

        for station in park_stations:
            reduced = boot_df.drop(
                columns=[c for c in boot_df.columns if c[0] == station],
                errors="ignore",
            )
            if reduced.shape[1] < 2:
                results[station][b] = 1.0
                continue
            n_comp_r = min(n_comp, reduced.shape[1])
            red_captured = PCA(n_components=n_comp_r, random_state=42).fit(
                reduced.values
            ).explained_variance_.sum()
            results[station][b] = max(
                0.0, (full_captured - red_captured) / full_captured
            )

    return results


def fit_kde_risk_summary(removal_losses: dict, threshold: float = 0.05) -> pd.DataFrame:
    """
    Fit a KDE to each station's bootstrap loss distribution and compute
    risk summary statistics.

    threshold : fraction-of-explained-variance loss above which removal is
                considered material (default 5%).

    Returns a DataFrame with columns:
        station, loss_mean, loss_median, loss_p05, loss_p95,
        prob_above_threshold, risk_label.
    """
    records = []
    for station, losses in removal_losses.items():
        losses_clean = losses[np.isfinite(losses)]
        if len(losses_clean) < 10:
            continue

        # Guard: if all bootstrap samples are identical, KDE covariance is singular.
        # Fall back to hard-threshold probability without KDE.
        if losses_clean.std() < 1e-9:
            prob_above = float(1.0 if float(losses_clean.mean()) > threshold else 0.0)
        else:
            kde = gaussian_kde(losses_clean)
            x_grid = np.linspace(losses_clean.min(), losses_clean.max(), 500)
            cdf_vals = np.array([
                kde.integrate_box_1d(-np.inf, xi) for xi in x_grid
            ])
            prob_above = float(1.0 - np.interp(threshold, x_grid, cdf_vals))

        if prob_above < 0.10:
            risk_label = "low"
        elif prob_above < 0.40:
            risk_label = "moderate"
        else:
            risk_label = "high"

        records.append({
            "station":             station,
            "loss_mean":           round(float(losses_clean.mean()), 4),
            "loss_median":         round(float(np.median(losses_clean)), 4),
            "loss_p05":            round(float(np.percentile(losses_clean, 5)), 4),
            "loss_p95":            round(float(np.percentile(losses_clean, 95)), 4),
            "prob_above_threshold": round(prob_above, 4),
            "threshold_used":      threshold,
            "risk_label":          risk_label,
        })

    return pd.DataFrame(records).sort_values("loss_mean", ascending=False)


# ---------------------------------------------------------------------------
# 10. Network-optimization recommendation synthesis
# ---------------------------------------------------------------------------

def synthesize_recommendations(
    loo_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    kde_summary: pd.DataFrame,
    fwi_validation: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Combine PCA LOO loss, Stanhope benchmark performance, and KDE removal risk
    into per-station network-optimization recommendations.

    Classification rules
    --------------------
    retain                : KDE risk=high OR LOO abs_var_loss > 10% OR
                            station is the unique FWI reference site.
    retain-with-caveat    : KDE risk=moderate AND LOO abs_var_loss 5–10%.
    candidate-consolidation: KDE risk=low AND LOO abs_var_loss < 5% AND
                              avg benchmark RMSE vs STA is below median.
    do-not-remove         : Greenwich (GRE) – retained regardless because of
                              unique coastal micro-climate coverage.

    Returns a DataFrame with columns:
        station, loo_abs_var_loss_pct, avg_benchmark_rmse, kde_risk_label,
        recommendation, reason.
    """
    records = []

    # Summarise benchmark: average RMSE across variables per Park station.
    avg_rmse = (
        benchmark_df.groupby("station")["rmse"]
        .mean()
        .rename("avg_benchmark_rmse")
        .reset_index()
        .set_index("station")
    )
    median_rmse = avg_rmse["avg_benchmark_rmse"].median()

    for station in PARK_STATIONS:
        # LOO variance loss
        loo_row = loo_df[loo_df["station"] == station]
        loo_loss = float(loo_row["rel_var_loss_pct"].values[0]) if not loo_row.empty else np.nan

        # Benchmark RMSE
        avg_bm = float(avg_rmse.at[station, "avg_benchmark_rmse"]) if station in avg_rmse.index else np.nan

        # KDE risk label
        kde_row = kde_summary[kde_summary["station"] == station]
        kde_risk = kde_row["risk_label"].values[0] if not kde_row.empty else "unknown"

        # Apply classification rules
        if station == "GRE":
            # GRE is always retained: unique coastal micro-climate and FWI station.
            rec = "do-not-remove"
            reason = "Unique coastal micro-climate; retained as FWI computation site."
        elif kde_risk == "high" or (not np.isnan(loo_loss) and loo_loss > 10.0):
            rec = "retain"
            reason = (
                f"KDE risk={kde_risk} and/or LOO variance loss={loo_loss:.1f}% >10%."
            )
        elif kde_risk == "moderate" or (not np.isnan(loo_loss) and 5.0 <= loo_loss <= 10.0):
            rec = "retain-with-caveat"
            reason = (
                f"Moderate removal risk (KDE={kde_risk}, LOO loss={loo_loss:.1f}%). "
                "Monitor data quality; retain pending additional seasonal evidence."
            )
        elif (
            kde_risk == "low"
            and (np.isnan(loo_loss) or loo_loss < 5.0)
            and (np.isnan(avg_bm) or avg_bm <= median_rmse)
        ):
            rec = "candidate-consolidation"
            reason = (
                f"Low KDE risk, LOO loss={loo_loss:.1f}% <5%, "
                f"benchmark RMSE={avg_bm:.3f} ≤ median. "
                "Station may be consolidation candidate; confirm with additional "
                "seasonal coverage before making a final decision."
            )
        else:
            rec = "retain-with-caveat"
            reason = (
                f"KDE risk={kde_risk}, LOO loss={loo_loss:.1f}%, "
                f"benchmark RMSE={avg_bm:.3f}. Insufficient evidence to remove."
            )

        records.append({
            "station":                station,
            "loo_abs_var_loss_pct":   round(loo_loss, 2) if not np.isnan(loo_loss) else None,
            "avg_benchmark_rmse":     round(avg_bm, 4) if not np.isnan(avg_bm) else None,
            "kde_risk_label":         kde_risk,
            "recommendation":         rec,
            "reason":                 reason,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 11. Figures
# ---------------------------------------------------------------------------

def plot_scree(pca_result: dict, figpath: Path) -> None:
    """
    Plot the PCA scree curve (explained variance per component) plus
    the cumulative explained variance line.
    """
    ev  = pca_result["explained_var"] * 100
    cum = pca_result["cum_var"] * 100
    n   = len(ev)
    pcs = list(range(1, n + 1))

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.bar(pcs, ev, color="steelblue", alpha=0.75, label="Per-component")
    ax1.set_xlabel("Principal Component")
    ax1.set_ylabel("Explained Variance (%)")
    ax1.set_xticks(pcs)

    ax2 = ax1.twinx()
    ax2.plot(pcs, cum, "o-", color="firebrick", linewidth=1.5, label="Cumulative")
    ax2.axhline(y=95, color="firebrick", linestyle="--", linewidth=0.8, alpha=0.6)
    ax2.set_ylabel("Cumulative Explained Variance (%)")
    ax2.set_ylim(0, 110)

    lines1, lbl1 = ax1.get_legend_handles_labels()
    lines2, lbl2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, lbl1 + lbl2, loc="center right")
    ax1.set_title("PCA Scree Plot – Park Stations Common Matrix")
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


def plot_pca_biplot(pca_result: dict, figpath: Path) -> None:
    """
    PC1-vs-PC2 biplot showing loading vectors (variables) and station colour-
    coded score centroids (mean PC1/PC2 per station).

    Loading vectors are scaled to be visible alongside the score distribution.
    """
    loadings = pca_result["loadings"]
    scores   = pca_result["scores"]

    fig, ax = plt.subplots(figsize=(10, 8))

    # Station score centroids
    stations = loadings["station_code"].unique()
    palette  = sns.color_palette("tab10", n_colors=len(stations))
    for i, stn in enumerate(sorted(stations)):
        stn_col = [
            c for c in scores.columns
            if c in loadings.loc[
                loadings["station_code"] == stn, "variable_name_std"
            ].values
        ]
        # Use overall score centroid as representative point for each station
        # (scores are global, not per-station — mark the mean highlight)
        ax.scatter(
            scores["PC1"].mean(), scores["PC2"].mean(),
            color=palette[i], marker="*", s=200, zorder=5,
        )

    # Loading vectors
    scale = scores[["PC1", "PC2"]].abs().max().max() * 0.6
    for _, row in loadings.iterrows():
        lx = float(row["PC1"]) * scale
        ly = float(row["PC2"]) * scale
        ax.annotate(
            "",
            xy=(lx, ly), xytext=(0, 0),
            arrowprops=dict(arrowstyle="->", color="grey", lw=1.0),
        )
        ax.text(lx * 1.05, ly * 1.05, f"{row['station_code']}:{row['variable_name_std']}",
                fontsize=6, color="dimgray")

    ax.axhline(0, color="lightgray", linewidth=0.5)
    ax.axvline(0, color="lightgray", linewidth=0.5)
    ax.set_xlabel(f"PC1 ({pca_result['explained_var'][0]*100:.1f}%)")
    ax.set_ylabel(f"PC2 ({pca_result['explained_var'][1]*100:.1f}%)")
    ax.set_title("PCA Biplot – Loading Vectors, Park Stations")
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


def plot_benchmark_heatmap(benchmark_df: pd.DataFrame, figpath: Path) -> None:
    """
    Heatmap of RMSE for each Park-station × benchmark-variable combination.
    Darker colour = larger RMSE (lower agreement with Stanhope).
    """
    if benchmark_df.empty:
        logger.warning("Benchmark table empty; skipping heatmap.")
        return

    pivot = benchmark_df.pivot(index="station", columns="variable", values="rmse")
    fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns) * 1.4), 5))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".3f",
        cmap="YlOrRd",
        ax=ax,
        cbar_kws={"label": "RMSE vs Stanhope"},
    )
    ax.set_title("Stanhope Benchmark – RMSE by Station and Variable")
    ax.set_xlabel("Variable")
    ax.set_ylabel("Park Station")
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


def plot_removal_risk(
    removal_losses: dict,
    kde_summary: pd.DataFrame,
    figpath: Path,
    threshold: float = 0.05,
) -> None:
    """
    KDE density curves for PCA variance-loss under station removal.
    One curve per Park station, coloured by risk label.
    Vertical dashed line marks the materiality threshold.
    """
    risk_colours = {"low": "steelblue", "moderate": "darkorange", "high": "firebrick",
                    "unknown": "grey"}
    fig, ax = plt.subplots(figsize=(10, 6))

    for station, losses in removal_losses.items():
        losses_clean = losses[np.isfinite(losses)]
        if len(losses_clean) < 10:
            continue

        risk_row = kde_summary[kde_summary["station"] == station]
        risk  = risk_row["risk_label"].values[0] if not risk_row.empty else "unknown"
        colour = risk_colours.get(risk, "grey")

        kde = gaussian_kde(losses_clean)
        x_g = np.linspace(max(0, losses_clean.min() - 0.01),
                           losses_clean.max() + 0.01, 300)
        ax.plot(x_g, kde(x_g), label=f"{station} ({risk})", color=colour, linewidth=1.8)

    ax.axvline(threshold, color="black", linestyle="--", linewidth=1.0,
               label=f"Threshold = {threshold*100:.0f}%")
    ax.set_xlabel("Fraction of Explained Variance Lost")
    ax.set_ylabel("KDE Density")
    ax.set_title("Station-Removal Risk: Bootstrap KDE of PCA Variance Loss")
    ax.legend()
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


def plot_fwi_timeseries(
    computed_dict: dict,
    figpath: Path,
) -> None:
    """
    Three-panel time series of FFMC, DMC, and DC for CAV, GRE, and STA.
    Only shows 'complete' data-flag rows.
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    codes = ["FFMC", "DMC", "DC"]
    colours = {"CAV": "steelblue", "GRE": "darkorange", "STA": "grey"}

    for ax, code in zip(axes, codes):
        for station, fwi_df in computed_dict.items():
            sub = fwi_df[fwi_df["data_flag"] == "complete"].copy()
            sub["date"] = pd.to_datetime(sub["date"])
            ax.plot(sub["date"], sub[code],
                    label=station, color=colours.get(station, "black"),
                    linewidth=0.9, alpha=0.85)
        ax.set_ylabel(code)
        ax.legend(fontsize=8)

    axes[-1].set_xlabel("Date")
    axes[0].set_title("Daily FWI Moisture Codes – CAV, GRE, STA")
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


def plot_fwi_validation(
    validation_df: pd.DataFrame,
    figpath: Path,
) -> None:
    """
    Bar chart of RMSE by station × FWI code, with outcome colour coding.
    Pass = green, Review = orange, Fail = red.
    """
    if validation_df.empty:
        logger.warning("FWI validation table empty; skipping figure.")
        return

    colour_map = {"pass": "#5cb85c", "review": "#f0ad4e", "fail": "#d9534f"}
    fig, ax = plt.subplots(figsize=(10, 5))

    x_labels = [
        f"{r['station']}/{r['reference']}/{r['fwi_code']}"
        for _, r in validation_df.iterrows()
    ]
    colours = [colour_map.get(r["outcome"], "grey") for _, r in validation_df.iterrows()]
    ax.barh(x_labels, validation_df["rmse"].values, color=colours, alpha=0.85)
    ax.set_xlabel("RMSE")
    ax.set_title("FWI Validation – RMSE vs Reference (green=pass, orange=review, red=fail)")
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


def plot_loo_bar(loo_df: pd.DataFrame, figpath: Path) -> None:
    """
    Horizontal bar chart of absolute variance loss per station in the LOO
    PCA analysis, colour-coded by the KDE risk tier thresholds.
    """
    if loo_df.empty:
        logger.warning("LOO table empty; skipping figure.")
        return

    colours = []
    for _, row in loo_df.iterrows():
        loss = row["rel_var_loss_pct"]
        if loss > 10:
            colours.append("#d9534f")   # high
        elif loss >= 5:
            colours.append("#f0ad4e")   # moderate
        else:
            colours.append("#5cb85c")   # low

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(loo_df["station"], loo_df["rel_var_loss_pct"], color=colours, alpha=0.85)
    ax.axvline(5, color="darkorange", linestyle="--", linewidth=0.8)
    ax.axvline(10, color="firebrick", linestyle="--", linewidth=0.8)
    ax.set_xlabel("Relative Variance Loss (% of full-model captured)")
    ax.set_title("Leave-One-Out PCA: Relative Variance Loss per Station")
    fig.tight_layout()
    fig.savefig(figpath, dpi=150)
    plt.close(fig)
    logger.info("Saved: %s", figpath)


# ---------------------------------------------------------------------------
# 12. Output writers
# ---------------------------------------------------------------------------

def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Wrote %s  (%d rows)", path, len(df))
