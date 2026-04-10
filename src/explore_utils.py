"""
explore_utils.py – Phase 3: Exploration Diagnostic Helpers
===========================================================
Reusable functions for generating Phase 3 diagnostic tables and figures
from the Phase 2 scrubbed artifacts.  All analysis is strictly read-only
and diagnostic; no PCA, benchmarking metrics, or FWI calculations are
performed here.

Imports from scrub_utils
  - Quality-flag vocabulary constants (SCRUB_FLAG_*)
  - Variable configuration constants (NO_INTERP_VARS, RANGE_LIMITS,
    DAILY_AGG_RULES)

Authored for the Parks Canada OSEM pipeline – Phase 3.
"""

import logging
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Import the authoritative Phase 2 vocabulary so Phase 3 uses identical labels.
# sys.path is extended in 03_explore.py before this module is imported.
from scrub_utils import (
    DAILY_AGG_RULES,
    NO_INTERP_VARS,
    RANGE_LIMITS,
    SCRUB_FLAG_EXCL_LONG,
    SCRUB_FLAG_INTERP_SHORT,
    SCRUB_FLAG_OK,
    SCRUB_FLAG_RANGE_FAILED,
    SCRUB_FLAG_SOURCE_FLAGGED,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Primary analysis window confirmed in Phase 2 summary.
# All Phase 4-readiness recommendations should use this range by default.
OVERLAP_START = pd.Timestamp("2023-07-25", tz="UTC")
OVERLAP_END   = pd.Timestamp("2025-11-01", tz="UTC")

# Five primary stations.  Tracadie (TRW) is present but treated as secondary.
PRIMARY_STATIONS = ["CAV", "GRE", "NRW", "SBW", "STA"]
ALL_STATIONS     = ["CAV", "GRE", "NRW", "SBW", "STA", "TRW"]

# Core atmospheric variables suitable for cross-station PCA/benchmarking.
# Marine-dominant and auxiliary variables are kept as secondary diagnostics.
CORE_ATMO_VARS = [
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

MARINE_VARS = [
    "water_temperature_c",
    "water_level_m",
    "water_pressure_kpa",
    "diff_pressure_kpa",
    "water_flow_ls",
]

# Variables required as daily FWI moisture-code inputs (Cavendish and Greenwich).
FWI_STATIONS     = ["CAV", "GRE"]
FWI_DAILY_VARS   = [
    "air_temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "precipitation_mm",
]


# ---------------------------------------------------------------------------
# 1. Ingestion validation
# ---------------------------------------------------------------------------

# Required columns that must be present in the Phase 2 long-form artifacts.
REQUIRED_COLUMNS = [
    "station_code",
    "station_name",
    "parser_family",
    "source_file",
    "timestamp_utc",
    "variable_name_std",
    "value",
    "unit_std",
    "quality_flag_source",
    "quality_flag_scrub",
    "imputation_flag",
    "resample_level",
]


def validate_handoff(df: pd.DataFrame, label: str) -> dict:
    """
    Check that the Phase 2 long-form artifact meets the expected contract.

    Returns a summary dict with counts, station coverage, variable coverage,
    and any missing-column warnings.  Warnings are also logged so they appear
    in the terminal even if the caller ignores the dict.

    Parameters
    ----------
    df    : Phase 2 hourly or daily DataFrame (already loaded).
    label : Short label for logging context, e.g. "hourly" or "daily".
    """
    summary = {}

    # Column presence check
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        log.warning("[%s] Missing expected columns: %s", label, missing_cols)
    summary["missing_columns"] = missing_cols

    # Ensure timestamp_utc is datetime
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    summary["total_rows"]         = len(df)
    summary["distinct_stations"]  = sorted(df["station_code"].unique().tolist()) if "station_code" in df.columns else []
    summary["distinct_variables"] = sorted(df["variable_name_std"].unique().tolist()) if "variable_name_std" in df.columns else []
    summary["n_stations"]         = len(summary["distinct_stations"])
    summary["n_variables"]        = len(summary["distinct_variables"])

    # Date ranges per station
    if "timestamp_utc" in df.columns and "station_code" in df.columns:
        ranges = (
            df.groupby("station_code")["timestamp_utc"]
            .agg(["min", "max"])
            .rename(columns={"min": "first_ts", "max": "last_ts"})
        )
        summary["station_date_ranges"] = ranges

    # Common-overlap subset size
    if "timestamp_utc" in df.columns:
        overlap_mask = (df["timestamp_utc"] >= OVERLAP_START) & (df["timestamp_utc"] <= OVERLAP_END)
        summary["overlap_rows"] = int(overlap_mask.sum())

    # Quality flag distribution check
    if "quality_flag_scrub" in df.columns:
        summary["flag_counts"] = df["quality_flag_scrub"].value_counts().to_dict()

    log.info(
        "[%s] Handoff validated: %d rows, %d stations, %d variables, %d overlap rows",
        label,
        summary["total_rows"],
        summary["n_stations"],
        summary["n_variables"],
        summary.get("overlap_rows", 0),
    )
    return summary


# ---------------------------------------------------------------------------
# 2. Station-variable fitness tables
# ---------------------------------------------------------------------------

def _longest_consecutive(series: pd.Series, condition: bool) -> int:
    """
    Return the length of the longest consecutive run in *series* where
    the boolean mask *condition* is True (or False when condition=False).

    Parameters
    ----------
    series    : Boolean Series (True = value present and valid).
    condition : Which state to measure (True = valid run, False = gap run).
    """
    mask = series == condition
    if not mask.any():
        return 0
    # Count run lengths using cumsum trick
    run_ids = (mask != mask.shift()).cumsum()
    run_lengths = mask.groupby(run_ids).transform("sum")
    return int(run_lengths[mask].max())


def build_fitness_table(hourly_df: pd.DataFrame, overlap_only: bool = False) -> pd.DataFrame:
    """
    Build a per (station_code, variable_name_std) fitness table suitable
    for Phase 4 readiness decisions.

    Columns produced
    ----------------
    station_code, variable_name_std, total_obs, valid_obs,
    pct_complete, pct_flagged, pct_interpolated, pct_excl_long,
    first_ts, last_ts, longest_valid_run_h, longest_missing_run_h,
    in_overlap_window, category

    Parameters
    ----------
    hourly_df    : Phase 2 hourly long-form DataFrame.
    overlap_only : If True, filter to the common-overlap window before analysis.
    """
    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    if overlap_only:
        df = df[(df["timestamp_utc"] >= OVERLAP_START) & (df["timestamp_utc"] <= OVERLAP_END)].copy()

    records = []
    for (station, variable), grp in df.groupby(["station_code", "variable_name_std"], sort=True):
        grp = grp.sort_values("timestamp_utc")

        n_total = len(grp)
        n_valid = int(grp["value"].notna().sum())
        n_miss  = n_total - n_valid
        pct_complete = round(100.0 * n_valid / n_total, 2) if n_total > 0 else 0.0

        # Flagged rows: anything other than native_ok
        n_flagged = int((grp["quality_flag_scrub"] != SCRUB_FLAG_OK).sum())
        pct_flagged = round(100.0 * n_flagged / n_total, 2) if n_total > 0 else 0.0

        n_interp = int((grp["quality_flag_scrub"] == SCRUB_FLAG_INTERP_SHORT).sum())
        pct_interp = round(100.0 * n_interp / n_total, 2) if n_total > 0 else 0.0

        n_excl = int((grp["quality_flag_scrub"] == SCRUB_FLAG_EXCL_LONG).sum())
        pct_excl = round(100.0 * n_excl / n_total, 2) if n_total > 0 else 0.0

        first_ts = grp["timestamp_utc"].min()
        last_ts  = grp["timestamp_utc"].max()

        # Consecutive-run statistics on the validity boolean series
        valid_bool = grp["value"].notna()
        longest_valid   = _longest_consecutive(valid_bool, True)
        longest_missing = _longest_consecutive(valid_bool, False)

        # Whether the series appears in the common-overlap period
        in_overlap = bool(
            (grp["timestamp_utc"] >= OVERLAP_START).any()
            and (grp["timestamp_utc"] <= OVERLAP_END).any()
        )

        # Variable category (atmospheric / marine / auxiliary)
        if variable in MARINE_VARS:
            category = "marine"
        elif variable in ["battery_v", "aux_air_temperature_c", "weather_desc",
                          "humidex", "wind_chill"]:
            category = "auxiliary"
        else:
            category = "atmospheric"

        records.append({
            "station_code":           station,
            "variable_name_std":      variable,
            "total_obs":              n_total,
            "valid_obs":              n_valid,
            "pct_complete":           pct_complete,
            "pct_flagged":            pct_flagged,
            "pct_interpolated":       pct_interp,
            "pct_excl_long":          pct_excl,
            "first_ts":               first_ts,
            "last_ts":                last_ts,
            "longest_valid_run_h":    longest_valid,
            "longest_missing_run_h":  longest_missing,
            "in_overlap_window":      in_overlap,
            "category":               category,
        })

    return pd.DataFrame(records)


def build_station_summary(fitness_df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a station-level rollup from the fitness table.

    Columns produced
    ----------------
    station_code, n_variables, n_atmo_vars, n_marine_vars,
    median_pct_complete_atmo, median_pct_complete_marine,
    mean_pct_excl_long_atmo, recommended_status
    """
    records = []
    for station, grp in fitness_df.groupby("station_code"):
        atmo  = grp[grp["category"] == "atmospheric"]
        marine = grp[grp["category"] == "marine"]

        med_atmo   = round(atmo["pct_complete"].median(), 1) if not atmo.empty else np.nan
        med_marine = round(marine["pct_complete"].median(), 1) if not marine.empty else np.nan
        mean_excl  = round(atmo["pct_excl_long"].mean(), 1) if not atmo.empty else np.nan

        # Conservative recommended status based on atmospheric completeness
        # 70 %+ → primary; 50–70 % → secondary; <50 % → diagnostic-only
        if pd.isna(med_atmo):
            status = "diagnostic-only"
        elif med_atmo >= 70.0:
            status = "primary"
        elif med_atmo >= 50.0:
            status = "secondary"
        else:
            status = "diagnostic-only"

        records.append({
            "station_code":                 station,
            "n_variables":                  len(grp),
            "n_atmo_vars":                  len(atmo),
            "n_marine_vars":                len(marine),
            "median_pct_complete_atmo":     med_atmo,
            "median_pct_complete_marine":   med_marine,
            "mean_pct_excl_long_atmo":      mean_excl,
            "recommended_status":           status,
        })

    return pd.DataFrame(records)


def build_variable_summary(fitness_df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a variable-level rollup showing coverage across stations.

    Columns produced
    ----------------
    variable_name_std, category, n_stations_present,
    n_primary_stations_present, mean_pct_complete,
    min_pct_complete, shared_across_primary,
    preliminary_inclusion_status
    """
    records = []
    for variable, grp in fitness_df.groupby("variable_name_std"):
        stations_present = set(grp["station_code"].tolist())
        primary_present  = stations_present & set(PRIMARY_STATIONS)
        category         = grp["category"].iloc[0]

        mean_pct = round(grp["pct_complete"].mean(), 1)
        min_pct  = round(grp["pct_complete"].min(), 1)

        # Preliminary inclusion classification:
        #   candidate-core      present in 4 or 5 primary stations at ≥60 % mean
        #   candidate-secondary present in 2–3 primary stations or 40–60 % mean
        #   exclude-from-primary otherwise (marine, auxiliary, sparse, never present)
        n_primary = len(primary_present)
        if category in ("marine", "auxiliary"):
            status = "exclude-from-primary"
        elif n_primary >= 4 and mean_pct >= 60.0:
            status = "candidate-core"
        elif n_primary >= 2 and mean_pct >= 40.0:
            status = "candidate-secondary"
        else:
            status = "exclude-from-primary"

        records.append({
            "variable_name_std":             variable,
            "category":                      category,
            "n_stations_present":            len(stations_present),
            "n_primary_stations_present":    n_primary,
            "mean_pct_complete":             mean_pct,
            "min_pct_complete":              min_pct,
            "shared_across_primary":         n_primary == len(PRIMARY_STATIONS),
            "preliminary_inclusion_status":  status,
        })

    df = pd.DataFrame(records)
    df = df.sort_values(["preliminary_inclusion_status", "n_primary_stations_present"],
                        ascending=[True, False])
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Quality-flag distribution table
# ---------------------------------------------------------------------------

def build_flag_distribution(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a per (station_code, variable_name_std, quality_flag_scrub) count
    table so Phase 4 can see exactly how coverage was assembled.

    Columns produced
    ----------------
    station_code, variable_name_std, quality_flag_scrub, n_rows, pct_of_total
    """
    df = hourly_df.copy()

    counts = (
        df.groupby(["station_code", "variable_name_std", "quality_flag_scrub"],
                   observed=True)
        .size()
        .reset_index(name="n_rows")
    )

    # Compute percent of total rows per (station, variable) pair
    totals = (
        df.groupby(["station_code", "variable_name_std"], observed=True)
        .size()
        .reset_index(name="total_rows")
    )
    counts = counts.merge(totals, on=["station_code", "variable_name_std"])
    counts["pct_of_total"] = (counts["n_rows"] / counts["total_rows"] * 100).round(2)
    counts = counts.drop(columns="total_rows")

    return counts.sort_values(["station_code", "variable_name_std", "quality_flag_scrub"])


# ---------------------------------------------------------------------------
# 4. Daily FWI-readiness table (Cavendish and Greenwich only)
# ---------------------------------------------------------------------------

def build_fwi_readiness(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Assess daily coverage of FWI moisture-code input variables for Cavendish
    (CAV) and Greenwich (GRE) only.  No moisture codes are calculated here.

    Columns produced
    ----------------
    station_code, variable_name_std, total_days, valid_days,
    pct_complete, n_missing_days, first_date, last_date,
    overlap_valid_days, overlap_pct_complete, fwi_input_status
    """
    df = daily_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    # Filter to FWI-relevant stations and variables
    mask = (
        df["station_code"].isin(FWI_STATIONS)
        & df["variable_name_std"].isin(FWI_DAILY_VARS)
    )
    df = df[mask].copy()

    records = []
    for (station, variable), grp in df.groupby(["station_code", "variable_name_std"]):
        grp = grp.sort_values("timestamp_utc")

        n_total = len(grp)
        n_valid = int(grp["value"].notna().sum())
        pct     = round(100.0 * n_valid / n_total, 2) if n_total > 0 else 0.0
        n_miss  = n_total - n_valid

        # Common-overlap sub-window
        ov = grp[(grp["timestamp_utc"] >= OVERLAP_START) & (grp["timestamp_utc"] <= OVERLAP_END)]
        ov_valid = int(ov["value"].notna().sum())
        ov_total = len(ov)
        ov_pct   = round(100.0 * ov_valid / ov_total, 2) if ov_total > 0 else 0.0

        # FWI input status: ready / review / not-ready
        if ov_pct >= 85.0:
            status = "ready"
        elif ov_pct >= 50.0:
            status = "review"
        else:
            status = "not-ready"

        records.append({
            "station_code":          station,
            "variable_name_std":     variable,
            "total_days":            n_total,
            "valid_days":            n_valid,
            "pct_complete":          pct,
            "n_missing_days":        n_miss,
            "first_date":            grp["timestamp_utc"].min(),
            "last_date":             grp["timestamp_utc"].max(),
            "overlap_valid_days":    ov_valid,
            "overlap_pct_complete":  ov_pct,
            "fwi_input_status":      status,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 5. Cadence and duplicate checks
# ---------------------------------------------------------------------------

def check_cadence(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Check each (station_code, variable_name_std) series for off-grid
    timestamps (not aligned to the top of the hour) and duplicate
    (station_code, variable_name_std, timestamp_utc) keys.

    Returns a summary DataFrame with one row per station-variable pair.

    Columns produced
    ----------------
    station_code, variable_name_std, n_rows, n_off_grid,
    n_duplicates, cadence_ok
    """
    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    records = []
    for (station, variable), grp in df.groupby(["station_code", "variable_name_std"], sort=True):
        n_rows = len(grp)

        # Off-grid: minute or second component is non-zero
        off_grid = int((
            (grp["timestamp_utc"].dt.minute != 0)
            | (grp["timestamp_utc"].dt.second != 0)
        ).sum())

        # Duplicates: same (station, variable, timestamp)
        n_dupes = int(grp["timestamp_utc"].duplicated().sum())

        records.append({
            "station_code":      station,
            "variable_name_std": variable,
            "n_rows":            n_rows,
            "n_off_grid":        off_grid,
            "n_duplicates":      n_dupes,
            "cadence_ok":        (off_grid == 0 and n_dupes == 0),
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 6. CAV / TRW timestamp spot-check (inherited -0300 offset caveat)
# ---------------------------------------------------------------------------

def build_tz_spot_check(hourly_df: pd.DataFrame, n_samples: int = 10) -> pd.DataFrame:
    """
    Build a spot-check table for CAV and TRW showing timestamp_local_raw,
    tz_token, and timestamp_utc side-by-side so the inherited -0300 offset
    issue is explicitly documented for Phase 4.

    Returns a DataFrame with columns:
        station_code, variable_name_std, timestamp_local_raw, tz_token,
        timestamp_utc, expected_utc_offset_h, actual_utc_offset_h, offset_ok
    """
    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    # Only applies to CAV and TRW
    df = df[df["station_code"].isin(["CAV", "TRW"])].copy()

    if df.empty or "timestamp_local_raw" not in df.columns or "tz_token" not in df.columns:
        log.warning("TZ spot-check: required columns not found or no CAV/TRW rows.")
        return pd.DataFrame()

    # Sample a few rows per station from within the common-overlap window
    samples = (
        df[
            (df["timestamp_utc"] >= OVERLAP_START)
            & (df["timestamp_utc"] <= OVERLAP_END)
            & df["timestamp_local_raw"].notna()
            & df["tz_token"].notna()
        ]
        .groupby("station_code", group_keys=False)
        .apply(lambda g: g.sample(min(n_samples, len(g)), random_state=42))
    )

    if samples.empty:
        return pd.DataFrame()

    # Parse timestamp_local_raw to compute the actual offset recorded
    rows = []
    for _, r in samples.iterrows():
        try:
            local_ts = pd.to_datetime(r["timestamp_local_raw"])
            utc_ts   = r["timestamp_utc"]
            if local_ts.tzinfo is None:
                # Assume the local string does not carry tz; compute naive offset
                diff_h = (utc_ts.replace(tzinfo=None) - local_ts).total_seconds() / 3600
            else:
                diff_h = (utc_ts - local_ts.tz_convert("UTC")).total_seconds() / 3600
        except Exception:
            diff_h = np.nan

        tz_token = str(r.get("tz_token", ""))
        # Expected offset is the numeric value from the tz_token (e.g. "-0300" → -3.0)
        try:
            sign   = -1 if tz_token.startswith("-") else 1
            digits = tz_token.lstrip("+-")
            h      = int(digits[:2])
            m      = int(digits[2:4]) if len(digits) >= 4 else 0
            expected_h = sign * (h + m / 60.0)
        except Exception:
            expected_h = np.nan

        rows.append({
            "station_code":          r["station_code"],
            "variable_name_std":     r["variable_name_std"],
            "timestamp_local_raw":   r.get("timestamp_local_raw", ""),
            "tz_token":              tz_token,
            "timestamp_utc":         utc_ts,
            "expected_utc_offset_h": expected_h,
            "actual_utc_offset_h":   round(diff_h, 3) if pd.notna(diff_h) else np.nan,
            "offset_ok": (
                abs(diff_h - expected_h) < 0.1
                if pd.notna(diff_h) and pd.notna(expected_h) else None
            ),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 7. Greenwich sensor comparison
# ---------------------------------------------------------------------------

def compare_gre_sensors(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare Greenwich air_temperature_c (S-THC, primary) against
    aux_air_temperature_c (S-TMB, auxiliary) on overlapping hourly timestamps.

    Returns a DataFrame with one row per overlapping hour containing:
        timestamp_utc, primary_c, aux_c, diff_c, abs_diff_c
    """
    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    gre = df[df["station_code"] == "GRE"].copy()

    primary = (
        gre[gre["variable_name_std"] == "air_temperature_c"][["timestamp_utc", "value"]]
        .rename(columns={"value": "primary_c"})
        .dropna(subset=["primary_c"])
    )
    aux = (
        gre[gre["variable_name_std"] == "aux_air_temperature_c"][["timestamp_utc", "value"]]
        .rename(columns={"value": "aux_c"})
        .dropna(subset=["aux_c"])
    )

    if primary.empty or aux.empty:
        log.info("GRE sensor comparison: one or both series are empty.")
        return pd.DataFrame()

    merged = primary.merge(aux, on="timestamp_utc")
    merged["diff_c"]     = merged["primary_c"] - merged["aux_c"]
    merged["abs_diff_c"] = merged["diff_c"].abs()

    return merged.sort_values("timestamp_utc").reset_index(drop=True)


def summarise_sensor_comparison(comparison_df: pd.DataFrame) -> dict:
    """
    Produce numeric summary statistics from the Greenwich sensor comparison.
    Returns an empty dict if the input DataFrame is empty.
    """
    if comparison_df.empty:
        return {}

    d = comparison_df["diff_c"]
    return {
        "n_overlap_hours":   len(comparison_df),
        "mean_diff_c":       round(d.mean(), 3),
        "median_diff_c":     round(d.median(), 3),
        "std_diff_c":        round(d.std(), 3),
        "max_abs_diff_c":    round(comparison_df["abs_diff_c"].max(), 3),
        "pct_within_0_5c":   round(100.0 * (comparison_df["abs_diff_c"] <= 0.5).mean(), 1),
        "pct_within_1_0c":   round(100.0 * (comparison_df["abs_diff_c"] <= 1.0).mean(), 1),
    }


# ---------------------------------------------------------------------------
# 8. Gap-structure diagnostics
# ---------------------------------------------------------------------------

def build_gap_summary(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (station_code, variable_name_std), compute gap-frequency and
    gap-length distribution statistics from the hourly long-form data.

    Columns produced
    ----------------
    station_code, variable_name_std, n_gap_events,
    mean_gap_len_h, median_gap_len_h, max_gap_len_h,
    n_short_gaps (<=2 h), n_long_gaps (>2 h)
    """
    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    records = []
    for (station, variable), grp in df.groupby(["station_code", "variable_name_std"], sort=True):
        grp = grp.sort_values("timestamp_utc")
        valid = grp["value"].notna().values

        # Identify transitions from valid → missing
        gap_lengths = []
        i = 0
        while i < len(valid):
            if not valid[i]:
                gap_start = i
                while i < len(valid) and not valid[i]:
                    i += 1
                gap_lengths.append(i - gap_start)
            else:
                i += 1

        n_gaps  = len(gap_lengths)
        if n_gaps > 0:
            gaps_arr = np.array(gap_lengths)
            mean_g   = round(float(gaps_arr.mean()), 2)
            med_g    = round(float(np.median(gaps_arr)), 1)
            max_g    = int(gaps_arr.max())
            n_short  = int((gaps_arr <= 2).sum())
            n_long   = int((gaps_arr > 2).sum())
        else:
            mean_g = med_g = max_g = n_short = n_long = 0

        records.append({
            "station_code":       station,
            "variable_name_std":  variable,
            "n_gap_events":       n_gaps,
            "mean_gap_len_h":     mean_g,
            "median_gap_len_h":   med_g,
            "max_gap_len_h":      max_g,
            "n_short_gaps":       n_short,
            "n_long_gaps":        n_long,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 9. Outlier summary
# ---------------------------------------------------------------------------

def build_outlier_summary(hourly_df: pd.DataFrame,
                          iqr_multiplier: float = 3.0) -> pd.DataFrame:
    """
    Flag potential outliers per (station_code, variable_name_std) using the
    IQR method on quality-filtered hourly values (native_ok only).  Counts
    are reported; data is never removed in Phase 3.

    Threshold: value < Q1 - k*IQR  or  value > Q3 + k*IQR  where k = iqr_multiplier.

    Columns produced
    ----------------
    station_code, variable_name_std, n_native_ok, q1, median, q3,
    lower_fence, upper_fence, n_outliers, pct_outliers
    """
    df = hourly_df[hourly_df["quality_flag_scrub"] == SCRUB_FLAG_OK].copy()
    df = df.dropna(subset=["value"])

    records = []
    for (station, variable), grp in df.groupby(["station_code", "variable_name_std"], sort=True):
        vals = grp["value"].values
        n    = len(vals)
        if n < 4:
            continue  # too few points to compute meaningful quantiles

        q1, med, q3 = np.percentile(vals, [25, 50, 75])
        iqr     = q3 - q1
        lower   = q1 - iqr_multiplier * iqr
        upper   = q3 + iqr_multiplier * iqr
        n_out   = int(((vals < lower) | (vals > upper)).sum())
        pct_out = round(100.0 * n_out / n, 3)

        records.append({
            "station_code":       station,
            "variable_name_std":  variable,
            "n_native_ok":        n,
            "q1":                 round(q1, 3),
            "median":             round(med, 3),
            "q3":                 round(q3, 3),
            "lower_fence":        round(lower, 3),
            "upper_fence":        round(upper, 3),
            "n_outliers":         n_out,
            "pct_outliers":       pct_out,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 10. Phase 4 handoff recommendations
# ---------------------------------------------------------------------------

def build_phase4_recommendations(fitness_df: pd.DataFrame,
                                  variable_summary_df: pd.DataFrame,
                                  gap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify every (station_code, variable_name_std) pair into:
        usable / usable-with-caveat / not-recommended

    with a plain-text reason, based on overlap-window fitness metrics,
    variable classification, and gap statistics.

    Columns produced
    ----------------
    station_code, variable_name_std, category,
    preliminary_inclusion_status, pct_complete_overlap,
    max_gap_len_h, recommendation, reason
    """
    # Bring in the preliminary inclusion status per variable
    var_map = variable_summary_df.set_index("variable_name_std")["preliminary_inclusion_status"].to_dict()
    gap_map = gap_df.set_index(["station_code", "variable_name_std"])["max_gap_len_h"].to_dict()

    records = []
    for _, row in fitness_df.iterrows():
        station  = row["station_code"]
        variable = row["variable_name_std"]
        pct      = row["pct_complete"]
        category = row["category"]
        in_ov    = row["in_overlap_window"]

        incl_status = var_map.get(variable, "exclude-from-primary")
        max_gap     = gap_map.get((station, variable), 0)

        # Build recommendation and reason
        if not in_ov:
            rec    = "not-recommended"
            reason = "Series does not appear in the common-overlap window"
        elif category in ("marine", "auxiliary"):
            rec    = "not-recommended"
            reason = f"Variable category '{category}' excluded from primary PCA matrix"
        elif pct < 50.0:
            rec    = "not-recommended"
            reason = f"Overlap completeness {pct:.1f}% is below 50% threshold"
        elif incl_status == "exclude-from-primary":
            rec    = "not-recommended"
            reason = "Variable not present in enough primary stations for common feature set"
        elif pct < 70.0 or max_gap > 720:
            rec    = "usable-with-caveat"
            reason = (
                f"Completeness {pct:.1f}% or max gap {max_gap}h exceeds caution thresholds; "
                "include with missingness weights or sensitivity check"
            )
        elif station in ("CAV", "TRW") and variable in CORE_ATMO_VARS:
            rec    = "usable-with-caveat"
            reason = "Inherited -0300 UTC offset; validate alignment before benchmarking"
        else:
            rec    = "usable"
            reason = f"Completeness {pct:.1f}%, max gap {max_gap}h, within thresholds"

        records.append({
            "station_code":                station,
            "variable_name_std":           variable,
            "category":                    category,
            "preliminary_inclusion_status": incl_status,
            "pct_complete":                pct,
            "max_gap_len_h":               max_gap,
            "recommendation":              rec,
            "reason":                      reason,
        })

    return pd.DataFrame(records).sort_values(
        ["recommendation", "station_code", "variable_name_std"]
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 11. Figures
# ---------------------------------------------------------------------------

def _ensure_dir(path: Path) -> None:
    """Create directory if it does not already exist."""
    path.mkdir(parents=True, exist_ok=True)


def plot_completeness_heatmap(fitness_df: pd.DataFrame,
                               out_path: Path,
                               title_suffix: str = "") -> None:
    """
    Plot a heatmap of pct_complete with stations as columns and variables
    as rows.  Saves the figure to out_path (PNG).

    Two versions are expected to be called externally:
        - Full-history fitness table
        - Common-overlap fitness table
    """
    _ensure_dir(out_path.parent)

    # Pivot to wide form for heatmap
    pivot = fitness_df.pivot_table(
        index="variable_name_std",
        columns="station_code",
        values="pct_complete",
        aggfunc="mean",
    )

    # Annotate variable category in row labels
    cat_map = fitness_df.drop_duplicates("variable_name_std").set_index("variable_name_std")["category"]
    pivot.index = [f"{v}  [{cat_map.get(v, '?')}]" for v in pivot.index]

    figw = max(8, len(pivot.columns) * 1.4)
    figh = max(6, len(pivot.index) * 0.45)
    fig, ax = plt.subplots(figsize=(figw, figh))

    sns.heatmap(
        pivot,
        ax=ax,
        annot=True,
        fmt=".0f",
        cmap="RdYlGn",
        vmin=0,
        vmax=100,
        linewidths=0.4,
        cbar_kws={"label": "% complete"},
    )
    ax.set_title(f"Station × Variable Completeness (%) {title_suffix}", fontsize=11)
    ax.set_xlabel("Station code")
    ax.set_ylabel("Standardized variable")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Figure saved: %s", out_path)


def plot_availability_timeline(hourly_df: pd.DataFrame,
                                out_path: Path,
                                resample_freq: str = "ME") -> None:
    """
    Gantt-style availability timeline showing fraction of valid hourly
    observations per station per calendar month.

    Each row is a station; colour intensity shows monthly completeness.
    Saves the figure to out_path (PNG).

    Parameters
    ----------
    resample_freq : Pandas offset alias for binning (default "ME" = month-end).
                    Use "W" for weekly if more granularity is needed.
    """
    _ensure_dir(out_path.parent)

    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    # Remove timezone for cleaner datetime axes
    df["ts"] = df["timestamp_utc"].dt.tz_localize(None)
    df["valid"] = df["value"].notna().astype(float)

    # Monthly completeness per station across all variables
    df["month"] = df["ts"].dt.to_period("M")
    monthly = (
        df.groupby(["station_code", "month"])["valid"]
        .mean()
        .mul(100)
        .reset_index()
    )
    monthly["month_ts"] = monthly["month"].dt.to_timestamp()
    pivot = monthly.pivot(index="station_code", columns="month_ts", values="valid")

    fig, ax = plt.subplots(figsize=(max(14, len(pivot.columns) * 0.35), 4))
    sns.heatmap(
        pivot,
        ax=ax,
        cmap="Blues",
        vmin=0,
        vmax=100,
        linewidths=0.1,
        cbar_kws={"label": "% valid (all vars)"},
        xticklabels=6,  # show every 6th month label to avoid crowding
    )
    ax.set_title("Station Data Availability Timeline (monthly % valid across all variables)")
    ax.set_xlabel("Month (UTC)")
    ax.set_ylabel("Station code")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Figure saved: %s", out_path)


def plot_distribution_grid(hourly_df: pd.DataFrame,
                            variables: list,
                            out_path: Path,
                            title: str = "Hourly value distributions",
                            overlap_only: bool = True) -> None:
    """
    Produce a grid of per-station boxplots for each variable in *variables*.
    Uses only native_ok quality-flagged values.

    Parameters
    ----------
    variables    : List of variable_name_std values to plot.
    overlap_only : If True, restrict to the common-overlap window.
    """
    _ensure_dir(out_path.parent)

    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    if overlap_only:
        df = df[(df["timestamp_utc"] >= OVERLAP_START) & (df["timestamp_utc"] <= OVERLAP_END)]

    # Keep only native_ok rows with a value
    df = df[(df["quality_flag_scrub"] == SCRUB_FLAG_OK) & df["value"].notna()]
    df = df[df["variable_name_std"].isin(variables)]

    if df.empty:
        log.warning("Distribution grid: no native_ok rows for the requested variables.")
        return

    ncols = 3
    nrows = int(np.ceil(len(variables) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 5, nrows * 3.5))
    axes = np.array(axes).flatten()

    for idx, variable in enumerate(variables):
        ax   = axes[idx]
        sub  = df[df["variable_name_std"] == variable]
        unit = sub["unit_std"].iloc[0] if not sub.empty and "unit_std" in sub.columns else ""

        if sub.empty:
            ax.set_visible(False)
            continue

        # Seaborn boxplot with one box per station
        station_order = sorted(sub["station_code"].unique())
        sns.boxplot(
            data=sub,
            x="station_code",
            y="value",
            hue="station_code",
            order=station_order,
            ax=ax,
            palette="Set2",
            legend=False,
            showfliers=False,  # hide extreme fliers; they are quantified in outlier table
        )
        ax.set_title(f"{variable}\n({unit})", fontsize=9)
        ax.set_xlabel("")
        ax.set_ylabel(unit, fontsize=8)
        ax.tick_params(axis="x", labelsize=8)

    # Hide any unused subplot panels
    for idx in range(len(variables), len(axes)):
        axes[idx].set_visible(False)

    fig.suptitle(title, fontsize=11, y=1.01)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Figure saved: %s", out_path)


def plot_gre_sensor_comparison(comparison_df: pd.DataFrame, out_path: Path) -> None:
    """
    Plot Greenwich primary vs. auxiliary air-temperature sensor agreement:
      - Left panel: scatter plot (primary vs. aux) coloured by absolute diff.
      - Right panel: time series of diff_c.

    Saves to out_path (PNG).
    """
    _ensure_dir(out_path.parent)

    if comparison_df.empty:
        log.info("GRE sensor comparison figure skipped: no overlapping data.")
        return

    ts = comparison_df["timestamp_utc"]
    if hasattr(ts.iloc[0], "tzinfo") and ts.iloc[0].tzinfo is not None:
        ts = ts.dt.tz_localize(None)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4))

    # Scatter plot
    sc = ax1.scatter(
        comparison_df["primary_c"],
        comparison_df["aux_c"],
        c=comparison_df["abs_diff_c"],
        cmap="hot_r",
        s=5,
        alpha=0.4,
        vmin=0,
        vmax=3,
    )
    plt.colorbar(sc, ax=ax1, label="|diff| °C")
    lims = [
        min(comparison_df["primary_c"].min(), comparison_df["aux_c"].min()),
        max(comparison_df["primary_c"].max(), comparison_df["aux_c"].max()),
    ]
    ax1.plot(lims, lims, "k--", linewidth=0.8, label="1:1 line")
    ax1.set_xlabel("Primary (S-THC) °C")
    ax1.set_ylabel("Auxiliary (S-TMB) °C")
    ax1.set_title("GRE Temperature Sensor Agreement")
    ax1.legend(fontsize=8)

    # Time series of difference
    ax2.plot(ts, comparison_df["diff_c"], linewidth=0.5, color="steelblue", alpha=0.7)
    ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax2.set_xlabel("Date (UTC)")
    ax2.set_ylabel("Primary − Auxiliary (°C)")
    ax2.set_title("GRE Temperature Sensor Difference Over Time")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Figure saved: %s", out_path)


def plot_gap_heatmap(gap_df: pd.DataFrame, out_path: Path) -> None:
    """
    Heatmap of max_gap_len_h per (station_code x variable_name_std).
    Highlights series with very long gaps that Phase 4 should be cautious about.
    Saves to out_path (PNG).
    """
    _ensure_dir(out_path.parent)

    pivot = gap_df.pivot_table(
        index="variable_name_std",
        columns="station_code",
        values="max_gap_len_h",
        aggfunc="max",
    )

    figw = max(8, len(pivot.columns) * 1.4)
    figh = max(6, len(pivot.index) * 0.45)
    fig, ax = plt.subplots(figsize=(figw, figh))

    sns.heatmap(
        pivot,
        ax=ax,
        annot=True,
        fmt=".0f",
        cmap="YlOrRd",
        linewidths=0.4,
        cbar_kws={"label": "Max gap length (h)"},
    )
    ax.set_title("Longest Single Gap per Station × Variable (hours)")
    ax.set_xlabel("Station code")
    ax.set_ylabel("Standardized variable")
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Figure saved: %s", out_path)


# ---------------------------------------------------------------------------
# 12. CSV writer helpers
# ---------------------------------------------------------------------------

def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV, creating the parent directory if needed."""
    _ensure_dir(path.parent)
    df.to_csv(path, index=False)
    log.info("CSV saved: %s  (%d rows)", path, len(df))
