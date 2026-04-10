"""
scrub_utils.py
==============
Helper functions for Phase 2: Scrub and Normalize.

All heavy lifting for the scrub pipeline lives here so that 02_scrub.py
remains a thin orchestration entry point.  Every function is stateless
and accepts only plain Python/pandas types so the module is easy to test
in isolation.

Responsibilities
----------------
- Parser-family-specific CSV readers (PEINP-HOBOlink, ECCC-LST)
- Timestamp validation and UTC normalization
- Variable mapping from raw sensor headers to the canonical long-form schema
- Unit harmonization
- Scrub-level quality-flag assignment
- Hourly regularization and conservative gap-aware interpolation
- Daily aggregation with variable-specific rules
- Audit artifact writers

Canonical long-form schema
--------------------------
Required columns (every row):
  station_code        str   e.g. "CAV"
  station_name        str   e.g. "Cavendish"
  parser_family       str   "PEINP-HOBOlink" | "ECCC-LST"
  source_file         str   original file path (relative to workspace root)
  timestamp_local_raw str   combined Date+Time string as read from source
  timestamp_utc       datetime64[ns, UTC]
  variable_name_std   str   standardized variable name (see VAR_MAP_* dicts)
  value               float64
  unit_std            str   standardized unit string
  quality_flag_source str   quality flag from the source (ECCC flag col or "")
  quality_flag_scrub  str   scrub-assigned flag (see SCRUB_FLAG_* constants)
  imputation_flag     str   "" | "interpolated_short_gap"
  resample_level      str   "raw" | "hourly" | "daily"

Optional provenance columns (added for debugging):
  raw_column_name     str   exact source column header
  tz_token            str   timezone token from the registry
  schema_variant      str   free-text schema variant note
  known_issue_tag     str   known-issue tag from the registry
"""

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scrub quality-flag vocabulary
# ---------------------------------------------------------------------------

# These flag values flow into the quality_flag_scrub column and are the
# authoritative vocabulary for downstream phases.

SCRUB_FLAG_OK              = "native_ok"
SCRUB_FLAG_SOURCE_FLAGGED  = "source_flagged"
SCRUB_FLAG_RANGE_FAILED    = "range_failed"
SCRUB_FLAG_DUPE_RESOLVED   = "duplicate_resolved"
SCRUB_FLAG_INTERP_SHORT    = "interpolated_short_gap"
SCRUB_FLAG_EXCL_LONG       = "excluded_long_gap"

# ---------------------------------------------------------------------------
# Conservative interpolation thresholds
# ---------------------------------------------------------------------------

# Maximum number of consecutive missing hourly values that may be filled
# by linear interpolation.  Gaps larger than this are left as NaN and
# flagged as excluded_long_gap.
MAX_INTERP_GAP_HOURS = 2

# Variables for which interpolation is *never* applied in the primary
# scrubbed products, regardless of gap length.
NO_INTERP_VARS = frozenset([
    "precipitation_mm",
    "accumulated_rain_mm",
    "level_m",
    "water_level_m",
    "water_pressure_kpa",
    "diff_pressure_kpa",
    "water_flow_ls",
])

# ---------------------------------------------------------------------------
# Physical range limits for core atmospheric variables
# (used for range_failed flag assignment; values outside range are set to NaN)
# ---------------------------------------------------------------------------

RANGE_LIMITS = {
    "air_temperature_c":      (-50.0,  60.0),
    "water_temperature_c":    (-5.0,   40.0),
    "dew_point_c":            (-60.0,  40.0),
    "relative_humidity_pct":  (0.0,   110.0),
    "wind_speed_kmh":         (0.0,   200.0),
    "wind_gust_kmh":          (0.0,   250.0),
    "wind_speed_ms":          (0.0,    55.0),
    "wind_gust_ms":           (0.0,    70.0),
    "wind_direction_deg":     (0.0,   360.0),
    "solar_radiation_wm2":    (0.0,  1500.0),
    "precipitation_mm":       (0.0,   200.0),
    "pressure_kpa":           (85.0,  110.0),
    "visibility_km":          (0.0,   100.0),
}

# ---------------------------------------------------------------------------
# Variable mapping: PEINP-HOBOlink
# ---------------------------------------------------------------------------
# Each entry maps a canonical keyword (matched case-insensitively against the
# start of the raw column name after stripping sensor-ID parenthesized tokens)
# to (variable_name_std, unit_std, variable_category).
#
# category is either "atmospheric" or "marine".
#
# Rules for duplicate / competing columns:
#   - Wind speed: prefer km/h column; m/s columns are retained as auxiliary.
#   - Wind gust:  prefer km/h column; m/s columns are retained as auxiliary.
#   - Temperature at marine stations (SBW/NRW/TRW): S-TMB sensor is the
#     atmospheric air temperature; water temperature uses M-WT sensor.
#   - Greenwich temperature: S-THC sensor family is preferred; S-TMB is
#     retained as auxiliary (aux_air_temperature_c).
#   - Accumulated Rain is an auxiliary diagnostic; primary precip is Rain.

PEINP_VAR_MAP = [
    # keyword pattern                  std_name                    unit       category   priority
    ("temperature",       "S-THB",     "air_temperature_c",        "°C",      "atmospheric", 1),
    ("temperature",       "S-THC",     "air_temperature_c",        "°C",      "atmospheric", 1),
    ("temperature",       "S-TMB",     "air_temperature_c",        "°C",      "atmospheric", 2),  # aux at GRE
    ("rh",                "",          "relative_humidity_pct",    "%",       "atmospheric", 1),
    ("dew point",         "",          "dew_point_c",              "°C",      "atmospheric", 1),
    ("rain",              "S-RGB",     "precipitation_mm",         "mm",      "atmospheric", 1),
    ("accumulated rain",  "",          "accumulated_rain_mm",      "mm",      "atmospheric", 2),
    ("average wind speed","",          "wind_speed_kmh",           "km/h",    "atmospheric", 1),
    ("avg wind speed",    "",          "wind_speed_kmh",           "km/h",    "atmospheric", 1),
    ("average wind speed","",          "wind_speed_kmh",           "km/h",    "atmospheric", 1),
    ("wind speed",        "km/h",      "wind_speed_kmh",           "km/h",    "atmospheric", 1),
    ("wind gust speed",   "km/h",      "wind_gust_kmh",            "km/h",    "atmospheric", 1),
    ("wind gust  speed",  "km/h",      "wind_gust_kmh",            "km/h",    "atmospheric", 1),  # double-space TRW
    ("wind gust speed",   "km/h",      "wind_gust_kmh",            "km/h",    "atmospheric", 1),
    ("wind gust speed",   "m/s",       "wind_gust_ms",             "m/s",     "atmospheric", 2),
    ("gust speed",        "km/h",      "wind_gust_kmh",            "km/h",    "atmospheric", 1),
    ("gust speed",        "m/s",       "wind_gust_ms",             "m/s",     "atmospheric", 2),
    ("wind speed",        "m/s",       "wind_speed_ms",            "m/s",     "atmospheric", 2),
    ("wind direction",    "",          "wind_direction_deg",       "°",       "atmospheric", 1),
    ("solar radiation",   "",          "solar_radiation_wm2",      "W/m²",    "atmospheric", 1),
    ("barometric pressure","",         "pressure_kpa",             "kPa",     "atmospheric", 1),
    ("water temperature", "",          "water_temperature_c",      "°C",      "marine",      1),
    ("water level",       "",          "water_level_m",            "m",       "marine",      1),
    ("water pressure",    "",          "water_pressure_kpa",       "kPa",     "marine",      1),
    ("diff pressure",     "",          "diff_pressure_kpa",        "kPa",     "marine",      1),
    ("water flow",        "",          "water_flow_ls",            "l/s",     "marine",      1),
    ("battery",           "",          "battery_v",                "V",       "auxiliary",   1),
]

# ECCC-LST variable mapping
# Maps the exact ECCC column header to (std_name, unit, category).
ECCC_VAR_MAP = {
    "Temp (°C)":              ("air_temperature_c",      "°C",     "atmospheric"),
    "Dew Point Temp (°C)":    ("dew_point_c",            "°C",     "atmospheric"),
    "Rel Hum (%)":            ("relative_humidity_pct",  "%",      "atmospheric"),
    "Precip. Amount (mm)":    ("precipitation_mm",       "mm",     "atmospheric"),
    "Wind Dir (10s deg)":     ("wind_direction_10s_deg", "10s°",   "atmospheric"),
    "Wind Spd (km/h)":        ("wind_speed_kmh",         "km/h",   "atmospheric"),
    "Visibility (km)":        ("visibility_km",          "km",     "atmospheric"),
    "Stn Press (kPa)":        ("pressure_kpa",           "kPa",    "atmospheric"),
    "Hmdx":                   ("humidex",                "",       "atmospheric"),
    "Wind Chill":             ("wind_chill",             "",       "atmospheric"),
    "Weather":                ("weather_desc",           "",       "auxiliary"),
}

# ECCC source-flag columns that accompany measurement columns.
# These are translated into quality_flag_source values.
ECCC_FLAG_COLS = {
    "Temp Flag", "Dew Point Temp Flag", "Rel Hum Flag",
    "Precip. Amount Flag", "Wind Dir Flag", "Wind Spd Flag",
    "Visibility Flag", "Stn Press Flag", "Hmdx Flag", "Wind Chill Flag",
}

# Daily aggregation rules per standardized variable.
# "mean", "sum", "max", "min", or a callable.
DAILY_AGG_RULES = {
    "air_temperature_c":      "mean",
    "dew_point_c":            "mean",
    "relative_humidity_pct":  "mean",
    "wind_speed_kmh":         "mean",
    "wind_gust_kmh":          "max",
    "wind_speed_ms":          "mean",
    "wind_gust_ms":           "max",
    "wind_direction_deg":     "mean",
    "wind_direction_10s_deg": "mean",
    "solar_radiation_wm2":    "mean",
    "pressure_kpa":           "mean",
    "visibility_km":          "mean",
    "precipitation_mm":       "sum",
    "accumulated_rain_mm":    "max",   # take end-of-day cumulative value
    "humidex":                "mean",
    "wind_chill":             "min",
    "water_temperature_c":    "mean",
    "water_level_m":          "mean",
    "water_pressure_kpa":     "mean",
    "diff_pressure_kpa":      "mean",
    "water_flow_ls":          "mean",
    "battery_v":              "min",
    "aux_air_temperature_c":  "mean",
}

# ---------------------------------------------------------------------------
# Encoding helper (shared with obtain_utils pattern)
# ---------------------------------------------------------------------------

def _read_csv_with_fallback(file_path: Path, **kwargs) -> pd.DataFrame:
    """
    Read a CSV trying UTF-8-BOM, plain UTF-8, then cp1252 encoding.
    Raises ValueError if all encodings fail.
    """
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return pd.read_csv(
                file_path,
                encoding=encoding,
                low_memory=False,
                na_values=["", " ", "NA", "None", "ERROR"],
                keep_default_na=True,
                **kwargs,
            )
        except UnicodeDecodeError:
            continue
    raise ValueError(
        f"Could not decode {file_path.name} with UTF-8 or cp1252 encoding"
    )


# ---------------------------------------------------------------------------
# PEINP-HOBOlink reader
# ---------------------------------------------------------------------------

def read_peinp_file(file_path: Path, registry_row: pd.Series) -> pd.DataFrame:
    """
    Read one PEINP-HOBOlink CSV and return a raw wide DataFrame.

    - Header is always row 0.
    - Column name whitespace is normalized: leading/trailing stripped,
      internal double-spaces collapsed to a single space (covers Tracadie
      double-space anomaly).
    - Returns the raw DataFrame with the original columns preserved.
      UTC normalization and variable mapping happen in subsequent steps.
    """
    df = _read_csv_with_fallback(file_path, header=0)

    # Normalize column names: strip outer whitespace; collapse internal spaces.
    df.columns = [
        re.sub(r"  +", " ", str(c).strip()) for c in df.columns
    ]

    log.debug(
        "PEINP read: %s  rows=%d  cols=%d",
        file_path.name, len(df), len(df.columns),
    )
    return df


# ---------------------------------------------------------------------------
# ECCC-LST reader
# ---------------------------------------------------------------------------

def read_eccc_file(file_path: Path, registry_row: pd.Series) -> pd.DataFrame:
    """
    Read one ECCC-LST CSV and return a raw wide DataFrame.

    ECCC files contain a blank line between every data row; pandas
    skip_blank_lines=True (the default) handles this transparently.
    The Date/Time (LST) column is preserved in its original string form;
    UTC conversion happens in a separate step.
    """
    df = _read_csv_with_fallback(
        file_path,
        header=0,
        skip_blank_lines=True,
    )

    log.debug(
        "ECCC-LST read: %s  rows=%d  cols=%d",
        file_path.name, len(df), len(df.columns),
    )
    return df


# ---------------------------------------------------------------------------
# Timestamp parsing and UTC normalization
# ---------------------------------------------------------------------------

def parse_peinp_timestamps(
    df: pd.DataFrame,
    tz_token: str,
    file_path: Path,
) -> tuple[pd.Series, pd.Series, list[dict]]:
    """
    Parse PEINP Date (MM/DD/YYYY) and Time (HH:MM:SS ±HHMM) columns into:
      - timestamp_local_raw  : combined string, preserved for traceability
      - timestamp_utc        : timezone-aware UTC Series

    The tz_token from the registry is trusted exactly as recorded in phase 1
    (including any -0300 values for CAV and TRW).

    Returns (timestamp_local_raw, timestamp_utc, audit_list).
    The audit_list contains dicts describing any parse or validation issues.
    """
    audit = []

    # Locate Date and Time columns (case-insensitive, stripped)
    col_map = {c.strip().lower(): c for c in df.columns}
    date_col = col_map.get("date")
    time_col = col_map.get("time")

    if date_col is None or time_col is None:
        audit.append({
            "file": str(file_path),
            "issue": "missing Date or Time column",
            "severity": "error",
        })
        empty = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
        return pd.Series("", index=df.index), empty, audit

    # Combine Date and Time into a raw string for traceability
    timestamp_local_raw = (
        df[date_col].astype(str).str.strip()
        + " "
        + df[time_col].astype(str).str.strip()
    )

    # The Time column embeds the offset, e.g. "00:00:00 -0300".
    # Parse using pandas to_datetime with utc=True which respects the offset.
    try:
        timestamp_utc = pd.to_datetime(
            timestamp_local_raw,
            format="%m/%d/%Y %H:%M:%S %z",
            utc=True,
            errors="coerce",
        )
    except Exception:
        # Fallback: try without a strict format in case of date-format variation
        timestamp_utc = pd.to_datetime(
            timestamp_local_raw,
            utc=True,
            errors="coerce",
        )

    n_failed = timestamp_utc.isna().sum()
    if n_failed > 0:
        audit.append({
            "file": str(file_path),
            "issue": f"{n_failed} timestamp parse failures",
            "severity": "warning",
        })

    # Monotonicity check
    valid_ts = timestamp_utc.dropna()
    if not valid_ts.empty and (valid_ts.diff().iloc[1:] < pd.Timedelta(0)).any():
        audit.append({
            "file": str(file_path),
            "issue": "non-monotonic timestamps detected",
            "severity": "warning",
        })

    # Duplicate check
    n_dups = timestamp_utc.duplicated().sum()
    if n_dups > 0:
        audit.append({
            "file": str(file_path),
            "issue": f"{n_dups} duplicate timestamps detected",
            "severity": "info",
        })

    return timestamp_local_raw, timestamp_utc, audit


def parse_eccc_timestamps(
    df: pd.DataFrame,
    file_path: Path,
) -> tuple[pd.Series, pd.Series, list[dict]]:
    """
    Parse the ECCC Date/Time (LST) column and apply a fixed UTC-4 offset.

    LST = Atlantic Standard Time = UTC-4.  No offset is embedded in the
    data string; the fixed -4 h shift is applied as documented in phase 1.

    Returns (timestamp_local_raw, timestamp_utc, audit_list).
    """
    audit = []

    dt_col = next(
        (c for c in df.columns if "Date/Time" in str(c) and "LST" in str(c)),
        None,
    )
    if dt_col is None:
        audit.append({
            "file": str(file_path),
            "issue": "missing Date/Time (LST) column",
            "severity": "error",
        })
        empty = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
        return pd.Series("", index=df.index), empty, audit

    timestamp_local_raw = df[dt_col].astype(str).str.strip()

    # Parse as naive then localize to UTC-4
    ts_naive = pd.to_datetime(
        timestamp_local_raw,
        format="%Y-%m-%d %H:%M",
        errors="coerce",
    )
    # Localize to Atlantic Standard Time (UTC-4), then convert to UTC
    try:
        import pytz
        ast_tz = pytz.FixedOffset(-240)   # -4 h * 60 min
        ts_local = ts_naive.dt.tz_localize(ast_tz, ambiguous="NaT", nonexistent="NaT")
        timestamp_utc = ts_local.dt.tz_convert("UTC")
    except ImportError:
        # pytz not available: apply offset manually using timedelta
        offset = pd.Timedelta(hours=4)
        timestamp_utc = (
            ts_naive + offset
        ).dt.tz_localize("UTC")

    n_failed = timestamp_utc.isna().sum()
    if n_failed > 0:
        audit.append({
            "file": str(file_path),
            "issue": f"{n_failed} timestamp parse failures",
            "severity": "warning",
        })

    # Monotonicity check
    valid_ts = timestamp_utc.dropna()
    if not valid_ts.empty and (valid_ts.diff().iloc[1:] < pd.Timedelta(0)).any():
        audit.append({
            "file": str(file_path),
            "issue": "non-monotonic timestamps detected",
            "severity": "warning",
        })

    n_dups = timestamp_utc.duplicated().sum()
    if n_dups > 0:
        audit.append({
            "file": str(file_path),
            "issue": f"{n_dups} duplicate timestamps detected",
            "severity": "info",
        })

    return timestamp_local_raw, timestamp_utc, audit


# ---------------------------------------------------------------------------
# Variable mapping helpers
# ---------------------------------------------------------------------------

def _normalize_col_base(col: str) -> str:
    """
    Strip sensor-ID parenthesized tokens and units from a PEINP column name
    and return a lower-case base name for pattern matching.

    Example:
      "Average wind speed (S-WCF 21114839:21107892-1),Km/h,Cavenish Green Gables"
      -> "average wind speed"
    """
    # Remove everything in (…) brackets (sensor IDs)
    base = re.sub(r"\(.*?\)", "", col).strip()
    # Remove trailing comma-separated unit and location fields
    base = base.split(",")[0].strip()
    return base.lower()


def _col_contains_unit(col: str, unit_hint: str) -> bool:
    """
    Return True if *unit_hint* appears (case-insensitively) anywhere in the
    raw column string.  Used to disambiguate competing wind-speed unit columns.
    An empty unit_hint always returns True (no unit constraint).
    """
    if not unit_hint:
        return True
    return unit_hint.lower() in col.lower()


def _col_sensor_family(col: str, sensor_hint: str) -> bool:
    """
    Return True if *sensor_hint* appears in the raw column string.
    An empty sensor_hint always returns True.
    """
    if not sensor_hint:
        return True
    return sensor_hint.upper() in col.upper()


def map_peinp_columns(
    df: pd.DataFrame,
    station_code: str,
) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    """
    Map PEINP-HOBOlink raw columns to the standardized variable vocabulary.

    Returns:
      mapped_rows : list of dicts with schema fields ready for the long-form
                    output (one dict per source column per data row).
      schema_audit: list of dicts describing each raw column mapping decision.
      unmapped    : list of dicts for columns that were not mapped.

    Strategy for priority conflicts (duplicate competing columns):
    - For each std_name, find all candidate raw columns that match.
    - Pick the candidate with the lowest priority number (1=preferred).
    - If two candidates share the same std_name and priority, pick the first
      occurrence; the other is logged as unmapped/auxiliary.
    - The Greenwich S-TMB temperature column is mapped to "aux_air_temperature_c"
      rather than the primary "air_temperature_c" to preserve both sensors.
    """
    schema_audit = []
    unmapped = []

    # Build a dict: raw_col -> (std_name, unit_std, category, priority)
    col_assignment: dict[str, tuple] = {}
    assigned_primary: dict[str, str] = {}   # std_name -> first-assigned raw col

    # Skip timestamp and non-data columns
    skip_cols = {"date", "time"}

    for raw_col in df.columns:
        col_lower = raw_col.strip().lower()
        if col_lower in skip_cols:
            continue

        base = _normalize_col_base(raw_col)
        matched = False

        for (
            kw, unit_hint, std_name, unit_std, category, priority
        ) in PEINP_VAR_MAP:
            if kw not in base:
                continue
            if not _col_contains_unit(raw_col, unit_hint):
                continue
            if not _col_sensor_family(raw_col, unit_hint if unit_hint.startswith("S-") else ""):
                continue

            # Greenwich-specific: S-TMB temperature becomes auxiliary
            this_std = std_name
            if (
                station_code == "GRE"
                and std_name == "air_temperature_c"
                and "S-TMB" in raw_col
            ):
                this_std = "aux_air_temperature_c"

            # Check if a primary assignment already exists for this std_name
            if this_std in assigned_primary and priority > 1:
                # This is a lower-priority duplicate; log as auxiliary unmapped
                unmapped.append({
                    "raw_column": raw_col,
                    "reason": (
                        f"lower-priority duplicate of "
                        f"'{this_std}' (primary: {assigned_primary[this_std]})"
                    ),
                })
                schema_audit.append({
                    "raw_column": raw_col,
                    "std_name": this_std + "_aux",
                    "unit_std": unit_std,
                    "priority": priority,
                    "action": "demoted-to-auxiliary",
                })
                matched = True
                break

            col_assignment[raw_col] = (this_std, unit_std, category, priority)
            if this_std not in assigned_primary:
                assigned_primary[this_std] = raw_col
            schema_audit.append({
                "raw_column": raw_col,
                "std_name": this_std,
                "unit_std": unit_std,
                "priority": priority,
                "action": "mapped",
            })
            matched = True
            break

        if not matched:
            unmapped.append({
                "raw_column": raw_col,
                "reason": "no keyword match in PEINP_VAR_MAP",
            })
            schema_audit.append({
                "raw_column": raw_col,
                "std_name": "",
                "unit_std": "",
                "priority": "",
                "action": "unmapped",
            })

    return col_assignment, schema_audit, unmapped


def map_eccc_columns(
    df: pd.DataFrame,
) -> tuple[dict, list[dict], list[dict]]:
    """
    Map ECCC-LST columns to the standardized variable vocabulary.

    Returns (col_assignment, schema_audit, unmapped) in the same shape
    as map_peinp_columns so the long-form builder works uniformly.
    """
    schema_audit = []
    unmapped = []
    col_assignment: dict[str, tuple] = {}

    # Columns to skip (metadata / timestamp / flag columns handled separately)
    skip_cols = {
        "Date/Time (LST)", "Time (LST)", "Year", "Month", "Day",
        "Longitude (x)", "Latitude (y)", "Station Name", "Climate ID", "Flag",
    } | ECCC_FLAG_COLS

    for raw_col in df.columns:
        if raw_col in skip_cols:
            continue
        if raw_col in ECCC_VAR_MAP:
            std_name, unit_std, category = ECCC_VAR_MAP[raw_col]
            col_assignment[raw_col] = (std_name, unit_std, category, 1)
            schema_audit.append({
                "raw_column": raw_col,
                "std_name": std_name,
                "unit_std": unit_std,
                "priority": 1,
                "action": "mapped",
            })
        else:
            unmapped.append({
                "raw_column": raw_col,
                "reason": "not in ECCC_VAR_MAP",
            })
            schema_audit.append({
                "raw_column": raw_col,
                "std_name": "",
                "unit_std": "",
                "priority": "",
                "action": "unmapped",
            })

    return col_assignment, schema_audit, unmapped


# ---------------------------------------------------------------------------
# Long-form record builder
# ---------------------------------------------------------------------------

def build_long_form(
    df: pd.DataFrame,
    col_assignment: dict,
    timestamp_local_raw: pd.Series,
    timestamp_utc: pd.Series,
    registry_row: pd.Series,
    eccc_flag_map: dict | None = None,
) -> pd.DataFrame:
    """
    Pivot a wide source DataFrame into the canonical long-form schema.

    Parameters
    ----------
    df                 : wide raw DataFrame (post-read, post-col-normalize)
    col_assignment     : mapping from raw_col -> (std_name, unit, category, priority)
    timestamp_local_raw: combined local-time string Series
    timestamp_utc      : UTC-normalized datetime Series
    registry_row       : single registry row for provenance fields
    eccc_flag_map      : optional dict raw_col -> flag_col for ECCC files

    Returns a long-form DataFrame with the canonical schema columns.
    """
    records = []
    source_file = str(registry_row["file_path"])
    station_code = str(registry_row["station_code"])
    station_name = str(registry_row["station_name"])
    parser_family = str(registry_row["parser_family"])
    tz_token = str(registry_row.get("tz_token", ""))
    schema_variant = str(registry_row.get("schema_notes", ""))
    known_issue_tag = str(registry_row.get("known_issues", ""))

    for raw_col, (std_name, unit_std, _, _priority) in col_assignment.items():
        if std_name == "":
            continue

        # Coerce source column to numeric; non-parseable values become NaN
        values = pd.to_numeric(df[raw_col], errors="coerce")

        # Source quality flag (ECCC only)
        if eccc_flag_map and raw_col in eccc_flag_map:
            flag_col = eccc_flag_map[raw_col]
            source_flags = (
                df[flag_col].fillna("").astype(str)
                if flag_col in df.columns
                else pd.Series("", index=df.index)
            )
        else:
            source_flags = pd.Series("", index=df.index)

        # Assign initial scrub flag
        scrub_flags = pd.Series(SCRUB_FLAG_OK, index=df.index)
        scrub_flags[source_flags.str.len() > 0] = SCRUB_FLAG_SOURCE_FLAGGED

        # Apply physical range check
        if std_name in RANGE_LIMITS:
            lo, hi = RANGE_LIMITS[std_name]
            out_of_range = values.notna() & ((values < lo) | (values > hi))
            if out_of_range.any():
                scrub_flags[out_of_range] = SCRUB_FLAG_RANGE_FAILED
                values[out_of_range] = np.nan
                log.debug(
                    "Range check: %d values flagged in %s / %s",
                    out_of_range.sum(), station_code, std_name,
                )

        col_df = pd.DataFrame({
            "station_code":        station_code,
            "station_name":        station_name,
            "parser_family":       parser_family,
            "source_file":         source_file,
            "timestamp_local_raw": timestamp_local_raw.values,
            "timestamp_utc":       timestamp_utc.values,
            "variable_name_std":   std_name,
            "value":               values.values,
            "unit_std":            unit_std,
            "quality_flag_source": source_flags.values,
            "quality_flag_scrub":  scrub_flags.values,
            "imputation_flag":     "",
            "resample_level":      "raw",
            "raw_column_name":     raw_col,
            "tz_token":            tz_token,
            "schema_variant":      schema_variant,
            "known_issue_tag":     known_issue_tag,
        })
        records.append(col_df)

    if not records:
        return pd.DataFrame(columns=[
            "station_code", "station_name", "parser_family", "source_file",
            "timestamp_local_raw", "timestamp_utc", "variable_name_std",
            "value", "unit_std", "quality_flag_source", "quality_flag_scrub",
            "imputation_flag", "resample_level", "raw_column_name",
            "tz_token", "schema_variant", "known_issue_tag",
        ])

    return pd.concat(records, ignore_index=True)


# ---------------------------------------------------------------------------
# ECCC source-flag column map builder
# ---------------------------------------------------------------------------

def build_eccc_flag_map(df: pd.DataFrame) -> dict:
    """
    Build a dict mapping each ECCC measurement column to its corresponding
    flag column (e.g., "Temp (°C)" -> "Temp Flag").
    """
    flag_map = {}
    for meas_col in ECCC_VAR_MAP:
        # Derive flag column name by appending ' Flag' to the base name
        base = meas_col.split("(")[0].strip()
        flag_col_candidate = base + " Flag"
        if flag_col_candidate in df.columns:
            flag_map[meas_col] = flag_col_candidate
    # Handle special cases
    if "Temp (°C)" in ECCC_VAR_MAP and "Temp Flag" in df.columns:
        flag_map["Temp (°C)"] = "Temp Flag"
    if "Dew Point Temp (°C)" in ECCC_VAR_MAP and "Dew Point Temp Flag" in df.columns:
        flag_map["Dew Point Temp (°C)"] = "Dew Point Temp Flag"
    return flag_map


# ---------------------------------------------------------------------------
# Hourly regularization and short-gap interpolation
# ---------------------------------------------------------------------------

def regularize_to_hourly(
    long_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each (station_code, variable_name_std) group:
      1. Sort by timestamp_utc.
      2. Resample to hourly using variable-specific aggregation (mean / max / sum),
         which simultaneously deduplicates sub-hourly observations.
      3. Reindex to a complete hourly UTC index spanning the station window.
      4. Apply short-gap linear interpolation (max MAX_INTERP_GAP_HOURS).
      5. Set resample_level to "hourly".

    Uses pandas resample() instead of a manual reindex loop so the function
    remains fast even on multi-million-row inputs.

    Returns a new long-form DataFrame at hourly resolution.
    """
    out_frames = []

    def _agg_method(var_name: str) -> str:
        if "gust" in var_name:
            return "max"
        if var_name in ("precipitation_mm", "accumulated_rain_mm"):
            return "sum"
        return "mean"

    groups = long_df.groupby(
        ["station_code", "variable_name_std"], sort=False
    )

    for (station_code, var_name), grp in groups:
        grp = grp.copy()

        # Ensure timestamp_utc is timezone-aware
        grp["timestamp_utc"] = pd.to_datetime(
            grp["timestamp_utc"], utc=True, errors="coerce"
        )
        grp = grp.dropna(subset=["timestamp_utc"])
        if grp.empty:
            continue

        grp = grp.sort_values("timestamp_utc")
        agg_meth = _agg_method(var_name)

        # Use resample to aggregate sub-hourly data to hourly grid.
        # This inherently deduplicates and produces the hourly index.
        grp_indexed = grp.set_index("timestamp_utc")

        # Aggregate the value column
        hourly_vals = grp_indexed["value"].resample("h").agg(agg_meth)

        # Carry first occurrence of provenance columns into each hour
        prov_cols = [
            "station_code", "station_name", "parser_family", "source_file",
            "variable_name_std", "unit_std", "raw_column_name",
            "tz_token", "schema_variant", "known_issue_tag",
            "quality_flag_source",
        ]
        prov_cols_present = [c for c in prov_cols if c in grp_indexed.columns]
        hourly_prov = grp_indexed[prov_cols_present].resample("h").first()

        # Build the hourly frame
        hourly_grp = hourly_prov.copy()
        hourly_grp["value"] = hourly_vals

        # Detect which hours had sub-hourly aggregation vs. originated from
        # a single raw observation (informational only — no separate flag for this)
        hourly_grp["resample_level"] = "hourly"
        hourly_grp["imputation_flag"] = ""

        # Worst-case scrub flag per hour: convert flags to integer priority,
        # take max, then convert back.  Fully vectorized with no Python apply.
        _FLAG_PRIORITY = {
            SCRUB_FLAG_OK:             0,
            SCRUB_FLAG_DUPE_RESOLVED:  1,
            SCRUB_FLAG_INTERP_SHORT:   2,
            SCRUB_FLAG_EXCL_LONG:      3,
            SCRUB_FLAG_SOURCE_FLAGGED: 4,
            SCRUB_FLAG_RANGE_FAILED:   5,
        }
        _PRIORITY_FLAG = {v: k for k, v in _FLAG_PRIORITY.items()}

        if "quality_flag_scrub" in grp_indexed.columns:
            flag_int = grp_indexed["quality_flag_scrub"].map(_FLAG_PRIORITY).fillna(0)
            worst_int = flag_int.resample("h").max().fillna(0).astype(int)
            hourly_grp["quality_flag_scrub"] = worst_int.map(_PRIORITY_FLAG).fillna(SCRUB_FLAG_OK)
        else:
            hourly_grp["quality_flag_scrub"] = SCRUB_FLAG_OK

        # Forward-fill provenance columns for hours that had no source data
        for col in prov_cols_present:
            hourly_grp[col] = hourly_grp[col].ffill().bfill()

        hourly_grp["quality_flag_source"] = hourly_grp["quality_flag_source"].fillna("")

        # ------------------------------------------------------------------
        # Vectorized gap-tagging and short-gap interpolation
        # ------------------------------------------------------------------
        is_nan = hourly_grp["value"].isna()

        if is_nan.any():
            if var_name not in NO_INTERP_VARS:
                # Label each contiguous NaN run with an integer group id
                gap_id = (is_nan != is_nan.shift()).cumsum()
                # Count the size of each NaN run
                gap_sizes = is_nan.groupby(gap_id).transform("sum")

                short_gap = is_nan & (gap_sizes <= MAX_INTERP_GAP_HOURS)
                long_gap  = is_nan & (gap_sizes >  MAX_INTERP_GAP_HOURS)

                hourly_grp.loc[short_gap, "imputation_flag"] = SCRUB_FLAG_INTERP_SHORT
                hourly_grp.loc[long_gap,  "imputation_flag"] = SCRUB_FLAG_EXCL_LONG

                # Interpolate short gaps; limit= ensures long gaps stay NaN
                hourly_grp["value"] = hourly_grp["value"].interpolate(
                    method="time", limit=MAX_INTERP_GAP_HOURS
                )
                # Re-check what is still NaN (long gaps remain after interpolate)
                still_nan = hourly_grp["value"].isna()
                hourly_grp.loc[still_nan, "imputation_flag"] = SCRUB_FLAG_EXCL_LONG
            else:
                # No interpolation for precip/level variables
                hourly_grp.loc[is_nan, "imputation_flag"] = SCRUB_FLAG_EXCL_LONG

        # Tag interpolated rows with the interpolated scrub flag
        interp_mask = hourly_grp["imputation_flag"] == SCRUB_FLAG_INTERP_SHORT
        hourly_grp.loc[interp_mask, "quality_flag_scrub"] = SCRUB_FLAG_INTERP_SHORT

        hourly_grp["timestamp_utc"] = hourly_grp.index
        out_frames.append(hourly_grp.reset_index(drop=True))

    if not out_frames:
        return pd.DataFrame()

    return pd.concat(out_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Daily aggregation
# ---------------------------------------------------------------------------

def aggregate_to_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate the hourly long-form DataFrame to daily resolution.

    - Daily products are derived from hourly UTC-normalized data.
    - Each (station_code, variable_name_std) group is aggregated using
      variable-specific rules from DAILY_AGG_RULES.
    - A day is defined as the UTC calendar date of the timestamp.
    - The quality_flag_scrub for each day is the worst-case flag value
      across all hourly inputs for that day.
    - resample_level is set to "daily".
    """
    if hourly_df.empty:
        return pd.DataFrame()

    hourly_df = hourly_df.copy()
    hourly_df["timestamp_utc"] = pd.to_datetime(
        hourly_df["timestamp_utc"], utc=True, errors="coerce"
    )
    hourly_df["date_utc"] = hourly_df["timestamp_utc"].dt.floor("D")

    out_frames = []

    for (station_code, var_name), grp in hourly_df.groupby(
        ["station_code", "variable_name_std"], sort=False
    ):
        agg_rule = DAILY_AGG_RULES.get(var_name, "mean")

        daily_vals = grp.groupby("date_utc")["value"].agg(agg_rule)

        # Vectorized worst-case scrub flag per day
        _FLAG_PRIORITY = {
            SCRUB_FLAG_OK:             0,
            SCRUB_FLAG_DUPE_RESOLVED:  1,
            SCRUB_FLAG_INTERP_SHORT:   2,
            SCRUB_FLAG_EXCL_LONG:      3,
            SCRUB_FLAG_SOURCE_FLAGGED: 4,
            SCRUB_FLAG_RANGE_FAILED:   5,
        }
        _PRIORITY_FLAG = {v: k for k, v in _FLAG_PRIORITY.items()}
        flag_int = grp["quality_flag_scrub"].map(_FLAG_PRIORITY).fillna(0)
        grp_tmp = grp.copy()
        grp_tmp["_flag_int"] = flag_int
        worst_int = grp_tmp.groupby("date_utc")["_flag_int"].max().fillna(0).astype(int)
        daily_flags = worst_int.map(_PRIORITY_FLAG).fillna(SCRUB_FLAG_OK)

        # Provenance: take the first value of stable fields per group
        prov_grp = grp.groupby("date_utc").first()[
            ["station_name", "parser_family", "source_file",
             "unit_std", "raw_column_name", "tz_token",
             "schema_variant", "known_issue_tag", "quality_flag_source"]
        ]

        daily_grp = prov_grp.copy()
        daily_grp["station_code"] = station_code
        daily_grp["variable_name_std"] = var_name
        daily_grp["value"] = daily_vals
        daily_grp["quality_flag_scrub"] = daily_flags
        daily_grp["imputation_flag"] = ""
        daily_grp["resample_level"] = "daily"
        daily_grp["timestamp_local_raw"] = ""
        daily_grp["timestamp_utc"] = daily_grp.index  # date_utc becomes timestamp_utc

        out_frames.append(daily_grp.reset_index(drop=True))

    if not out_frames:
        return pd.DataFrame()

    daily_df = pd.concat(out_frames, ignore_index=True)
    return daily_df


# ---------------------------------------------------------------------------
# Completeness metric builder
# ---------------------------------------------------------------------------

def build_completeness_report(
    hourly_df: pd.DataFrame,
    label: str = "hourly",
) -> pd.DataFrame:
    """
    Compute per-(station_code, variable_name_std) completeness metrics
    from a long-form DataFrame.

    Returns a summary DataFrame with columns:
      station_code, variable_name_std, total_rows, n_valid, n_missing,
      pct_complete, n_interpolated, n_excluded_long, label.
    """
    if hourly_df.empty:
        return pd.DataFrame()

    records = []
    for (station_code, var_name), grp in hourly_df.groupby(
        ["station_code", "variable_name_std"], sort=False
    ):
        total = len(grp)
        n_valid = grp["value"].notna().sum()
        n_missing = grp["value"].isna().sum()
        pct = round(100.0 * n_valid / total, 2) if total > 0 else 0.0
        n_interp = (grp["imputation_flag"] == SCRUB_FLAG_INTERP_SHORT).sum()
        n_excl = (grp["imputation_flag"] == SCRUB_FLAG_EXCL_LONG).sum()
        records.append({
            "station_code":     station_code,
            "variable_name_std": var_name,
            "total_rows":       total,
            "n_valid":          int(n_valid),
            "n_missing":        int(n_missing),
            "pct_complete":     pct,
            "n_interpolated":   int(n_interp),
            "n_excluded_long":  int(n_excl),
            "label":            label,
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Artifact writers
# ---------------------------------------------------------------------------

def write_long_form_csv(df: pd.DataFrame, out_path: Path) -> None:
    """Write a long-form DataFrame to CSV, overwriting any prior run."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Convert timezone-aware column to ISO-format string for CSV portability
    df_out = df.copy()
    if "timestamp_utc" in df_out.columns:
        df_out["timestamp_utc"] = df_out["timestamp_utc"].astype(str)
    df_out.to_csv(out_path, index=False)
    log.info("Wrote %d rows -> %s", len(df_out), out_path)


def write_schema_audit(
    schema_rows: list[dict],
    out_path: Path,
) -> None:
    """Write the consolidated schema-mapping audit to CSV."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(schema_rows).to_csv(out_path, index=False)
    log.info("Wrote schema audit (%d rows) -> %s", len(schema_rows), out_path)


def write_completeness_report(df: pd.DataFrame, out_path: Path) -> None:
    """Write the completeness/quality report to CSV."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    log.info(
        "Wrote completeness report (%d rows) -> %s", len(df), out_path
    )


def write_ts_audit(rows: list[dict], out_path: Path) -> None:
    """Write the timestamp-validation audit to CSV."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    log.info("Wrote timestamp audit (%d rows) -> %s", len(rows), out_path)
