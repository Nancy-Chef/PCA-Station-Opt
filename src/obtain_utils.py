"""
obtain_utils.py
===============
Helper functions for Phase 1 raw-data discovery and schema reconnaissance.

All functions in this module are strictly observational: no variable
renaming, unit conversion, or UTC normalization is performed.  Phase 2
(02_scrub.py) reads the registry artifacts produced here and builds on
the verified source-file behaviour that this module documents.

Station families detected
-------------------------
PEINP-HOBOlink             Cavendish, Greenwich, Stanley Bridge (2023+),
                           North Rustico Wharf, Tracadie Wharf
ECCC-LST                   ECCC Stanhope Weather Station
special-case-csv-metadata  Stanley Bridge 2022 metadata-preamble CSV
special-case-xle           HOBOlink binary/compressed logger files (.xle)
xlsx                       Excel seasonal exports (.xlsx / .xls)
"""

import csv
import json
import logging
import re
import traceback
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Map lower-cased station directory name -> (station_code, station_name)
STATION_META = {
    "cavendish":                     ("CAV", "Cavendish"),
    "greenwich":                     ("GRE", "Greenwich"),
    "eccc stanhope weather station": ("STA", "ECCC Stanhope"),
    "stanley bridge wharf":          ("SBW", "Stanley Bridge Wharf"),
    "north rustico wharf":           ("NRW", "North Rustico Wharf"),
    "tracadie wharf":                ("TRW", "Tracadie Wharf"),
}

SUPPORTED_EXTENSIONS   = {".csv"}
UNSUPPORTED_EXTENSIONS = {".xle", ".xlsx", ".xls"}

# Month abbreviation / full-name -> integer month number
# Covers all spelling variants seen in the raw filenames.
MONTH_TOKEN_MAP = {
    "jan": 1,  "january":   1,
    "feb": 2,  "february":  2,
    "mar": 3,  "march":     3,
    "apr": 4,  "april":     4,
    "may": 5,
    "jun": 6,  "june":      6,
    "jul": 7,  "july":      7,
    "aug": 8,  "august":    8,
    "sep": 9,  "sept":      9,  "september": 9,
    "oct": 10, "october":   10,
    "nov": 11, "november":  11,
    "dec": 12, "december":  12,
}

# Ordered column list for the registry CSV output
REGISTRY_COLUMNS = [
    "file_path", "station_code", "station_name", "year", "month_token",
    "month_num", "filename", "extension", "parser_family", "supported",
    "parse_status", "header_row", "delimiter", "col_count", "row_sample",
    "ts_pattern", "tz_token", "ts_first_local", "ts_last_local",
    "utc_feasible", "schema_notes", "known_issues", "raw_columns",
]


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def walk_raw_directory(raw_root: Path) -> list:
    """
    Recursively walk *raw_root* and return a flat list of dicts, one per
    file, containing only the raw ``file_path``.  Files with unrecognised
    extensions that are not blank are still included so nothing is silently
    dropped from the registry.

    Results are sorted by path for deterministic, reproducible output.
    """
    records = []
    for file_path in sorted(raw_root.rglob("*")):
        if not file_path.is_file():
            continue
        ext = file_path.suffix.lower()
        # Include supported CSVs, known-unsupported types, and any other
        # non-empty extension so they appear in the skipped section.
        if ext or file_path.name != file_path.stem:
            records.append({"file_path": file_path})
    return records


# ---------------------------------------------------------------------------
# Station and filename metadata
# ---------------------------------------------------------------------------

def extract_station_metadata(file_path: Path, raw_root: Path) -> dict:
    """
    Derive station_code, station_name, year, month_token, and month_num
    from the directory structure and filename.  No month list is
    hard-coded; extraction uses regex so novel abbreviations are logged as
    empty strings rather than causing failures.

    Expected directory layout::

        <raw_root>/<station_dir>/<year>/<optional_subdir>/<file>

    Assumptions are documented in the returned dict.
    """
    try:
        rel = file_path.relative_to(raw_root)
    except ValueError:
        rel = file_path

    parts = rel.parts  # e.g. ("Cavendish", "2023", "PEINP_Cav_WeatherStn_Apr2023.csv")

    station_dir = parts[0].strip() if len(parts) >= 1 else ""
    year_token  = parts[1].strip() if len(parts) >= 2 else ""
    stem        = file_path.stem   # filename without extension

    # Map station directory to canonical code and display name
    key = station_dir.lower()
    if key in STATION_META:
        station_code, station_name = STATION_META[key]
    else:
        # Fallback: derive a short code from the directory name
        station_code = re.sub(r"[^A-Z0-9]", "", station_dir.upper())[:6]
        station_name = station_dir

    # Validate year token (must be a four-digit number)
    year = year_token if re.fullmatch(r"\d{4}", year_token) else ""

    month_token = ""
    month_num   = ""

    # --- Pattern 1: ECCC Stanhope --- Stanhope_Hourly_YYYY_MM
    m = re.search(r"_(\d{2})$", stem)
    if m:
        month_token = m.group(1)
        try:
            month_num = str(int(month_token))
        except ValueError:
            pass

    # --- Pattern 2: PEINP and others --- <prefix>_MonthYYYY or _MonthYY ---
    if not month_token:
        m = re.search(r"_([A-Za-z]+)\d{2,4}$", stem)
        if m:
            month_token = m.group(1)
            month_num_val = MONTH_TOKEN_MAP.get(month_token.lower())
            month_num = str(month_num_val) if month_num_val else ""

    # --- Pattern 3: date-range filenames --- Station_YYYY-MM-DD_YYYY-MM-DD ---
    if not month_token:
        m = re.search(r"[_ ](\d{4}-\d{2}-\d{2})[_ ]", stem)
        if m:
            month_token = "date-range"
            try:
                month_num = str(int(m.group(1)[5:7]))
            except ValueError:
                pass

    return {
        "station_code": station_code,
        "station_name": station_name,
        "year":         year,
        "month_token":  month_token,
        "month_num":    month_num,
        "filename":     file_path.name,
    }


# ---------------------------------------------------------------------------
# Parser-family classification
# ---------------------------------------------------------------------------

def classify_file(file_path: Path, station_code: str, year: str) -> dict:
    """
    Classify a file into a parser family using its extension and, for CSV
    files, the content of the first non-empty line.

    Never raises; parsing errors are captured in the returned dict under
    ``parse_status`` so the registry run can continue.
    """
    ext = file_path.suffix.lower()

    # --- Non-CSV types are documented and skipped ---
    if ext == ".xle":
        return {
            "parser_family": "special-case-xle",
            "supported":     False,
            "parse_status":  "skipped-unsupported",
            "known_issues":  (
                "HOBOlink binary/compressed logger file. "
                "Requires Onset SDK or manual CSV export to parse."
            ),
        }

    if ext in (".xlsx", ".xls"):
        return {
            "parser_family": "xlsx",
            "supported":     False,
            "parse_status":  "skipped-unsupported",
            "known_issues":  (
                "Excel file; likely a seasonal data export. "
                "Requires openpyxl reader in a future phase."
            ),
        }

    if ext != ".csv":
        return {
            "parser_family": "unknown",
            "supported":     False,
            "parse_status":  "skipped-unknown-extension",
            "known_issues":  f"Unrecognised file extension: {ext}",
        }

    # --- CSV: inspect first non-empty line to determine family ---
    try:
        first_line = _read_first_nonempty_line(file_path)
    except Exception as exc:
        return {
            "parser_family": "error",
            "supported":     False,
            "parse_status":  f"read-error: {exc}",
            "known_issues":  traceback.format_exc(limit=2),
        }

    # ECCC Stanhope: header contains Government Climate Data column names
    if "Date/Time (LST)" in first_line or "Station Name" in first_line:
        return {
            "parser_family": "ECCC-LST",
            "supported":     True,
            "parse_status":  "ok",
            "known_issues":  "",
        }

    # Stanley Bridge 2022 metadata-preamble CSV
    if "Serial_number" in first_line:
        return {
            "parser_family": "special-case-csv-metadata",
            "supported":     False,
            "parse_status":  "skipped-unsupported",
            "known_issues":  (
                "Stanley Bridge 2022 CSV with multi-row metadata preamble "
                "(Serial_number, Project ID, Location …). Data header begins "
                "after the metadata block. Requires a dedicated parser."
            ),
        }

    # PEINP-HOBOlink: Date,Time header with embedded sensor ID strings
    # Sensor IDs appear as patterns like:  S-THB 21114839:20824084
    if first_line.startswith("Date,Time,"):
        if re.search(r"S-[A-Z]{2,4}\s+\d{8}", first_line):
            return {
                "parser_family": "PEINP-HOBOlink",
                "supported":     True,
                "parse_status":  "ok",
                "known_issues":  "",
            }
        # Date,Time header without visible sensor IDs – still treat as PEINP
        return {
            "parser_family": "PEINP-HOBOlink",
            "supported":     True,
            "parse_status":  "ok",
            "known_issues":  (
                "Classified as PEINP-HOBOlink by Date,Time header; "
                "sensor IDs not detected in first line."
            ),
        }

    # Unrecognised CSV structure
    return {
        "parser_family": "unknown-csv",
        "supported":     False,
        "parse_status":  "skipped-unrecognised",
        "known_issues":  f"Unrecognised CSV header: {first_line[:120]}",
    }


def _read_first_nonempty_line(file_path: Path) -> str:
    """
    Return the first non-blank line of *file_path* as a plain string.
    Tries UTF-8 (with BOM) first, then plain UTF-8, then cp1252.
    """
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            with open(file_path, "r", encoding=encoding, errors="replace") as fh:
                for line in fh:
                    stripped = line.strip()
                    if stripped:
                        return stripped
            return ""
        except Exception:
            continue
    return ""


# ---------------------------------------------------------------------------
# Schema inspection for supported CSV files
# ---------------------------------------------------------------------------

def inspect_csv_schema(file_path: Path, parser_family: str) -> dict:
    """
    Read a small sample of *file_path* to detect: delimiter, header row
    index, raw column names, column count, a sample row count, and any
    anomalies such as embedded sensor IDs, duplicate column base-names, or
    double-spaced headers.

    Column names are preserved exactly as they appear in the source file;
    no renaming is performed here.
    """
    result = {
        "header_row":  "",
        "delimiter":   ",",
        "col_count":   "",
        "row_sample":  "",
        "raw_columns": "",
        "schema_notes": "",
    }

    try:
        if parser_family == "PEINP-HOBOlink":
            df = _read_peinp_sample(file_path, n_rows=20)
            result["header_row"] = 0
        elif parser_family == "ECCC-LST":
            df = _read_eccc_sample(file_path, n_rows=20)
            result["header_row"] = 0
        else:
            return result

        result["delimiter"]   = ","
        result["col_count"]   = len(df.columns)
        result["row_sample"]  = len(df)
        result["raw_columns"] = "|".join(str(c) for c in df.columns)

        notes = []

        # Anomaly: possible duplicate variable names (e.g., wind speed in
        # both km/h and m/s columns sharing the same base name)
        col_bases = [
            re.sub(r"\s*\(.*?\)", "", str(c)).strip().lower()
            for c in df.columns
        ]
        seen_bases: dict = {}
        for i, base in enumerate(col_bases):
            if base in seen_bases:
                notes.append(
                    f"Possible duplicate: '{df.columns[i]}' vs "
                    f"'{df.columns[seen_bases[base]]}'"
                )
            else:
                seen_bases[base] = i

        # Anomaly: double-space inside a column name (seen in Tracadie files)
        for col in df.columns:
            if "  " in str(col):
                notes.append(f"Double-space in column name: '{col}'")

        # Informational: count columns that embed sensor hardware IDs
        sensor_id_cols = [
            c for c in df.columns if re.search(r"\d{8}:\d{8}", str(c))
        ]
        if sensor_id_cols:
            notes.append(
                f"Sensor IDs embedded in {len(sensor_id_cols)} column(s)"
            )

        result["schema_notes"] = "; ".join(notes) if notes else ""

    except Exception as exc:
        result["schema_notes"] = f"Inspection error: {exc}"
        log.warning("Schema inspection failed for %s: %s", file_path.name, exc)

    return result


def _read_peinp_sample(file_path: Path, n_rows: int = 20) -> pd.DataFrame:
    """
    Read up to *n_rows* data rows from a PEINP-HOBOlink CSV.
    Header is always row 0.  Tries UTF-8 encodings first, falls back to
    cp1252 for legacy files.
    """
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return pd.read_csv(
                file_path,
                header=0,
                nrows=n_rows,
                encoding=encoding,
                low_memory=False,
                na_values=["", " ", "NA", "None", "ERROR"],
                keep_default_na=True,
            )
        except UnicodeDecodeError:
            continue
    raise ValueError(
        f"Could not read {file_path.name} with UTF-8 or cp1252 encoding"
    )


def _read_eccc_sample(file_path: Path, n_rows: int = 20) -> pd.DataFrame:
    """
    Read up to *n_rows* data rows from an ECCC-LST CSV.
    ECCC exports contain a blank line between every data row; pandas
    ``skip_blank_lines=True`` (the default) handles this transparently.
    """
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return pd.read_csv(
                file_path,
                header=0,
                nrows=n_rows,
                encoding=encoding,
                skip_blank_lines=True,
                low_memory=False,
                na_values=["", " ", "NA", "None"],
                keep_default_na=True,
            )
        except UnicodeDecodeError:
            continue
    raise ValueError(
        f"Could not read {file_path.name} with UTF-8 or cp1252 encoding"
    )


# ---------------------------------------------------------------------------
# Timestamp-convention sampling
# ---------------------------------------------------------------------------

def sample_timestamps(
    file_path: Path, parser_family: str, raw_columns: str
) -> dict:
    """
    Detect the timestamp convention in use and sample the first and last
    local timestamps in *file_path*.

    No UTC conversion is performed.  The ``utc_feasible`` flag and
    ``tz_token`` values are recorded so Phase 2 can build its normalization
    logic on verified source behaviour.
    """
    result = {
        "ts_pattern":     "",
        "tz_token":       "",
        "ts_first_local": "",
        "ts_last_local":  "",
        "utc_feasible":   "",
    }

    try:
        if parser_family == "PEINP-HOBOlink":
            result.update(_sample_peinp_timestamps(file_path))
        elif parser_family == "ECCC-LST":
            result.update(_sample_eccc_timestamps(file_path))
    except Exception as exc:
        result["ts_pattern"] = f"sampling-error: {exc}"
        log.warning(
            "Timestamp sampling failed for %s: %s", file_path.name, exc
        )

    return result


def _sample_peinp_timestamps(file_path: Path) -> dict:
    """
    PEINP timestamp convention:
      - Separate ``Date`` (MM/DD/YYYY) and ``Time`` (HH:MM:SS ±HHMM) columns
      - UTC offset is embedded as a suffix in the Time string, e.g. -0300
      - This offset represents Atlantic Standard Time (AST, UTC-4) but
        historically some loggers report -0300 year-round; Phase 2 must
        verify the offset per file rather than assuming a fixed constant.
    """
    df_head = _read_peinp_sample(file_path, n_rows=10)

    date_col = next(
        (c for c in df_head.columns if str(c).strip().lower() == "date"), None
    )
    time_col = next(
        (c for c in df_head.columns if str(c).strip().lower() == "time"), None
    )

    if date_col is None or time_col is None:
        return {
            "ts_pattern":   "unknown-missing-date-time-cols",
            "utc_feasible": "no",
        }

    valid_head = df_head[[date_col, time_col]].dropna()

    # Read the full file to obtain the last timestamp
    df_full = _read_full_peinp(file_path)
    valid_tail = (
        df_full[[date_col, time_col]].dropna()
        if date_col in df_full.columns
        else valid_head
    )

    first_dt = (
        f"{valid_head.iloc[0][date_col]} {valid_head.iloc[0][time_col]}"
        if not valid_head.empty
        else ""
    )
    last_dt = (
        f"{valid_tail.iloc[-1][date_col]} {valid_tail.iloc[-1][time_col]}"
        if not valid_tail.empty
        else ""
    )

    # Extract the UTC offset token from the first valid Time value
    # e.g. "00:00:00 -0300"  ->  "-0300"
    tz_token = ""
    if not valid_head.empty:
        tz_match = re.search(r"([+-]\d{4})", str(valid_head.iloc[0][time_col]))
        tz_token = tz_match.group(1) if tz_match else ""

    return {
        "ts_pattern":     "PEINP-Date/Time-with-offset",
        "tz_token":       tz_token,
        "ts_first_local": first_dt,
        "ts_last_local":  last_dt,
        "utc_feasible":   "yes" if tz_token else "partial",
    }


def _sample_eccc_timestamps(file_path: Path) -> dict:
    """
    ECCC timestamp convention:
      - Single ``Date/Time (LST)`` column in ISO 8601 format: YYYY-MM-DD HH:MM
      - LST = Local Standard Time = Atlantic Standard Time = UTC-4.
        No offset is embedded in the string; Phase 2 must apply a fixed
        -4 h offset for UTC conversion.
    """
    df_head = _read_eccc_sample(file_path, n_rows=10)

    dt_col = next(
        (c for c in df_head.columns if "Date/Time" in str(c)), None
    )
    if dt_col is None:
        return {
            "ts_pattern":   "unknown-missing-datetime-col",
            "utc_feasible": "no",
        }

    valid_head = df_head[[dt_col]].dropna()
    first_dt   = str(valid_head.iloc[0][dt_col]) if not valid_head.empty else ""

    df_full   = _read_full_eccc(file_path)
    dt_col_f  = next(
        (c for c in df_full.columns if "Date/Time" in str(c)), None
    )
    valid_full = df_full[dt_col_f].dropna() if dt_col_f else pd.Series(dtype=str)
    last_dt    = str(valid_full.iloc[-1]) if not valid_full.empty else ""

    return {
        "ts_pattern":     "ECCC-ISO-LST",
        "tz_token":       "LST(UTC-4)",
        "ts_first_local": first_dt,
        "ts_last_local":  last_dt,
        "utc_feasible":   "yes-with-fixed-offset",
    }


def _read_full_peinp(file_path: Path) -> pd.DataFrame:
    """Read all data rows from a PEINP CSV (used for last-timestamp sampling)."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return pd.read_csv(
                file_path,
                header=0,
                encoding=encoding,
                low_memory=False,
                na_values=["", " ", "NA", "None", "ERROR"],
                keep_default_na=True,
            )
        except UnicodeDecodeError:
            continue
    return pd.DataFrame()


def _read_full_eccc(file_path: Path) -> pd.DataFrame:
    """Read all data rows from an ECCC CSV (used for last-timestamp sampling)."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return pd.read_csv(
                file_path,
                header=0,
                encoding=encoding,
                skip_blank_lines=True,
                low_memory=False,
                na_values=["", " ", "NA", "None"],
                keep_default_na=True,
            )
        except UnicodeDecodeError:
            continue
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Artifact writing
# ---------------------------------------------------------------------------

def write_registry_csv(records: list, output_path: Path) -> None:
    """
    Write the flat file registry to *output_path* as a UTF-8 CSV.
    Overwrites any prior run so repeated executions are idempotent.
    Only the columns in ``REGISTRY_COLUMNS`` are written; extra fields in
    individual records are silently dropped.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=REGISTRY_COLUMNS, extrasaction="ignore"
        )
        writer.writeheader()
        for rec in records:
            writer.writerow(
                {col: rec.get(col, "") for col in REGISTRY_COLUMNS}
            )


def write_json_summary(records: list, output_path: Path) -> None:
    """
    Write a machine-readable JSON summary to *output_path*.

    The summary includes:
      - Total / supported / skipped / errored file counts
      - File counts by station and by parser family
      - Unique schema variants and timestamp patterns observed
      - A list of files that require manual follow-up (unsupported types,
        anomalous schemas, or read errors)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    by_station: dict = {}
    by_family:  dict = {}
    supported   = 0
    skipped     = 0
    errored     = 0
    follow_up   = []

    for rec in records:
        code   = rec.get("station_code", "UNKNOWN")
        family = rec.get("parser_family", "unknown")
        status = str(rec.get("parse_status", ""))

        by_station[code]  = by_station.get(code, 0)  + 1
        by_family[family] = by_family.get(family, 0) + 1

        if rec.get("supported"):
            supported += 1
        else:
            skipped += 1

        if status.startswith(("error", "read-error")):
            errored += 1

        # Flag files that need manual attention
        needs_follow_up = (
            not rec.get("supported")
            or rec.get("known_issues")
            or (rec.get("schema_notes") and "error" in str(rec.get("schema_notes", "")).lower())
        )
        if needs_follow_up:
            follow_up.append({
                "file":   str(rec.get("file_path", "")),
                "family": family,
                "issue":  rec.get("known_issues") or rec.get("schema_notes") or "",
            })

    ts_patterns     = sorted({rec.get("ts_pattern")  for rec in records if rec.get("ts_pattern")})
    schema_variants = sorted({rec.get("parser_family") for rec in records if rec.get("parser_family")})

    summary = {
        "totals": {
            "all_files": len(records),
            "supported": supported,
            "skipped":   skipped,
            "errored":   errored,
        },
        "by_station":       by_station,
        "by_parser_family": by_family,
        "schema_variants":  schema_variants,
        "ts_patterns":      ts_patterns,
        "follow_up_files":  follow_up,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)


def print_terminal_summary(records: list) -> None:
    """
    Print a concise human-readable summary of the discovery run to stdout.
    Caps the warning list at 10 lines; the full detail is in the registry CSV.
    """
    total     = len(records)
    supported = sum(1 for r in records if r.get("supported"))
    skipped   = total - supported
    warnings  = [
        r for r in records
        if r.get("schema_notes") or str(r.get("parse_status", "")).startswith("error")
    ]

    by_station: dict = {}
    for r in records:
        code = r.get("station_code", "UNKNOWN")
        by_station.setdefault(code, {"supported": 0, "skipped": 0})
        if r.get("supported"):
            by_station[code]["supported"] += 1
        else:
            by_station[code]["skipped"] += 1

    separator = "=" * 62
    print(f"\n{separator}")
    print("  PHASE 1 – RAW FILE DISCOVERY SUMMARY")
    print(separator)
    print(f"  Total files found  : {total}")
    print(f"  Supported CSV      : {supported}")
    print(f"  Skipped            : {skipped}")
    print(f"  With schema notes  : {len(warnings)}")
    print()
    print("  File counts by station:")
    for code, counts in sorted(by_station.items()):
        print(
            f"    {code:<6}  supported={counts['supported']:<4}"
            f"  skipped={counts['skipped']}"
        )
    if warnings:
        print()
        print("  Files with schema notes (first 10):")
        for r in warnings[:10]:
            fname = Path(str(r.get("file_path", ""))).name
            note  = r.get("schema_notes") or r.get("parse_status", "")
            print(f"    {fname}  ->  {str(note)[:80]}")
        if len(warnings) > 10:
            print(f"    … and {len(warnings) - 10} more (see registry CSV)")
    print(f"{separator}\n")
