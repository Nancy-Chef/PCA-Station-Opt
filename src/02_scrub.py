"""
02_scrub.py – Phase 2: Scrub and Normalize
==========================================
Registry-driven scrub pipeline that processes only the 220 supported CSV
files identified in Phase 1.  Reads each file by parser family, validates
and converts timestamps to UTC, maps raw sensor headers to a canonical
long-form schema, applies conservative gap-aware missing-data rules, and
emits hourly and daily long-form products alongside schema and quality
audit artifacts.

Supported families processed in this phase
-------------------------------------------
PEINP-HOBOlink   Cavendish, Greenwich, Stanley Bridge (2023+),
                 North Rustico Wharf, Tracadie Wharf
ECCC-LST         ECCC Stanhope Weather Station

Deferred (documented but not processed)
-----------------------------------------
special-case-xle           HOBOlink binary logger files
xlsx                       Excel seasonal exports
special-case-csv-metadata  Stanley Bridge 2022 metadata-preamble CSV
unknown / error            Any other unclassified file

Outputs (all written to data/scrubbed/)
-----------------------------------------
phase2_hourly.csv          Long-form hourly UTC-normalized scrubbed data
phase2_daily.csv           Long-form daily aggregates
phase2_schema_audit.csv    Per-column mapping decisions for every source file
phase2_completeness.csv    Station-variable completeness before/after imputation
phase2_ts_audit.csv        Timestamp validation issues per source file

Usage
-----
  python src/02_scrub.py

All outputs overwrite prior runs; reruns are safe and idempotent.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

ROOT         = Path(__file__).resolve().parent.parent
SCRUBBED_DIR = ROOT / "data" / "scrubbed"

REGISTRY_CSV     = SCRUBBED_DIR / "phase1_registry.csv"
HOURLY_OUT       = SCRUBBED_DIR / "phase2_hourly.csv"
DAILY_OUT        = SCRUBBED_DIR / "phase2_daily.csv"
SCHEMA_AUDIT_OUT = SCRUBBED_DIR / "phase2_schema_audit.csv"
COMPLETENESS_OUT = SCRUBBED_DIR / "phase2_completeness.csv"
TS_AUDIT_OUT     = SCRUBBED_DIR / "phase2_ts_audit.csv"

# Families that are in scope for this phase
SUPPORTED_FAMILIES = {"PEINP-HOBOlink", "ECCC-LST"}

# Add /src to sys.path so scrub_utils can be imported directly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scrub_utils import (  # noqa: E402
    aggregate_to_daily,
    build_completeness_report,
    build_eccc_flag_map,
    build_long_form,
    map_eccc_columns,
    map_peinp_columns,
    parse_eccc_timestamps,
    parse_peinp_timestamps,
    read_eccc_file,
    read_peinp_file,
    regularize_to_hourly,
    write_completeness_report,
    write_long_form_csv,
    write_schema_audit,
    write_ts_audit,
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
# Registry loader and filter
# ---------------------------------------------------------------------------

def load_registry(registry_path: Path):
    """
    Load phase1_registry.csv and return only supported rows that belong
    to the PEINP-HOBOlink or ECCC-LST parser families.

    Logs a summary of excluded files so deferred formats are documented
    rather than silently ignored.
    """
    if not registry_path.exists():
        log.error("Registry not found: %s", registry_path)
        sys.exit(1)

    df = pd.read_csv(registry_path)
    total = len(df)

    in_scope = df[
        df["supported"].astype(str).str.lower().isin(["true", "1"])
        & df["parser_family"].isin(SUPPORTED_FAMILIES)
    ].copy()

    deferred = df[~df.index.isin(in_scope.index)]

    log.info("Registry loaded: %d total files", total)
    log.info("  In scope (processing): %d", len(in_scope))
    log.info("  Deferred / excluded:   %d", len(deferred))

    # Log each deferred family so the omission is explicit
    for family, grp in deferred.groupby("parser_family", dropna=False):
        log.info("    Deferred family '%s': %d file(s)", family, len(grp))

    return in_scope


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_peinp_file(
    registry_row,
) -> tuple[list, list[dict], list[dict]]:
    """
    Process one PEINP-HOBOlink CSV file and return:
      (long_form_rows, schema_audit_rows, ts_audit_rows)
    """
    file_path = Path(registry_row["file_path"])
    station_code = str(registry_row["station_code"])
    tz_token = str(registry_row.get("tz_token", ""))

    log.debug("Processing PEINP: %s", file_path.name)

    try:
        df = read_peinp_file(file_path, registry_row)
    except Exception as exc:
        log.warning("Failed to read %s: %s", file_path.name, exc)
        return [], [], [{
            "file": str(file_path),
            "issue": f"read error: {exc}",
            "severity": "error",
        }]

    # Timestamp parsing and UTC normalization
    timestamp_local_raw, timestamp_utc, ts_audit = parse_peinp_timestamps(
        df, tz_token, file_path
    )

    # Variable mapping
    col_assignment, schema_audit, _unmapped = map_peinp_columns(
        df, station_code
    )

    # Annotate schema audit with file provenance
    for row in schema_audit:
        row["source_file"] = str(file_path)
        row["station_code"] = station_code

    # Build long-form records
    long_df = build_long_form(
        df,
        col_assignment,
        timestamp_local_raw,
        timestamp_utc,
        registry_row,
        eccc_flag_map=None,
    )

    return long_df, schema_audit, ts_audit


def process_eccc_file(
    registry_row,
) -> tuple[list, list[dict], list[dict]]:
    """
    Process one ECCC-LST CSV file and return:
      (long_form_df, schema_audit_rows, ts_audit_rows)
    """
    file_path = Path(registry_row["file_path"])
    station_code = str(registry_row["station_code"])

    log.debug("Processing ECCC: %s", file_path.name)

    try:
        df = read_eccc_file(file_path, registry_row)
    except Exception as exc:
        log.warning("Failed to read %s: %s", file_path.name, exc)
        return [], [], [{
            "file": str(file_path),
            "issue": f"read error: {exc}",
            "severity": "error",
        }]

    # Timestamp parsing and UTC normalization
    timestamp_local_raw, timestamp_utc, ts_audit = parse_eccc_timestamps(
        df, file_path
    )

    # Source-flag column map
    eccc_flag_map = build_eccc_flag_map(df)

    # Variable mapping
    col_assignment, schema_audit, _unmapped = map_eccc_columns(df)

    for row in schema_audit:
        row["source_file"] = str(file_path)
        row["station_code"] = station_code

    # Build long-form records
    long_df = build_long_form(
        df,
        col_assignment,
        timestamp_local_raw,
        timestamp_utc,
        registry_row,
        eccc_flag_map=eccc_flag_map,
    )

    return long_df, schema_audit, ts_audit


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate the full Phase 2 scrub pipeline:
      1. Load and filter the phase-1 registry.
      2. Process each supported file by parser family.
      3. Concatenate all long-form records.
      4. Regularize to hourly cadence with conservative gap imputation.
      5. Aggregate to daily.
      6. Write all output artifacts.
      7. Print a concise terminal summary.
    """
    # Step 1: Load registry
    registry = load_registry(REGISTRY_CSV)

    all_long_frames = []
    all_schema_audit = []
    all_ts_audit = []

    processed = 0
    skipped = 0

    # Steps 2–3: Process each file
    for _, row in registry.iterrows():
        family = str(row["parser_family"])

        if family == "PEINP-HOBOlink":
            long_df, s_audit, ts_audit = process_peinp_file(row)
        elif family == "ECCC-LST":
            long_df, s_audit, ts_audit = process_eccc_file(row)
        else:
            # Should not occur because load_registry filters to supported families
            log.warning("Unexpected family '%s' – skipping %s", family, row["file_path"])
            skipped += 1
            continue

        if isinstance(long_df, list) and len(long_df) == 0:
            # File produced no records (likely a read error already logged)
            skipped += 1
        else:
            all_long_frames.append(long_df)
            processed += 1

        all_schema_audit.extend(s_audit)
        all_ts_audit.extend(ts_audit)

        if processed % 25 == 0 and processed > 0:
            log.info("  Progress: %d files processed ...", processed)

    log.info("File processing complete: %d processed, %d skipped", processed, skipped)

    if not all_long_frames:
        log.error("No records produced. Aborting artifact writes.")
        sys.exit(1)

    # Concatenate all long-form records
    log.info("Concatenating long-form records ...")
    long_df_raw = pd.concat(all_long_frames, ignore_index=True)
    log.info("  Total raw long-form rows: %d", len(long_df_raw))

    # Step 4: Regularize to hourly UTC
    log.info("Regularizing to hourly cadence ...")
    hourly_df = regularize_to_hourly(long_df_raw)
    log.info("  Total hourly rows: %d", len(hourly_df))

    # Completeness before imputation (based on raw long-form)
    completeness_raw = build_completeness_report(long_df_raw, label="raw")

    # Completeness after hourly regularization and imputation
    completeness_hourly = build_completeness_report(hourly_df, label="hourly")

    # Step 5: Aggregate to daily
    log.info("Aggregating to daily ...")
    daily_df = aggregate_to_daily(hourly_df)
    log.info("  Total daily rows: %d", len(daily_df))

    completeness_daily = build_completeness_report(daily_df, label="daily")

    # Step 6: Write artifacts
    log.info("Writing output artifacts ...")

    write_long_form_csv(hourly_df, HOURLY_OUT)
    write_long_form_csv(daily_df,  DAILY_OUT)
    write_schema_audit(all_schema_audit, SCHEMA_AUDIT_OUT)

    all_completeness = pd.concat(
        [completeness_raw, completeness_hourly, completeness_daily],
        ignore_index=True,
    )
    write_completeness_report(all_completeness, COMPLETENESS_OUT)
    write_ts_audit(all_ts_audit, TS_AUDIT_OUT)

    all_completeness = pd.concat(
        [completeness_raw, completeness_hourly, completeness_daily],
        ignore_index=True,
    )
    write_completeness_report(all_completeness, COMPLETENESS_OUT)

    # Step 7: Terminal summary
    _print_summary(
        registry=registry,
        processed=processed,
        skipped=skipped,
        n_hourly=len(hourly_df),
        n_daily=len(daily_df),
        ts_audit=all_ts_audit,
        completeness=completeness_hourly,
    )


def _print_summary(
    registry,
    processed: int,
    skipped: int,
    n_hourly: int,
    n_daily: int,
    ts_audit: list[dict],
    completeness,
) -> None:
    """Print a concise terminal summary of the scrub run."""
    n_ts_errors   = sum(1 for r in ts_audit if r.get("severity") == "error")
    n_ts_warnings = sum(1 for r in ts_audit if r.get("severity") == "warning")

    print("\n" + "=" * 60)
    print("Phase 2 Scrub Summary")
    print("=" * 60)
    print(f"  Registry files in scope:  {len(registry)}")
    print(f"  Files processed:          {processed}")
    print(f"  Files skipped (errors):   {skipped}")
    print(f"  Hourly long-form rows:    {n_hourly:,}")
    print(f"  Daily long-form rows:     {n_daily:,}")
    print(f"  Timestamp audit errors:   {n_ts_errors}")
    print(f"  Timestamp audit warnings: {n_ts_warnings}")

    if not completeness.empty:
        print("\n  Station-variable completeness (hourly, %):")
        summary = (
            completeness
            .groupby("station_code")["pct_complete"]
            .mean()
            .round(1)
            .reset_index()
        )
        for _, r in summary.iterrows():
            print(f"    {r['station_code']:6s}  {r['pct_complete']:5.1f}%")

    print("\n  Outputs written to data/scrubbed/:")
    print("    phase2_hourly.csv")
    print("    phase2_daily.csv")
    print("    phase2_schema_audit.csv")
    print("    phase2_completeness.csv")
    print("    phase2_ts_audit.csv")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
