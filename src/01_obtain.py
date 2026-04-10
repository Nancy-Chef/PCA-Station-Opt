"""
01_obtain.py – Phase 1: Obtain and Registry Building
=====================================================
Inventories every raw file under /data/raw, samples each supported CSV to
detect schema and timestamp conventions, and emits two artifacts:

  data/scrubbed/phase1_registry.csv   – one row per discovered file
  data/scrubbed/phase1_summary.json   – machine-readable counts and anomalies

This script is strictly observational.  No variable renaming, unit
conversion, or UTC normalisation is performed.  Phase 2 (02_scrub.py)
reads these artifacts and builds on the source-file behaviour documented
here.

Supported station families after this run
------------------------------------------
PEINP-HOBOlink             Cavendish, Greenwich, Stanley Bridge (2023+),
                           North Rustico Wharf, Tracadie Wharf
ECCC-LST                   ECCC Stanhope Weather Station

Skipped (documented but not parsed)
-------------------------------------
special-case-xle            HOBOlink binary logger files (.xle)
xlsx                        Excel seasonal exports (.xlsx / .xls)
special-case-csv-metadata   Stanley Bridge 2022 metadata-preamble CSV

Usage
-----
  python src/01_obtain.py

Outputs overwrite any prior run – reruns are safe and idempotent.
"""

import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

# Workspace root is one level above /src regardless of the working directory.
ROOT = Path(__file__).resolve().parent.parent

RAW_DIR      = ROOT / "data" / "raw"
SCRUBBED_DIR = ROOT / "data" / "scrubbed"

REGISTRY_CSV = SCRUBBED_DIR / "phase1_registry.csv"
SUMMARY_JSON = SCRUBBED_DIR / "phase1_summary.json"

# Add /src to sys.path so obtain_utils can be imported directly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from obtain_utils import (  # noqa: E402
    extract_station_metadata,
    classify_file,
    inspect_csv_schema,
    print_terminal_summary,
    sample_timestamps,
    walk_raw_directory,
    write_json_summary,
    write_registry_csv,
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
# Registry builder
# ---------------------------------------------------------------------------

def build_registry(raw_root: Path) -> list:
    """
    Walk *raw_root* recursively and build a complete per-file registry.

    For each discovered file the function:
      1. Extracts station metadata from the directory path and filename.
      2. Classifies the file into a parser family (or skipped category).
      3. Inspects the schema of supported CSV files (header row, columns,
         anomalies) without renaming or transforming any values.
      4. Samples the first and last local timestamps for supported files.

    Returns a list of dicts – one per file – with all metadata fields
    needed by write_registry_csv and write_json_summary.
    """
    log.info("Walking raw directory: %s", raw_root)
    file_entries = walk_raw_directory(raw_root)
    log.info("Candidate files found: %d", len(file_entries))

    records = []

    for entry in file_entries:
        file_path = entry["file_path"]
        log.debug("Processing: %s", file_path.name)

        # Step 1 – derive station code/name, year, month token from path
        meta = extract_station_metadata(file_path, raw_root)

        # Step 2 – classify into parser family using extension + header
        classification = classify_file(
            file_path,
            station_code=meta["station_code"],
            year=meta["year"],
        )

        record: dict = {
            "file_path": str(file_path),
            "extension": file_path.suffix.lower(),
            **meta,
            **classification,
        }

        # Steps 3 and 4 only run for files the parser can read
        if classification.get("supported"):
            # Step 3 – lightweight schema inspection (no data transformation)
            schema = inspect_csv_schema(
                file_path, classification["parser_family"]
            )
            record.update(schema)

            # Step 4 – timestamp-convention sampling (no UTC conversion)
            ts = sample_timestamps(
                file_path,
                classification["parser_family"],
                schema.get("raw_columns", ""),
            )
            record.update(ts)

        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Build the registry, write both artifacts, and print a run summary."""

    if not RAW_DIR.exists():
        log.error("Raw data directory not found: %s", RAW_DIR)
        sys.exit(1)

    # Build registry
    records = build_registry(RAW_DIR)

    # Write artifacts (overwrite any prior run)
    log.info("Writing registry CSV  -> %s", REGISTRY_CSV)
    write_registry_csv(records, REGISTRY_CSV)

    log.info("Writing JSON summary  -> %s", SUMMARY_JSON)
    write_json_summary(records, SUMMARY_JSON)

    # Human-readable terminal summary
    print_terminal_summary(records)

    log.info("Phase 1 complete.")


if __name__ == "__main__":
    main()
