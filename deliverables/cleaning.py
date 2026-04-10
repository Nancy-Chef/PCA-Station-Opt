"""Consolidated data ingestion and cleaning entry point for submission.

This wrapper keeps the assignment deliverable aligned with the tested project
implementation by reusing the existing Phase 1 and Phase 2 scripts rather than
copying their logic into a second pipeline.

Usage:
    python deliverables/cleaning.py
    python deliverables/cleaning.py --skip-phase1
"""

from __future__ import annotations

import argparse
import runpy
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PHASE_1_SCRIPT = ROOT / "src" / "01_obtain.py"
PHASE_2_SCRIPT = ROOT / "src" / "02_scrub.py"


def _run_script(script_path: Path) -> None:
    """Execute one existing pipeline script in its own __main__ scope."""
    runpy.run_path(str(script_path), run_name="__main__")


def parse_args() -> argparse.Namespace:
    """Parse optional flags so the wrapper can resume from Phase 2 if needed."""
    parser = argparse.ArgumentParser(
        description="Run the submission-ready ingestion and cleaning pipeline."
    )
    parser.add_argument(
        "--skip-phase1",
        action="store_true",
        help="Reuse the existing phase1 registry and start from Phase 2.",
    )
    return parser.parse_args()


def main() -> None:
    """Run Phase 1 obtain and Phase 2 scrub in sequence."""
    args = parse_args()

    if not args.skip_phase1:
        print("[cleaning.py] Running Phase 1 obtain/registry pipeline...")
        _run_script(PHASE_1_SCRIPT)
        print(
            "[cleaning.py] Phase 1 complete. Summary: discovered raw files, "
            "classified schemas, and wrote the registry artifacts."
        )

    print("[cleaning.py] Running Phase 2 cleaning/normalization pipeline...")
    _run_script(PHASE_2_SCRIPT)
    print(
        "[cleaning.py] Phase 2 complete. Summary: normalized timestamps to UTC, "
        "standardized variables, applied conservative gap handling, and wrote "
        "hourly/daily scrubbed outputs."
    )


if __name__ == "__main__":
    main()