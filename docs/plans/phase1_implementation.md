## Plan: Phase 1 Obtain Implementation

Build phase 1 as a narrow, reliable raw-ingest and registry step that inventories every raw CSV under `/data/raw`, samples each station family to detect schema and timestamp differences, and emits two inspection-friendly artifacts: a row-level file registry CSV and a machine-readable JSON summary. Keep `01_obtain.py` focused on discovery and normalization metadata only, while documenting but skipping non-CSV files such as `.xlsx` and `.xle` in this phase.

**Steps**
1. Define the phase-1 scope and artifact contract.
   - Confirm that phase 1 covers raw CSV discovery, schema reconnaissance, timestamp-convention capture, and registry artifact generation only.
   - Treat `.xlsx` and `.xle` files as out of scope for parsing in this phase; include them in a skipped-files section so later phases can decide whether to support them.
   - Lock the two output artifacts up front: a file-level registry CSV for inspection and filtering, and a JSON summary for downstream automation.

2. Establish the module layout under `/src`. *Blocks step 3 onward.*
   - Create one thin entrypoint script for phase 1 orchestration.
   - Create small helper modules for path discovery, filename and station metadata extraction, schema inspection, datetime sampling, and artifact writing.
   - Keep functions narrow and composable so phase 2 can reuse station metadata and schema-detection results without re-reading plan logic.

3. Implement raw-file discovery and station metadata extraction. *Depends on step 2.*
   - Recursively walk `/data/raw` and collect all candidate files.
   - Separate supported CSV files from unsupported-but-documented files such as `.xlsx`, `.xle`, and other extensions.
   - Derive station name, station code, year, and filename tokens from the directory structure and filename instead of hard-coding month lists.
   - Add robust handling for filename irregularities such as `Apr` or `April`, `Jul` or `July`, `Sep` or `Sept`, and date-range names.

4. Implement station-family classification. *Depends on step 3.*
   - Classify each discovered file into a parser family such as `PEINP-HOBOlink`, `ECCC-Stanhope`, or unsupported special case.
   - Use directory and sampled-header signatures together so classification is not dependent on file naming alone.
   - Explicitly flag Stanley Bridge 2022 as a special-case unsupported format if the sampled file does not match the PEINP CSV pattern.

5. Implement lightweight schema inspection for representative CSVs. *Depends on step 4.*
   - Read only enough rows from each file to detect delimiter, header row, column names, obvious unit duplication, and timestamp column shape.
   - Preserve raw source column names exactly in the inspection output; do not rename variables yet.
   - Record parse assumptions and anomalies per file, including embedded sensor IDs, duplicate unit variants, double-spacing in headers, or missing expected fields.
   - Prefer a fail-soft path: capture read errors in the registry rather than aborting the whole run.

6. Implement timestamp-convention sampling. *Parallel with late step 5 once headers are known.*
   - Detect whether the source uses separate `Date` and `Time` fields with embedded UTC offsets, a single ISO datetime column, or another unsupported pattern.
   - Sample first and last valid timestamps per file and record both source-local values and whether UTC conversion appears feasible in later phases.
   - Do not perform full UTC normalization in phase 1; this phase only documents conventions needed by phase 2.

7. Define the registry schema and JSON summary schema. *Depends on steps 3-6.*
   - Registry CSV should include at minimum: file path, station code, station name, year, inferred month token, source type, parser family, extension, supported flag, header row, delimiter, row sample count, column count, raw timestamp pattern, timezone token if present, first and last sampled local timestamp, known issues, and parse status.
   - JSON summary should include counts by station, counts by parser family, supported versus skipped files, schema variants discovered, timestamp patterns discovered, and a compact list of files requiring manual follow-up.
   - Include explicit null or empty values where metadata could not be inferred so downstream steps can distinguish unknown from absent.

8. Implement artifact writing and CLI behavior. *Depends on step 7.*
   - Write deterministic outputs to a stable project location agreed for intermediate data products.
   - Make the phase 1 script runnable from the command line without notebook dependencies and print a concise terminal summary of counts, skipped files, and parsing warnings.
   - Ensure repeated runs overwrite or refresh artifacts cleanly rather than appending duplicate registry rows.

9. Add phase documentation output. *Depends on successful end-to-end dry run.*
   - Write a short phase summary in `/docs/plans` describing what phase 1 now does, why non-CSV files were deferred, and which station or schema anomalies were discovered.
   - Keep the summary synchronized with the actual artifact names and parser-family decisions.

10. Verify the phase before handoff to phase 2. *Depends on steps 1-9.*
   - Compare discovered file counts against the directory tree by station and year.
   - Manually inspect at least one representative file from each family to confirm header and timestamp detection is correct.
   - Confirm skipped-file reporting includes `.xlsx` and `.xle` cases instead of silently ignoring them.
   - Run a syntax check on the created phase-1 scripts.
   - Review the generated CSV and JSON artifacts for stable column names, readable issue messages, and correct station and year attribution.

**Relevant Files**
- `/docs/plans/doc.md` - Master project plan; phase 1 must stay aligned with step 1 and the documentation requirement.
- `/standards/python.md` - Phase 1 code style baseline: descriptive names, PEP 8, modular functions, comments for assumptions.
- `/standards/data_handling.md` - Phase 1 data-work expectations: pandas-based handling and documented transformation assumptions.
- `/standards/git.md` - Repo workflow guidance if the phase is later committed.
- `/data/raw` - Input root for recursive file discovery.
- `/src` - New phase 1 script and helper modules will be created here during implementation.
- `/docs/plans` - Phase-completion summary will be written here after implementation.

**Verification**
1. Cross-check total discovered CSV count against a recursive file listing of `/data/raw`.
2. Confirm at least one file each from Cavendish, Greenwich, ECCC Stanhope, Stanley Bridge, North Rustico, and Tracadie is correctly classified into a parser family or explicitly marked unsupported.
3. Validate timestamp-pattern detection against sampled raw strings from both PEINP-style `Date` and `Time` files and ECCC ISO datetime files.
4. Confirm the registry CSV contains one row per discovered file with stable columns and no duplicate paths on rerun.
5. Confirm the JSON summary reports supported, skipped, errored, and follow-up-required file counts consistently with the CSV.
6. Run a Python syntax or compile check on the created phase-1 source files.

**Decisions**
- Include only raw CSV parsing in phase 1; document and skip `.xlsx` and `.xle` files.
- Emit both CSV and JSON registry outputs.
- Keep phase 1 strictly observational: no schema standardization, unit harmonization, or UTC conversion yet.
- Prefer parser-family detection and anomaly logging over early normalization so phase 2 can build on verified source behavior.

**Further Considerations**
1. Recommended intermediate artifact placement: keep phase-1 registry artifacts in a stable data or output location that phase 2 can read directly without recomputing discovery.
2. Stanley Bridge 2022 should be treated as a first-class anomaly in the registry, not a silent edge case, because it likely requires a dedicated parser later.
3. If Greenwich or North Rustico subdirectories contain more non-CSV raw files than expected, record that explicitly in the phase summary so future scope decisions are evidence-based.