## Plan: Parks Canada OSEM Pipeline

Build an OSEM pipeline that ingests every raw CSV under `/data/raw`, normalizes station-specific timestamps to UTC, standardizes schemas across heterogeneous weather stations, resamples to hourly and daily products, imputes missing values with gap-aware rules, and prepares outputs for PCA-based redundancy analysis, benchmarking against ECCC Stanhope, daily FWI moisture-code calculation, and uncertainty quantification.

**Confirmed defaults**
- Use the existing workspace layout under `/docs/plans` for phase summaries, because `/doc/plans` is referenced in the instructions but does not exist in the workspace.
- Treat `/standards/*.md` as the authoritative coding/data-handling guidance, and ignore the typo path in the copilot instructions.
- Start from the currently available raw files only; `/src` is empty in the workspace, so the implementation will create the pipeline scripts from scratch.
- Ingest all available years, but make the primary redundancy/PCA and Stanhope benchmarking window the common overlap across stations, with all-year sensitivity checks kept secondary.
- Use one common reduced feature set for the main PCA/benchmarking matrix so the five stations can be compared on shared atmospheric variables, while keeping marine-dominant variables as auxiliary diagnostics.
- Use conservative gap-aware imputation: interpolate only short gaps, flag or exclude long gaps from primary analyses, and reserve any heavier imputation for exploratory sensitivity work.

**Steps**
1. Obtain and registry building, then implement `01_obtain.py`.
   - Inventory all CSVs by station/year/month and build a machine-readable registry of file paths, timestamps, station metadata, and source schema notes.
   - Read representative files from each station family to detect schema variants, delimiter/header differences, timestamp formats, and station-year anomalies.
   - Emit a normalized raw-ingest artifact that preserves source columns and records parsing assumptions.
   - *Depends on confirmed file inventory and timestamp conventions.*

2. Scrub, timezone normalization, and resampling, then implement `02_scrub.py`.
   - Parse station-specific time strings, convert all records to UTC, and preserve original local timestamps for traceability.
   - Standardize variable names across stations, attach quality flags, and separate atmospheric versus marine-dominant variables.
   - Apply gap-aware missing-data handling with explicit thresholds for short-gap interpolation versus long-gap exclusion or station/variable dropping.
   - Produce hourly aggregates for redundancy analysis and daily products for FWI-ready downstream work.
   - *Depends on step 1 outputs and confirmed missing-data policy.*

3. Explore, then implement `03_explore.py`.
   - Generate descriptive statistics, completeness summaries, station-by-station data availability diagnostics, and distribution checks.
   - Compare time-step regularity, missingness structure, and variable coverage across stations after scrubbing.
   - Produce exploratory plots and tables that reveal which stations and variables are suitable for downstream modeling.
   - Keep this step diagnostic only: no PCA, benchmarking, or FWI calculation here.
   - *Depends on scrubbed hourly/daily outputs.*

4. Modeling, benchmarking, FWI, and uncertainty analysis, then implement `04_model.py` and supporting analysis utilities.
   - Construct the PCA-ready matrix for the Parks Canada redundancy analysis using the agreed common reduced feature set.
   - Benchmark each station against ECCC Stanhope on shared variables using aligned hourly UTC series and compute uncertainty-aware redundancy metrics, including bootstrap or resampling intervals for RMSE/correlation-style comparisons.
   - Calculate daily FFMC, DMC, and DC for Cavendish and Greenwich from the required daily inputs, using 16:00 UTC observation logic and season restart handling for long gaps.
   - Produce model outputs, figures, tables, and text summaries under `/outputs/figures` and companion text/CSV outputs.
   - *Depends on exploratory diagnostics and scrubbed hourly/daily outputs.*

5. Document the phase outputs.
   - After each completed phase, write a short summary file under `/docs/plans` describing what was done, why it was done, and any assumptions or caveats.
   - Keep these summaries synchronized with the actual pipeline behavior so later sessions can resume without needing to re-validate earlier phases.

**Relevant files**
- `/data/raw` — raw station CSVs to ingest and inspect.
- `/src/01_obtain.py` — planned raw ingest and schema registry creation.
- `/src/02_scrub.py` — planned cleaning, UTC normalization, resampling, and imputation logic.
- `/src/03_explore.py` — planned exploratory diagnostics, completeness summaries, and distribution checks.
- `/src/04_model.py` — planned PCA, benchmarking, FWI, and uncertainty outputs.
- `/standards/data_handling.md` — guidance for cleaning and transformation.
- `/standards/python.md` — Python style and modularization guidance.
- `/standards/git.md` — branch and commit conventions.
- `/outputs/figures` — target figure outputs.
- `/docs/plans` — target location for phase summaries.

**Verification**
1. Validate parsed timestamps against source samples from each station family before any aggregation.
2. Compare row counts, null counts, and time-step distributions before and after UTC normalization and hourly/daily resampling.
3. Confirm that benchmark variables line up with Stanhope after unit and timezone harmonization.
4. Check that daily FWI moisture codes for Cavendish and Greenwich reproduce published ECCC values within an acceptable tolerance.
5. Review bootstrap or resampling intervals for stability and ensure sparse stations do not dominate the PCA or benchmarking results.
6. Run file-level lint or syntax checks on any created scripts and confirm outputs land in the documented folders.

**Decisions**
- The current workspace strongly suggests a heterogeneous schema problem rather than a single uniform weather feed, so the plan emphasizes station-aware standardization instead of forcing every file into one rigid schema.
- North Rustico and Stanley Bridge are expected to need special handling because their atmospheric variables are sparse or partially absent.
- Stanhope should remain the reference station for hourly benchmarking after timezone alignment.
- Cavendish and Greenwich are the only stations targeted for the daily FWI moisture-code workflow unless you request a broader scope.
- The primary benchmark window should be the common overlap across stations; longer station-specific histories remain useful for sensitivity checks but should not drive the headline comparisons.
- The primary PCA matrix should stay common across all stations for comparability; marine-dominant variables can be retained as secondary diagnostics rather than forcing them into the headline matrix.
- Missing-data handling should remain conservative so it supports uncertainty analysis instead of manufacturing false precision from sparse stations.