## Plan: OSE Pipeline for PCA and FWI

Build the first three OSEMN stages so the repository can reliably ingest heterogeneous Parks Canada HOBOlink weather files, acquire or ingest Stanhope benchmark data, scrub everything to a consistent hourly/daily analysis model, and produce exploration outputs that confirm readiness for five-station PCA-based benchmarking and Cavendish/Greenwich full daily FWI calculations. The recommended approach is to separate file inventory/schema validation from transformation logic, standardize a station-aware canonical weather schema in UTC, and treat PCA prep and FWI prep as two downstream products from one shared scrubbed dataset.

**Steps**
1. Phase 1: Scope the canonical inputs and outputs for O, S, and E. Define the target analytical products before any implementation: one shared hourly canonical dataset for all five PCA stations plus Stanhope, one daily FWI-ready dataset for Cavendish and Greenwich, one data-quality inventory, and one exploration output bundle. This step blocks all later steps because it determines required variables, units, and resampling rules.
2. Phase 1: Formalize the station registry and file-discovery rules. Map the five PCA stations to accepted folder names and file-name aliases, explicitly including Stanley Bridge SB/STB variants and known cross-folder contamination cases. Mark this as a first-class artifact in Obtain so the loader validates station identity from both folder path and filename before reading contents.
3. Phase 1: Define the external Stanhope acquisition path. Because the repository currently contains metadata and an old R download script but not the benchmark observations, the plan should include either a Python acquisition step that downloads the required ECCC hourly records or a constrained ingestion interface that accepts externally supplied Stanhope files in a documented raw-data location. This can proceed in parallel with step 2 but must finish before finalizing the Obtain verification outputs.
4. Phase 2: Design the Obtain stage around inventory-first validation. 01_obtain.py should recursively inventory raw files, classify each file by station, year, and source format, detect schema families, and emit a machine-readable audit showing coverage, duplicate candidates, cross-station contamination, file-format anomalies, and missing months. This stage should read representative headers and row counts but stop short of cleaning transformations.
5. Phase 2: Define the canonical variable model. Standardize the atmospheric fields needed for PCA and FWI into canonical names such as timestamp_local_raw, timestamp_utc, station_id, station_name, air_temp_c, relative_humidity_pct, dew_point_c, precip_mm, wind_speed_kmh, wind_gust_kmh, wind_dir_deg, solar_wm2, battery_v, plus source-specific extras retained separately. Explicitly distinguish core analytical variables from auxiliary marine variables so North Rustico, Stanley Bridge, and Tracadie can contribute to PCA without forcing marine fields into the common schema.
6. Phase 2: Design scrub rules for time normalization, unit reconciliation, and gap handling. 02_scrub.py should parse embedded timezone offsets from the Time field, convert everything to UTC, preserve original local timestamps for traceability, harmonize wind units where both km/h and m/s appear, resample sub-hourly records to hourly values using variable-specific aggregation rules, and produce a cleaning log that documents assumptions required by project standards.
7. Phase 2: Design branch-specific scrub outputs. From the shared hourly canonical dataset, create one PCA-ready hourly or daily feature table covering the five Parks Canada stations and Stanhope, and one Cavendish/Greenwich daily FWI table containing all fields needed for FFMC, DMC, DC, ISI, BUI, and FWI. This depends on steps 5 and 6.
8. Phase 2: Specify station overlap and inclusion rules for all available years. Because you selected all available years, the plan should preserve every available record from 2022 to 2025, but the scrubbed outputs must also include completeness metrics by station and by year so later modeling can choose an overlap subset without losing the wider archive.
9. Phase 3: Design the Explore stage to answer readiness questions rather than just generate generic plots. 03_explore.py should summarize temporal coverage, missingness, variable availability, station comparability against Stanhope, distributions of PCA candidate variables, resampling effects, and FWI input continuity for Cavendish and Greenwich. This step depends on the finalized scrub outputs.
10. Phase 3: Define exploration outputs for the PCA branch. Include correlation heatmaps, station-by-variable completeness matrices, seasonal coverage summaries, pairwise comparisons versus Stanhope, and dimensionality-screening views that show whether the common variables are sufficiently aligned for downstream PCA or redundancy-style multivariate modeling.
11. Phase 3: Define exploration outputs for the FWI branch. Include daily continuity checks, precipitation and humidity diagnostics, wind and temperature seasonality summaries, and explicit flags for any periods where the full FWI chain cannot be computed reliably because of missing antecedent inputs.
12. Phase 3: Build verification and phase-summary checkpoints into the workflow. After each stage, save a written phase summary in docs/plans, recording what was done, why it was done, unresolved data issues, and any assumptions. Verification should include schema assertions, timestamp checks, station coverage audits, resampling sanity checks, and exploratory output review before any later modeling stage begins.

**Relevant files**
- c:\Nancy\Holland College\3210\Vs code2\.github\copilot-instructions.md — governing contract for OSEMN-only workflow, no notebooks, and required phase summaries.
- c:\Nancy\Holland College\3210\Vs code2\standards\python.md — coding style baseline for descriptive names, modular functions, and reproducibility.
- c:\Nancy\Holland College\3210\Vs code2\standards\data_handling.md — required libraries and expectation to document cleaning assumptions.
- c:\Nancy\Holland College\3210\Vs code2\standards\git.md — version-control expectations for future implementation work.
- c:\Nancy\Holland College\3210\Vs code2\requirements.txt — currently available Python stack; likely missing ECCC download and FWI-specific dependencies if external packages are chosen later.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\Cavendish — representative HOBOlink atmospheric station and one of the two FWI stations.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\Greenwich — representative HOBOlink atmospheric station and the other FWI station.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\North Rustico Wharf — representative mixed atmospheric and marine schema showing why canonical field separation is necessary.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\Stanley Bridge Wharf — station with naming and format anomalies that must be handled explicitly in Obtain.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\Tracadie Wharf — station with partial-year early coverage and month-name inconsistencies relevant to file discovery.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\ECCC Stanhope Weather Station — contains Stanhope metadata and prior download logic but not the target benchmark observations.

**Verification**
1. Confirm the raw-file inventory reports every station-year-month present in data/raw and separately flags misplaced or misnamed files before any cleaning begins.
2. Confirm the canonical schema covers all PCA variables shared across the five Parks Canada stations and Stanhope, while preserving non-core marine variables without polluting the common feature set.
3. Confirm all timestamps parse with their embedded timezone offsets and convert to UTC without duplicated or ambiguous hourly bins.
4. Confirm hourly resampling rules are variable-specific and documented, especially for precipitation, wind, solar radiation, and humidity.
5. Confirm the Cavendish and Greenwich daily table supports the full FWI chain and identifies any days that fail prerequisite continuity checks.
6. Confirm exploration outputs expose overlap windows, missingness structure, and station comparability clearly enough to support the later PCA and redundancy-analysis design.
7. Confirm each completed stage is summarized into docs/plans so future sessions do not need to rediscover earlier validation decisions.

**Decisions**
- Included scope: only the first three OSEMN stages, ending at validated exploratory outputs and readiness artifacts.
- Included scope: all available years from 2022 through 2025, with completeness reporting to support later overlap filtering.
- Included scope: full daily FWI chain for Cavendish and Greenwich, not just FFMC, DMC, and DC.
- Included scope: Stanhope benchmark acquisition planning, because the benchmark observations are not currently present in the repository.
- Excluded scope: downstream PCA model fitting, formal redundancy analysis execution, and interpretation of final ecological or fire-weather results.
- Excluded scope: notebook workflows, because the project contract forbids them.

**Further Considerations**
1. Stanhope acquisition method: prefer a Python-native Obtain step for reproducibility unless institutional constraints require retaining the R-based download process as a documented pre-step.
2. PCA temporal grain: defer the final decision between hourly and daily PCA inputs until Explore quantifies overlap, missingness, and autocorrelation after scrubbing.
3. FWI implementation source: decide during implementation whether to code the Canadian formulas directly or use a vetted package, then verify package assumptions against the project’s UTC and daily-aggregation rules.
