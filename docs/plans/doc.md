## Plan: OSE for Weather RDA and FWI

Build the first three OSEMN stages around a location-aware ingestion and cleaning pipeline that standardizes inconsistent HOBOlink exports into hourly, analysis-ready weather datasets. The plan prepares two downstream paths from the same cleaned source: exploratory weather-only ordination readiness for later RDA/PCA-style work, and Fire Weather Index readiness using the subset of stations and hours that contain the required atmospheric variables.

**Steps**
1. Phase 1: Confirm analytical targets and data contracts
2. Define the predictor dataset contract for the first three stages: timestamps normalized to UTC, station identifier preserved, original file provenance retained, and weather variables standardized to canonical names and units. This blocks all later steps because the cleaning and explore stages depend on a stable schema.
3. Define the temporary RDA scope as weather-only feature preparation rather than true redundancy analysis, because no response matrix is currently in the workspace. Include explicit placeholders for later response-data joining so the Obtain and Scrub stages preserve join keys such as station, datetime, year, month, and any derived seasonal labels.
4. Define the FWI input contract early: temperature, relative humidity, wind speed, and precipitation at hourly resolution, with clear fallback rules for stations that do not provide enough atmospheric coverage. This depends on the schema contract and should drive what variables are mandatory versus optional.
5. Phase 2: Obtain stage design
6. Inventory all raw files under the station/year folder hierarchy and build a file manifest containing station name, year folder, filename, inferred file type, and expected parser pathway. This can run in parallel with raw column profiling.
7. Split ingestion logic into at least two parser classes or parser branches: modern HOBOlink weather exports and legacy/special-case formats such as the Stanley Bridge 2022 logger export. This depends on the manifest because parser assignment must be deterministic.
8. Profile columns across all files to build a column crosswalk from verbose HOBOlink headers to canonical variable names. The crosswalk should capture: raw column text, canonical name, physical unit, station availability, whether it is required for FWI, and whether it is excluded from the core atmospheric table. This can run in parallel with the manifest build.
9. Specify station metadata capture during ingestion: station display name, short code, station type classification (weather-focused versus mixed marine), source file path, and parser version. This depends on steps 6 to 8.
10. Specify robust timestamp assembly rules for every file family: combine Date and Time fields when needed, parse embedded timezone offsets, handle 12-hour time in legacy files, and preserve the original local timestamp string in audit columns before UTC conversion. This depends on parser identification.
11. Define Obtain-stage validation outputs: file counts by station/year, row counts before concatenation, min/max timestamps per file, duplicate header detection, and a report of unknown columns that do not map through the crosswalk. This depends on the crosswalk and parser rules.
12. Phase 3: Scrub stage design
13. Design a two-table cleaning strategy rather than forcing one universal wide table immediately: a master atmospheric table for shared weather variables and optional station-specific supplemental tables for marine or water-level variables. This avoids losing data from wharf stations while still producing an FWI-ready core table. This depends on the Obtain-stage crosswalk.
14. Standardize canonical variable names and units during cleaning. Resolve duplicate wind columns by selecting one authoritative unit, storing conversion logic explicitly, and dropping redundant columns only after validation. This depends on the column crosswalk.
15. Define missing-data handling rules by variable class instead of one blanket rule. Examples: do not impute precipitation across long gaps, allow short-gap interpolation or controlled filling only where scientifically defensible for continuous variables, and retain data-quality flags for hours derived from sparse sub-hourly observations. This depends on the FWI contract and should be documented in code comments per project standards.
16. Normalize all timestamps to UTC, then resample within each station using hourly bins. Specify aggregation rules per variable family before implementation: mean for temperature and relative humidity, sum for precipitation, vector-safe or documented approximation for wind direction, max or mean depending on the chosen wind-speed convention for FWI support, and mean or last-observation rules for battery or engineering signals if retained. This depends on timestamp parsing and canonical variable naming.
17. Add explicit quality controls for irregular sampling and sparse atmospheric sensors. For each station-hour, track observation counts per variable, mark hours that fail minimum coverage thresholds, and keep these flags in the scrubbed outputs so later FWI calculations can exclude low-confidence records. This depends on the resampling design.
18. Handle schema outliers and legacy data explicitly rather than silently coercing them. Stanley Bridge 2022 should be isolated as a non-core source unless it can contribute meaningfully to atmospheric analysis; marine-only variables at Tracadie and North Rustico should remain available in supplemental outputs but excluded from the core FWI table unless required variables exist. This depends on the two-table strategy.
19. Specify scrubbed outputs in two layers: one master hourly dataset spanning all stations and one set of station-level cleaned datasets. Include a companion data dictionary and a cleaning summary report describing dropped columns, excluded files, suspicious constants, and remaining gaps. This depends on all prior scrub rules.
20. Phase 4: Explore stage design
21. Start exploratory work with data-audit outputs rather than only visual plots. Summarize station coverage, variable availability, hourly completeness, and the percentage of usable records for FWI inputs. This depends on the scrubbed master dataset.
22. Produce exploratory summaries that directly support later ordination work: variable distributions, pairwise correlations among weather predictors, seasonal and station-level variability, and multicollinearity screening. This depends on canonical weather variables being standardized across stations.
23. Build visualizations in layers: overall project-level coverage plots, station-specific time series for core atmospheric variables, missingness heatmaps, and distribution plots after resampling. Keep the plotting scope aligned with the standards by using matplotlib and seaborn only.
24. Include an ordination-readiness checkpoint in Explore: identify candidate predictor variables, remove or flag near-zero variance fields, document highly collinear variables, and define the scaled feature matrix that a later PCA or true RDA step would consume. This depends on the exploratory summaries.
25. Include an FWI-readiness checkpoint in Explore: quantify which stations and periods satisfy the required hourly weather inputs, compare available wind and precipitation representations across stations, and produce a readiness table that clearly shows where FWI can be calculated without unsupported imputation. This depends on the quality flags and hourly resampling outputs.
26. Phase 5: Execution order and dependency notes
27. Build the raw file manifest and raw column profile in parallel.
28. Finalize the canonical crosswalk and parser rules before any cleaning logic.
29. Finalize resampling and missing-data rules before designing the Explore outputs, because all exploratory statistics must reflect the same cleaned data contract.
30. Design the Explore outputs for the master atmospheric table first, then extend to station-specific supplemental datasets where useful.
31. Phase 6: Verification strategy
32. Verify ingestion with file-level audits: every raw file should appear in the manifest, parser assignment should be explicit, and unknown columns should be reported rather than silently dropped.
33. Verify timestamp handling with targeted checks across at least one file from each station family, including modern offset-based files and the Stanley Bridge legacy file. Confirm UTC conversion, monotonic station-level timestamps, and duplicate-hour handling after resampling.
34. Verify cleaning with before/after metrics: row counts, hourly counts by station, missingness percentages by variable, and comparison of raw versus cleaned min/max ranges for temperature, RH, precipitation, and wind.
35. Verify exploratory readiness by producing a station-variable availability matrix, an FWI-input completeness table, and a predictor screening summary that lists variables kept, flagged, or excluded for later ordination.
36. Phase 7: Documentation and standards alignment
37. Structure the future pipeline as 01_obtain.py, 02_scrub.py, and 03_explore.py under /src, with each script using modular functions, descriptive variable names, and concise comments documenting assumptions and major transformations.
38. Add short explanatory summaries after each major code block when implementation begins, matching the project instruction to mentor junior data scientists through clear rationale.
39. Keep the first-three-stage scope limited to acquisition, cleaning, and exploratory readiness. Do not implement true RDA or final Fire Weather Index calculations in this phase; instead, ensure the outputs are validated and explicitly ready for those later stages.

**Relevant files**
- c:\Nancy\Holland College\3210\Vs code2\standards\python.md — coding conventions to preserve in the future scripts: PEP 8, descriptive names, modular functions, explanatory comments, reproducibility where relevant.
- c:\Nancy\Holland College\3210\Vs code2\standards\data_handling.md — tool and documentation constraints for pandas, matplotlib, seaborn, and documented cleaning assumptions.
- c:\Nancy\Holland College\3210\Vs code2\standards\git.md — version-control expectations for later implementation work.
- c:\Nancy\Holland College\3210\Vs code2\requirements.txt — current dependency boundary; note that only pandas, matplotlib, seaborn, and scikit-learn are presently available.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\raw — authoritative raw-data source that the manifest and parser design must cover.
- c:\Nancy\Holland College\3210\Vs code2\data\scrubbed — target location for the master hourly dataset, station-level cleaned outputs, and cleaning reports.

**Verification**
1. Confirm all raw files are represented in a manifest with station, year, filename, parser type, and row-count audit.
2. Confirm the canonical column crosswalk covers all weather variables needed for hourly FWI preparation and explicitly lists excluded marine-only or engineering columns.
3. Confirm UTC normalization and hourly resampling rules on representative files from Cavendish, Greenwich, Tracadie, North Rustico Wharf, and Stanley Bridge Wharf.
4. Confirm the scrubbed master dataset can support a single project-wide exploratory workflow while the station-level outputs preserve location-specific detail.
5. Confirm the Explore-stage design produces both ordination-readiness artifacts and an FWI readiness table without requiring unsupported imputation.

**Decisions**
- Include all stations in scope for O, S, and E, but separate the shared atmospheric core from station-specific supplemental variables.
- Use only the weather data for now; treat true redundancy analysis as blocked on a future response dataset.
- Target one master scrubbed dataset plus station-level scrubbed outputs.
- Keep the first-phase deliverable focused on readiness for later RDA and FWI rather than performing those final analyses.

**Further Considerations**
1. Recommendation: adopt a canonical station metadata table early so parser rules, data-quality flags, and future joins stay consistent across all scripts.
2. Recommendation: decide before implementation whether wind direction will be retained only for exploratory plots or also transformed for modeling, because this affects resampling and ordination preprocessing.
3. Recommendation: define a minimum hourly coverage threshold for FWI readiness during Scrub, because this threshold will materially affect how much data is considered usable in Explore.