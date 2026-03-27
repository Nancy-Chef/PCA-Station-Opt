## Plan: Obtain, Scrub, Explore for PCA Benchmarking and FWI Moisture Codes

Focus the first three OSEMN stages on two assignment-aligned deliverables: prepare a comparable multivariate weather dataset for the five Parks Canada weather stations benchmarked against the ECCC Stanhope reference station, and prepare a daily Fire Weather Index moisture-code dataset using only the Cavendish and Greenwich stations. The plan keeps implementation grounded in the project standards by emphasizing modular pandas-based workflows, explicit assumptions, and audit-friendly data products.

**Steps**
1. Phase 1: Lock the analytical data contracts
2. Define two explicit analysis tracks before any implementation begins. Track A is PCA-based multivariate modeling for the five Parks Canada stations compared against Stanhope. Track B is daily FWI moisture-code preparation limited to Cavendish and Greenwich. This decision blocks all later Obtain and Scrub rules because the required stations, temporal resolution, and retained variables differ by track.
3. Define the shared core schema for both tracks: station identifier, station class, source file, raw local timestamp, UTC timestamp, local calendar date, year, month, and canonical weather variable names with standardized units. This blocks all downstream cleaning and exploratory work.
4. Define the PCA modeling contract early: the cleaned dataset must support direct comparison of the five Parks Canada stations with the Stanhope benchmark using a harmonized set of atmospheric predictor variables and comparable time aggregation rules. This means only variables available across the benchmarked station set should enter the final PCA-ready matrix.
5. Define the FWI contract separately: retain only the variables required for daily moisture-code calculation at Cavendish and Greenwich, with daily summaries derived from sub-daily observations under explicit aggregation rules. This depends on the shared schema but should not be constrained by variables missing at other stations.
6. Phase 2: Obtain stage design
7. Build a full file and source manifest for all Parks Canada raw weather files and add a source record for the ECCC Stanhope reference dataset. If the Stanhope data is not yet stored in the workspace, the Obtain plan must include a documented external-source placeholder and ingestion contract so later sessions can slot it in without redesigning the pipeline.
8. Split source handling into at least three logical parser branches: modern HOBOlink weather exports, special legacy formats such as Stanley Bridge 2022, and the Stanhope reference format. This depends on the manifest because each source must have a deterministic parser pathway.
9. Build a canonical column crosswalk that maps verbose raw headers to standard atmospheric variable names. The crosswalk should explicitly identify: variables used in the PCA track, variables used in the FWI track, variables excluded as station-specific or engineering-only, and any duplicate measurements stored in different units.
10. Capture station metadata during Obtain, including the five Parks Canada stations, their short codes, whether they are eligible for PCA, whether they are eligible for FWI, and how they relate to the Stanhope benchmark. This depends on the manifest and crosswalk.
11. Specify timestamp parsing rules for each source family: combine Date and Time fields where needed, parse embedded timezone offsets, handle 12-hour legacy formats, preserve raw local timestamps, and convert all observations to UTC for internal consistency. This depends on parser assignment.
12. Define Obtain-stage audits that prove comparability before cleaning: file counts by station and year, min and max timestamps, unknown columns, duplicate timestamps, and a variable-availability table showing overlap between each Parks Canada station and Stanhope. This depends on the crosswalk and parser rules.
13. Phase 3: Scrub stage design
14. Design the cleaning outputs around two analysis-ready tables rather than one generic master table. Output A is an hourly or otherwise harmonized atmospheric comparison table for PCA benchmarking across the five Parks Canada stations plus Stanhope. Output B is a daily FWI table for Cavendish and Greenwich only. Shared preprocessing logic should be reused, but each output should have its own validation criteria.
15. Standardize canonical variable names and units during cleaning. Resolve duplicate measurements explicitly, especially wind-speed columns reported in both km/h and m/s, and document the chosen authoritative unit in the cleaning logic and data dictionary. This depends on the crosswalk.
16. Define missing-data rules by analysis track. For the PCA track, favor conservative filtering so the comparison matrix remains defensible and directly comparable across stations. For the FWI track, prohibit unsupported imputation of precipitation and other key inputs, and preserve daily quality flags so low-confidence days can be excluded later. This depends on the analytical contracts.
17. Normalize timestamps to UTC first, then derive any local-date fields needed for daily products. For the PCA track, define the comparison resolution required for the multivariate model and apply the same temporal aggregation across all benchmarked stations, including Stanhope. For the FWI track, derive daily summaries from the cleaned Cavendish and Greenwich time series using documented aggregation rules appropriate for moisture-code inputs. This depends on timestamp parsing and canonical variable naming.
18. Add explicit quality controls for observation density, irregular sampling, and station comparability. For each station-period, track observation counts, variable completeness, and the proportion of usable records. For the FWI daily product, carry forward day-level flags showing whether each required moisture-code input is complete enough to support calculation. This depends on the resampling and aggregation design.
19. Handle outlier schemas and stations intentionally rather than forcing them into both outputs. Data from stations or periods that lack the atmospheric variables needed for PCA benchmarking should be excluded from the final PCA matrix but still documented in cleaning reports. Stanley Bridge legacy level-focused data should remain outside the core PCA and FWI products unless a directly relevant atmospheric subset is confirmed. This depends on the variable-availability audit.
20. Specify Scrub-stage deliverables: a harmonized PCA-ready comparison dataset including Stanhope, a daily FWI-preparation dataset for Cavendish and Greenwich, a shared data dictionary, and a cleaning report that documents exclusions, assumptions, and coverage gaps. This depends on all prior scrub rules.
21. Phase 4: Explore stage design
22. Begin Explore with coverage and comparability diagnostics rather than modeling outputs. Summarize which stations, years, and variables are available for the PCA benchmark and which dates at Cavendish and Greenwich are usable for daily FWI moisture codes. This depends on the scrubbed outputs.
23. For the PCA track, produce exploratory summaries that support PCA-based multivariate modeling: distributions of shared atmospheric variables, pairwise correlations, seasonal and station-level variability, standardized-variable screening, and a benchmark comparison against Stanhope to identify bias, drift, or structural differences before dimensionality reduction. This depends on the harmonized comparison dataset.
24. For the FWI track, produce exploratory summaries that test readiness for daily moisture-code calculation: daily completeness tables, distributions for temperature, relative humidity, wind, and precipitation, and station-by-station comparisons between Cavendish and Greenwich to highlight missingness or systematic differences. This depends on the daily FWI-preparation dataset.
25. Build visualization outputs in layers using only matplotlib and seaborn. For PCA readiness, prioritize overlap plots, missingness heatmaps, and variable-comparison plots between each Parks Canada station and Stanhope. For FWI readiness, prioritize daily coverage plots and distributions of the required moisture-code inputs for Cavendish and Greenwich. This depends on the scrubbed outputs and the project standards.
26. End Explore with two explicit readiness checkpoints. The PCA checkpoint should define the final shared variable set, excluded variables, scaling requirements, and the benchmark comparison notes needed before modeling. The FWI checkpoint should define which Cavendish and Greenwich date ranges are complete enough for daily moisture-code calculation without unsupported imputation. This depends on the exploratory summaries.
27. Phase 5: Execution order and dependency notes
28. Build the Parks Canada manifest, raw column profile, and provisional variable-overlap table in parallel.
29. Finalize the canonical crosswalk and Stanhope ingestion contract before any cleaning rules are locked.
30. Finalize the temporal aggregation and missing-data rules separately for the PCA track and the FWI track before designing Explore outputs.
31. Design Explore outputs for the PCA benchmark first, then the Cavendish/Greenwich daily FWI branch, because the benchmarking scope is broader and sets the comparability requirements.
32. Phase 6: Verification strategy
33. Verify Obtain by confirming every Parks Canada raw file is represented in the manifest and the Stanhope source is either ingested or explicitly documented as a pending dependency with a fixed schema contract.
34. Verify column harmonization by confirming that the final PCA variable set exists across the five Parks Canada stations and Stanhope, and that the FWI-required variables exist for Cavendish and Greenwich over the intended study period.
35. Verify timestamp handling on representative files from each source family, including offset-based HOBOlink files, any irregular wharf files that remain in scope, and the Stanhope reference format.
36. Verify Scrub outputs with before-and-after row counts, variable completeness summaries, and aggregation checks at both the comparison-table level and the daily FWI-table level.
37. Verify Explore readiness by producing a station-variable overlap matrix for PCA benchmarking and a day-level completeness table for Cavendish and Greenwich FWI inputs.
38. Phase 7: Documentation and standards alignment
39. Structure the future scripts as 01_obtain.py, 02_scrub.py, and 03_explore.py under /src, using descriptive variable names, modular functions, and concise explanatory comments that document assumptions and major transformations.
40. Add short explanatory summaries after each major code block when implementation begins so the pipeline remains understandable to junior data scientists, as required by the project instructions.
41. Keep this first-three-stage scope limited to obtaining, cleaning, and exploring data for PCA benchmarking and daily FWI moisture-code readiness. Do not perform the final PCA interpretation or final FWI moisture-code calculations in this phase.

**Relevant files**
- c:\Nancy\Holland College\3210\Vs code2\standards\python.md — coding conventions for the future pipeline scripts: PEP 8, descriptive names, modular functions, comments, and reproducibility where relevant.
- c:\Nancy\Holland College\3210\Vs code2\standards\data_handling.md — pandas, matplotlib, seaborn, and documented-cleaning-assumption requirements.
- c:\Nancy\Holland College\3210\Vs code2\standards\git.md — version-control expectations for later implementation work.
- c:\Nancy\Holland College\3210\Vs code2\requirements.txt — current dependency boundary; confirm whether Stanhope ingestion or later FWI work requires additional packages only when implementation begins.
- c:\Nancy\Holland College\3210\Vs code2\data\raw\raw — raw Parks Canada weather source to be inventoried and profiled.
- c:\Nancy\Holland College\3210\Vs code2\data\scrubbed — target location for the harmonized PCA comparison dataset, daily FWI-preparation dataset, and supporting reports.

**Verification**
1. Confirm all Parks Canada raw files are represented in a manifest with station, year, filename, parser type, and row-count audit.
2. Confirm the final PCA comparison schema contains only variables shared across the five Parks Canada stations and the Stanhope reference station.
3. Confirm the daily FWI-preparation schema contains the required moisture-code inputs for Cavendish and Greenwich and clearly flags incomplete days.
4. Confirm UTC normalization and source-specific timestamp parsing on representative files from each relevant source family.
5. Confirm the Explore-stage design produces both a PCA-benchmark readiness artifact and a Cavendish/Greenwich daily FWI readiness artifact without unsupported imputation.

**Decisions**
- Narrow the multivariate objective to PCA-based modeling of the five Parks Canada stations benchmarked against ECCC Stanhope.
- Narrow the FWI objective to daily moisture-code preparation for Cavendish and Greenwich only.
- Keep the first-three-stage deliverable focused on data readiness, not final modeling outputs.
- Treat Stanhope as a required benchmark source whose ingestion contract must be defined during Obtain even if the raw file is not yet present in the workspace.

**Further Considerations**
1. Recommendation: define early whether the PCA benchmark will use hourly, daily, or another harmonized time step, because that choice affects comparability with Stanhope and the amount of overlap retained.
2. Recommendation: define the exact daily aggregation rules needed for the FWI moisture codes before coding Scrub so the Cavendish and Greenwich outputs are scientifically defensible.
3. Recommendation: confirm the source format and availability window for the ECCC Stanhope reference data as soon as possible, because it is the main external dependency in the revised scope.