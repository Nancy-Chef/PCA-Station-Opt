# Phase 1 Implementation Plan

## Objective
Phase 1 is a specification phase. Its purpose is to remove major design ambiguity before any source code is written for the Obtain, Scrub, and Explore stages.

The required outputs of this phase are:
- a canonical schema definition for downstream PCA and FWI work
- a station registry and file-discovery specification
- a Stanhope acquisition or ingestion decision
- an Obtain-stage audit specification for future implementation in `01_obtain.py`

## Scope Boundary
This phase is planning only.

Included in scope:
- defining planning artifacts
- reviewing representative source-file structures
- documenting assumptions and unresolved issues
- defining acceptance criteria for later implementation

Excluded from scope:
- creating `src` scripts
- adding new dependencies
- transforming or resampling data
- calculating FWI
- generating figures

## Step-by-Step Plan

### Step 1: Lock the Phase 1 Boundary
Use the current project plan in [doc.md](c:\Nancy\Holland College\3210\Vs code2\docs\plans\doc.md) as the governing scope reference and keep Phase 1 limited to specification artifacts only.

Why this step matters:
- it prevents planning drift into implementation
- it keeps later phase summaries consistent with the project workflow

Expected outcome:
- a clear agreement that Phase 1 produces planning documents, not code

### Step 2: Define the Required Deliverables
Create the Phase 1 checklist with four required artifacts:
1. canonical schema table
2. station registry and file-discovery rules
3. Stanhope acquisition or ingestion path
4. Obtain-stage audit specification

Why this step matters:
- it makes the phase testable
- it prevents missing a required planning artifact before implementation starts

Expected outcome:
- a complete deliverables checklist that Phase 1 can be evaluated against

### Step 3: Review Representative Raw Source Structures
Inspect representative files and folders from:
- Cavendish
- Greenwich
- North Rustico Wharf
- Stanley Bridge Wharf
- Tracadie Wharf
- ECCC Stanhope Weather Station

Capture only structure-level facts needed for planning:
- header patterns
- timestamp formats
- timezone encoding
- units
- file naming conventions
- obvious anomalies

Why this step matters:
- the plan must reflect real data structures rather than assumptions
- later parser design depends on what is actually present in raw files

Expected outcome:
- a documented summary of source-format characteristics and edge cases

### Step 4: Define the Canonical Variable Model
Create a canonical schema that separates core atmospheric variables from auxiliary marine or source-specific variables.

For each canonical field, document:
- canonical field name
- expected unit
- likely source column patterns
- intended analytical use: `PCA`, `FWI`, `both`, or retained-only
- allowed null behavior
- planned future aggregation rule

Suggested core fields to assess:
- `timestamp_local_raw`
- `timestamp_utc`
- `station_id`
- `station_name`
- `air_temp_c`
- `relative_humidity_pct`
- `dew_point_c`
- `precip_mm`
- `wind_speed_kmh`
- `wind_gust_kmh`
- `wind_dir_deg`
- `solar_wm2`
- `battery_v`
- `source_schema`

Why this step matters:
- downstream scrub and exploration logic depends on stable field definitions
- FWI and PCA need overlapping but not identical variables

Expected outcome:
- a code-ready schema definition that later scripts can implement directly

### Step 5: Build the Station Registry and File-Discovery Rules
Define one canonical station identifier and station name for each Parks Canada site. Document:
- accepted folder names
- known filename aliases
- year and month folder expectations
- mismatch rules between path and filename
- contamination or misfile detection rules

Why this step matters:
- raw data is organized by folder, but filenames and schemas may not always align cleanly
- Obtain needs explicit rules to classify files consistently

Expected outcome:
- a registry specification that can later become a Python dictionary or configuration table

### Step 6: Decide the Stanhope Pathway
Use the existing Stanhope metadata and legacy R script as evidence of source and identity, then choose one supported approach for Phase 2:
- Python-native download and ingestion workflow
- externally supplied Stanhope files placed in a documented raw-data location

Also define the minimum Stanhope fields required to join the canonical schema later.

Why this step matters:
- Stanhope is part of the benchmark design but the observations are not currently in the repository
- the Obtain-stage plan is incomplete until this path is defined

Expected outcome:
- one approved Stanhope ingestion approach with minimum schema expectations

### Step 7: Specify the Obtain-Stage Audit Outputs
Define what `01_obtain.py` must eventually report without writing the code yet.

Required audit checks should include:
- file counts by station, year, and month
- schema-family classification
- duplicate file candidates
- missing-month detection
- timestamp-format validation
- anomaly and unresolved-issue log

Why this step matters:
- implementation is easier and safer when success criteria are defined before coding
- the Obtain stage should validate structure before any transformation begins

Expected outcome:
- a precise audit-output specification that can guide and test the future script

### Step 8: Define Phase 1 Acceptance Criteria
Phase 1 is complete only when:
- each required station has a registry entry
- each needed analytical field has a canonical definition
- Stanhope handling is unambiguous
- the Obtain audit output is specific enough to implement without reopening planning questions
- unresolved issues are documented explicitly

Why this step matters:
- it gives a concrete stop condition for the phase
- it prevents hidden assumptions from carrying into implementation

Expected outcome:
- a checklist that clearly says whether Phase 1 is complete

### Step 9: Prepare the Phase Summary Structure
When Phase 1 is completed, save a summary in the [docs/plans](c:\Nancy\Holland College\3210\Vs code2\docs\plans) directory describing:
- what was done
- why it was done
- assumptions made
- unresolved questions
- what Phase 2 must honor

Why this step matters:
- the repository instructions require phase summaries for continuity across sessions
- future work should not need to rediscover planning decisions

Expected outcome:
- a clear summary template for the formal Phase 1 completion note

## Verification Checklist
1. Confirm the canonical schema covers variables required for the five-station PCA benchmark.
2. Confirm the canonical schema covers the daily FWI workflow for Cavendish and Greenwich.
3. Confirm every required station has a canonical registry entry with aliases and discovery rules.
4. Confirm timestamp assumptions are documented from observed raw formats, including embedded UTC offsets where present.
5. Confirm Stanhope has one approved acquisition or ingestion path.
6. Confirm Stanhope minimum fields are mapped into the canonical schema.
7. Confirm the Obtain audit specification is detailed enough to drive implementation and later testing.
8. Confirm deferred decisions are explicitly labeled rather than implied.

## Recommended Decisions
- Keep PCA temporal grain as a later design decision until the Explore stage quantifies overlap and missingness.
- Define hourly-compatible canonical fields now so either hourly or daily PCA can be supported later.
- Prefer a Python-native Stanhope workflow in Phase 2 for reproducibility unless external constraints require manual ingestion.
- Allow the canonical model to preserve source-specific extras rather than forcing marine variables into the core analytical schema.

## Key References
- [docs/plans/doc.md](c:\Nancy\Holland College\3210\Vs code2\docs\plans\doc.md)
- [.github/copilot-instructions.md](c:\Nancy\Holland College\3210\Vs code2\.github\copilot-instructions.md)
- [standards/python.md](c:\Nancy\Holland College\3210\Vs code2\standards\python.md)
- [standards/data_handling.md](c:\Nancy\Holland College\3210\Vs code2\standards\data_handling.md)
- [data/raw/Cavendish/2023/PEINP_Cav_WeatherStn_Jan2023.csv](c:\Nancy\Holland College\3210\Vs code2\data\raw\Cavendish\2023\PEINP_Cav_WeatherStn_Jan2023.csv)
- [data/raw/ECCC Stanhope Weather Station/ECCC_Weather data bulk upload_Rcode.R](c:\Nancy\Holland College\3210\Vs code2\data\raw\ECCC Stanhope Weather Station\ECCC_Weather data bulk upload_Rcode.R)
- [data/raw/ECCC Stanhope Weather Station/en_climate_hourly_metadata_PE_8300590.txt](c:\Nancy\Holland College\3210\Vs code2\data\raw\ECCC Stanhope Weather Station\en_climate_hourly_metadata_PE_8300590.txt)