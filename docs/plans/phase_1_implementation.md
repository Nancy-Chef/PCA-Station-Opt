# Phase 1 Implementation Plan

Lock the analytical contract before writing pipeline code so the Obtain, Scrub, and Explore stages can be implemented against one stable schema. This Phase 1 plan defines the minimum decisions, contracts, and verification steps required before implementation begins.

## Goal

Produce a Phase 1 handoff package that fixes the project's current data-contract decisions:

- Cavendish and Greenwich are the initial candidates for the FWI-ready atmospheric core.
- North Rustico Wharf and Tracadie Wharf remain in scope for supplemental outputs unless later profiling proves they satisfy the full FWI input contract.
- Stanley Bridge Wharf 2022 is supplemental only and excluded from the FWI-ready core.
- Embedded timezone offsets are used when present; Atlantic Time fallback is used only for legacy files that lack explicit offsets.
- Wind speed is standardized to m/s.
- A station-hour is considered FWI-usable only when at least 3 sub-hourly observations support each required variable.

## Scope

### Included

- Predictor dataset contract for the first three OSEMN stages
- Temporary weather-only RDA readiness scope
- FWI input contract and fallback rules
- Station classification and output-layer rules
- Timestamp and parser policy needed for later implementation
- Verification criteria for approving the Phase 1 contract

### Excluded

- True RDA implementation
- Final FWI calculation
- Obtain, Scrub, or Explore code
- Broad imputation policy beyond Phase 1 readiness rules

## Step-by-Step Plan

### 1. Write the Phase 1 charter

Record the approved planning decisions in one short charter so later phases do not reopen them implicitly.

Output:
- A short approved-scope statement
- A list of locked Phase 1 decisions
- A list of explicitly deferred decisions

Acceptance criteria:
- The charter states the station scope, timezone policy, wind unit, and hourly usability threshold.

### 2. Define the Phase 1 deliverables

List the exact documents and contracts that must exist before implementation begins.

Output:
- Station metadata contract
- Predictor dataset contract
- Canonical variable and unit policy
- Timestamp and parser policy
- FWI input contract
- Master-versus-supplemental output contract
- Phase 1 verification checklist
- Phase 1 handoff note for implementation

Acceptance criteria:
- Each deliverable has a clear purpose and a user for the later implementation stages.

### 3. Build the station metadata contract

Define one authoritative metadata structure for every station so parser selection, data-quality rules, and later joins all rely on the same lookup.

Required fields:
- station_code
- station_name
- station_family
- parser_family
- station_type
- fwi_core_status
- expected_weather_variables
- supplemental_variables
- timezone_policy
- notes

Initial classification:
- Cavendish: atmospheric core candidate
- Greenwich: atmospheric core candidate
- North Rustico Wharf: provisional supplemental
- Tracadie Wharf: provisional supplemental
- Stanley Bridge Wharf 2022: supplemental only

Acceptance criteria:
- Every station has one classification and one parser family.
- No station is left ambiguous about FWI-core eligibility.

### 4. Define the predictor dataset contract

Specify the minimum fields that the cleaned atmospheric dataset must preserve for downstream exploratory work and later response-data joins.

Required row-level fields:
- station_code
- station_name
- station_family
- source_file
- parser_type
- datetime_local_raw
- datetime_utc
- year
- month
- season
- record_origin

Core weather fields to reserve in the contract:
- temperature_c
- relative_humidity_pct
- wind_speed_mps
- precipitation_mm
- wind_direction_deg
- wind_gust_mps
- solar_radiation_wm2

Acceptance criteria:
- The contract preserves provenance, join keys, and later ordination/FWI readiness fields.

### 5. Define the canonical variable and unit policy

Choose one canonical name and one canonical unit for each planned atmospheric field so the later crosswalk maps every raw header into a stable schema.

Canonical policy:
- Temperature: `temperature_c` in degrees C
- Relative humidity: `relative_humidity_pct` in percent
- Wind speed: `wind_speed_mps` in m/s
- Wind gust: `wind_gust_mps` in m/s
- Wind direction: `wind_direction_deg` in degrees
- Precipitation: `precipitation_mm` in mm
- Solar radiation: `solar_radiation_wm2` in W/m^2
- Battery voltage: `battery_voltage_v` in volts

Rule:
- Duplicate raw wind columns are mapped through the later crosswalk but do not become separate canonical fields unless they are confirmed to represent distinct measurements rather than alternate units.

Acceptance criteria:
- Each core variable has one canonical field name and one canonical unit.

### 6. Define timestamp and parser policy

Document how every file family will be interpreted before any resampling logic is written.

Policy:
- Modern HOBOlink files parse embedded timezone offsets from the Time field.
- The original raw local timestamp text is preserved for audit.
- Parsed timestamps are converted to `datetime_utc` before any hourly aggregation.
- Legacy Stanley Bridge files use a separate parser family.
- Legacy files without explicit offsets use Atlantic Time fallback and preserve the original raw date and time fields.

Acceptance criteria:
- Modern and legacy file families are both covered.
- No parser branch leaves timezone handling undefined.

### 7. Define the FWI input contract and fallback rules

Lock the FWI-ready weather requirements early so missing-data and resampling rules can be designed around them.

Required hourly variables:
- temperature_c
- relative_humidity_pct
- wind_speed_mps
- precipitation_mm

Rules:
- A station-hour is FWI-usable only when each required variable has at least 3 contributing sub-hourly observations.
- Stations missing required variables remain outside the initial FWI-ready atmospheric core.
- Phase 1 does not approve unsupported imputation to force missing stations into the FWI core.
- Missing required variables are handled as scope exclusions, not as silent substitutions.

Acceptance criteria:
- The contract clearly distinguishes mandatory FWI variables from optional exploratory fields.

### 8. Define the master-versus-supplemental output contract

Separate the shared atmospheric analysis table from station-specific marine, engineering, or legacy-only data.

Master atmospheric table:
- Cross-station hourly weather data
- Limited to canonical atmospheric variables and quality fields
- Designed to support project-wide Explore outputs

Supplemental outputs:
- Station-level cleaned datasets for marine, water-level, engineering, or legacy-only variables
- Preserve data that should not be forced into the atmospheric core

Assignment rules:
- Cavendish and Greenwich feed the initial atmospheric core if they satisfy the hourly contract.
- North Rustico Wharf and Tracadie Wharf remain supplemental unless later profiling proves they satisfy the required FWI variables.
- Stanley Bridge Wharf remains supplemental only.

Acceptance criteria:
- Every variable family has a destination: master table or supplemental output.

### 9. Write the Phase 1 verification checklist

Define how to confirm that Phase 1 is complete and that implementation can start without reopening contract decisions.

Checklist:
- Confirm the charter includes the locked station scope, timezone policy, wind unit, and hourly coverage rule.
- Confirm every station has one metadata classification and parser family.
- Confirm every required atmospheric variable has one canonical name and one canonical unit.
- Confirm timestamp policy covers both modern offset-based files and legacy files.
- Confirm the FWI contract states required variables, scope exclusions, and the hourly usability threshold.
- Confirm master-versus-supplemental assignment is explicit.

Acceptance criteria:
- A future implementation can be reviewed directly against the checklist.

### 10. Write the implementation handoff note

Record what Phase 2 can start immediately once Phase 1 is approved.

Phase 2 can begin with:
- Raw file manifest design
- Raw column profiling
- Canonical crosswalk construction
- Parser branching design
- Timestamp validation on representative files

Still out of scope after Phase 1:
- Final FWI calculation
- Response-matrix joining for true RDA
- Full imputation strategy

Acceptance criteria:
- The handoff note lets implementation start without revisiting Phase 1 contract decisions.

## Deliverables

1. Phase 1 charter
2. Station metadata contract
3. Predictor dataset contract
4. Canonical variable and unit policy
5. Timestamp and parser policy
6. FWI input contract
7. Master-versus-supplemental output contract
8. Phase 1 verification checklist
9. Phase 1 implementation handoff note

## Verification

1. Confirm the charter records the four locked planning decisions: FWI-core candidate stations, timezone policy, canonical wind unit, and hourly coverage threshold.
2. Confirm each station is classified as atmospheric-core candidate, provisional supplemental, or supplemental only.
3. Confirm each required atmospheric field has one canonical name, one canonical unit, and a defined downstream role.
4. Confirm timestamp policy is explicit for both modern HOBOlink files and Stanley Bridge legacy files.
5. Confirm the FWI contract states the non-imputation position for Phase 1 and the minimum hourly usability threshold.
6. Confirm the handoff package is specific enough to support the Obtain-stage design without reopening Phase 1 decisions.

## Decisions Locked for Phase 1

- Cavendish and Greenwich are the only initial FWI-core candidates.
- North Rustico Wharf and Tracadie Wharf remain supplemental unless later profiling proves otherwise.
- Stanley Bridge Wharf 2022 is supplemental only.
- Embedded timezone offsets are authoritative when present.
- Atlantic Time fallback is used only for legacy files without explicit offsets.
- Wind speed is standardized to m/s.
- FWI readiness requires at least 3 sub-hourly observations per required variable within the hour.

## Next Use of This Plan

Use this document as the approval gate before creating the Obtain-stage manifest design, parser branches, and crosswalk work products.

## Deliverables Status

All Phase 1 contracts have been produced and are in [docs/plans/phase_1_contracts.md](phase_1_contracts.md). That file is the authoritative reference for all implementation work that follows.