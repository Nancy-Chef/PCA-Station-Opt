# Phase 1 Completion Summary

## Status: COMPLETE
**Completed:** 2026-03-29

---

## What Was Done

Phase 1 was a pure specification phase.  No source code was written and no data was
transformed.  The following four required artifacts were produced:

| Artifact | File |
|---|---|
| Canonical variable model | `docs/plans/phase1_canonical_schema.md` |
| Station registry and file-discovery rules | `docs/plans/phase1_station_registry.md` |
| Stanhope acquisition pathway decision | `docs/plans/phase1_stanhope_pathway.md` |
| Obtain-stage audit specification | `docs/plans/phase1_obtain_audit_spec.md` |

Decisions were grounded in direct inspection of representative raw files from all five
Parks Canada HOBOlink stations and the ECCC Stanhope metadata.

---

## Why It Was Done This Way

Phase 1 exists to eliminate design ambiguity before incurring the cost of writing code
that might need to be rewritten.  The raw data contains enough structural variation
(four distinct schema families, inconsistent filename conventions, one confirmed
cross-station contamination, missing humidity at Stanley Bridge) that implementing
`01_obtain.py` without these specifications would produce a fragile and assumption-laden
loader.  Locking the canonical schema before scrub code prevents field-name drift.

---

## Key Findings from Raw File Inspection

### Timestamp Structure
All HOBOlink files (schema A and B) use two columns:
- `Date` — `MM/DD/YYYY` format
- `Time` — `HH:MM:SS ±HH00` with embedded UTC offset per row

Atlantic Standard Time appears as `-0400`; Atlantic Daylight Time as `-0300`.  Both
are legitimate; the offset is correct in the raw files and must be parsed at the row
level, not assumed station-wide.

### Schema A vs B Distinction
Cavendish and Greenwich are pure atmospheric stations (schema A).  North Rustico,
Tracadie, and Stanley Bridge 2023+ have mixed atmospheric-marine headers (schema B).
Stanley Bridge 2022 is a completely different Solinst water-level format (schema C) with
no atmospheric data and no UTC offset.

### Stanley Bridge Humidity Gap
The Stanley Bridge HOBOlink files do not contain an RH column.  Only `Temperature
(S-TMB)` is present for atmospheric sensing.  This is a structural data limitation that
will constrain Stanley Bridge's contribution to PCA components involving humidity.

### Greenwich Dec 2022 Sensor Gap
Many rows in Greenwich December 2022 are missing temperature, RH, and dew point.  Only
wind and rain appear populated for those rows.  This is likely because the humidity
sensor was not yet installed at that time.

### Cross-Station File Contamination
`PEINP_TR_WeatherStn_Feb2025.csv` exists inside `North Rustico Wharf/2025/`.  This is
a Tracadie file deposited in the wrong folder.  The Obtain stage must detect and flag it
rather than loading it as North Rustico data.

### Stanley Bridge Filename Anomaly
`PEINP_SB_WeatherStn_Jul202.csv` exists in Stanley Bridge 2023.  The year token is
truncated (`202` instead of `2023`).  The file must be flagged, not silently loaded.

### Stanley Bridge Prefix Change
2023 files use prefix `PEINP_SB_`; 2024+ files use `PEINP_STB_`.  Both map to `SBW`.

### Month-Name Inconsistencies
Multiple stations use non-abbreviated month names or alternate abbreviations: `April`,
`July`, `June`, `Sept`.  These are normalised by the discovery rules but logged as
`MONTH_NAME_NONSTANDARD`.

### Stanhope Has No Observations
The ECCC Stanhope folder contains only metadata and historical R download code.  No
hourly observation files exist in the repository.  Acquisition via Python and the ECCC
bulk data API is required before Stanhope can participate in any analysis.

### Wind Column Duplication
HOBOlink exports contain both km/h and m/s wind columns for the same sensor.  The m/s
columns were observed to be empty in all inspected files.  The km/h columns are the
authoritative source.

---

## Assumptions Made

1. The UTC offset embedded in the HOBOlink Time column is correct and does not need
   external correction.  If an offset is wrong in the raw data, it is a source-data
   error and will be documented in the anomaly log at Obtain time.

2. The ECCC bulk download URL with `stationID=8300590` will return complete hourly
   records for PEI Stanhope for 2022–2025.

3. Stanley Bridge 2022 Solinst files are excluded from PCA and FWI processing.  If a
   project stakeholder later decides they want water-level data included, that is a scope
   change requiring a new planning decision.

4. North Rustico and Tracadie 2022 folders are genuinely empty (stations not yet
   deployed), not missing from the repository due to a transfer error.

5. Greenwich 2023–2025 folders appearing empty in the workspace listing is a data-gap
   issue, not a filesystem problem.  The Obtain audit will confirm this at runtime.

6. The `relative_humidity_pct` field cannot be reliably obtained for Stanley Bridge.
   This is treated as a permanent station characteristic, not a recoverable data gap.

---

## Unresolved Questions (carry into Phase 2)

| Issue | Impact | When to Resolve |
|---|---|---|
| Greenwich 2023–2025 folder emptiness | Major: GRE is a FWI station; gaps will affect FWI completeness | Verify at start of Phase 2 Obtain run |
| Stanley Bridge RH gap | Medium: limits SBW's PCA contribution | Quantify in Explore stage before finalising PCA variables |
| Stanhope solar radiation absence | Medium: affects inter-station solar comparison in PCA | Defer to Explore stage |
| PCA temporal grain (hourly vs daily) | Medium: affects how overlap windows are computed | Defer to Explore stage after missingness is quantified |
| Tracadie Feb 2025 contamination file | Low (one file): needs manual resolution | Resolve before loading 2025 data in Obtain |
| FWI package selection (coded vs library) | Medium: affects Phase 2 implementation approach | Decide at start of Phase 2 Scrub design |
| Month continuity of Cavendish/Greenwich for FWI | Medium: FWI requires unbroken daily inputs | Quantify in Scrub/Explore stages |

---

## What Phase 2 Must Honor

1. **Canonical field names are fixed.** Phase 2 scrub logic must produce exactly the
   column names defined in `phase1_canonical_schema.md`.  Do not invent new names.

2. **Schema family codes are fixed.** `A`, `B`, `C`, `D` as defined.  `01_obtain.py`
   assigns them; `02_scrub.py` reads them to select the correct parsing branch.

3. **Station IDs are fixed.** `CAV`, `GRE`, `NRW`, `SBW`, `TRW`, `STA`.  These must
   appear in every output row.

4. **Contamination candidate files must not be silently loaded.** Any file with status
   `CONTAMINATION_CANDIDATE` in the inventory must be excluded from scrub until
   explicitly resolved.

5. **Stanley Bridge 2022 Solinst files are excluded from PCA and FWI.** Only inventory
   them; do not transform.

6. **Stanhope timestamp conversion uses `+4 hours` fixed offset** (LST = UTC-4,
   year-round, regardless of DST).

7. **HOBOlink timestamp conversion uses the per-row embedded offset** from the Time
   column.  Do not assume a fixed offset for HOBOlink files.

8. **km/h wind columns are authoritative** for all HOBOlink stations.  m/s columns are
   auxiliary and may be empty.

9. **Hourly resampling is a Scrub responsibility**, not Obtain.  Obtain reads raw
   sub-hourly files but does not resample them.

10. **The Obtain audit outputs must exist and be reviewed** before Phase 2 Scrub begins.
    The audit is a prerequisite gate, not an optional report.

---

## Phase 1 Acceptance Checklist

- [x] Each required station has a registry entry with folder names and known aliases
- [x] Each needed analytical field has a canonical definition with unit and aggregation rule
- [x] Stanhope handling is unambiguous (Python-native ECCC bulk download)
- [x] Obtain audit output is specific enough to implement without reopening planning questions
- [x] Unresolved issues are documented explicitly with carry-forward tracking
- [x] Raw file structures were observed, not assumed
- [x] Stanley Bridge schema C (Solinst) exclusion is documented
- [x] Cross-station contamination is flagged with resolution guidance
