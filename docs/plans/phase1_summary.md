## Phase 1 Summary – Obtain and Registry Building

### What was done

Phase 1 is complete.  The following source files were created:

- `src/01_obtain.py` – orchestration entry point; runnable from the command line
- `src/obtain_utils.py` – helper module covering file discovery, station-metadata extraction, parser-family classification, schema inspection, timestamp sampling, and artifact writing

Running `python src/01_obtain.py` from the workspace root produces two ingest registry artifacts that Phase 2 reads directly:

- `data/scrubbed/phase1_registry.csv` – one row per discovered file, 23 columns
- `data/scrubbed/phase1_summary.json` – machine-readable counts, schema variants, and follow-up file list

### Discovery results (run on 09 Apr 2026)

| Metric | Count |
|--------|-------|
| Total files discovered | 232 |
| Supported CSV (parseable) | 220 |
| Skipped (documented) | 12 |
| Read / parse errors | 0 |

**By station:**

| Code | Station | Files | Notes |
|------|---------|-------|-------|
| CAV | Cavendish | 37 | PEINP-HOBOlink |
| GRE | Greenwich | 42 | PEINP-HOBOlink; includes xlsx seasonal files |
| STA | ECCC Stanhope | 49 | ECCC-LST; includes a `.docx` reference link |
| SBW | Stanley Bridge Wharf | 36 | Mix: 2022 special-case + 2023–2024 PEINP |
| NRW | North Rustico Wharf | 37 | PEINP-HOBOlink; includes xlsx seasonal files |
| TRW | Tracadie Wharf | 31 | PEINP-HOBOlink |

**Skipped file breakdown:**

| Category | Count | Reason |
|----------|-------|--------|
| `.xlsx` / `.xls` | 6 | Seasonal exports; need openpyxl in a future phase |
| `.xle` | 4 | HOBOlink binary logger files; need Onset SDK |
| `special-case-csv-metadata` | 1 | Stanley Bridge 2022 CSV with metadata preamble |
| Unknown extension (`.docx`) | 1 | Reference link file in ECCC Stanhope folder |

### Schema families detected

**PEINP-HOBOlink** (CAV, GRE, SBW 2023+, NRW, TRW)
- Header row 0; comma-delimited
- Two separate timestamp columns: `Date` (MM/DD/YYYY) and `Time` (HH:MM:SS ±HHMM)
- UTC offset is embedded as a suffix in the `Time` string
- Column names embed hardware sensor IDs, e.g. `Temperature (S-THB 21114839:20824084-1),°C,...`
- Many files include duplicate wind-speed columns in both km/h and m/s
- Tracadie files contain double-spaced column names (`Wind gust  speed`) requiring `.strip()` treatment

**ECCC-LST** (STA)
- Header row 0; comma-delimited; blank line between every data row (pandas `skip_blank_lines=True` handles this)
- Single `Date/Time (LST)` column in ISO 8601 format: `YYYY-MM-DD HH:MM`
- LST = Atlantic Standard Time = UTC-4; no offset embedded in data
- Dedicated flag columns for each measurement variable

**special-case-csv-metadata** (SBW 2022)
- Has a multi-row metadata block (Serial_number, Project ID, Location, offsets) before the actual data header
- Data columns are limited to `Date,Time,ms,LEVEL,TEMPERATURE`
- Requires a dedicated parser; excluded from Phase 2 primary analysis pending that work

### Key decisions and assumptions

1. Phase 1 is strictly observational.  No variable renaming, unit harmonisation, or UTC conversion is performed.
2. Non-CSV files are documented and skipped: `.xlsx`, `.xle`, `.docx`. Their paths appear in `phase1_summary.json` under `follow_up_files` for future phases.
3. Month tokens are extracted from filenames by regex rather than a hard-coded list, covering variants such as `Apr`/`April`, `Jul`/`July`, `Sep`/`Sept`, along with Stanhope zero-padded numbers and Stanley Bridge date-range names.
4. Reruns overwrite prior artifacts cleanly; row counts can be used as a reproducibility check.

### Critical finding for Phase 2: UTC offset variation

The embedded UTC offset in PEINP `Time` strings is **not uniform** across stations.  Observed values from the registry:

| Station | Observed offset | Implication |
|---------|----------------|-------------|
| CAV | `-0300` | Possibly mislabelled (AST is UTC-4; `-0300` = ADT/summer) |
| GRE | `-0400` | Consistent with AST (UTC-4) |
| TRW | `-0300` | Same concern as CAV |

Phase 2 must not assume a single fixed offset.  The `tz_token` column in the registry records the per-file observed offset, and Phase 2 should validate this field before applying UTC conversion.

### Verification performed

1. Total file count (232) was cross-checked against a manual `rglob` count of the raw directory.
2. One representative file from each station family was spot-checked to confirm correct parser-family assignment, header/timestamp detection, and first-local-timestamp value.
3. Skipped-file reporting (12 files) was confirmed to cover all `.xlsx`, `.xle`, metadata CSV, and `.docx` cases.
4. No read errors were produced; all 220 supported files were inspected successfully.
5. The registry CSV and JSON summary were opened and inspected for stable column names, readable issue messages, and correct station attribution.

### Artifacts produced

| Artifact | Location | Description |
|----------|----------|-------------|
| File registry | `data/scrubbed/phase1_registry.csv` | 232 rows × 23 columns; one row per discovered file |
| Summary | `data/scrubbed/phase1_summary.json` | Counts, schema variants, follow-up list |

### Next steps (Phase 2 inputs)

Phase 2 (`02_scrub.py`) should:
- Load `phase1_registry.csv` and filter to `supported=True` rows
- Apply parser-family-specific readers for PEINP-HOBOlink and ECCC-LST
- Use the per-file `tz_token` column to validate (not assume) the UTC offset before conversion
- Standardise variable names and attach quality flags
- Handle the column-name anomalies flagged in `schema_notes` (duplicates, double-spaces, sensor IDs)
- Produce hourly and daily resampled outputs for Phase 3 exploration
