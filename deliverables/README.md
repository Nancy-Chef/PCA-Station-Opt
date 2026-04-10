# Final Submission Bundle

This folder is the instructor-facing submission package for the Parks Canada optimization assignment. It collects the required deliverables in one place so the final work can be reviewed without navigating the full project pipeline.

## Contents

- `cleaning.py` – consolidated ingestion and cleaning pipeline wrapper
- `analysis.ipynb` – documented notebook for EDA, PCA/clustering, FWI logic, and visual evidence
- `technical_report.md` – technical report draft ready for export to PDF or Word
- `figures/` – final visuals referenced in the report and notebook

## Recommended Review Order

1. Read `technical_report.md` for the executive summary, methods, findings, and recommendation.
2. Open `analysis.ipynb` for the documented analytical walkthrough.
3. Review `cleaning.py` if you want the assignment-specific cleaning entry point.

## Reuse Decisions

This submission bundle reuses the existing phased project wherever possible.

- `cleaning.py` reuses `src/01_obtain.py` and `src/02_scrub.py` rather than reimplementing the cleaning logic.
- The report and notebook reuse validated figures from `outputs/figures/` where those figures are already suitable.
- The original PCA biplot was not reused because the saved artifact is effectively blank. A new clustering figure, `figures/station_profile_clustermap.png`, replaces it in the final submission.

## Final Recommendation

Retain all five Park stations. No station is a defensible removal candidate based on the completed PCA redundancy analysis, Stanhope benchmarking, bootstrap uncertainty analysis, and FWI workflow requirements.