# data-quality-validation

Upstream cleaning stage of the Camping Grubhof import: takes the raw protel
export from SharePoint and produces the cleaned CSV that
`camping-grubhof-import/imp_camping_20260807.sql` stages into MySQL.

## Pipeline

1. `download_and_load.py` — fetches `2607_Data cleaned - Grubhof.xlsx` from
   SharePoint via Microsoft Graph (credentials in `.env`, never committed).
   Worksheet 2 is the source of truth; worksheet 1 contains broken Excel
   transformations and is never read.
2. `person_account_export.ipynb` — **the authoritative pipeline.** Reproduces
   the cleaning and adds the decisions the production run used (lowercased
   emails, gender from salutation, initials-capitalization fix, impossible
   phones blanked instead of rejected, dedup on the four matching fields,
   mapping to the 40-column standard import format with the
   `data_issue`/`contract_valid`/`import_ready` flags). Writes
   `../local_data/csv/20260807_camping_grubhof_cleaned.csv` (146,996 x 43).
   This produced the CSV behind batch `2026-08-10_new_camping_import`.
3. `person_account_data_quality.ipynb` — the earlier analysis / QA record
   (completeness profiling, pandera contract, funnel summaries). Kept as
   history; its transform code has drifted from the export notebook and is
   NOT what ran. Do not "fix" it to match — consolidate into the export
   notebook if anything.

Notebook outputs are stripped before committing: saved outputs contain guest
emails. The as-run funnel counts live in the header of
`imp_camping_20260807.sql` and the camping README.

Environment: `uv sync` (Python, polars, pandera, phonenumbers, nameparser,
pycountry). See `AGENTS.md` for the working agreements.
