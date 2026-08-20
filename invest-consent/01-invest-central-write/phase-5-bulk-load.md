# Phase 5: bulk load

[Overview](overview.md)

**Gated: does not run without Arsal's explicit go-ahead, and not before D1
confirms we are the writer.**

## Goal

All staged consents in production: the OptIn batch, then the OptOut batch.

## Changes

- Run `insert_consents_invest.py` with the OptIn batch id, watch every job to
  `JobComplete`, check `numberRecordsFailed = 0`, then the OptOut batch id.
- ~6,200 records means two jobs at batch size 5,000; minutes, not hours.
- Any failed rows: failed-results CSVs land in `local_data/`, diagnose before
  rerunning. A rerun is safe by construction (processed-at lock plus the
  purpose-only skip query), that is the phase 3 contract.
- Load-day log kept per `show-me-your-work`: commands, job ids, counts,
  anomalies.

## Verification

Job states and processed/failed counts recorded; MySQL
`_consent_processed_at` count equals successful records per batch; skip file
empty or explained.
