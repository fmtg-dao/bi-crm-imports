# Phase 3: consent script for invest_central

[Overview](overview.md)

## Goal

`invest-consent/insert_consents_invest.py`, an adaptation of the camping
consent script that combines the central payload with the newer safety
guards.

## Changes

Start from `camping-grubhof-import/insert_consents_camping.py` (it has the
post-run guards) but replace its payload with the central field list from
`post-migration-imports/insert_consents_bulk.py`:

- Fields written: `Name`, `ContactPointId` (from `sf_cp_email_id`),
  `DataUsePurposeId = 0ZWTe0000000X5dOAE`, `PrivacyConsentStatus` (constant
  per run, OptIn or OptOut), `CaptureDate`, `EffectiveFrom` (same UTC
  timestamp), `CaptureSource`, `SourceSystem__c`. Name convention and
  ConsentKey__c per the D5 query result. No `Property__c`, `HotelName__c`,
  `Region__c` on a central purpose.
- Constants changed together: `CONSENT_NAME`, `DATA_USE_PURPOSE_ID`,
  `CONSENT_FLAG_COLUMN = 'consent_invest'`, plus a `PRIVACY_CONSENT_STATUS`
  constant selected by CLI flag.
- Batch id is a required CLI argument, no `DEFAULT_BATCH_ID` fallback.
- Guards kept from the camping folder version: pre-load SOQL skip if the CPE
  already has a consent for this purpose (purpose-only filter, no Property),
  skip file written to `local_data/`, abort on duplicate `sf_cp_email_id`
  within a run, per-batch MySQL writeback of `_consent_processed_at`,
  `Aborted` treated as a failed job but `successfulResults` still read first.
- Row selection: `consent_invest = 1 AND _consent_processed_at IS NULL AND
  sf_cp_email_id IS NOT NULL AND _excluded = 0 AND _batch_id = :cli_batch_id`.
- Batch size 5,000, poll every 10s, failed results dumped to `local_data/`.

## Data structures

The CSV row is the mapper dict, keys sorted alphabetically, None fields
dropped entirely (Bulk API 2.0 ignores empty cells; absent is the only safe
"not set").

## Verification

Static: script runs `--dry-run` (build CSVs, no job creation) against the
staged batches and the produced row counts equal the phase 2 counts; a
generated CSV is eyeballed for the exact field list. `/deslop` on the diff
before commit.
