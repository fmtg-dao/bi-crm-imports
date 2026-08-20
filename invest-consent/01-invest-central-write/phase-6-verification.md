# Phase 6: verification

[Overview](overview.md)

## Goal

Proof the write landed as predicted, recorded in the recon file.

## Changes

- Refresh the consent and CPE mirrors (`refresh_sf_mirrors.py`), then re-run
  `../create_mirror_indexes.sql` (a refresh drops the indexes).
- Re-run the audit pivot (recon query 12/14 pattern, consent options as OptIn
  columns): the invest_central column must equal the phase 2 prediction, all
  other columns unchanged from the before-pivot.
- Live SOQL cross-check: `SELECT COUNT() FROM ContactPointConsent WHERE
  DataUsePurposeId = '0ZWTe0000000X5dOAE'` split by PrivacyConsentStatus,
  equals staged OptIn/OptOut counts.
- Spot-check three records by SOQL (one OptIn, one OptOut, one dual-CPE
  account if D3 chose per-CPE).
- Append the results as a new numbered query to `../recon_invest_consent.sql`
  and commit.

## Verification

This phase is the verification; done means predicted equals actual with any
delta explained line by line, and the recon file updated. Report to Oleg
follows from here.
