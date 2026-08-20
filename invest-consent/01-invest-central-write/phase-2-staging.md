# Phase 2: stage the population

[Overview](overview.md)

## Goal

The population frozen in `crm_imp_person_accounts`, one row per
ContactPointConsent to write, with the CPE Id prefilled and the predicted
after-state counts recorded.

## Changes

- New numbered SQL file in `../` (continuing the recon file's style): refresh
  check, then `INSERT INTO crm_imp_person_accounts` selecting the population
  from the mirrors with the D2-confirmed predicate.
- Two batch ids, same date, one per consent status, because the consent script
  writes one constant `PrivacyConsentStatus` per run:
  `YYYY-MM-DD_invest_central_optin` and `YYYY-MM-DD_invest_central_optout`.
  The OptOut batch is the 11 exclusion matches, resolved by the same
  normalized-email match the audit used.
- `consent_invest = 1`, all other consent flags 0, `sf_cp_email_id` prefilled
  in the INSERT via `PartyID__c = PersonContactId` (no manual backfill step,
  unlike camping). Per D3, one row per CPE if per-CPE wins.
- No `_operation` ambiguity: these batch ids are consent-only, no account
  insert shares them.

## Data structures

`crm_imp_person_accounts` as defined in
`sql/Scripts/imp_contacts_table_definition.sql`; the only columns this phase
cares about are `email`, `sf_account_id`, `sf_cp_email_id`, `consent_invest`,
`_batch_id`, `_excluded`, `_consent_processed_at` (live-table ALTER, not in
the CREATE).

## Verification

Counts computed and written into the SQL file as comments before phase 3
starts: rows staged per batch id, distinct accounts, distinct CPEs, rows with
`sf_cp_email_id IS NULL` (must be exactly the D7 account, or zero if D7 says
exclude), and the predicted post-load pivot cell (OptIn consents = staged
OptIn rows, OptOut = 11 accounts' CPE count). Mirror refresh, if run, is
followed by `../create_mirror_indexes.sql`.
