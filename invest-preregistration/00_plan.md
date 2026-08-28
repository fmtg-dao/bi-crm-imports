# Invest pre-registration import — plan (2026-08-28)

## Context

`invest_preregistrations_2022-to-2026-08-25_exported-2026-08-26.csv`: 3,193 rows of invest
pre-registrants (source `Migration`, origin `conda-pre-reg` — corrected 2026-08-28 from the
CSV's `Pre-Reg.` so records tie back to the Conda export system; lowercase-slug convention
per existing values `grubhof`/`fmtg-at`), never loaded (2/3,192 external_ids
in org). Mirror match by email (2026-08-27 mirror): **~1,695 existing person accounts**
(almost all non-invest, created 2026 during migration) and **~1,497 net-new**.

**Authority correction:** the invest team may only write `InvestCustomer__pc`,
`InvestmentStatus__pc`, `InvestmentExpirationDate__pc`, and the **invest** consent.
`consent_central` / `consent_camping` / `consent_residences` in the CSV are a mistake —
dropped at staging, never read, never written, no reconciliation.

## Scope (user decisions 2026-08-28)

| Population | Writes |
|---|---|
| ~1,695 existing (email match) | Account update: `InvestCustomer__pc=true` + invest_central consent. Nothing else — the list is PROSPECTS (team, 2026-08-28); `investment_status`/`expiration` are NULL in all 3,192 rows, and the loaders don't map them. |
| ~1,497 net-new | Person Account insert: full CSV row (identity, birth, gender, email, phone, language, nationality, address, external ids, source, invest flag + invest fields). Other BU flags stay 0; other consents ignored. |
| `consent_invest=1` | ContactPointConsent OptIn on purpose invest_central `0ZWTe0000000X5dOAE` |
| `consent_invest=0` | ContactPointConsent **OptOut** (same purpose) |

## Salesforce objects touched

Only two objects are ever written:

1. **Account** (Person Account, RecordTypeId `012Te0000018UgIIAU` — wrong/missing → silent Business Account).
2. **ContactPointConsent** — the ONLY consent object we insert.

The rest of the consent chain exists or is auto-created:
- **DataUsePurpose** — exists; invest_central = `0ZWTe0000000X5dOAE`. Never insert.
- **Individual** + **ContactPointEmail** — org automation creates them on Person Account
  insert; nothing in this repo ever inserts them. Chain: `Account.PersonContactId` →
  `Individual.Contact__c` → `ContactPointEmail.ParentId` (join CPE via
  `crm_cp_email_sfid_prod.PartyID__c = PersonContactId`, never by email alone — one email
  can have many CPEs/Individuals).
- Not used in this org's model: DataUseLegalBasis, PartyConsent, ContactPointTypeConsent,
  AuthorizationFormConsent.

ContactPointConsent payload (copy `invest-consent:insert_consents_invest.py` semantics):
`Name='invest_central'`, `ContactPointId=sf_cp_email_id`, `DataUsePurposeId`,
`PrivacyConsentStatus` from batch suffix (`_optin`/`_optout`), `CaptureDate`/`EffectiveFrom`
= one UTC now per run, `ConsentKey__c = '<cpe>|<purpose>|CENTRAL'`,
`CaptureSource`/`SourceSystem__c` = `conda-pre-reg` (NOT the row's `source` 'Migration' —
same Conda-traceability decision as `SourceOrigin__pc`, 2026-08-28).

## Files to create (per-job copies are deliberate — no shared helpers)

```
invest-preregistration/
  01_create_mirror_indexes.sql      # reuse from mailing-address-backfill
  02_recon_prereg.sql               # funnel: email match, multi-match emails, CPE coverage,
                                    # existing invest consents, consent_invest=0 count
  03_stage_prereg.ipynb             # load CSV → crm_imp_person_accounts, split batches
  04_run_prereg_accounts.ipynb      # update job + insert job + id writebacks
  05_run_prereg_consents.ipynb      # probe + consent load + verify (mirror 04_run_invest_consents)
  insert_person_accounts_prereg.py  # from post-migration-imports/insert_person_accounts_bulk.py
  update_accounts_prereg.py         # from update_contacts_bulk.py, field map ONLY the 3 invest fields
  insert_consents_prereg.py         # from invest-consent:insert_consents_invest.py
```

Loader conventions to keep: REPO_ROOT-relative paths, explicit batch id argument (never
DEFAULT_BATCH_ID), None-stripped payloads (Bulk 2.0: omitted = untouched; never emit empty
cells), duplicate-`sf_account_id`/CPE abort, `successfulResults` readback, per-batch
`_*_processed_at` writeback, `--dry-run` mode, circuit breaker + re-auth from
`mailing-address-backfill/update_mailing_addresses.py`.

## Batches (staging `crm_imp_person_accounts`; verify live ALTER'd columns first — repo DDL is stale)

- `2026-08-28_prereg_update` (`_operation='update'`, sf ids joined from mirror at staging)
- `2026-08-28_prereg_insert` (`_operation='insert'`; separate batch id is mandatory —
  insert loader doesn't filter `_operation`)
- consent: `2026-08-28_prereg_consent_optin` / `_optout` (suffix drives status)

`uk_source_external_id`: external_id is unique in CSV (3,192 + 1 NULL) — OK.

## Order of operations

1. **Recon** — refresh mirrors (`refresh_sf_mirrors.py`), re-apply indexes, run 02: freeze
   funnel numbers; resolve one-email→many-accounts matches (pick deterministic rule:
   most-recently-modified, else exclude to review file); the 2 external_id matches join
   by ExternalID__pc not email; exclude the 1 no-email row (`_excluded=1`).
2. **Stage** (03) — MySQL only. Contract asserts: row counts per batch, ASCII-email +
   punycode-domain check (`domain.encode('idna')`), plausible birth years, no account in
   both batches, no duplicate sf_account_id in update batch.
3. **Account update load** (04) — dry-run → probe 1 account (REST, readback, sign-off) →
   bulk update → `_processed_at` writeback.
4. **Account insert load** (04) — dry-run → bulk insert → writeback `sf_account_id`
   (from successfulResults via ExternalID__pc) → fetch `PersonContactId` (200-id chunks) →
   error on any non-person-account.
5. **Mirror refresh + re-index** (mandatory between insert and CPE backfill), then manual
   CPE backfill SQL (join `PartyID__c = sf_person_contact_id AND EmailAddress = email`,
   punycode-aware; dup-CPE probe first). Existing-account batch gets CPE ids the same way.
   Rows with NO CPE after backfill: exclude + report (we never create CPEs).
6. **Consent load** (05) — restage consent batches by `consent_invest` → dry-run → probe →
   bulk load (existing-consent skip + status-conflict abort built in) → verify: live SOQL
   count by status on invest_central vs contract; `still_open=0` in staging.
7. **Archive** — `CALL sp_archive_crm_imp_person_accounts(<batch>, 'arsal')` per batch;
   commit as-run notebooks on `invest-preregistration` branch.

## Verification

- After 3: SOQL count `InvestCustomer__pc=true` = 6,158 + ~1,695 (minus drift skips).
- After 4: inserted count = staged insert batch; all have PersonContactId.
- After 6: invest_central consent counts = August contract (6,212/10) + new OptIn/OptOut
  contract; zero CPEs with >1 invest_central consent.
- Review exports for every excluded/skipped row under `local_data/`.

## Open points

- Multi-account email matches: rule above needs user sign-off at recon time.
- Whether to stamp `ExternalID__pc`/`SourceOrigin__pc` on the 1,695 existing accounts
  (identity linkage vs. authority scope) — default NO, ask.
- `investment_status` values in CSV must map to valid picklist values ('Owner' is banned).
