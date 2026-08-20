# Camping Grubhof import

The August 2026 upload of Camping Grubhof guests into Salesforce production, for Carmen
Marti. This folder holds the SQL that built the batch and the two bulk scripts as
configured for the run.

The bulk scripts are copies. `post-migration-imports/` keeps the shared versions that the
investor imports use. Both scripts carry per-run constants you edit before each run, so a
camping run and an investor run cannot share one file.

## What ran

Batch `2026-08-10_new_camping_import`, staged from `stg_imp_camping_grubhof_20260807`
(146,996 rows, 96,933 `import_ready`).

- 2026-08-10, staging insert: 89,898 rows.
- 2026-08-10, `insert_person_accounts_camping.py`: 89,887 Person Accounts, 11 failures.
- 2026-08-10, `sf_cp_email_id` backfill by `EmailAddress` join: 89,887 rows.
- 2026-08-17, `insert_consents_camping.py`: 89,887 consents, 18 of 18 jobs complete, 0 failures.

The remaining `import_ready` rows are 4,720 that already have an account, 2,042 sharing a
household email, and 273 that exist as Leads.

## Consent configuration

Christoph Crepaz ruled on 2026-08-11 that Grubhof uploads get a property consent and no
central consent.

	Name                  Marketing_Property
	DataUsePurposeId      0ZWTe0000000X8rOAE      marketing_property
	Property__c           a0QTe00000La2dVMAR      Camping Grubhof, Apaleo FCG
	HotelName__c          Camping Grubhof
	Region__c             Saalachtal
	ConsentKey__c         <CPE Id>|<purpose Id>|<property Id>
	PrivacyConsentStatus  OptIn
	CaptureSource         camping

Purpose `0ZWTe0000000ZyfOAE` is a different record whose `Name` is misspelled
`marekting_property`. Do not use it.

The account script wrote `RecordTypeId 012Te0000018UgIIAU` and `CampingCustomer__pc`.

## How to re-run a step

Run from the repository root, so `config.py` and `salesforce_client_prod.py` resolve. Pass
the batch id explicitly, even though both scripts default to this batch, so the target is
visible in the shell history.

	./.venv/Scripts/python.exe camping-grubhof-import/insert_person_accounts_camping.py 2026-08-10_new_camping_import
	./.venv/Scripts/python.exe camping-grubhof-import/insert_consents_camping.py 2026-08-10_new_camping_import

If a run stops halfway, compare `_account_processed_at` and `_consent_processed_at` against
Salesforce before starting it again.

`imp_camping_20260807.sql` runs statement by statement in DBeaver. Start with its
`CREATE INDEX` block. A mirror refresh drops every index, and the consent join then takes
about 106 seconds instead of 3.

`refresh_sf_mirrors.py` re-downloads Account, ContactPointEmail, ContactPointConsent, and
Lead from Salesforce production and rebuilds the merged consent table in the
`crm_*_sfid_prod` mirrors. Run it from the repository root when the mirrors are stale.

	./.venv/Scripts/python.exe camping-grubhof-import/refresh_sf_mirrors.py

Every mirror table is replaced, which drops its indexes, so re-run the `CREATE INDEX` block
from the SQL afterward.

## Changes since the run

Commit `1c819cc` holds both scripts exactly as they ran. The versions in this folder now
differ from that record in ways that change control flow, never the payload. A check that
renders both versions against the same staging rows shows identical Salesforce fields and an
identical CSV header.

- The account query filters `_operation = 'insert'`, so an insert run cannot pick up update
  rows staged under the same batch id.
- A job state other than `JobComplete` counts as a failure and appears in the closing
  summary. Both scripts still read `successfulResults` first, because a `Failed` or
  `Aborted` job can hold records that Salesforce already committed. Discarding them would
  create duplicates on the next run.
- Salesforce ids reach MySQL after each bulk batch, inside one transaction per batch, and
  every update must affect exactly one row.
- `PersonContactId` is fetched for the batch rows that have an account id and no contact id,
  read from MySQL rather than from the current run, so a crashed earlier run gets repaired.
- The consent script asks Salesforce which contact points already hold a consent for this
  purpose and property, drops those rows, and writes them to
  `local_data/skipped_contact_point_consents_<batch>_<timestamp>.json` with the consent ids,
  statuses, and dates. Their `_consent_processed_at` stays NULL, so a person decides. It
  also aborts when one contact point appears twice in a batch, because the bulk response
  carries no `row_id` and the matchback would mark the wrong row.

Codex (gpt-5.6-sol) reviewed those changes and found eight problems, all fixed here. Six of
them existed only because of the changes above.

## What we learned

`ContactPointEmail` hangs off the `Individual` record, not the Contact or the Account. Join
it by `EmailAddress`. A join on `ParentId = sf_person_contact_id` returns no rows. Nothing
in the pipeline fills `sf_cp_email_id`, so that backfill is a manual step between the
account step and the consent step.

Salesforce does not fill `ConsentKey__c`. We inserted one consent without the field and it
stayed empty, immediately and 60 seconds later. All 1,036,585 keyed property consents in
production come from the Apaleo and Protel migration, which built the key itself. A consent
created by hand in the UI on 2026-08-07 has no key either.

The UI column labelled Hotel Name reads `HotelName__c`, a text field, not the `Property__c`
lookup. Setting only the lookup leaves that column empty in the view Carmen filters on. A
field diff against migration record `0ZXTe000000017VOAQ` found the three fields the
migration denormalizes: `ConsentKey__c`, `HotelName__c`, and `Region__c`.

Salesforce stores non-ASCII email domains as punycode, so `nörder.it` becomes
`xn--nrder-jua.it` on both `PersonEmail` and the `ContactPointEmail`, and an `EmailAddress`
join misses those rows. A non-ASCII local part is rejected with `INVALID_EMAIL_ADDRESS`.

Bulk API 2.0 ignores empty CSV cells on update, and `#N/A` is the explicit null. The German
comments in the shared scripts state this backwards.

## Open items

Eleven rows never reached Salesforce. Seven hold an umlaut in the email local part and need
a corrected address from Carmen. Four carry a three-digit birth year (`G048590`, `G064867`,
`G073384`, `G082763`) and we can fix those by clearing the birth date and inserting again.

Batch 2 sets `CampingCustomer__pc` on the overlap rows and is not staged. Trim the
`update_contacts_bulk.py` mapper to `Id` and `CampingCustomer__pc` first, because it also
writes `ExternalID__pc`, `SourceSystem__pc`, and `InvestCustomer__pc`. The mirrors now hold
our own 89,887 accounts, so the batch-2 join grows from 4,053 rows to 93,937 without
`SourceSystem__pc <> 'camping'`. Both counts are from the 2026-08-19 mirrors.

Consent for batch 2 needs its own gate. Of its 4,011 accounts, 2,071 have no Grubhof
consent, 1,179 already hold an `OptIn`, 413 hold one that sits on a Lead, and 348 hold an
`OptOut`. Stage batch 2 with `consent_camping = 0` so the flag update cannot write consents,
then stage the rows that should get one as their own batch with `consent_camping = 1`. The
consent script only selects rows where the flag is `1`, so a batch staged with `0` produces
"Keine Records".

Carmen owns the parked groups: 2,042 household emails, 273 Lead collisions, 688 name
mismatches, and 7 emails with more than one account.

`sp_archive_crm_imp_person_accounts` does not exist in the database, although
`crm_imp_person_accounts_history` holds 14 archived batches. Recreate it from
`sql/Scripts/imp_contacts_table_definition.sql` before archiving batch 1.
