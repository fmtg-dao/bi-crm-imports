# Plan: write invest_central consents to Salesforce

## Context

The DataUsePurpose `invest_central` (Id `0ZWTe0000000X5dOAE`) has existed since
2026-04-14 with zero ContactPointConsent records because permission was
missing. Permission is now granted. Oleg's mail (2026-08-20) asks for
invest_central consents for investors and invest prospects, with OptOut for the
people on the delivered exclusion list. The audit is complete
(`../recon_invest_consent.sql`, queries 1-16). This plan covers the write.

## Scope

In scope:

- ContactPointConsent records for `invest_central`, population =
  `InvestCustomer__pc = 'True' AND InvestmentStatus__pc set AND <> 'Owner'`
  (6,146 accounts, working assumption pending Oleg's confirmation, decision D2).
- `PrivacyConsentStatus = 'OptOut'` for the 11 exclusion-list matches inside
  the population, `'OptIn'` for the rest.
- Staging, CSV generation, Bulk API 2.0 load, verification.

Explicitly out of scope (separate data-fix track):

- The 571 flag-only accounts, the 6 flagless Diamond/Ambassador accounts, the
  3 status-less conda accounts, the 208 stale loyalty tiers, the 19 consent
  rows with a raw ConsentKey pasted into Name, anything touching
  marketing_central, and all Lead records.

## Constraints

- Bulk API 2.0 insert via `salesforce_client_prod.py`; no external Id field on
  ContactPointConsent, so idempotency comes from staging-side locks and a
  pre-load skip query, not upsert (see phase 3).
- CPE resolution only via `crm_cp_email_sfid_prod.PartyID__c =
  crm_person_account_sfid_prod.PersonContactId`. Never by EmailAddress:
  2,005 population accounts share an email with a Lead, and camping's
  EmailAddress backfill was only safe because those emails were brand new.
- Bulk API 2.0 ignores empty CSV cells. The mapper drops None fields entirely;
  never send a blank column expecting a Salesforce default.
- Mirrors were refreshed 2026-08-19. A refresh drops the indexes; re-run
  `../create_mirror_indexes.sql` after every refresh.
- The exclusion list and all files under `../data/` are PII and stay
  gitignored. Nothing is pushed; all commits stay local until Arsal says
  otherwise.
- The Bulk API load (phase 5) does not run without Arsal's explicit go-ahead.

## Alternatives considered

1. **Adapt the camping consent pipeline** (chosen). Stage the population in
   `crm_imp_person_accounts` with `consent_invest = 1` and a prefilled
   `sf_cp_email_id`, then run an invest copy of the consent script. Reuses the
   `_consent_processed_at` rerun lock and the proven job control flow.
2. Mirror-only direct CSV, no working table. Fewer moving parts but loses the
   per-row processed-at lock, so a crashed job cannot be resumed safely.
   Rejected.
3. Let Oleg's API job do the write and only audit afterwards. His mail
   describes his own job writing invest_central. Whether we write or he does is
   decision D1 and must be settled before phase 5; both writing means
   duplicates.

## Applicable skills

`how` before touching the consent script family, `interrogate` on the load
design before phase 5, `/deslop` on every diff before commit, `unslop` on any
prose, `show-me-your-work` for the load-day log.

## Phases

1. [Decision memo](phase-1-decision-memo.md), resolves D1-D8
2. [Stage the population](phase-2-staging.md)
3. [Consent script for invest_central](phase-3-consent-script.md)
4. [Probe record](phase-4-probe.md)
5. [Bulk load](phase-5-bulk-load.md), gated on Arsal
6. [Verification](phase-6-verification.md)

## Verification

Definition of done: the post-load consent pivot (recon query pattern, purpose
columns as OptIn counts) matches the predicted after-state computed in phase 2,
and live SOQL counts match the mirror counts. No control-ui/cli skill covers
the Salesforce surface; the substitute is the named SOQL probes in phases 4
and 6.
