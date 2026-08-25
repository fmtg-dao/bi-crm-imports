-- Billing -> PersonMailing address recon, 2026-08-25.
-- Run against the crm_person_account_sfid_prod mirror (refreshed 2026-08-25).
-- Results as of that snapshot are noted after each query.
--
-- Background: the April 2026 migration loaded addresses into Billing*
-- (country into the custom BillingCountryCode__c, NOT BillingCountry);
-- the live integration (May 2026 onwards) writes PersonMailing*, which is
-- the correct convention (Arsal, 2026-08-25). Correction: COPY Billing*
-- into PersonMailing* where the PersonMailing side is entirely empty.
-- Billing* stays untouched.

-- 1. Era x address-side pivot.
-- era                              billing_only  mailing_only  both_same  both_differ  neither     total
-- pre-May (migration era)               732,862           187      1,280          961  467,555  1,202,845
-- May onwards (live integration)              6       250,656         11            6   42,394    293,073
SELECT
  CASE WHEN CreatedDate < '2026-05-01' THEN 'pre-May (migration era)'
       ELSE 'May onwards (live integration)' END AS era,
  SUM(BillingStreet IS NOT NULL AND BillingStreet <> ''
      AND (PersonMailingStreet IS NULL OR PersonMailingStreet = '')) AS billing_only,
  SUM((BillingStreet IS NULL OR BillingStreet = '')
      AND PersonMailingStreet IS NOT NULL AND PersonMailingStreet <> '') AS mailing_only,
  SUM(BillingStreet IS NOT NULL AND BillingStreet <> ''
      AND PersonMailingStreet IS NOT NULL AND PersonMailingStreet <> ''
      AND BillingStreet = PersonMailingStreet) AS both_same,
  SUM(BillingStreet IS NOT NULL AND BillingStreet <> ''
      AND PersonMailingStreet IS NOT NULL AND PersonMailingStreet <> ''
      AND BillingStreet <> PersonMailingStreet) AS both_differ,
  SUM((BillingStreet IS NULL OR BillingStreet = '')
      AND (PersonMailingStreet IS NULL OR PersonMailingStreet = '')) AS neither,
  COUNT(*) AS total
FROM crm_person_account_sfid_prod
GROUP BY 1;

-- 2. Source systems of the billing-only accounts: the migration across all
-- source systems, not one importer.
-- protel 474,413 / gms 175,742 / apaleo 78,218 / conda 4,480 / null 15.
SELECT SourceSystem__pc, COUNT(*) AS billing_only_accounts
FROM crm_person_account_sfid_prod
WHERE BillingStreet IS NOT NULL AND BillingStreet <> ''
  AND (PersonMailingStreet IS NULL OR PersonMailingStreet = '')
GROUP BY 1 ORDER BY 2 DESC;

-- 3. Field completeness within the billing-only population (732,868 all eras).
-- KEY FINDING: BillingCountry is almost never filled (37); the country lives
-- in the custom BillingCountryCode__c (731,743, ISO-2). BillingState is
-- negligible (12) and is NOT part of the backfill.
-- accounts 732,868 | city 730,121 | postal 722,145 | country 37 | state 12 |
-- country_code 731,743 | mailing_city_already 152 | mailing_country_already 505
SELECT
  COUNT(*) AS accounts,
  SUM(BillingCity IS NOT NULL AND BillingCity <> '') AS has_city,
  SUM(BillingPostalCode IS NOT NULL AND BillingPostalCode <> '') AS has_postal,
  SUM(BillingCountry IS NOT NULL AND BillingCountry <> '') AS has_country,
  SUM(BillingState IS NOT NULL AND BillingState <> '') AS has_state,
  SUM(BillingCountryCode__c IS NOT NULL AND BillingCountryCode__c <> '') AS has_country_code,
  SUM(PersonMailingCity IS NOT NULL AND PersonMailingCity <> '') AS mailing_city_already,
  SUM(PersonMailingCountry IS NOT NULL AND PersonMailingCountry <> '') AS mailing_country_already
FROM crm_person_account_sfid_prod
WHERE BillingStreet IS NOT NULL AND BillingStreet <> ''
  AND (PersonMailingStreet IS NULL OR PersonMailingStreet = '');

-- 4. Format check: BillingCountryCode__c uses the same ISO-2 codes the live
-- integration writes into PersonMailingCountry (AT 274,872 / DE 160,431 /
-- IT 93,287 / ...). So BillingCountryCode__c -> PersonMailingCountry is a
-- straight copy, no translation table.
SELECT BillingCountryCode__c, COUNT(*) AS n
FROM crm_person_account_sfid_prod
WHERE BillingStreet IS NOT NULL AND BillingStreet <> ''
  AND (PersonMailingStreet IS NULL OR PersonMailingStreet = '')
GROUP BY 1 ORDER BY n DESC;

-- 5. The final population (phase-2 selection): BillingStreet set AND the
-- ENTIRE PersonMailing destination empty. The ~505 accounts with a partial
-- PersonMailing side (street empty but city/country set) are skipped by
-- decision (Arsal 2026-08-25): only process where the destination is empty.
-- No BillingStreet exceeds 255 chars. Expected ~732,363.
SELECT COUNT(*) AS backfill_population
FROM crm_person_account_sfid_prod
WHERE BillingStreet IS NOT NULL AND BillingStreet <> ''
  AND (PersonMailingStreet IS NULL OR PersonMailingStreet = '')
  AND (PersonMailingCity IS NULL OR PersonMailingCity = '')
  AND (PersonMailingPostalCode IS NULL OR PersonMailingPostalCode = '')
  AND (PersonMailingCountry IS NULL OR PersonMailingCountry = '');

-- 6. Review list, NOT part of the backfill: accounts where both sides are
-- filled and differ (961 pre-May + 6 post-May). Under the PersonMailing
-- convention the Mailing side is presumed correct; the differing Billing
-- values are migration leftovers. Exported for review by the staging
-- notebook, no writes.
SELECT Id, PersonEmail, BillingStreet, PersonMailingStreet, CreatedDate, SourceSystem__pc
FROM crm_person_account_sfid_prod
WHERE BillingStreet IS NOT NULL AND BillingStreet <> ''
  AND PersonMailingStreet IS NOT NULL AND PersonMailingStreet <> ''
  AND BillingStreet <> PersonMailingStreet;
