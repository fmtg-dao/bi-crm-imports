-- =============================================================================
-- 05: Recon for the invest PersonMailingCountry backfill
-- =============================================================================
-- Goal: for invest customers, PersonMailingCountry should hold the ISO English
-- country NAME ("Austria", not "AT"). The org has no mailing country-code field
-- (State/Country Picklists are off; BillingCountryCode__c is the only ISO-2
-- picklist and has no mailing twin), so the name goes into the standard
-- free-text field and the ISO-2 code stays queryable via BillingCountryCode__c
-- / NationalityCountryCode__pc.
--
-- Core rule: address data only ever moves as a COMPLETE block from ONE source.
-- Never fill a single mailing field next to mailing fields from elsewhere -
-- that is the only way to end up with a Vienna street under a Germany country.
--
-- Populations (invest = InvestCustomer__pc, 6,158 as of 2026-08-26 live):
--   A: mailing block fully empty, BillingStreet set        -> copy full billing
--      address block, country as mapped name
--   B: mailing block fully empty, no BillingStreet, but
--      BillingCountryCode__c set                           -> country name only
--      (no city exists that could contradict it)
--   C: PersonMailingCountry holds a bare ISO-2 code        -> in-place code->name
--      conversion, address untouched. Includes the 23 accounts where billing
--      and mailing country genuinely differ (AT->DE 21, AT->CH 1, DE->HR 1):
--      mailing wins, we only convert its format.
--   D: partial mailing block (street/city set, country
--      empty)                                              -> NOT staged; review
--      export (the Vienna/Germany danger zone)
--
-- Values already equal to a name (e.g. the 4 "Austria") are skipped - nothing
-- to convert.
--
-- Mapping source: nationality-backfill/country_names.py, generated from
-- https://github.com/datasets/country-codes (CLDR display names, "St."/"&"
-- expanded) + overrides (GB/US spelled out, UK alias, XK Kosovo). FL and XX
-- are deliberately unmapped; rows carrying them are excluded, never guessed.
--
-- Prerequisites: fresh mirror (repo root: python sf_objects_download.py),
-- then re-apply 01_create_mirror_indexes.sql (the refresh drops the indexes).
-- The refresh also picks up the 2,669 nationality values loaded 2026-08-26.
--
-- The mirror stores empty as NULL or ''; both are treated as empty throughout.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Query 1: the four populations from the fresh mirror
-- -----------------------------------------------------------------------------
-- These WHERE fragments are copied verbatim into 06_stage_mailing_country.ipynb;
-- the observed counts below become the staging contract.

SELECT
    COUNT(*) AS invest_total,
    -- mailing block fully empty
    SUM(  (PersonMailingStreet     IS NULL OR PersonMailingStreet     = '')
      AND (PersonMailingCity       IS NULL OR PersonMailingCity       = '')
      AND (PersonMailingPostalCode IS NULL OR PersonMailingPostalCode = '')
      AND (PersonMailingCountry    IS NULL OR PersonMailingCountry    = '')) AS block_empty,
    -- A: block empty + billing street present
    SUM(  (PersonMailingStreet     IS NULL OR PersonMailingStreet     = '')
      AND (PersonMailingCity       IS NULL OR PersonMailingCity       = '')
      AND (PersonMailingPostalCode IS NULL OR PersonMailingPostalCode = '')
      AND (PersonMailingCountry    IS NULL OR PersonMailingCountry    = '')
      AND  BillingStreet IS NOT NULL AND BillingStreet <> ''            ) AS pop_a,
    -- B: block empty, no billing street, but billing country code present
    SUM(  (PersonMailingStreet     IS NULL OR PersonMailingStreet     = '')
      AND (PersonMailingCity       IS NULL OR PersonMailingCity       = '')
      AND (PersonMailingPostalCode IS NULL OR PersonMailingPostalCode = '')
      AND (PersonMailingCountry    IS NULL OR PersonMailingCountry    = '')
      AND (BillingStreet IS NULL OR BillingStreet = '')
      AND  BillingCountryCode__c IS NOT NULL AND BillingCountryCode__c <> '') AS pop_b,
    -- C: mailing country is a bare 2-letter code
    SUM(PersonMailingCountry REGEXP '^[A-Za-z]{2}$')                      AS pop_c,
    -- already a name (nothing to do)
    SUM(PersonMailingCountry IS NOT NULL AND PersonMailingCountry <> ''
      AND PersonMailingCountry NOT REGEXP '^[A-Za-z]{2}$')                AS already_name,
    -- D: partial block - some address data but no country (NOT staged)
    SUM(  (PersonMailingCountry IS NULL OR PersonMailingCountry = '')
      AND (   (PersonMailingStreet     IS NOT NULL AND PersonMailingStreet     <> '')
           OR (PersonMailingCity       IS NOT NULL AND PersonMailingCity       <> '')
           OR (PersonMailingPostalCode IS NOT NULL AND PersonMailingPostalCode <> ''))) AS pop_d,
    -- residual: block empty and no billing source at all (out of reach)
    SUM(  (PersonMailingStreet     IS NULL OR PersonMailingStreet     = '')
      AND (PersonMailingCity       IS NULL OR PersonMailingCity       = '')
      AND (PersonMailingPostalCode IS NULL OR PersonMailingPostalCode = '')
      AND (PersonMailingCountry    IS NULL OR PersonMailingCountry    = '')
      AND (BillingStreet IS NULL OR BillingStreet = '')
      AND (BillingCountryCode__c IS NULL OR BillingCountryCode__c = '')  ) AS unreachable
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True';

-- Observed (mirror 2026-08-26, post-refresh):
--   <PASTE>


-- -----------------------------------------------------------------------------
-- Query 2: distinct-code inventory -> mapping coverage
-- -----------------------------------------------------------------------------
-- Every code that A (billing code), B (billing code) or C (mailing code) can
-- produce must be a key in country_names.COUNTRY_NAMES. 06 re-asserts this in
-- Python; this query is the human-readable version.

SELECT 'billing' AS src, BillingCountryCode__c AS code, COUNT(*) AS n
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True'
  AND BillingCountryCode__c IS NOT NULL AND BillingCountryCode__c <> ''
GROUP BY 2
UNION ALL
SELECT 'mailing', PersonMailingCountry, COUNT(*)
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True'
  AND PersonMailingCountry REGEXP '^[A-Za-z]{2}$'
GROUP BY 2
ORDER BY 1, 3 DESC;

-- Observed:
--   <PASTE>


-- -----------------------------------------------------------------------------
-- Query 3: postal-pattern sanity on population A billing data
-- -----------------------------------------------------------------------------
-- If a billing address is internally inconsistent in the source (Vienna street
-- with a DE code), the copy would faithfully reproduce it. Cheap smoke test:
-- postal-code shape by country. AT/CH: 4 digits, DE/IT: 5 digits. Mismatches
-- get eyeballed before staging (include/exclude decision, documented here).

SELECT BillingCountryCode__c AS code,
       COUNT(*) AS n,
       SUM(CASE
             WHEN BillingCountryCode__c IN ('AT','CH') THEN BillingPostalCode NOT REGEXP '^[0-9]{4}$'
             WHEN BillingCountryCode__c IN ('DE','IT') THEN BillingPostalCode NOT REGEXP '^[0-9]{5}$'
             ELSE 0
           END) AS postal_mismatch,
       SUM(BillingPostalCode IS NULL OR BillingPostalCode = '') AS postal_empty,
       SUM(BillingCountryCode__c IS NULL OR BillingCountryCode__c = '') AS code_empty
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True'
  AND (PersonMailingStreet     IS NULL OR PersonMailingStreet     = '')
  AND (PersonMailingCity       IS NULL OR PersonMailingCity       = '')
  AND (PersonMailingPostalCode IS NULL OR PersonMailingPostalCode = '')
  AND (PersonMailingCountry    IS NULL OR PersonMailingCountry    = '')
  AND  BillingStreet IS NOT NULL AND BillingStreet <> ''
GROUP BY 1 WITH ROLLUP;

-- Observed:
--   <PASTE>

-- Note: A rows with an EMPTY BillingCountryCode__c still get street/city/postal
-- copied - just no country. They keep the block-from-one-source guarantee; the
-- country simply stays unknown rather than guessed.


-- -----------------------------------------------------------------------------
-- Query 4: the billing<>mailing conflicts (population C, documented)
-- -----------------------------------------------------------------------------
-- Mailing wins; we only convert its format. Listed so the decision is on file.

SELECT Id, PersonEmail, BillingCountryCode__c, PersonMailingCountry,
       PersonMailingCity, LastModifiedDate
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True'
  AND PersonMailingCountry REGEXP '^[A-Za-z]{2}$'
  AND BillingCountryCode__c IS NOT NULL AND BillingCountryCode__c <> ''
  AND UPPER(PersonMailingCountry) <> UPPER(BillingCountryCode__c);

-- Observed:
--   <PASTE>


-- -----------------------------------------------------------------------------
-- Query 5: population D - the partial blocks (review export, never staged)
-- -----------------------------------------------------------------------------
-- Street/city from one import, country missing: filling the country from
-- billing here is exactly how a Vienna/Germany mismatch would be born.
-- 06 exports these to local_data/invest_mailing_partial_review.csv.

SELECT Id, PersonEmail, PersonMailingStreet, PersonMailingCity,
       PersonMailingPostalCode, BillingCountryCode__c, BillingCity,
       SourceSystem__pc, LastModifiedDate
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True'
  AND (PersonMailingCountry IS NULL OR PersonMailingCountry = '')
  AND (   (PersonMailingStreet     IS NOT NULL AND PersonMailingStreet     <> '')
       OR (PersonMailingCity       IS NOT NULL AND PersonMailingCity       <> '')
       OR (PersonMailingPostalCode IS NOT NULL AND PersonMailingPostalCode <> ''));

-- Observed:
--   <PASTE>
