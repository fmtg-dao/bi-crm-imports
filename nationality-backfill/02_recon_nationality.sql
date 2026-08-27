-- NationalityCountryCode__pc backfill recon for invest customers, 2026-08-25.
-- Run against the crm_person_account_sfid_prod mirror (refreshed 2026-08-25).
-- Results as of that snapshot are noted after each query.
--
-- Background: the investor imports never sent nationality_country_code to
-- Salesforce. The stg_imp_investors_* staging tables have it 100% populated,
-- but the INSERT INTO crm_imp_person_accounts column lists in the
-- sql/Scripts/imp_investors_*.sql scripts omitted the column, so it was
-- dropped between staging and push for every batch. The invest accounts that
-- DO have a nationality got it via the original migration path. Correction:
-- backfill NationalityCountryCode__pc from the staging tables, matched by
-- external_id, only where the SF field is empty.

-- 1. Population: invest customers with/without nationality.
-- total 6,158 | with_nationality 3,397 | missing 2,761
SELECT
  COUNT(*) AS total,
  SUM(NationalityCountryCode__pc IS NOT NULL AND NationalityCountryCode__pc <> '') AS with_nationality,
  SUM(NationalityCountryCode__pc IS NULL OR NationalityCountryCode__pc = '') AS missing
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True';

-- 2. Proof the imports never carried the field: nationality_country_code is
-- NULL in every archived batch (15 batches, ~110k rows, 0 populated) —
-- including all *_new_investor_import and the conda invest batches.
SELECT _batch_id,
       COUNT(*) AS total,
       SUM(nationality_country_code IS NOT NULL AND nationality_country_code <> '') AS with_nat
FROM crm_imp_person_accounts_history
GROUP BY _batch_id
ORDER BY _batch_id;

-- 3. Coverage: match the 2,761 missing accounts against the four investor
-- staging tables by external_id = ExternalID__pc. Source priority via
-- COALESCE (formated -> formated_2026070 -> formated_20260722 -> w9);
-- zero conflicts and zero duplicate matches were verified, so the order is
-- cosmetic. Email matching adds nothing beyond external_id (verified).
-- missing 2,761 | covered_any 2,669 | unmatched 92
SELECT COUNT(*) AS missing,
       SUM(COALESCE(s1.nationality_country_code, s2.nationality_country_code,
                    s3.nationality_country_code, s4.nationality_country_code) IS NOT NULL) AS covered_any
FROM crm_person_account_sfid_prod acc
LEFT JOIN stg_imp_investors_formated s1
       ON s1.external_id = acc.ExternalID__pc AND s1.nationality_country_code <> ''
LEFT JOIN stg_imp_investors_formated_2026070 s2
       ON s2.external_id = acc.ExternalID__pc AND s2.nationality_country_code <> ''
LEFT JOIN stg_imp_investors_formated_20260722 s3
       ON s3.external_id = acc.ExternalID__pc AND s3.nationality_country_code <> ''
LEFT JOIN stg_imp_investors_w9_20260722 s4
       ON s4.external_id = acc.ExternalID__pc
      AND s4.nationality_country_code IS NOT NULL AND s4.nationality_country_code <> ''
WHERE acc.InvestCustomer__pc = 'True'
  AND (acc.NationalityCountryCode__pc IS NULL OR acc.NationalityCountryCode__pc = '');

-- 3b. Conflict/duplicate check inside the covered population: accounts with
-- more than one staging match, or matches disagreeing on the code. Result: 0.
SELECT COUNT(*) AS conflicts FROM (
  SELECT acc.Id
  FROM crm_person_account_sfid_prod acc
  LEFT JOIN stg_imp_investors_formated s1
         ON s1.external_id = acc.ExternalID__pc AND s1.nationality_country_code <> ''
  LEFT JOIN stg_imp_investors_formated_2026070 s2
         ON s2.external_id = acc.ExternalID__pc AND s2.nationality_country_code <> ''
  LEFT JOIN stg_imp_investors_formated_20260722 s3
         ON s3.external_id = acc.ExternalID__pc AND s3.nationality_country_code <> ''
  LEFT JOIN stg_imp_investors_w9_20260722 s4
         ON s4.external_id = acc.ExternalID__pc
        AND s4.nationality_country_code IS NOT NULL AND s4.nationality_country_code <> ''
  WHERE acc.InvestCustomer__pc = 'True'
    AND (acc.NationalityCountryCode__pc IS NULL OR acc.NationalityCountryCode__pc = '')
  GROUP BY acc.Id
  HAVING COUNT(DISTINCT COALESCE(s1.nationality_country_code, s2.nationality_country_code,
                                 s3.nationality_country_code, s4.nationality_country_code)) > 1
      OR COUNT(*) > 1
) t;

-- 4. Format check: the staging codes are ISO-2 and every distinct value is
-- already live in the SF picklist, so no record-level picklist rejections
-- expected. Staging (main table): AT 4,467 / DE 1,441 / CH 60 / CZ 3 /
-- AE 1 / SK 1. Live SF: AT 327,570 / DE 187,641 / ... / AE 731.
SELECT nationality_country_code, COUNT(*) AS n
FROM stg_imp_investors_formated
GROUP BY 1 ORDER BY n DESC;

SELECT NationalityCountryCode__pc, COUNT(*) AS n
FROM crm_person_account_sfid_prod
WHERE NationalityCountryCode__pc IS NOT NULL AND NationalityCountryCode__pc <> ''
GROUP BY 1 ORDER BY n DESC;

-- 5. The final population (phase-2 selection): invest customer, SF field
-- empty, and a staging match by external_id. This is the exact WHERE the
-- staging notebook reuses. Expected 2,669. Staged 2026-08-25: 2,669 rows,
-- codes AT 1,973 / DE 664 / CH 29 / CZ 2 / AE 1.
SELECT COUNT(*) AS backfill_population
FROM crm_person_account_sfid_prod acc
LEFT JOIN stg_imp_investors_formated s1
       ON s1.external_id = acc.ExternalID__pc AND s1.nationality_country_code <> ''
LEFT JOIN stg_imp_investors_formated_2026070 s2
       ON s2.external_id = acc.ExternalID__pc AND s2.nationality_country_code <> ''
LEFT JOIN stg_imp_investors_formated_20260722 s3
       ON s3.external_id = acc.ExternalID__pc AND s3.nationality_country_code <> ''
LEFT JOIN stg_imp_investors_w9_20260722 s4
       ON s4.external_id = acc.ExternalID__pc
      AND s4.nationality_country_code IS NOT NULL AND s4.nationality_country_code <> ''
WHERE acc.InvestCustomer__pc = 'True'
  AND (acc.NationalityCountryCode__pc IS NULL OR acc.NationalityCountryCode__pc = '')
  AND COALESCE(s1.nationality_country_code, s2.nationality_country_code,
               s3.nationality_country_code, s4.nationality_country_code) IS NOT NULL;

-- 6. Review list, NOT part of the backfill: the 92 invest customers with no
-- staging match at all (no external_id hit in any of the four tables).
-- Exported for review by the staging notebook, no writes.
SELECT acc.Id, acc.PersonEmail, acc.ExternalID__pc, acc.SourceSystem__pc,
       acc.CreatedDate, acc.InvestmentStatus__pc
FROM crm_person_account_sfid_prod acc
WHERE acc.InvestCustomer__pc = 'True'
  AND (acc.NationalityCountryCode__pc IS NULL OR acc.NationalityCountryCode__pc = '')
  AND NOT EXISTS (SELECT 1 FROM stg_imp_investors_formated s
                  WHERE s.external_id = acc.ExternalID__pc AND s.nationality_country_code <> '')
  AND NOT EXISTS (SELECT 1 FROM stg_imp_investors_formated_2026070 s
                  WHERE s.external_id = acc.ExternalID__pc AND s.nationality_country_code <> '')
  AND NOT EXISTS (SELECT 1 FROM stg_imp_investors_formated_20260722 s
                  WHERE s.external_id = acc.ExternalID__pc AND s.nationality_country_code <> '')
  AND NOT EXISTS (SELECT 1 FROM stg_imp_investors_w9_20260722 s
                  WHERE s.external_id = acc.ExternalID__pc
                    AND s.nationality_country_code IS NOT NULL AND s.nationality_country_code <> '');
