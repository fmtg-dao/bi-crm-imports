-- Invest consent recon, 2026-08-20.
-- Rough numbers for the InvestCustomer__pc population, run against the
-- crm_*_sfid_prod mirrors (refreshed 2026-08-19 via
-- camping-grubhof-import/refresh_sf_mirrors.py).
-- Results as of that snapshot are noted after each query.

-- 1. Flag population.
-- False 1,484,119 / True 6,722.
SELECT InvestCustomer__pc, COUNT(*) AS n
FROM crm_person_account_sfid_prod
GROUP BY InvestCustomer__pc;

-- 2. Flagged accounts that have at least one ContactPointEmail.
-- 6,721 of 6,722. One flagged account has no CPE.
SELECT COUNT(DISTINCT a.Id) AS invest_with_cpe
FROM crm_person_account_sfid_prod a
JOIN crm_cp_email_sfid_prod e ON e.PartyID__c = a.PersonContactId
WHERE a.InvestCustomer__pc = 'True';

-- 3. Consent options held by flagged accounts.
-- Marketing_Property OptIn 12,316 consents / 4,455 accounts
-- marketing_central   OptIn  6,756 consents / 5,382 accounts
-- Marketing_Property OptOut    240 consents /    84 accounts
-- marketing_central  OptOut    123 consents /   108 accounts
-- residences_central OptIn/OptOut 36 / 1
-- plus 18 consents whose Name is a raw key '<CPE>|0ZWTe0000000X7FOAU|CENTRAL',
-- which is the central purpose, so they count as central consents.
SELECT c.Name, c.PrivacyConsentStatus, COUNT(*) AS consents, COUNT(DISTINCT c.AccountId) AS accounts
FROM crm_consent_sfid_prod c
JOIN crm_person_account_sfid_prod a ON c.AccountId = a.Id
WHERE a.InvestCustomer__pc = 'True'
GROUP BY c.Name, c.PrivacyConsentStatus
ORDER BY consents DESC;

-- 4. Flagged accounts with a central OptIn (marketing_central plus the
-- key-named strays on purpose 0ZWTe0000000X7FOAU).
-- 5,398.
SELECT COUNT(DISTINCT a.Id) AS invest_with_central_optin
FROM crm_person_account_sfid_prod a
JOIN crm_consent_sfid_prod c ON c.AccountId = a.Id
WHERE a.InvestCustomer__pc = 'True'
  AND c.PrivacyConsentStatus = 'OptIn'
  AND (c.Name = 'marketing_central' OR c.Name LIKE '%|0ZWTe0000000X7FOAU|CENTRAL');

-- 5. Flagged accounts with no central consent at all.
-- 1,222. The remaining 102 hold a central consent but no OptIn, so they
-- are opted out. 6,722 = 5,398 OptIn + 102 OptOut-only + 1,222 nothing.
SELECT COUNT(DISTINCT a.Id) AS invest_no_central_consent
FROM crm_person_account_sfid_prod a
WHERE a.InvestCustomer__pc = 'True'
  AND NOT EXISTS (
    SELECT 1 FROM crm_consent_sfid_prod c
    WHERE c.AccountId = a.Id
      AND (c.Name = 'marketing_central' OR c.Name LIKE '%|0ZWTe0000000X7FOAU|CENTRAL')
  );

-- 6. Every consent Name in production. There is no invest-specific purpose
-- anywhere: only Marketing_Property, marketing_central, hotel_newsletter,
-- residences_central, and the key-named strays on the central purpose.
SELECT Name, COUNT(*) AS n
FROM crm_cp_consent_sfid_prod
GROUP BY Name
ORDER BY n DESC
LIMIT 15;

-- 7. Live SOQL check of DataUsePurpose (2026-08-20, read-only, via
-- sf_objects_download.sf_query: SELECT FIELDS(ALL) FROM DataUsePurpose).
-- Six purposes exist, all IsActive__c = true:
--   0ZWTe0000000X41OAE  camping_central
--   0ZWTe0000000X5dOAE  invest_central      <- exists since 2026-04-14, ZERO consents reference it
--   0ZWTe0000000ZyfOAE  marekting_property  <- typo record, do not use
--   0ZWTe0000000X7FOAU  marketing_central
--   0ZWTe0000000X8rOAE  marketing_property  (only one with RequiresHotel__c = true)
--   0ZWTe0000000XATOA2  residences_central
-- The May 2026 conda investor batch wrote marketing_central although
-- invest_central already existed.

-- 8. Population cross-tab: flag x InvestmentStatus__pc x conda source.
-- flag + status + conda        5,869   the core group
-- flag only (gms 510, protel 55, apaleo 6)  571   flag came from hotel systems, no status
-- no flag, status, not conda     335   see query 10
-- flag + status, not conda       279
-- flag + conda, no status          3
-- Flag total 6,722. Status total 6,483.
SELECT
  InvestCustomer__pc AS flag,
  (InvestmentStatus__pc IS NOT NULL AND InvestmentStatus__pc <> '') AS has_invest_status,
  (SourceSystem__pc = 'conda') AS from_conda,
  COUNT(*) AS n
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'True'
   OR (InvestmentStatus__pc IS NOT NULL AND InvestmentStatus__pc <> '')
   OR SourceSystem__pc = 'conda'
GROUP BY 1, 2, 3
ORDER BY n DESC;

-- 9. InvestmentStatus__pc x flag.
-- Ambassador 140 True / 1 False. Blue 407 True. Diamond 5,246 True / 5 False.
-- Gold 353 True. Owner 2 True / 329 False.
SELECT InvestmentStatus__pc, InvestCustomer__pc, COUNT(*) AS n
FROM crm_person_account_sfid_prod
WHERE InvestmentStatus__pc IS NOT NULL AND InvestmentStatus__pc <> ''
GROUP BY 1, 2
ORDER BY InvestmentStatus__pc, InvestCustomer__pc;

-- 10. The 335 status-but-no-flag accounts by source.
-- Owner: Excel Owner 122, gms 117, protel 78, apaleo 11, null 1.
-- Diamond: gms 2, protel 2, apaleo 1. Ambassador: gms 1.
-- Owners came in through an "Excel Owner" upload and the hotel systems,
-- never through conda, so whether Owner counts as an invest customer is a
-- business question, not a defect per se.
SELECT InvestmentStatus__pc, SourceSystem__pc, COUNT(*) AS n
FROM crm_person_account_sfid_prod
WHERE InvestCustomer__pc = 'False'
  AND InvestmentStatus__pc IS NOT NULL AND InvestmentStatus__pc <> ''
GROUP BY 1, 2 ORDER BY n DESC;

-- Decision context, 2026-08-20: invest_central was never written because the
-- permission for it was missing. That permission now exists, so the plan is
-- to write invest_central as an ADDITIONAL consent for invest customers and
-- leave marketing_central untouched. Audit first, write later.

-- 11. Where the central consents of flagged accounts come from.
-- gms 4,353 / protel Reservierung 1,200 / apaleo Reservierung 910 /
-- no source 174 / gustaffo 153 / conda 89.
SELECT c.CaptureSource, c.SourceSystem__c, COUNT(*) AS n
FROM crm_consent_sfid_prod c
JOIN crm_person_account_sfid_prod a ON c.AccountId = a.Id
WHERE a.InvestCustomer__pc = 'True'
  AND c.Name = 'marketing_central'
GROUP BY c.CaptureSource, c.SourceSystem__c
ORDER BY n DESC;

-- 12. Full audit table: the population groups with CPE and consent coverage.
-- Needs the indexes from create_mirror_indexes.sql, without them this runs
-- for more than 10 minutes, with them 1.4s.
--
-- flag  status conda  accounts  HasCPE  CentralOptIn  CentralAny  PropertyOptIn
-- True  set    yes       5,869   5,868         4,920       5,010          4,046
-- True  empty  no          571     571           360         371            338
-- False set    no          334     334           178         180            137
-- True  set    no          279     279           115         116             70
-- True  empty  yes           3       3             3           3              1
-- False set    null          1       1             0           0              0
--
-- OptOut-only per group = CentralAny - CentralOptIn. No central = accounts - CentralAny.
-- Even in the clean conda core, 949 accounts lack a central OptIn
-- (859 no central consent, 90 opted out).
SELECT
  a.InvestCustomer__pc AS InvestCustomer,
  (a.InvestmentStatus__pc IS NOT NULL AND a.InvestmentStatus__pc <> '') AS HasStatus,
  (a.SourceSystem__pc = 'conda') AS FromConda,
  COUNT(*) AS accounts,
  SUM(EXISTS(SELECT 1 FROM crm_cp_email_sfid_prod e
             WHERE e.PartyID__c = a.PersonContactId)) AS HasCPE,
  SUM(EXISTS(SELECT 1 FROM crm_consent_sfid_prod c
             WHERE c.AccountId = a.Id AND c.PrivacyConsentStatus = 'OptIn'
               AND (c.Name = 'marketing_central' OR c.Name LIKE '%|0ZWTe0000000X7FOAU|CENTRAL'))) AS CentralOptIn,
  SUM(EXISTS(SELECT 1 FROM crm_consent_sfid_prod c
             WHERE c.AccountId = a.Id
               AND (c.Name = 'marketing_central' OR c.Name LIKE '%|0ZWTe0000000X7FOAU|CENTRAL'))) AS CentralAny,
  SUM(EXISTS(SELECT 1 FROM crm_consent_sfid_prod c
             WHERE c.AccountId = a.Id AND c.PrivacyConsentStatus = 'OptIn'
               AND c.Name = 'Marketing_Property')) AS PropertyOptIn
FROM crm_person_account_sfid_prod a
WHERE a.InvestCustomer__pc = 'True'
   OR (a.InvestmentStatus__pc IS NOT NULL AND a.InvestmentStatus__pc <> '')
   OR a.SourceSystem__pc = 'conda'
GROUP BY 1, 2, 3
ORDER BY accounts DESC;

-- 13. Ground truth check against the May 2026 conda export
-- (stg_imp_invest_20260519: conda_uid, email, personaccountid, status,
-- status_ablaufdatum, i.e. the fields InvestmentStatus__pc and
-- InvestmentExpirationDate__pc mirror). SourceSystem__pc = 'conda' only
-- records which import created the account, so list membership is the
-- real investor signal, not the source field.
--
-- flag  status  accounts  InMayListById  InMayListByEmail
-- True  set        6,148          5,484             5,701
-- True  empty        574              3                 4
-- False set          335              0                 3
--
-- The 571 hotel-sourced checkbox-only accounts are absent from the conda
-- list: their InvestCustomer flag has no conda backing. The Owners are
-- absent too, consistent with a deliberate separate upload. The ~450
-- flag+status accounts unmatched by email are later joiners or list churn;
-- a fresh conda export would settle them.
SELECT
  a.InvestCustomer__pc AS InvestCustomer,
  (a.InvestmentStatus__pc IS NOT NULL AND a.InvestmentStatus__pc <> '') AS HasStatus,
  COUNT(*) AS accounts,
  SUM(EXISTS(SELECT 1 FROM stg_imp_invest_20260519 s WHERE s.personaccountid = a.Id)) AS InMayListById,
  SUM(EXISTS(SELECT 1 FROM stg_imp_invest_20260519 s WHERE s.email = a.PersonEmail)) AS InMayListByEmail
FROM crm_person_account_sfid_prod a
WHERE a.InvestCustomer__pc = 'True'
   OR (a.InvestmentStatus__pc IS NOT NULL AND a.InvestmentStatus__pc <> '')
GROUP BY 1, 2
ORDER BY accounts DESC;

-- 14. Oleg's plan (mail 2026-08-20): write invest_central consents for
-- investors and invest prospects, OptOut for contacts on the invest
-- exclusion list, selection based on the investment fields plus the
-- InvestCustomer flag. Working population assumption per Arsal:
-- InvestCustomer__pc = 'True' AND InvestmentStatus__pc set AND <> 'Owner'
-- = 6,146 accounts (6,145 with CPE, 6,219 CPEs, 73 accounts with a 2nd CPE,
-- one account with no CPE at all). 1,978 of the 6,146 also exist as a Lead
-- with the same email; the consent write does not touch the Lead side.
--
-- Exclusion list received 2026-08-20:
-- invest-consent/data/20260820_FMTG Invest_exclusion list Salesforce.xls
-- (gitignored, password-protected). 111 rows, columns email/country,
-- 108 unique emails after trim+lowercase. Matched by email
-- (pandas, normalized, against the 2026-08-19 mirrors):
--   in the 6,146 population:            11  -> OptOut; OptIn split 6,135 / 11
--   matching any other person account:  46
--   matching a lead:                    10
--   matching nothing in SF:             49
-- Open question for Oleg: do the 95 excluded people outside the population
-- get a pre-emptive invest_central OptOut, or are they simply not written?

-- 15. Oleg's "214 Diamond Spirit Club members without invest fields":
-- reproduced exactly from the loyalty mirror (2026-07-29). Of 5,409 members
-- with tier Diamond Spirit Club (TierName__c or LegacyTier__c):
-- 5,186 flag+status / 194 no invest fields / 17 no account / 9 flag only /
-- 3 status only. 194+17+3 = 214 without the flag.
-- Conda cross-check: of the 197 with an account, only 6 match the May conda
-- export by email, 0 by gms_loyalty_id = MembershipNumber. The tier (mostly
-- LegacyTier__c, ex-gms) is the only investor signal for the other 208.
-- Reading: stale legacy tiers, not a gap in the consent selection. Fix the 6,
-- hand the 208 to the loyalty owner.
SELECT
  SUM(a.Id IS NOT NULL) AS with_account,
  SUM(EXISTS(SELECT 1 FROM stg_imp_invest_20260519 s WHERE s.email = a.PersonEmail)) AS in_conda_list_by_email,
  SUM(EXISTS(SELECT 1 FROM stg_imp_invest_20260519 s WHERE s.gms_loyalty_id = l.MembershipNumber)) AS in_conda_list_by_membership
FROM crm_loyality_sfid_prod l
LEFT JOIN crm_person_account_sfid_prod a ON a.PersonContactId = l.ContactId
WHERE (l.TierName__c = 'Diamond Spirit Club' OR l.LegacyTier__c = 'Diamond Spirit Club')
  AND (a.Id IS NULL OR a.InvestCustomer__pc = 'False');

-- 16. Decision-shrinking recon for the consent-write plan (2026-08-20).
-- (a) ContactPointEmail cardinality across the 6,146 population accounts:
--       n_cpe 0 -> 1 account | n_cpe 1 -> 6,071 | n_cpe 2 -> 74
--     98.8% have exactly one CPE, so per-account vs per-CPE only differs
--     for 74 accounts (148 CPEs). Supersedes the "73" in note 14, which was
--     derived from totals (6,219 - 6,145) rather than counted directly.
-- (b) Lead twins by email (population CPE EmailAddress vs Lead Email,
--     lower+trim): 2,005 population accounts share an email with >= 1 lead,
--     5,043 lead rows in total. Status of those lead rows:
--       Processed 2,506 (2,499 linked via RelatedPersonAccount__c)
--       New 2,498 | Open 39 (none linked)
--     Supersedes the 1,978 in note 14 (different matching method).
SELECT t.n_cpe, COUNT(*) AS n_accounts
FROM (
  SELECT a.Id, COUNT(e.Id) AS n_cpe
  FROM crm_person_account_sfid_prod a
  LEFT JOIN crm_cp_email_sfid_prod e ON e.PartyID__c = a.PersonContactId
  WHERE a.InvestCustomer__pc = 'True'
    AND a.InvestmentStatus__pc IS NOT NULL AND a.InvestmentStatus__pc <> ''
    AND a.InvestmentStatus__pc <> 'Owner'
  GROUP BY a.Id
) t
GROUP BY t.n_cpe
ORDER BY t.n_cpe;

SELECT
  SUM(l.RelatedPersonAccount__c IS NOT NULL AND l.RelatedPersonAccount__c <> '') AS leads_linked_via_RelatedPersonAccount,
  l.Status,
  COUNT(*) AS lead_rows
FROM (
  SELECT DISTINCT LOWER(TRIM(e.EmailAddress)) AS email
  FROM crm_person_account_sfid_prod a
  JOIN crm_cp_email_sfid_prod e ON e.PartyID__c = a.PersonContactId
  WHERE a.InvestCustomer__pc = 'True'
    AND a.InvestmentStatus__pc IS NOT NULL AND a.InvestmentStatus__pc <> ''
    AND a.InvestmentStatus__pc <> 'Owner'
) p
JOIN crm_person_lead_sfid_prod l ON LOWER(TRIM(l.Email)) = p.email
GROUP BY l.Status;
