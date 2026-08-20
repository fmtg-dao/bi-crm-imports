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

-- 7. Where the central consents of flagged accounts come from.
-- gms 4,353 / protel Reservierung 1,200 / apaleo Reservierung 910 /
-- no source 174 / gustaffo 153 / conda 89.
SELECT c.CaptureSource, c.SourceSystem__c, COUNT(*) AS n
FROM crm_consent_sfid_prod c
JOIN crm_person_account_sfid_prod a ON c.AccountId = a.Id
WHERE a.InvestCustomer__pc = 'True'
  AND c.Name = 'marketing_central'
GROUP BY c.CaptureSource, c.SourceSystem__c
ORDER BY n DESC;
