-- =====================================================================
-- Invest pre-registration import — recon (read-only)
-- Läuft NACH dem Raw-Load der CSV (03_stage_prereg.ipynb, Schritt 1 legt
-- stg_imp_prereg_20260828 an) und NACH refresh_sf_mirrors.py +
-- 01_create_mirror_indexes.sql. Friert die Funnel-Zahlen ein, bevor
-- irgendetwas nach crm_imp_person_accounts gestaged wird.
-- CSV: invest_preregistrations_2022-to-2026-08-25_exported-2026-08-26.csv
-- =====================================================================

-- 0. Mirror-Frische: MAX(LastModifiedDate) muss < 1 Tag alt sein, sonst
--    Refresh wiederholen (die Live-Integration schreibt den ganzen Tag).
SELECT MAX(LastModifiedDate) AS mirror_max_modified,
       COUNT(*)              AS mirror_rows,
       SUM(InvestCustomer__pc = 'true') AS invest_flagged
FROM   crm_person_account_sfid_prod;

-- 1. Raw-Zahlen: 3.192 Zeilen erwartet (die CSV hat 3.193 Zeilen, aber die
--    letzte ist leer und wird beim Raw-Load verworfen), external_id/email
--    beide unique, keine Zeile ohne E-Mail.
SELECT COUNT(*)                                   AS raw_rows,
       COUNT(DISTINCT external_id)                AS uniq_external_ids,
       COUNT(DISTINCT email)                      AS uniq_emails,
       SUM(email IS NULL OR email = '')           AS no_email,
       SUM(consent_invest = 1)                    AS consent_invest_1,
       SUM(consent_invest = 0 OR consent_invest IS NULL) AS consent_invest_0,
       SUM(consent_central = 1)                   AS consent_central_1_MISTAKE_IGNORED
FROM   stg_imp_prereg_20260828;

-- 2. investment_status-Werte gegen die Picklist prüfen ('Owner' ist KEIN
--    gültiger Status — wurde 2026-08-25 aus Prod entfernt).
SELECT investment_status, COUNT(*) AS n
FROM   stg_imp_prereg_20260828
GROUP  BY investment_status
ORDER  BY n DESC;

-- 3. Matches per ExternalID__pc (erwartet: 2).
SELECT COUNT(*) AS extid_matches
FROM   stg_imp_prereg_20260828 s
JOIN   crm_person_account_sfid_prod a
       ON a.ExternalID__pc = s.external_id;

-- 4. Matches per E-Mail: wie viele CSV-Zeilen treffen 0 / 1 / >1 Accounts?
--    (>1 → deterministische Auswahl: jüngstes LastModifiedDate; Vorschau in 5.)
SELECT match_cnt, COUNT(*) AS csv_rows
FROM (
    SELECT s.external_id,
           COUNT(a.Id) AS match_cnt
    FROM   stg_imp_prereg_20260828 s
    LEFT   JOIN crm_person_account_sfid_prod a
           ON LOWER(a.PersonEmail) = LOWER(s.email)
    WHERE  s.email IS NOT NULL AND s.email <> ''
    GROUP  BY s.external_id
) t
GROUP  BY match_cnt
ORDER  BY match_cnt;

-- 5. Vorschau der Mehrfach-Matches (eine E-Mail → mehrere Accounts) mit dem
--    Kandidaten, den die Auswahlregel nehmen würde. VOR dem Staging vom
--    Business absegnen lassen.
SELECT s.external_id, s.email,
       a.Id, a.FirstName, a.LastName, a.InvestCustomer__pc,
       a.SourceOrigin__pc, a.CreatedDate, a.LastModifiedDate
FROM   stg_imp_prereg_20260828 s
JOIN   crm_person_account_sfid_prod a
       ON LOWER(a.PersonEmail) = LOWER(s.email)
WHERE  s.email IN (
    SELECT email FROM (
        SELECT s2.email
        FROM   stg_imp_prereg_20260828 s2
        JOIN   crm_person_account_sfid_prod a2
               ON LOWER(a2.PersonEmail) = LOWER(s2.email)
        GROUP  BY s2.email
        HAVING COUNT(a2.Id) > 1
    ) x
)
ORDER  BY s.email, a.LastModifiedDate DESC;

-- 6. Zustand der Email-Matches: Invest-Flag und Quelle (Erwartung: fast
--    alles nicht-invest, Migration 2026).
SELECT a.InvestCustomer__pc, COUNT(DISTINCT a.Id) AS accounts
FROM   stg_imp_prereg_20260828 s
JOIN   crm_person_account_sfid_prod a
       ON LOWER(a.PersonEmail) = LOWER(s.email)
GROUP  BY a.InvestCustomer__pc;

-- 7. CPE-Abdeckung der gematchten Accounts (Join IMMER über PartyID__c =
--    PersonContactId, NIE nur über die E-Mail — eine E-Mail hat oft viele
--    CPEs von Leads/Individuals anderer Herkunft).
SELECT COUNT(DISTINCT a.Id)                                    AS matched_accounts,
       COUNT(DISTINCT CASE WHEN cpe.Id IS NOT NULL THEN a.Id END) AS with_cpe
FROM   stg_imp_prereg_20260828 s
JOIN   crm_person_account_sfid_prod a
       ON LOWER(a.PersonEmail) = LOWER(s.email)
LEFT   JOIN crm_cp_email_sfid_prod cpe
       ON  cpe.PartyID__c   = a.PersonContactId
       AND cpe.EmailAddress = a.PersonEmail;

-- 8. Doubletten-Probe: Accounts mit >1 CPE auf derselben Adresse (müssen vor
--    dem Consent-Load geklärt oder ausgeschlossen werden).
SELECT cpe.PartyID__c, cpe.EmailAddress, COUNT(*) AS cpe_cnt
FROM   crm_cp_email_sfid_prod cpe
JOIN   crm_person_account_sfid_prod a ON a.PersonContactId = cpe.PartyID__c
JOIN   stg_imp_prereg_20260828 s ON LOWER(s.email) = LOWER(a.PersonEmail)
GROUP  BY cpe.PartyID__c, cpe.EmailAddress
HAVING COUNT(*) > 1;

-- 9. Bestehende invest_central-Consents auf den gematchten Accounts
--    (Erwartung: nur die 2 bereits invest-geflaggten; der Loader skippt sie).
SELECT c.PrivacyConsentStatus, COUNT(*) AS n
FROM   stg_imp_prereg_20260828 s
JOIN   crm_person_account_sfid_prod a
       ON LOWER(a.PersonEmail) = LOWER(s.email)
JOIN   crm_cp_email_sfid_prod cpe
       ON  cpe.PartyID__c   = a.PersonContactId
       AND cpe.EmailAddress = a.PersonEmail
JOIN   crm_cp_consent_sfid_prod c
       ON  c.ContactPointId = cpe.Id
GROUP  BY c.PrivacyConsentStatus;
-- Hinweis: der Consent-Mirror kennt DataUsePurposeId nicht — die verbindliche
-- Purpose-Prüfung (invest_central = 0ZWTe0000000X5dOAE) läuft als Live-SOQL
-- im Loader (existing_consents) und in 05_run_prereg_consents.ipynb.

-- 10. Nicht-ASCII-E-Mails (lokaler Teil → INVALID_EMAIL_ADDRESS beim Insert;
--     Domain → Punycode-Falle beim CPE-Join). Kandidaten fürs Review-File.
SELECT external_id, email
FROM   stg_imp_prereg_20260828
WHERE  email IS NOT NULL AND email <> CONVERT(email USING ascii);
