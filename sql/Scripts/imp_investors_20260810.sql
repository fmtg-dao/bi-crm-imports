/*
   imp_investors_20260810 - W9 Nachzuegler (3 neue Investoren)

   Quelle:  local_data/csv/20260722_16new_Investors_W_9.csv (semikolon-getrennt)
   Staging: stg_imp_investors_w9_20260722 (eigene Tabelle, bestehende
            stg_imp_investors_formated_20260722 bleibt unangetastet)

   Methode wie imp_investors_20260728.sql. Statements einzeln in DBeaver ausfuehren.

   4 Zeilen im File: 3 Investoren + 1 Leerzeile (nur Semikolons) -> data_issue = 1.

   ACHTUNG - Franz Strebinger (4e2b216b-710e-4be3-9277-9e8e8e4d146f):
   existiert bereits in SF (001Te00000aitIjIAI, email + external_id match,
   loyalty member 0lMTe0000000kG1MAI). InvestCustomer__pc steht aber auf False
   und InvestmentExpirationDate__pc = 2028-05-30 vs. 2032-05-30 im File.
   Account wurde am 2026-07-16 ausserhalb der Pipeline gepflegt (nicht in history).
   Er faellt aus beiden Batches raus (insert: NOT EXISTS; update: ExternalID__pc
   ist nicht null) -> MANUELLE PRUEFUNG, wird nicht ueber diese Batches importiert.
*/


/* prepare table */

select * from stg_imp_investors_w9_20260722

-- bereits per Python beim Laden ausgefuehrt (2026-08-10), bei Neuladen der
-- Staging-Tabelle wiederholen:

ALTER TABLE stg_imp_investors_w9_20260722
ADD COLUMN birth_date_corrected DATE;

ALTER TABLE stg_imp_investors_w9_20260722
ADD COLUMN investment_expiration_date_corrected DATE;

ALTER TABLE stg_imp_investors_w9_20260722
ADD COLUMN data_issue BOOLEAN NOT NULL DEFAULT 0;

UPDATE stg_imp_investors_w9_20260722
SET birth_date_corrected = STR_TO_DATE(birth_date, '%d.%m.%Y')
WHERE birth_date IS NOT NULL
  AND TRIM(birth_date) <> ''
  AND birth_date <> '00.01.1900';

UPDATE stg_imp_investors_w9_20260722
SET investment_expiration_date_corrected = STR_TO_DATE(investment_expiration_date, '%d.%m.%Y')
WHERE investment_expiration_date IS NOT NULL
  AND TRIM(investment_expiration_date) <> '';

-- 'Diamond ' mit trailing space im File
UPDATE stg_imp_investors_w9_20260722
SET investment_status = TRIM(REPLACE(investment_status, CONVERT(0xC2A0 USING utf8mb4), ' '))

CREATE INDEX idx_investors_w9_email
    ON stg_imp_investors_w9_20260722 (email);

CREATE INDEX idx_investors_w9_external_id
    ON stg_imp_investors_w9_20260722 (external_id);


/* data issues */

-- Leerzeile aus dem CSV (Zeile 5, nur Semikolons)
UPDATE stg_imp_investors_w9_20260722
SET data_issue = 1
WHERE external_id = ''

-- 4 zeilen: 3 investoren + 1 leerzeile
select external_id, first_name, last_name, email, birth_date_corrected,
       investment_status, investment_expiration_date_corrected, info, data_issue
from stg_imp_investors_w9_20260722


/* check for duplicates in import file */

-- 0
select email, count(*)
from stg_imp_investors_w9_20260722
where data_issue = 0
group by email having count(*) > 1

-- 0
select external_id, count(*)
from stg_imp_investors_w9_20260722
where data_issue = 0
group by external_id having count(*) > 1


/* check bereits vorhanden in SF */

-- 1: Franz Strebinger, email + external_id match, InvestCustomer__pc = False
--    -> manuelle pruefung, siehe header
select i.first_name as inv_first_name,
		c.FirstName as sf_first_name,
		i.last_name as inv_last_name,
		c.LastName as sf_last_name,
		i.email,
		c.ExternalID__pc,
		i.external_id,
		c.SourceSystem__pc,
		c.InvestCustomer__pc,
		c.InvestmentStatus__pc,
		c.InvestmentExpirationDate__pc,
		c.Id as account_id
from crm_person_account_sfid_prod c
inner join stg_imp_investors_w9_20260722 i
	on i.email = c.PersonEmail
	or i.external_id = c.ExternalID__pc
where i.data_issue = 0


/* check loyality legacy */

-- 1: Franz Strebinger (0lMTe0000000kG1MAI, Diamond Spirit Club, 368289610)
select i.email, loy.Id, loy.LegacyMemberId__c, loy.LegacyTier__c, loy.MembershipNumber
from stg_imp_investors_w9_20260722 i
inner join crm_loyality_sfid_prod loy
	on loy.LegacyMemberId__c = i.email
where i.data_issue = 0


/* update-kandidaten (email match, name gleich, ohne external id) */

-- 0: Franz hat ExternalID__pc bereits gesetzt -> kein update-kandidat,
--    Regina Lind / Juliane Lange sind nicht in SF -> kein update batch noetig
select i.first_name, c.FirstName, i.last_name, c.LastName, i.email, c.Id
from crm_person_account_sfid_prod c
inner join stg_imp_investors_w9_20260722 i
	on i.email = c.PersonEmail
where i.first_name = c.FirstName
and c.ExternalID__pc is null
and i.data_issue = 0


/* query for new investors */

-- erwartet: 2 (Regina Lind, Juliane Lange)

INSERT INTO crm_imp_person_accounts (
    /* Pipeline Control */
    _operation,
    _batch_id,

    /* SF IDs (für UPDATE-by-SFID Operation) */
    sf_account_id,
    sf_person_contact_id,
    sf_loyalty_member_id,
    sf_cp_email_id,

    /* Source Tracking */
    source,
    source_origin,

    /* External IDs */
    external_id,

    /* Profile: Identity */
    salutation,
    first_name,
    last_name,
    birth_date,
    gender,

    /* Profile: Communication */
    email,
    preferred_language,

    /* Address */
    address,
    postal_code,
    city,
    country,

    /* Business Unit Flags */
    invest_customer,

    /* Loyalty */
    loyalty_program,
    loyalty_legacy_tier,
    loyalty_legacy_number,

    /* Investment */
    investment_status,
    investment_expiration_date,

    /* Consent */
    consent_central,
    consent_invest,
    /*point */
    loyalty_points_balance
)
SELECT distinct
    'insert'                                            AS _operation,
    '2026-08-10_new_investor_import'	                AS _batch_id,

    null                                                AS sf_account_id,
    null				                                AS sf_person_contact_id,
    null	                                            AS sf_loyalty_member_id,
    null                                                AS sf_cp_email_id,

    inv.source_origin                                   AS source,
    inv.source_origin                                   AS source_origin,

    inv.external_id                                     AS external_id,
	inv.salutation										AS salutation,
    inv.first_name                                      AS first_name,
    inv.last_name                                       AS last_name,
    inv.birth_date_corrected							AS birth_date,
    inv.gender                                          AS gender,

    inv.email                                           AS email,
	inv.preferred_language								AS preferred_language,

	inv.address											AS address,
	inv.postal_code										AS postal_code,
	inv.city											AS city,
	inv.country											AS country,

    inv.invest_customer                                 AS invest_customer,

    '0lpTe000000004rIAA'                                AS loyalty_program,
    inv.loyalty_legacy_tier                             AS loyalty_legacy_tier,
    inv.loyalty_legacy_number                           AS loyalty_legacy_number,

    inv.investment_status                               AS investment_status,
    inv.investment_expiration_date_corrected			AS investment_expiration_date,

    0                                                   AS consent_central,
    1                                                   AS consent_invest,
    null												AS loyalty_points_balance
-- select inv.*
FROM stg_imp_investors_w9_20260722 inv
WHERE inv.data_issue = 0
  AND NOT EXISTS (
      SELECT 1
      FROM crm_person_account_sfid_prod acc
      WHERE acc.PersonEmail = inv.email
  )
  AND NOT EXISTS (
      SELECT 1
      FROM crm_person_account_sfid_prod acc
      WHERE acc.ExternalID__pc = inv.external_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM crm_loyality_sfid_prod loy
      WHERE loy.LegacyMemberId__c = inv.email
  );


-- kontrolle: 2 zeilen im batch
select * from crm_imp_person_accounts
where _batch_id = '2026-08-10_new_investor_import'


/* danach: python scripts der reihe nach, jeweils mit batch id als argument

   1. insert_person_accounts_bulk.py 2026-08-10_new_investor_import
   2. insert_loyalty_members_bulk.py 2026-08-10_new_investor_import
   3. (punkte entfallen - loyalty_points_balance ist leer)
   4. CPE-mirror refreshen, sf_cp_email_id backfillen,
      insert_consents_bulk.py Konstanten auf invest purpose umstellen, dann laufen lassen
*/


/* STATUS 2026-08-10 (arsal.jabbar) - schritte 1+2 gelaufen, consent GEPARKT

   Regina Lind:   account 001Te00000gEy2eIAC / contact 003Te00000yCpacIAC /
                  loyalty 0lMTe000000U92fMAC (368297282) / cpe 9VlTe000006aDEcKAM
   Juliane Lange: account 001Te00000gEy2fIAC / contact 003Te00000yCpadIAC /
                  loyalty 0lMTe000000U92gMAC (368297283) / cpe 9VlTe000006aDEdKAM

   sf_cp_email_id backfill: CPE haengt am Individual (0PK...), NICHT am Contact ->
   join ueber EmailAddress, nicht ParentId.

   Consent wartet auf purpose-entscheidung (frage an Oleg):
   - invest_central existiert seit 2026-04-14 (0ZWTe0000000X5dOAE), 0 consents
   - mai-batch (89 investoren) wurde als marketing_central geschrieben
   - residences nutzt dagegen das eigene residences_central
   - batches seit juni haben NIE consents bekommen (184 investoren offen)
   danach: CONSENT_FLAG_COLUMN = 'consent_invest' setzen, purpose id je nach
   entscheidung, dann insert_consents_bulk.py 2026-08-10_new_investor_import

   Franz Strebinger (001Te00000aitIjIAI): manuelle pruefung durch arsal.jabbar,
   InvestCustomer__pc fehlt, expiration 2028 vs 2032 im file.
*/


/* after completion archive */

-- CALL sp_archive_crm_imp_person_accounts(
--     '2026-08-10_new_investor_import',
--     'arsal.jabbar')
