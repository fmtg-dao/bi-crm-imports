

/*check for dublicates */


select * from stg_imp_invest_new_investors_20260519

-- conda id
select conda_uid, count(*)
from stg_imp_invest_new_investors_20260519
group by conda_uid having count(*) > 1

-- email
select email, count(*)
from stg_imp_invest_new_investors_20260519
group by email having count(*) > 1


/* cleaning */ 


UPDATE stg_imp_invest_new_investors_20260519
SET
  conda_uid             = TRIM(conda_uid),
  fname                 = TRIM(fname),
  lname                 = TRIM(lname),
  email                 = TRIM(email),
  country               = TRIM(country),
  salutations_full      = TRIM(salutations_full),
  account_type          = TRIM(account_type),
  sub_platform          = TRIM(sub_platform),
  gender                = TRIM(gender),
  current_age           = TRIM(current_age),
  incert_id             = TRIM(incert_id),
  incert_email          = TRIM(incert_email),
  date_of_birth         = TRIM(date_of_birth),
  natural_legal_city    = TRIM(natural_legal_city),
  natural_legal_address = TRIM(natural_legal_address),
  natural_legal_zip     = TRIM(natural_legal_zip),
  investment_type       = TRIM(investment_type),
  gms_loyalty_id        = TRIM(gms_loyalty_id),
  status                = TRIM(status),
  status_ablaufdatum    = TRIM(status_ablaufdatum),
  punkte_per_2705       = TRIM(punkte_per_2705);


UPDATE stg_imp_invest_new_investors_20260519
SET gms_loyalty_id = TRIM(REPLACE(REPLACE(REPLACE(REPLACE(gms_loyalty_id,
              '\t',''), '\r',''), '\n',''), _utf8mb3 0xC2A0, ''));




UPDATE stg_imp_invest_new_investors_20260519
SET country = UPPER(country);


UPDATE stg_imp_invest_new_investors_20260519
SET gender = CASE LOWER(TRIM(gender))
               WHEN 'female' THEN 'Female'
               WHEN 'male'   THEN 'Male'
               ELSE gender   -- alles andere unangetastet lassen
             END;



-- 1. Saubere DATE-Spalten ergänzen
ALTER TABLE stg_imp_invest_new_investors_20260519
  ADD COLUMN date_of_birth_d      DATE NULL AFTER date_of_birth,
  ADD COLUMN status_ablaufdatum_d DATE NULL AFTER status_ablaufdatum;

-- 2. Parsen und befüllen
UPDATE stg_imp_invest_new_investors_20260519
SET
  date_of_birth_d = STR_TO_DATE(
                      NULLIF(TRIM(date_of_birth), ''),
                      '%d.%m.%Y'
                    ),
  status_ablaufdatum_d = STR_TO_DATE(
                           NULLIF(TRIM(status_ablaufdatum), ''),
                           '%d.%m.%Y'
                         );



SELECT date_of_birth, status_ablaufdatum
FROM stg_imp_invest_new_investors_20260519
WHERE (NULLIF(TRIM(date_of_birth), '')      IS NOT NULL AND date_of_birth_d      IS NULL)
   OR (NULLIF(TRIM(status_ablaufdatum), '') IS NOT NULL AND status_ablaufdatum_d IS NULL);




-- 1. Feld anlegen
ALTER TABLE stg_imp_invest_new_investors_20260519
  ADD COLUMN preferred_language VARCHAR(10) NULL AFTER country;

-- 2. Aus dem (bereits ge-UPPER-ten) country ableiten
UPDATE stg_imp_invest_new_investors_20260519
SET preferred_language = CASE UPPER(TRIM(country))
                           WHEN 'AT' THEN 'de'
                           WHEN 'DE' THEN 'de'
                           WHEN 'SK' THEN 'sk'
                           ELSE NULL   -- alles Unerwartete bleibt leer & fällt auf
                         END;




-- 1. Normalisierte Spalte anlegen
ALTER TABLE stg_imp_invest_new_investors_20260519
  ADD COLUMN salutation_norm VARCHAR(10) NULL AFTER salutations_full;

-- 2. Aus dem Volltext ableiten
UPDATE stg_imp_invest_new_investors_20260519
SET salutation_norm = CASE
        WHEN salutations_full LIKE '%Frau%' THEN 'Ms.'
        WHEN salutations_full LIKE '%Herr%' THEN 'Mr.'
        ELSE NULL   -- alles ohne klare Anrede bleibt leer & fällt auf
      END;

select * from stg_imp_invest_new_investors_20260519


/**  check existing aaccounts **/


select *
FROM        stg_imp_invest_new_investors_20260519             inv
INNER JOIN  crm_cp_email_sfid_prod              cpe ON  cpe.EmailAddress    = inv.email
INNER JOIN  crm_person_account_sfid_prod        acc ON  acc.PersonContactId = cpe.PartyID__c
                                                    AND acc.PersonEmail     = inv.email
LEFT  JOIN  crm_loyality_sfid_prod              loy ON  loy.ContactId       = cpe.PartyID__c

WHERE   cpe.EmailAddress    IS NOT NULL
  AND   LEFT(cpe.PartyID__c, 3) = '003'
  AND   inv.fname             = acc.FirstName
  AND	loy.Id is null




/** insert statement for create contacts **/

INSERT INTO crm_imp_person_accounts (
    _operation, _batch_id,
    source, source_origin,
    external_id,
    salutation, first_name, last_name, birth_date, gender,
    email, preferred_language,
    address, postal_code, city, country,
    invest_customer,
    loyalty_legacy_number, loyalty_legacy_tier, loyalty_points_balance,
    investment_status, investment_expiration_date,
    consent_central
)
SELECT
    'insert'                                 AS _operation,
    'conda_2026-05-19_new_investors_b1_new'  AS _batch_id,

    'conda'                                  AS source,
    inv.sub_platform                         AS source_origin,

    inv.conda_uid                            AS external_id,

    inv.salutation_norm                      AS salutation,
    inv.fname                                AS first_name,
    inv.lname                                AS last_name,
    inv.date_of_birth_d                      AS birth_date,
    inv.gender                               AS gender,

    inv.email                                AS email,
    inv.preferred_language                   AS preferred_language,

    inv.natural_legal_address                AS address,
    inv.natural_legal_zip                    AS postal_code,
    inv.natural_legal_city                   AS city,
    inv.country                              AS country,

    1                                        AS invest_customer,

    inv.gms_loyalty_id                       AS loyalty_legacy_number,
    inv.status                               AS loyalty_legacy_tier,
    inv.punkte_per_2705                      AS loyalty_points_balance,

    inv.status			                     AS investment_status,
    inv.status_ablaufdatum_d                 AS investment_expiration_date,

    1                                        AS consent_central

FROM stg_imp_invest_new_investors_20260519 inv
WHERE NOT EXISTS (
        SELECT 1
        FROM        crm_cp_email_sfid_prod        cpe
        INNER JOIN  crm_person_account_sfid_prod  acc
                ON  acc.PersonContactId = cpe.PartyID__c
        WHERE   cpe.EmailAddress    = inv.email
          AND   LEFT(cpe.PartyID__c, 3) = '003'
      );




select * 
from crm_imp_person_accounts   
where _batch_id = 'conda_2026-05-19_new_investors_b1_new'


update crm_imp_person_accounts
set loyalty_enrollment_date = '2026-05-19'
where _batch_id = 'conda_2026-05-19_new_investors_b1_new'


/*** cpe befüllen ***/


SELECT cpe.PartyID__c, COUNT(*) AS cpe_count
FROM   crm_cp_email_sfid_prod cpe
WHERE  cpe.PartyID__c IN (
         SELECT sf_person_contact_id
         FROM   crm_imp_person_accounts
         WHERE  _batch_id = 'conda_2026-05-19_new_investors_b1_new'
           AND  sf_person_contact_id IS NOT NULL
       )
GROUP BY cpe.PartyID__c
HAVING COUNT(*) > 1;





UPDATE      crm_imp_person_accounts pa
INNER JOIN  crm_cp_email_sfid_prod  cpe
        ON  cpe.PartyID__c   = pa.sf_person_contact_id
        AND cpe.EmailAddress = pa.email
SET    pa.sf_cp_email_id = cpe.Id
WHERE  pa._batch_id            = 'conda_2026-05-19_new_investors_b1_new'
  AND  pa.sf_person_contact_id IS NOT NULL
  AND  pa.sf_cp_email_id       IS NULL;


/* batch archivieren */

CALL sp_archive_crm_imp_person_accounts(
    'conda_2026-05-19_new_investors_b1_new',
    'oleg.danilov')

CALL sp_archive_crm_imp_person_accounts(
    'conda_2026-05-28_invest_loyalty_import',
    'oleg.danilov')
    
   
CALL sp_archive_crm_imp_person_accounts(
    'conda_2026-05-28_invest_loyalty_upt',
    'oleg.danilov')
    
    
/* batches TODO:
    
    conda_2026-05-28_invest_loyalty_import
conda_2026-05-28_invest_loyalty_upt
   */ 
    
    
select count(*) from stg_imp_invest_all_investors_points
    
 select *  from crm_imp_person_accounts where _batch_id = 'conda_2026-05-28_invest_loyalty_import' and sf_loyalty_member_id is null

 
  select count(*)  from crm_imp_person_accounts where _batch_id = 'conda_2026-05-28_invest_loyalty_upt' and sf_loyalty_member_id is null
  
   select *  from crm_imp_person_accounts
   
   
   
   
   select * 
   from crm_cp_email_sfid_prod where EmailAddress 
   	in ('beate_schader@gmx.at', 'christoph.scharinger@frieden.at'
   	, 'chung_my_hanh@hotmail.com', 'jhasv23@gmail.com', 'julia.kriegl@hotmail.com', 'leonmakovka@gmail.com')
   