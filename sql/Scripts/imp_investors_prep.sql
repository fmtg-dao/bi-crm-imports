
/** **/


CREATE INDEX idx_inv_email
ON stg_imp_invest_20260519 (email);

CREATE INDEX idx_cpe_party
ON crm_cp_email_sfid_prod (PartyID__c);

CREATE INDEX idx_acc_person_contact_id
ON crm_person_account_sfid_prod (PersonContactId);

CREATE INDEX idx_loy_contact
ON crm_loyality_sfid_prod (ContactId);




/** existing accounts to be updated**/



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
    -- salutation,
    first_name,
    last_name,
    birth_date,
    gender,

    /* Profile: Communication */
    email,

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
    consent_central
)
SELECT distinct
    'update'                                            AS _operation,
    'conda_2026-05-28_invest_loyalty_upt'                AS _batch_id,

    acc.Id                                              AS sf_account_id,
    acc.PersonContactId                                 AS sf_person_contact_id,
    loy.Id                                              AS sf_loyalty_member_id,
    cpe.Id                                              AS sf_cp_email_id,

    'conda'                                             AS source,
    'conda'                                             AS source_origin,

    inv.conda_uid                                       AS external_id,

    inv.fname                                           AS first_name,
    inv.lname                                           AS last_name,
    STR_TO_DATE(
    NULLIF(TRIM(inv.date_of_birth), ''),
    '%d.%m.%Y'
	)          											AS birth_date,
    inv.gender                                          AS gender,

    inv.email                                           AS email,

    1                                                   AS invest_customer,

    '0lpTe000000004rIAA'                                AS loyalty_program,
    inv.spirit_short                                    AS loyalty_legacy_tier,
    inv.gms_loyalty_id                                  AS loyalty_legacy_number,

    inv.status                                          AS investment_status,     
    STR_TO_DATE(
    NULLIF(TRIM(inv.status_ablaufdatum), ''),
    '%d.%m.%Y'
	)          											AS investment_expiration_date,

    1                                                   AS consent_central
-- select *
FROM        stg_imp_invest_20260519             inv
INNER JOIN  crm_cp_email_sfid_prod              cpe ON  cpe.EmailAddress    = inv.email
INNER JOIN  crm_person_account_sfid_prod        acc ON  acc.PersonContactId = cpe.PartyID__c
                                                    AND acc.PersonEmail     = inv.email
INNER  JOIN  crm_loyality_sfid_prod              loy ON  loy.ContactId       = cpe.PartyID__c

WHERE   cpe.EmailAddress    IS NOT NULL
  AND   LEFT(cpe.PartyID__c, 3) = '003'
  AND   inv.fname             = acc.FirstName
  AND  inv.`conda_uid` not in ('1f1b59df-979a-4dc2-92bb-6d568f0e7572', 
'a69ac4fe-2593-4a65-8046-f773d6d8b608', 
'564a5c90-e958-4da7-9194-4c856277bd07',
'4649f898-2b80-44b0-958c-886466c9bef7');



update stg_imp_invest_20260519 set date_of_birth = '01.01.1900' where date_of_birth is null 

select * from stg_imp_invest_20260519  where date_of_birth   is null

select * from stg_imp_invest_20260519 order by date_of_birth 


SELECT
    inv.date_of_birth,
    SUBSTRING_INDEX(inv.date_of_birth, '.', 1) AS day_part,
    SUBSTRING_INDEX(SUBSTRING_INDEX(inv.date_of_birth, '.', 2), '.', -1) AS month_part,
    SUBSTRING_INDEX(inv.date_of_birth, '.', -1) AS year_part,
    LENGTH(inv.date_of_birth) AS len,
    HEX(inv.date_of_birth) AS hex_value,
    COUNT(*) AS cnt
FROM stg_imp_invest_20260519 inv
GROUP BY
    inv.date_of_birth,
    day_part,
    month_part,
    year_part,
    len,
    hex_value
ORDER BY cnt DESC;



SELECT *
FROM stg_imp_invest_20260519 inv
WHERE inv.`conda_uid` in ('1f1b59df-979a-4dc2-92bb-6d568f0e7572', 
'a69ac4fe-2593-4a65-8046-f773d6d8b608', 
'564a5c90-e958-4da7-9194-4c856277bd07',
'4649f898-2b80-44b0-958c-886466c9bef7')


select * from crm_imp_person_accounts
'Ambassador'
select * from crm

Ambassador 
update crm_imp_person_accounts set investment_status = TRIM(REPLACE(investment_status, CONVERT(0xC2A0 USING utf8mb4), ''))

inv.status_ablaufdatum
select * from 
'07.03.1995'
'inv.status_ablaufdatum'

CALL sp_archive_crm_import_contacts(
    'protel_2026-05-28_initial',
    'import_persons_bulk.py'
    
    
CALL sp_archive_crm_imp_person_accounts(
    'conda_2026-05-28_invest_enrichment',
    'oleg.danilov')


select * from crm_imp_person_accounts

