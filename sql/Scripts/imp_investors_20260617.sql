

-- 5753

-- 5768

select count(*)
from stg_imp_invest_20260519 i
left join crm_person_account_sfid_prod c
	on i.email = c.PersonEmail 
group by i.email 



select i.email, count(*)
from stg_imp_invest_20260519 i
left join crm_person_account_sfid_prod c
	on i.email = c.PersonEmail 
group by i.email having count(*) > 1


select InvestmentStatus__pc, c.* 
from crm_person_account_sfid_prod c
where c.PersonEmail = 'peter.knueppel@gmail.com'


select c.PersonEmail, count(*)
from crm_person_account_sfid_prod c
where c.PersonEmail in (select distinct i.email
		from stg_imp_invest_20260519 i )
group by c.PersonEmail having count(*) > 1


select email
from stg_imp_invest_20260519 i 
group by email having count(*) > 1

select *
from stg_imp_invest_20260519 i
where i.email = 'peter.knueppel@gmail.com'




select *
from crm_person_account_sfid_prod c
where c.PersonEmail in (select distinct i.email
		from stg_imp_invest_20260519 i )
and InvestmentStatus__pc is null




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
    consent_central,
    /*point */
    loyalty_points_balance
)
SELECT distinct
    'update'                                            AS _operation,
    '2026-06-17_update_investors'		                AS _batch_id,

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

    1                                                   AS consent_central,
    inv.points											AS loyalty_points_balance
-- select *
FROM        stg_imp_invest_20260519             inv
INNER JOIN  crm_cp_email_sfid_prod              cpe ON  cpe.EmailAddress    = inv.email
INNER JOIN  crm_person_account_sfid_prod        acc ON  acc.PersonContactId = cpe.PartyID__c
                                                    AND acc.PersonEmail     = inv.email
INNER  JOIN  crm_loyality_sfid_prod              loy ON  loy.ContactId       = cpe.PartyID__c

WHERE   cpe.EmailAddress    IS NOT NULL
  

