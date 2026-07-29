/* prepare table */



select * from  stg_imp_investors_formated_2026070

ALTER TABLE stg_imp_investors_formated_2026070
ADD COLUMN investment_expiration_date_corrected DATE;

ALTER TABLE stg_imp_investors_formated_2026070
ADD COLUMN birth_date_corrected DATE;

UPDATE stg_imp_investors_formated_2026070
SET investment_expiration_date_corrected = STR_TO_DATE(investment_expiration_date, '%d.%m.%Y')
WHERE investment_expiration_date IS NOT NULL
  AND TRIM(investment_expiration_date) <> '';


ALTER TABLE stg_imp_investors_formated_2026070
ADD COLUMN data_issue BOOLEAN NOT NULL DEFAULT 0;

UPDATE stg_imp_investors_formated_2026070
SET birth_date_corrected = STR_TO_DATE(birth_date, '%d.%m.%Y')
WHERE birth_date IS NOT NULL
  AND TRIM(birth_date) <> ''
  AND birth_date <> '00.01.1900';



CREATE INDEX idx_investors_email
    ON stg_imp_investors_formated_2026070 (email);

CREATE INDEX idx_person_email
    ON crm_person_account_sfid_prod (PersonEmail);

CREATE INDEX idx_investors_external_id_email
    ON stg_imp_investors_formated_2026070 (external_id, email);

CREATE INDEX idx_person_external_id_email
    ON crm_person_account_sfid_prod (ExternalID__pc, PersonEmail);


-- 5973 invest file
-- 5205 sf

select * from stg_imp_investors_formated_2026070 where info <> 'bestand'
select count(*) from stg_imp_investors_formated_2026070 where info = 'neu'

select * from stg_imp_investors_formated_2026070





select email, count(*) 
from stg_imp_investors_formated_2026070 
group by email having count(*) > 1


select * from stg_imp_investors_formated_2026070

select count(*) 
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_2026070 i
	on i.external_id = c.ExternalID__pc
	and i.email = c.PersonEmail
where i.external_id is null
and c.ExternalID__pc is not null



/* check neue investoren */ 

-- 242 neue invstoren
-- 197 bereits vorhanden


select count(*) 
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_2026070 i
	on i.email = c.PersonEmail
where i.info = 'neu'
and c.ExternalID__pc is null


/* check for duplicates in import file */


select email, count(*) 
from stg_imp_investors_formated_2026070
group by email having count(*) > 1

select external_id, count(*) 
from stg_imp_investors_formated_2026070
group by external_id having count(*) > 1


/* check for alternative names with sf by email */

select i.first_name as inv_first_name, 
		c.FirstName as sf_first_name, 
		i.last_name as inv_last_name, 
		c.LastName as sf_last_name, 
		i.email, 
		c.ExternalID__pc,
		i.external_id,
		c.Id as account_id
		-- c.*, i.*
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_2026070 i
	on i.email = c.PersonEmail
where  i.first_name <>  c.FirstName 


/* check for duplicates by person in invest data */

select first_name, 
		last_name, 
		birth_date_corrected,  
		GROUP_CONCAT(
        DISTINCT email
        ORDER BY email
        SEPARATOR ' | '
    	) AS email,
		GROUP_CONCAT(
        DISTINCT external_id
        ORDER BY external_id
        SEPARATOR ' | '
    	) AS external_id,    	
    	count(*)
    	
from stg_imp_investors_formated_2026070
group by first_name, last_name, birth_date_corrected
having count(*)> 1


/* check duplicats by email with external id */

select i.first_name as inv_first_name, 
		c.FirstName as sf_first_name, 
		i.last_name as inv_last_name, 
		c.LastName as sf_last_name, 
		i.email, 
		c.ExternalID__pc, 
		i.external_id,  c.*, i.*
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_2026070 i
	on i.email = c.PersonEmail
where c.ExternalID__pc is not null
and c.ExternalID__pc <> i.external_id

/* check for duplicates in sf by email */

select i.external_id, i.email, count(*)
from stg_imp_investors_formated_2026070  i
inner join crm_person_account_sfid_prod c
	on i.email = c.PersonEmail 
group by  i.external_id, i.email having count(*) > 1


update stg_imp_investors_formated_2026070
set data_issue = 1
where data_issue = 0
and external_id in (
'466066ae-414c-404e-9ff8-27d65d5cbe5d' -- abweichender name

)
 

/* manual updates */

select * from stg_imp_investors_formated_2026070

update stg_imp_investors_formated_2026070
set email = 'oleg.danilov@falkensteiner.com'
where external_id = '38383e6c-334b-474d-a6ab-7d17c3e3bf35'

/* -------------- */



select * from crm_imp_person_accounts

/* select for update of investor status */

select i.first_name as inv_first_name, 
		c.FirstName as sf_first_name, 
		i.last_name as inv_last_name, 
		c.LastName as sf_last_name, 
		i.email, 
		c.ExternalID__pc,
		i.external_id,
		c.InvestmentStatus__pc,
		c.InvestmentExpirationDate__pc,
		c.Id as account_id,
		i.birth_date_corrected,
		c.PersonBirthdate
		-- c.*, i.*
		
-- select c.*
-- select *
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_2026070 i
	on i.email = c.PersonEmail
where  i.first_name =  c.FirstName 
and c.ExternalID__pc is null
and i.data_issue = 0







/*  insert into final import table - update contacts */

select * from crm_imp_person_accounts

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
    consent_invest,
    /*point */
    loyalty_points_balance
)
SELECT distinct
    'update'                                            AS _operation,
    '2026-07-08_update_invest_status_1'	                AS _batch_id,

    acc.Id                                              AS sf_account_id,
    acc.PersonContactId                                 AS sf_person_contact_id,
    null	                                            AS sf_loyalty_member_id,
    null                                                AS sf_cp_email_id,

    'conda'                                             AS source,
    'conda'                                             AS source_origin,

    inv.external_id                                     AS external_id,

    inv.first_name                                      AS first_name,
    inv.last_name                                       AS last_name,
    inv.birth_date_corrected							AS birth_date,
    inv.gender                                          AS gender,

    inv.email                                           AS email,

    1                                                   AS invest_customer,

    '0lpTe000000004rIAA'                                AS loyalty_program,
    inv.loyalty_legacy_tier                             AS loyalty_legacy_tier,
    inv.loyalty_legacy_number                           AS loyalty_legacy_number,

    inv.investment_status                               AS investment_status,     
    inv.investment_expiration_date_corrected			AS investment_expiration_date,

    0                                                   AS consent_central,
    1                                                   AS consent_invest,
    null												AS loyalty_points_balance
-- select inv.*
from crm_person_account_sfid_prod acc
inner join stg_imp_investors_formated_2026070 inv
	on inv.email = acc.PersonEmail

where  inv.first_name =  acc.FirstName 
and acc.ExternalID__pc is null
and inv.data_issue = 0
  

/* after completion archive */
   
CALL sp_archive_crm_imp_person_accounts(
    '2026-07-08_update_invest_status_1',
    'oleg.danilov')

    

 /* exclude from update if loyality already exists */   
    
    
update crm_imp_person_accounts i
inner join crm_loyality_sfid_prod l
	on i.sf_person_contact_id = l.ContactId
set _excluded = 1, _exclude_reason = 'has loyality'
where l.MembershipNumber is not null

select * 
from crm_loyality_sfid_prod clsp



select * from crm_imp_person_accounts



/* query for new investors */



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
    '2026-07-08_new_investor_import'	                AS _batch_id,

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
FROM stg_imp_investors_formated_2026070 inv
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



select count(*) 
from crm_imp_person_accounts_history
where _batch_id = '2026-07-08_new_investor_import'

select * 
from crm_imp_person_accounts




SELECT 
    inv.external_id,
    inv.salutation,
    inv.first_name,
    inv.middle_name,
    inv.last_name,
    inv.gender,
    inv.email,
    inv.preferred_language,
    inv.nationality_country_code,
    inv.birth_place,
    inv.address,
    inv.postal_code,
    inv.city,
    inv.country,
    inv.loyalty_legacy_tier,
    inv.investment_status,
    inv.investment_expiration_date,
    inv.info,
    inv.data_issue,

    acc.Id                          AS sf_account_id,
    acc.PersonContactId             AS sf_person_contact_id,
    acc.CreatedDate                 AS sf_created_date,
    acc.LastModifiedDate            AS sf_last_modified_date,
    acc.SourceSystem__pc            AS sf_source_system,
    acc.ExternalID__pc              AS sf_external_id,
    acc.EntraExternalID__pc         AS sf_entra_external_id,
    acc.Salutation                  AS sf_salutation,
    acc.FirstName                   AS sf_first_name,
    acc.MiddleName                  AS sf_middle_name,
    acc.LastName                    AS sf_last_name,
    acc.PersonGenderIdentity        AS sf_gender_identity,
    acc.PersonBirthdate             AS sf_birthdate,
    acc.BirthPlace__pc              AS sf_birth_place,
    acc.NationalityCountryCode__pc  AS sf_nationality_country_code,
    acc.PreferredLanguage__pc       AS sf_preferred_language,
    acc.PersonEmail                 AS sf_email,
    acc.PersonMobilePhone           AS sf_mobile_phone,
    acc.BillingStreet               AS sf_billing_street,
    acc.BillingCity                 AS sf_billing_city,
    acc.BillingPostalCode           AS sf_billing_postal_code,
    acc.BillingCountryCode__c       AS sf_billing_country_code,
    acc.HotelCustomer__pc           AS sf_hotel_customer,
    acc.CampingCustomer__pc         AS sf_camping_customer,
    acc.ResidencesCustomer__pc      AS sf_residences_customer,
    acc.InvestCustomer__pc          AS sf_invest_customer,
    acc.InvestmentStatus__pc        AS sf_investment_status,
    acc.InvestmentExpirationDate__pc AS sf_investment_expiration_date,

    loy.Id                          AS sf_loyalty_id,
    loy.MembershipNumber            AS sf_loyalty_membership_number,
    loy.MemberStatus                AS sf_loyalty_member_status,
    loy.LegacyMemberId__c           AS sf_loyalty_legacy_member_id,
    loy.LegacyTier__c               AS sf_loyalty_legacy_tier,
    loy.TierName__c                 AS sf_loyalty_tier_name,
    loy.EntraID__c                  AS sf_loyalty_entra_id,
    loy.CreatedDate                 AS sf_loyalty_created_date

FROM stg_imp_investors_formated_2026070 inv
LEFT JOIN crm_person_account_sfid_prod acc
    ON acc.ExternalID__pc = inv.external_id
LEFT JOIN crm_loyality_sfid_prod loy
    ON loy.ContactId = acc.PersonContactId;




select * 
from stg_imp_investors_formated inv
left join  crm_person_account_sfid_prod acc
	 ON acc.ExternalID__pc = inv.external_id
where acc.ExternalID__pc is null


