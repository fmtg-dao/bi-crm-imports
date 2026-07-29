/* prepare table */

select * from  stg_imp_investors_formated_20260722



ALTER TABLE stg_imp_investors_formated_20260722
ADD COLUMN investment_expiration_date_corrected DATE;

UPDATE stg_imp_investors_formated_20260722
SET investment_expiration_date_corrected = STR_TO_DATE(investment_expiration_date, '%d.%m.%Y')
WHERE investment_expiration_date IS NOT NULL
  AND TRIM(investment_expiration_date) <> '';


ALTER TABLE stg_imp_investors_formated_20260722
ADD COLUMN data_issue BOOLEAN NOT NULL DEFAULT 0;

UPDATE stg_imp_investors_formated_20260722
SET birth_date_corrected = STR_TO_DATE(birth_date, '%d.%m.%Y')
WHERE birth_date IS NOT NULL
  AND TRIM(birth_date) <> ''
  AND birth_date <> '00.01.1900';



CREATE INDEX idx_investors_email
    ON stg_imp_investors_formated_20260722_2026070 (email);

CREATE INDEX idx_person_email
    ON crm_person_account_sfid_prod (PersonEmail);

CREATE INDEX idx_investors_external_id_email
    ON stg_imp_investors_formated_20260722_2026070 (external_id, email);

CREATE INDEX idx_person_external_id_email
    ON crm_person_account_sfid_prod (ExternalID__pc, PersonEmail);


-- 5973 invest file
-- 5205 sf

select * from stg_imp_investors_formated_20260722_2026070 where info <> 'bestand'
select count(*) from stg_imp_investors_formated_20260722_2026070 where info = 'neu'

select * from stg_imp_investors_formated_20260722_2026070 





select email, count(*) 
from stg_imp_investors_formated_20260722_2026070 
group by email having count(*) > 1


select * from stg_imp_investors_formated_20260722_2026070

select count(*) 
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_20260722_2026070 i
	on i.external_id = c.ExternalID__pc
	and i.email = c.PersonEmail
where i.external_id is null
and c.ExternalID__pc is not null



/* check neue investoren */ 

-- 242 neue invstoren
-- 197 bereits vorhanden


select count(*) 
from crm_person_account_sfid_prod c
inner join stg_imp_investors_formated_20260722_2026070 i
	on i.email = c.PersonEmail
where i.info = 'neu'
and c.ExternalID__pc is null


/* check for duplicates in import file */


select email, count(*) 
from stg_imp_investors_formated_20260722_2026070 
group by email having count(*) > 1

select external_id, count(*) 
from stg_imp_investors_formated_20260722_2026070 
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
inner join stg_imp_investors_formated_20260722_2026070 i
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
    	
from stg_imp_investors_formated_20260722_2026070 
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
inner join stg_imp_investors_formated_20260722_2026070 i
	on i.email = c.PersonEmail
where c.ExternalID__pc is not null
and c.ExternalID__pc <> i.external_id

/* check for duplicates in sf by email */

select i.external_id, i.email, count(*)
from stg_imp_investors_formated_20260722_2026070  i
inner join crm_person_account_sfid_prod c
	on i.email = c.PersonEmail 
group by  i.external_id, i.email having count(*) > 1


update stg_imp_investors_formated_20260722_2026070
set data_issue = 1
where data_issue = 0
and external_id in (
'a9eebaac-3e4e-438c-bf1a-9fcc5c267618',
'3913ea8d-18e6-4603-8a46-50cea917404c',
'4d57ca7e-8f9d-4398-9084-4323d7a5362f',
'59c06d09-43bd-4227-a8a3-0bec6e274dff',
'56a85003-4ea3-4033-aded-a77fc00ebb9f',
'64d1fd3a-1bcd-4fe3-adb6-798b3b110076',
'a2da4ac3-6fd0-4886-91fd-330b07d34a09',
'253f2408-719a-4c3e-b6bd-259c629b33c1',
'ebd81fd4-07e9-40bf-ba11-8363b53a53d6',
'ca1cd07d-21ac-49c2-9eb7-ebceb57a4cb9',
'acd27ecc-1c85-4f99-8d20-8012f4279f51',
'28e63983-94ef-44dd-a5cb-0af71f890558',
'd4473577-dbc8-4794-b833-49ed4e8b9eae',
'e517e8d4-d7c4-4d34-920b-f61ec381dce9',
'b08d8ebb-daef-4a5b-b34a-923ac218d69e',
'a2ba9689-7d2d-422d-9763-27486e89ab35',
'05db123e-b2fa-4544-acd5-0e65f80dd3bd',
'ca845018-cbbb-4faf-9382-3852d7993bd0',
'3326366c-8b55-49fd-8f00-e375c287840d',
'7bbb6814-6767-4d31-9e26-a63070856395',
/* owner which would be overwritten */
'1e1d5fb0-0d13-4a09-815d-e3211ddbe402',
'e6c139f5-096a-443c-a4e5-32e7f364a57d',
'912417bc-1c50-4478-aef7-c1539e8436ef',
'101d0156-847c-417d-8e2c-f0356a68438b',
/* external id doppelt */
'99f70e5f-f654-475a-94d9-317b083e25a2',
/* doppelte personen in invest file */
'40b9cb94-485d-41a1-832b-d5a7294de3c0',
'59d41bdf-6ab0-4669-9786-7be1777cb2bc',
'5a305250-36fa-45be-8cda-26edf3578745',
'9238d6b2-6bb6-4bec-8370-2c6598b3765d',
'9fffe000-2b2a-4229-82e9-8c7eeaa93801',
'16091777-1179-4cad-8302-4c232f75af08',
'7165205e-9b9c-4a6f-a327-b84d69958cf9',
'22954982-6072-42a4-9dbb-aaf321ee930c',
'4393e21e-26c0-48fe-aa61-7263a0a5e1d5',
'355a12b9-7ae5-49e2-a2e9-6138a7ba75b7',
'74ea8d55-96b4-42df-9684-281d794c65a0',
'a6559315-8322-4bc5-9f0a-1ff895230721',
'b47829c0-5dcb-40ff-93a8-791fa0bbd784',
'd4dab24a-87ea-4fb4-8f72-ece6da30943c',
'ec56c3c4-8c45-4f66-8daa-9b0a095790e7',
'52b033d8-1154-4005-8f4a-e52f0db533ab',
'c5ccfb0a-cbae-4f10-b754-1c410dd9e03d',
/* exist already in loyality legacy - cannot create */
'a593e2ec-9732-4d8f-809d-062e794ae34c',
'3e3653f4-351e-4dc4-bb30-c06410bacbe4',
'98bc3d8a-af66-43c7-9912-c85abc4a337a',
'e8a9f370-93c8-44ad-a768-5f8d08c4c22d',
'c7322c5f-29f8-48a2-b03d-bded9a33078a',
'7dd8b0ad-4c5d-4b45-9e60-b58791e3faa0',
'a3e98e5d-526d-49dd-a123-3bfd8e361c9f',
'5d62b8b2-5412-4638-b40a-aa0ad27c4209',
'89b1a33b-177f-4b9e-8dcb-2a1b6cf90f20',
'f3da0e2f-a12a-4432-82b4-5177ee7e0c9c',
'f81ea91f-8840-421f-8d96-90f1986fd836',
'27fdf557-8974-477b-afcc-910643ce6e30',
'4f428e8b-b857-4ad0-ab76-e4b1c6ce4ddc',
'bc675fd1-d7b6-47f3-8ab0-77f7f1a053d8',
'5dc246c0-b73a-4bc3-b36e-2a2f469e2057',
'e11239c4-061f-45d1-b927-e953fa6c3605',
'2ba82069-0479-4cdd-aa77-405b11315411',
'4ea9b803-cf78-4954-b99e-94cb0a246fc5',
'49dbd09b-6bc0-4934-b14b-3236e6e88992',
'58098269-bfe5-4aff-a462-fbaff0094fd5',
'113a556a-661b-46a6-908f-9d885978c980',
'd99d1745-24c9-40f8-a425-59e7fc7f24a9'

)
 

/* manual updates */

update stg_imp_investors_formated_20260722_2026070
set first_name = 'Herwig'
where external_id = 'f1e0467d-973d-470b-af3b-15c926f392c7'

update stg_imp_investors_formated_20260722_2026070
set first_name = 'Wolfgang'
where external_id = '5426a10c-306d-4b86-85d4-1f33bc96bece'

update stg_imp_investors_formated_20260722_2026070
set investment_status = TRIM(REPLACE(investment_status, CONVERT(0xC2A0 USING utf8mb4), ''))

update crm_imp_person_accounts
set investment_status = TRIM(REPLACE(investment_status, CONVERT(0xC2A0 USING utf8mb4), ''))

update  stg_imp_investors_formated_20260722_2026070
set data_issue = 1
where external_id = '99f70e5f-f654-475a-94d9-317b083e25a2'



select * from crm_person_account_sfid_prod 
where  ExternalID__pc = '99f70e5f-f654-475a-94d9-317b083e25a2'

DELETE from crm_imp_person_accounts
where external_id = '99f70e5f-f654-475a-94d9-317b083e25a2'

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
inner join stg_imp_investors_formated_20260722_2026070 i
	on i.email = c.PersonEmail
where  i.first_name =  c.FirstName 
and c.ExternalID__pc is null
and i.data_issue = 0



update stg_imp_investors_formated_20260722_2026070
set birth_date_corrected =  '1952-12-01' 
where external_id = 'b4e01c03-0fc1-42a6-927f-679816bb57ca'



/*  insert into final import table */

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
    '2026-06-22_update_invest_status'	                AS _batch_id,

    acc.Id                                              AS sf_account_id,
    acc.PersonContactId                                 AS sf_person_contact_id,
    null	                                            AS sf_loyalty_member_id,
    null                                                AS sf_cp_email_id,

    'conda'                                             AS source,
    'conda'                                             AS source_origin,

    inv.external_id                                     AS external_id,

    inv.first_name                                       AS first_name,
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
inner join stg_imp_investors_formated_20260722_2026070 inv
	on inv.email = acc.PersonEmail
where  inv.first_name =  acc.FirstName 
and acc.ExternalID__pc is null
and inv.data_issue = 0
  

/* after completion archive */
   
CALL sp_archive_crm_imp_person_accounts(
    '2026-06-22_update_invest_status',
    'oleg.danilov')



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
    '2026-06-22_new_investor_import'	                AS _batch_id,

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
FROM stg_imp_investors_formated_20260722_2026070 inv
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
where _batch_id = '2026-06-22_update_invest_status'



select * from crm_person_account_sfid_prod where InvestCustomer__pc <> false

select * from crm_person_account_sfid_prod where InvestCustomer__pc = True

select * from crm_person_account_sfid_prod where ExternalID__pc = 'e219972f-8b24-4dc3-9fe4-46d4deff4c84'

select * from crm_person_account_sfid_prod where PersonContactId = '003Te00000v0j6GIAQ'


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

FROM stg_imp_investors_formated_20260722_2026070 inv
LEFT JOIN crm_person_account_sfid_prod acc
    ON acc.ExternalID__pc = inv.external_id
LEFT JOIN crm_loyality_sfid_prod loy
    ON loy.ContactId = acc.PersonContactId;

	


