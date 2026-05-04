

select * from mig_raw_crm_reservations_clean where rate_plan_code is null and sf_reservation_id is not null


select * from crm_person_lead_sfid_prod cplsp 


select count(*) 
from crm_person_lead_sfid_prod cplsp 


select *
from crm_person_lead_sfid_prod cplsp 
where cplsp.Email like '%expedia%'


SELECT
    SUBSTRING_INDEX(LOWER(TRIM(Email)), '@', -1)    AS email_domain,
    COUNT(*)                                        AS occurrences
FROM        crm_person_lead_sfid_prod
WHERE       Email IS NOT NULL
  AND       Email <> ''
  AND       Email LIKE '%@_%.__%'
GROUP BY    email_domain
ORDER BY    occurrences DESC;



select * from crm_person_lead_sfid_prod cplsp 
where MatchCandidateCount__c not in ('1.0', '0.0') 

select * from crm_person_lead_sfid_prod l 
where MatchCheckStatus__c = 

 
 select * from crm_person_lead_sfid_prod l 
 where MatchCheckStatus__c is not null

select distinct MatchCandidateCount__c from  crm_person_lead_sfid_prod cplsp 

select 
l.id as lead_id, l.FirstName, l.FirstName, l.Title, l.Email, l.BirthdayString__c,
a.id as account_id, a.FirstName , a.LastName, a.PersonEmail, l.
from crm_person_lead_sfid_prod l 
inner join crm_person_account_sfid_prod a
	on l.MatchedContact__c = a.PersonContactId 

select * from mig_crm_investors_accounts  	where cluster_email = 'sabrina_blazek@hotmail.com'

select * from mig_mapping_investor_clean mmic  where email = 'sabrina_blazek@hotmail.com' email = '003Te00000ps5EmIAI'

select * from gms_all_profiles gap where email = 'elisabethka54@gmail.com'

select * from gms_all_profiles gap where gap.sf_account_id = '001Te00000ZpBeWIAV'

select * from crm_loyality_sfid_prod where ExternalMemberId__c = '358038008'
select * from crm_loyality_sfid_prod where  ContactId = '003Te00000ps5XsIAI'

select * from gms_loyalty_liability gll where email = 'christoph.schulze.1985@gmail.com'

select * from gms_all_profiles gap where email = 'otto.stingl@aon.at'


se

select * from crm_person_account_sfid_prod cpasp 

SELECT
    Id,
    FirstName,
    LastName,
    Email,
    CASE
        WHEN LastName IS NULL OR LastName = '' THEN 'NoLastName'
        WHEN LOWER(SUBSTRING_INDEX(Email, '@', 1)) LIKE CONCAT('%', LOWER(LastName), '%')
            THEN 'Match'
        ELSE 'NoMatch'
    END AS lastname_in_email
FROM        crm_person_lead_sfid_prod
WHERE       LOWER(Email) LIKE '%@falkensteiner.com'
ORDER BY    lastname_in_email, LastName;



SELECT
    Id,
    FirstName,
    LastName,
    Email, 
    l.*
FROM        crm_person_lead_sfid_prod l
WHERE       LOWER(Email) LIKE '%@falkensteiner.com'
  AND       LastName IS NOT NULL
  AND       LastName <> ''
  AND       LOWER(SUBSTRING_INDEX(Email, '@', 1)) NOT LIKE CONCAT('%', LOWER(LastName), '%')
ORDER BY    LastName;