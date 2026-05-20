

select distinct property_code  from mig_aleno_contacts where email is not null and newsletter = 1

/*
FBZ, marketing_property_Bozen, 0ZWTe0000000X8rOAE
FJE, marketing_property_Jesolo,
FMP, marketing_property_Prague,
FSV, marketing_property_Velden

*/



select * from crm_consent_sfid_prod ccsp 

select a.property_code, a.email, p.Id 
from mig_aleno_contacts a
left join crm_properties_sfid_prod p
	on p.ApaleoID__c = a.property_code 
where a.email is not null
and a.newsletter = 1 
group by a.property_code, a.email, p.Id 


select * 
from (

select a.property_code, a.email, p.Id as sf_property_id 
from mig_aleno_contacts a
left join crm_properties_sfid_prod p
	on p.ApaleoID__c = a.property_code 
where a.email is not null
and a.newsletter = 1 
group by a.property_code, a.email, p.Id 

) aleno 

inner join crm_consent_sfid_prod con
on aleno.email = con.EmailAddress
and aleno.sf_property_id = con.Property__c

where left(con.PartyID__c,3) = '003'  


with aleno as (
    select a.property_code, a.email, p.Id as sf_property_id 
    from mig_aleno_contacts a
    left join crm_properties_sfid_prod p
        on p.ApaleoID__c = a.property_code 
    where a.email is not null
      and a.newsletter = 1 
    group by a.property_code, a.email, p.Id 
)

select 

		cpe.id as cpe_id, 
		'aleno' as source,
		sf_property_id,
		case property_code
			when 'FSV' then 'marketing_property_Velden'
			when 'FJE' then 'marketing_property_Jesolo'
			when 'FBZ' then 'marketing_property_Bozen'
			when 'FMP' then 'marketing_property_Prague'
		end as consent_type
from aleno a
inner join crm_cp_email_sfid_prod cpe
	on cpe.EmailAddress = a.email
	and a.email not like '%@falkensteiner.com'
where not exists ( 
    select 1 
    from crm_consent_sfid_prod con 
    where a.email = con.EmailAddress
      and a.sf_property_id = con.Property__c
)


select * from crm_cp_consent_sfid_prod cccsp cccsp 



/** IMPORT TABLE **/

CREATE TABLE mig_aleno_consent_imp20260508 AS
WITH aleno AS (
    SELECT a.property_code, a.email, p.Id AS sf_property_id 
    FROM mig_aleno_contacts a
    LEFT JOIN crm_properties_sfid_prod p
        ON p.ApaleoID__c = a.property_code 
    WHERE a.email IS NOT NULL
      AND a.newsletter = 1 
    GROUP BY a.property_code, a.email, p.Id 
)
SELECT 
    cpe.id AS cpe_id, 
    'aleno' AS source,
    a.sf_property_id,
    CASE a.property_code
        WHEN 'FSV' THEN 'marketing_property_Velden'
        WHEN 'FJE' THEN 'marketing_property_Jesolo'
        WHEN 'FBZ' THEN 'marketing_property_Bozen'
        WHEN 'FMP' THEN 'marketing_property_Prague'
    END AS consent_type
FROM aleno a
INNER JOIN crm_cp_email_sfid_prod cpe
    ON cpe.EmailAddress = a.email
    AND a.email NOT LIKE '%@falkensteiner.com'
WHERE NOT EXISTS ( 
    SELECT 1 
    FROM crm_consent_sfid_prod con 
    WHERE a.email = con.EmailAddress
      AND a.sf_property_id = con.Property__c
);



select count(*) 
from mig_aleno_consent_imp20260508 i



select source, central_consent, count(*) 
from mig_raw_crm_reservations_clean 
where sf_reservation_id is not null
group by source, central_consent 


select count(*) 
from gms_all_profiles gap where gap.sf_contact_id is not null and gap.central_consent = 1


select count(*) from crm_loyality_sfid_prod clsp 

