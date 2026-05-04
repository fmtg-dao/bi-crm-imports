

select data_quality_bucket, count(*) 
from mig_raw_crm_reservations_clean c
where sf_property_id is not null
and reservation_status not in ('Cancelled', 'NoShow')
and EXISTS (
        SELECT 1 
        FROM V2D_Property_Attributes a 
        WHERE a.is_active = 1 
          AND a.PAS_code3 = c.property_id
      )
group by data_quality_bucket 



SELECT 
    YEAR(booking_at) AS booking_year,
    SUM(CASE WHEN data_quality_bucket = '3_partial_data' THEN 1 ELSE 0 END) AS partial_data_count,
    COUNT(*) AS total_count,
    ROUND(
        100.0 * SUM(CASE WHEN data_quality_bucket = '3_partial_data' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) AS partial_data_pct
FROM mig_raw_crm_reservations_clean c
where sf_property_id is not null
and reservation_status not in ('Cancelled', 'NoShow', 'Optional')
and YEAR(booking_at) >= 2016
and arrival_at < '2026-04-01'
-- and source = 'apaleo'
AND EXISTS (
        SELECT 1 
        FROM V2D_Property_Attributes a 
        WHERE a.is_active = 1 
          AND a.PAS_code3 = c.property_id
      )
GROUP BY YEAR(booking_at)
ORDER BY booking_year;




select 1 from V2D_Property_Attributes where is_active = 1 and PAS_code3 = 

select property_id, count(*) 
from mig_raw_crm_reservations_clean
where sf_property_id is not null
and reservation_status not in ('Cancelled', 'NoShow', 'Optional')
and YEAR(booking_at) >= 2016
and data_quality_bucket = '3_partial_data'
and arrival_at < '2026-04-01'
and source = 'apaleo'
group by property_id 

select * 
from mig_raw_crm_reservations_clean
where sf_property_id is not null
and reservation_status not in ('Cancelled', 'NoShow', 'Optional')
and YEAR(booking_at) >= 2025
and data_quality_bucket = '3_partial_data'
and arrival_at < '2026-04-01'
and property_id = 'FDO'


SELECT 
    c.property_id,
    SUM(CASE WHEN c.data_quality_bucket = '3_partial_data' THEN 1 ELSE 0 END) AS partial_data_count,
    COUNT(*) AS total_count,
    ROUND(
        100.0 * SUM(CASE WHEN c.data_quality_bucket = '3_partial_data' THEN 1 ELSE 0 END) / COUNT(*), 
        2
    ) AS partial_data_pct
FROM mig_raw_crm_reservations_clean c
WHERE c.sf_property_id IS NOT NULL
  AND c.reservation_status NOT IN ('Cancelled', 'NoShow', 'Optional')
  AND YEAR(c.booking_at) = 2025
  AND c.arrival_at < '2026-04-01'
  -- AND c.source = 'apaleo'
  AND EXISTS (
        SELECT 1 
        FROM V2D_Property_Attributes a 
        WHERE a.is_active = 1 
          AND a.PAS_code3 = c.property_id
      )
GROUP BY c.property_id
ORDER BY partial_data_pct DESC;




SELECT 
    data_quality_bucket,
    COUNT(*) AS bucket_count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 
        2
    ) AS pct_of_total
FROM mig_raw_crm_reservations_clean c
WHERE sf_property_id IS NOT NULL
  AND reservation_status NOT IN ('Cancelled', 'NoShow')
  AND EXISTS (
        SELECT 1 
        FROM V2D_Property_Attributes a 
        WHERE a.is_active = 1 
          AND a.PAS_code3 = c.property_id
      )
GROUP BY data_quality_bucket
ORDER BY bucket_count DESC;




WITH agg AS (
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN first_name         IS NOT NULL AND first_name         <> '' THEN 1 ELSE 0 END) AS first_name_cnt,
        SUM(CASE WHEN middle_name        IS NOT NULL AND middle_name        <> '' THEN 1 ELSE 0 END) AS middle_name_cnt,
        SUM(CASE WHEN last_name          IS NOT NULL AND last_name          <> '' THEN 1 ELSE 0 END) AS last_name_cnt,
        SUM(CASE WHEN email              IS NOT NULL AND email              <> '' THEN 1 ELSE 0 END) AS email_cnt,
        SUM(CASE WHEN phone              IS NOT NULL AND phone              <> '' THEN 1 ELSE 0 END) AS phone_cnt,
        SUM(CASE WHEN birth_date         IS NOT NULL                              THEN 1 ELSE 0 END) AS birth_date_cnt,
        SUM(CASE WHEN birth_place        IS NOT NULL AND birth_place        <> '' THEN 1 ELSE 0 END) AS birth_place_cnt,
        SUM(CASE WHEN salutation         IS NOT NULL AND salutation         <> '' THEN 1 ELSE 0 END) AS salutation_cnt,
        SUM(CASE WHEN gender             IS NOT NULL AND gender             <> '' THEN 1 ELSE 0 END) AS gender_cnt,
        SUM(CASE WHEN preferred_language IS NOT NULL AND preferred_language <> '' THEN 1 ELSE 0 END) AS preferred_language_cnt,
        SUM(CASE WHEN nationality        IS NOT NULL AND nationality        <> '' THEN 1 ELSE 0 END) AS nationality_cnt,
        SUM(CASE WHEN address            IS NOT NULL AND address            <> '' THEN 1 ELSE 0 END) AS address_cnt,
        SUM(CASE WHEN postal_code        IS NOT NULL AND postal_code        <> '' THEN 1 ELSE 0 END) AS postal_code_cnt,
        SUM(CASE WHEN city               IS NOT NULL AND city               <> '' THEN 1 ELSE 0 END) AS city_cnt,
        SUM(CASE WHEN country            IS NOT NULL AND country            <> '' THEN 1 ELSE 0 END) AS country_cnt
    FROM mig_raw_crm_reservations_clean c
    WHERE c.sf_property_id IS NOT NULL
      AND c.reservation_status NOT IN ('Cancelled', 'NoShow')
      AND EXISTS (
            SELECT 1 
            FROM V2D_Property_Attributes a 
            WHERE a.is_active = 1 
              AND a.PAS_code3 = c.property_id
          )
)
SELECT field, total_rows, filled_count,
       ROUND(100.0 * filled_count / total_rows, 2) AS fill_rate_pct
FROM (
    SELECT 'first_name'         AS field, total_rows, first_name_cnt         AS filled_count FROM agg
    UNION ALL SELECT 'middle_name',         total_rows, middle_name_cnt         FROM agg
    UNION ALL SELECT 'last_name',           total_rows, last_name_cnt           FROM agg
    UNION ALL SELECT 'email',               total_rows, email_cnt               FROM agg
    UNION ALL SELECT 'phone',               total_rows, phone_cnt               FROM agg
    UNION ALL SELECT 'birth_date',          total_rows, birth_date_cnt          FROM agg
    UNION ALL SELECT 'birth_place',         total_rows, birth_place_cnt         FROM agg
    UNION ALL SELECT 'salutation',          total_rows, salutation_cnt          FROM agg
    UNION ALL SELECT 'gender',              total_rows, gender_cnt              FROM agg
    UNION ALL SELECT 'preferred_language',  total_rows, preferred_language_cnt  FROM agg
    UNION ALL SELECT 'nationality',         total_rows, nationality_cnt         FROM agg
    UNION ALL SELECT 'address',             total_rows, address_cnt             FROM agg
    UNION ALL SELECT 'postal_code',         total_rows, postal_code_cnt         FROM agg
    UNION ALL SELECT 'city',                total_rows, city_cnt                FROM agg
    UNION ALL SELECT 'country',             total_rows, country_cnt             FROM agg
) x
ORDER BY fill_rate_pct DESC;







SELECT 
    _entity_id,
    COUNT(*) AS reservation_count,
    COUNT(DISTINCT CONCAT(COALESCE(first_name,''), '|', COALESCE(last_name,''))) AS distinct_names,
    GROUP_CONCAT(DISTINCT CONCAT(first_name, ' ', last_name) ORDER BY first_name SEPARATOR ' | ') AS names_seen,
    MAX(email)      AS shared_email,
    MAX(birth_date) AS shared_birthday
FROM mig_raw_crm_reservations_clean
WHERE _entity_id IS NOT NULL
  AND birth_date IS NOT NULL
  AND email IS NOT NULL AND email <> ''
GROUP BY _entity_id
HAVING distinct_names    >= 2
   AND COUNT(DISTINCT birth_date) = 1
   AND COUNT(DISTINCT email)      = 1
ORDER BY distinct_names DESC, reservation_count DESC
LIMIT 100;


select * from mig_raw_crm_reservations_clean 
where email in ("claudio.sturm@falkensteiner.com")


SELECT distinct
    source,
    gender,
	salutation,
    first_name,
    last_name,
    email,
    birth_date,
    phone
FROM mig_raw_crm_reservations_clean
WHERE email in ("claudio.sturm@falkensteiner.com")
order by 4,5,6,7 desc


/*
ursula@ursula-holzer.com
m.ainberger@aon.at
hschutzelhofer@gmail.com
b.schneller@gmx.at
office@stopper-consulting.at
anu.ahas@hotmail.com
claudiaundfred@gmx.de

*/


select * from gms_reservations gr where 

select * from mig_raw_crm_reservations_clean where reservation_id = 15692509


select * from gms_reservations where is_duplicate = 1

select * from gms_reservations where  reservation_id = '9925979'

where list_id = 418853975



select * from gms_all_profiles gap where email = 'j.kukavica3101@gmail.com'



select * from V2D_Property_Attributes vdpa 


CREATE INDEX IX_crm_cp_consent_sfid_prod_PartyId
ON crm_cp_consent_sfid_prod (PartyId);

-- On the email table: ensure Id is indexed (usually already the PK)
-- If Id is not already a primary key or unique index:
CREATE UNIQUE INDEX IX_crm_cp_email_sfid_prod_Id
ON crm_cp_email_sfid_prod (Id);




select * from gustaffo_newsletter_contacts


select Name, count(*) 
from crm_cp_consent_sfid_prod con
inner join crm_cp_email_sfid_prod eml 
	on con.ContactPointId  = eml.Id
where con.PrivacyConsentStatus = 'OptIn'
group by Name




select * from mig_crm_investors_accounts mcia 

-- 5960

-- 6379

select count(*) 
from mig_mapping_investor_clean i
inner join crm_person_account_sfid_prod p
	on p.PersonEmail = i.email
where p.PersonEmail is null


select * 
from mig_mapping_investor_clean i
inner join gms_all_profiles   p
	on p.email = i.email
where not exists (select 1 from crm_person_account_sfid_prod a where a.PersonEmail = p.email)


select * from crm_loyality_sfid_prod clsp where clsp.ExternalMemberId__c = '364384008'

select * from crm_person_account_sfid_prod p where Id = '001Te00000ZpCHHIA3'

select * 
from gms_all_profiles p 
where sf_account_id = '001Te00000ZpCHHIA3'


select count(*) from ( 
	select sf_account_id, count(*) 
	from gms_all_profiles p
	where sf_account_id is not null
	group by sf_account_id having count(*) > 1  
) a



/*** duplicated records gms cleansing */ 


SELECT *
FROM gms_all_profiles
WHERE sf_account_id IS NOT NULL
  AND sf_account_id IN (
        SELECT sf_account_id
        FROM gms_all_profiles
        WHERE sf_account_id IS NOT NULL
        GROUP BY sf_account_id
        HAVING COUNT(*) > 1
  )
AND `domain`  like '%falkensteiner%'
ORDER BY sf_account_id;


select * from mig_raw_crm_reservations_clean where rate_plan_code is null and sf_reservation_id is not null


select * from crm_person_lead_sfid_prod cplsp 



select * from  gms_all_profiles gap  where email = 'anton.hoellmueller72@gmail.com';
select * from  gms_all_profiles gap  where email = 'lena.kraemer@aon.at';

select * from mig_raw_crm_reservations_clean where email = 'anton.hoellmueller72@gmail.com'

select * from  gms_all_profiles gap  where email = 'lena.kraemer@aon.at'

select * from  gms_all_profiles gap  where gap.sf_account_id = '001Te00000ZpCHHIA3'





                                    
                                    
select count(*)
from mig_mapping_investor_clean ic
left join gms_all_profiles gap
	on gap.email = ic.email 
where gap.email is not null
and gap.sf_member_id is null
-- and gap.sf_contact_id is null

select * from mig_crm_gms_accounts_imp20260421 where is_investor = 1


'helmut.kammel@justiz.gv.at'
select * from gms_all_profiles gap where email = 'helmut.kammel@justiz.gv.at'
select * from gms_all_profiles gap where list_id = '400109448'
select * from gms_loyalty_liability gll where list_id = '400257233'
select * from gms_loyalty_liability gll where list_id = '400109448'

update gms_all_profiles gap 
set exclude_email = 0, is_investor = 1
where list_id = '400109448'

400109448

select * from gms_loyalty_liability gll where email = 'heli.kammel@gmx.at'



select * from crm_loyality_sfid_prod clsp 
select * from crm_person_account_sfid_prod


select  
		sfc.PersonEmail,
		sfc.FirstName,
		sfc.LastName,
		sfc.PersonBirthdate,
		sfc.PersonGenderIdentity,
		sfc.Salutation,
		sfc.BillingPostalCode,
		sfc.BillingCountryCode__c,
		sfc.PersonContactId, 
		sfc.Id as PersonAccountId,
		sfl.Id as MembershipID,
		sfl.TierName__c,
		sfl.MembershipNumber, 
		sfl.LegacyMemberId__c,
		sfl.LegacyTier__c,
		sfl.EntraID__c
		
from crm_person_account_sfid_prod sfc
left join crm_loyality_sfid_prod sfl 
	on sfc.PersonContactId = sfl.ContactId
where 1=1
	and sfc.IsPersonAccount = 'True'
	and sfc.InvestCustomer__pc = 'True'



	



