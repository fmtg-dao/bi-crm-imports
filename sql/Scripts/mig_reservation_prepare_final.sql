
/* create backup of the reservation table */ 
CREATE TABLE `mig_raw_crm_reservations_clean_copy` LIKE `mig_raw_crm_reservations_clean`;
INSERT INTO `mig_raw_crm_reservations_clean_copy` SELECT * FROM `mig_raw_crm_reservations_clean`;


/* set all property ids  */ 

update mig_raw_crm_reservations_clean c
inner join V2D_Property_Attributes pr 
	on pr.PAS_Protel_ID = c.property_protel_id 
SET c.property_id = pr.PAS_code3, c.property_fmtg_id = pr.PAS_FMTG_ID 
where c.source = 'protel';

-- exclude not mapped properties
update mig_raw_crm_reservations_clean c
set _excluded = 1
where c.source = 'protel'
 and c.property_fmtg_id is null ;


/* exclude cancelled and nowshow */

select * 
from mig_raw_crm_reservations_clean res
where reservation_status in ('Cancelled', 'NoShow')


update mig_raw_crm_reservations_clean res
set _excluded = 1
where reservation_status in ('Cancelled', 'NoShow')
and _excluded = 0 


/* exclude overlapping reservation from pms migration */ 

with cte as ( 
SELECT *
FROM mig_raw_crm_reservations_clean res
JOIN V2D_Property_Attributes a
  ON a.PAS_Protel_ID = res.property_protel_id
 AND a.PAS_apaleo_switch_from IS NOT NULL
 AND (
        (a.PAS_apaleo_switch_to IS NOT NULL
         AND DATE(res.arrival_at) BETWEEN DATE(a.PAS_apaleo_switch_from) AND DATE(a.PAS_apaleo_switch_to))
     OR (a.PAS_apaleo_switch_to IS NULL
         AND DATE(res.arrival_at) >= DATE(a.PAS_apaleo_switch_from))
     )
WHERE res.source = 'protel'
and res._excluded = 0
) 



-- update overlapped
UPDATE mig_raw_crm_reservations_clean res
JOIN V2D_Property_Attributes a
  ON a.PAS_Protel_ID = res.property_protel_id
 AND a.PAS_apaleo_switch_from IS NOT NULL
 AND (
        (a.PAS_apaleo_switch_to IS NOT NULL
         AND DATE(res.arrival_at) BETWEEN DATE(a.PAS_apaleo_switch_from) AND DATE(a.PAS_apaleo_switch_to))
     OR (a.PAS_apaleo_switch_to IS NULL
         AND DATE(res.arrival_at) >= DATE(a.PAS_apaleo_switch_from))
     )
SET res._excluded = 1
WHERE res.source = 'protel'
  AND res._excluded = 0;



/** Set salesforce reservation id **/ 

select * from crm_reservation_sfid_prod
select * from mig_raw_crm_reservations_clean

update mig_raw_crm_reservations_clean c
inner join crm_reservation_sfid_prod s
	on c.reservation_id = s.ReservationID__c 
set c.sf_reservation_id = s.Id


/** Analyse and remove reservation without data **/

select count(*)
from mig_raw_crm_reservations_clean res
where res._excluded = 0 and res.sf_reservation_id is null

-- add quality column 
ALTER TABLE `mig_raw_crm_reservations_clean`
ADD COLUMN `data_quality_bucket` varchar(32)
    CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL,
ADD KEY `idx_data_quality_bucket` (`data_quality_bucket`);

-- reset bucket

UPDATE mig_raw_crm_reservations_clean
SET data_quality_bucket = NULL
WHERE data_quality_bucket IS NOT NULL;


-- fill buckets 
UPDATE `mig_raw_crm_reservations_clean`
SET `data_quality_bucket` = CASE
    -- No personal data at all (inkl. last_name = 'Deleted' o.ä.)
    WHEN COALESCE(first_name, '') = ''
     AND COALESCE(middle_name, '') = ''
     AND (COALESCE(last_name, '') = '' OR LOWER(TRIM(last_name)) = 'deleted')
     AND COALESCE(email, '') = ''
     AND COALESCE(salutation, '') = ''
     AND COALESCE(gender, '') = ''
     AND birth_date IS NULL
     AND COALESCE(phone, '') = ''
     AND COALESCE(address, '') = ''
     AND COALESCE(city, '') = ''
     AND COALESCE(postal_code, '') = ''
     AND COALESCE(country, '') = ''
     AND COALESCE(nationality, '') = ''
        THEN '1_no_personal_data'
    -- Minimum viable: last_name AND email (Marker zählen nicht als last_name)
    WHEN COALESCE(last_name, '') <> ''
     AND LOWER(TRIM(last_name)) <> 'deleted'
     AND COALESCE(email, '') <> ''
        THEN '2_has_lastname_and_email'
    -- Has some data, but missing the minimum
    ELSE '3_partial_data'
END
WHERE `data_quality_bucket` IS NOT NULL;



-- check distribution
SELECT
	source,
    `data_quality_bucket`,
    COUNT(*) AS reservations,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM `mig_raw_crm_reservations_clean`
WHERE 1=1 -- and `_excluded` = 0
GROUP BY `data_quality_bucket`, source
ORDER BY source, `data_quality_bucket`;

-- 3. partial data bucket analyis 
select * from `mig_raw_crm_reservations_clean` where data_quality_bucket = '3_partial_data'

--- exclude last_name deleted 
update mig_raw_crm_reservations_clean
set _excluded = 1 
where lower(last_name) = 'deleted'


select last_name, count(*)
from `mig_raw_crm_reservations_clean` 
where data_quality_bucket = '3_partial_data'
and _excluded = 0
group by last_name
order by 2 desc


/* match with gms reservations */

select * from gms_reservations g


-- 1245597
-- check - must return zero rows
select *
-- select cr.reservation_id, count(*)
from `mig_raw_crm_reservations_clean` cr
inner join gms_reservations gr
	on cr.reservation_id = gr.pms_reservation_id
inner join gms_all_profiles gp
	on gr.list_id = gp.list_id 
where 1=1 
	and cr.sf_person_contact_id is null  -- not mapped
	and cr.sf_reservation_id is null -- not imported
	and gp.sf_contact_id is not null -- imported contact
	and gr.is_duplicate = 0  -- no dups in gms res
	-- and cr.reservation_id = '13021529'
group by cr.reservation_id having count(*) > 1



select * 
from gms_reservations  gr
inner join gms_all_profiles gp
	on gr.list_id = gp.list_id 
where reservation_id = '13021529'



select cr.*, gp.email , gp.fname, gp.lname, gp.sf_contact_id, gp.sf_account_id

-- select count(*)
from `mig_raw_crm_reservations_clean` cr
inner join gms_reservations gr
	on cr.reservation_id = gr.pms_reservation_id
inner join gms_all_profiles gp
	on gr.list_id = gp.list_id 
where 1=1 
	and cr.sf_person_contact_id is null  -- not mapped
	and cr.sf_reservation_id is null -- not imported
	-- and gp.sf_contact_id is not null -- imported contact
	and gr.is_duplicate = 0  -- no dups in gms res
	and cr._excluded = 0  -- excluded
	and cr.sf_property_id is not null -- property exists in sf
	and gp.sf_contact_id is not null
	and gp.email = cr.email
	and cr.data_quality_bucket = '2_has_lastname_and_email'
	-- and cr.reservation_id = '13021529'
 

/** create import table for reservations with contacts **/

CREATE TABLE mig_raw_crm_reservations_clean_matched_imp20260427
LIKE mig_raw_crm_reservations_clean;


select * from mig_raw_crm_reservations_clean_matched_imp20260427_2

INSERT INTO mig_raw_crm_reservations_clean_matched_imp20260427_2 (
    row_id, cluster_id, _entity_id, _excluded, _ptable,
    reservation_id, source, property_id, property_fmtg_id, property_protel_id,
    booking_id, reservation_status, group_name,
    arrival_at, departure_at, booking_at, checkin_at, checkout_at, cancelled_at, noshow_at,
    market_segment, market_channel, rate_plan_code, booker_company_id,
    adults_num, children_num, unit_group_code, travel_purpose, external_code, guest_role, room_nights,
    first_name, middle_name, last_name, email, birth_date, salutation, gender,
    preferred_language, address, city, postal_code, country, phone, birth_place, nationality,
    revenue_room, revenue_fnb, revenue_extra, revenue_total,
    sf_preferred_language, sf_reservation_status, sf_property_id,
    sf_person_contact_id, sf_person_account_id,
    is_investor, central_consent, sf_reservation_id, sf_property_id_uat, data_quality_bucket
)
SELECT
    cr.row_id, cr.cluster_id, cr._entity_id, cr._excluded, cr._ptable,
    cr.reservation_id, cr.source, cr.property_id, cr.property_fmtg_id, cr.property_protel_id,
    cr.booking_id, cr.reservation_status, cr.group_name,
    cr.arrival_at, cr.departure_at, cr.booking_at, cr.checkin_at, cr.checkout_at, cr.cancelled_at, cr.noshow_at,
    cr.market_segment, cr.market_channel, cr.rate_plan_code, cr.booker_company_id,
    cr.adults_num, cr.children_num, cr.unit_group_code, cr.travel_purpose, cr.external_code, cr.guest_role, cr.room_nights,
    cr.first_name, cr.middle_name, cr.last_name, cr.email, cr.birth_date, cr.salutation, cr.gender,
    cr.preferred_language, cr.address, cr.city, cr.postal_code, cr.country, cr.phone, cr.birth_place, cr.nationality,
    cr.revenue_room, cr.revenue_fnb, cr.revenue_extra, cr.revenue_total,
    cr.sf_preferred_language, cr.sf_reservation_status, cr.sf_property_id,
    gp.sf_contact_id  AS sf_person_contact_id,
    gp.sf_account_id  AS sf_person_account_id,
    cr.is_investor, cr.central_consent, cr.sf_reservation_id, cr.sf_property_id_uat, cr.data_quality_bucket
FROM mig_raw_crm_reservations_clean cr
INNER JOIN gms_reservations  gr ON cr.reservation_id = gr.pms_reservation_id
INNER JOIN gms_all_profiles  gp ON gr.list_id        = gp.list_id
WHERE cr.sf_person_contact_id IS NULL
  AND cr.sf_reservation_id    IS NULL
  AND gr.is_duplicate          = 0
  AND cr._excluded             = 0
  AND cr.sf_property_id       IS NOT NULL
  AND gp.sf_contact_id        IS NOT NULL
  AND gp.email                 = cr.email
  AND cr.data_quality_bucket   = '2_has_lastname_and_email';


truncate table mig_raw_crm_reservations_clean_matched_imp20260427

-- remove contacts from future reservations. 
update mig_raw_crm_reservations_clean_matched_imp20260427
set sf_person_contact_id = null, sf_person_account_id = null
where arrival_at > '2026-05-05'



select * from  mig_raw_crm_reservations_clean_matched_imp20260427



/* remove appartment owner  */



update`mig_raw_crm_reservations_clean` cr
set _excluded = 1
where cr.rate_plan_code in ('OWN', 'APP', 'APPET', 'APP')


where lower(rate_plan_code) like '%app%'
and source = 'protel'
 ('OWN', 'APP')
group by rate_plan_code 

/* --> apaleo entfernen APPET und APP  */

/***  update sf_reservation_id  ***/
update mig_raw_crm_reservations_clean c -- _matched_imp20260427 c
inner join crm_reservation_sfid_prod s
	on c.reservation_id = s.ReservationID__c  
set c.sf_reservation_id = s.Id



/***  analyse what else can be imported  ***/

select c.market_segment, count(*) 
from  mig_raw_crm_reservations_clean c
where 1=1 -- _excluded = 0 
and c.data_quality_bucket = '1_no_personal_data'
group by c.market_segment 


select c.data_quality_bucket, count(*) 
from  mig_raw_crm_reservations_clean c
where _excluded = 0 
and c.sf_reservation_id is null
group by c.data_quality_bucket


select * from crm_person_account_sfid_prod

select source_id as reservation_id, 
	sp.id as sf_person_account_id, 
	sp.PersonContactId as sf_person_contact_id,
	sp.PersonEmail as sf_person_email,
	sp.ClusterID__pc as cluster_id,
	r.email,
	sp.FirstName,
	sp.LastName,
	r.first_name,
	r.last_name 
from mig_crm_person_accounts pa
inner join crm_person_account_sfid_prod sp
	on sp.ClusterID__pc = pa.cluster_id
inner join mig_raw_crm_reservations_clean r
	on r.reservation_id = pa.source_id
where r.sf_reservation_id is null
and r._excluded = 0 



CREATE TABLE mig_raw_crm_reservations_clean_matched_imp20260427_2
LIKE mig_raw_crm_reservations_clean;


INSERT INTO mig_raw_crm_reservations_clean_matched_imp20260427_2 (
    row_id, cluster_id, _entity_id, _excluded, _ptable,
    reservation_id, source, property_id, property_fmtg_id, property_protel_id,
    booking_id, reservation_status, group_name,
    arrival_at, departure_at, booking_at, checkin_at, checkout_at, cancelled_at, noshow_at,
    market_segment, market_channel, rate_plan_code, booker_company_id,
    adults_num, children_num, unit_group_code, travel_purpose, external_code, guest_role, room_nights,
    first_name, middle_name, last_name, email, birth_date, salutation, gender,
    preferred_language, address, city, postal_code, country, phone, birth_place, nationality,
    revenue_room, revenue_fnb, revenue_extra, revenue_total,
    sf_preferred_language, sf_reservation_status, sf_property_id,
    sf_person_contact_id, sf_person_account_id,
    is_investor, central_consent, sf_reservation_id, sf_property_id_uat, data_quality_bucket
)
SELECT
    r.row_id, r.cluster_id, r._entity_id, r._excluded, r._ptable,
    r.reservation_id, r.source, r.property_id, r.property_fmtg_id, r.property_protel_id,
    r.booking_id, r.reservation_status, r.group_name,
    r.arrival_at, r.departure_at, r.booking_at, r.checkin_at, r.checkout_at, r.cancelled_at, r.noshow_at,
    r.market_segment, r.market_channel, r.rate_plan_code, r.booker_company_id,
    r.adults_num, r.children_num, r.unit_group_code, r.travel_purpose, r.external_code, r.guest_role, r.room_nights,
    r.first_name, r.middle_name, r.last_name, r.email, r.birth_date, r.salutation, r.gender,
    r.preferred_language, r.address, r.city, r.postal_code, r.country, r.phone, r.birth_place, r.nationality,
    r.revenue_room, r.revenue_fnb, r.revenue_extra, r.revenue_total,
    r.sf_preferred_language, r.sf_reservation_status, r.sf_property_id,
    sp.PersonContactId  AS sf_person_contact_id,
    sp.Id               AS sf_person_account_id,
    r.is_investor, r.central_consent, r.sf_reservation_id, r.sf_property_id_uat, r.data_quality_bucket
FROM mig_raw_crm_reservations_clean r
INNER JOIN mig_crm_person_accounts pa
    ON pa.source_id = r.reservation_id
INNER JOIN crm_person_account_sfid_prod sp
    ON sp.ClusterID__pc  =  pa.cluster_id
WHERE r.sf_reservation_id IS NULL
  AND r._excluded = 0;
  -- AND sf_property_id IS NOT NULL ;









SELECT
    COUNT(*) AS total_rows,
    -- city
    MAX(CHAR_LENGTH(city))                          AS city_max_len,
    SUM(CHAR_LENGTH(city) > 40)                     AS city_over_40,
    SUM(CHAR_LENGTH(city) > 80)                     AS city_over_80,
    -- first_name
    MAX(CHAR_LENGTH(first_name))                    AS first_name_max_len,
    SUM(CHAR_LENGTH(first_name) > 40)               AS first_name_over_40,
    SUM(CHAR_LENGTH(first_name) > 80)               AS first_name_over_80,
    -- last_name
    MAX(CHAR_LENGTH(last_name))                     AS last_name_max_len,
    SUM(CHAR_LENGTH(last_name) > 40)                AS last_name_over_40,
    SUM(CHAR_LENGTH(last_name) > 80)                AS last_name_over_80
FROM mig_raw_crm_reservations_clean_matched_imp20260427_2;


select * 
from mig_raw_crm_reservations_clean_matched_imp20260427_2
where CHAR_LENGTH(last_name) > 40


select property_id, count(*)
from mig_raw_crm_reservations_clean_matched_imp20260427_2
where sf_property_id is null
group by property_id

select sa.FirstName, sa.LastName, sa.PersonEmail, cr.first_name, cr.last_name, cr.email, cr.reservation_id, cr.source, cr.arrival_at 
select cr.reservation_id, sa.PersonContactId, sa.id 
from mig_raw_crm_reservations_clean cr
left join crm_person_account_sfid_prod sa
	on cr.email = sa.PersonEmail
	and cr.last_name = sa.LastName
	and cr.first_name =  sa.FirstName 
where _excluded = 0
and data_quality_bucket = '2_has_lastname_and_email'
and cr.sf_reservation_id is null
-- and sa.PersonEmail = 'angela@feufel.de'
and sa.PersonEmail is not null


CREATE INDEX idx_crm_person_account_sfid_prod_match3
ON crm_person_account_sfid_prod (PersonEmail, LastName, FirstName);

CREATE TABLE mig_raw_crm_reservations_clean_matched_imp20260427_3
LIKE mig_raw_crm_reservations_clean;

INSERT INTO mig_raw_crm_reservations_clean_matched_imp20260427_3 (
    row_id, cluster_id, _entity_id, _excluded, _ptable,
    reservation_id, source, property_id, property_fmtg_id, property_protel_id,
    booking_id, reservation_status, group_name,
    arrival_at, departure_at, booking_at, checkin_at, checkout_at, cancelled_at, noshow_at,
    market_segment, market_channel, rate_plan_code, booker_company_id,
    adults_num, children_num, unit_group_code, travel_purpose, external_code, guest_role, room_nights,
    first_name, middle_name, last_name, email, birth_date, salutation, gender,
    preferred_language, address, city, postal_code, country, phone, birth_place, nationality,
    revenue_room, revenue_fnb, revenue_extra, revenue_total,
    sf_preferred_language, sf_reservation_status, sf_property_id,
    sf_person_contact_id, sf_person_account_id,
    is_investor, central_consent, sf_reservation_id, sf_property_id_uat, data_quality_bucket
)
SELECT
    cr.row_id, cr.cluster_id, cr._entity_id, cr._excluded, cr._ptable,
    cr.reservation_id, cr.source, cr.property_id, cr.property_fmtg_id, cr.property_protel_id,
    cr.booking_id, cr.reservation_status, cr.group_name,
    cr.arrival_at, cr.departure_at, cr.booking_at, cr.checkin_at, cr.checkout_at, cr.cancelled_at, cr.noshow_at,
    cr.market_segment, cr.market_channel, cr.rate_plan_code, cr.booker_company_id,
    cr.adults_num, cr.children_num, cr.unit_group_code, cr.travel_purpose, cr.external_code, cr.guest_role, cr.room_nights,
    cr.first_name, cr.middle_name, cr.last_name, cr.email, cr.birth_date, cr.salutation, cr.gender,
    cr.preferred_language, cr.address, cr.city, cr.postal_code, cr.country, cr.phone, cr.birth_place, cr.nationality,
    cr.revenue_room, cr.revenue_fnb, cr.revenue_extra, cr.revenue_total,
    cr.sf_preferred_language, cr.sf_reservation_status, cr.sf_property_id,
    sa.PersonContactId  AS sf_person_contact_id,    -- ← resolved
    sa.Id               AS sf_person_account_id,    -- ← resolved
    cr.is_investor, cr.central_consent, cr.sf_reservation_id, cr.sf_property_id_uat, cr.data_quality_bucket
FROM mig_raw_crm_reservations_clean cr
INNER JOIN crm_person_account_sfid_prod sa
    ON  sa.PersonEmail = cr.email
    AND sa.LastName    = cr.last_name
    AND sa.FirstName   = cr.first_name
WHERE cr._excluded            = 0
  AND cr.data_quality_bucket  = '2_has_lastname_and_email'
  AND cr.sf_reservation_id   IS NULL
  AND sa.PersonEmail         IS NOT NULL;



select sa.FirstName, sa.LastName, sa.PersonEmail, cr.first_name, cr.last_name, cr.email, cr.reservation_id, cr.source, cr.arrival_at 
-- select cr.reservation_id, sa.PersonContactId, sa.id 
from mig_raw_crm_reservations_clean cr
INNER JOIN crm_person_account_sfid_prod sa
	on sa.ClusterID__pc = cr.cluster_id
	-- and sa.FirstName = cr.first_name
	-- and sa.LastName = cr.last_name 
  AND cr._excluded            = 0
  AND cr.data_quality_bucket  = '2_has_lastname_and_email'
  AND cr.sf_reservation_id   IS NULL
  AND sa.PersonEmail is not null
  
  
  -- irina.vl.zotova,@gmail.com
  
  
select * 
from mig_raw_crm_reservations_clean cr
  WHERE cr._excluded            = 0
  AND cr.data_quality_bucket  = '2_has_lastname_and_email'
  AND cr.sf_reservation_id   IS NULL
  AND cr.reservation_status not in ('Optional')
  
  
  
select count(*) 
from mig_raw_crm_reservations_clean cr
  WHERE cr._excluded            = 0
  and cr.sf_reservation_id   IS NOT NULL

select * from crm_person_account_sfid_prod where id in ('001Te00000ZpKktIAF',
'001Te00000ZpKyEIAV')


select PersonEmail, count(*)
from crm_person_account_sfid_prod
group by PersonEmail having  count(*) > 1

   



update mig_raw_crm_reservations_clean cr
inner join crm_reservation_sfid_prod sr
	on cr.reservation_id = sr.ReservationID__c 
set cr.sf_reservation_id = sr.Id
where cr.sf_reservation_id is null




select * from crm_reservation_sfid_prod

SELECT
    SUM(Lead__c IS NOT NULL AND Contact__c IS NULL)    AS lead_only,
    SUM(Contact__c IS NOT NULL AND Lead__c IS NULL)    AS contact_only,
    SUM(Lead__c IS NOT NULL AND Contact__c IS NOT NULL) AS contact_lead,
    SUM(Lead__c IS NULL AND Contact__c IS NULL)        AS  no_nothing,
    SUM(ReservationID__c is not NULL )					AS total
FROM crm_reservation_sfid_prod;


select count(*) 
from crm_consent_sfid_prod
where Name = 'marketing_central'
and CaptureDate > '2026-04-26'

select Name, count(*) 
from crm_consent_sfid_prod
group by Name

select count(*) from crm_consent_sfid_prod ccsp 
where PrivacyConsentStatus <> 'OptIn'


select count(distinct ccsp.EmailAddress ) from crm_consent_sfid_prod ccsp
where PrivacyConsentStatus <> 'OptIn'
