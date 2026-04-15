
/* Prepare Table */ 

/* ->> CHECK TABLE NAME <<- */

ALTER TABLE `mig_raw_crm_reservations_clean`
ADD INDEX `idx_reservation_id` (`reservation_id`);

ALTER TABLE `mig_raw_crm_reservations_clean`
ADD INDEX `idx_email` (`email`);


ALTER TABLE `gms_all_profiles`
MODIFY COLUMN `email` VARCHAR(255);

ALTER TABLE `gms_all_profiles`
ADD INDEX `idx_email` (`email`);


/* set checkin, checkout and cancelled, noshow */

UPDATE mig_raw_crm_reservations_iw r
INNER JOIN crm_protel_reservation_events e
    ON r.reservation_id = e.buchnr

SET
    r.checkin_at   = COALESCE(e.checkin_at, r.checkin_at),
    r.checkout_at  = COALESCE(e.checkout_at, r.checkout_at),
    r.cancelled_at = COALESCE(e.cancelled_at, r.cancelled_at),
    r.noshow_at    = COALESCE(e.noshow_at, r.noshow_at)
WHERE
    r.source = 'protel';



/* Update Segments */ 

UPDATE mig_raw_crm_reservations_iw
SET market_segment = CASE
    WHEN market_segment = 'Individual'       THEN 'INDIVIDUAL'
    WHEN market_segment = 'Agent/FIT'        THEN 'FIT'
    WHEN market_segment = 'CORP/BUS Indiv'   THEN 'LCR'
    WHEN market_segment = 'Other'            THEN 'OTHERS'
    WHEN market_segment = 'OTHER'            THEN 'OTHERS'
    WHEN market_segment = 'Leisure GRP'      THEN 'LEISUREGR'
    WHEN market_segment = 'MICE/BUS Grp'     THEN 'MICEGR'
    WHEN market_segment = 'Account'          THEN 'OTHER'
    WHEN market_segment = 'TIME'             THEN 'TIME'
    WHEN market_segment = 'Sport'            THEN 'SPORTSGR'
    WHEN market_segment = 'SPORTGR'          THEN 'SPORTSGR'
    WHEN market_segment = 'Appartment'       THEN 'OTHERS'
    WHEN market_segment = 'Leisure Indiv'    THEN 'INDIVIDUAL'
    ELSE market_segment
END;


select distinct market_segment from mig_raw_crm_reservations_clean;


/* Correction Travel Purpose */ 

-- waiting for matching definition 

UPDATE mig_raw_crm_reservations_iw
SET travel_purpose = CASE
    WHEN travel_purpose = 'Business'                          THEN 'Business'
    WHEN travel_purpose = 'Account'                           THEN null
    WHEN travel_purpose = 'IT CENTRAL Reservation'            THEN null
    WHEN travel_purpose = 'IT CENTRAL Group'                  THEN null
    WHEN travel_purpose = 'HR CENTRAL Reservation'            THEN null
    WHEN travel_purpose = 'HR CENTRAL Group'                  THEN null
    WHEN travel_purpose = 'FMTG'                              THEN null

    WHEN travel_purpose = 'Leisure'                           THEN 'Leisure'
    WHEN travel_purpose = 'Repeater'                          THEN 'Leisure'
    WHEN travel_purpose = 'Concierge'                         THEN null
    WHEN travel_purpose = 'Recommendation'                    THEN null

    WHEN travel_purpose = 'Other'                             THEN null
    WHEN travel_purpose = 'Bonus Card direct/FMTG Website'    THEN null
    WHEN travel_purpose = 'Bonus Card Agency/Internet'        THEN null
    WHEN travel_purpose = 'Search Engine/Internet'            THEN null
    WHEN travel_purpose = 'PR/Advertisment'                   THEN null
    WHEN travel_purpose = 'Catalogue'                         THEN null
    WHEN travel_purpose = 'Promotion/CO-Marketing'            THEN null
    WHEN travel_purpose = 'TrustForce'                        THEN null

    ELSE 'Other'
END;



select 
		source,travel_purpose, count(*) as count_records
from mig_raw_crm_reservations_iw
group by travel_purpose, source
order by 3 desc ;

select * 
from mig_raw_crm_reservations_clean
where travel_purpose = 'Account'


/* Correction Reservation Status  */

-- cancelled
/* >>> make sure to normalize status before this <<< */

select * from mig_raw_crm_reservations_clean
where reservation_status <> 'Cancelled'
and cancelled_at is not null;


update mig_raw_crm_reservations_clean
set reservation_status = 'Cancelled'
where reservation_status <> 'Cancelled'
and cancelled_at is not null;

-- noshow

select * from mig_raw_crm_reservations_clean
where reservation_status <> 'NoShow'
and noshow_at is not null;


update mig_raw_crm_reservations_clean
set reservation_status = 'NoShow'
where reservation_status <> 'NoShow'
and noshow_at is not null;

-- missing status

select * from mig_raw_crm_reservations_clean
where reservation_status is null
and departure_at < DATE(SYSDATE())
and source = 'protel'
and _ptable = 'history'

select distinct reservation_status from mig_raw_crm_reservations_clean


;


/* Update SF Property ID */

-- table with uat property ids.
select * from crm_properties_sfid_uat

-- protel
update mig_raw_crm_reservations_clean c
inner join V2D_Property_Attributes pr 
	on pr.PAS_Protel_ID = c.property_protel_id 
inner join crm_properties_sfid_uat p
on p.ApaleoID__c = pr.PAS_code3 
set sf_property_id =  p.Id 
where c.source = 'protel'
and sf_property_id is null;


select * from mig_raw_crm_reservations_clean c
inner join V2D_Property_Attributes pr 
	on pr.PAS_Protel_ID = c.property_protel_id 
inner join crm_properties_sfid_uat p
on p.ApaleoID__c = pr.PAS_code3 
where c.source = 'protel';

select c.property_protel_id, pr.PAS_name_short, count(*) as count_records 
from mig_raw_crm_reservations_clean c
left join V2D_Property_Attributes pr
	on pr.PAS_Protel_ID = c.property_protel_id 
where c.source = 'protel' 
and c.sf_property_id is null
and c.arrival_at >= DATE(SYSDATE())
group by c.property_protel_id, pr.PAS_name_short


-- apaleo
update mig_raw_crm_reservations_clean c
inner join crm_properties_sfid_uat p 
on p.ApaleoID__c = c.property_id 
set sf_property_id =  p.Id 
where c.source = 'apaleo';


select c.property_protel_id, pr.PAS_name_short, count(*) as count_records 
from mig_raw_crm_reservations_clean c
left join V2D_Property_Attributes pr
	on pr.PAS_Protel_ID = c.property_protel_id 
where c.source = 'apaleo' 
and c.sf_property_id is null
and c.arrival_at >= DATE(SYSDATE())
group by c.property_protel_id, pr.PAS_name_short



/* Recover original email for OTA */ 


update mig_raw_crm_reservations_clean c
inner join mig_raw_crm_reservations r
	on r.reservation_id = c.reservation_id 
	and r._ptable = c._ptable 
	and r.source = c.source 
SET c.email = r.email
where c.email is null
 and r.email like '%booking.com'
 
update mig_raw_crm_reservations_clean c
inner join mig_raw_crm_reservations r
	on r.reservation_id = c.reservation_id 
	and r._ptable = c._ptable 
	and r.source = c.source 
SET c.email = r.email
where c.email is null
 and r.email like '%m.expediapartnercentral.com'
 
 
update mig_raw_crm_reservations_clean c
inner join mig_raw_crm_reservations r
	on r.reservation_id = c.reservation_id 
	and r._ptable = c._ptable 
	and r.source = c.source 
SET c.email = r.email
where c.email is null
 and r.email like '%hrs.de'

 
 

select c.email , r.email 
select count(*)
from mig_raw_crm_reservations_clean c
inner join mig_raw_crm_reservations r
	on r.reservation_id = c.reservation_id 
	and r._ptable = c._ptable 
	and r.source = c.source 
where c.email is null
 and r.email like '%booking.com'

select count(*) 
from mig_raw_crm_reservations 


/** select for import reservations on test **/

select *
from mig_raw_crm_reservations_clean c
where c.arrival_at > '2026-04-30' 
and c.arrival_at <= '2027-12-31'
and c.cancelled_at is null
and c.sf_property_id is not null
and not exists (
				select 1 
				from crm_reservation_import_20260322 i 
				where i.reservation_id = c.reservation_id
				)


CREATE TABLE crm_reservation_import_20260407 AS
SELECT 
    ROW_NUMBER() OVER (
        ORDER BY c.reservation_id
    ) AS row_id_new,

	c.*
	from mig_raw_crm_reservations_clean c
	where c.arrival_at > '2026-04-30' 
	and c.arrival_at <= '2027-12-31'
	and c.cancelled_at is null
	and c.sf_property_id is not null
	and not exists (
					select 1 
					from crm_reservation_import_20260322 i 
					where i.reservation_id = c.reservation_id
					)

					
/** Remove invalid emails **/

SELECT 
    reservation_id,
    email
FROM mig_raw_crm_reservations_clean
WHERE arrival_at > '2025-01-01'
  AND email IS NOT NULL
  AND email NOT REGEXP '^[^@\\s,]+@[^@\\s]+\\.[^@\\s]{2,}$';


select count() 
from  mig_raw_crm_reservations_clean
where email like '%m.expedia%'

select * 
from  mig_raw_crm_reservations 
where email like '%guest.booking.com%'

/***********************************/
/****  Add Consent Information   ***/

ALTER TABLE `mig_raw_crm_reservations_iw`
ADD COLUMN `central_consent` tinyint(1) unsigned NOT NULL DEFAULT 0;

ALTER TABLE `mig_raw_crm_reservations_clean`
ADD COLUMN `central_consent` tinyint(1) unsigned NOT NULL DEFAULT 0;


/*** Update central consent hotelbird ***/

SELECT count(*)
FROM mig_raw_crm_reservations_clean r
WHERE EXISTS (
		select 1 
		from raw_hotelbird_newsletter_consent c
		where newsletter_consent = 1
		and c.reservation_id = r.reservation_id
);

UPDATE mig_raw_crm_reservations_clean r
SET central_consent = 1 
WHERE EXISTS (
		select 1 
		from raw_hotelbird_newsletter_consent c
		where newsletter_consent = 1
		and c.reservation_id = r.reservation_id
);


UPDATE mig_raw_crm_reservations_iw r
SET central_consent = 1 
WHERE EXISTS (
		select 1 
		from raw_hotelbird_newsletter_consent c
		where newsletter_consent = 1
		and c.reservation_id = r.reservation_id
);


/*** Update central consent gms profile ***/


select count(*) from  mig_raw_crm_reservations_iw r
WHERE EXISTS (
			select 1 
			from gms_all_profiles gap 
			where gap.current_opt_in = 'Yes'
			and gap.email is not null
			and gap.exclude_email = 0
			and r.email = gap.email 
);


UPDATE mig_raw_crm_reservations_iw r
SET central_consent = 1 
WHERE EXISTS (
			select 1 
			from gms_all_profiles gap 
			where gap.current_opt_in = 'Yes'
			and gap.email is not null
			and gap.exclude_email = 0
			and r.email = gap.email 
);

select 1 
from gms_all_profiles gap 
where gap.current_opt_in = 'Yes'
and gap.email is not null
and gap.exclude_email = 0


/***********************************/
/****  Add Investor Information   ***/

ALTER TABLE `mig_raw_crm_reservations_iw`
ADD COLUMN `is_investor` tinyint(1) unsigned NOT NULL DEFAULT 0;



ALTER TABLE `mig_mapping_investor`
ADD INDEX `idx_email` (`email`);



/*** update investor flag on reservation ***/

update `mig_raw_crm_reservations_clean` r
set r.is_investor = 1
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE r.email = i.email
);


update `mig_raw_crm_reservations_iw` r
set r.is_investor = 1
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE r.email = i.email
);



/***********************************/
/****  Add Cluster IDs   ***/
/* ->> prepare the accounts first <<- */

select * from mig_crm_person_accounts

select * from mig_raw_crm_reservations_clean

explain analyze 
select count(*) 
from mig_raw_crm_reservations_clean r 
inner join mig_crm_person_accounts p
	on r.reservation_id = p.source_id 
	and r.source = p.source 
 
update mig_raw_crm_reservations_clean r 
inner join mig_crm_person_accounts p
	on r.reservation_id = p.source_id 
	and r.source = p.source 
set r.cluster_id = p.cluster_id

update mig_raw_crm_reservations r 
inner join mig_crm_person_accounts p
	on r.reservation_id = p.source_id 
	and r.source = p.source 
set r.cluster_id = p.cluster_id

CREATE INDEX `idx_cluster_id` ON `mig_raw_crm_reservations_clean` (`cluster_id`);
CREATE INDEX `idx_cluster_id` ON `mig_raw_crm_reservations` (`cluster_id`);

/****  Add SF IDs   ***/

select * from crm_person_account_sfid_prod where ClusterID__pc is not null

select count(*)
select *
from mig_raw_crm_reservations_clean r 
inner join crm_person_account_sfid_prod s
	on r.cluster_id  = s.ClusterID__pc 
	

update mig_raw_crm_reservations_clean r 
inner join crm_person_account_sfid_prod s
	on r.cluster_id  = s.ClusterID__pc
set r.sf_person_account_id = s.Id,
	r.sf_person_contact_id = s.PersonContactId 
	
	
update mig_raw_crm_reservations r 
inner join crm_person_account_sfid_prod s
	on r.cluster_id  = s.ClusterID__pc
set r.sf_person_account_id = s.Id,
	r.sf_person_contact_id = s.PersonContactId 




/*** add property id ***/
-- sf_property_id

select * from crm_properties_sfid_prod
select * from mig_raw_crm_reservations  

select * 
from mig_raw_crm_reservations r
inner join crm_properties_sfid_prod p
	on p.ProtelID__c =  r.property_protel_id
where r.source = 'protel'


update mig_raw_crm_reservations r
inner join crm_properties_sfid_prod p
	on p.ProtelID__c =  r.property_protel_id
set r.sf_property_id = p.Id
where r.source = 'protel'


update mig_raw_crm_reservations r
inner join crm_properties_sfid_prod p
	on p.ApaleoID__c  =  r.property_id 
set r.sf_property_id = p.Id
where r.source = 'apaleo'


update mig_raw_crm_reservations_clean r
inner join crm_properties_sfid_prod p
	on p.ProtelID__c =  r.property_protel_id
set r.sf_property_id = p.Id
where r.source = 'protel'


update mig_raw_crm_reservations_clean r
inner join crm_properties_sfid_prod p
	on p.ApaleoID__c  =  r.property_id 
set r.sf_property_id = p.Id
where r.source = 'apaleo'



/*** import query for sf batch ***/


select r.*

from mig_raw_crm_reservations_clean r
inner join mig_crm_person_accounts_imp20260414 p
	on r.cluster_id = p.cluster_id 
where r.sf_person_contact_id is not null 
	and r.sf_property_id is not null
	and r.reservation_status = 'CheckedOut'
	and r.is_investor = 0
	and p.is_investor = 0
	and r.adults_num = 0
	and r.children_num = 0
	
-- history reservertaion ohne invest
CREATE TABLE mig_raw_crm_reservations_hist_imp20260414 AS
SELECT r.*
FROM mig_raw_crm_reservations_clean r
INNER JOIN mig_crm_person_accounts_imp20260414 p
    ON r.cluster_id = p.cluster_id
WHERE r.sf_person_contact_id IS NOT NULL
  AND r.sf_property_id IS NOT NULL
  AND r.reservation_status = 'CheckedOut'
  AND r.is_investor = 0
  AND p.is_investor = 0;


select * from mig_raw_crm_reservations_hist_imp20260414


-- future ohne inverst
drop table mig_raw_crm_reservations_future_imp20260414
CREATE TABLE mig_raw_crm_reservations_future_imp20260414 AS
SELECT r.*
FROM mig_raw_crm_reservations_clean r
INNER JOIN mig_crm_person_accounts_imp20260414 p
    ON r.cluster_id = p.cluster_id
WHERE r.sf_person_contact_id IS NOT NULL
  AND r.sf_property_id IS NOT NULL
  AND r.reservation_status in ('Confirmed', 'InHouse', 'Optional')
  AND r.is_investor = 0
  AND p.is_investor = 0;


-- for test remove sf contact and account ids 
UPDATE   mig_raw_crm_reservations_future_imp20260414
set sf_person_contact_id = null, sf_person_account_id = null

	