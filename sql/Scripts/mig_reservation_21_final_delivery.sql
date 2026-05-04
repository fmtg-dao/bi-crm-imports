
/*** last update of the production system with reservations as of 2026-05-04 13PM ***/

/* checking for duplicates within the source */ 

SELECT 
    reservation_id, 
    source, 
    COUNT(*) AS row_count,
    GROUP_CONCAT(_ptable) AS ptables
FROM mig_raw_crm_reservations_21_clean
GROUP BY reservation_id, source
HAVING COUNT(*) > 1
LIMIT 20;


/** add update date colum to main reservation table **/
ALTER TABLE mig_raw_crm_reservations_clean
ADD COLUMN last_updated_at DATETIME NULL DEFAULT NULL
    COMMENT 'Timestamp of last data refresh from source (Protel/Apaleo)'
    AFTER sf_person_account_id,
ADD COLUMN is_changed TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'Flag: 1 if reservation has changed since last sync, 0 otherwise'
    AFTER last_updated_at,
ADD INDEX idx_last_updated_at (last_updated_at),
ADD INDEX idx_is_changed (is_changed);

/** add column for invalid emails **/
ALTER TABLE mig_raw_crm_reservations_clean
ADD COLUMN email_invalid_reason VARCHAR(50) DEFAULT NULL
    COMMENT 'Reason if email is invalid: malformed, disposable, bounced, blocklist, etc. NULL = valid'
    AFTER email,
ADD INDEX idx_email_invalid_reason (email_invalid_reason);

ALTER TABLE mig_raw_crm_reservations_clean
ADD COLUMN is_email_invalid TINYINT(1) NOT NULL DEFAULT 0
    COMMENT 'Flag: 1 if email is invalid'
    AFTER email

-- create backup

CREATE TABLE mig_raw_crm_reservations_clean_bkp_20260504 
LIKE mig_raw_crm_reservations_clean;

INSERT INTO mig_raw_crm_reservations_clean_bkp_20260504 
SELECT * FROM mig_raw_crm_reservations_clean;


/* check the excluded */
update mig_raw_crm_reservations_21_clean c
inner join V2D_Property_Attributes pr 
	on pr.PAS_Protel_ID = c.property_protel_id 
SET c.property_id = pr.PAS_code3, c.property_fmtg_id = pr.PAS_FMTG_ID 
where c.source = 'protel' 
and c.property_id is null;


/* check the excluded */

select * 
from mig_raw_crm_reservations_21_clean
where _excluded = 1
order by room_nights desc

/* reset  _excluded  */

UPDATE  
mig_raw_crm_reservations_21_clean 
set  _excluded = 0





/* set checkin, checkout and cancelled, noshow */
select * from mig_raw_crm_reservations_21_clean r
INNER JOIN crm_protel_reservation_events e
    ON r.reservation_id = e.buchnr

UPDATE mig_raw_crm_reservations_21_clean r
INNER JOIN crm_protel_reservation_events e
    ON r.reservation_id = e.buchnr

SET
    r.checkin_at   = COALESCE(r.checkin_at, e.checkin_at),
    r.checkout_at  = COALESCE(r.checkout_at, e.checkout_at),
    r.cancelled_at = COALESCE(r.cancelled_at, e.cancelled_at),
    r.noshow_at    = COALESCE(r.noshow_at, e.noshow_at)
WHERE
    r.source = 'protel';


-- validate 
select * 
from mig_raw_crm_reservations_21_clean 
where source = 'protel'
and departure_at < '2026-05-04'
and reservation_status not in ('Cancelled', 'NoShow')
and checkout_at is null 
order by booking_at desc


/* Update Segments */ 

UPDATE mig_raw_crm_reservations_21_clean
SET market_segment = CASE
    WHEN market_segment = 'Individual'       THEN 'INDIVIDUAL'
    WHEN market_segment = 'Agent/FIT'        THEN 'FIT'
    WHEN market_segment = 'CORP/BUS Indiv'   THEN 'LCR'
    WHEN market_segment = 'Other'            THEN 'OTHERS'
    WHEN market_segment = 'OTHER'            THEN 'OTHERS'
    WHEN market_segment = 'Leisure GRP'      THEN 'LEISUREGR'
    WHEN market_segment = 'MICE/BUS Grp'     THEN 'MICEGR'
    WHEN market_segment = 'Account'          THEN 'OTHERS'
    WHEN market_segment = 'TIME'             THEN 'TIME'
    WHEN market_segment = 'Sport'            THEN 'SPORTSGR'
    WHEN market_segment = 'SPORTGR'          THEN 'SPORTSGR'
    WHEN market_segment = 'Appartment'       THEN 'OTHERS'
    WHEN market_segment = 'Leisure Indiv'    THEN 'INDIVIDUAL'
    ELSE market_segment
END;



select distinct market_segment from mig_raw_crm_reservations_21_clean;


/* correct status of any */

select reservation_status, count(*)
from mig_raw_crm_reservations_21_clean
group by reservation_status


update mig_raw_crm_reservations_21
set reservation_status = 'Cancelled'
where reservation_status <> 'Cancelled'
and cancelled_at is not null;


/* Correction Travel Purpose */ 

-- waiting for matching definition 

UPDATE mig_raw_crm_reservations_21_clean
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



/**  add new reservations to main table **/

-- check what will be inserted
SELECT 
    n.source,
    n._ptable,
    COUNT(*) AS new_rows
FROM mig_raw_crm_reservations_21_clean n
LEFT JOIN mig_raw_crm_reservations_clean c
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source
WHERE c.reservation_id IS NULL
GROUP BY n.source, n._ptable;


-- insert new reservation 
INSERT INTO mig_raw_crm_reservations_clean (
    row_id, cluster_id, _entity_id, _excluded, _ptable,
    reservation_id, source,
    property_id, property_fmtg_id, property_protel_id,
    booking_id, reservation_status, group_name,
    arrival_at, departure_at, booking_at,
    checkin_at, checkout_at, cancelled_at, noshow_at,
    market_segment, market_channel, rate_plan_code,
    booker_company_id, adults_num, children_num,
    unit_group_code, travel_purpose, external_code,
    guest_role, room_nights,
    first_name, middle_name, last_name,
    email, birth_date, salutation, gender,
    preferred_language, address, city, postal_code,
    country, phone, birth_place, nationality,
    revenue_room, revenue_fnb, revenue_extra, revenue_total,
    sf_preferred_language, sf_reservation_status,
    sf_property_id, sf_person_contact_id, sf_person_account_id,
    central_consent, is_investor,
    last_updated_at, is_changed
)
SELECT 
    n.row_id, n.cluster_id, n._entity_id, COALESCE(n._excluded, 0), n._ptable,
    n.reservation_id, n.source,
    n.property_id, n.property_fmtg_id, n.property_protel_id,
    n.booking_id, n.reservation_status, n.group_name,
    n.arrival_at, n.departure_at, n.booking_at,
    n.checkin_at, n.checkout_at, n.cancelled_at, n.noshow_at,
    n.market_segment, n.market_channel, n.rate_plan_code,
    n.booker_company_id, n.adults_num, n.children_num,
    n.unit_group_code, n.travel_purpose, n.external_code,
    n.guest_role, n.room_nights,
    n.first_name, n.middle_name, n.last_name,
    n.email, n.birth_date, n.salutation, n.gender,
    n.preferred_language, n.address, n.city, n.postal_code,
    n.country, n.phone, n.birth_place, n.nationality,
    n.revenue_room, n.revenue_fnb, n.revenue_extra, n.revenue_total,
    n.sf_preferred_language, n.sf_reservation_status,
    n.sf_property_id, n.sf_person_contact_id, n.sf_person_account_id,
    COALESCE(n.central_consent, 0), COALESCE(n.is_investor, 0),
    NOW(),       -- last_updated_at
    0            -- is_changed = 0 (these are NEW, not changed)
FROM mig_raw_crm_reservations_21_clean n
LEFT JOIN mig_raw_crm_reservations_clean c
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source
WHERE c.reservation_id IS NULL;


-- check what was just inserted
SELECT COUNT(*) AS newly_inserted
FROM mig_raw_crm_reservations_clean
WHERE last_updated_at >= CURDATE();

-- Breakdown by source
SELECT 
    source, 
    _ptable,
    COUNT(*) AS rows_inserted_today
FROM mig_raw_crm_reservations_clean
WHERE last_updated_at >= CURDATE()
GROUP BY source, _ptable;

SELECT *
FROM mig_raw_crm_reservations_clean
WHERE last_updated_at >= CURDATE();



/*************************************************************/
/** 		UPDATE EXISTING RESERVATIONS IN MAIN TABLE		**/ 

-- check what has changed 

SELECT COUNT(*) AS rows_with_changes
select *
FROM mig_raw_crm_reservations_clean      c
INNER JOIN mig_raw_crm_reservations_21_clean n
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source
WHERE NOT (
        -- Use <=> (NULL-safe equal) so NULL = NULL counts as equal
        c._ptable             <=> n._ptable
    AND c.booking_id          <=> n.booking_id
    AND c.reservation_status  <=> n.reservation_status
    AND c.group_name          <=> n.group_name
    AND c.arrival_at          <=> n.arrival_at
    AND c.departure_at        <=> n.departure_at
    AND c.booking_at          <=> n.booking_at
    AND c.checkin_at          <=> n.checkin_at
    AND c.checkout_at         <=> n.checkout_at
    AND c.cancelled_at        <=> n.cancelled_at
    AND c.noshow_at           <=> n.noshow_at
    AND c.market_segment      <=> n.market_segment
    AND c.market_channel      <=> n.market_channel
    AND c.rate_plan_code      <=> n.rate_plan_code
    AND c.booker_company_id   <=> n.booker_company_id
    AND c.adults_num          <=> n.adults_num
    AND c.children_num        <=> n.children_num
    AND c.unit_group_code     <=> n.unit_group_code
    AND c.travel_purpose      <=> n.travel_purpose
    AND c.external_code       <=> n.external_code
    AND c.guest_role          <=> n.guest_role
    AND c.room_nights         <=> n.room_nights
    AND c.first_name          <=> n.first_name
    AND c.middle_name         <=> n.middle_name
    AND c.last_name           <=> n.last_name
    AND c.email               <=> n.email
    AND c.birth_date          <=> n.birth_date
    AND c.salutation          <=> n.salutation
    AND c.gender              <=> n.gender
    AND c.preferred_language  <=> n.preferred_language
    AND c.address             <=> n.address
    AND c.city                <=> n.city
    AND c.postal_code         <=> n.postal_code
    AND c.country             <=> n.country
    AND c.phone               <=> n.phone
    AND c.birth_place         <=> n.birth_place
    AND c.nationality         <=> n.nationality
    AND c.revenue_room        <=> n.revenue_room
    AND c.revenue_fnb         <=> n.revenue_fnb
    AND c.revenue_extra       <=> n.revenue_extra
    AND c.revenue_total       <=> n.revenue_total
);


-- sanity check 

SELECT
    SUM(CASE WHEN NOT (c.reservation_status <=> n.reservation_status) THEN 1 ELSE 0 END) AS status_changed,
    SUM(CASE WHEN NOT (c._ptable            <=> n._ptable)            THEN 1 ELSE 0 END) AS ptable_changed,
    SUM(CASE WHEN NOT (c.arrival_at         <=> n.arrival_at)         THEN 1 ELSE 0 END) AS arrival_changed,
    SUM(CASE WHEN NOT (c.departure_at       <=> n.departure_at)       THEN 1 ELSE 0 END) AS departure_changed,
    SUM(CASE WHEN NOT (c.cancelled_at       <=> n.cancelled_at)       THEN 1 ELSE 0 END) AS cancelled_changed,
    SUM(CASE WHEN NOT (c.checkin_at         <=> n.checkin_at)         THEN 1 ELSE 0 END) AS checkin_changed,
    SUM(CASE WHEN NOT (c.checkout_at        <=> n.checkout_at)        THEN 1 ELSE 0 END) AS checkout_changed,
    SUM(CASE WHEN NOT (c.revenue_total      <=> n.revenue_total)      THEN 1 ELSE 0 END) AS revenue_changed,
    SUM(CASE WHEN NOT (c.email              <=> n.email)              THEN 1 ELSE 0 END) AS email_changed,
    SUM(CASE WHEN NOT (c.first_name         <=> n.first_name)         THEN 1 ELSE 0 END) AS first_name_changed,
    SUM(CASE WHEN NOT (c.last_name          <=> n.last_name)          THEN 1 ELSE 0 END) AS last_name_changed,
    SUM(CASE WHEN NOT (c.adults_num         <=> n.adults_num)         THEN 1 ELSE 0 END) AS adults_changed,
    SUM(CASE WHEN NOT (c.children_num       <=> n.children_num)       THEN 1 ELSE 0 END) AS children_changed
FROM mig_raw_crm_reservations_clean      c
INNER JOIN mig_raw_crm_reservations_21_clean n
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source;


SELECT 
    c.reservation_id,
    c.source,
    c._ptable,
    'OLD (clean)' AS version,
    c.email,
    c.first_name,
    c.last_name,
    c.last_updated_at
FROM mig_raw_crm_reservations_clean      c
INNER JOIN mig_raw_crm_reservations_21_clean n
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source
WHERE NOT (c.email <=> n.email)

UNION ALL

SELECT 
    n.reservation_id,
    n.source,
    n._ptable,
    'NEW (21_clean)' AS version,
    n.email,
    n.first_name,
    n.last_name,
    NULL AS last_updated_at
FROM mig_raw_crm_reservations_clean      c
INNER JOIN mig_raw_crm_reservations_21_clean n
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source
WHERE NOT (c.email <=> n.email)

ORDER BY reservation_id, source, version DESC;


/*** UPDATE RESERVATION MAIN TABLE ***/

UPDATE mig_raw_crm_reservations_clean      c
INNER JOIN mig_raw_crm_reservations_21_clean n
    ON  c.reservation_id = n.reservation_id
    AND c.source         = n.source
SET
    c._ptable             = n._ptable,
    c.booking_id          = n.booking_id,
    c.reservation_status  = n.reservation_status,
    c.group_name          = n.group_name,
    c.arrival_at          = n.arrival_at,
    c.departure_at        = n.departure_at,
    c.booking_at          = n.booking_at,
    c.checkin_at          = n.checkin_at,
    c.checkout_at         = n.checkout_at,
    c.cancelled_at        = n.cancelled_at,
    c.noshow_at           = n.noshow_at,
    c.market_segment      = n.market_segment,
    c.market_channel      = n.market_channel,
    c.rate_plan_code      = n.rate_plan_code,
    c.booker_company_id   = n.booker_company_id,
    c.adults_num          = n.adults_num,
    c.children_num        = n.children_num,
    c.unit_group_code     = n.unit_group_code,
    c.travel_purpose      = n.travel_purpose,
    c.external_code       = n.external_code,
    c.guest_role          = n.guest_role,
    c.room_nights         = n.room_nights,
    c.first_name          = n.first_name,
    c.middle_name         = n.middle_name,
    c.last_name           = n.last_name,
    c.email               = n.email,
    c.birth_date          = n.birth_date,
    c.salutation          = n.salutation,
    c.gender              = n.gender,
    c.preferred_language  = n.preferred_language,
    c.address             = n.address,
    c.city                = n.city,
    c.postal_code         = n.postal_code,
    c.country             = n.country,
    c.phone               = n.phone,
    c.birth_place         = n.birth_place,
    c.nationality         = n.nationality,
    c.revenue_room        = n.revenue_room,
    c.revenue_fnb         = n.revenue_fnb,
    c.revenue_extra       = n.revenue_extra,
    c.revenue_total       = n.revenue_total,
    c.last_updated_at     = NOW(),
    c.is_changed          = 1
WHERE NOT (
        c._ptable             <=> n._ptable
    AND c.booking_id          <=> n.booking_id
    AND c.reservation_status  <=> n.reservation_status
    AND c.group_name          <=> n.group_name
    AND c.arrival_at          <=> n.arrival_at
    AND c.departure_at        <=> n.departure_at
    AND c.booking_at          <=> n.booking_at
    AND c.checkin_at          <=> n.checkin_at
    AND c.checkout_at         <=> n.checkout_at
    AND c.cancelled_at        <=> n.cancelled_at
    AND c.noshow_at           <=> n.noshow_at
    AND c.market_segment      <=> n.market_segment
    AND c.market_channel      <=> n.market_channel
    AND c.rate_plan_code      <=> n.rate_plan_code
    AND c.booker_company_id   <=> n.booker_company_id
    AND c.adults_num          <=> n.adults_num
    AND c.children_num        <=> n.children_num
    AND c.unit_group_code     <=> n.unit_group_code
    AND c.travel_purpose      <=> n.travel_purpose
    AND c.external_code       <=> n.external_code
    AND c.guest_role          <=> n.guest_role
    AND c.room_nights         <=> n.room_nights
    AND c.first_name          <=> n.first_name
    AND c.middle_name         <=> n.middle_name
    AND c.last_name           <=> n.last_name
    AND c.email               <=> n.email
    AND c.birth_date          <=> n.birth_date
    AND c.salutation          <=> n.salutation
    AND c.gender              <=> n.gender
    AND c.preferred_language  <=> n.preferred_language
    AND c.address             <=> n.address
    AND c.city                <=> n.city
    AND c.postal_code         <=> n.postal_code
    AND c.country             <=> n.country
    AND c.phone               <=> n.phone
    AND c.birth_place         <=> n.birth_place
    AND c.nationality         <=> n.nationality
    AND c.revenue_room        <=> n.revenue_room
    AND c.revenue_fnb         <=> n.revenue_fnb
    AND c.revenue_extra       <=> n.revenue_extra
    AND c.revenue_total       <=> n.revenue_total
);



/*****************************************/
/** 		CLEAN MAIN TABLE AGAIN		**/ 

/* remove appartment owner  */
update`mig_raw_crm_reservations_clean` cr
set _excluded = 1
where cr.rate_plan_code in ('OWN', 'APP', 'APPET', 'APP')
and _excluded = 0


/* correct status of any */

select reservation_status, count(*)
from mig_raw_crm_reservations_clean
group by reservation_status

-- optional status excluded
update mig_raw_crm_reservations_clean
set _excluded = 1 
where reservation_status = 'Optional'
and _excluded = 0

-- optional status excluded - fix existing res
update mig_raw_crm_reservations_clean
set _excluded = 0 
where reservation_status = 'Optional'
and _excluded = 1 and sf_reservation_id is not null

-- check status
select reservation_status, count(*)
from  mig_raw_crm_reservations_clean
where _excluded = 0 
group by reservation_status


select property_id, count(*) 
from mig_raw_crm_reservations_clean
where _excluded = 0 
group by property_id 

select * from crm_properties_sfid_prod

-- set sf property ids 

update mig_raw_crm_reservations_clean c
inner join crm_properties_sfid_prod p
on p.ApaleoID__c = c.property_id 
set c.sf_property_id =  p.Id 
where sf_property_id is null;

-- remove reservation without property id 
update mig_raw_crm_reservations_clean c
set _excluded = 1
where sf_property_id is null
and c.sf_reservation_id is not null

-- check for prod reservations without property mapping
select * 
FROM   mig_raw_crm_reservations_clean c
where sf_property_id is null
and c.sf_reservation_id is not null


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


-- remove new cancelled reservation 
update mig_raw_crm_reservations_clean res
set _excluded = 1
where reservation_status in ('Cancelled', 'NoShow')
and _excluded = 0 
and res.sf_reservation_id is null 


/* Central Consent */ 

UPDATE mig_raw_crm_reservations_clean r
SET central_consent = 1 
WHERE EXISTS (
		select 1 
		from raw_hotelbird_newsletter_consent c
		where newsletter_consent = 1
		and c.reservation_id = r.reservation_id
)
AND central_consent = 0 
;


/** EMAIL VALIDATATION AND REPAIR **/

-- check for invalid emails 
SELECT 
    reservation_id,
    email
FROM  mig_raw_crm_reservations_clean
WHERE  _excluded = 0
  AND email IS NOT NULL
  AND email NOT REGEXP '^[^@\\s,]+@[^@\\s]+\\.[^@\\s]{2,}$'
  AND sf_reservation_id is not null
  

  
-- check what will be updated
SELECT COUNT(*) AS rows_to_flag
-- SELECT *
FROM mig_raw_crm_reservations_clean
WHERE email IS NOT NULL
  AND TRIM(email) <> ''
  AND (
        LOWER(email) NOT REGEXP '^[^@[:space:],]+@[^@[:space:]]+\\.[^@[:space:]]{2,}$'
        OR email LIKE '.%'
        OR email LIKE '%.@%'
        OR email LIKE '%@.%'
        OR email LIKE '%..%'
        OR email LIKE '%@%@%'
  )
  AND is_email_invalid = 0;

-- UPDATE flags
UPDATE mig_raw_crm_reservations_clean
SET is_email_invalid = 1, 
    email_invalid_reason = 'malformed'
WHERE email IS NOT NULL
  AND TRIM(email) <> ''
  AND (
        LOWER(email) NOT REGEXP '^[a-z0-9._+-]+@[a-z0-9.-]+\\.[a-z]{2,}$'
        OR email LIKE '.%'
        OR email LIKE '%.@%'
        OR email LIKE '%@.%'
        OR email LIKE '%..%'
        OR email LIKE '%@%@%'
  )
  AND is_email_invalid = 0;
  
 



/** CREATE TABLE FOR IMPORT OF NEW RESERVATIONS  **/


CREATE TABLE mig_raw_crm_reservations_21_clean_new_imp20260504
LIKE mig_raw_crm_reservations_clean;


INSERT INTO mig_raw_crm_reservations_21_clean_new_imp20260504
SELECT *
FROM mig_raw_crm_reservations_clean
WHERE last_updated_at >= '2026-05-04'
  AND _excluded = 0;


select count(*) from mig_raw_crm_reservations_21_clean_new_imp20260504
where market_segment = 'OTHERS'


select * from mig_raw_crm_reservations_21_clean_new_imp20260504
where email = 'zvonikca7(at9@gmail.com'

/* POST IMPORT */

-- FIRST update local reservation data
-- set reservation sf id 
update mig_raw_crm_reservations_clean r
inner join crm_reservation_sfid_prod s
	on r.reservation_id = s.ReservationID__c
set r.sf_reservation_id = s.Id
where r.sf_reservation_id is null

update mig_raw_crm_reservations_21_clean_new_imp20260504 r
inner join crm_reservation_sfid_prod s
	on r.reservation_id = s.ReservationID__c
set r.sf_reservation_id = s.Id
where r.sf_reservation_id is null



select *
from mig_raw_crm_reservations_clean r
where r.reservation_status = 'Confirmed' 
and r.sf_reservation_id is null
and r._ptable = 'reservation'
-- and exists( select 1 from V2D_Property_Attributes p where p.pas_code3 = r.property_id and pas_pms = 'protel' and p.is_active = 1)
and r.departure_at >= '2026-05-03' 
and r._excluded = 0




select * from V2D_Property_Attributes p where p.pas_code3 = r.property_id and pas_pms = 'protel'

select * 
from crm_reservation_sfid_prod
where ReservationStatus__c = 'Confirmed' 
