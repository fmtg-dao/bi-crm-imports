

/* set checkin, checkout and cancelled, noshow */

UPDATE mig_raw_crm_reservations r
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

UPDATE mig_raw_crm_reservations_clean
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
    WHEN market_segment = 'SPORTGR'            THEN 'SPORTSGR'
    WHEN market_segment = 'Appartment'       THEN 'OTHERS'
    WHEN market_segment = 'Leisure Indiv'    THEN 'INDIVIDUAL'
    ELSE market_segment
END;


select distinct market_segment from mig_raw_crm_reservations_clean;


/* Correction Travel Purpose */ 

-- waiting for matching definition 

UPDATE crm_reservation_import_20260407 
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



select distinct travel_purpose, null as category  
from crm_reservation_import_20260407
-- from mig_raw_crm_reservations_clean
where source = 'protel'

select 
		source,travel_purpose, count(*) as count_records
from mig_raw_crm_reservations_clean
group by travel_purpose, source
order by 3 desc ;

select * 
from mig_raw_crm_reservations_clean
where travel_purpose = 'Account'

/* One Time FIX  for Revenues */ 

update crm_reservation_import_20260407 
set revenue_room = null
where revenue_room = '0.0000'

update crm_reservation_import_20260407 
set revenue_fnb = null
where revenue_fnb = '0.0000'


update crm_reservation_import_20260407 
set revenue_extra = null
where revenue_extra = '0.0000'


update crm_reservation_import_20260407 
set revenue_total = null
where revenue_total = '0.0000'

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

					
/** some other findings **/

		select * from mig_raw_crm_reservations_clean 
		where email like '%alexander.erbach@telekom.de/pn-invoice.tel-it@invoicedtse.telekom.d%'
					
