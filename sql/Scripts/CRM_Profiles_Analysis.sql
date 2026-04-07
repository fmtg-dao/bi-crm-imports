



select * 
from V2V_EXPOSURE_EUDT_OCCUPANCY_DAILY_V1
where  rev_date > '2026-03-12'
and property_id = 'FCA'



select * from pred_gms_golden_records

-- 617201
select * from raw_gms_loyalty_members rglm 
where email = 'anweitzi@t-online.de'


select * from raw_gms_loyalty_members rglm 


select * from pred_gms_entity_clusters
where cluster_id = 357060



select year(created_date), month(created_date), count(*)
from gms_all_profiles
group by year(created_date), month(created_date)


select count(*), count(distinct email)
from gms_all_profiles
where email is not null


select email, count(*)
from gms_all_profiles gap 
where email is not null
group by email having count(*) > 1


select 
SUBSTRING_INDEX(email, '@', -1) AS domain, count(*) anzahl
from gms_all_profiles 
where email is not null
group by SUBSTRING_INDEX(email, '@', -1)
order by 2 desc


select * 
from gms_all_profiles
where email like '%@AEXP.COM'

select datasource_name, count(*)
from gms_all_profiles
group by datasource_name
order by 2 desc


select current_opt_in , count(*)
from gms_all_profiles
group by current_opt_in
order by 2 desc

ALTER TABLE gms_all_profiles
ADD COLUMN domain VARCHAR(255);

update gms_all_profiles
SET domain = SUBSTRING_INDEX(email, '@', -1);



select * 
from gms_all_profiles p
where p.exclude_email   = 0


select * 
from gms_all_profiles p
inner join gms_loyalty_liability l
	on p.list_id  = l.list_id 

	
	
select distinct bounce_flag 
from gms_all_profiles p	
	
select * 
from gms_loyalty_liability l
INNER join gms_all_profiles p
	on p.list_id  = l.list_id 
where l.list_id is null 


select * from gms_loyalty_liability gll where list_id =  400061211 --  402382345

select * from gms_loyalty_liability gll where list_id =  402382345

select * from gms_loyalty_liability gll where list_id =  400230066

/* Migrations Opfer 
 * 
 * nur mit verified_flag = 1  nehmen
 * 418877860
 * 
 *  */
select * from gms_loyalty_liability gll where list_id =  418877860

select l.list_id, count(*) 
from gms_loyalty_liability l
group by l.list_id having count(*) > 1
order by 2 DESC 

select * from gms_all_profiles gap 

select * from gms_opt_in_history goih 



select * from map_segments_apaleo msa ;


select * from  gms_all_profiles gap where email = 'frankmuehleck@yahoo.com'

select * from gms_loyalty_liability gll where list_id = '727855100'


select * 
from gms_loyalty_liability gll
where gll.assigned_member_number like '%.0%'

UPDATE gms_loyalty_liability
SET assigned_member_number = REGEXP_REPLACE(assigned_member_number, '\\.0$', '')
WHERE assigned_member_number REGEXP '\\.0$';


select * from int_crm_reservations


select *  from int_crm_person_accounts a where email = 'frankmuehleck@yahoo.com'

-- 1551440138591

-- 1551440138591.0


select * 
from int_crm_person_accounts a
inner join gms_loyalty_liability l
	on a.member_id  = l.member_number
	and l.verified_flag = 1
	
	
select * 
from int_crm_person_accounts a
where exists (  select * 
				from gms_loyalty_liability l
				where l.verified_flag = 1
				and l.inactive_flag = 0
				and a.member_id  = l.member_number  
			)

			

select *
from int_crm_person_accounts a
where exists (  select * 
				from gms_loyalty_liability l
				where l.verified_flag = 1
				and l.inactive_flag = 0
				and a.member_id  = l.assigned_member_number
			)
and a.has_active_loyalty = 1


select *
from int_crm_person_accounts a 
left join gms_loyalty_liability l 
	on a.member_id  = l.assigned_member_number
where a.has_active_loyalty = 0
and a.member_tier is not null
and a.email = 'stephanie.poller@gmx.at'


select * from int_crm_reservations icr where icr.cluster_id = '17480'


select * 
from gms_reservations gr 
where list_id = 407987535


select * from gms_all_profiles gap 


select *
from int_crm_person_accounts a
where exists (  select * 
				from gms_all_profiles p
				where p.exclude_email = 1
				and a.email  = p.email
			)


			
			
select * from int_crm_person_accounts p


SELECT cluster_id, _entity_id, first_name, last_name, email, birth_date, salutation, gender, middle_name, address, city, postal_code, country, phone, birth_place, nationality, `language`, member_id, member_tier, source_system, has_active_loyalty
FROM int_crm_person_accounts;


select * from int_crm_person_accounts where cluster_id <= 1000 order by cluster_id asc


select * from int_crm_person_accounts where cluster_id =  75078 

select * from crm_person_account_sfid_uat
select * from crm_properties_sfid_uat



select a.cluster_id, i.PersonContactId, a.member_id, a.source_system, date('2026-01-01') as enrollment_date
from int_crm_person_accounts a
inner join crm_person_account_sfid_uat i
	on a.cluster_id = i.ClusterID__pc
where a.has_active_loyalty = 1 and a.member_id = '104233@mail.muni.cz'
order by a.cluster_id asc


update V2D_Property_Attributes
set PAS_FMTG_ID = 'x1180x'
where PAS_code3 = 'FAS'

select * from V2D_Property_Attributes
select * from crm_properties_sfid_uat


a.first_name,
a.last_name,
a.email,
a.birth_date,
a.salutation,
a.gender,
a.middle_name,
a.address,
a.city,
a.postal_code,
a.country,
a.phone,
a.birth_place,
a.nationality,
a.preferred_language


CREATE TABLE crm_reservation_import_20260322 AS
select 
		ROW_NUMBER() OVER (ORDER BY reservation_id) AS row_id,
		r.*,
		CASE 
		    WHEN departure_at > arrival_at 
		    THEN DATEDIFF(departure_at, arrival_at)
		    ELSE 0
		END AS room_nights,
		sp.Id as sf_property_id, 
		c.Id as person_contact_id, 
		a.first_name,
		a.last_name,
		a.email,
		a.birth_date,
		a.salutation,
		a.gender,
		a.middle_name,
		a.address,
		a.city,
		a.postal_code,
		a.country,
		a.phone,
		a.birth_place,
		a.nationality,
		a.preferred_language
from int_crm_reservations r
inner join int_crm_person_accounts a
	on r.cluster_id = a.cluster_id
inner join V2D_Property_Attributes p
	on p.PAS_code3 = r.property_id
inner join crm_properties_sfid_uat sp 
	on p.PAS_FMTG_ID = sp.FMTGID__c 
inner join crm_person_account_sfid_uat c 
	on c.ClusterID__pc = r.cluster_id 
order by cluster_id asc
	

	
CREATE TABLE crm_protel_reservation_events (
    buchnr        VARCHAR(50) NOT NULL,
    mpehotel      VARCHAR(50) NOT NULL,

    checkin_at    DATETIME NULL,
    checkout_at   DATETIME NULL,
    noshow_at     DATETIME NULL,
    cancelled_at  DATETIME NULL,
    deleted_at    DATETIME NULL,

    PRIMARY KEY (buchnr, mpehotel)
);


select * from crm_protel_reservation_events where buchnr = 14979328
select * from  int_crm_reservations r where reservation_id = 14987321
select * from V2D_Property_Attributes 

UPDATE int_crm_reservations r
INNER JOIN crm_protel_reservation_events e
    ON r.reservation_id = e.buchnr

SET
    r.checkin_at   = COALESCE(e.checkin_at, r.checkin_at),
    r.checkout_at  = COALESCE(e.checkout_at, r.checkout_at),
    r.cancelled_at = COALESCE(e.cancelled_at, r.cancelled_at),
    r.noshow_at    = COALESCE(e.noshow_at, r.noshow_at)
WHERE
    r.source = 'protel';


select *
from int_crm_reservations r
WHERE
    r.source = 'protel'
    and checkout_at is null
    and r.departure_at < '2025-01-01'
 
    

    
UPDATE int_crm_reservations r
INNER JOIN (
    SELECT
        buchnr,
        MIN(checkin_at)  AS checkin_at,
        MAX(checkout_at) AS checkout_at
    FROM crm_protel_reservation_events
    GROUP BY buchnr
    HAVING COUNT(*) > 1
) x
    ON x.buchnr = r.reservation_id
SET
    r.checkin_at  = COALESCE(x.checkin_at, r.checkin_at),
    r.checkout_at = COALESCE(x.checkout_at, r.checkout_at)
WHERE r.source = 'protel';
    


ALTER TABLE int_crm_reservations 
ADD COLUMN reservation_status_sf VARCHAR(255);


select distinct reservation_status  from  int_crm_reservations 

Confirmed
InHouse
CheckedOut
Cancelled
NoShow

select * 
from int_crm_reservations r
where r.checkout_at is null
and r.noshow_at is null
and r.cancelled_at is null
and r.source = 'protel'
and r.reservation_status is null
and r.departure_at < '2026-02-28'


update int_crm_reservations r
set r.reservation_status = 'CheckedOut'
where r.checkout_at is null
and r.noshow_at is null
and r.cancelled_at is null
and r.source = 'protel'
and r.reservation_status is null
and r.departure_at < '2026-02-28'
    


select * 
from int_crm_reservations r
where r.checkout_at is null
and r.noshow_at is null
and r.cancelled_at is not null
and r.source = 'protel'
and r.reservation_status is null
and r.departure_at < '2026-02-28'


select * 
from int_crm_reservations r
where r.noshow_at is not null
and r.checkout_at is not null
and r.checkin_at is not null


select distinct market_segment 
from int_crm_reservations r

select * from int_crm_reservations icr 
where icr.market_segment = 'Account'


select distinct r.reservation_status  from int_crm_reservations r
where r.reservation_status is null



select distinct r.market_segment 
from int_crm_reservations r


update crm_reservation_import_20260322 
set person_account_id = person_contact_id


CREATE INDEX idx_res_person_account_id 
ON crm_reservation_import_20260322(person_account_id);

CREATE INDEX idx_person_id 
ON crm_person_account_sfid_uat(id);


update crm_reservation_import_20260322 r
inner join crm_person_account_sfid_uat c 
	on c.id = r.person_account_id 

	set r.person_contact_id = c.PersonContactId
	
	
SELECT * FROM crm_reservation_import_20260322 WHERE row_id <= 5000

select * 

from crm_reservation_import_20260322 r
inner join crm_person_account_sfid_uat c 
	on c.id = r.person_contact_id 

ALTER TABLE crm_person_account_sfid_uat
MODIFY COLUMN id VARCHAR(50);

ALTER TABLE crm_reservation_import_20260322
MODIFY COLUMN person_account_id VARCHAR(50);


CREATE TABLE crm_reservation_import_20260322 AS
SELECT 
    ROW_NUMBER() OVER (
        ORDER BY r.cluster_id ASC, r.reservation_id
    ) AS row_id,

    r.*,

    CASE 
        WHEN departure_at > arrival_at 
        THEN DATEDIFF(departure_at, arrival_at)
        ELSE 0
    END AS room_nights,

    sp.Id as sf_property_id, 
    c.Id as person_contact_id,
    c.

    a.first_name,
    a.last_name,
    a.email,
    a.birth_date,
    a.salutation,
    a.gender,
    a.middle_name,
    a.address,
    a.city,
    a.postal_code,
    a.country,
    a.phone,
    a.birth_place,
    a.nationality,
    a.preferred_language

FROM int_crm_reservations r
INNER JOIN int_crm_person_accounts a
    ON r.cluster_id = a.cluster_id
INNER JOIN V2D_Property_Attributes p
    ON p.PAS_code3 = r.property_id
INNER JOIN crm_properties_sfid_uat sp 
    ON p.PAS_FMTG_ID = sp.FMTGID__c 
INNER JOIN crm_person_account_sfid_uat c 
    ON c.ClusterID__pc = r.cluster_id;




LCR
OTHERS
FIT
MICEGR
OTA
LEISUREGR
SPORTSGR
HOUSE
TIME


select distinct market_segment from crm_reservation_import_20260322

select * from int_crm_reservations where booking_at > arrival_at is null



select distinct `language`  from int_crm_person_accounts


select distinct gap.`domain`  from gms_all_profiles gap where gap.exclude_email = 1


select * 
from Protel_SalesProfiles psp
where psp.Profilenumber = 5654112


UPDATE int_crm_person_accounts
SET preferred_language =
    CASE
        WHEN LOWER(language) IN ('hr','cz','sk','nl','fr','de','en')
            THEN LOWER(language)
        ELSE 'en'
    END;



select * from map_segments_protel msp 

UPDATE crm_reservation_import_20260322
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




select max(length(rate_plan_code))  from int_crm_reservations
