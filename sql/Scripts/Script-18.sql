



select count(*) from mig_raw_crm_reservations_21_clean


select * from mig_raw_crm_reservations_21_clean


select * from mig_raw_crm_reservations_21_clean where email like '%booking%'

select length(email), email from mig_raw_crm_reservations_21_clean order by 1 desc

select length(city), city from mig_raw_crm_reservations_21_clean order by 1 desc

select birth_date, count(*) 
from mig_raw_crm_reservations_21_clean 
group by birth_date 
order by 2 desc



select distinct market_channel from mig_raw_crm_reservations_clean where market_channel is null

select source,market_channel, market_segment, count(*)
from mig_raw_crm_reservations_clean
group by market_channel, source, market_segment
order by 2 desc


select rar.channel_code, rar.market_segment__id,  count(*) 
from raw_apaleo_reservations rar
where rar.created > '2026-04-01'
 group by channel_code, rar.market_segment__id


where rar.channel_code  = 'ChannelManager'



select property_id, unit_group_code, count(*) 
from mig_raw_crm_reservations_clean
where unit_group_code like '%ACC%'
group by unit_group_code ,property_id 


select rate_plan_code , count(*) 
from mig_raw_crm_reservations_clean 
where 1=1 --property_protel_id in (14, 37, 40)
and rate_plan_code in ('OWN', 'APP')
group by rate_plan_code 


select *
from mig_raw_crm_reservations_clean 
where lower(last_name) like '%svetits%'

where lower(rate_plan_code) like '%app%'
and source = 'protel'
 ('OWN', 'APP')
group by rate_plan_code 

/* --> apaleo entfernen APPET und APP  */

select * from V2D_Property_Attributes

select * from mig_raw_crm_reservations_clean where booking_id = 'PL1561297822622P37'

select * 
from mig_raw_crm_reservations_iw
where reservation_id = '14909697'

select * 
from mig_raw_crm_reservations_clean
where external_code = '827329986'

select * 
from mig_raw_crm_reservations_iw
where external_code = '908523125'

select count(*)
FROM mig_raw_crm_reservations_clean 
where source = 'protel' 
and arrival_at > '2024-09-22'
and property_protel_id = 28
and sf_reservation_id is not null


with cte as (

select external_code, count(distinct source)
from mig_raw_crm_reservations_clean r
 inner join crm_reservation_sfid_prod s 
 	on s.ReservationID__c = r.reservation_id
 	-- and s.Lead__c is not null
group by external_code having count(distinct source) > 1
)

select count(*) from cte
 

