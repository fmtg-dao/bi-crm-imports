

select * from raw_apaleo_reservations rar where id = 'GWUKOHKR-1'

select * from mig_raw_crm_reservations_clean where rate_plan_code like 'FIT%'




select source, _ptable, count(*) 
from mig_raw_crm_reservations_iw
group by source, _ptable

select count(*) from V2I_RateCodeOverview virco 

select source, reservation_status, count(*) 
from  mig_raw_crm_reservations_iw
group by source, reservation_status 


-- 3381578
select count(*) 
from mig_raw_crm_reservations_clean  

-- 3381578
select *
from mig_raw_crm_reservations_iw


select * 
from mig_raw_crm_reservations_clean  
where source = 'apaleo'

select * 
from mig_raw_crm_reservations_clean  
where email like '%m.expedia%'

select * 
from mig_raw_crm_reservations_clean  
where reservation_id = '10154015'

select * 
from mig_raw_crm_reservations_iw  
where email like '%m.expedia%'


select * 
from mig_raw_crm_reservations_iw  
where email like '%zentrales.reservierungssystem-porsche@porsche.co.at%'



select reservation_id, email, length(email) as lng
from mig_raw_crm_reservations_clean  
order by 3 desc


SELECT 
    reservation_id,
    email
FROM mig_raw_crm_reservations_clean
WHERE arrival_at > '2025-01-01'
  AND email IS NOT NULL
  AND email NOT REGEXP '^[^@\\s,]+@[^@\\s]+\\.[^@\\s]{2,}$';




select birth_date, count(*) 
from mig_raw_crm_reservations_clean
group by birth_date 
order by 2 desc





select count(*) 
from mig_raw_crm_reservations_clusters
where cluster_id  = 83598


select source, count(*)
from mig_raw_crm_reservations_clusters
group by source

select *
from mig_raw_crm_reservations_clusters

mig_crm_person_accounts

select * from int_crm_person_accounts

select * from gms_all_profiles gap 

select * from gms_all_profiles gap 

select 
		PAS_FMTG_ID as fmtg_id,
		PAS_code3 as apaleo_id,
		PAS_name_short as name_short,
		PAS_name_long  as name_long,
		PAS_Protel_ID as protel_id,
		PAS_pms as pms
		
from V2D_Property_Attributes vdpa
where is_active = 1


