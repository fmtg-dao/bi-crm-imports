

/***** Create cleaning fields ****/ 

ALTER TABLE gms_all_profiles
ADD COLUMN domain VARCHAR(255);

ALTER TABLE gms_all_profiles
ADD COLUMN exclude_email TINYINT(1) DEFAULT 0;


update gms_all_profiles
SET domain = SUBSTRING_INDEX(email, '@', -1);



/* exclude record without email */

update gms_all_profiles
SET exclude_email = 1
where email is null;2


/* exclude record temp emails */




update gms_all_profiles
SET exclude_email = 1
where domain in ('GUEST.BOOKING.COM', 'm.expediapartnercentral.com')



/* exclude record with bounces */

select distinct current_opt_in from gms_all_profiles gap and


select count(*) from gms_all_profiles where exclude_email = 0 and bounce <> 'No'  and bounce_flag > 1


update gms_all_profiles
SET exclude_email = 1
where bounce_flag > 1


/* exclude hard opt outs */


select distinct tag_list from gms_tags where tag_list like '%FMTG_2_optout%'

240319_FMTG_2_optout, 240320_FMTG_2_optout



/*   Target Group */


select 
	count(distinct email )
from gms_all_profiles 
where email is not null
and exclude_email = 0 and bounce = 0 and current_opt_in = 'Yes'



select 
	domain, count(*) anzahl
from gms_all_profiles 
where email is not null
and exclude_email = 0 and bounce = 0 and current_opt_in = 'Yes'
group by domain
order by 2 desc


SELECT 
    domain,
    COUNT(*) AS anzahl,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS anzahl_pct
FROM gms_all_profiles 
WHERE email IS NOT NULL
  AND exclude_email = 0 
  AND bounce = 0 
  AND current_opt_in = 'Yes'
GROUP BY domain
ORDER BY anzahl DESC;




/*  High Value Guest  */

with res as (
select list_id, currency_code, round(sum(res.revenue),0) as revenue_ccy  
from gms_reservations res
where res.cancelled = 'No'
and res.no_show = 'No'
group by list_id, currency_code 
order by 3 desc
),


loy as (

select gll.list_id, max(tier) as tier
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0 
and gll.points > 0 
group by gll.list_id

)



select * 
from gms_all_profiles gap
inner join loy 
	on loy.list_id = gap.list_id 
left join res
	on res.list_id = gap.list_id
where res.revenue_ccy > 40000








