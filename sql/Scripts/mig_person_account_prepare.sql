

/***** Create cleaning fields ****/ 

ALTER TABLE gms_all_profiles
ADD COLUMN domain VARCHAR(255);

ALTER TABLE gms_all_profiles
ADD COLUMN exclude_email TINYINT(1) DEFAULT 0;

ALTER TABLE `gms_all_profiles`
ADD COLUMN `cluster_id` VARCHAR(100) NULL DEFAULT NULL;

CREATE INDEX `idx_cluster_id` ON `gms_all_profiles` (`cluster_id`);


ALTER TABLE `gms_loyalty_liability`
ADD COLUMN `cluster_id` VARCHAR(100) NULL DEFAULT NULL;

CREATE INDEX `idx_cluster_id` ON `gms_loyalty_liability` (`cluster_id`);

update gms_all_profiles
SET domain = SUBSTRING_INDEX(email, '@', -1);


CREATE INDEX `idx_list_id` ON `gms_loyalty_liability` (`list_id`(255));
CREATE INDEX `idx_list_id` ON `gms_all_profiles` (`list_id`(255));

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




/***************************************/
/***    PMS Profiles      **************/



select * from mig_crm_person_accounts 

/***    create an identity table     **************/

CREATE TABLE mig_crm_person_accounts_imp20260414 AS
WITH ranked AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.cluster_id
            ORDER BY
                CASE
                    WHEN t.source = 'gms' THEN 0
                    WHEN t.source IN ('protel', 'apaleo') THEN 1
                    ELSE 2
                END,
                t.complete_fields DESC,
                CASE
                    WHEN t._rule_bin = 1 THEN 0
                    WHEN t._rule_bin = 0 THEN 1
                    ELSE 2
                END,
                t.source ASC,
                t.source_id ASC
        ) AS rn
    FROM mig_crm_person_accounts t
    WHERE t.cluster_id IS NOT NULL
      -- AND t.complete_fields = 4
      AND cluster_last_name is not null
      AND cluster_email is not null
      AND cluster_first_name is not null
      AND t._rule_bin IN (0, 1)
)
SELECT
    *
FROM ranked
WHERE rn = 1;


ALTER TABLE `mig_crm_person_accounts_imp20260414`
ADD COLUMN `central_consent` tinyint(1) unsigned NOT NULL DEFAULT 0;

ALTER TABLE `mig_crm_person_accounts_imp20260414`
ADD COLUMN `is_investor` tinyint(1) unsigned NOT NULL DEFAULT 0;

ALTER TABLE `mig_crm_person_accounts_imp20260414`
ADD COLUMN `sf_contact_id` VARCHAR(255) NULL DEFAULT NULL;

ALTER TABLE `mig_crm_person_accounts_imp20260414`
ADD COLUMN `sf_account_id` VARCHAR(255) NULL DEFAULT NULL;

ALTER TABLE `mig_crm_person_accounts_imp20260414`
ADD COLUMN `member_number_new` VARCHAR(255) NULL DEFAULT NULL;

ALTER TABLE `mig_crm_person_accounts_imp20260414`
ADD COLUMN `enrollment_date` DATE NULL DEFAULT NULL;


CREATE INDEX `idx_cluster_email` ON `mig_crm_person_accounts_imp20260414` (`cluster_email`);
CREATE INDEX `idx_cluster_id` ON `mig_crm_person_accounts_imp20260414` (`cluster_id`);

CREATE INDEX `idx_cluster_id` ON `mig_crm_person_accounts_imp20260414` (`cluster_id`);


select *  from mig_crm_person_accounts_imp20260414

/***    check  identity table     **************/

 select cluster_id, count(*)
 from mig_crm_person_accounts_imp20260414
 group by cluster_id having count(*) > 1
 
/***    remove dups 				************/
 select count(*) 
 from mig_crm_person_accounts_imp20260414 t
 where exists (
	 select 1 from (
	 select  cluster_email, count(*)
	 from mig_crm_person_accounts_imp20260414
	 group by cluster_email having count(*) > 1
 	) a where a.cluster_email =  t.cluster_email
 )
 
 
 delete
 from mig_crm_person_accounts_imp20260414 t
 where exists (
	 select 1 from (
	 select  cluster_email, count(*)
	 from mig_crm_person_accounts_imp20260414
	 group by cluster_email having count(*) > 1
 	) a where a.cluster_email =  t.cluster_email
 )
 
 
 /***    flag central_consent		************/
 
 select count(*) 
 from  mig_crm_person_accounts_imp20260414 p
	WHERE EXISTS (
				select 1 
				from gms_all_profiles gap 
				where gap.current_opt_in = 'Yes'
				and gap.email is not null
				and gap.exclude_email = 0
				and p.cluster_email = gap.email 
	);
 
 
 UPDATE mig_crm_person_accounts_imp20260414 p
 set central_consent = 1
	WHERE EXISTS (
				select 1 
				from gms_all_profiles gap 
				where gap.current_opt_in = 'Yes'
				and gap.email is not null
				and gap.exclude_email = 0
				and p.cluster_email = gap.email 
	);
 
 
  /***    flag central_consent		************/
 
 select count(*) 
 from  mig_crm_person_accounts_imp20260414 p
	WHERE EXISTS (
				select 1 
				from gms_all_profiles gap 
				where gap.current_opt_in = 'Yes'
				and gap.email is not null
				and gap.exclude_email = 0
				and p.cluster_email = gap.email 
	);
 
 
   /***   delete excluded contacts ************/
 
 select count(*)
 select *
 from  mig_crm_person_accounts_imp20260414 p
	WHERE EXISTS (
				select 1 
				from gms_all_profiles gap 
				where gap.exclude_email = 1
				and p.cluster_email = gap.email 
	);
 
 select * from gms_all_profiles 
 where email in ('vesel.vesna1@gmail.com', 'koblizek@drivecompany.at', 'damir.jurecic@cprz.hr')

 
 delete from mig_crm_person_accounts_imp20260414 p
	WHERE EXISTS (
				select 1 
				from gms_all_profiles gap 
				where gap.exclude_email = 1
				and p.cluster_email = gap.email 
	);
 
 
 select * from mig_crm_person_accounts_imp20260414 
 
 
 /***    flag investor		************/
 
 
select *
from mig_crm_person_accounts_imp20260414 p
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE p.cluster_email = i.email
);
 
 
 
update mig_crm_person_accounts_imp20260414 p
set p.is_investor = 1
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE p.cluster_email = i.email
);


/***    delete investors for ip warming		************/
 
 delete from mig_crm_person_accounts_imp20260414 
 where is_investor = 1
  
 select * 
 from mig_crm_person_accounts_imp20260414
 where is_investor = 1
 
 
/*** create member id  ***/
 
 UPDATE mig_crm_person_accounts_imp20260414
 set member_number_new = 358000360 + cluster_id
 
 select member_number_new, a.* 
 FROM  mig_crm_person_accounts_imp20260414 a
 order by member_number_new desc
 
select count(*) from (
select gll.list_id, max(tier) as tier
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0 
and gll.points > 0 
group by gll.list_id
) s
 
 /*******************************************/
 /****     SF IDs            ***************/
 /****  ->> fist download sf accounts <<-- **/
 /****  Add SF IDs   ***/
 
CREATE INDEX `idx_ClusterID__pc` ON `crm_person_account_sfid_prod` (`ClusterID__pc`(40));
 
 

select * from crm_person_account_sfid_prod where ClusterID__pc is not null
 
 select count(*) 
 from mig_crm_person_accounts_imp20260414 p
 inner join crm_person_account_sfid_prod s
 	on p.cluster_id = s.ClusterID__pc 
 
 
 update mig_crm_person_accounts_imp20260414 p
 inner join crm_person_account_sfid_prod s
  	on p.cluster_id = s.ClusterID__pc
 set p.sf_contact_id = s.PersonContactId,
 p.sf_account_id = s.Id

 select * from mig_crm_person_accounts_imp20260414 p
 
 /*** delete not imported accounts ****/
 
 select count(*) from mig_crm_person_accounts_imp20260414 p where p.sf_account_id is null;
 -- delete from mig_crm_person_accounts_imp20260414 p where p.sf_account_id is null;
 
/***************************************/
/***    Loyalty Members      **************/
 
 /*   set cluster_id in gms all profiles */

select count(*) 
from gms_all_profiles g
inner join mig_crm_person_accounts_imp20260414 p
	on p.cluster_email = g.email 
	
	
update gms_all_profiles g
inner join mig_crm_person_accounts_imp20260414 p
	on p.cluster_email = g.email 
 set g.cluster_id = p.cluster_id
 
 
  /*   set cluster_id in gms_loyalty_liability */
 
 select count(*) 
 from gms_loyalty_liability l
 inner join gms_all_profiles g
 	on l.list_id = g.list_id
 	
 	
 update gms_loyalty_liability l
 inner join gms_all_profiles g
 	on l.list_id = g.list_id
 set l.cluster_id = g.cluster_id
 
 select * from gms_loyalty_liability
 
 /* set member id, member tier, enrollment date on mig_crm_person_accounts_imp20260414  */
 
 -- remove existing values
UPDATE mig_crm_person_accounts_imp20260414 a
 set a.member_id = null, a.member_tier = null
 
 
UPDATE mig_crm_person_accounts_imp20260414 a
INNER JOIN (
    WITH ranked AS (
        SELECT 
            cluster_id,
            start_date,
            tier,
            member_number,
            lasttransactiondate,
            points,
            ROW_NUMBER() OVER (
                PARTITION BY cluster_id
                ORDER BY 
                    COALESCE(lasttransactiondate, '1900-01-01') DESC,
                    COALESCE(points, 0) DESC,
                    tier DESC
            ) AS rn
        FROM gms_loyalty_liability
        WHERE verified_flag = 1
          AND inactive_flag = 0
          AND member_number IS NOT NULL
          AND cluster_id IS NOT NULL
    )
    SELECT 
        cluster_id,
        member_number,
        tier,
        start_date
    FROM ranked
    WHERE rn = 1
) b ON a.cluster_id = b.cluster_id
SET 
    a.member_id       = b.member_number,
    a.member_tier     = b.tier,
    a.enrollment_date = b.start_date;
 
 -- update missing enrollment date with gms create date
 
 select * from gms_all_profiles gap 
 
 -- 648972
 update mig_crm_person_accounts_imp20260414 p
 inner join gms_all_profiles g
 	on g.cluster_id = p.cluster_id 
 set p.enrollment_date =  coalesce(p.enrollment_date, g.created_date)
 where p.member_tier is not null 
 and p.enrollment_date is null
 
 select count(*) 
 from mig_crm_person_accounts_imp20260414 p
 where p.member_tier is not null 
 and p.enrollment_date is null
 
 
 -- check tiers 
 
 select member_tier, count(*)
 from mig_crm_person_accounts_imp20260414 p
 group by member_tier
 
 -- check with source changes -- 
 
 select count(*)
 from mig_crm_person_accounts_imp20260414 p
 where source = 'gms'
 and p.source_id <> p.member_id 
 
 select *
 from mig_crm_person_accounts_imp20260414 p
 where source = 'gms'
 and p.source_id <> p.member_id 
 
 
 /****** import query for sf batch *****/
 
 
  select  
  		cluster_id,
  		sf_contact_id,
  		source,
  		member_id as legacy_member_number,
  		member_tier,
  		member_number_new,
  		enrollment_date
		
  from mig_crm_person_accounts_imp20260414 
  where member_tier is not null 
  and sf_contact_id is not null 
  -- and member_number_new = 358000362

 
/***************************************/
/***    Other stuff      **************/
 
/*   Target Group */

select * from mig_crm_person_accounts where cluster_email = 'markus.baier@gmx.de';
select * from mig_crm_person_accounts where cluster_id = '438716'
select * from mig_raw_crm_reservations_clean where email  = 'markus.baier@gmx.de'
select * from gms_all_profiles where email = 'markus.baier@gmx.de'
 
select * from  mig_crm_person_accounts_imp20260414 where member_id is not null
select count(*) from  mig_crm_person_accounts_imp20260414 where member_id is not null
select count(*) from  mig_crm_person_accounts_imp20260414 where source = 'gms'
select count(*) from  mig_crm_person_accounts where source = 'gms';
select count(*) from gms_all_profiles gap where gap.exclude_email = 0

select email, count(*) 
from gms_all_profiles
group by email having count(*) >1 

select * 
from gms_all_profiles g
inner join mig_crm_person_accounts_imp20260414 p
	on p.cluster_email = g.email 


WITH ranked AS (
    SELECT 
        cluster_id,
        start_date,
        tier,
        member_number,
        lasttransactiondate,
        points,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id
            ORDER BY 
                COALESCE(lasttransactiondate, '1900-01-01') DESC,
                COALESCE(points, 0) DESC,
                tier DESC
        ) AS rn
    FROM gms_loyalty_liability
    WHERE verified_flag = 1
      AND inactive_flag = 0
      AND member_number IS NOT NULL
      AND cluster_id IS NOT NULL
)
SELECT 
    cluster_id,
    start_date,
    tier,
    member_number,
    lasttransactiondate,
    points
FROM ranked
WHERE rn = 1
	
	
select * from gms_all_profiles gap 

select gll.list_id, max(tier) as tier
select *   
from gms_loyalty_liability gll

select *
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0 
and gll.member_number is null



select member_number, count(*)
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0
and gll.member_number is not null
group by member_number having count(*) > 1

select *
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0
and gll.member_number is not null
and gll.email is null


select gll.list_id, count(*) 
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0
and gll.member_number is not null
and gll.email is not null
group by gll.list_id having count(*) > 1

select * from gms_loyalty_liability gll  where list_id = 400230066

select * 
from gms_loyalty_liability gll 
where gll.verified_flag = 1
and gll.inactive_flag = 0
and gll.member_number is not null
and gll.email is not null


 select * from gms_all_profiles gap where list_id = '425619762'
 
 select * from gms_loyalty_liability where list_id = '425619762'
 
 select * from mig_crm_person_accounts mcpa where mcpa.cluster_email = 'kreisl.irmgard@gmx.at'

select * from gms_loyalty_liability where lasttransactiondate is null member_number = 'cenzino.1986@gmail.com'

/*
 *  rockastrid33@gmail.com
	andreaschen@gmx.at
	michal.piwek@gmail.com
	cenzino.1986@gmail.com
 */
 
 
/*   Target Group */

select * from mig_crm_person_accounts_imp20260414 order by cluster_id asc limit 10
 select count(*) from mig_crm_person_accounts_imp20260414
 select * from mig_crm_person_accounts_imp20260414 where clean_email is null
 
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



select count(distinct email) 
from mig_raw_crm_reservations_clean
where is_investor = 0
and central_consent = 1

select count(*) 
from mig_crm_person_accounts
where _outcome = 'NEW'
and clean_email is not null
order by 1 ASC  

select * from mig_raw_crm_reservations_clean where email = 'jirovcova1@seznam.cz'

select * from mig_crm_person_accounts where source_id = '14816617'


select * from mig_crm_person_accounts where cluster_id = '79827'

select * from mig_crm_person_accounts where clean_email = 'jirovcova1@seznam.cz'

select list_id from gms_all_profiles gap where email = '04024@gmx.at'

select * from 


select * 
from gms_loyalty_liability gll 
where list_id in (select list_id from gms_all_profiles gap where email = '04024@gmx.at')

select * from raw_gms_loyalty_members rglm where email = '04024@gmx.at'

select * from int_crm_person_accounts

