

/***** Create cleaning fields ****/ 

ALTER TABLE gms_all_profiles
ADD COLUMN domain VARCHAR(255);

ALTER TABLE gms_all_profiles
ADD COLUMN exclude_email TINYINT(1) DEFAULT 0;

ALTER TABLE gms_all_profiles
ADD COLUMN missing_name TINYINT(1) DEFAULT 0;

ALTER TABLE `gms_all_profiles`
ADD COLUMN `cluster_id` VARCHAR(100) NULL DEFAULT NULL;


ALTER TABLE `gms_all_profiles`
ADD COLUMN `gender` VARCHAR(50) NULL DEFAULT NULL;

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


/* lower email */

update gms_all_profiles
set email = lower(email)


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


/* missing name		 */

SELECT *
FROM gms_all_profiles
WHERE (fname IS NULL OR TRIM(fname) = '')
  AND (lname IS NULL OR TRIM(lname) = '');


update gms_all_profiles
set missing_name = 1
WHERE (fname IS NULL OR TRIM(fname) = '')
  AND (lname IS NULL OR TRIM(lname) = '');

update gms_all_profiles
set missing_name = 1
WHERE (lname IS NULL OR TRIM(lname)='')

SELECT fname, lname, email FROM gms_all_profiles
WHERE  LENGTH(TRIM(lname)) = 1
LIMIT 100;

update gms_all_profiles
set missing_name = 1
WHERE  LENGTH(TRIM(lname)) = 1

SELECT fname, lname, email FROM gms_all_profiles
WHERE  lower(lname) like '%travel%'
and exclude_email = 0
and fname is null
LIMIT 100;

update gms_all_profiles
set exclude_email = 1
WHERE  lower(lname) like '%travel%'
and exclude_email = 0
and fname is null
 

update gms_all_profiles
SET exclude_email = 1
WHERE  lower(fname) like '%test%'
and exclude_email = 0

update gms_all_profiles
SET exclude_email = 1
WHERE  lower(fname) like '%booking%'
and exclude_email = 0

update gms_all_profiles
SET exclude_email = 1
WHERE  lower(fname) like '%_temporär%'
and exclude_email = 0

update gms_all_profiles
SET missing_name = 1
WHERE  lower(fname) like '%_temporär%'
and exclude_email = 0


update gms_all_profiles
SET exclude_email = 1
WHERE  lower(lname) like '%fmtg%'
and exclude_email = 0

select lname, count(*) 
from gms_all_profiles
where exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
group by lname 
order by 1 desc


SELECT list_id, email, fname, lname
FROM gms_all_profiles
WHERE lname REGEXP
   '[^\x00\x7F]|'                -- any non-ASCII character
  '[\\p{Han}]|'                  -- CJK (Chinese/Japanese/Korean)
  '[\\p{Hangul}]|'               -- Korean
  '[\\p{Arabic}]|'               -- Arabic, Persian
  '[\\p{Hebrew}]|'               -- Hebrew
  '[\\p{Cyrillic}]|'             -- Russian, Ukrainian, Bulgarian etc.
  '[\\p{Armenian}]|'             -- Armenian (Գույումճեան)
  '[\\p{Myanmar}]'               -- Burmese (နေ)
and exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
ORDER BY lname;

SELECT *
FROM gms_all_profiles
WHERE exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
and fname is null
and lower(lname) like '%gmbh%'


update gms_all_profiles
set missing_name = 1
WHERE 1=1 --
-- and exclude_email = 0
and missing_name = 0
-- and current_opt_in = 'Yes'
and fname is null
and lower(lname) like '%gmbh%'



SELECT *
FROM gms_all_profiles
WHERE exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
AND email REGEXP'^(spam|noreply|no-reply|donotreply|do-not-reply)@'

-- '^(contact|admin|support|sales|service|post|team|hello|hallo|help|noreply|no-reply|donotreply|do-not-reply|news|newsletter|billing|accounting|hr|jobs|career|careers|press|media|legal|privacy|security|abuse|spam|webmaster|hostmaster|postmaster|marketing|reception|general|enquiries|enquiry|inquiry|shop|store|order|orders|booking|bookings|reservations|events|conference|training|education|welcome|member|members|office1|office2|info1|info2)@'

-- spam|noreply|no-reply|donotreply|do-not-reply|

update gms_all_profiles
set exclude_email = 1
WHERE exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
AND email REGEXP'^(spam|noreply|no-reply|donotreply|do-not-reply)@'

SELECT email, COUNT(*) as cnt
FROM gms_all_profiles
WHERE exclude_email = 0
  AND email REGEXP '^(info|office|contact|admin|support|sales|service|mail|post|team|hello|hallo|help|noreply|no-reply|donotreply|do-not-reply|news|newsletter|billing|accounting|hr|jobs|career|careers|press|media|legal|privacy|security|abuse|spam|webmaster|hostmaster|postmaster|marketing|reception|general|enquiries|enquiry|inquiry|shop|store|order|orders|booking|bookings|reservations|events|conference|training|education|welcome|member|members|office1|office2|info1|info2)@'
GROUP BY email
ORDER BY cnt DESC;


UPDATE gms_all_profiles
SET exclude_email = 1
WHERE email IN (
  'aina+test@xo7.fr',
  'spieletest@gmail.com',
  'test@abv.bg',
  'test@tesd.cz',
  'helene+test@xo7.fr',
  'test@hotmail.com',
  'constantin-graf-test@constantingraf.at',
  'test@test.cm',
  'test@gmx.at',
  'test@sdfsdf.com',
  'test@sey.cry',
  'accounting@falktravel.de'
);

SELECT *
FROM gms_all_profiles
WHERE exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
and lname LIKE '%\_%'

UPDATE gms_all_profiles
set missing_name = 1
WHERE exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
and lname LIKE '%\_%'

UPDATE gms_all_profiles
set missing_name = 1
where lname like '%???%'
and missing_name = 0

UPDATE gms_all_profiles
set missing_name = 1
where lname in ('----',  '­')
and missing_name = 0 

UPDATE gms_all_profiles
SET lname = CONVERT(BINARY CONVERT(lname USING latin1) USING utf8mb4)
WHERE lname REGEXP '[├│┬┐┘╜╗╛»«]'
and email = 'amaea@email.cz'

select * from gms_all_profiles where email = 'amaea@email.cz'

update gms_all_profiles gap 
set missing_name = 1 
where fname like 'Компенсация-Http%'

update gms_all_profiles gap 
set missing_name = 1 
where lname = '_temporaryProfile'



UPDATE gms_all_profiles
SET missing_name = 1
WHERE fname LIKE '%@@%'
   OR lname LIKE '%@@%';


UPDATE gms_all_profiles
SET missing_name = 1
WHERE fname LIKE '%Http://%'
   OR lname LIKE '%Http://%';

update gms_all_profiles
set exclude_email = 1
WHERE lname REGEXP
  -- '[^\x00-\x7F]|'                -- any non-ASCII character
  '[\\p{Han}]|'                  -- CJK (Chinese/Japanese/Korean)
  '[\\p{Hangul}]|'               -- Korean
  '[\\p{Arabic}]|'               -- Arabic, Persian
  '[\\p{Hebrew}]|'               -- Hebrew
  '[\\p{Cyrillic}]|'             -- Russian, Ukrainian, Bulgarian etc.
  '[\\p{Armenian}]|'             -- Armenian (Գույումճեան)
  '[\\p{Myanmar}]'               -- Burmese (နေ)
ORDER BY lname;



SELECT email, fname, lname
FROM gms_all_profiles
WHERE lname REGEXP '^[A-Za-zÀ-ÖØ-öø-ÿ]\\.$'
and exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes';


update gms_all_profiles
set missing_name = 0
WHERE lname REGEXP '^[A-Za-zÀ-ÖØ-öø-ÿ]\\.$'
and exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes';

select lname, count(*)
from gms_all_profiles
where exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
group by lname having count(*) > 1
order by 2 asc



/* clean codes check */

select distinct country
from gms_all_profiles

select distinct citizenship
from gms_all_profiles

select distinct language_code
from gms_all_profiles


select * 
from gms_all_profiles
where exclude_email = 0
and missing_name = 0
and current_opt_in = 'Yes'
and city is not null 
and country is null



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



/********************************************/
/* GMS ACCOUNTS */

CREATE TABLE mig_crm_gms_accounts AS
SELECT
	null as cluster_id,
	'gms' as source,
	email as source_id,
	null as complete_fields,
	null as cluster_count,
	fname as cluster_first_name,
	lname as cluster_last_name,
	email as cluster_email,
	birthday as cluster_birth_date,
	null as clean_first_name,
	null as clean_last_name,
	null as clean_email,
	null as clean_birth_date,
	salutation as salutation,
	gender as gender,
	address as address,
	city as city,
	zip as postal_code,
	country as country,
	coalesce(cell_phone, home_phone) as phone,
	null as birth_place,
	citizenship as nationality,
	language_code as `language`,
	null as member_id,
	null as member_tier,
	null as enrollment_date
FROM
	gms_all_profiles gap
	
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE gap.email = i.email
);


select * 
from gms_all_profiles gap 
where gap.exclude_email = 0
and gap.missing_name = 0
and gap.current_opt_in = 'Yes'
and gap.cluster_id is null


select * from mig_crm_person_accounts mcpa 

/***** find cluster id for gms contacts via gms reservation  ***/


select list_id, count(*) from (

select gap.list_id, res.cluster_id as res_cluster_id
from gms_reservations gr  
inner join mig_raw_crm_reservations_clean res
 on gr.reservation_id = res.reservation_id
inner join gms_all_profiles gap 
	on gap.list_id = gr.list_id 
 where gap.cluster_id is null
  and gap.exclude_email = 0
  and res.email is not null
  and res.reservation_status not in ('Cancelled', 'NoShow')
 group by gap.list_id, res.cluster_id
 
 ) a
 
 group by list_id having count(*) = 1
 order by 2 desc
 
 
 -- 400190512, 411791799 -- 407345739
 
select gap.*, res.*
from gms_reservations gr  
inner join mig_raw_crm_reservations_clean res
 on gr.reservation_id = res.reservation_id
inner join gms_all_profiles gap 
	on gap.list_id = gr.list_id 
 where gap.cluster_id is null
	 and gap.exclude_email = 0
	 and res.email is not null
	 and gap.list_id = '407345739'
 

/* 
 * 
 * 407345739
406575833
422088463
 * 
 */	 
	 
	 
select * from mig_crm_person_accounts mcpa where mcpa.cluster_email = 'info@der-wiesenthaler.de'
 
 
 select * from mig_crm_person_
 
 ALTER TABLE mig_raw_crm_reservations_clean
ADD COLUMN sf_reservation_id VARCHAR(255) NULL;
 select * from crm_reservation_sfid_prod crsp 
 

/* set  cluster id in gms_all_profiles from reservation */
 
 
SELECT list_id, MAX(res_cluster_id) AS res_cluster_id
FROM (
  SELECT gap.list_id, res.cluster_id AS res_cluster_id
  FROM gms_reservations gr
  INNER JOIN mig_raw_crm_reservations_clean res
    ON gr.reservation_id = res.reservation_id
  INNER JOIN gms_all_profiles gap
    ON gap.list_id = gr.list_id
  WHERE gap.cluster_id IS NULL
    AND gap.exclude_email = 0
    AND res.email IS NOT NULL
    AND res.reservation_status NOT IN ('Cancelled', 'NoShow')
  GROUP BY gap.list_id, res.cluster_id
) a
GROUP BY list_id
HAVING COUNT(DISTINCT res_cluster_id) = 1;



UPDATE gms_all_profiles gap
JOIN (
  SELECT list_id, MAX(res_cluster_id) AS res_cluster_id
  FROM (
    SELECT gap2.list_id, res.cluster_id AS res_cluster_id
    FROM gms_reservations gr
    INNER JOIN mig_raw_crm_reservations_clean res
      ON gr.reservation_id = res.reservation_id
    INNER JOIN gms_all_profiles gap2
      ON gap2.list_id = gr.list_id
    WHERE gap2.cluster_id IS NULL
      AND gap2.exclude_email = 0
      AND res.email IS NOT NULL
      AND res.reservation_status NOT IN ('Cancelled', 'NoShow')
    GROUP BY gap2.list_id, res.cluster_id
  ) a
  GROUP BY list_id
  HAVING COUNT(DISTINCT res_cluster_id) = 1
) src ON gap.list_id = src.list_id
SET gap.cluster_id = src.res_cluster_id
WHERE gap.cluster_id IS NULL;

SELECT ROW_COUNT() AS cluster_ids_updated;



select * 
from mig_crm_person_accounts i
where exists (
		select 1 
		from gms_all_profiles gap
		left join crm_person_account_sfid_prod acc
			on gap.cluster_id = acc.ClusterID__pc
			and acc.ClusterID__pc is not null
		where gap.cluster_id is not null
		and  acc.ClusterID__pc is null
		and i.cluster_id = gap.cluster_id )
 

select * from crm_c

select count(*) from mig_crm_person_accounts_imp20260420
drop table mig_crm_person_accounts_imp20260420
CREATE TABLE mig_crm_person_accounts_imp20260420 AS
SELECT
  cluster_id,
  clean_first_name,
  clean_last_name,
  clean_email,
  clean_birth_date,
  salutation,
  gender,
  address,
  city,
  postal_code,
  country,
  phone,
  birth_place,
  nationality,
  language,
  member_id,
  member_tier,
  enrollment_date
FROM mig_crm_person_accounts i
WHERE exists (select 1 
		from gms_all_profiles gap
		left join crm_person_account_sfid_prod acc
			on gap.cluster_id = acc.ClusterID__pc
			and acc.ClusterID__pc is not null
		where gap.cluster_id is not null
		and  acc.ClusterID__pc is null
		and i.cluster_id = gap.cluster_id )
GROUP BY
  cluster_id,
  clean_first_name,
  clean_last_name,
  clean_email,
  clean_birth_date,
  salutation,
  gender,
  address,
  city,
  postal_code,
  country,
  phone,
  birth_place,
  nationality,
  language,
  member_id,
  member_tier,
  enrollment_date

  
  
  
  
  

