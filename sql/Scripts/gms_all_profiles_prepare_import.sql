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
ADD COLUMN `cluster_id` BIGINT NULL DEFAULT NULL;

CREATE INDEX `idx_cluster_id` ON `gms_loyalty_liability` (`cluster_id`);

update gms_all_profiles
SET domain = SUBSTRING_INDEX(email, '@', -1);


CREATE INDEX `idx_list_id` ON `gms_loyalty_liability` (`list_id`(255));
CREATE INDEX `idx_list_id` ON `gms_all_profiles` (`list_id`(255));

CREATE INDEX `idx_email` ON `gms_all_profiles` (`email`);
CREATE INDEX `idx_email` ON `mig_mapping_investor_clean` (`email`);


ALTER TABLE gms_all_profiles
  ADD COLUMN member_id VARCHAR(100) NULL,
  ADD COLUMN member_tier VARCHAR(50) NULL,
  ADD COLUMN enrollment_date DATE NULL,
  ADD COLUMN sf_contact_id VARCHAR(255) NULL,
  ADD COLUMN sf_account_id VARCHAR(255) NULL,
  ADD COLUMN sf_member_id VARCHAR(200) NULL,
  ADD COLUMN sf_cpe_id VARCHAR(200) NULL,
  ADD COLUMN is_investor TINYINT(1) DEFAULT 0,
  ADD COLUMN central_consent TINYINT(1) DEFAULT 0
  ADD COLUMN sf_entra_id VARCHAR(200) NULL
  ADD COLUMN member_number_new BIGINT NULL;

ALTER TABLE gms_all_profiles
  ADD COLUMN is_investor TINYINT(1) DEFAULT 0,
member_number_new

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


/*************************************/
/********* Investors Profiles **********/

/* update invest information */ 
UPDATE gms_all_profiles p
set p.is_investor = 1
where exists ( select 1 from mig_mapping_investor_clean i where p.email = i.email ) 


/* >>> !!! Es fehlen 70-73 investoren !!! <<<*/
-- missing in gms profiles
select *
from mig_mapping_investor_clean i
where not exists (select 1 from gms_all_profiles p where is_investor = 1 and p.email = i.email)

-- search in loyality 
select *  
from mig_mapping_investor_clean i
where not exists (select 1 from gms_loyalty_liability p where p.member_number = i.gms_id)

/*************************************/
/********* Loyalty Profiles **********/

select * from gms_all_profiles
 
UPDATE gms_all_profiles a
INNER JOIN (
    WITH ranked AS (
        SELECT 
            list_id,
            start_date,
            tier,
            member_number,
            lasttransactiondate,
            points,
            ROW_NUMBER() OVER (
                PARTITION BY list_id
                ORDER BY 
                    COALESCE(lasttransactiondate, '1900-01-01') DESC,
                    COALESCE(points, 0) DESC,
                    tier DESC
            ) AS rn
        FROM gms_loyalty_liability
        WHERE verified_flag = 1
          AND inactive_flag = 0
          AND member_number IS NOT NULL
          AND list_id IS NOT NULL
    )
    SELECT 
        list_id,
        member_number,
        tier,
        start_date
    FROM ranked
    WHERE rn = 1
) b ON a.list_id = b.list_id
SET 
    a.member_id       = b.member_number,
    a.member_tier     = b.tier,
    a.enrollment_date = b.start_date;

 
 -- Investor forced gms loyality mapping


UPDATE gms_all_profiles a
INNER JOIN (
    WITH ranked AS (
        SELECT 
            l.list_id,
            l.start_date,
            l.tier,
            l.member_number,
            l.lasttransactiondate,
            l.points,
            ROW_NUMBER() OVER (
                PARTITION BY l.list_id
                ORDER BY 
                    COALESCE(l.lasttransactiondate, '1900-01-01') DESC,
                    COALESCE(l.points, 0) DESC,
                    l.tier DESC
            ) AS rn

        FROM gms_loyalty_liability l
        WHERE 1=1 -- verified_flag = 1
          -- AND inactive_flag = 0
          AND l.member_number IS NOT NULL
          AND l.list_id IS NOT NULL
          AND EXISTS (select * from mig_mapping_investor_clean ic where ic.gms_id = l.member_number  )
    )
    SELECT 
        list_id,
        member_number,
        tier,
        start_date
    FROM ranked
    WHERE rn = 1
) b ON a.list_id = b.list_id
	AND a.member_id is null
SET 
    a.member_id       = b.member_number,
    a.member_tier     = b.tier,
    a.enrollment_date = b.start_date;


select * from gms_loyalty_liability gll where gll.email = 'alexander.kovac@kovac.com'
 
 -- update missing enrollment date with gms create date
 
 select * 
 from gms_all_profiles gap
 where gap.member_tier is not null 
 and enrollment_date is null
 
 select * from gms_loyalty_liability gll where list_id = 407944635
 
 -- 
 update gms_all_profiles
 set enrollment_date =  coalesce(enrollment_date, created_date)
 where member_tier is not null 
 and enrollment_date is null
 
 select count(*) 
 from gms_all_profiles p
 where p.member_tier is not null 
 and p.enrollment_date is null

 
 -- diese 4.000 profiles können repariert werden  
 select count(*) 
 -- select *
 from gms_all_profiles gap
 where gap.member_tier is not null
 and ( gap.exclude_email = 1 or  gap.missing_name = 1)

 
 /*************************************/
/********* Birtday **********/
 
 
 select gap.birthday, count(*)
 from gms_all_profiles gap
 where gap.exclude_email = 0
 and gap.missing_name =  0
 and gap.birthday is not null
 group by birthday 
 order by 1 asc
 
 select gap.birthday, gap.*
 from gms_all_profiles gap
 where gap.exclude_email = 0
 and gap.missing_name =  0
 and gap.birthday is not null
 order by 1 asc
 
 SELECT
  gap.birthday,
  TIMESTAMPDIFF(YEAR, STR_TO_DATE(gap.birthday, '%Y-%m-%d'), CURDATE()) AS age,
  gap.*
FROM gms_all_profiles gap
WHERE gap.exclude_email = 0
  AND gap.missing_name = 0
  AND gap.birthday IS NOT NULL
ORDER BY gap.birthday ASC;
 
 update gms_all_profiles gap
 set birthday = null 
 where birthday = '1900-01-01'
 
 update gms_all_profiles gap
 set birthday = null 
 where birthday > '2026-04-20'
 
 update gms_all_profiles gap
 set birthday = null 
 where birthday < '1800-01-01'
 
 
  /*************************************/
/********* Salutation and Gender **********/
 
ALTER TABLE gms_all_profiles
ADD COLUMN salutation_orginal VARCHAR(255);

update gms_all_profiles
set salutation_orginal = salutation; 
 
 select fname, Salutation, count(*)
 from gms_all_profiles gap
 where country in ('AT', 'DE')
 and Salutation is not null
 group by fname, Salutation
 
 update gms_all_profiles gap 
 set salutation = 'Ms.'
 where gap.salutation = 'Miss.'

-- create table with salutation and names

CREATE TABLE mig_crm_dim_fname_gender_world AS
WITH base AS (
    SELECT 
        LOWER(TRIM(fname)) AS fname,
        salutation,
        COUNT(*) AS cnt
    FROM gms_all_profiles
    WHERE 1=1 -- country NOT IN ('AT','DE', 'CH')
      AND salutation IS NOT NULL
      AND fname IS NOT NULL
      AND missing_name = 0
      AND salutation IN ('Mr.', 'Mrs.', 'Ms.')
    GROUP BY LOWER(TRIM(fname)), salutation
),
totals AS (
    SELECT 
        fname,
        SUM(cnt) AS total_cnt
    FROM base
    GROUP BY fname
),
ranked AS (
    SELECT 
        b.fname,
        b.salutation,
        b.cnt,
        t.total_cnt,
        ROUND(b.cnt * 1.0 / t.total_cnt, 4) AS ratio,
        ROW_NUMBER() OVER (PARTITION BY b.fname ORDER BY b.cnt DESC) AS rn
    FROM base b
    JOIN totals t ON b.fname = t.fname
)
SELECT 
    fname,
    CASE 
        WHEN salutation = 'Mr.' THEN 'Male'
        WHEN salutation IN ('Mrs.', 'Ms.', 'Ms.') THEN 'Female'
    END AS gender,
    salutation,
    total_cnt,
    cnt AS dominant_cnt,
    ratio,
    'inferred_from_name' AS source
FROM ranked
WHERE rn = 1
  AND total_cnt >= 50
  AND ratio >= 0.9;
 


select * from mig_crm_dim_fname_gender_german;

SELECT t.fname, d.salutation, COUNT(*) 
FROM gms_all_profiles t
JOIN mig_crm_dim_fname_gender_german d
  ON LOWER(TRIM(t.fname)) = d.fname
WHERE t.salutation IS NULL
AND t.fname IS NOT NULL
AND t.missing_name = 0
GROUP BY t.fname, d.salutation
ORDER BY COUNT(*) DESC;

-- set salutation and gender
CREATE INDEX idx_gms_all_profiles_fname ON gms_all_profiles (fname(255));
CREATE INDEX idx_mig_crm_dim_fname_gender_german_fname 
ON mig_crm_dim_fname_gender_german (fname(255));

CREATE INDEX idx_mig_crm_dim_fname_gender_world_fname 
ON mig_crm_dim_fname_gender_world (fname(255));

UPDATE gms_all_profiles t
JOIN mig_crm_dim_fname_gender_world d
  ON LOWER(TRIM(t.fname)) = d.fname
SET t.salutation = d.salutation, t.gender = coalesce(t.gender, d.gender) 
WHERE t.salutation not in ('Mr.', 'Mrs.', 'Ms.', 'Ms.', 'Dr.', 'Prof.', 'Mx')
  AND t.fname IS NOT NULL
  AND t.missing_name = 0;

-- update gender
UPDATE gms_all_profiles t
JOIN mig_crm_dim_fname_gender_world d
  ON LOWER(TRIM(t.fname)) = d.fname
SET t.gender = coalesce(t.gender, d.gender) 
WHERE t.salutation in ('Mr.', 'Mrs.', 'Ms.', 'Ms.', 'Dr.', 'Prof.', 'Mx')
  AND t.fname IS NOT NULL
  AND t.gender is null
  AND t.missing_name = 0;

select t.salutation, count(*) 
from gms_all_profiles t
where t.missing_name = 0
and fname is not null
and t.salutation not in ('Mr.', 'Mrs.', 'Ms.', 'Ms.', 'Dr.', 'Prof.', 'Mx')
group by t.salutation
order by 2 desc

select count(*) 
select *
from gms_all_profiles t
where t.missing_name = 0
and fname is not null
-- and t.salutation not in ('Mr.', 'Mrs.', 'Ms.', 'Ms.', 'Dr.', 'Prof.', 'Mx')
and t.salutation = 'Family'
and t.exclude_email = 0



UPDATE gms_all_profiles t
set t.salutation = 'Mrs.', t.gender = 'Female'
where fname in ('Antonella', 'Antonietta', 'Antonija', 
	'Antonina', 'Arianna', 'Laura', 'Emma', 'Lena', 'Marie',
	'Valentina', 'Sophie', 'Jana', 'Elena', 'Sara', 'Emilia',
	'Lea', 'Luisa', 'Mia', 'Hannah', 'Lara', 'Sophia', 'Leonie',
	'Elisa', 'Ivana', 'Hanna', 'Amelie', 'Ella', 'Francesca', 'Lina',
	'Chiara', 'Helena', 'Jasmin', 'Charlotte', 'Linda', 'Maja', 'Sofia', 'Emily',
	'Nora', 'Ana', 'Jessica', 'Lucia', 'Mila', 'Greta', 'Alice', 'Clara', 'Alina', 'Simona',
	'Paulina', 'Marta', 'Roberta', 'Lilly', 'Cristina', 'Lenka', 'Katarina', 'Katerina',
	'Olga', 'Selina', 'Melina', 'Maya', 'Paula', 'Olivia'
	)
and t.salutation not in ('Mr.', 'Mrs.', 'Ms.', 'Ms.', 'Dr.', 'Prof.', 'Mx')

select t.fname, count(*) 
from gms_all_profiles t
where t.missing_name = 0
and t.fname is not null
and t.salutation not in ('Mr.', 'Mrs.', 'Ms.', 'Ms.', 'Dr.', 'Prof.', 'Mx')
group by t.fname
order by 2 desc


UPDATE gms_all_profiles t
set t.salutation = 'Ms.'
where t.missing_name = 0
and t.salutation is not null
and t.salutation like 'Mademoiselle %'

UPDATE gms_all_profiles
SET gender = CASE 
    WHEN salutation = 'Mr.' THEN 'Male'
    WHEN salutation IN ('Mrs.', 'Ms.') THEN 'Female'
END
WHERE gender IS NULL
  AND salutation IN ('Mr.', 'Mrs.', 'Ms.', 'Ms.');

-- remove invalid values
update gms_all_profiles
set salutation = null
where  salutation not in ('Mr.', 'Mrs.', 'Ms.', 'Dr.', 'Prof.', 'Mx')

select t.gender, t.salutation, count(*) 
from gms_all_profiles t
group by t.gender, t.salutation
order by 2 desc

  /*************************************/
/********* set salesforce ids **********/

-- account and contact
select * from crm_person_account_sfid_prod 
update gms_all_profiles t
left join crm_person_account_sfid_prod s
	on s.PersonEmail = t.email
set t.sf_contact_id = coalesce(t.sf_contact_id, s.PersonContactId), t.sf_account_id = coalesce(t.sf_account_id, s.Id)
where s.PersonEmail = t.email

-- contact point email - only contacts
select * from crm_cp_email_sfid_prod 
update gms_all_profiles t
left join crm_cp_email_sfid_prod s
	on s.EmailAddress = t.email
set t.sf_cpe_id = s.Id
where s.EmailAddress = t.email
and left(s.PartyID__c,3) = '003'

-- loyality program member
select * from crm_loyality_sfid_prod

CREATE INDEX idx_gms_sf_contact_id 
ON gms_all_profiles (sf_contact_id);
CREATE INDEX idx_crm_loyality_contactid 
ON crm_loyality_sfid_prod (ContactId);

update gms_all_profiles t
left join crm_loyality_sfid_prod s
	on s.ContactId = t.sf_contact_id
set t.sf_member_id = s.Id
where s.ContactId = t.sf_contact_id

update gms_all_profiles t
left join crm_loyality_sfid_prod s
	on s.ContactId = t.sf_contact_id
set t.member_number_new = s.ExternalMemberId__c
where s.ContactId = t.sf_contact_id



-- 84288
select count(*) from crm_loyality_sfid_prod
-- 84288
select count(*) from gms_all_profiles where sf_member_id is not null

-- 4 Abweichende Member Id aber selbe list id
select count(*) 
select t.member_id, s.LegacyMemberId__c, t.*, s.*
from gms_all_profiles t
left join crm_loyality_sfid_prod s
	on s.ContactId = t.sf_contact_id
where s.ContactId = t.sf_contact_id
and s.LegacyMemberId__c <> t.member_id

select * from gms_loyalty_liability gll where list_id = '563042256'

  /*************************************/
/********* central consent **********/

update gms_all_profiles
set central_consent = 1 
where current_opt_in = 'Yes'


  /*************************************/
/********* Some final cleaning **********/

update gms_all_profiles
set missing_name = 1
WHERE lname is null

update gms_all_profiles
set missing_name = 1
WHERE lname REGEXP '^[0-9]+[a-z]+$'
  AND LENGTH(lname) >= 8;

update gms_all_profiles
set missing_name = 1
WHERE lname REGEXP '�|\\?\\?'
and  missing_name = 0
ORDER BY lname;


update gms_all_profiles
set missing_name = 1
WHERE lname in ('..', ',s' , '^rudy', '´')
and  missing_name = 0
ORDER BY lname;

update gms_all_profiles
set missing_name = 1
WHERE lower(lname)  like '%gesmbh%'
and  missing_name = 0
ORDER BY lname;


select * 
from gms_all_profiles gap 
where gap.exclude_email = 0
and gap.missing_name = 0
and (gap.current_opt_in = 'Yes' or gap.member_tier is not null)
and gap.sf_contact_id is null
order by lname asc

SELECT * 
FROM gms_all_profiles gap
WHERE lname REGEXP '^[0-9]+$'
and gap.missing_name = 0
and (gap.current_opt_in = 'Yes' or gap.member_tier is not null)
and gap.sf_contact_id is null


  /*************************************/
/********* set cluster_id **********/

SET @cnt := 5999999;

UPDATE gms_all_profiles
SET cluster_id = (@cnt := @cnt + 1)
WHERE cluster_id IS NULL
ORDER BY created_date asc;  -- oder ein stabiler Key!

select cluster_id, t.*
from gms_all_profiles t 
order by cluster_id desc

  /*************************************/
/********* set new member number **********/
select * from gms_all_profiles

update gms_all_profiles gap
set member_number_new = 358000360 + cluster_id
where member_tier is not null
and member_number_new is null


select * from gms_all_profiles where cluster_id is null

  /*************************************/
/********* create import table **********/

SELECT * 
FROM gms_all_profiles gap
WHERE gap.missing_name = 0
and (gap.current_opt_in = 'Yes' or gap.member_tier is not null)
and gap.exclude_email = 0
and gap.sf_contact_id is null
and 

select max(cluster_id) from mig_crm_person_accounts

CREATE TABLE mig_crm_gms_accounts_imp20260421 AS
SELECT
	cluster_id as cluster_id,
	'gms' as source,
	list_id as source_id,
	fname as clean_first_name,
	lname as clean_last_name,
	email as clean_email,
	birthday as clean_birth_date,
	salutation as salutation,
	gender as gender,
	address as address,
	city as city,
	zip as postal_code,
	country as country,
	coalesce(cell_phone, home_phone, office_phone) as phone,
	null as birth_place,
	citizenship as nationality,
	language_code as `language`,
	member_id as legacy_member_number,
	member_number_new,
	member_tier as member_tier,
	enrollment_date as enrollment_date,
	sf_contact_id,
	sf_account_id,
	sf_member_id,
	sf_cpe_id,
	sf_entra_id,
	central_consent,
	is_investor
FROM
	gms_all_profiles gap
	
WHERE gap.missing_name = 0 
and gap.exclude_email = 0
and (gap.current_opt_in = 'Yes' or gap.member_tier is not null)
and gap.sf_contact_id is null


select count(*) from mig_crm_gms_accounts_imp20260421

select central_consent, count(*) 
from mig_crm_gms_accounts_imp20260421
group by central_consent


-- Investor post import 

CREATE TABLE mig_crm_gms_accounts_imp20260430_invest AS
SELECT
	cluster_id as cluster_id,
	'gms' as source,
	list_id as source_id,
	fname as clean_first_name,
	lname as clean_last_name,
	email as clean_email,
	birthday as clean_birth_date,
	salutation as salutation,
	gender as gender,
	address as address,
	city as city,
	zip as postal_code,
	country as country,
	coalesce(cell_phone, home_phone, office_phone) as phone,
	null as birth_place,
	citizenship as nationality,
	language_code as `language`,
	member_id as legacy_member_number,
	member_number_new,
	member_tier as member_tier,
	enrollment_date as enrollment_date,
	sf_contact_id,
	sf_account_id,
	sf_member_id,
	sf_cpe_id,
	sf_entra_id,
	central_consent,
	is_investor

-- select count(*)
FROM
	gms_all_profiles gap
	
WHERE gap.missing_name = 0 
-- and gap.exclude_email = 0
-- and gap.member_tier is not null
-- and gap.sf_contact_id is not null
and gap.sf_contact_id is  null
-- and gap.sf_cpe_id is not null
-- and gap.sf_member_id is null
and gap.is_investor = 1;

select * from mig_crm_gms_accounts_imp20260430_invest
select * from mig_loyality_entra_id_2 mlei  where Mail = 'patrick.gruendler@gmx.at'

select count(*) from crm_loyality_sfid_prod clsp where clsp.EntraID__c is null

  /*************************************/
/********* set salesforce ids **********/

select * from crm_person_account_sfid_prod s

-- account and contact
select count(*) from crm_person_account_sfid_prod 
update gms_all_profiles t
left join crm_person_account_sfid_prod s
	on s.PersonEmail = t.email
set t.sf_contact_id = s.PersonContactId, t.sf_account_id = s.Id
where s.PersonEmail = t.email
and t.sf_contact_id is null

CREATE INDEX `idx_email` ON `mig_crm_gms_accounts_imp20260421` (`clean_email`);

select * from crm_person_account_sfid_prod 
update mig_crm_gms_accounts_imp20260421 t
left join crm_person_account_sfid_prod s
	on s.PersonEmail = t.clean_email
set t.sf_contact_id = s.PersonContactId, t.sf_account_id = s.Id
where s.PersonEmail = t.clean_email
and t.sf_contact_id is null

-- contact point email - only contacts
select * from crm_cp_email_sfid_prod 
update gms_all_profiles t
left join crm_cp_email_sfid_prod s
	on s.EmailAddress = t.email
set t.sf_cpe_id = s.Id
where s.EmailAddress = t.email
and left(s.PartyID__c,3) = '003'
and t.sf_cpe_id is null

update mig_crm_gms_accounts_imp20260421 t
left join crm_cp_email_sfid_prod s
	on s.EmailAddress = t.clean_email
set t.sf_cpe_id = s.Id
where s.EmailAddress = t.clean_email
and left(s.PartyID__c,3) = '003'

-- loyality program member
select * from crm_loyality_sfid_prod

CREATE INDEX idx_gms_sf_contact_id 
ON mig_crm_gms_accounts_imp20260421 (sf_contact_id);

select * from mig_crm_gms_accounts_imp20260421 where loy

update gms_all_profiles t
join crm_loyality_sfid_prod s
	on s.ContactId = t.sf_contact_id
set t.sf_member_id = s.Id, t.member_number_new = s.ExternalMemberId__c
where s.ContactId = t.sf_contact_id
and t.sf_member_id is null

update mig_crm_gms_accounts_imp20260421 t
join crm_loyality_sfid_prod s
	on s.ContactId = t.sf_contact_id
set t.sf_member_id = s.Id, t.member_number_new = s.ExternalMemberId__c
where s.ContactId = t.sf_contact_id
and t.sf_member_id is null

create table mig_error_mapping_20260421 as 
select * from mig_crm_gms_accounts_imp20260421 t
join crm_loyality_sfid_prod s
	on s.ContactId = t.sf_contact_id
where s.ContactId = t.sf_contact_id
and t.sf_member_id is null

select * from gms_loyalty_liability gll where gll.member_number ='thomas.fasching1980@gmail.com';
select * from gms_all_profiles gap where list_id = '418874188'

  /*************************************/
/********* consent import query  **********/

select distinct i.sf_cpe_id as cpe_id, 'gms' as 'source' 
from mig_crm_gms_accounts_imp20260421 i
where central_consent = 1
and i.sf_cpe_id is not null 
and not exists (select 1 from crm_cp_consent_sfid_prod c 
					where c.PartyId = i.sf_cpe_id and Name = 'marketing_central')
					
					
ALTER TABLE mig_crm_gms_accounts_imp20260421
  ADD COLUMN is_investor TINYINT(1) DEFAULT 0
  
update mig_crm_gms_accounts_imp20260421 i
join gms_all_profiles gap 
	on gap.cluster_id = i.cluster_id
 set i.is_investor = gap.is_investor
 where gap.is_investor is not null
 
 select * from mig_crm_gms_accounts_imp20260421 where is_investor = 1
 
   /*************************************/
/********* loyality import query  **********/
 
 select  
                cluster_id,
                sf_contact_id,
                source,
                legacy_member_number,
                member_tier,
                member_number_new,
                enrollment_date
   
        from mig_crm_gms_accounts_imp20260421 
        where member_tier is not null 
        and sf_contact_id is not null
        and sf_member_id is null
        
        
   /*************************************/
        
/*** enrich entra id for loyality   ***/

select * 
from mig_loyality_entra_id e
left join gms_all_profiles p 
	on e.Mail = p.email 
where p.email is null
        
update gms_all_profiles p
inner join mig_loyality_entra_id e
	 on e.Mail = p.email
set sf_entra_id = e.Id


update gms_all_profiles p
inner join mig_loyality_entra_id_2 e
	 on e.Mail = p.email
set sf_entra_id = e.Id
	


select count(*) 
from gms_all_profiles
where sf_entra_id is not null

-- update entra id and legacy tier

select count(*) from crm_loyality_sfid_prod


select distinct 
		p.member_number_new, 
        p.member_tier, 
        sf_entra_id  
    from gms_all_profiles p
    inner join crm_loyality_sfid_prod l
        on p.sf_member_id = l.Id 
    where p.member_tier is not null 
        
/********* statistics & analysis  **********/
        

select '1. person_accounts' as sf_object, count(*) as cnt from crm_person_account_sfid_prod a where a.PersonContactId is not null
union
select '2. Leads' as sf_object, count(*) as cnt from crm_person_lead_sfid_prod
union 
select '3. loyality_progam_member' as sf_object, count(*) as cnt from crm_loyality_sfid_prod
union
select '4. Reservations' as sf_object, count(*) as cnt from crm_reservation_sfid_prod

select '3. cp_email' as sf_object, count(*) as cnt from crm_cp_email_sfid_prod
union
select Name as '4. sf_object', count(*) as cnt from crm_consent_sfid_prod group by Name order by 2 DESC 



select 
    PartyType,
    case 
        when Name = 'marketing_central' then 'central'
        when Name like 'marketing_property%' then 'property'
        else Name
    end as ConsentType,
    count(*) 
from crm_consent_sfid_prod
group by 
    PartyType,
    case 
        when Name = 'marketing_central' then 'central'
        when Name like 'marketing_property%' then 'property'
        else Name
    end

union all

select 
    PartyType,
    'Total',
    count(distinct ContactPointId)
from crm_consent_sfid_prod
group by PartyType

union all


select 
    'Total' as PartyType,
    '--',
    count(distinct ContactPointId)
from crm_consent_sfid_prod



select 
    case 
        when Name = 'marketing_central' then 'central'
        when Name like 'marketing_property%' then 'property'
        else Name
    end as ConsentType,
    count(*) 
from crm_consent_sfid_prod
group by 
    case 
        when Name = 'marketing_central' then 'central'
        when Name like 'marketing_property%' then 'property'
        else Name
    end
    
union all

select 
    'Total' as PartyType,
    count(*)
from crm_consent_sfid_prod   


