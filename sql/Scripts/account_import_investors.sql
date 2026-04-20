


ALTER TABLE `mig_crm_person_accounts_imp20260419`
ADD COLUMN `central_consent` tinyint(1) unsigned NOT NULL DEFAULT 0;

ALTER TABLE `mig_crm_person_accounts_imp20260419`
ADD COLUMN `is_investor` tinyint(1) unsigned NOT NULL DEFAULT 0;

ALTER TABLE `mig_crm_person_accounts_imp20260419`
ADD COLUMN `sf_contact_id` VARCHAR(255) NULL DEFAULT NULL;

ALTER TABLE `mig_crm_person_accounts_imp20260419`
ADD COLUMN `sf_account_id` VARCHAR(255) NULL DEFAULT NULL;

ALTER TABLE `mig_crm_person_accounts_imp20260419`
ADD COLUMN `member_number_new` VARCHAR(255) NULL DEFAULT NULL;

ALTER TABLE `mig_crm_person_accounts_imp20260419`
ADD COLUMN `enrollment_date` DATE NULL DEFAULT NULL;


CREATE INDEX `idx_cluster_email` ON `mig_crm_person_accounts_imp20260419` (`cluster_email`);
CREATE INDEX `idx_cluster_id` ON `mig_crm_person_accounts_imp20260419` (`cluster_id`);

CREATE INDEX `idx_cluster_id` ON `mig_crm_person_accounts_imp20260419` (`cluster_id`);






/***    remove dups 				************/
 select count(*) 
 from mig_crm_person_accounts_imp20260419 t
 where exists (
	 select 1 from (
	 select  cluster_email, count(*)
	 from mig_crm_person_accounts_imp20260419
	 group by cluster_email having count(*) > 1
 	) a where a.cluster_email =  t.cluster_email
 )
 
 
 delete
 from mig_crm_person_accounts_imp20260419 t
 where exists (
	 select 1 from (
	 select  cluster_email, count(*)
	 from mig_crm_person_accounts_imp20260419
	 group by cluster_email having count(*) > 1
 	) a where a.cluster_email =  t.cluster_email
 )
 
 
 
  /***    flag investor		************/
 
 CREATE TABLE mig_mapping_investor_clean AS
 select distinct email, gms_id from mig_mapping_investor
 
 select count(*) 
 from gms_all_profiles gap 
 where gap.email in (select distinct email from mig_mapping_investor)
 
 -- 6377
 select count(distinct email) 
 from mig_mapping_investor
 
 -- 
  select*
 from mig_mapping_investor 
 where email in ( 
		 select email 
		 from mig_mapping_investor
		 group by email having count(*) > 1
		 )
 -- 6399
 select count(*) 
 from mig_mapping_investor
 
select *
from mig_crm_person_accounts_imp20260419 p
 
select count(*)
from mig_crm_person_accounts_imp20260419 p
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE p.cluster_email = i.email
);
 
 
 
update mig_crm_person_accounts_imp20260419 p
set p.is_investor = 1
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE p.cluster_email = i.email
);


/*  find investors in gms  */

-- 6304
select *
from gms_all_profiles gap 
WHERE EXISTS (
    SELECT 1
    FROM mig_mapping_investor i
    WHERE gap.email = i.email
);


select count(*) 
from mig_mapping_investor_clean i
WHERE EXISTS (
    SELECT 1
    FROM gms_all_profiles gap 
    WHERE gap.email = i.email
);


select * from mig_crm_person_accounts mcpa 

select * from gms_all_profiles gap 


CREATE TABLE mig_crm_investors_accounts AS
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
	null as clean_salutation,
	null as gender,
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




select * from mig_crm_investors_accounts






