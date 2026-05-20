


select * from mig_residence_owner_upload
select count(*) from mig_residence_owner_upload


select * from gms_all_profiles


/**/

-- Rename First Name → first_name
ALTER TABLE mig_residence_owner_upload
    CHANGE COLUMN `First Name` `first_name` VARCHAR(255) COLLATE utf8mb3_unicode_ci DEFAULT NULL;

-- Rename Last Name → last_name
ALTER TABLE mig_residence_owner_upload
    CHANGE COLUMN `Last Name` `last_name` VARCHAR(255) COLLATE utf8mb3_unicode_ci DEFAULT NULL;


-- Add gender column
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN gender VARCHAR(10) NULL AFTER salutation;

-- Populate gender from salutation
UPDATE mig_residence_owner_upload
SET gender = CASE salutation
    WHEN 'Mr.'  THEN 'Male'
    WHEN 'Ms.'  THEN 'Female'
    WHEN 'Mrs.'	THEN 'Female'
    ELSE NULL
END;

-- Add preferred_language column
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN preferred_language VARCHAR(5) NULL AFTER gender;

-- Map language names to ISO codes (Salesforce convention)
UPDATE mig_residence_owner_upload
SET preferred_language = CASE Language
    WHEN 'German'  THEN 'de'
    WHEN 'English' THEN 'en'
    WHEN 'Italian' THEN 'it'
    ELSE NULL
END;

-- Sanity checks
SELECT salutation, gender, COUNT(*) AS cnt
FROM mig_residence_owner_upload
GROUP BY salutation, gender;

SELECT Language, preferred_language, COUNT(*) AS cnt
FROM mig_residence_owner_upload
GROUP BY Language, preferred_language;


-- Add source column
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN source VARCHAR(50) NULL AFTER preferred_language;

-- Populate all rows
UPDATE mig_residence_owner_upload
SET source = 'Excel Owner';

-- Sanity check
SELECT source, COUNT(*) AS cnt
FROM mig_residence_owner_upload
GROUP BY source;


-- Add investor_status column
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN investor_status VARCHAR(50) NULL AFTER source;

UPDATE mig_residence_owner_upload
SET investor_status = 'Crystal';

-- Add investor_expiration_date column
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN investor_expiration_date DATE NULL AFTER investor_status;

UPDATE mig_residence_owner_upload
SET investor_expiration_date = '2056-01-01';

-- Sanity check
SELECT investor_status, investor_expiration_date, COUNT(*) AS cnt
FROM mig_residence_owner_upload
GROUP BY investor_status, investor_expiration_date;


-- check if exists 

select sfa.ClusterID__pc as cluster_id, Id as person_account_id, sfa.PersonContactId as person_contact_id
from crm_person_account_sfid_prod sfa
inner join mig_residence_owner_upload rw
	on rw.Email = sfa.PersonEmail
where  sfa.ClusterID__pc is not null

select * 
from mig_residence_owner_upload


-- Add the three new columns
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN cluster_id          VARCHAR(50)  NULL AFTER investor_expiration_date,
    ADD COLUMN person_account_id   VARCHAR(18)  NULL AFTER cluster_id,
    ADD COLUMN person_contact_id   VARCHAR(18)  NULL AFTER person_account_id;

-- Populate from crm_person_account_sfid_prod via email match
CREATE INDEX idx_crm_person_account_sfid_prod_email
    ON crm_person_account_sfid_prod (PersonEmail);


UPDATE mig_residence_owner_upload rw
INNER JOIN crm_person_account_sfid_prod sfa
    ON sfa.PersonEmail = rw.Email
SET rw.cluster_id        = coalesce(sfa.ClusterID__c, rw.cluster_id),
    rw.person_account_id = sfa.Id,
    rw.person_contact_id = sfa.PersonContactId
WHERE sfa.ClusterID__pc IS NOT NULL;

-- Sanity checks
SELECT
    COUNT(*)                                              AS total_rows,
    COUNT(cluster_id)                                     AS matched_rows,
    COUNT(*) - COUNT(cluster_id)                          AS unmatched_rows
FROM mig_residence_owner_upload;

-- Inspect a few matches
SELECT Email, cluster_id, person_account_id, person_contact_id
FROM mig_residence_owner_upload
WHERE cluster_id IS NOT NULL
LIMIT 10;

-- Inspect unmatched rows (no SF account found by email)
SELECT Email, COUNT(*) AS cnt
FROM mig_residence_owner_upload
WHERE cluster_id IS NULL
GROUP BY Email
LIMIT 20;



-- Add the two new columns
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN data_use_purpose VARCHAR(50)  NULL AFTER person_contact_id,
    ADD COLUMN consent_type     VARCHAR(50)  NULL AFTER data_use_purpose;

-- Populate
UPDATE mig_residence_owner_upload
SET data_use_purpose = '0ZWTe0000000XATOA2',
    consent_type     = 'residences_central';

-- Sanity check
SELECT data_use_purpose, consent_type, COUNT(*) AS cnt
FROM mig_residence_owner_upload
GROUP BY data_use_purpose, consent_type;

-- Add the column
ALTER TABLE mig_residence_owner_upload
    ADD COLUMN _import_operation VARCHAR(10) NULL AFTER consent_type;

-- Populate based on person_contact_id
UPDATE mig_residence_owner_upload
SET _import_operation = CASE
    WHEN person_contact_id IS NOT NULL THEN 'update'
    ELSE 'create'
END;

-- Sanity check
SELECT _import_operation, COUNT(*) AS cnt
FROM mig_residence_owner_upload
GROUP BY _import_operation;



-- check with gms contacts 
SELECT p.cluster_id  
from gms_all_profiles p
inner join mig_residence_owner_upload r
	on p.email = r.Email
where r.person_contact_id is null


-- Backfill cluster_id from gms_all_profiles for rows that don't yet have one
UPDATE mig_residence_owner_upload r
INNER JOIN gms_all_profiles p
    ON p.email = r.Email
SET r.cluster_id = p.cluster_id
WHERE r.cluster_id IS NULL
  AND p.cluster_id IS NOT NULL;

-- Sanity checks
SELECT
    COUNT(*)                                       AS total_rows,
    COUNT(cluster_id)                              AS rows_with_cluster_id,
    COUNT(*) - COUNT(cluster_id)                   AS rows_still_missing
FROM mig_residence_owner_upload;

-- Cross-check: how many rows got matched in this step
SELECT _import_operation, COUNT(*) AS cnt, COUNT(cluster_id) AS with_cluster
FROM mig_residence_owner_upload
GROUP BY _import_operation;



-- check with cpe

select * 
from crm_cp_email_sfid_prod cpe
inner join mig_residence_owner_upload rw
	on cpe.EmailAddress = rw.Email 
where rw.person_contact_id is null
and _import_operation = 'create'



select max(cluster_id) from gms_all_profiles gap where cluster_id is null


select sfa.FirstName, sfa.LastName, rw.first_name, rw.last_name, sfa.PersonEmail, sfa.ClusterID__C

from  mig_residence_owner_upload rw
inner join crm_person_account_sfid_prod sfa
	on rw.Email = sfa.PersonEmail 
where sfa.FirstName <> rw.first_name 


UPDATE mig_residence_owner_upload rw
INNER JOIN crm_person_account_sfid_prod sfa
    ON rw.Email = sfa.PersonEmail
SET rw._import_operation = 'none'
WHERE sfa.FirstName <> rw.first_name;

update mig_residence_owner_upload mrou 


select * from mig_residence_owner_upload rw
select * from gms_all_profiles gap where cluster_id = '733079'

update gms_all_profiles gap set cluster_id = '6290536' where email = 'dubravkozuzinjak@gmail.com'


update mig_residence_owner_upload rw set cluster_id = '6036639' where email = 'petrakacmarikova@gmail.com'


select * 
from mig_residence_owner_upload rw
inner join crm_cp_email_sfid_prod cpe
	on rw.Email = cpe.EmailAddress 
where rw._import_operation = 'create' 
and person_account_id is not null

select * from crm_person_account_sfid_prod where PersonEmail = 'dubravkozuzinjak@gmail.com'
select * from mig_residence_owner_upload rw where email = 'dubravkozuzinjak@gmail.com'
select * from mig_raw_crm_reservations_clean where reservation_id = '8506296'

select * from gms_all_profiles gap where gap.email = 'vadimv@autotrend.ua'


select ClusterID__c, ClusterID__pc  
from crm_person_account_sfid_prod 
where  ClusterID__c is null
and clusterID__pc is not null


select id, acc.ClusterID__pc  
from mig_residence_owner_upload rw
inner join crm_person_account_sfid_prod acc
	on rw.Email = acc.PersonEmail 
where rw._import_operation = 'create' 
and person_account_id is not null


update mig_residence_owner_upload rw
set _import_operation = 'update'
where _import_operation = 'create'
and person_account_id is not null


select rw.cluster_id, acc.ClusterID__c, acc.ClusterID__pc, acc.Id, rw.*  
from mig_residence_owner_upload rw
inner join crm_cp_email_sfid_prod cpe
	on cpe.EmailAddress = rw.Email
left join crm_person_account_sfid_prod acc
	on acc.PersonContactId = cpe.PartyID__c  
where _import_operation = 'create'


select * from gms_all_profiles gap where email = 't.baron@aon.at'


select rw.*  
from mig_residence_owner_upload rw
inner join  gms_all_profiles gp
	on rw.Email = gp.email
where rw. _import_operation = 'create'
and rw.cluster_id is null


select cast(ClusterID__c as UNSIGNED) 
from crm_person_account_sfid_prod
where ClusterID__pc <> 'TEST-UPSERT-001'
order by 1 desc

select cluster_id 
from gms_all_profiles gap
order by 

-- Update NULL cluster_id values with sequential numbers starting at 9850000
SET @seq := 9849999;

UPDATE mig_residence_owner_upload
SET cluster_id = (@seq := @seq + 1)
WHERE cluster_id IS NULL
ORDER BY email;   -- deterministic order; swap for whatever ordering you prefer

-- Sanity check
SELECT
    COUNT(*)                                     AS total_rows,
    COUNT(cluster_id)                            AS with_cluster,
    MIN(CAST(cluster_id AS UNSIGNED))            AS min_assigned,
    MAX(CAST(cluster_id AS UNSIGNED))            AS max_assigned
FROM mig_residence_owner_upload
WHERE cluster_id REGEXP '^[0-9]+$';




select 
		cluster_id,
		source,
		investor_status,
		investor_expiration_date,
		first_name as clean_first_name,
		last_name as clean_last_name,
		Salutation as salutation,
		email as clean_email,
		preferred_language as language,
		gender
from mig_residence_owner_upload
where _import_operation = 'create'



-- Create new table from the SELECT (structure + data in one go)
CREATE TABLE mig_residence_owner_upload_create AS
SELECT 
    cluster_id,
    source,
    investor_status,
    investor_expiration_date,
    first_name              AS clean_first_name,
    last_name               AS clean_last_name,
    Salutation              AS salutation,
    email                   AS clean_email,
    preferred_language      AS language,
    gender
FROM mig_residence_owner_upload
WHERE _import_operation = 'create';

-- Sanity check
SELECT COUNT(*) AS row_count FROM mig_residence_owner_upload_create;

SELECT * FROM mig_residence_owner_upload_create LIMIT 10;




-- Create new table from the SELECT (structure + data in one go)
CREATE TABLE mig_residence_owner_upload_update AS
SELECT 
    cluster_id,
    -- source,
    investor_status,
    investor_expiration_date
    -- first_name              AS clean_first_name,
    -- last_name               AS clean_last_name,
    -- Salutation              AS salutation,
    -- email                   AS clean_email,
    -- preferred_language      AS language,
    -- gender
FROM mig_residence_owner_upload
WHERE _import_operation = 'update';

-- Sanity check
SELECT COUNT(*) AS row_count FROM mig_residence_owner_upload_update;

SELECT * FROM mig_residence_owner_upload_update LIMIT 10;


select * from mig_residence_owner_upload where cluster_id in (6079170, 9850057 )


SELECT _import_operation, count(*) 
FROM mig_residence_owner_upload 
group by _import_operation




select  rw.cluster_id, rw.data_use_purpose, rw.consent_type, cpe.id as cpe_id, rw.source 
from mig_residence_owner_upload rw
inner join crm_cp_email_sfid_prod cpe
	on cpe.EmailAddress = rw.Email
inner join crm_person_account_sfid_prod acc
	on acc.PersonContactId = cpe.PartyID__c  
where _import_operation <> 'none'

drop table mig_residence_owner_upload_consent
CREATE TABLE mig_residence_owner_upload_consent AS
SELECT
    rw.cluster_id,
    rw.data_use_purpose,
    rw.consent_type,
    cpe.id          AS cpe_id,
    rw.source,
    rw.email
FROM       mig_residence_owner_upload   rw
INNER JOIN crm_cp_email_sfid_prod       cpe ON cpe.EmailAddress    = rw.Email
INNER JOIN crm_person_account_sfid_prod acc ON acc.PersonContactId = cpe.PartyID__c
WHERE rw._import_operation <> 'none';


select * from mig_residence_owner_upload_consent



select 1
from crm_consent_sfid_prod c
where c.Name = 'residences_central'
 and Id_cpe = 

/* 20.05.2026 nachtrag consent  */
 
 
 
CREATE TABLE mig_residence_owner_upload_consent_20260520 AS
SELECT
    pc.ClusterID__pc as cluster_id,
    '0ZWTe0000000XATOA2' as data_use_purpose,
    'residences_central' as consent_type,
    cpe.id          AS cpe_id,
    'Excel Owner' as source,
    cpe.EmailAddress as email 

from crm_person_account_sfid_prod pc
inner join crm_cp_email_sfid_prod cpe
	on cpe.PartyID__c = pc.PersonContactId 
where InvestmentStatus__pc = 'Owner'
and not exists (select *
					from crm_consent_sfid_prod c
					where c.Name = 'residences_central'
					 and c.Id_cpe = cpe.Id )
    

					 
					 
					 select * from mig_residence_owner_upload 
    
select *
from crm_consent_sfid_prod c
inner join mig_residence_owner_upload o
	on c.PersonContactId = o.person_contact_id
where Name like '%residences%'


select *
from crm_person_account_sfid_prod pc
inner join crm_cp_email_sfid_prod cpe
	on cpe.PartyID__c = pc.PersonContactId 
where InvestmentStatus__pc = 'Owner'
and not exists (select *
					from crm_consent_sfid_prod c
					where c.Name = 'residences_central'
					 and c.Id_cpe = cpe.Id )	



					 
					 

select * from mig_residence_owner_upload where Email = 'zdravko.zetovic@gmail.com'

