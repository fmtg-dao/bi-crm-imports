



select count(*) 
from gms_all_profiles gap
where gap.cluster_id is not null



select count(*) 
from gms_all_profiles gap
where gap.cluster_id is not null

select count(*) 
from gms_all_profiles gap
where gap.current_opt_in = 'Yes'
and gap.cluster_id is not null

select *
from gms_all_profiles gap
where gap.current_opt_in = 'Yes'
and gap.exclude_email = 0


select * from mig_raw_crm_reservations limit 100



select count(*)
from gms_all_profiles gap
where gap.current_opt_in = 'Yes'
and gap.exclude_email = 0
and exists ( 
		select 1 
		from mig_crm_person_accounts_imp20260414 p
		where p.cluster_email = gap.email 
)



select count(*) 
select *
from mig_crm_person_accounts_imp20260414 
where central_consent = 1


select count(*)
from mig_raw_crm_reservations_clean r
where r.reservation_status not in ('CheckedOut', 'Cancelled')
and r.market_segment in ('INDIVIDUAL', 'OTA')
and email is not null
and email not in ('admin@motorhometours.co.uk')
and r.sf_property_id is not null
and r.sf_person_contact_id is null
and r.is_investor = 0
and r.booking_id is not null
and r.arrival_at > '2026-04-16'
and not exists ( select 1 
					from mig_raw_crm_reservations_hist_imp20260414 i where r.reservation_id = i.reservation_id  )
and not exists ( select 1 
					from mig_raw_crm_reservations_future_imp20260414 f where r.reservation_id = f.reservation_id  )
					
					
select * from mig_crm_person_accounts mcpa 
where mcpa.cluster_id = 92368

select * from mig_raw_crm_reservations_clean mrcrc where mrcrc.email = 'eduard.gerum@t-online.de'


select * from crm_person_account_sfid_prod

select count(*) 
from gms_all_profiles gap 
where not exists (select 1 from gms_reservations gr 
					where gap.list_id = gr.list_id) 
and gap.exclude_email = 0
and gap.current_opt_in = 'Yes'


select count(*) 
from gms_all_profiles gap 
where not exists (select 1 from mig_crm_person_accounts a
					where a.clean_email = gap.email ) 
and gap.exclude_email = 0
and gap.bounce_flag = 0
and gap.current_opt_in = 'Yes'



select * from gms_reservations


select count(*) 
from gms_all_profiles gap 


;

select * from crm_reservation_sfid_prod

ALTER TABLE `crm_reservation_sfid_prod`
ADD INDEX `idx_ReservationID__c` (`ReservationID__c`);

ALTER TABLE `mig_raw_crm_reservations_iw`
ADD INDEX `idx_reservation_id` (`reservation_id`);

ALTER TABLE `crm_person_account_sfid_prod`
ADD INDEX `idx_PersonEmail` (`PersonEmail`);


ALTER TABLE `crm_person_account_sfid_prod`
ADD INDEX `idx_Id` (`Id`);

ALTER TABLE `crm_person_account_sfid_prod`
ADD INDEX `idx_PersonContactId` (`PersonContactId`);

ALTER TABLE `crm_cp_email_sfid_prod`
ADD INDEX `idx_EmailAddress` (`EmailAddress`);

select count(*) from crm_consent_sfid_prod where Name = 'marketing_central'

select ContactPointId, count(*)
from crm_consent_sfid_prod 
where Name = 'marketing_central'
group by ContactPointId having count(*) > 1


select distinct Id as cpe_id, 'gms' as 'source', cpe.EmailAddress
select count(*)
from crm_cp_email_sfid_prod cpe
inner join gms_all_profiles gap 
	on gap.email = cpe.EmailAddress
where gap.current_opt_in = 'Yes'
and not exists (select 1 from crm_consent_sfid_prod c 
					where Name = 'marketing_central' 
					and cpe.EmailAddress = c.EmailAddress
					)


select count(*) 
from crm_person_account_sfid_prod sfp
inner join gms_all_profiles gap 
	on gap.email = sfp.PersonEmail 
where gap.email is not null
	and gap.current_opt_in = 'Yes'
and not exists (select 1 from crm_consent_sfid_prod c where Name = 'marketing_central' and c.EmailAddress = sfp.PersonEmail  )


select count(*) from crm_reservation_sfid_prod

select count(*)
from mig_raw_crm_reservations_clean   pr
left join crm_reservation_sfid_prod sr
	on sr.ReservationID__c = pr.reservation_id
where sr.ReservationID__c is not null


select count(distinct acc.cluster_id ) 
select * 
from mig_crm_person_accounts acc 
where cluster_id is not null
and not exists ( select 1 
				 from crm_person_account_sfid_prod psf 
				 where psf.ClusterID__pc = acc.cluster_id)


				 

select count(*) 
from gms_all_profiles gap
left join crm_person_account_sfid_prod a
	on gap.email = a.PersonEmail 
where a.PersonEmail is not null




select *
from gms_all_profiles gap
where gap.exclude_email = 0
and gap.current_opt_in = 'Yes'
and lname is null 
and exists (select 1 from gms_reservations gr where gr.list_id = gap.list_id)


select `domain` , count(*)
from gms_all_profiles gap
where gap.exclude_email = 0
and gap.current_opt_in = 'Yes'
and email like '%info%'
group by `domain` 
order by 2 desc


select *
from gms_all_profiles gap
where gap.exclude_email = 0
and gap.current_opt_in = 'Yes'
and email like '%info%'
 

select * from gms_reservations gr where list_id = '412328946'


select * from mig_raw_crm_reservations_clean mrcrc where mrcrc.reservation_id = '13989526'



select count(distinct cluster_id) 
select *
from mig_crm_person_accounts
where cluster_id is not null
  AND cluster_last_name is not null
  AND cluster_email is not null
  AND cluster_first_name is not null

  
  
  
  select cpe.EmailAddress from crm_cp_email_sfid_prod cpe
  

CREATE TABLE mig_crm_person_accounts_imp20260419 AS
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
      -- AND t.clean_email not in ( select distinct cpe.EmailAddress from crm_cp_email_sfid_prod cpe)
)
SELECT
    *
FROM ranked
WHERE rn = 1;


select count(*)
from mig_crm_person_accounts_imp20260419 t
where exists ( select 1 from crm_cp_email_sfid_prod cpe where cpe.EmailAddress = t.cluster_email )


select t.cluster_email, count(*)
from mig_crm_person_accounts_imp20260419 t
group by t.cluster_email having count(*)= 1
order by 2 desc


select * from mig_crm_person_accounts_imp20260419 where cluster_email = 'p.hahn@apotronik.at'




