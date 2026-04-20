


select * from V2D_TAA


select * from V2D_Apaleo_grossTransactions vdagt 



select * from dim_account_mapping
select * from dim_BMD_account_mapping

select *
from raw_apaleo_services where description like '%ski%'


select          cluster_id,
                -- sf_contact_id,
                -- sf_account_id,
                -- source,
                clean_first_name as givenName,
                Clean_last_name as surname,
                clean_email as email,
                -- member_id as legacy_member_number,
                -- member_tier,
                member_number_new as member_number
                -- enrollment_date
        from mig_crm_person_accounts_imp20260414 
        where member_tier is not null 
        and sf_contact_id is not null 