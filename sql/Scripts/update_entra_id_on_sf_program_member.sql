

-- 1. On crm_loyality_sfid_prod: covers WHERE filter + join column
CREATE INDEX idx_sfl_entraid_contact 
    ON crm_loyality_sfid_prod (EntraID__c, ContactId);

-- 2. On crm_person_account_sfid_prod: covers both join sides
CREATE INDEX idx_sfc_contact_email 
    ON crm_person_account_sfid_prod (PersonContactId, PersonEmail);

-- 3. On mig_loyality_entra_id_3: covers the email join
CREATE INDEX idx_etr_email 
    ON mig_loyality_entra_id_4 (email);


select count(*) 
from mig_loyality_entra_id_4


select sfl.ExternalMemberId__c as member_number_new, etr.object_id  as sf_entra_id 
from crm_loyality_sfid_prod sfl
inner join crm_person_account_sfid_prod sfc
	on sfl.ContactId = sfc.PersonContactId
inner join mig_loyality_entra_id_4 etr
	on etr.email = sfc.PersonEmail  
where sfl.EntraID__c is null

select * from crm_person_account_sfid_prod cpasp 
select * from crm_loyality_sfid_prod sfl where EntraID__c is null

select sfl.ExternalMemberId__c as member_number_new, sfc.FirstName, sfc.LastName, sfl.*
from crm_loyality_sfid_prod sfl
inner join crm_person_account_sfid_prod sfc
	on sfl.ContactId = sfc.PersonContactId
where sfl.EntraID__c is null