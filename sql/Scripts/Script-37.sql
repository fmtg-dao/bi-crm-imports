



select count(*) from mig_unsubscriber_cnl_2605_upload


select *
from mig_unsubscriber_cnl_2605_upload usb
left join crm_cp_email_sfid_prod cpe 
	on usb.Email = cpe.EmailAddress 
where 


select distinct Id as cpc_id
from mig_unsubscriber_cnl_2605_upload usb
inner join crm_consent_sfid_prod con 
	on usb.Email = con.EmailAddress 
	and con.Name = 'marketing_central'
where con.PrivacyConsentStatus = 'OptIn'
	and con.Name = 'marketing_central'
	and id = '0ZXTe0000000I9lOAE'
	
	
select *
from mig_unsubscriber_cnl_2605_upload usb
inner join crm_consent_sfid_prod con 
	on usb.Email = con.EmailAddress 
	and con.Name = 'marketing_central'
where con.PrivacyConsentStatus = 'OptIn'
	and con.Name = 'marketing_central'
	and id = '0ZXTe0000000I9lOAE'
	

select con.PrivacyConsentStatus, count(*) 
from mig_unsubscriber_cnl_2605_upload usb
left join crm_consent_sfid_prod con 
	on usb.Email = con.EmailAddress 
	and con.Name = 'marketing_central'
group by con.PrivacyConsentStatus





select crm_person_account_sfid_prod.PreferredLanguage__pc, count(*)
from crm_person_account_sfid_prod
group by crm_person_account_sfid_prod.PreferredLanguage__pc 


select PreferredLanguage__pc, count(*) 
from crm_person_account_sfid_prod
where BillingCountryCode__c = 'NL'
group by PreferredLanguage__pc 

select * from crm_person_account_sfid_prod


select count(*) from gms_all_profiles gap where gap.country = 'NL' and gap.language_code = 'nl'




select conda_uid, count(*) 
from stg_imp_invest_20260519
group by conda_uid 
order by 2 desc


select *  from stg_imp_invest_20260519

select distinct inv.* 
from stg_imp_invest_20260519 inv
left join crm_cp_email_sfid_prod cpe
	on cpe.EmailAddress = inv.email
left join crm_loyality_sfid_prod loy
	on cpe.PartyID__c = loy.ContactId
where  cpe.EmailAddress is null
and LEFT(cpe.ParentId,3) = '003' 
-- and loy.Id is not null




select *
from stg_imp_invest_20260519 inv
inner join crm_cp_email_sfid_prod cpe
	on cpe.EmailAddress = inv.email
inner join crm_person_account_sfid_prod acc
	on acc.PersonContactId = cpe.PartyID__c
inner join crm_loyality_sfid_prod loy
	on cpe.PartyID__c = loy.ContactId
where  cpe.EmailAddress is not null
and LEFT(cpe.PartyID__c,3) = '003' 
-- and loy.Id is not null


select * from crm_loyality_sfid_prod clsp 
select * from crm_cp_email_sfid_prod cpe


select * 
from crm_person_account_sfid_prod acc
where 









