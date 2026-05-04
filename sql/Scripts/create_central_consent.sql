

/* Update Central Consent  */

select * 
from crm_cp_consent_sfid_prod con
inner join crm_cp_email_sfid_prod eml 
	on con.ContactPointId  = eml.Id

select count(*) 
from crm_cp_email_sfid_prod cpe
inner join  gustaffo_newsletter_contacts gus 
	on gus.email = cpe.EmailAddress
where not EXISTS ( 
			select 1 from crm_cp_consent_sfid_prod con where con.ContactPointId  = cpe.Id and con.Name = 'marketing_central'
) 



select * from gustaffo_newsletter_contacts

select * from gms_all_profiles gap 

-- update gms_all_profiles gap 

select *
from gms_all_profiles gap
inner join gustaffo_newsletter_contacts gfc
	on gap.email = gfc.email
where gap.central_consent = 0
	

update gms_all_profiles gap
inner join gustaffo_newsletter_contacts gfc
	on gap.email = gfc.email
set gap.central_consent = 1
where gap.central_consent = 0



/**** query for sales force central consent from gustaffo ***/

select * from crm_cp_email_sfid_prod cpe

select count(*) 
from crm_cp_email_sfid_prod cpe
inner join  gustaffo_newsletter_contacts gus 
	on gus.email = cpe.EmailAddress
where not EXISTS ( 
			select 1 from crm_cp_consent_sfid_prod con where con.ContactPointId  = cpe.Id and con.Name = 'marketing_central'
) 
and left(cpe.PartyID__c,3) = '003' 


select 'gustaffo' as 'source', cpe.id  as cpe_id
from crm_cp_email_sfid_prod cpe
inner join  gustaffo_newsletter_contacts gus 
	on gus.email = cpe.EmailAddress
where not EXISTS ( 
			select 1 from crm_cp_consent_sfid_prod con where con.ContactPointId  = cpe.Id and con.Name = 'marketing_central'
) 
and left(cpe.PartyID__c,3) = '003' 

