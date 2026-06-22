

select * from   crm_loyality_sfid_prod l

select  l.LegacyTier__c, count(*) 
from crm_loyality_sfid_prod l
where l.LegacyTier__c is not null 
and l.LegacyTier__c not in (
	'Blue Spirit Club', 
	'Blue Spirit Club',
	'Crystal Spirit Club',
	'Diamond Spirit Club',
	'Direct Booking Club',
	'Gold Spirit Club',
	'Ambassador Spirit Club'	
)	
group by l.LegacyTier__c 


select  l.LegacyTier__c, count(*) 
from crm_loyality_sfid_prod l
group by l.LegacyTier__c 


select * from crm_imp_person_accounts 

INSERT INTO crm_imp_person_accounts (
    _operation, 
    _batch_id,
    source,
    last_name,
    sf_loyalty_member_id,
    loyalty_legacy_tier
)
SELECT
    'update'                                 AS _operation,
    'legacy_tier_correction_20260608'  AS _batch_id,
    'sabrina_email' as source,
    '-' as last_name,
    Id as sf_loyalty_member_id,
	CASE l.LegacyTier__c 
	    WHEN 'GoldSpirit Club' THEN 'Gold Spirit Club'
	    WHEN 'Gold'            THEN 'Gold Spirit Club'
	    WHEN 'BlueSpirit Club' THEN 'Blue Spirit Club'
	    WHEN 'Blue'            THEN 'Blue Spirit Club'
	    WHEN 'Ambassador'      THEN 'Ambassador Spirit Club'
	    WHEN 'Diamond'         THEN 'Diamond Spirit Club'   
	    ELSE l.LegacyTier__c 
	END as loyalty_legacy_tier
	
from crm_loyality_sfid_prod l
where l.LegacyTier__c is not null 
and l.LegacyTier__c not in (
	'Blue Spirit Club', 
	'Blue Spirit Club',
	'Crystal Spirit Club',
	'Diamond Spirit Club',
	'Direct Booking Club',
	'Gold Spirit Club',
	'Ambassador Spirit Club'	
)	



select loyalty_legacy_tier, count(*)
from crm_imp_person_accounts 
where _batch_id =  'legacy_tier_correction_20260608'
group by loyalty_legacy_tier

