






select 
		coalesce(RCO_plan_name, RCO_crs_code) as Name,
		RCO_crs_code as RateCode__c,
		RCO_category as RateCodeCategory__c,
		RCO_market_segment as RateType__c,
		RCO_thematik as RateDescription__c
from V2I_RateCodeOverview