SELECT * 
FROM V2I_FXRates 
WHERE FX_cur3 IN ('CZK','RSD') 
	AND FX_year=2026 
	AND FX_month >= 1
	
	
update V2I_FXRates vif 
set FX_avg = 24.3660
WHERE FX_cur3 IN ('CZK') 
  AND FX_year=2026 
  AND FX_month >= 5
  
 
update V2I_FXRates vif 
set FX_avg = 117.3952 
WHERE FX_cur3 IN ('RSD') 
  AND FX_year=2026 
  AND FX_month >= 5
  
