



select count(distinct email) from mig_aleno_contacts
where newsletter = 1 





select count(*) from mig_aleno_contacts
where newsletter = 1 


select * 
from gold_consolidated_segment_report
where property_fmtg_id in (500410,500420, 500421, 500510, 500511, 500540, 500541, 520010, 520011, 50012, 50020, 50040)
and rev_year_month = '2026-04'