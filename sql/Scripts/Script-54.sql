


select * 
from gms_all_profiles gap 
where fname = 'Matthias'
and lname = 'Hofmann'


hofmann.matthias@gmx.at


-- hofman papa 412598148   matthias.hofmann57@outlook.com



select * 
from gms_all_profiles gap 
where email = 'matthias.hofmann57@outlook.com'


select * 
from gms_all_profiles gap 
where email = 'hofmann.matthias@gmx.at'


select * 
from gms_loyalty_liability gll
where email = 'hofmann.matthias@gmx.at'



select * 
from gms_loyalty_liability gll
where email in ('matthias.hofmann57@outlook.com', 'hofmann.matthias@gmx.at')



select * 
from gms_all_profiles gap 
where list_id = 'hofmann.matthias@gmx.at'

