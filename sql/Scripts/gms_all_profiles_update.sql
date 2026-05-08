

/** create new table for updated gms data **/
CREATE TABLE gms_all_profiles_v2 
LIKE gms_all_profiles;


select count(*) from gms_all_profiles_v2 

4412433
4908433

select * from gms_all_profiles where email = 'nth@weyland.at'

select * from gms_all_profiles gap where gap.lname = 'Windbichler' and gap.fname = 'Alexander'


-- 488414050

select * from gms_loyalty_liability gll 
where list_id in ('412065589', '693816744', '804691964')


select * from gms_loyalty_liability gll 
where lname = 'Mayer' and fname= 'Gabriele'

select * from gms_loyalty_liability gll 
where gll.member_number  = 'awindbichler@anexia.com'


select * from gms_all_profiles gap 
where list_id = '411816487'

