


select * 
from V2I_GuestHistoryDaily_Detailed 
where  GHD_leistacc = 15666998


update V2I_GuestHistoryDaily_Detailed 
set GHD_anzkin3 = 0
where  GHD_leistacc = 15666998


select * 
from V2I_GuestHistoryDaily 
where  GHD_leistacc = 15666998


update V2I_GuestHistoryDaily 
set GHD_anzkin3 = 0
where  GHD_leistacc = 15666998


select * from  V2V_GuestHistoryReservation where `Reservation number` = 15666998