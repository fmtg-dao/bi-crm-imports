




calculated_arrangement = 
    SUMMARIZE
    (
        Arrangement_Daily
    ,
        Arrangement_Daily[Protel Hotel]
        , Arrangement_Daily[Revenue date]
        , Arrangement_Daily[Reservation_date]  
        , Arrangement_Daily[Market_Code]
        , Arrangement_Daily[Source_Code]
        , Arrangement_Daily[ARR_reschar]
        , Arrangement_Daily[ISO_country]
        , Arrangement_Daily[leadtime_days]
        , Arrangement_Daily[stay_length_days]
        , Arrangement_Daily[repeater]
        , Arrangement_Daily[guest_age]
        , Arrangement_Daily[ao_fam]
        , Arrangement_Daily[guest_gender]
        , Arrangement_Daily[guest_RFMNr]
        , Arrangement_Daily[spirit_club_tier]
        , Arrangement_Daily[kat]
        , Arrangement_Daily[Rate code]
    )
    
    
    
    
select * from V2V_ArrangementDaily


select * from V2V_ArrangementReservation


select * from V2V_GuestProfile


select * from V2V_ProtelRoomCategories


select * from V2V_StaysGuestRepeater


select * from V2V_GuestDistribution



select * from V2V_GuestFutureDaily_SelectedImpDates


select * from V2V_GuestFutureReservation_SelectedImpDates


select * from V2V_OTB_aggregated



ALTER TABLE `FMT_Reporting`.`V2I_GuestFutureDaily`       ADD INDEX ix_gfd_imp (GFD_datumimp, GFD_leistacc, GFD_katnr);
ALTER TABLE `FMT_Reporting`.`V2I_GuestFutureReservation` ADD INDEX ix_gfr_imp_leist (GFR_datumimp, GFR_leistacc);
ALTER TABLE `FMT_Reporting`.`V2C_GuestStays`             ADD INDEX ix_gs_leistacc (GS_leistacc);   -- hinter V2V_StaysGuestRepeater
-- Profil-/Kategorie-Basistabellen auf GP_kdnr bzw. katnr indexieren, falls nicht vorhanden