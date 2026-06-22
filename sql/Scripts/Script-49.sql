

select * from V2V_GuestHistoryDaily


select * from V2V_GuestHistoryReservation



select * from V2V_past_aggregated


SELECT
    ID,
    USER,
    HOST,
    DB,
    TIME,
    STATE,
    INFO
FROM information_schema.PROCESSLIST
WHERE COMMAND != 'Sleep'
ORDER BY TIME DESC;


kill 25463


ALTER TABLE `FMT_Reporting`.`V2I_GuestHistoryReservation` ADD INDEX ix_ghr_leist (GHR_leistacc);
ALTER TABLE `FMT_Reporting`.`V2I_GuestHistoryDaily`       ADD INDEX ix_ghd_leist_dat (GHD_leistacc, GHD_datum);
ALTER TABLE `FMT_Reporting`.`V2C_GuestStays`             ADD INDEX ix_gs_leist (GS_leistacc);
ALTER TABLE `FMT_Reporting`.`V2D_Property_Attributes`    ADD INDEX ix_pas_id (PAS_Protel_ID);
-- Profil-/Kategorie-Basis auf GP_kdnr bzw. katnr, falls nicht vorhanden