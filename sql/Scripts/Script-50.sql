CREATE OR REPLACE VIEW `FMT_Reporting`.`V2V_past_aggregated` AS
SELECT
  ghd.GHD_mpehotel AS `Protel Hotel`,
  ghd.GHD_datum    AS `Revenue date actual`,
  ghr.GHR_datumres AS `Reservation_date`,
  ghr.GHR_market   AS `Market_Code`,
  ghr.GHR_source   AS `Source_Code`,
  ghr.GHR_reschar  AS `GHR_reschar`,
  LEFT(ghr.GHR_res_zipcodekey, 2) AS `ISO_country`,
  CASE WHEN ghr.GHR_datumres='1900-01-01' OR ghr.GHR_datumres > ghr.GHR_datumvon THEN 0
       ELSE TIMESTAMPDIFF(DAY, ghr.GHR_datumres, ghr.GHR_datumvon) END AS `leadtime_days`,
  CASE WHEN ghr.GHR_datumvon > ghr.GHR_datumbis THEN 0
       ELSE TIMESTAMPDIFF(DAY, ghr.GHR_datumvon, ghr.GHR_datumbis) END AS `stay_length_days`,
  sr.repeater_code AS `repeater`,
  TIMESTAMPDIFF(YEAR, ghr.GHR_kunden_DOB, ghr.GHR_datumvon) AS `guest_age`,
  CASE WHEN ghd.GHD_anzerw>0 AND ghd.GHD_kbett=0 AND (ghd.GHD_anzkin1+ghd.GHD_anzkin2+ghd.GHD_anzkin3+ghd.GHD_anzkin4)=0 THEN 'adults only'
       WHEN ghd.GHD_anzerw>0 AND (ghd.GHD_kbett>0 OR (ghd.GHD_anzkin1+ghd.GHD_anzkin2+ghd.GHD_anzkin3+ghd.GHD_anzkin4)>0) THEN 'family'
       ELSE 'other' END AS `ao_fam`,
  CASE g.GP_gender WHEN -1 THEN 'company' WHEN 1 THEN 'male' WHEN 2 THEN 'female' ELSE NULL END AS `guest_gender`,
  g.GP_RFMNr       AS `guest_RFMNr`,
  ghr.GHR_spirit   AS `spirit_club_tier`,
  COALESCE(prc.kat,'99') AS `kat`,
  ghd.GHD_preistypgr AS `Rate code`,
  YEAR(ghr.GHR_kunden_DOB) AS `guest_birth_year`,
  CASE WHEN ghr.GHR_firmennr>0 THEN ghr.GHR_firmennr WHEN ghr.GHR_reisenr>0 THEN ghr.GHR_reisenr
       WHEN ghr.GHR_gruppennr>0 THEN ghr.GHR_gruppennr ELSE 0 END AS `sales_contact_kdnr`,
  SUM(ghd.GHD_n_logis_EUR) AS `Room_rev`,
  SUM(ghd.GHD_n_fb_EUR) + SUM(ghd.GHD_n_bqt_EUR) AS `FnB_rev`,
  SUM(ghd.GHD_n_other_EUR) + SUM(ghd.GHD_n_ski_EUR) + SUM(ghd.GHD_n_spa_EUR) AS `Other_extended`,
  SUM(CASE WHEN ghd.GHD_resstatus NOT IN (3,-1) AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN ghd.GHD_roomnights END) AS `R_nights`,
  SUM(CASE WHEN ghd.GHD_resstatus=1 AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 AND ghr.GHR_leistacc=ghr.GHR_sharenr THEN 1 ELSE 0 END) AS `Reservations`,
  SUM(CASE WHEN ghd.GHD_resstatus NOT IN (3,-1) AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN ghd.GHD_anzerw END) AS `Adult_nights`,
  SUM(CASE WHEN ghd.GHD_resstatus NOT IN (3,-1) AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN (ghd.GHD_anzkin1+ghd.GHD_anzkin2+ghd.GHD_anzkin3+ghd.GHD_anzkin4) END) AS `Children_nights`,
  SUM(CASE WHEN ghd.GHD_resstatus NOT IN (3,-1) AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN (ghd.GHD_anzerw+ghd.GHD_anzkin1+ghd.GHD_anzkin2+ghd.GHD_anzkin3+ghd.GHD_anzkin4) END) AS `Total_guest_nights`,
  SUM(CASE WHEN ghd.GHD_resstatus=1 AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN ghd.GHD_anzerw END) AS `Arrival_adults`,
  SUM(CASE WHEN ghd.GHD_resstatus=1 AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN (ghd.GHD_anzkin1+ghd.GHD_anzkin2+ghd.GHD_anzkin3+ghd.GHD_anzkin4) END) AS `Arrival_children`,
  SUM(CASE WHEN ghd.GHD_resstatus=1 AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN (ghd.GHD_anzerw+ghd.GHD_anzkin1+ghd.GHD_anzkin2+ghd.GHD_anzkin3+ghd.GHD_anzkin4) END) AS `Arrival_total_guests`,
  SUM(CASE WHEN ghd.GHD_resstatus=1 AND ghr.GHR_zimmer=1 AND ghd.GHD_typ<>4 THEN 1 ELSE 0 END) AS `No_of_rooms`
FROM `FMT_Reporting`.`V2I_GuestHistoryDaily` ghd
JOIN `FMT_Reporting`.`V2D_Property_Attributes` pas
       ON pas.PAS_Protel_ID = ghd.GHD_mpehotel AND pas.PAS_pms='protel' AND pas.is_active=1
LEFT JOIN `FMT_Reporting`.`V2I_GuestHistoryReservation` ghr
       ON ghr.GHR_leistacc = ghd.GHD_leistacc
      AND ghr.GHR_datumvon >= '2017-01-01' AND ghr.GHR_mpehotel NOT IN (38,39,48) AND ghr.GHR_datumres < CURDATE()
LEFT JOIN `FMT_Reporting`.`V2V_GuestProfile` g ON g.GP_kdnr = ghr.GHR_kundennr
LEFT JOIN (
    SELECT `leistacc`,
        CASE WHEN `actual_repeater`=0 THEN '0'
             WHEN (FLOOR(`arrival_date`/10000)-FLOOR(`previous_stay_date`/10000))<10
                  THEN CONCAT('1-0', FLOOR(`arrival_date`/10000)-FLOOR(`previous_stay_date`/10000))
             ELSE CONCAT('1-',  FLOOR(`arrival_date`/10000)-FLOOR(`previous_stay_date`/10000)) END AS `repeater_code`
    FROM `FMT_Reporting`.`V2V_StaysGuestRepeater`
) sr ON sr.`leistacc` = ghd.GHD_leistacc
LEFT JOIN `FMT_Reporting`.`V2V_ProtelRoomCategories` prc ON prc.katnr = ghr.GHR_katnr
WHERE ghd.GHD_datum > '2023-12-31'
GROUP BY
  ghd.GHD_mpehotel, ghd.GHD_datum, ghr.GHR_datumres, ghr.GHR_market, ghr.GHR_source, ghr.GHR_reschar,
  `ISO_country`, `leadtime_days`, `stay_length_days`, sr.repeater_code, `guest_age`, `ao_fam`,
  `guest_gender`, g.GP_RFMNr, ghr.GHR_spirit, `kat`, ghd.GHD_preistypgr, `guest_birth_year`, `sales_contact_kdnr`;