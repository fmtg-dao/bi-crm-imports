CREATE OR REPLACE VIEW `FMT_Reporting`.`V2V_OTB_aggregated` AS
WITH imp AS (SELECT MAX(GFD_datumimp) AS dmax FROM `FMT_Reporting`.`V2I_GuestFutureDaily`),
sel AS (
    SELECT dmax                    AS imp_date, 1 AS code FROM imp
    UNION ALL SELECT dmax - INTERVAL 1 YEAR,  2 FROM imp
    UNION ALL SELECT dmax - INTERVAL 1 DAY,   3 FROM imp
    UNION ALL SELECT dmax - INTERVAL 7 DAY,   4 FROM imp
)
SELECT
  d.GFD_mpehotel  AS `Protel Hotel`,
  d.GFD_datum     AS `Revenue date actual`,
  r.GFR_datumres  AS `Reservation_date`,
  r.GFR_market    AS `Market_Code`,
  r.GFR_source    AS `Source_Code`,
  r.GFR_reschar   AS `GFR_reschar`,
  LEFT(r.GFR_res_zipcodekey, 2) AS `ISO_country`,
  CASE WHEN r.GFR_datumres='1900-01-01' OR r.GFR_datumres > r.GFR_datumvon THEN 0
       ELSE TIMESTAMPDIFF(DAY, r.GFR_datumres, r.GFR_datumvon) END AS `leadtime_days`,
  CASE WHEN r.GFR_datumvon > r.GFR_datumbis THEN 0
       ELSE TIMESTAMPDIFF(DAY, r.GFR_datumvon, r.GFR_datumbis) END AS `stay_length_days`,
  CASE WHEN sgr.leistacc IS NULL THEN NULL          -- keine StaysGuestRepeater-Zeile -> blank (wie Modell)
       WHEN sgr.actual_repeater=0 THEN '0'
       WHEN (FLOOR(sgr.arrival_date/10000)-FLOOR(sgr.previous_stay_date/10000))<10
            THEN CONCAT('1-0', FLOOR(sgr.arrival_date/10000)-FLOOR(sgr.previous_stay_date/10000))
       ELSE CONCAT('1-',  FLOOR(sgr.arrival_date/10000)-FLOOR(sgr.previous_stay_date/10000))
  END AS `repeater`,
  CASE WHEN COALESCE(g.GP_DOB,19000101)=19000101 THEN NULL ELSE 2020 - FLOOR(g.GP_DOB/10000) END AS `guest_age`,
  CASE WHEN d.GFD_anzerw>0 AND d.GFD_kbett=0 AND (d.GFD_anzkin1+d.GFD_anzkin2+d.GFD_anzkin3+d.GFD_anzkin4)=0 THEN 'adults only'
       WHEN d.GFD_anzerw>0 AND (d.GFD_kbett>0 OR (d.GFD_anzkin1+d.GFD_anzkin2+d.GFD_anzkin3+d.GFD_anzkin4)>0) THEN 'family'
       ELSE 'other' END AS `ao_fam`,
  CASE g.GP_gender WHEN -1 THEN 'company' WHEN 1 THEN 'male' WHEN 2 THEN 'female' ELSE NULL END AS `guest_gender`,
  g.GP_RFMNr      AS `guest_RFMNr`,
  r.GFR_spirit    AS `spirit_club_tier`,
  COALESCE(prc.kat,'99') AS `kat`,
  d.GFD_preistypgr AS `Rate code`,
  FLOOR(g.GP_DOB/10000)  AS `guest_birth_year`,
  CASE WHEN r.GFR_firmennr>0 THEN r.GFR_firmennr WHEN r.GFR_reisenr>0 THEN r.GFR_reisenr
       WHEN r.GFR_gruppennr>0 THEN r.GFR_gruppennr ELSE 0 END AS `sales_contact_kdnr`,
  sel.code        AS `Imp_Date_Selection`,
  SUM(d.GFD_n_logis_EUR) AS `Room_rev`,
  SUM(d.GFD_n_fb_EUR)    AS `FnB_rev`,
  SUM(d.GFD_n_other_EUR) AS `Other_extended`,
  SUM(CASE WHEN d.GFD_resstatus NOT IN (3,-1) AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN d.GFD_roomnights END) AS `R_nights`,
  SUM(CASE WHEN d.GFD_resstatus=1 AND r.GFR_zimmer=1 AND d.GFD_typ<>4 AND r.GFR_leistacc=r.GFR_sharenr THEN 1 ELSE 0 END) AS `Reservations`,
  SUM(CASE WHEN d.GFD_resstatus NOT IN (3,-1) AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN d.GFD_anzerw END) AS `Adult_nights`,
  SUM(CASE WHEN d.GFD_resstatus NOT IN (3,-1) AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN (d.GFD_anzkin1+d.GFD_anzkin2+d.GFD_anzkin3+d.GFD_anzkin4) END) AS `Children_nights`,
  SUM(CASE WHEN d.GFD_resstatus NOT IN (3,-1) AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN (d.GFD_anzerw+d.GFD_anzkin1+d.GFD_anzkin2+d.GFD_anzkin3+d.GFD_anzkin4) END) AS `Total_guest_nights`,
  SUM(CASE WHEN d.GFD_resstatus=1 AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN d.GFD_anzerw END) AS `Arrival_adults`,
  SUM(CASE WHEN d.GFD_resstatus=1 AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN (d.GFD_anzkin1+d.GFD_anzkin2+d.GFD_anzkin3+d.GFD_anzkin4) END) AS `Arrival_children`,
  SUM(CASE WHEN d.GFD_resstatus=1 AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN (d.GFD_anzerw+d.GFD_anzkin1+d.GFD_anzkin2+d.GFD_anzkin3+d.GFD_anzkin4) END) AS `Arrival_total_guests`,
  SUM(CASE WHEN d.GFD_resstatus=1 AND r.GFR_zimmer=1 AND d.GFD_typ<>4 THEN 1 ELSE 0 END) AS `No_of_rooms`
FROM `FMT_Reporting`.`V2I_GuestFutureDaily` d
JOIN sel ON d.GFD_datumimp = sel.imp_date
LEFT JOIN `FMT_Reporting`.`V2I_GuestFutureReservation` r ON r.GFR_leistacc = d.GFD_leistacc AND r.GFR_datumimp = d.GFD_datumimp
LEFT JOIN `FMT_Reporting`.`V2V_GuestProfile`         g   ON g.GP_kdnr   = r.GFR_kundennr
LEFT JOIN `FMT_Reporting`.`V2V_StaysGuestRepeater`   sgr ON sgr.leistacc= d.GFD_leistacc
LEFT JOIN `FMT_Reporting`.`V2V_ProtelRoomCategories` prc ON prc.katnr   = d.GFD_katnr
WHERE (d.GFD_mpehotel <> 34 OR d.GFD_datum <= '2025-10-16' OR d.GFD_datumimp <= '2025-10-16')
GROUP BY
  d.GFD_mpehotel, d.GFD_datum, r.GFR_datumres, r.GFR_market, r.GFR_source, r.GFR_reschar,
  `ISO_country`, `leadtime_days`, `stay_length_days`, `repeater`, `guest_age`, `ao_fam`,
  `guest_gender`, g.GP_RFMNr, r.GFR_spirit, `kat`, d.GFD_preistypgr, `guest_birth_year`,
  `sales_contact_kdnr`, sel.code;