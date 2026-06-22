CREATE OR REPLACE VIEW `FMT_Reporting`.`V2V_CalculatedArrangement` AS
SELECT
  a.`Protel Hotel`, a.`Revenue date`, a.`Rate code`,
  r.`Reservation date`              AS `Reservation_date`,
  r.`Market Code`                   AS `Market_Code`,
  r.`Source Code`                   AS `Source_Code`,
  r.`ARR_reschar`,
  LEFT(r.`Guest country of residence ZipCodeKey`,2) AS `ISO_country`,
  r.`Lead time reservation_arrival` AS `leadtime_days`,
  r.`Length of stay`                AS `stay_length_days`,
  r.`Spiritclub Tier`               AS `spirit_club_tier`,
  -- Keys für Modell-Dims (statt Labels):
  a.`ARD_katnr`                     AS `katnr`,            -- -> Protel_Room_Categories
  g.`GP_gender`                     AS `gender_code`,      -- -> guest_gender[GD_Nr]
  a.`Guest_distribution`,                                  -- -> guest_group_structure
  g.`GP_RFMNr`                      AS `guest_RFMNr`,
  CASE WHEN COALESCE(g.`GP_DOB`,19000101)=19000101 THEN NULL
       ELSE FLOOR(r.`Arrival date`/10000) - FLOOR(g.`GP_DOB`/10000) END AS `guest_age`,
  CASE WHEN COALESCE(sgr.`actual_repeater`,0)=0 THEN '0'
       WHEN (FLOOR(sgr.`arrival_date`/10000)-FLOOR(sgr.`previous_stay_date`/10000))<10
            THEN CONCAT('1-0', FLOOR(sgr.`arrival_date`/10000)-FLOOR(sgr.`previous_stay_date`/10000))
       ELSE CONCAT('1-',  FLOOR(sgr.`arrival_date`/10000)-FLOOR(sgr.`previous_stay_date`/10000))
  END                               AS `arrival_previous_year_diff`,  -- -> stay_repeat[L1]
  -- Aggregate (Filter aus OTB-Familie abgeleitet -> bestätigen):
  SUM(a.`Rev_Room_Net_EUR`)  AS `Room_rev`,
  SUM(a.`Rev_FB_Net_EUR`)    AS `FnB_rev`,
  SUM(a.`Rev_Other_Net_EUR`) AS `Other_extended`,
  SUM(CASE WHEN a.`Reservation status` NOT IN (3,-1) AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN a.`ARD_roomnights` END)                            AS `R_nights`,
  SUM(CASE WHEN a.`Reservation status` NOT IN (3,-1) AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN a.`Adults` END)                                    AS `Adult_nights`,
  SUM(CASE WHEN a.`Reservation status` NOT IN (3,-1) AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN (a.`Child1`+a.`Child2`+a.`Child3`+a.`Child4`) END) AS `Children_nights`,
  SUM(CASE WHEN a.`Reservation status` NOT IN (3,-1) AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN (a.`Adults`+a.`Child1`+a.`Child2`+a.`Child3`+a.`Child4`) END) AS `Total_guest_nights`,
  SUM(CASE WHEN a.`Reservation status`=1 AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN a.`Adults` END)                                                AS `Arrival_adults`,
  SUM(CASE WHEN a.`Reservation status`=1 AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN (a.`Child1`+a.`Child2`+a.`Child3`+a.`Child4`) END)             AS `Arrival_children`,
  SUM(CASE WHEN a.`Reservation status`=1 AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN (a.`Adults`+a.`Child1`+a.`Child2`+a.`Child3`+a.`Child4`) END)  AS `Arrival_total_guests`,
  COUNT(DISTINCT CASE WHEN a.`Reservation status`=1 AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 THEN a.`Reservation number` END)                         AS `No_of_rooms`,
  COUNT(DISTINCT CASE WHEN a.`Reservation status`=1 AND r.`BoolGuestRoom`=1 AND a.`Rate code typ`<>4 AND r.`Main_res`=1 THEN a.`Reservation number` END)      AS `Reservations`
FROM `FMT_Reporting`.`V2V_ArrangementDaily` a
JOIN      `FMT_Reporting`.`V2V_ArrangementReservation` r ON r.`Reservation number` = a.`Reservation number`
LEFT JOIN `FMT_Reporting`.`V2V_GuestProfile`           g ON g.`GP_kdnr`           = r.`Guest profile number`
LEFT JOIN `FMT_Reporting`.`V2V_StaysGuestRepeater`   sgr ON sgr.`leistacc`        = a.`Reservation number`
GROUP BY
  a.`Protel Hotel`, a.`Revenue date`, a.`Rate code`,
  `Reservation_date`, `Market_Code`, `Source_Code`, r.`ARR_reschar`, `ISO_country`,
  `leadtime_days`, `stay_length_days`, `spirit_club_tier`,
  `katnr`, `gender_code`, a.`Guest_distribution`, `guest_RFMNr`, `guest_age`, `arrival_previous_year_diff`;