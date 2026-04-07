SELECT
    'protel' AS `source_system`,
    ghd.`GHD_datum`     AS `rev_date`,
    ghd.`GHD_mpehotel`  AS `protel_id`,
    pa.`PAS_code3`      AS `property_id`,
    pa.`PAS_name_short` AS `name_short`,
    CAST(IFNULL(SUM(ghd.`GHD_roomnights`), 0) AS UNSIGNED) AS `room_nights`,
    CAST(IFNULL(
        SUM(ghd.`GHD_anzerw`)
      + SUM(ghd.`GHD_anzkin1`)
      + SUM(ghd.`GHD_anzkin2`)
      + SUM(ghd.`GHD_anzkin3`)
      + SUM(ghd.`GHD_anzkin4`),
      0
    ) AS UNSIGNED) AS `bed_nights`,
    IFNULL(SUM(ghd.`GHD_anzerw`), 0) AS `adults_num`,
    CAST(IFNULL(
        SUM(ghd.`GHD_anzkin1`)
      + SUM(ghd.`GHD_anzkin2`)
      + SUM(ghd.`GHD_anzkin3`)
      + SUM(ghd.`GHD_anzkin4`),
      0
    ) as UNSIGNED) AS `children_num`
FROM `FMT_Reporting`.`V2I_GuestHistoryDaily` ghd
JOIN `FMT_Reporting`.`V2D_Property_Attributes` pa
  ON pa.`PAS_Protel_ID` = ghd.`GHD_mpehotel`
 AND pa.`PAS_pms` = 'protel'
WHERE ghd.`GHD_datum` >= '2025-02-01'
  AND ghd.`GHD_datum` <  '2025-03-01'
  AND ghd.`GHD_resstatus` IN (1, 2)
  AND ghd.`GHD_typ` <> 4
  AND EXISTS (
      SELECT 1
      FROM `FMT_Reporting`.`V2I_GuestHistoryReservation` res
      WHERE res.`GHR_leistacc` = ghd.`GHD_leistacc`
        AND res.`GHR_reschar` = 0
        AND res.`GHR_zimmer` = 1
        AND res.`GHR_datumcxl` = '1900-01-01'
  )
GROUP BY
    ghd.`GHD_datum`,
    ghd.`GHD_mpehotel`,
    pa.`PAS_code3`,
    pa.`PAS_name_short`

UNION ALL

SELECT
    'apaleo' AS `source_system`,
    x.`rev_date`     AS `rev_date`,
    x.`protel_id`    AS `protel_id`,
    x.`property_id`  AS `property_id`,
    x.`name_short`   AS `name_short`,
    CAST(SUM(x.`room_nights`) as UNSIGNED) AS `room_nights`,
    CAST(SUM(x.`adults_num` + x.`children_num`) as UNSIGNED) AS `bed_nights`,
    CAST(SUM(x.`adults_num`) as UNSIGNED) AS `adults_num`,
    CAST(SUM(x.`children_num`) as UNSIGNED) AS `children_num`
FROM (
    SELECT
        pro.`PAS_code3`      AS `property_id`,
        pro.`PAS_Protel_ID`  AS `protel_id`,
        pro.`PAS_name_short` AS `name_short`,
        ts.`service_date`    AS `rev_date`,
        1                    AS `room_nights`,
        COALESCE(rs.`adults`, 0) AS `adults_num`,
        COALESCE(chl.`num_children`, 0) AS `children_num`
    FROM `FMT_Reporting`.`raw_apaleo_reservations__time_slices` ts
    JOIN `FMT_Reporting`.`raw_apaleo_reservations` rs
      ON rs.`_dlt_id` = ts.`_dlt_parent_id`
    JOIN `FMT_Reporting`.`V2D_Property_Attributes` pro
      ON pro.`PAS_code3` = rs.`property__id`
     AND pro.`PAS_pms` = 'apaleo'
    JOIN `FMT_Reporting`.`raw_apaleo_unitgroups` ug
      ON ug.`id` = ts.`unit_group__id`
     AND ug.`type` = 'BedRoom'
    LEFT JOIN (
        SELECT
            ca.`_dlt_parent_id` AS `_dlt_parent_id`,
            COUNT(*) AS `num_children`
        FROM `FMT_Reporting`.`raw_apaleo_reservations__children_ages` ca
        GROUP BY ca.`_dlt_parent_id`
    ) chl
      ON chl.`_dlt_parent_id` = rs.`_dlt_id`
    WHERE ts.`service_date` >= '2025-02-01'
      AND ts.`service_date` < '2025-03-01'
      AND rs.`status` NOT IN ('Canceled', 'NoShow')
   
) x
GROUP BY
    x.`rev_date`,
    x.`protel_id`,
    x.`property_id`,
    x.`name_short`;