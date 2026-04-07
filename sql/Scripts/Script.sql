
DROP PROCEDURE IF EXISTS `FMT_Reporting`.`sp_exposure_eudt_occupancy_kpis_by_date`$$

CREATE PROCEDURE `FMT_Reporting`.`sp_exposure_eudt_occupancy_kpis_by_date`(
    IN p_date DATE
)
BEGIN
    /*
      Returns KPI rows in the format:
        pa.PAS_code3_KPINAME | Value | rev_date | source_system | protel_id | name_short

      Date logic:
        rev_date >= p_date
        rev_date <  p_date + 1 day
    */

    WITH base AS (

        /* =========================
           PROTEL
           ========================= */
        SELECT
            'protel' AS source_system,
            ghd.GHD_datum AS rev_date,
            ghd.GHD_mpehotel AS protel_id,
            pa.PAS_code3 AS property_id,
            pa.PAS_name_short AS name_short,
            IFNULL(SUM(ghd.GHD_roomnights), 0) AS room_nights,
            IFNULL(
                SUM(ghd.GHD_anzerw)
              + SUM(ghd.GHD_anzkin1)
              + SUM(ghd.GHD_anzkin2)
              + SUM(ghd.GHD_anzkin3)
              + SUM(ghd.GHD_anzkin4),
              0
            ) AS bed_nights
        FROM FMT_Reporting.V2I_GuestHistoryDaily ghd
        JOIN FMT_Reporting.V2D_Property_Attributes pa
          ON pa.PAS_Protel_ID = ghd.GHD_mpehotel
         AND pa.PAS_pms = 'protel'
        WHERE ghd.GHD_datum >= p_date
          AND ghd.GHD_datum <  (p_date + INTERVAL 1 DAY)
          AND ghd.GHD_resstatus IN (1, 2)
          AND ghd.GHD_typ <> 4
          AND EXISTS (
              SELECT 1
              FROM FMT_Reporting.V2I_GuestHistoryReservation res
              WHERE res.GHR_leistacc = ghd.GHD_leistacc
                AND res.GHR_reschar = 0
                AND res.GHR_zimmer = 1
                AND res.GHR_datumcxl = '1900-01-01'
          )
        GROUP BY
            ghd.GHD_datum,
            ghd.GHD_mpehotel,
            pa.PAS_code3,
            pa.PAS_name_short

        UNION ALL

        /* =========================
           APALEO
           ========================= */
        SELECT
            'apaleo' AS source_system,
            x.rev_date,
            x.protel_id,
            x.property_id,
            x.name_short,
            SUM(x.room_nights) AS room_nights,
            SUM(x.adults_num + x.children_num) AS bed_nights
        FROM (
            SELECT
                pro.PAS_code3 AS property_id,
                pro.PAS_Protel_ID AS protel_id,
                pro.PAS_name_short AS name_short,
                ts.service_date AS rev_date,
                1 AS room_nights,
                COALESCE(rs.adults, 0) AS adults_num,
                COALESCE(chl.num_children, 0) AS children_num
            FROM FMT_Reporting.raw_apaleo_reservations__time_slices ts
            JOIN FMT_Reporting.raw_apaleo_reservations rs
              ON rs._dlt_id = ts._dlt_parent_id
            JOIN FMT_Reporting.V2D_Property_Attributes pro
              ON pro.PAS_code3 = rs.property__id
             AND pro.PAS_pms = 'apaleo'
            JOIN FMT_Reporting.raw_apaleo_unitgroups ug
              ON ug.id = ts.unit_group__id
             AND ug.type = 'BedRoom'
            LEFT JOIN (
                SELECT
                    _dlt_parent_id,
                    COUNT(*) AS num_children
                FROM FMT_Reporting.raw_apaleo_reservations__children_ages
                GROUP BY _dlt_parent_id
            ) chl
              ON chl._dlt_parent_id = rs._dlt_id
            WHERE ts.service_date >= p_date
              AND ts.service_date <  (p_date + INTERVAL 1 DAY)
              AND rs.status NOT IN ('Canceled', 'NoShow')
        ) x
        GROUP BY
            x.rev_date,
            x.protel_id,
            x.property_id,
            x.name_short
    )

    /* =========================
       KPI “transpose”
       ========================= */
    SELECT
        CONCAT(property_id, '_ROOMNIGHT1') AS `pa.PAS_code3_KPINAME`,
        room_nights AS `Value`,
        rev_date,
        source_system,
        protel_id,
        name_short
    FROM base

    UNION ALL

    SELECT
        CONCAT(property_id, '_BEDNIGHT1'),
        bed_nights,
        rev_date,
        source_system,
        protel_id,
        name_short
    FROM base

    UNION ALL

    SELECT
        CONCAT(property_id, '_OPEN1'),
        CASE WHEN (room_nights + bed_nights) > 0 THEN 1 ELSE 0 END,
        rev_date,
        source_system,
        protel_id,
        name_short
    FROM base

    ORDER BY rev_date, `pa.PAS_code3_KPINAME`, source_system;

END