DROP PROCEDURE IF EXISTS  sp_mig_raw_crm_reservations_apaleo;

CALL sp_mig_raw_crm_reservations_apaleo()


delete from mig_raw_crm_reservations 
where source = 'protel' 
and _ptable = 'reservation'
and reservation_id = 15651292
truncate table FMT_Reporting.crm_protel_reservation_events
truncate table mig_raw_crm_reservations

delete from mig_raw_crm_reservations 
where source = 'protel' 
and _ptable = 'cancelled'

delete from mig_raw_crm_reservations 
where source = 'protel' 
and _ptable = 'history'

select * from mig_raw_crm_reservations where reservation_id = 7425702
Failure happened on 'destination' side. 'Type=MySqlConnector.MySqlException,Message=Duplicate entry '7425702-protel-cancelled' for key 'mig_raw_crm_reservations.PRIMARY',Source=MySqlConnector,'
ALTER TABLE mig_raw_crm_reservations
DROP PRIMARY KEY,
ADD PRIMARY KEY (reservation_id, source, _ptable);

Failure happened on 'destination' side. 'Type=MySqlConnector.MySqlException,Message=Duplicate entry '15651292-protel' for key 'mig_raw_crm_reservations.PRIMARY',Source=MySqlConnector,'

delete
from mig_raw_crm_reservations 
where source = 'protel' and _ptable = 'cancelled'

CREATE PROCEDURE sp_mig_raw_crm_reservations_apaleo()
BEGIN

    /* ------------------------------------------------------------------ */
    /* STEP 1: Create table if not exists                                  */
    /* ------------------------------------------------------------------ */

    CREATE TABLE IF NOT EXISTS mig_raw_crm_reservations (
        row_id                      INT UNSIGNED,
        cluster_id                  INT UNSIGNED,
        _entity_id                  INT UNSIGNED,
        _excluded                   INT UNSIGNED,
        _ptable						VARCHAR(100),
        reservation_id              VARCHAR(100),
        source                      VARCHAR(50),
        property_id                 VARCHAR(50),
        property_fmtg_id            VARCHAR(50),
        property_protel_id          INT UNSIGNED,
        booking_id                  VARCHAR(100),
        reservation_status          VARCHAR(100),
        group_name                  VARCHAR(255),
        arrival_at                  DATETIME,
        departure_at                DATETIME,
        booking_at                  DATETIME,
        checkin_at                  DATETIME,
        checkout_at                 DATETIME,
        cancelled_at                DATETIME,
        noshow_at                   DATETIME,
        market_segment              VARCHAR(100),
        market_channel              VARCHAR(100),
        rate_plan_code              VARCHAR(100),
        booker_company_id           VARCHAR(100),
        adults_num                  INT UNSIGNED,
        children_num                INT UNSIGNED,
        unit_group_code             VARCHAR(100),
        travel_purpose              VARCHAR(100),
        external_code               VARCHAR(100),
        guest_role                  VARCHAR(50),
        room_nights                 INT UNSIGNED,
        first_name                  VARCHAR(100),
        middle_name                 VARCHAR(100),
        last_name                   VARCHAR(100),
        email                       VARCHAR(255),
        birth_date                  DATE,
        salutation                  VARCHAR(50),
        gender                      VARCHAR(50),
        preferred_language          VARCHAR(50),
        address                     VARCHAR(255),
        city                        VARCHAR(100),
        postal_code                 VARCHAR(20),
        country                     VARCHAR(10),
        phone                       VARCHAR(50),
        birth_place                 VARCHAR(100),
        nationality                 VARCHAR(10),
        revenue_room                DECIMAL(18,4),
        revenue_fnb                 DECIMAL(18,4),
        revenue_extra               DECIMAL(18,4),
        revenue_total               DECIMAL(18,4),
        sf_preferred_language       VARCHAR(10),
        sf_reservation_status       VARCHAR(100),
        sf_property_id              VARCHAR(100),
        sf_person_contact_id        VARCHAR(100),
        sf_person_account_id        VARCHAR(100),

        PRIMARY KEY (reservation_id, source, _ptable),
        INDEX idx_source            (source),
        INDEX idx_property          (property_id),
        INDEX idx_status            (reservation_status)
    );


    /* ------------------------------------------------------------------ */
    /* STEP 2: Delete existing apaleo records                              */
    /* ------------------------------------------------------------------ */

    DELETE FROM mig_raw_crm_reservations
    WHERE  source = 'apaleo' and _ptable = 'reservation';


    /* ------------------------------------------------------------------ */
    /* STEP 3: Insert reservations                                         */
    /* ------------------------------------------------------------------ */

    INSERT INTO mig_raw_crm_reservations (
        row_id, cluster_id, _entity_id, _excluded, _ptable,
        reservation_id, source,
        property_id, property_fmtg_id, property_protel_id,
        booking_id, reservation_status, group_name,
        arrival_at, departure_at, booking_at,
        checkin_at, checkout_at, cancelled_at, noshow_at,
        market_segment, market_channel, rate_plan_code,
        booker_company_id, adults_num, children_num,
        unit_group_code, travel_purpose, external_code,
        guest_role, room_nights,
        first_name, middle_name, last_name,
        email, birth_date, salutation, gender,
        preferred_language, address, city, postal_code,
        country, phone, birth_place, nationality,
        revenue_room, revenue_fnb, revenue_extra, revenue_total,
        sf_preferred_language, sf_reservation_status,
        sf_property_id, sf_person_contact_id, sf_person_account_id
    )
    SELECT
        NULL                                                AS row_id,
        NULL                                                AS cluster_id,
        NULL                                                AS _entity_id,
        NULL                                                AS _excluded,
        'reservation'										AS _ptable,
        res.id                                              AS reservation_id,
        'apaleo'                                            AS source,
        res.property__id                                    AS property_id,
        NULL                                                AS property_fmtg_id,
        NULL                                                AS property_protel_id,
        res.booking_id                                      AS booking_id,
        res.status                                          AS reservation_status,
        CASE
            WHEN res.group_name IS NOT NULL
            THEN CONCAT(res.booking_id, ' | ', res.group_name)
            ELSE NULL
        END                                                 AS group_name,
        res.arrival                                         AS arrival_at,
        res.departure                                       AS departure_at,
        res.created                                         AS booking_at,
        res.check_in_time                                   AS checkin_at,
        res.check_out_time                                  AS checkout_at,
        res.cancellation_time                               AS cancelled_at,
        res.no_show_time                                    AS noshow_at,
        res.market_segment__code                            AS market_segment,
        res.channel_code                                    AS market_channel,
        res.rate_plan__code                                 AS rate_plan_code,
        com.code                                            AS booker_company_id,
        res.adults                                          AS adults_num,
        res.children                                        AS children_num,
        res.unit_group__code                                AS unit_group_code,
        res.travel_purpose                                  AS travel_purpose,
        res.external_code                                   AS external_code,
        'PRIMARY'                                           AS guest_role,
        CAST(
            CASE
                WHEN res.departure > res.arrival
                THEN DATEDIFF(res.departure, res.arrival)
                ELSE 0
            END
        AS UNSIGNED)                                        AS room_nights,
        res.primary_guest__first_name                       AS first_name,
        res.primary_guest__middle_initial                   AS middle_name,
        res.primary_guest__last_name                        AS last_name,
        res.primary_guest__email                            AS email,
        CAST(res.primary_guest__birth_date AS DATE)         AS birth_date,
        res.primary_guest__title                            AS salutation,
        res.primary_guest__gender                           AS gender,
        res.primary_guest__preferred_language               AS preferred_language,
        res.primary_guest__address__address_line1           AS address,
        res.primary_guest__address__city                    AS city,
        res.primary_guest__address__postal_code             AS postal_code,
        res.primary_guest__address__country_code            AS country,
        res.primary_guest__phone                            AS phone,
        res.primary_guest__birth_place                      AS birth_place,
        res.primary_guest__nationality_country_code         AS nationality,
        NULL                                                AS revenue_room,
        NULL                                                AS revenue_fnb,
        NULL                                                AS revenue_extra,
        NULL                                                AS revenue_total,
        NULL                                                AS sf_preferred_language,
        NULL                                                AS sf_reservation_status,
        NULL                                                AS sf_property_id,
        NULL                                                AS sf_person_contact_id,
        NULL                                                AS sf_person_account_id

    FROM        raw_apaleo_reservations     res
    LEFT JOIN   raw_apaleo_companies        com ON  com.id = res.company__id
    WHERE   res.unit_group__type    = 'BedRoom'
      -- AND   res.status              IN ('Canceled', 'NoShow')
      AND   res.property__id        IN (
                'FBL','FCA','FCR','FEW','FFK','FSA',
                'FSG','FST','FSV','FCZ','FMO','FHS','FCG'
            );


    /* ------------------------------------------------------------------ */
    /* STEP 4: Update revenues from folio                                  */
    /* ------------------------------------------------------------------ */

    DROP TEMPORARY TABLE IF EXISTS tmp_revenues;

    CREATE TEMPORARY TABLE tmp_revenues (
        reservation_id  VARCHAR(100)    NOT NULL,
        revenue_room    DECIMAL(18,4),
        revenue_fnb     DECIMAL(18,4),
        revenue_extra   DECIMAL(18,4),
        revenue_total   DECIMAL(18,4),
        PRIMARY KEY (reservation_id)
    );

    INSERT INTO tmp_revenues (
        reservation_id,
        revenue_room,
        revenue_fnb,
        revenue_extra,
        revenue_total
    )
    SELECT
        fol.reservation__id                                         AS reservation_id,
        SUM(CASE WHEN chg.service_type IN ('Accommodation', 'NoShow')
                 THEN chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
            END)                                                    AS revenue_room,
        SUM(CASE WHEN chg.service_type = 'FoodAndBeverages'
                 THEN chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
            END)                                                    AS revenue_fnb,
        SUM(CASE WHEN chg.service_type = 'Other'
                 THEN chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
            END)                                                    AS revenue_extra,
        SUM(       chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
            )                                                       AS revenue_total

    FROM            raw_apaleo_folios           fol
    LEFT JOIN       raw_apaleo_folios__charges  chg ON  chg.folio_id         = fol.id
    LEFT JOIN (
                    SELECT
                        folio_id,
                        source_charge_id,
                        SUM(amount__gross_amount)                   AS amount__gross_amount
                    FROM    raw_apaleo_folios__allowances
                    WHERE   moved_to__id IS NULL
                    GROUP BY
                        folio_id,
                        source_charge_id
                )                               alw ON  alw.source_charge_id = chg.id
                                                    AND alw.folio_id         = chg.folio_id

    WHERE   chg.is_posted       = 1
      AND   chg.routed_to__id   IS NULL
      AND   chg.moved_to__id    IS NULL
      AND   chg.service_type    NOT IN ('CityTax', 'SecondCityTax')
      AND   fol.`type`          = 'Guest'
      AND   fol.status          = 'ClosedWithInvoice'

    GROUP BY
        fol.reservation__id;

    UPDATE  mig_raw_crm_reservations    r
    INNER JOIN tmp_revenues             rev ON rev.reservation_id = r.reservation_id
    SET     r.revenue_room  = rev.revenue_room,
            r.revenue_fnb   = rev.revenue_fnb,
            r.revenue_extra = rev.revenue_extra,
            r.revenue_total = rev.revenue_total
    WHERE   r.source = 'apaleo';

    DROP TEMPORARY TABLE tmp_revenues;


    /* ------------------------------------------------------------------ */
    /* STEP 5: Update property attributes                                  */
    /* ------------------------------------------------------------------ */

    UPDATE  mig_raw_crm_reservations    r
    INNER JOIN V2D_Property_Attributes  p ON  p.PAS_code3 = r.property_id
    SET     r.property_fmtg_id = p.PAS_FMTG_ID
    WHERE   r.source    = 'apaleo'
      AND   p.PAS_pms   = 'apaleo';


    /* ------------------------------------------------------------------ */
    /* STEP 6: Fix reservation status typo Canceled → Cancelled            */
    /* ------------------------------------------------------------------ */

    UPDATE  mig_raw_crm_reservations
    SET     reservation_status = 'Cancelled'
    WHERE   reservation_status = 'Canceled'
      AND   source             = 'apaleo';
    
    SELECT 'OK' AS status;
END




