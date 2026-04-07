

CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW FMT_Reporting.crm_reservation_test AS

SELECT
        pro.PAS_Code3                                                   AS property_id,
        pro.PAS_FMTG_ID													AS property_fmtg_id,
        rs.id                                                           AS reservation_id,
        rs.booking_id													AS booking_id,
        rs.status														AS reservation_status,
        rs.group_name													AS group_name,
        NULL               												AS revenue_room_net_eur,
        NULL                                                            AS revenue_total_net_eur,
        NULL                                                            AS room_nights,
        NULL 												            AS bed_nights,
        rs.arrival														AS arrival_at,
        rs.departure													AS departure_at,
        rs.created														AS booking_at,
        rs.check_in_time												AS checkin_at,
        rs.check_out_time											    AS checkout_at,
        rs.is_pre_checked_in											AS is_pre_checkedin,
        rs.cancellation_time											AS cancelled_at,
        rs.no_show_time													AS noshow_at,
        rs.market_segment__code                                         AS market_segment,
        rs.channel_code                                                 AS market_channel,
        CAST(SUBSTRING(
        	TRIM(rs.primary_guest__address__country_code),1,2) AS CHAR(2)
        	) 															AS mk_country,
        rs.rate_plan__code                                              AS rate_plan_code,
        COALESCE(rs.Adults, 0)                                          AS adults_num,
        COALESCE(chl.num_children, 0)                                   AS children_num,
        rs.unit_group__code												AS unit_group_code,
        rs.travel_purpose												AS travel_purpose,
        'apaleo'                                                        AS pms_name,
        rs.primary_guest__gender										AS pg_gender,						
		rs.primary_guest__preferred_language							AS pg_preferred_language,
		rs.primary_guest__birth_date									AS pg_birth_date,
		rs.primary_guest__birth_place									AS pg_birth_place,
		rs.primary_guest__title											AS pg_title,
		rs.primary_guest__nationality_country_code						AS pg_nationality_country_code,
		rs.primary_guest__identification_number							AS pg_id_number,
		rs.primary_guest__identification_issue_date						AS pg_id_issue_date,
		rs.primary_guest__identification_expiry_date					AS pg_expiry_date,
		rs.primary_guest__identification_type							AS pg_type,
		rs.primary_guest__first_name									AS pg_first_name,
		rs.primary_guest__middle_initial								AS pg_middle_name,
		rs.primary_guest__last_name										AS pg_last_name,
		rs.primary_guest__email											AS pg_email,
		rs.primary_guest__phone											AS pg_phone,
		rs.primary_guest__address__address_line1						AS pg_mailing_street,
		rs.primary_guest__address__postal_code							AS pg_mailing_postal_code,
		rs.primary_guest__address__city									AS pg_mailing_city,
		rs.primary_guest__address__country_code							AS pg_mailing_country_code,
		'PRIMARY'														AS guest_role,
		rs.external_code												AS external_code,
		cls.cluster_id													AS cluster_id


      --  select count(*) 
    FROM raw_apaleo_reservations rs
    JOIN V2D_Property_Attributes pro
      ON pro.PAS_code3 = rs.property__id
    LEFT JOIN raw_apaleo_rateplans rp
      ON rp.id =  rs.rate_plan__code
    LEFT JOIN (
        SELECT _dlt_parent_id, COUNT(*) AS num_children
        FROM raw_apaleo_reservations__children_ages
        GROUP BY _dlt_parent_id
    ) chl
      ON chl._dlt_parent_id = rs._dlt_id

    /* Include only rows that sit INSIDE apaleo windows:
       - returned to Protel: [from, to] inclusive
       - still on apaleo (to IS NULL): include [from, +∞) if p_include_open_interval = 1
    */
    JOIN (
        SELECT
            p.PAS_code3,
            p.PAS_apaleo_switch_from AS sw_from,
            p.PAS_apaleo_switch_to   AS sw_to
        FROM V2D_Property_Attributes p
        WHERE p.PAS_apaleo_switch_from IS NOT NULL
    ) a
      ON a.PAS_code3 = rs.property__id
     AND rs.created >= a.sw_from
     
    /* filter for interessting reservations */ 
    JOIN (
    
			SELECT pgc.reservation_id, pgc.cluster_id
			FROM pred_guest_clusters pgc
			JOIN (
			    SELECT cluster_id
			    FROM pred_guest_clusters
			    GROUP BY cluster_id
			    HAVING COUNT(*) BETWEEN 1 AND 3 
			    	OR COUNT(*) BETWEEN 10 AND 50
			) c
			    ON c.cluster_id = pgc.cluster_id
    
    ) cls 
    	ON cls.reservation_id = rs.id

    WHERE rs.created >= '2026-01-01'
    and rs.property__id in ('FCG', 'FSV', 'FEW', 'FCZ' )

      ;
      
      

      
      
      
	SELECT cluster_id, count(*)
    FROM pred_guest_clusters
    GROUP BY cluster_id
    
    select count(*)  from raw_apaleo_reservations rar 
    
    select * from pred_guest_clusters where cluster_id =  210
    
    
    
    
    
    SELECT * FROM crm_reservation_test
    
    
    
WITH base AS (
SELECT *

FROM crm_reservation_test rs
-- your joins here (pro/rs/chl/cls etc.)
WHERE rs.arrival_at <= CURRENT_DATE()
),
ranked AS (
    SELECT
        base.*,
        ROW_NUMBER() OVER (
            PARTITION BY base.cluster_id
            ORDER BY base.arrival_at DESC, base.reservation_id DESC
        ) AS rn
    FROM base
)
SELECT *
FROM ranked
WHERE rn = 1;
    
    