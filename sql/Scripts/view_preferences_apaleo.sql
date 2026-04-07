-- FMT_Reporting.crm_preferences_test source

CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `FMT_Reporting`.`crm_preferences_test` AS
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Campsite Booker' AS `preference_name`,
    'Camping' AS `preference_category`,
    'PREF001' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    (`FMT_Reporting`.`crm_reservation_test`.`property_id` in ('FCZ', 'FGP'))
union all
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Apartment' AS `preference_name`,
    'Room Category Booker' AS `preference_category`,
    'PREF002' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    (`FMT_Reporting`.`crm_reservation_test`.`property_id` = 'FEW')
union all
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Booker with Children' AS `preference_name`,
    'Family' AS `preference_category`,
    'PREF003' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    (`FMT_Reporting`.`crm_reservation_test`.`children_num` > 0)
union all
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Booker with Children' AS `preference_name`,
    'Family' AS `preference_category`,
    'PREF004' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    (`FMT_Reporting`.`crm_reservation_test`.`children_num` > 0)
union all
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Mountain' AS `preference_name`,
    'Destination' AS `preference_category`,
    'PREF005' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    (`FMT_Reporting`.`crm_reservation_test`.`property_id` = 'FEW')
union all
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Car' AS `preference_name`,
    'Travel Preference' AS `preference_category`,
    'PREF006' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    (`FMT_Reporting`.`crm_reservation_test`.`property_id` = 'FCZ')
union all
select
    distinct `FMT_Reporting`.`crm_reservation_test`.`cluster_id` AS `cluster_id`,
    'Suite' AS `preference_name`,
    'Room Categoriy Booker' AS `preference_category`,
    'PREF007' AS `external_id`,
    'apaleo' AS `source_system`
from
    `FMT_Reporting`.`crm_reservation_test`
where
    ((`FMT_Reporting`.`crm_reservation_test`.`property_id` = 'FSV')
        and (`FMT_Reporting`.`crm_reservation_test`.`unit_group_code` in ('J3C', 'J3D', 'S3C', 'J3E', 'S3E', 'S2D', 'P4C', 'P2D', 'P4D')));