
-- 1. Create the mapping table
CREATE TABLE `gms_property_mapping` (
  `property_name` varchar(255) COLLATE utf8mb3_unicode_ci NOT NULL,
  `property_id` varchar(8) COLLATE utf8mb3_unicode_ci NOT NULL,
  PRIMARY KEY (`property_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- 2. Insert the mapping
INSERT INTO `gms_property_mapping` (`property_name`, `property_id`) VALUES
('Falkensteiner Adriana', 'FAD'),
('Falkensteiner Balance Resort Stegersbach', 'FST'),
('Falkensteiner Club Funimation Borik', 'FFB'),
('Falkensteiner Club Funimation Katschberg', 'FFK'),
('Falkensteiner Family Hotel Diadora', 'FDI'),
('Falkensteiner Family Resort Lido', 'FEH'),
('Falkensteiner Funimation Garden Calabria', 'FFC'),
('Falkensteiner Hotel & Asia Spa Leoben', 'FLE'),
('Falkensteiner Hotel & Spa Carinzia', 'FCA'),
('Falkensteiner Hotel & Spa Falkensteinerhof', 'FFH'),
('Falkensteiner Hotel & Spa Iadera', 'FIA'),
('Falkensteiner Hotel & Spa Jesolo', 'FJE'),
('Falkensteiner Hotel & Spa Sonnenparadies', 'FSP'),
('Falkensteiner Hotel Antholz', 'FAA'),
('Falkensteiner Hotel Bad Leonfelden', 'FBL'),
('Falkensteiner Hotel Belgrade', 'FBE'),
('Falkensteiner Hotel Bozen WaltherPark', 'FBZ'),
('Falkensteiner Hotel Bratislava', 'FBA'),
('Falkensteiner Hotel Cristallo', 'FCR'),
('Falkensteiner Hotel Kronplatz', 'FKP'),
('Falkensteiner Hotel Montafon', 'FMO'),
('Falkensteiner Hotel Montenegro', 'FQM'),
('Falkensteiner Hotel Park Punat', 'FPP'),
('Falkensteiner Hotel Prague', 'FMP'),
('Falkensteiner Hotel Schladming', 'FSG'),
('Falkensteiner Hotel Sonnenalpe', 'FSA'),
('Falkensteiner Lake Garda Resort', 'FMG'),
('Falkensteiner Premium Apartments Edelweiss', 'FEW'),
('Falkensteiner Premium Apartments Senia', 'FSE'),
('Falkensteiner Premium Camping Zadar', 'FCZ'),
('Falkensteiner Resort Capo Boi', 'FCB'),
('Falkensteiner Resort Chia', 'FTC'),
('Falkensteiner Schlosshotel Velden', 'FSV'),
('Falkensteiner Spa Resort Marienbad Premium Collection', 'FMB'),
('Hotel Donat', 'FDO');



-- 3. Add the property_id column to gms_reservations
ALTER TABLE `gms_reservations`
ADD COLUMN `property_id` varchar(8) COLLATE utf8mb3_unicode_ci DEFAULT NULL,
ADD KEY `idx_property_id` (`property_id`);


-- 4. Populate it via the mapping
UPDATE `gms_reservations` r
JOIN `gms_property_mapping` m
  ON TRIM(r.property_name) = m.property_name
SET r.property_id = m.property_id;


-- 5. Verify
SELECT
    CASE WHEN property_id IS NULL THEN 'unmapped' ELSE 'mapped' END AS status,
    COUNT(*) AS rows_count
FROM `gms_reservations`
GROUP BY status;


-- 6. Inspect any unmapped property_names
SELECT TRIM(property_name) AS property_name, COUNT(*) AS reservations
FROM `gms_reservations`
WHERE property_id IS NULL
GROUP BY TRIM(property_name)
ORDER BY reservations DESC;


-- 7. add orginal reservation id for apaleo 

ALTER TABLE `gms_reservations`
ADD COLUMN `pms_reservation_id` varchar(50) DEFAULT NULL,
ADD KEY `idx_pms_reservation_id` (`pms_reservation_id`);


select gap.sf_contact_id, r.sf_person_contact_id


select 
		gr.reservation_id, 
		r.external_code, 
		r.reservation_id, 
		r.property_id, 
		gr.property_id, 
		gr.check_in, 
		r.arrival_at, 
		gr.check_out, 
		r.departure_at,
		gr.rate_code,
		r.rate_plan_code,
		r.first_name,
		r.last_name, 
		gap.fname,
		gap.lname,
		r.email as r_email,
		gap.email as g_email,
		gap.sf_contact_id, 
		r.sf_reservation_id
select count(*)
from gms_reservations gr 
inner join  mig_raw_crm_reservations_clean r
	on r.external_code = gr.reservation_id
	and cast(r.arrival_at as date) = gr.check_in 
	and r.external_code is not null
	and r.property_id = gr.property_id 
left join gms_all_profiles gap
	on gr.list_id = gap.list_id 
where r.source = 'apaleo'
-- and gap.sf_contact_id is not null
and r.sf_reservation_id is   null
and (gap.sf_contact_id = r.sf_person_contact_id or r.sf_person_contact_id is null)



-- apaleo 
UPDATE gms_reservations gr
INNER JOIN mig_raw_crm_reservations_clean r
    ON r.external_code = gr.reservation_id
    AND CAST(r.arrival_at AS DATE) = gr.check_in
    AND r.external_code IS NOT NULL
    AND r.property_id = gr.property_id
SET gr.pms_reservation_id = r.reservation_id
WHERE r.source = 'apaleo'
  AND r.sf_reservation_id IS NULL

  
select pms_reservation_id, count(*)
from gms_reservations
group by pms_reservation_id having count(*)> 1
order by 2 desc;


-- protel

select * from gms_reservations gr
INNER JOIN mig_raw_crm_reservations_clean r
    ON r.reservation_id = gr.reservation_id
    AND CAST(r.arrival_at AS DATE) = gr.check_in
    AND r.property_id = gr.property_id
WHERE  r.source = 'protel'
  AND r.reservation_id = '10087753';

UPDATE gms_reservations gr
INNER JOIN mig_raw_crm_reservations_clean r
    ON r.reservation_id = gr.reservation_id
    AND CAST(r.arrival_at AS DATE) = gr.check_in
    AND r.property_id = gr.property_id
SET gr.pms_reservation_id = r.reservation_id
WHERE r.source = 'protel'
  AND gr.pms_reservation_id is null;

  
select count(*) 
from gms_reservations gr
where pms_reservation_id is not null


/** flag dublicated reservations **/

ALTER TABLE gms_reservations
ADD COLUMN id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;

ALTER TABLE gms_reservations
ADD COLUMN is_duplicate TINYINT(1) NOT NULL DEFAULT 0;

CREATE INDEX idx_gms_reservations_is_duplicate
ON gms_reservations (is_duplicate);

UPDATE gms_reservations gr
INNER JOIN (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY reservation_id, property_id
            ORDER BY STR_TO_DATE(created_datetime, '%Y-%m-%d %H:%i:%s') DESC,
                     id DESC
        ) AS rn
    FROM gms_reservations
) ranked ON ranked.id = gr.id
SET gr.is_duplicate = CASE WHEN ranked.rn = 1 THEN 0 ELSE 1 END;


-- check - must return zero rows
SELECT reservation_id, property_id,
       SUM(is_duplicate = 0) AS winners,
       COUNT(*) AS total
FROM gms_reservations
GROUP BY reservation_id, property_id
HAVING winners <> 1;
