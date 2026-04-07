
/* create storage procedure  mig_raw_crm_reservations_apaleo */


/* Main Reservation Table for Migration into Salesforce */


/* create  "mig_raw_crm_reservations" table if not exists from this query and fill it. 
 * 
 * if exists delete all reservations with the source = 'apaleo'
 * 
 * */



SELECT
	cast(null as unsigned) as row_id,
	cast(null as unsigned) as cluster_id,
	cast(null as unsigned) as _entity_id,
	cast(null as unsigned) as _excluded,
	res.id as reservation_id,
	cast('apaleo' as char(50)) as source,
	
	res.property__id as property_id,
	cast(null as char(50)) as property_fmtg_id,
	cast(null as unsigned) as property_protel_id,
	res.booking_id as booking_id,
	res.status as reservation_status,
	case 
		when res.group_name is not null 
		then concat(res.booking_id, ' | ', res.group_name) 
		else null 
	end as group_name,
	res.arrival as arrival_at,
	res.departure as departure_at,
	res.created as booking_at,
	res.check_in_time as checkin_at,
	res.check_out_time as checkout_at,
	res.cancellation_time as cancelled_at,
	res.no_show_time as noshow_at,
	res.market_segment__code as market_segment,
	res.channel_code as market_channel,
	res.rate_plan__code as rate_plan_code,
	com.code as booker_company_id, 
	res.adults adults_num,
	res.children as children_num,
	res.unit_group__code as unit_group_code,
	res.travel_purpose as travel_purpose,
	res.external_code as external_code,
	cast('PRIMARY' as char(50)) as guest_role,
	cast(
		case 
		    when res.departure > res.arrival 
		    then datediff(res.departure, res.arrival)
		    else 0
		end 
	as unsigned) as room_nights,
	res.primary_guest__first_name as first_name,
	res.primary_guest__middle_initial as middle_name,
	res.primary_guest__last_name as last_name,
	res.primary_guest__email as email,
	cast(res.primary_guest__birth_date as date) as birth_date,
	res.primary_guest__title as salutation,
	res.primary_guest__gender as gender,
	res.primary_guest__preferred_language as preferred_language,
	res.primary_guest__address__address_line1 as address,
	res.primary_guest__address__city as city,
	res.primary_guest__address__postal_code as postal_code,
	res.primary_guest__address__country_code as country,
	res.primary_guest__phone as phone,
	res.primary_guest__birth_place as birth_place,
	res.primary_guest__nationality_country_code as nationality,
	cast(null as signed) as revenue_room,
	cast(null as signed) as revenue_fnb,
	cast(null as signed) as revenue_extra,
	cast(null as signed) as revenue_total,
	cast(null as char(10)) as sf_preferred_language,
	cast(null as char(100)) as sf_reservation_status,
	cast(null as char(100)) as sf_property_id,
	cast(null as char(100)) as sf_person_contact_id,
	cast(null as char(100)) as sf_person_account_id
	

-- select *
from raw_apaleo_reservations res
left join raw_apaleo_companies com 
	on com.id = res.company__id 
where 1=1
	and res.unit_group__type = 'BedRoom'
	and res.status  in ('Canceled', 'NoShow')
	-- and res.primary_guest__email is not null
	-- and res.primary_guest__first_name  is null
	-- and res.primary_guest__last_name is null
	and res.property__id in ('FBL','FCA','FCR','FEW','FFK','FSA','FSG','FST','FSV','FCZ','FMO','FHS','FCG')


	
/*  REVENUES FROM FOLIO */ 
	
/* update the main reservation table with this revenues  */
	
	

SELECT
    fol.reservation__id                                     AS reservation_id,
    SUM(CASE WHEN chg.service_type IN ('Accommodation', 'NoShow')
             THEN chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
             END)                                           AS revenue_room,
    SUM(CASE WHEN chg.service_type = 'FoodAndBeverages'
             THEN chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
             END)                                           AS revenue_fnb,
    SUM(CASE WHEN chg.service_type = 'Other'
             THEN chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0)
             END)                                           AS revenue_extra,
    SUM(chg.amount__gross_amount - COALESCE(alw.amount__gross_amount, 0))
                                                            AS revenue_total
FROM raw_apaleo_folios fol
LEFT JOIN raw_apaleo_folios__charges chg
    ON  chg.folio_id      = fol.id
LEFT JOIN (
    SELECT folio_id, source_charge_id,
           SUM(amount__gross_amount) AS amount__gross_amount
    FROM raw_apaleo_folios__allowances
    WHERE moved_to__id IS NULL
    GROUP BY folio_id, source_charge_id
) alw
    ON  alw.source_charge_id = chg.id
    AND alw.folio_id         = chg.folio_id
WHERE chg.is_posted     = 1
  AND chg.routed_to__id IS NULL
  AND chg.moved_to__id  IS NULL
  AND fol.`type`        = 'Guest'
  AND fol.status        = 'ClosedWithInvoice'
  AND chg.service_type NOT IN ('CityTax', 'SecondCityTax')
  AND fol.reservation__id = 'AJKIFGGW-1'
GROUP BY
    fol.reservation__id;
	
  
  


/*  Update Property Details  */

/* update property attributes in the main reservation using this query */ 


update mig_crm_reservations r
inner join V2D_Property_Attributes p
	on r.property_id = p.PAS_code3
set r.property_fmtg_id = p.PAS_FMGT_ID
where p.PAS_pms = 'apaleo'
		


/* Update Reservation Status in Main Table */ 

UPDATE mig_crm_reservations
SET    status = 'Cancelled'
WHERE  status = 'Canceled';



/* Validierung Revenue */


SELECT  chg.*

FROM raw_apaleo_folios fol
LEFT JOIN raw_apaleo_folios__charges chg
    ON  chg.folio_id      = fol.id
LEFT JOIN (
    SELECT folio_id, source_charge_id,
           SUM(amount__gross_amount) AS amount__gross_amount
    FROM raw_apaleo_folios__allowances
    WHERE moved_to__id IS NULL
    GROUP BY folio_id, source_charge_id
) alw
    ON  alw.source_charge_id = chg.id
    AND alw.folio_id         = chg.folio_id
WHERE chg.is_posted     = 1
  AND chg.routed_to__id IS NULL
  AND chg.moved_to__id  IS NULL
  AND fol.`type`        = 'Guest'
  AND fol.status        = 'ClosedWithInvoice'
  AND chg.service_type NOT IN ('CityTax', 'SecondCityTax')
  AND fol.reservation__id = 'AJKIFGGW-1'




/* here will be latter some more transformations */

  select * from V2D_Property_Attributes



SELECT  r.*, rr.booker_company_id, rr.revenue_room, rr.revenue_fnb, rr.revenue_total, rr.revenue_extra
-- select count(*)
FROM crm_reservation_import_20260322 r
inner join  mig_raw_crm_reservations rr
	on rr.reservation_id = r.reservation_id
where r.source  = 'apaleo' 
and r.reservation_status not in ('Confirmed')


select distinct reservation_status from mig_raw_crm_reservations where reservation_id = 'MJMIKVKT-1'