


select * From V2V_TAA vvt where other

select *
from V2D_Property_Attributes vdpa


select * from map_segments_apaleo msa 
select * from map_segments_protel mpa


select * from V2D_Property_Attributes vdpa 


select  

		CASE
			
            WHEN ghd.GHD_resstatus NOT IN (3,-1)
              AND cat.PRC_zimmer = 1
              AND ghr.GHR_zimmernr > 0
              AND (ghr.GHR_leistacc = ghr.GHR_sharenr or ghr.GHR_sharenr is null)
              AND ghd.GHD_typ NOT IN (4) 
            THEN COALESCE(ghd.GHD_roomnights, 0)
            
            ELSE 0
        END AS room_nights,
        ghr.GHR_sharenr,
        ghr.GHR_leistacc,
        ghd.*

FROM V2I_GuestHistoryDaily ghd
    JOIN V2I_GuestHistoryReservation ghr
      ON ghr.GHR_mpehotel = ghd.GHD_mpehotel
     AND ghr.GHR_leistacc = ghd.GHD_leistacc
     AND ghr.GHR_reschar NOT IN (2, 3)

    LEFT JOIN V2D_Market mkt
      ON mkt.MK_Nr = ghr.GHR_market

    LEFT JOIN V2D_Source vds
      ON vds.SC_Nr = ghr.GHR_source

    JOIN V2D_Property_Attributes pro
      ON pro.PAS_Protel_ID = ghd.GHD_mpehotel
      
	LEFT JOIN V2D_ProtelRooms rms
	  ON rms.PR_zinr = ghr.GHR_zimmernr 
    
    LEFT JOIN V2D_ProtelRoomCategories cat
  	  ON cat.PRC_katnr = ghr.GHR_katnr
  	  
  	LEFT JOIN V2I_RateCodeStructure rcs
	  ON rcs.RCS_level1_nr = ghd.GHD_preistypgr
	  
	LEFT JOIN Protel_SalesProfiles ta /* operator or agency */
	  ON ghr.GHR_reisenr = ta.Profilenumber 
	  
	LEFT JOIN Protel_SalesProfiles cc /* customer company */
	  ON ghr.GHR_firmennr = cc.Profilenumber 
where GHR_leistacc = 15663929


select * from V2I_GuestHistoryDaily where GHD_leistacc = 15663929

select * from V2I_GuestHistoryReservation where GHR_sharenr is null

15663929
15682500


select vdpa.PAS_Protel_ID as property_protel_id, vdpa.PAS_apaleo_switch_from, vdpa.PAS_apaleo_switch_to
from V2D_Property_Attributes vdpa
where vdpa.PAS_Protel_ID is not null
and vdpa.PAS_apaleo_switch_from is not null


select * from V2V_CalculatedPastDataDetailed where CPDD_mpehotel = 40



select status, count(*)
from raw_apaleo_blocks
group by status 


select * from raw_apaleo_blocks where status = 'Tentative'

select distinct status from raw_apaleo_reservations





select * from gold_commercial_report_units gcru 


select * 
from gold_consolidated_segment_report gcsr 
where gcsr.property_fmtg_id in ('500540', '500510', '500511', '520010')
and gcsr.rev_year_month = '2026-04'





select * from V2D_Property_Attributes vdpa where 


select sum(room_nights), sum(bed_nights)
-- select *
from gold_protel_revenue_daily
where property_id = 'FSE'
and rev_date >= '2026-04-20'
and rev_date <= '2026-04-30'
and bed_nights < room_nights 

select rev_date, sum(room_nights), sum(bed_nights)
from gold_protel_revenue_daily
where property_id = 'FSE'
and rev_date >= '2026-04-20'
and rev_date <= '2026-04-30'
group by rev_date
order by 1 ASC 

select rev_date, sum(room_nights), sum(bed_nights)
select distinct reservation_id 
from gold_protel_revenue_daily
where property_id = 'FSE'
and rev_date >= '2026-04-21'
and rev_date <= '2026-04-21'
group by rev_date
order by 1 ASC 

select * 
from gold_protel_revenue_daily
where property_id = 'FSE'
and rev_date >= '2026-04-21'
and rev_date <= '2026-04-21'
and room_nights > 0
order by reservation_id asc






-- FMT_Reporting.gold_commercial_report_units source

CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `FMT_Reporting`.`gold_commercial_report_units` AS
select
    date_format(`pr`.`rev_date`, '%Y-%m') AS `rev_year_month`,
    `pr`.`property_id` AS `property_id`,
    `pr`.`market_segment` AS `market_segment`,
    `pr`.`market_channel` AS `market_channel`,
    `pr`.`mk_sc_group` AS `mk_sc_group`,
    `msp`.`report_segment` AS `report_segment`,
    `pr`.`mk_country` AS `mk_country`,
    `pr`.`rate_plan_code` AS `rate_plan_code`,
    cast(round(sum(coalesce(`pr`.`revenue_total_net_eur`, 0)), 2) as decimal(38, 3)) AS `revenue_total_net_eur`,
    cast(round(sum(coalesce(`pr`.`revenue_room_net_eur`, 0)), 2) as decimal(38, 3)) AS `revenue_room_net_eur`,
    cast(sum(coalesce(`pr`.`room_nights`, 0)) as unsigned) AS `room_nights`,
    cast(sum(coalesce(`pr`.`bed_nights`, 0)) as unsigned) AS `bed_nights`,
    `pr`.`unit_group_name` AS `unit_group_name`,
    `pr`.`unit_id` AS `unit_id`,
    `pr`.`unit_name` AS `unit_name`,
    `pr`.`unit_description` AS `unit_description`,
    `pr`.`booker_company_code` AS `booker_company_code`,
    `pr`.`booker_company_name` AS `booker_company_name`,
    `pr`.`customer_company_code` AS `customer_company_code`,
    `pr`.`customer_company_name` AS `customer_company_name`,
    'apaleo' AS `pms_name`
from
    (`FMT_Reporting`.`gold_apaleo_revenue_daily` `pr`
left join `FMT_Reporting`.`map_segments_apaleo` `msp` on
    ((`msp`.`apaleo_segment` = `pr`.`mk_sc_group`)))
group by
    `rev_year_month`,
    `pr`.`property_id`,
    `pr`.`market_segment`,
    `pr`.`market_channel`,
    `msp`.`report_segment`,
    `pr`.`mk_country`,
    `pr`.`rate_plan_code`,
    `pr`.`mk_sc_group`,
    `pr`.`unit_group_name`,
    `pr`.`unit_id`,
    `pr`.`unit_name`,
    `pr`.`unit_description`,
    `pr`.`booker_company_code`,
    `pr`.`booker_company_name`,
    `pr`.`customer_company_code`,
    `pr`.`customer_company_name`
union all
select
    date_format(`pr`.`rev_date`, '%Y-%m') AS `rev_year_month`,
    `pr`.`property_id` AS `property_id`,
    `pr`.`market_segment` AS `market_segment`,
    `pr`.`market_channel` AS `market_channel`,
    `pr`.`mk_sc_group` AS `mk_sc_group`,
    `msp`.`report_segment` AS `report_segment`,
    `pr`.`mk_country` AS `mk_country`,
    `pr`.`rate_plan_code` AS `rate_plan_code`,
    cast(round(sum(coalesce(`pr`.`revenue_total_net_eur`, 0)), 2) as decimal(38, 3)) AS `revenue_total_net_eur`,
    cast(round(sum(coalesce(`pr`.`revenue_room_net_eur`, 0)), 2) as decimal(38, 3)) AS `revenue_room_net_eur`,
    cast(sum(coalesce(`pr`.`room_nights`, 0)) as unsigned) AS `room_nights`,
    cast(sum(coalesce(`pr`.`bed_nights`, 0)) as unsigned) AS `bed_nights`,
    `pr`.`unit_group_name` AS `unit_group_name`,
    `pr`.`unit_id` AS `unit_id`,
    `pr`.`unit_name` AS `unit_name`,
    `pr`.`unit_description` AS `unit_description`,
    `pr`.`booker_company_code` AS `booker_company_code`,
    `pr`.`booker_company_name` AS `booker_company_name`,
    `pr`.`customer_company_code` AS `customer_company_code`,
    `pr`.`customer_company_name` AS `customer_company_name`,
    'protel' AS `pms_name`
from
    (`FMT_Reporting`.`gold_protel_revenue_daily` `pr`
left join `FMT_Reporting`.`map_segments_protel` `msp` on
    ((`msp`.`protel_segment` = `pr`.`mk_sc_group`)))
group by
    `rev_year_month`,
    `pr`.`property_id`,
    `pr`.`market_segment`,
    `pr`.`market_channel`,
    `msp`.`report_segment`,
    `pr`.`mk_country`,
    `pr`.`rate_plan_code`,
    `pr`.`mk_sc_group`,
    `pr`.`unit_group_name`,
    `pr`.`unit_id`,
    `pr`.`unit_name`,
    `pr`.`unit_description`,
    `pr`.`booker_company_code`,
    `pr`.`booker_company_name`,
    `pr`.`customer_company_code`,
    `pr`.`customer_company_name`;


    