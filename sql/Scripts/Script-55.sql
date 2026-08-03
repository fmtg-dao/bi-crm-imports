



select *
from V2I_TM1IFRSBFC
where T1IB_Version = 'FC_V07_20260723'
and T1IB_FMTGID = '2005'
and T1IB_CostCenter = 'KST_101210'

602801  60220

select * from V2D_LinkPropertyBudget vdlpb 


select * from V2I_ProtelHouseStatus where PHS_mpehotel = 33

select * from V2D_Property_Attributes vdpa 


select * from vw_apaleo_open_days


CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `FMT_Reporting`.`vw_apaleo_open_days` AS
select
    distinct cast(`gt`.`date` as date) AS `date`,
    `gt`.`id` AS `id`
from
    `FMT_Reporting`.`V2I_GrossTransactions` `gt`
where
    ((`gt`.`reservation.status` in ('CheckedOut', 'InHouse'))
        and (`gt`.`date` >= '2023-01-01')
            and (`gt`.`creditedAccount.parentNumber` in ('5000', '6000'))
                and (`gt`.`accountName` is not null))
order by
    `gt`.`id`,
    `date`;


    SELECT  gt.property__id	 							 		AS id,
            gt.credited_account__name					 		AS accountName,
            ANY_VALUE(gt.`reservation__id`)              		AS bookingid,
            ANY_VALUE(gt.credited_account__parent_number)		AS accountParentNumber,
            ANY_VALUE(gt.command)                        		AS command,
            ANY_VALUE(gt.`date`)                         		AS `date`,
            ANY_VALUE(gt.`credited_account__name`)         		AS `creditedAccount.name`,
            ANY_VALUE(gt.`credited_account__number`)    		AS `creditedAccount.subNumber`,
            ANY_VALUE(gt.`credited_account__parent_number`) 	AS `creditedAccount.parentNumber`,
            SUM(gt.net_amount)                            		AS sum_net,
            SUM(gt.gross_amount)                          		AS sum_gross
    FROM    FMT_Reporting.raw_apaleo_grosstransactions gt
    WHERE   gt.`credited_account__parent_number` IN ('5000','6000')
      AND   gt.credited_account__name IS NOT NULL
      AND 	gt.command = 'PostCharge'
      
      
CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `FMT_Reporting`.`vw_apaleo_open_days` AS
SELECT DISTINCT
    CAST(`gt`.`date` AS DATE)      AS `date`,
    `gt`.`property__id`            AS `id`
FROM `FMT_Reporting`.`raw_apaleo_grosstransactions` `gt`
WHERE `gt`.`reservation__status` IN ('CheckedOut', 'InHouse')
  AND `gt`.`date` >= '2024-01-01'
  AND `gt`.`credited_account__parent_number` IN ('5000', '6000')
  AND `gt`.`credited_account__name` IS NOT NULL
ORDER BY `id`, `date`;




CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `FMT_Reporting`.`vw_apaleo_open_days` AS
SELECT
    gt.property__id        AS id,
    gt.`date`              AS `date`
    -- COUNT(*)               AS postings,
    -- SUM(gt.gross_amount)   AS sum_gross
FROM FMT_Reporting.raw_apaleo_grosstransactions gt
WHERE gt.reservation__status IN ('CheckedOut','InHouse')
  AND gt.`date` >= '2024-01-01'
  AND gt.credited_account__parent_number IN ('5000','6000')
  AND gt.credited_account__name IS NOT NULL
  AND gt.command = 'PostCharge'
GROUP BY gt.property__id, gt.`date`;




ALTER TABLE FMT_Reporting.raw_apaleo_grosstransactions
  ADD INDEX ix_opendays (
    credited_account__parent_number,
    reservation__status,
    `date`,
    property__id,
    gross_amount
  );