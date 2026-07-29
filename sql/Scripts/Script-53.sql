


select crsp.SourceSystem__c, count(*)
from crm_reservation_sfid_prod crsp 
where crsp.TotalRevenue__c is null
and crsp.Departure__c < '2026-05-01'
and crsp.Departure__c > '2026-01-01'
group by crsp.SourceSystem__c 


select *
from crm_reservation_sfid_prod crsp 
where crsp.TotalRevenue__c is null




select rag.`date`, sum(net_amount)
from raw_apaleo_grosstransactions rag 
where rag.property__id = 'FSG'
and rag.`date` >= '2026-07-04'
and rag.`date` <= '2026-07-05'
and command = 'PostCharge'
and rag.credited_account__name like 'Revenues Accommodation%'
group by rag.`date`




select * 
from V2V_GrossTransactions
where id = 'FSG'
and `date` = '2026-07-04' 


select *
from raw_apaleo_grosstransactions
where property__id = 'FSG'
and `date` = '2026-07-04' 
and credited_account__parent_number  in ('5000', '6000')
and `accountName` is not null



select *
from V2I_GrossTransactions
where id = 'FSG'
and `date` >= '2026-01-01' 
and `creditedAccount.parentNumber` in ('5000', '6000')
and `accountName` is not null



select id, count(*), sum(netAmount)
from vw_apaleo_gross_transactions
where 1=1 -- id = 'FSG'
and `date` >= '2026-01-01' 
and command = 'PostCharge'
group by id 


select id, count(*), sum(netAmount) 
from V2V_GrossTransactions
where 1=1 -- id = 'FSG'
and `date` >= '2026-01-01' 
and command = 'PostCharge'
group by id 



CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `FMT_Reporting`.`vw_apaleo_gross_transactions` AS
WITH gt_agg AS (
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
    GROUP BY gt.id, gt.credited_account__name

    UNION ALL

    SELECT  gtn.id,
            gtn.creditedAccount_name,
            ANY_VALUE(gtn.reservation_id),
            ANY_VALUE(gtn.accountParentNumber),
            ANY_VALUE(gtn.command),
            ANY_VALUE(gtn.`date`),
            ANY_VALUE(gtn.creditedAccount_name),
            ANY_VALUE(gtn.creditedAccount_subNumber),
            ANY_VALUE(gtn.creditedAccount_parentNumber),
            SUM(ROUND(gtn.netAmount, 2)),
            SUM(gtn.grossAmount)
    FROM    FMT_Reporting.V2I_GrossTransactions_Novacom gtn
    WHERE   gtn.creditedAccount_name IS NOT NULL
    GROUP BY gtn.id, gtn.creditedAccount_name
)
SELECT /*+ NO_INDEX(mp uq_mp_accountName) */
        g.bookingid, g.accountParentNumber, g.command, g.id, g.`date`,
        g.`creditedAccount.name`,
        ROUND(g.sum_net,   4) AS netAmount,
        g.`creditedAccount.subNumber`,
        ROUND(g.sum_gross, 4) AS grossAmount,
        g.`creditedAccount.parentNumber`,
        g.accountName,
        mp.TAA, mp.not_revenue,
        0 AS reschar
FROM    gt_agg g
JOIN    FMT_Reporting.V2D_Apaleo_grossTransactions mp
          ON mp.accountName = g.accountName;
    
    
    
    
    
    
    
    
SELECT id,
       SUM(src='new')                                    AS cnt_new,
       SUM(src='old')                                    AS cnt_old,
       SUM(src='new') - SUM(src='old')                   AS d_cnt,
       ROUND(SUM(IF(src='new',  netAmount, 0)), 2)       AS net_new,
       ROUND(SUM(IF(src='old',  netAmount, 0)), 2)       AS net_old,
       ROUND(SUM(IF(src='new',  netAmount, -netAmount)), 2) AS d_net
FROM (
    SELECT 'new' AS src, id, netAmount
    FROM   vw_apaleo_gross_transactions
    WHERE  `date` >= '2026-01-01' AND command = 'PostCharge'
    UNION ALL
    SELECT 'old', id, netAmount
    FROM   FMT_Reporting.V2V_GrossTransactions
    WHERE  `date` >= '2026-01-01' AND command = 'PostCharge'
) x
GROUP BY id
HAVING d_cnt <> 0 OR ABS(d_net) > 0.01
ORDER BY ABS(d_net) DESC;
