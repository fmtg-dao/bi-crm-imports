
/* FIX APALEO REPORT  */

select vigt.accountName, sum(netAmount) 
from V2I_GrossTransactions vigt
left join V2D_Apaleo_grossTransactions tr
	on vigt.accountName = tr.accountName 
where 1=1 -- vigt.id = 'FCG'
and vigt.`creditedAccount.parentNumber` = '5000'
-- and vigt.accountName like 'Revenues Food and beverages%'
and vigt.`date` >= '2025-01-01'
and vigt.`date` <= '2026-04-22'
and tr.accountName is null
group by vigt.accountName



slect * from V2D_Gross

select * 
from V2D_Apaleo_grossTransactions

INSERT INTO V2D_Apaleo_grossTransactions
(accountName, GroupOrd1, GroupOrd1_order, TAA, not_revenue)
VALUES
('Revenues Food and beverages - Shop 1 Food 10% (10%)', 'F&B', 2, 102006, null),
('Revenues Food and beverages - Restaurant 1 Non Alcoholic Drinks (20%)', 'F&B', 2, 102006, null),
('Revenues Food and beverages - Shop 1 Non Alcoholic Drinks (20%)', 'F&B', 2, 102006, null),
('Revenues Food and beverages - Shop 1 Food 20% (20%)', 'F&B', 2, 102006, null),
('Revenues Food and beverages - Shop 1 Beer (20%)', 'F&B', 2, 102006, null);