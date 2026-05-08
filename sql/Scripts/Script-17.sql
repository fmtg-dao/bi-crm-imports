
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
where accountName like 'Revenues Other - Shop Magazines & Papers 10% (10%)%'

INSERT IGNORE INTO V2D_Apaleo_grossTransactions
    (accountName, GroupOrd1, GroupOrd1_order, TAA, not_revenue)
VALUES
    ('Revenues Food and beverages - Shop 1 Food 10% (10%)', 'F&B', 2, 102006, NULL),
    ('Revenues Food and beverages - Restaurant 1 Non Alcoholic Drinks (20%)', 'F&B', 2, 102006, NULL),
    ('Revenues Food and beverages - Shop 1 Non Alcoholic Drinks (20%)', 'F&B', 2, 102006, NULL),
    ('Revenues Food and beverages - Shop 1 Food 20% (20%)', 'F&B', 2, 102006, NULL),
    ('Revenues Food and beverages - Shop 1 Beer (20%)', 'F&B', 2, 102006, NULL),
    ('Revenues Accommodation - Home Accommodation (no VAT)', 'Logis', 1, 101001, NULL),
    ('Revenues Accommodation - Tent Accommodation (10%)', 'Logis', 1, 101000, NULL),
    ('Revenues Food and beverages - Restaurant 3 Beer (20%)', 'F&B', 2, 102006, NULL),
    ('Revenues Food and beverages - Shop 1 Food 10% (13%)', 'F&B', 2, 102006, NULL),
    ('Revenues Other - Access/Zugang (20%)', 'Sales', 5, 403013, NULL),
    ('Revenues Other - Additional Items 10% (10%)', 'Sales', 5, 403013, NULL),
    ('Revenues Other - Miscellaneous 0% (0%)', 'Sales', 5, 105002, NULL),
    ('Revenues Other - Miscellaneous 0% (no VAT)', 'Sales', 5, 105002, NULL),
    ('Revenues Other - Non-Food & Hygiene Articles (20%)', 'Sales', 5, 403013, NULL),
    ('Revenues Other - Sauna (13%)', 'Spa', 3, 338206, NULL),
    ('Revenues Other - Shop Magazines & Papers 10% (10%)', 'Sales', 5, 105009, NULL);





