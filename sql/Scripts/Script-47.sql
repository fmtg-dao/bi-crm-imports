        SELECT 
            '2026-06-18' AS Reservation_date,
            Year(ghd.GHD_datum) AS Rev_Year, 
            Month(ghd.GHD_datum) AS Rev_Month, 
            ghr.GHR_market AS Market, 
            ghd.GHD_mpehotel AS Mpehotel,     
            ghr.GHR_reschar AS Reschar,   
            
            SUM(GHD_n_EUR) AS Board_food_rev
            FROM V2I_GuestHistoryReservation ghr
        
        INNER JOIN V2I_GuestHistoryDaily_Detailed ghd
        ON ghd.GHD_leistacc = ghr.GHR_leistacc
        
        INNER JOIN V2D_TAA taa
        ON ghd.GHD_TAA = taa.TAA_TAA
WHERE
    taa.TAA_Arrangement = 1
    AND GHR_reschar NOT IN (2,3)
    AND GHD_mpehotel = 5
    AND ghr.GHR_market = 7
    AND Month(ghd.GHD_datum) = '5'
    AND Year(ghd.GHD_datum) = '2026'
            AND GHD_datum <= '2026-06-18'
            AND YEAR(GHD_datum) >= YEAR('2026-06-18') - 4
            
            AND ghr.GHR_datumres <= '2026-06-18'
            AND ghr.GHR_datumbis <= '2026-06-18'
        
        GROUP BY ghr.GHR_market, ghd.GHD_mpehotel, Year(ghd.GHD_datum), Month(ghd.GHD_datum), ghr.GHR_reschar
        
        
        
        
        
        
        
 SELECT 
            '2026-06-18' AS Reservation_date,
            Year(ghd.GHD_datum) AS Rev_Year, 
            Month(ghd.GHD_datum) AS Rev_Month, 
            ghr.GHR_market AS Market, 
            ghd.GHD_mpehotel AS Mpehotel,     
            ghr.GHR_reschar AS Reschar,   
            
            GHD_n_EUR AS Board_food_rev,
            ghr.*,
            ghd.*
            FROM V2I_GuestHistoryReservation ghr
        
        INNER JOIN V2I_GuestHistoryDaily_Detailed ghd
        ON ghd.GHD_leistacc = ghr.GHR_leistacc
        
        INNER JOIN V2D_TAA taa
        ON ghd.GHD_TAA = taa.TAA_TAA
WHERE
    taa.TAA_Arrangement = 1
    AND GHR_reschar NOT IN (2,3)
    AND GHD_mpehotel = 5
    AND ghr.GHR_market = 7
    -- AND ghr.ghr_leistacc = 15590213
    AND Month(ghd.GHD_datum) = '5'
    AND Year(ghd.GHD_datum) = '2026'
            AND GHD_datum <= '2026-06-18'
            AND YEAR(GHD_datum) >= YEAR('2026-06-18') - 4
            
            AND ghr.GHR_datumres <= '2026-06-18'
            AND ghr.GHR_datumbis <= '2026-06-18'
            
            

            
select * from V2I_GuestHistoryDaily_Detailed where GHD_leistacc  = 15590213
        