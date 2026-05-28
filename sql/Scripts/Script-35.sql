


select * 
from V2I_QuestAnswer
where QA_dateEntry > '2026-05-18'
and QA_response like '%2147%'


V2V_QuestAnswerPostStay
V2V_QuestAnswerPostStay
V2V_QuestAnswerPostStay
V2V_QuestQuestionPostStay
V2V_QuestAnswerPostStay

select * 
from V2V_QuestAnswerPostStay
where QA_dateEntry < '2026-05-19'




select * from raw_apaleo_folios raf 

select * from raw_apaleo_folios__charges where folio_id = 'ZRCEJKTJ-1-1';

select * from raw_apaleo_services ras 



select distinct sub_account_id, name 
from raw_apaleo_folios__charges
order by name

select * from 

select * from V2V_GrossTransactions
select * from V2I_GrossTransactions_Novacom

SELECT command, COUNT(*) AS cnt 
FROM V2I_GrossTransactions 
WHERE creditedAccount.parentNumber IN ('5000','6000') 
  AND accountName IS NOT NULL
GROUP BY command;

SELECT command, COUNT(*) AS cnt 
FROM V2I_GrossTransactions
GROUP BY command;

-- Und der accountName-Filter?
SELECT COUNT(*) 
FROM V2I_GrossTransactions 
WHERE accountName = 'Revenues Other - Merkur 20p.VAT postings (20%)';