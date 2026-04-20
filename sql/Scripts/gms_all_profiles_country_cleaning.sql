-- =============================================================================
-- COUNTRY CODE NORMALISATION
-- Table: gms_all_profiles
-- Replaces all country variants → ISO 3166-1 alpha-2 codes
-- Values that cannot be resolved → NULL (review separately)
-- =============================================================================
 
UPDATE gms_all_profiles
SET country = CASE
 
  -- ── ALREADY VALID ISO-2 (pass-through, just trim) ────────────────────────
  WHEN TRIM(country) IN (
    'CH','AT','DE','IT','CZ','PL','US','HR','UA','ES','CN','LK','BA','ZA',
    'SE','IE','KR','BE','NL','SI','SK','HU','LV','CA','DK','FR','GR','NO',
    'RU','AU','LU','PT','TR','TW','CL','IN','JP','BR','EE','NZ','ID','FI',
    'IL','IQ','LB','RS','HK','SZ','TH','LI','SA','ME','BH','RO','AR','CR',
    'VN','AE','BG','IS','SG','LT','SV','PH','AD','AL','GT','MX','KW','JO',
    'MA','TJ','MC','IR','KZ','PE','GN','GA','CO','MK','MY','GP','SM','KY',
    'LY','EG','AM','EC','BY','AZ','MD','MT','RW','UG','MN','NI','GE','SY',
    'UY','PY','ML','BN','BO','SR','AG','QA','TN','PW','NP','CM','SD','MO',
    'NG','FJ','CU','AW','BZ','CY','KE','BS','DJ','BF','SL','VE','GH','PK',
    'KN','UZ','DZ','PF','LR','ET','BB','NC','BD','SN','WF','DO','AF','JM',
    'GQ','TZ','ST','HN','AO','KG','KH','MG','ZM','NR','TO','GI','PA','OM',
    'LA','ER','TT','MU','FO','TM','PR','CG','VI','IO','VG','CI','AI','AS',
    'TF','HM','GG','MW','VU','YE','CV','CC','TD','PM','GL','MV','SO','VA',
    'GY','PS','BT','HT','CF','MM','BI','GD','TC','ZW','LS','SC','CK','BJ',
    'BM','IM','GM','GW','DM','SJ','AQ','GF','JE','MZ','MS','AN','BW','MP',
    'RE','AX','PN','CD','KM','TK','CX','FK','YT','KP','PG','TV','EH','TG',
    'LC','SH','SB','VC','WS','MQ','GS','NE','UM'
  ) THEN TRIM(country)
 
  -- ── GERMANY ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('D','Deutschland')                              THEN 'DE'
 
  -- ── AUSTRIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Oesterreich','Österreich')                    THEN 'AT'
 
  -- ── SWITZERLAND ───────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Schweiz')                                     THEN 'CH'
 
  -- ── UNITED KINGDOM ────────────────────────────────────────────────────────
  -- UK is not official ISO-2 (should be GB) but widely used — map to GB
  WHEN TRIM(country) IN ('UK','Great Britain and No')                   THEN 'GB'
 
  -- ── CROATIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Croatia','Hrvatska','Croaria')                THEN 'HR'
 
  -- ── CZECH REPUBLIC ────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Czechia','CS')                                THEN 'CZ'
  -- Note: CS was the ISO code for Czechoslovakia (dissolved 1993)
 
  -- ── SERBIA ────────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Serbia','SRB')                                THEN 'RS'
 
  -- ── YUGOSLAVIA (historical) ───────────────────────────────────────────────
  -- YU dissolved — cannot determine successor state without more data → NULL
  WHEN TRIM(country) = 'YU'                                             THEN NULL
 
  -- ── UNITED ARAB EMIRATES ─────────────────────────────────────────────────
  WHEN TRIM(country) IN ('UAE')                                         THEN 'AE'
 
  -- ── KOSOVO ────────────────────────────────────────────────────────────────
  -- XK is the user-assigned (unofficial) ISO code for Kosovo
  WHEN TRIM(country) IN ('Kosovo','KV','XK')                            THEN 'XK'
 
  -- ── MONTENEGRO ────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Montenegro')                                  THEN 'ME'
 
  -- ── BOSNIA AND HERZEGOVINA ────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Bosnia')                                      THEN 'BA'
 
  -- ── NORTH MACEDONIA ───────────────────────────────────────────────────────
  -- MK already in pass-through above
 
  -- ── ROMANIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('România')                                     THEN 'RO'
 
  -- ── NETHERLANDS ───────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Nederland')                                   THEN 'NL'
 
  -- ── DENMARK ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Denmark.')                                    THEN 'DK'
 
  -- ── NORWAY ────────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Norge')                                       THEN 'NO'
 
  -- ── SLOVAKIA ──────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Slovak Republic')                             THEN 'SK'
 
  -- ── MOLDOVA ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Moldova, Republic of')                        THEN 'MD'
 
  -- ── KOREA (REPUBLIC OF) ───────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Korea (the Republic','KOREA NTH')             THEN 'KR'
  -- Note: KOREA NTH would be KP (North Korea) if literal — flagged as KR
  -- since context is likely a data entry error; review if needed
 
  -- ── SYRIA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('Syrian Arab Republic')                        THEN 'SY'
 
  -- ── CURACAO ───────────────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('CURACAO')                                     THEN 'CW'
 
  -- ── SAINT MARTIN ──────────────────────────────────────────────────────────
  -- French part = MF, Dutch part = SX — defaulting to MF (French side)
  WHEN TRIM(country) IN ('SAINT MARTIN')                                THEN 'MF'
 
  -- ── SAINT BARTHELEMY ──────────────────────────────────────────────────────
  WHEN TRIM(country) IN ('SAINT BARTHELEMY')                            THEN 'BL'
 
  -- ── TIMOR-LESTE ───────────────────────────────────────────────────────────
  -- TP was the old ISO code (now TL)
  WHEN TRIM(country) IN ('TP')                                          THEN 'TL'
 
  -- ── ARGENTINA ─────────────────────────────────────────────────────────────
  -- RA is the FIFA/IOC code, not ISO-2
  WHEN TRIM(country) IN ('RA')                                          THEN 'AR'
 
  -- ── SOUTH AFRICA ─────────────────────────────────────────────────────────
  -- ZA already in pass-through; no extra variants
 
  -- ── REGION / STATE ENTRY ERRORS → NULL ───────────────────────────────────
  -- "Salzburg" is an Austrian state, not a country
  WHEN TRIM(country) IN ('Salzburg')                                    THEN NULL
 
  -- ── GARBAGE / PLACEHOLDER / UNRESOLVABLE → NULL ──────────────────────────
  WHEN TRIM(country) IN (
    'XX',             -- not a real ISO code
    'Q',              -- single char, unresolvable
    '0',              -- numeric zero
    'default',
    'Auswaehlen',     -- German UI placeholder "Select..."
    'UNKNOWN',
    'en',             -- language code, not country
    'pravi klik',     -- Croatian for "right click" — data entry error
    'Other Africa',
    'yOther Africa',  -- typo variant
    'Other Foreign Countr'
  )                                                                     THEN NULL
 
  ELSE country  -- leave unmatched for review
END
WHERE country IS NOT NULL;
 
SELECT ROW_COUNT() AS rows_updated;
 
-- ── REVIEW: anything still not a clean ISO-2 code ────────────────────────────
SELECT
  country            AS remaining_value,
  COUNT(*)           AS cnt
FROM gms_all_profiles
WHERE country IS NOT NULL
  AND (LENGTH(TRIM(country)) != 2 OR country != UPPER(country))
GROUP BY country
ORDER BY cnt DESC;