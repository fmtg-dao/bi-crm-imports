-- =============================================================================
-- CITIZENSHIP CODE NORMALISATION
-- Table: gms_all_profiles
-- Replaces informal 3-letter abbreviations + variants → ISO 3166-1 alpha-2
-- =============================================================================

UPDATE gms_all_profiles
SET citizenship = CASE

  -- ── ALREADY VALID ISO-2 (pass-through, just trim) ────────────────────────
  WHEN TRIM(citizenship) IN (
    'AT','DE','IT','CZ','PL','US','HR','UA','ES','CN','LK','BA','ZA','SE',
    'IE','KR','BE','NL','SI','SK','HU','LV','CA','DK','FR','GR','NO','RU',
    'AU','LU','PT','TR','TW','CL','IN','JP','BR','EE','NZ','ID','FI','IL',
    'IQ','LB','RS','HK','SZ','TH','LI','SA','ME','BH','RO','AR','CR','VN',
    'AE','BG','IS','SG','LT','SV','PH','AD','XK','AL','GT','MX','KW','JO',
    'MA','TJ','MC','IR','KZ','PE','GN','GA','CO','MK','MY','GP','SM','KY',
    'LY','EG','AM','EC','BY','AZ','MD','MT','RW','UG','MN','NI','GE','SY',
    'UY','PY','ML','BN','BO','SR','AG','QA','TN','PW','NP','CM','SD','MO',
    'NG','FJ','CU','AW','BZ','CY','KE','BS','DJ','BF','SL','VE','GH','PK',
    'KN','UZ','DZ','PF','LR','ET','BB','NC','BD','SN','WF','DO','AF','JM',
    'GQ','TZ','ST','HN','AO','KG','KH','MG','ZM','NR','TO','GI','PA','OM',
    'LA','ER','TT','MU','FO','TM','PR','CG','VI','CD','NE','UM','GU','AS',
    'AQ','CH','BE','PL','NL','FR','RO','LU','DK','SE','NO','PT','GR','FI',
    'LT','LV','MT','EE','SI','MK','AL','BA','ME','RS','HR','HU','SK','BG',
    'MN','ZA','BO','TR','AE','CI','QA','TH','MY','VN','KZ','SA','NZ','HK',
    'MX','BR','IN','JP','CN','AU','CA','KR','IL','IR','SY','AR','CO','PH',
    'UA','BY','AZ','AM','MD','GE','UZ','TJ','KG','TM','KP'
  ) THEN TRIM(citizenship)

  -- ── SWITZERLAND ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Swi','SUI','Sch','Schweiz')               THEN 'CH'

  -- ── AUSTRIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Aus','AUT','Öst','Oes','Österreich')      THEN 'AT'

  -- ── USA ───────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('USA','Ame','Uni')                         THEN 'US'
  -- Note: 'Uni' could be UK too — defaulting US; adjust if needed

  -- ── CHINA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Chi')                                     THEN 'CN'

  -- ── CROATIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cro')                                     THEN 'HR'

  -- ── GERMANY ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ger')                                     THEN 'DE'

  -- ── SRI LANKA ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sri')                                     THEN 'LK'

  -- ── BOSNIA AND HERZEGOVINA ────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bos')                                     THEN 'BA'

  -- ── NETHERLANDS ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Net','Hol')                               THEN 'NL'

  -- ── CZECH REPUBLIC ────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cze','Rep','CS')                          THEN 'CZ'
  -- Note: 'Rep' is ambiguous but most likely CZ in this dataset context

  -- ── SWEDEN ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Swe')                                     THEN 'SE'

  -- ── IRELAND ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ire')                                     THEN 'IE'

  -- ── BELGIUM ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bel')                                     THEN 'BE'

  -- ── POLAND ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Pol')                                     THEN 'PL'

  -- ── ITALY ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ita')                                     THEN 'IT'

  -- ── GREECE ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gre')                                     THEN 'GR'

  -- ── SLOVENIA ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Slo')                                     THEN 'SI'

  -- ── HUNGARY ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Hun','Hon')                               THEN 'HU'

  -- ── LATVIA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lat')                                     THEN 'LV'

  -- ── SERBIA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ser')                                     THEN 'RS'

  -- ── CANADA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Can')                                     THEN 'CA'

  -- ── DENMARK ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Den')                                     THEN 'DK'

  -- ── FRANCE ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Fra','Fre')                               THEN 'FR'

  -- ── SPAIN ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Spa')                                     THEN 'ES'

  -- ── NORWAY ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Nor')                                     THEN 'NO'

  -- ── RUSSIA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Rus')                                     THEN 'RU'

  -- ── NORTH MACEDONIA ───────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mac')                                     THEN 'MK'

  -- ── JAPAN ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Jap')                                     THEN 'JP'

  -- ── LUXEMBOURG ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lux')                                     THEN 'LU'

  -- ── PORTUGAL ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Por')                                     THEN 'PT'

  -- ── COSTA RICA ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cos')                                     THEN 'CR'

  -- ── TURKEY ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Tur')                                     THEN 'TR'

  -- ── TAIWAN ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Tai')                                     THEN 'TW'

  -- ── INDIA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ind')                                     THEN 'IN'

  -- ── BRAZIL ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bra')                                     THEN 'BR'

  -- ── ESTONIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Est')                                     THEN 'EE'

  -- ── NEW ZEALAND ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('New')                                     THEN 'NZ'

  -- ── ROMANIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Rom')                                     THEN 'RO'

  -- ── FINLAND ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Fin')                                     THEN 'FI'

  -- ── ISRAEL ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Isr')                                     THEN 'IL'

  -- ── UKRAINE ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ukr')                                     THEN 'UA'

  -- ── ALBANIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Alb')                                     THEN 'AL'

  -- ── IRAN ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ira')                                     THEN 'IR'

  -- ── LIECHTENSTEIN ─────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lie')                                     THEN 'LI'

  -- ── ARGENTINA ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Arg')                                     THEN 'AR'

  -- ── MONACO ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mon')                                     THEN 'MC'

  -- ── THAILAND ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Tha')                                     THEN 'TH'

  -- ── MEXICO ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mex')                                     THEN 'MX'

  -- ── VIETNAM ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Vie')                                     THEN 'VN'

  -- ── BULGARIA ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bul')                                     THEN 'BG'

  -- ── ICELAND ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ice','Isl')                               THEN 'IS'

  -- ── VENEZUELA ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ven')                                     THEN 'VE'

  -- ── SINGAPORE ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sin')                                     THEN 'SG'

  -- ── LEBANON ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Leb')                                     THEN 'LB'

  -- ── MAURITIUS ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mau')                                     THEN 'MU'

  -- ── ESWATINI (Swaziland) ──────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Swa')                                     THEN 'SZ'

  -- ── LITHUANIA ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lit')                                     THEN 'LT'

  -- ── DJIBOUTI ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Dji')                                     THEN 'DJ'

  -- ── PANAMA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Pan')                                     THEN 'PA'

  -- ── PHILIPPINES ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Phi')                                     THEN 'PH'

  -- ── MOROCCO ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mar')                                     THEN 'MA'

  -- ── KUWAIT ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Kuw')                                     THEN 'KW'

  -- ── GUATEMALA ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gua')                                     THEN 'GT'

  -- ── JORDAN ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Jor')                                     THEN 'JO'

  -- ── EGYPT ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Egy')                                     THEN 'EG'

  -- ── EL SALVADOR ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('El')                                      THEN 'SV'

  -- ── MALDIVES ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mal')                                     THEN 'MV'

  -- ── TAJIKISTAN ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Taj')                                     THEN 'TJ'

  -- ── COLOMBIA ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Col')                                     THEN 'CO'

  -- ── KAZAKHSTAN ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Kaz')                                     THEN 'KZ'

  -- ── PERU ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Per')                                     THEN 'PE'

  -- ── GUINEA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gui')                                     THEN 'GN'

  -- ── SAN MARINO ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('San')                                     THEN 'SM'

  -- ── GABON ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gab')                                     THEN 'GA'

  -- ── PAKISTAN ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Pak')                                     THEN 'PK'

  -- ── LIBYA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lyb')                                     THEN 'LY'

  -- ── ARMENIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Arm')                                     THEN 'AM'

  -- ── ECUADOR ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ecu')                                     THEN 'EC'

  -- ── AZERBAIJAN ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Aze')                                     THEN 'AZ'

  -- ── SAUDI ARABIA ──────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sau')                                     THEN 'SA'

  -- ── BARBADOS ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bar')                                     THEN 'BB'

  -- ── UNITED KINGDOM ────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('UK','Bri','Wal')                          THEN 'GB'
  -- Note: Wales is part of GB; 'Bri' = British

  -- ── RWANDA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Rwa')                                     THEN 'RW'

  -- ── UGANDA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Uga')                                     THEN 'UG'

  -- ── QATAR ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Qat')                                     THEN 'QA'

  -- ── GEORGIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Geo')                                     THEN 'GE'

  -- ── SYRIA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Syr')                                     THEN 'SY'

  -- ── NEPAL ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Nep')                                     THEN 'NP'

  -- ── URUGUAY ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Uru')                                     THEN 'UY'

  -- ── KIRIBATI ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Kir')                                     THEN 'KI'

  -- ── BRUNEI ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bru')                                     THEN 'BN'

  -- ── TUVALU ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Tuv')                                     THEN 'TV'

  -- ── DOMINICAN REPUBLIC ────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Dom')                                     THEN 'DO'

  -- ── HAITI ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Hai')                                     THEN 'HT'

  -- ── ANDORRA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('And')                                     THEN 'AD'

  -- ── TUNISIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Tun')                                     THEN 'TN'

  -- ── PAPUA NEW GUINEA ──────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Pap')                                     THEN 'PG'

  -- ── CAMBODIA ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cam')                                     THEN 'KH'

  -- ── SIERRA LEONE ──────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sie')                                     THEN 'SL'

  -- ── EL SALVADOR (duplicate entry) ────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sva')                                     THEN 'SV'

  -- ── KENYA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ken')                                     THEN 'KE'

  -- ── SUDAN ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sud')                                     THEN 'SD'

  -- ── NIGERIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Nig')                                     THEN 'NG'

  -- ── SURINAME ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sur')                                     THEN 'SR'

  -- ── FIJI ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Fij')                                     THEN 'FJ'

  -- ── CUBA ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cub')                                     THEN 'CU'

  -- ── ARUBA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Aru')                                     THEN 'AW'

  -- ── CYPRUS ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cyp')                                     THEN 'CY'

  -- ── BAHRAIN ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bah')                                     THEN 'BH'

  -- ── BANGLADESH ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ban')                                     THEN 'BD'

  -- ── ALGERIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Alg')                                     THEN 'DZ'

  -- ── BURKINA FASO ──────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bur')                                     THEN 'BF'

  -- ── GHANA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gha')                                     THEN 'GH'

  -- ── NAMIBIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Nam')                                     THEN 'NA'

  -- ── SAINT LUCIA / SAINT KITTS (ambiguous 'Sai') ──────────────────────────
  WHEN TRIM(citizenship) IN ('Sai')                                     THEN NULL
  -- Too ambiguous — could be Saint Lucia (LC), Saint Kitts (KN), etc.

  -- ── UZBEKISTAN ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Uzb')                                     THEN 'UZ'

  -- ── LIBERIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lib')                                     THEN 'LR'

  -- ── ETHIOPIA ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Eth')                                     THEN 'ET'

  -- ── OMAN ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Oma')                                     THEN 'OM'

  -- ── YEMEN ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Jem')                                     THEN 'YE'

  -- ── PARAGUAY ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Par')                                     THEN 'PY'

  -- ── SENEGAL ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sen')                                     THEN 'SN'

  -- ── AFGHANISTAN ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Afg')                                     THEN 'AF'

  -- ── JAMAICA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Jam')                                     THEN 'JM'

  -- ── EQUATORIAL GUINEA ─────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Aeq')                                     THEN 'GQ'

  -- ── BOLIVIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bol')                                     THEN 'BO'

  -- ── SAO TOME AND PRINCIPE ─────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sao')                                     THEN 'ST'

  -- ── MADAGASCAR ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mad')                                     THEN 'MG'

  -- ── ZAMBIA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Zam')                                     THEN 'ZM'

  -- ── NAURU ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Nau')                                     THEN 'NR'

  -- ── TONGA ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ton')                                     THEN 'TO'

  -- ── GIBRALTAR ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gib')                                     THEN 'GI'

  -- ── LAOS ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Lao')                                     THEN 'LA'

  -- ── ERITREA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Eri')                                     THEN 'ER'

  -- ── TRINIDAD AND TOBAGO ───────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Tri')                                     THEN 'TT'

  -- ── PUERTO RICO ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Pue')                                     THEN 'PR'

  -- ── CONGO ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Con')                                     THEN 'CG'

  -- ── VIRGIN ISLANDS (US) ───────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Vir')                                     THEN 'VI'

  -- ── PALESTINE ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Pal')                                     THEN 'PS'

  -- ── ANTIGUA AND BARBUDA ───────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ant')                                     THEN 'AG'

  -- ── ZIMBABWE ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Zim')                                     THEN 'ZW'

  -- ── IVORY COAST ───────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ivo')                                     THEN 'CI'

  -- ── VANUATU ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Van')                                     THEN 'VU'

  -- ── COCOS (KEELING) ISLANDS ───────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Coc')                                     THEN 'CC'

  -- ── CHAD ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cha')                                     THEN 'TD'

  -- ── NICARAGUA ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Nic')                                     THEN 'NI'

  -- ── SOMALIA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Som')                                     THEN 'SO'

  -- ── ANGOLA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ang')                                     THEN 'AO'

  -- ── GUYANA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Guy')                                     THEN 'GY'

  -- ── BHUTAN ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bhu')                                     THEN 'BT'

  -- ── CENTRAL AFRICAN REPUBLIC ──────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cen')                                     THEN 'CF'

  -- ── MYANMAR ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Mya')                                     THEN 'MM'

  -- ── CAPE VERDE ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Cap')                                     THEN 'CV'

  -- ── LESOTHO ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Les')                                     THEN 'LS'

  -- ── SEYCHELLES ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Sey')                                     THEN 'SC'

  -- ── COOK ISLANDS ──────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Coo')                                     THEN 'CK'

  -- ── BENIN ─────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ben')                                     THEN 'BJ'

  -- ── BERMUDA ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ber')                                     THEN 'BM'

  -- ── EAST TIMOR (see TP above) ─────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Eas')                                     THEN 'TL'

  -- ── GAMBIA ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gam')                                     THEN 'GM'

  -- ── FAROE ISLANDS ─────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Far')                                     THEN 'FO'

  -- ── JERSEY ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Jer')                                     THEN 'JE'

  -- ── MOZAMBIQUE ────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Moz')                                     THEN 'MZ'

  -- ── BOTSWANA ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Bot')                                     THEN 'BW'

  -- ── REUNION ───────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Reu')                                     THEN 'RE'

  -- ── ALAND ISLANDS ─────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Ala')                                     THEN 'AX'

  -- ── NIUE ──────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Niu')                                     THEN 'NU'

  -- ── FALKLAND ISLANDS ──────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Fal')                                     THEN 'FK'

  -- ── GREENLAND ─────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gro')                                     THEN 'GL'

  -- ── KOSOVO ────────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('XK','KV')                                 THEN 'XK'

  -- ── CIS (Commonwealth of Independent States — not a country) ─────────────
  WHEN TRIM(citizenship) IN ('CIS')                                     THEN NULL

  -- ── GUERNSEY ──────────────────────────────────────────────────────────────
  WHEN TRIM(citizenship) IN ('Gue')                                     THEN 'GG'

  -- ── HEATHROW / INTERNAL CODES / GARBAGE → NULL ───────────────────────────
  WHEN TRIM(citizenship) IN (
    'Hea',        -- likely "Heathrow" or data entry error
    'Oth',        -- "Other"
    'Unk',        -- "Unknown"
    'UNKNOWN','0','XX','CIS',
    'Bri'         -- handled as GB above; duplicate safety
  )                                                                     THEN NULL

  ELSE citizenship   -- leave unmatched for review
END
WHERE citizenship IS NOT NULL;

SELECT ROW_COUNT() AS rows_updated;

-- ── REVIEW: anything still not a clean ISO-2 code ────────────────────────────
SELECT
  citizenship        AS remaining_value,
  COUNT(*)           AS cnt
FROM gms_all_profiles
WHERE citizenship IS NOT NULL
  AND (LENGTH(TRIM(citizenship)) != 2 OR citizenship != UPPER(citizenship))
GROUP BY citizenship
ORDER BY cnt DESC;