

-- Null / blank email
SELECT
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
  SUM(CASE WHEN TRIM(email) = '' THEN 1 ELSE 0 END) AS blank_email,
  SUM(CASE WHEN email IS NOT NULL AND TRIM(email) != '' THEN 1 ELSE 0 END) AS has_email
FROM gms_all_profiles
WHERE exclude_email = 0 
and current_opt_in = 'Yes' 
and missing_name = 0
;


SELECT
  COUNT(*) AS total,
  -- fname
  SUM(CASE WHEN fname IS NULL OR TRIM(fname)='' THEN 1 ELSE 0 END) AS fname_missing,
  -- lname
  SUM(CASE WHEN lname IS NULL OR TRIM(lname)='' THEN 1 ELSE 0 END) AS lname_missing,
  -- email
  SUM(CASE WHEN email IS NULL OR TRIM(email)='' THEN 1 ELSE 0 END) AS email_missing,
  -- salutation
  SUM(CASE WHEN salutation IS NULL OR TRIM(salutation)='' THEN 1 ELSE 0 END) AS salutation_missing,
  -- phone fields
  SUM(CASE WHEN cell_phone IS NULL OR TRIM(cell_phone)='' THEN 1 ELSE 0 END) AS cell_missing,
  SUM(CASE WHEN home_phone IS NULL OR TRIM(home_phone)='' THEN 1 ELSE 0 END) AS home_missing,
  SUM(CASE WHEN office_phone IS NULL OR TRIM(office_phone)='' THEN 1 ELSE 0 END) AS office_missing,
  -- address
  SUM(CASE WHEN address IS NULL OR TRIM(address)='' THEN 1 ELSE 0 END) AS address_missing,
  SUM(CASE WHEN city IS NULL OR TRIM(city)='' THEN 1 ELSE 0 END) AS city_missing,
  SUM(CASE WHEN state IS NULL OR TRIM(state)='' THEN 1 ELSE 0 END) AS state_missing,
  SUM(CASE WHEN country IS NULL OR TRIM(country)='' THEN 1 ELSE 0 END) AS country_missing,
  -- company
  SUM(CASE WHEN company IS NULL OR TRIM(company)='' THEN 1 ELSE 0 END) AS company_missing
FROM gms_all_profiles
WHERE exclude_email = 0 
and current_opt_in = 'Yes' 
and missing_name = 0
;

select count(*) 
FROM gms_all_profiles
WHERE exclude_email = 0 
and current_opt_in = 'Yes' 
and missing_name = 0
and (lname IS NULL OR TRIM(lname)='')

-- ALL CAPS names
SELECT fname, lname, email FROM gms_all_profiles
WHERE fname = UPPER(fname) AND fname REGEXP '[A-Z]{2,}'
   OR lname = UPPER(lname) AND lname REGEXP '[A-Z]{2,}'
LIMIT 100;

-- all lowercase names
SELECT fname, lname, email FROM gms_all_profiles
WHERE fname = LOWER(fname) AND fname REGEXP '[a-z]{2,}'
   OR lname = LOWER(lname) AND lname REGEXP '[a-z]{2,}'
LIMIT 100;

-- Digits inside name fields
SELECT fname, lname, email FROM gms_all_profiles
WHERE fname REGEXP '[0-9]' OR lname REGEXP '[0-9]'
LIMIT 100;

-- Special / non-alpha chars (allow hyphens and apostrophes as legitimate)
SELECT fname, lname, email FROM gms_all_profiles
WHERE fname REGEXP '[^a-zA-ZÀ-ÖØ-öø-ÿ ''\-\.]'
   OR lname REGEXP '[^a-zA-ZÀ-ÖØ-öø-ÿ ''\-\.]'
LIMIT 100;

-- Single-character names (likely garbage)
SELECT fname, lname, email FROM gms_all_profiles
WHERE LENGTH(TRIM(fname)) = 1 OR LENGTH(TRIM(lname)) = 1
LIMIT 100;

-- Common placeholder / test values
SELECT fname, lname, email FROM gms_all_profiles
WHERE LOWER(TRIM(fname)) IN ('test','unknown','n/a','none','na','null','xxx','first')
   OR LOWER(TRIM(lname)) IN ('test','unknown','n/a','none','na','null','xxx','last')
LIMIT 100;




-- Basic format check (must have @, a dot after @, no spaces)
SELECT email FROM gms_all_profiles
WHERE email IS NOT NULL
  AND (
    email NOT REGEXP '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
    OR email LIKE '% %'
    OR email LIKE '%..%'
  )
LIMIT 200;

-- Emails with leading/trailing whitespace
SELECT email, TRIM(email) AS cleaned FROM gms_all_profiles
WHERE email != TRIM(email)
LIMIT 100;

-- Mixed case (should be lower)
SELECT email FROM gms_all_profiles
WHERE email != LOWER(email) AND email IS NOT NULL
LIMIT 100;

-- Top domains (spot disposable / unusual domains)
SELECT domain, COUNT(*) AS cnt
FROM gms_all_profiles
WHERE domain IS NOT NULL AND domain != ''
GROUP BY domain
ORDER BY cnt DESC
LIMIT 50;

-- Known disposable/throwaway domains
SELECT email, domain FROM gms_all_profiles
WHERE domain IN (
  'mailinator.com','guerrillamail.com','tempmail.com',
  'throwaway.email','yopmail.com','trashmail.com',
  'sharklasers.com','guerrillamailblock.com'
)
LIMIT 100;

select * from gms_all_profiles 
update gms_all_profiles 
set exclude_email = 1
WHERE email IS NOT NULL
  AND (
    email NOT REGEXP '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
    OR email LIKE '% %'
    OR email LIKE '%..%'
  )
  
  
  
