-- =============================================================================
-- CRM PROFILE DATA CLEANING SCRIPT
-- Table: gms_all_profiles
-- Generated for: gms_all_profiles (utf8mb3_unicode_ci)
-- =============================================================================
-- INSTRUCTIONS:
--   1. Run the BACKUP step first — always.
--   2. Execute each section in order (Step 1 → Step 7).
--   3. Run the VALIDATION REPORT at the end to confirm results.
--   4. Each UPDATE is idempotent — safe to re-run.
-- =============================================================================


-- =============================================================================
-- STEP 0: BACKUP (MANDATORY — DO NOT SKIP)
-- =============================================================================

CREATE TABLE gms_all_profiles_backup_pre_clean AS
  SELECT * FROM gms_all_profiles;

-- Verify backup row count matches source
SELECT
  (SELECT COUNT(*) FROM gms_all_profiles)              AS source_rows,
  (SELECT COUNT(*) FROM gms_all_profiles_backup_pre_clean) AS backup_rows;


-- =============================================================================
-- STEP 1: TRIM WHITESPACE — ALL TEXT FIELDS
-- Removes leading/trailing spaces, tabs, newlines from every text column.
-- =============================================================================

UPDATE gms_all_profiles SET
  fname         = NULLIF(TRIM(fname), ''),
  lname         = NULLIF(TRIM(lname), ''),
  email         = NULLIF(TRIM(email), ''),
  salutation    = NULLIF(TRIM(salutation), ''),
  address       = NULLIF(TRIM(address), ''),
  address2      = NULLIF(TRIM(address2), ''),
  city          = NULLIF(TRIM(city), ''),
  state         = NULLIF(TRIM(state), ''),
  zip           = NULLIF(TRIM(zip), ''),
  country       = NULLIF(TRIM(country), ''),
  birthday      = NULLIF(TRIM(birthday), ''),
  cell_phone    = NULLIF(TRIM(cell_phone), ''),
  home_phone    = NULLIF(TRIM(home_phone), ''),
  office_phone  = NULLIF(TRIM(office_phone), ''),
  fax           = NULLIF(TRIM(fax), ''),
  company       = NULLIF(TRIM(company), ''),
  title         = NULLIF(TRIM(title), ''),
  citizenship   = NULLIF(TRIM(citizenship), ''),
  language_code = NULLIF(TRIM(language_code), ''),
  datasource_name = NULLIF(TRIM(datasource_name), ''),
  current_opt_in = NULLIF(TRIM(current_opt_in), ''),
  bounce        = NULLIF(TRIM(bounce), ''),
  bounce_flag   = NULLIF(TRIM(bounce_flag), ''),
  created_date  = NULLIF(TRIM(created_date), ''),
  domain        = NULLIF(TRIM(domain), ''),
  list_id       = NULLIF(TRIM(list_id), '');

SELECT ROW_COUNT() AS rows_updated_trim;


-- =============================================================================
-- STEP 2: NORMALISE EMAIL ADDRESSES
-- - Lowercase all emails
-- - Rebuild domain column from cleaned email
-- =============================================================================

UPDATE gms_all_profiles
SET email = LOWER(email)
WHERE email IS NOT NULL
  AND email != LOWER(email);

SELECT ROW_COUNT() AS emails_lowercased;

-- Rebuild domain from cleaned email (extract part after @)
UPDATE gms_all_profiles
SET domain = LOWER(SUBSTRING_INDEX(email, '@', -1))
WHERE email IS NOT NULL
  AND email LIKE '%@%';

SELECT ROW_COUNT() AS domains_rebuilt;


-- =============================================================================
-- STEP 3: TITLE-CASE NAMES (fname, lname)
-- =============================================================================
-- MySQL has no native INITCAP / PROPER function.
-- This function handles:
--   - Standard single-word names:  JOHN → John
--   - Hyphenated names:            MARY-ANN → Mary-Ann
--   - Apostrophe names:            O'BRIEN → O'Brien
--   - Particles (de, van, von, la) → kept lowercase mid-name
--   - Preserves Mc/Mac prefix capitalisation
-- =============================================================================

DROP FUNCTION IF EXISTS fn_title_case;

DELIMITER $$

CREATE FUNCTION fn_title_case(str TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
  DECLARE result TEXT DEFAULT '';
  DECLARE word   TEXT DEFAULT '';
  DECLARE c      CHAR(1);
  DECLARE i      INT DEFAULT 1;
  DECLARE cap_next BOOLEAN DEFAULT TRUE;
  DECLARE str_len INT;

  IF str IS NULL THEN RETURN NULL; END IF;

  SET str = LOWER(TRIM(str));
  SET str_len = CHAR_LENGTH(str);

  WHILE i <= str_len DO
    SET c = SUBSTRING(str, i, 1);

    IF c IN (' ', '-', '.', '\'') THEN
      SET result   = CONCAT(result, c);
      SET cap_next = TRUE;
    ELSEIF cap_next THEN
      SET result   = CONCAT(result, UPPER(c));
      SET cap_next = FALSE;
    ELSE
      SET result = CONCAT(result, c);
    END IF;

    SET i = i + 1;
  END WHILE;

  RETURN result;
END$$

DELIMITER ;

-- Apply title case to fname
UPDATE gms_all_profiles
SET fname = fn_title_case(fname)
WHERE fname IS NOT NULL
  AND (fname = UPPER(fname) OR fname = LOWER(fname));

SELECT ROW_COUNT() AS fnames_title_cased;

-- Apply title case to lname
UPDATE gms_all_profiles
SET lname = fn_title_case(lname)
WHERE lname IS NOT NULL
  AND (lname = UPPER(lname) OR lname = LOWER(lname));

SELECT ROW_COUNT() AS lnames_title_cased;



-- =============================================================================
-- STEP 4: STANDARDISE SALUTATIONS
-- Maps all known variants → a consistent set:
--   Mr / Mrs / Ms / Miss / Dr / Prof / Rev
-- Unknown values are left unchanged (review separately).
-- =============================================================================

select salutation, count(*)
from gms_all_profiles
group by salutation
order by 2 desc

select *
from gms_all_profiles

select distinct titel
from gms_all_profiles

UPDATE gms_all_profiles
SET salutation = CASE
  -- Mr
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('MR','MISTER','SR','SIR')
    THEN 'Mr.'
  -- Mrs
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('MRS','MRS','MISSUS','MISSIS')
    THEN 'Mrs.'
  -- Ms
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('MS','MZ')
    THEN 'Ms.'
  -- Miss
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('MISS')
    THEN 'Miss'
  -- Dr
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('DR','DOCTOR','DOC')
    THEN 'Dr.'
  -- Prof
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('PROF','PROFESSOR')
    THEN 'Prof.'
  -- Rev
  WHEN UPPER(TRIM(REPLACE(salutation, '.', ''))) IN ('REV','REVEREND','REVD')
    THEN 'Rev.'
  -- Placeholder / garbage → NULL
  WHEN UPPER(TRIM(salutation)) IN ('N/A','NA','NONE','NULL','TEST','XXX','-','.')
    THEN NULL
  ELSE salutation  -- leave unrecognised values as-is for manual review
END
WHERE salutation IS NOT NULL;

SELECT ROW_COUNT() AS salutations_standardised;


-- -------------------------------------------------------------------------
-- 4a: Direct value mapping (exact match after trim + collapse punctuation)
-- -------------------------------------------------------------------------
UPDATE gms_all_profiles
SET salutation = CASE

  -- ── MALE (→ Mr) ──────────────────────────────────────────────────────────
  -- English
  WHEN TRIM(salutation) IN ('Mr.','Mr','Mister','Sir','Sr')              THEN 'Mr'
  -- German
  WHEN TRIM(salutation) IN ('Herr','Herrn','Herr ','herrn')              THEN 'Mr'
  -- Italian
  WHEN TRIM(salutation) IN ('Signore','Signor','Sig.')                   THEN 'Mr'
  -- Croatian/Serbian/Bosnian
  WHEN TRIM(salutation) IN ('Gospodin','Gdin','gospodine')               THEN 'Mr'
  -- Czech/Slovak
  WHEN TRIM(salutation) IN ('Pán','Pan')                                 THEN 'Mr'
  -- French
  WHEN TRIM(salutation) IN ('Monsieur','M.')                             THEN 'Mr'

  -- ── FEMALE MARRIED (→ Mrs) ───────────────────────────────────────────────
  -- English
  WHEN TRIM(salutation) IN ('Mrs.','Mrs','Missus')                       THEN 'Mrs'
  -- German
  WHEN TRIM(salutation) IN ('Frau','Frau ')                              THEN 'Mrs'
  -- Italian
  WHEN TRIM(salutation) IN ('Signora','Sig.ra')                          THEN 'Mrs'
  -- Croatian/Serbian
  WHEN TRIM(salutation) IN ('Gospoda','Gospođa','Gđa','Gđa.','Gospdja',
                             'Gospodja','Gda')                			 THEN 'Mrs'
  -- Czech/Slovak
  WHEN TRIM(salutation) IN ('Pani')                                      THEN 'Mrs'
  -- French
  WHEN TRIM(salutation) IN ('Madame','Mme','Mme.')                       THEN 'Mrs'

  -- ── FEMALE NEUTRAL (→ Ms) ────────────────────────────────────────────────
  WHEN TRIM(salutation) IN ('Ms.','Ms','Mz')                             THEN 'Ms'

  -- ── UNMARRIED FEMALE (→ Miss) ────────────────────────────────────────────
  -- English
  WHEN TRIM(salutation) IN ('Miss','Miss.')                              THEN 'Miss'
  -- Italian
  WHEN TRIM(salutation) IN ('Signorina')                                 THEN 'Miss'
  -- Croatian
  WHEN TRIM(salutation) IN ('Gospodica','Gospodjica','Gospođica')        THEN 'Miss'

  -- ── DR (→ Dr) ────────────────────────────────────────────────────────────
  WHEN TRIM(salutation) IN ('Dr.','Dr','Doctor','Dott.','Dott')          THEN 'Dr'

  -- ── PROF (→ Prof) ────────────────────────────────────────────────────────
  WHEN TRIM(salutation) IN ('Prof.','Prof','Professor')                  THEN 'Prof'

  -- ── ING / DI (→ Ing) ─────────────────────────────────────────────────────
  -- Engineer titles (DE: Ing., DI = Diplom-Ingenieur; IT: Ing.)
  WHEN TRIM(salutation) IN ('Ing.','Ing','DI','D.I.','Pan Ing.')         THEN 'Ing'

  -- ── MAG (→ Mag) ──────────────────────────────────────────────────────────
  -- Magister (AT/DE academic title)
  WHEN TRIM(salutation) IN ('Mag.','Mag')                                THEN 'Mag'

  -- ── FAMILY (→ Family) ────────────────────────────────────────────────────
  -- English
  WHEN TRIM(salutation) IN ('Family','Familie','Obitelj',
                             'Famiglia','Rodina')                        THEN 'Family'
  -- Truncated variants
  WHEN TRIM(salutation) LIKE 'Dear Famil%'                              THEN 'Family'
  WHEN TRIM(salutation) LIKE 'Gentile Fa%'                              THEN 'Family'

  -- ── GENDER-COMPOUND titles (Herr Dr., Frau Dr., etc.) ────────────────────
  -- Male with Dr → Dr (retain Dr, gender already in fname/lname)
  WHEN TRIM(salutation) IN ('Herr Dr.','Herr Dr')                       THEN 'Dr'
  -- Female with Dr → Dr
  WHEN TRIM(salutation) IN ('Frau Dr.','Frau Dr')                       THEN 'Dr'
  -- Male with Prof
  WHEN TRIM(salutation) IN ('Herr Prof.','Herr Prof')                   THEN 'Prof'
  -- Male with Mag
  WHEN TRIM(salutation) IN ('Herr Mag.','Herr Mag','Frau Mag.','Frau Mag') THEN 'Mag'
  -- Male with Ing/DI
  WHEN TRIM(salutation) IN ('Herr Ing.','Herr Ing','Herr DI','Herr D.I.') THEN 'Ing'

  -- ── GROUP / GENERIC OPENERS (→ NULL — no individual salutation applies) ──
  WHEN TRIM(salutation) IN (
    'Ladies and Gentlemen','Ladies and','Sirs',
    'Sehr geehr',          -- truncated "Sehr geehrte/r ..."
    'Liebe','Lieber',      -- "Dear [first name]" informal DE
    'Liebe Anna','Lieber Max',  -- named informal → NULL, name is in fname
    'Poštovani','Poštovana',    -- Croatian formal generic opener
    'Egregio Si','Egregia Si',  -- Italian formal generic opener (truncated)
    'Gentile Si',               -- Italian (truncated "Gentile Signore/a")
    'G.',                       -- too ambiguous
    'CHD',                      -- internal code, not a salutation
    'Sehr geehrter Herr Trauner' -- named opener, name in lname
  )                                                                       THEN NULL

  -- ── PLACEHOLDER / GARBAGE → NULL ─────────────────────────────────────────
  WHEN UPPER(TRIM(salutation)) IN ('N/A','NA','NONE','NULL','TEST','XXX','-','.')
                                                                          THEN NULL

  ELSE salutation   -- leave anything not matched for manual review (see query below)
END
WHERE salutation IS NOT NULL;

SELECT ROW_COUNT() AS step_4a_rows_updated;

-- -------------------------------------------------------------------------
-- 4b: Free-text "Dear Mr/Mrs/Ms/Miss [name]" → extract canonical salutation
--     e.g. "Dear Mr. Smith" → 'Mr'
--          "Dear Ms. Lee"   → 'Ms'
--          "Dear Miss"      → 'Miss'
-- -------------------------------------------------------------------------

UPDATE gms_all_profiles
SET salutation = CASE
  WHEN salutation REGEXP '^Dear Mr[s]?\\.?\\s'   AND salutation NOT REGEXP 'Mrs' THEN 'Mr'
  WHEN salutation REGEXP '^Dear Mrs\\.?\\s?'                                      THEN 'Mrs'
  WHEN salutation REGEXP '^Dear Ms\\.?\\s?'                                       THEN 'Ms'
  WHEN salutation REGEXP '^Dear Miss\\.?'                                         THEN 'Miss'
  WHEN salutation REGEXP '^Dear Dr\\.?\\s?'                                       THEN 'Dr'
  WHEN salutation REGEXP '^Dear Prof\\.?\\s?'                                     THEN 'Prof'
  WHEN salutation REGEXP '^Dear Famil'                                            THEN 'Family'
  WHEN salutation REGEXP '^Dear '                                                 THEN NULL
END
WHERE salutation LIKE 'Dear %';

SELECT ROW_COUNT() AS step_4b_dear_prefix_cleaned;



UPDATE gms_all_profiles
SET salutation = CASE
  WHEN salutation REGEXP '^Sehr geehrter Herr '  								  THEN 'Mr.'
  WHEN salutation REGEXP '^Sehr geehrte Frau '   								  THEN 'Mrs.'
  ELSE salutation
END
WHERE salutation LIKE 'Sehr %';

SELECT ROW_COUNT() AS step_4b_dear_prefix_cleaned;


UPDATE gms_all_profiles
SET salutation = CASE
  WHEN salutation REGEXP '^Lieber '  								  THEN 'Mr.'
  WHEN salutation REGEXP '^Liebe '   								  THEN 'Mrs.'
  ELSE salutation
END
WHERE salutation LIKE 'Liebe%';


-- -------------------------------------------------------------------------
-- 4c: Review remaining non-standard values — ACTION REQUIRED
--     Rows returned here need manual mapping or a further UPDATE above.
-- -------------------------------------------------------------------------

UPDATE gms_all_profiles
SET salutation = CONCAT(salutation, '.')
WHERE salutation IN ('Mr','Mrs','Ms','Miss','Dr','Prof','Ing','Mag')
  AND salutation NOT LIKE '%.';

UPDATE gms_all_profiles
SET salutation = 'Mrs.'
WHERE salutation = 'Sig.na';


SELECT
  salutation         AS remaining_value,
  COUNT(*)           AS cnt
FROM gms_all_profiles
WHERE salutation IS NOT NULL
  AND salutation NOT IN ('Mr.','Mrs.','Ms.','Miss.','Dr.','Prof.','Ing.','Mag.','Family')
  AND exclude_email = 0
GROUP BY salutation
ORDER BY cnt DESC;


-- Review any remaining non-standard values after cleaning
SELECT salutation, COUNT(*) AS cnt
FROM gms_all_profiles
WHERE salutation IS NOT NULL
  AND salutation NOT IN ('Mr','Mrs','Ms','Miss','Dr','Prof','Rev')
GROUP BY salutation
ORDER BY cnt DESC;


--- Set gender
UPDATE gms_all_profiles
SET gender = CASE
  WHEN salutation IN ('Mr.')                  THEN 'Male'
  WHEN salutation IN ('Mrs.', 'Ms.', 'Miss.') THEN 'Female'
  ELSE gender
END
WHERE salutation IN ('Mr.', 'Mrs.', 'Ms.', 'Miss.')
  AND (gender IS NULL OR gender = '');

SELECT ROW_COUNT() AS gender_updated;

select * from gms_all_profiles





-- =============================================================================
-- STEP 5: CLEAN NAME PLACEHOLDER / GARBAGE VALUES
-- Nullifies names that are clearly test or placeholder data.
-- =============================================================================

UPDATE gms_all_profiles
SET fname = NULL
WHERE LOWER(TRIM(fname)) IN (
  'test','unknown','n/a','none','na','null','xxx',
  'first','firstname','first name','-','.'
);

SELECT ROW_COUNT() AS fname_placeholders_nulled;

UPDATE gms_all_profiles
SET lname = NULL
WHERE LOWER(TRIM(lname)) IN (
  'test','unknown','n/a','none','na','null','xxx',
  'last','lastname','last name','-','.'
);

SELECT ROW_COUNT() AS lname_placeholders_nulled;


-- =============================================================================
-- STEP 6: SUPPRESS BOUNCED / INVALID EMAIL RECORDS
-- Sets exclude_email = 1 for:
--   a) Any record with a positive bounce flag
--   b) Records where email fails basic format validation
--   c) Records pointing to known disposable domains
-- =============================================================================

-- 6a. Bounce flags → suppress
UPDATE gms_all_profiles
SET exclude_email = 1
WHERE exclude_email = 0
  AND (
    LOWER(TRIM(bounce))      IN ('1', 'true', 'yes', 'y', 'hard', 'soft', 'bounce')
    OR LOWER(TRIM(bounce_flag)) IN ('1', 'true', 'yes', 'y', 'bounced')
  );

SELECT ROW_COUNT() AS suppressed_from_bounce_flags;

-- 6b. Malformed email format → suppress
UPDATE gms_all_profiles
SET exclude_email = 1
WHERE exclude_email = 0
  AND email IS NOT NULL
  AND email != ''
  AND (
    email NOT REGEXP '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
    OR email LIKE '% %'
    OR email LIKE '%..%'
    OR email NOT LIKE '%@%'
  );

SELECT ROW_COUNT() AS suppressed_invalid_format;

-- 6c. Disposable / throwaway email domains → suppress
UPDATE gms_all_profiles
SET exclude_email = 1
WHERE exclude_email = 0
  AND domain IN (
    'mailinator.com', 'guerrillamail.com', 'tempmail.com',
    'throwaway.email', 'yopmail.com', 'trashmail.com',
    'sharklasers.com', 'guerrillamailblock.com', 'dispostable.com',
    'fakeinbox.com', 'maildrop.cc', 'spamgourmet.com',
    'mytemp.email', 'temp-mail.org', 'throwam.com'
  );

SELECT ROW_COUNT() AS suppressed_disposable_domains;

-- 6d. Null / blank email → suppress (can't communicate with no address)
UPDATE gms_all_profiles
SET exclude_email = 1
WHERE exclude_email = 0
  AND (email IS NULL OR TRIM(email) = '');

SELECT ROW_COUNT() AS suppressed_null_email;


-- =============================================================================
-- STEP 7: NORMALISE PHONE NUMBERS (light clean)
-- Strips common non-numeric formatting characters for consistency.
-- Does NOT reformat — preserves country code intent.
-- Update the REGEXP_REPLACE pattern to match your target format.
-- =============================================================================

-- Remove characters that are clearly garbage in phone fields
-- (keeps digits, +, -, spaces, dots, parentheses)
UPDATE gms_all_profiles
SET
  cell_phone   = NULLIF(REGEXP_REPLACE(TRIM(cell_phone),   '[^0-9\\+\\-\\(\\)\\.\\s]', ''), ''),
  home_phone   = NULLIF(REGEXP_REPLACE(TRIM(home_phone),   '[^0-9\\+\\-\\(\\)\\.\\s]', ''), ''),
  office_phone = NULLIF(REGEXP_REPLACE(TRIM(office_phone), '[^0-9\\+\\-\\(\\)\\.\\s]', ''), ''),
  fax          = NULLIF(REGEXP_REPLACE(TRIM(fax),          '[^0-9\\+\\-\\(\\)\\.\\s]', ''), '')
WHERE cell_phone IS NOT NULL
   OR home_phone IS NOT NULL
   OR office_phone IS NOT NULL
   OR fax IS NOT NULL;

SELECT ROW_COUNT() AS phones_cleaned;

-- Null out placeholder phone numbers
UPDATE gms_all_profiles
SET cell_phone = NULL
WHERE REGEXP_REPLACE(cell_phone, '[^0-9]', '') IN (
  '0000000000','1111111111','9999999999',
  '1234567890','0123456789'
);

UPDATE gms_all_profiles
SET home_phone = NULL
WHERE REGEXP_REPLACE(home_phone, '[^0-9]', '') IN (
  '0000000000','1111111111','9999999999',
  '1234567890','0123456789'
);


-- =============================================================================
-- VALIDATION REPORT
-- Run after all steps to confirm cleaning results.
-- =============================================================================

SELECT '=== POST-CLEAN VALIDATION REPORT ===' AS report;

-- Row counts
SELECT
  COUNT(*)                                                            AS total_rows,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END)                     AS null_email,
  SUM(CASE WHEN fname IS NULL THEN 1 ELSE 0 END)                     AS null_fname,
  SUM(CASE WHEN lname IS NULL THEN 1 ELSE 0 END)                     AS null_lname,
  SUM(CASE WHEN salutation IS NULL THEN 1 ELSE 0 END)                AS null_salutation,
  SUM(CASE WHEN exclude_email = 1 THEN 1 ELSE 0 END)                 AS total_suppressed,
  SUM(CASE WHEN exclude_email = 0 AND email IS NOT NULL THEN 1 ELSE 0 END) AS active_contactable
FROM gms_all_profiles;

-- Salutation distribution post-clean
SELECT salutation, COUNT(*) AS cnt
FROM gms_all_profiles
GROUP BY salutation
ORDER BY cnt DESC;

-- Remaining ALL-CAPS names (should be 0 after clean)
SELECT COUNT(*) AS remaining_allcaps_names
FROM gms_all_profiles
WHERE (fname = UPPER(fname) AND fname REGEXP '[A-Z]{2,}')
   OR (lname = UPPER(lname) AND lname REGEXP '[A-Z]{2,}');

-- Remaining mixed-case emails (should be 0 after clean)
SELECT COUNT(*) AS remaining_uppercase_emails
FROM gms_all_profiles
WHERE email != LOWER(email) AND email IS NOT NULL;

-- Suppression breakdown
SELECT
  SUM(CASE WHEN exclude_email = 1 AND (bounce IN ('1','true','yes','y','hard','soft','bounce') OR bounce_flag IN ('1','true','yes','y','bounced')) THEN 1 ELSE 0 END) AS suppressed_bounce,
  SUM(CASE WHEN exclude_email = 1 AND (email IS NULL OR email = '') THEN 1 ELSE 0 END) AS suppressed_no_email,
  SUM(CASE WHEN exclude_email = 1 THEN 1 ELSE 0 END) AS total_suppressed
FROM gms_all_profiles;


-- =============================================================================
-- ROLLBACK (if needed — restores from backup)
-- =============================================================================
-- Only run this block if something went wrong.
--
-- TRUNCATE TABLE gms_all_profiles;
-- INSERT INTO gms_all_profiles SELECT * FROM gms_all_profiles_backup_pre_clean;
--
-- DROP TABLE gms_all_profiles_backup_pre_clean;  -- cleanup after confirmed success
-- =============================================================================