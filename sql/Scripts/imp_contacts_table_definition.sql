DROP TABLE IF EXISTS crm_imp_person_accounts;


select * from  mig_raw_crm_contacts

CREATE TABLE crm_imp_person_accounts (
    /* ================================================================ */
    /* PIPELINE CONTROL FIELDS                                          */
    /* ================================================================ */
    row_id                          INT UNSIGNED AUTO_INCREMENT,
    cluster_id                      VARCHAR(40),            -- Matching/Dedup-Cluster, mappt auf ClusterID__c (External ID)
    _excluded                       TINYINT(1) DEFAULT 0,   -- 1 = vom Import ausschliessen
    _exclude_reason                 VARCHAR(250),           -- Erklärung warum
    _operation                      VARCHAR(20) NOT NULL,   -- 'insert' | 'upsert' | 'update'
    _batch_id                       VARCHAR(50),            -- z.B. 'protel_2026-05-28_initial'
    _processed_at                   DATETIME,               -- gefüllt nach erfolgreichem SF-Push

    /* ================================================================ */
    /* SALESFORCE IDs (gefüllt nach Import / für Updates & Matching)    */
    /* ================================================================ */
    sf_account_id                   VARCHAR(18),            -- Person Account ID
    sf_person_contact_id            VARCHAR(18),            -- PersonContactId (auto-created)
    sf_loyalty_member_id            VARCHAR(18),            -- LoyaltyProgramMember ID
    sf_cp_email_id                  VARCHAR(18),            -- ContactPointEmail ID

    /* ================================================================ */
    /* SOURCE TRACKING                                                  */
    /* ================================================================ */
    source                          VARCHAR(50) NOT NULL,   -- 'protel' | 'apaleo' | 'manual' | 'loyalty_xls' | ...
    source_origin                   VARCHAR(255),           -- → SourceOrigin__pc (z.B. Excel-Dateiname)

    /* ================================================================ */
    /* EXTERNAL IDS (für Upsert & Matching)                             */
    /* ================================================================ */
    external_id                     VARCHAR(40),            -- → ExternalID__c (B2C Contact External ID)
    entra_external_id               VARCHAR(255),           -- → EntraExternalID__c (Azure AD / SSO)

    /* ================================================================ */
    /* PROFILE: IDENTITY                                                */
    /* ================================================================ */
    salutation                      VARCHAR(40),            -- → Salutation (Picklist: Mr., Mrs., ...)
    first_name                      VARCHAR(40),            -- → FirstName
    middle_name                     VARCHAR(40),            -- → MiddleName
    last_name                       VARCHAR(80) NOT NULL,   -- → LastName (Pflicht in SF!)

    birth_date                      DATE,                   -- → PersonBirthdate
    birth_place                     VARCHAR(255),           -- → BirthPlace__pc
    gender                          VARCHAR(50),            -- → PersonGenderIdentity (Picklist)

    /* ================================================================ */
    /* PROFILE: COMMUNICATION                                           */
    /* ================================================================ */
    email                           VARCHAR(255),           -- → PersonEmail
    phone                           VARCHAR(50),            -- → Phone

    preferred_language              VARCHAR(10),            -- → PreferredLanguage__pc (ISO Code, z.B. 'de', 'en')
    nationality_country_code        VARCHAR(10),            -- → NationalityCountryCode__pc (ISO-2)

    /* ================================================================ */
    /* PROFILE: ADDRESS (Mailing)                                       */
    /* ================================================================ */
    address                         VARCHAR(255),           -- → PersonMailingStreet
    postal_code                     VARCHAR(20),            -- → PersonMailingPostalCode
    city                            VARCHAR(40),            -- → PersonMailingCity
    state                           VARCHAR(80),            -- → PersonMailingState
    country                         VARCHAR(80),            -- → PersonMailingCountry (ISO-2)

    /* ================================================================ */
    /* BUSINESS UNIT FLAGS (CRMIM-192)                                  */
    /* ================================================================ */
    hotel_customer                  TINYINT(1) DEFAULT 0,   -- → HotelCustomer__pc
    camping_customer                TINYINT(1) DEFAULT 0,   -- → CampingCustomer__pc
    residences_customer             TINYINT(1) DEFAULT 0,   -- → ResidencesCustomer__pc
    invest_customer                 TINYINT(1) DEFAULT 0,   -- → InvestCustomer__pc

    primary_property_id             VARCHAR(18),            -- → PrimaryProperty__pc (SF ID)

    /* ================================================================ */
    /* LOYALTY                                                          */
    /* ================================================================ */
    loyalty_program                 VARCHAR(100),           -- z.B. 'FMTG_REWARDS'
    loyalty_membership_number       VARCHAR(100),
    loyalty_legacy_tier             VARCHAR(50),
    loyalty_legacy_number           VARCHAR(250),
    loyalty_points_balance          INT,
    loyalty_enrollment_date         DATE,

    /* ================================================================ */
    /* INVESTMENT (B2C Investment-Profil)                               */
    /* ================================================================ */
    investment_status               VARCHAR(50),            -- → InvestmentStatus__pc
    investment_expiration_date      DATE,                   -- → InvestmentExpirationDate__pc

    /* ================================================================ */
    /* CONSENT                                                          */
    /* ================================================================ */
    consent_central                 TINYINT(1) DEFAULT 0,   -- Hotel-Consent (Central)
    consent_camping                 TINYINT(1) DEFAULT 0,
    consent_residences              TINYINT(1) DEFAULT 0,
    consent_invest                  TINYINT(1) DEFAULT 0,

    /* ================================================================ */
    /* AUDIT                                                            */
    /* ================================================================ */
    created_at                      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at                      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    /* ================================================================ */
    /* KEYS & INDEXES                                                   */
    /* ================================================================ */
    PRIMARY KEY (row_id),
    UNIQUE KEY uk_source_external_id    (source, external_id),

    INDEX idx_operation             (_operation),
    INDEX idx_excluded              (_excluded),
    INDEX idx_batch                 (_batch_id),
    INDEX idx_processed             (_processed_at),
    INDEX idx_source                (source),
    INDEX idx_cluster               (cluster_id),
    INDEX idx_external_id           (external_id),
    INDEX idx_email                 (email),
    INDEX idx_sf_account            (sf_account_id),
    INDEX idx_loyalty_membership    (loyalty_membership_number)
);




DROP TABLE IF EXISTS crm_imp_person_accounts_history;

CREATE TABLE crm_imp_person_accounts_history (
    history_id                      BIGINT UNSIGNED AUTO_INCREMENT,
    row_id                          INT UNSIGNED NOT NULL,  -- FK auf crm_imp_person_accounts.row_id
    version_at                      DATETIME DEFAULT CURRENT_TIMESTAMP,
    change_type                     VARCHAR(20),            -- 'insert' | 'update' | 'exclude' | 'sf_push'
    change_source                   VARCHAR(100),           -- z.B. Skript-/User-Name

    /* ---- Snapshot aller fachlichen Felder zum Zeitpunkt der Änderung ---- */
    cluster_id                      VARCHAR(40),
    _excluded                       TINYINT(1),
    _exclude_reason                 VARCHAR(250),
    _operation                      VARCHAR(20),
    _batch_id                       VARCHAR(50),
    _processed_at                   DATETIME,

    sf_account_id                   VARCHAR(18),
    sf_person_contact_id            VARCHAR(18),
    sf_loyalty_member_id            VARCHAR(18),
    sf_cp_email_id                  VARCHAR(18),

    source                          VARCHAR(50),
    source_origin                   VARCHAR(255),

    external_id                     VARCHAR(40),
    entra_external_id               VARCHAR(255),

    salutation                      VARCHAR(40),
    first_name                      VARCHAR(40),
    middle_name                     VARCHAR(40),
    last_name                       VARCHAR(80),
    birth_date                      DATE,
    birth_place                     VARCHAR(255),
    gender                          VARCHAR(50),

    email                           VARCHAR(255),
    phone                           VARCHAR(50),
    preferred_language              VARCHAR(10),
    nationality_country_code        VARCHAR(10),

    address                         VARCHAR(255),
    postal_code                     VARCHAR(20),
    city                            VARCHAR(40),
    state                           VARCHAR(80),
    country                         VARCHAR(80),

    hotel_customer                  TINYINT(1),
    camping_customer                TINYINT(1),
    residences_customer             TINYINT(1),
    invest_customer                 TINYINT(1),
    primary_property_id             VARCHAR(18),

    loyalty_program                 VARCHAR(100),
    loyalty_membership_number       VARCHAR(100),
    loyalty_legacy_tier             VARCHAR(50),
    loyalty_legacy_number           VARCHAR(250),
    loyalty_points_balance          INT,
    loyalty_enrollment_date         DATE,

    investment_status               VARCHAR(50),
    investment_expiration_date      DATE,

    consent_central                 TINYINT(1),
    consent_camping                 TINYINT(1),
    consent_residences              TINYINT(1),
    consent_invest                  TINYINT(1),

    PRIMARY KEY (history_id),
    INDEX idx_hist_row_id           (row_id),
    INDEX idx_hist_version_at       (version_at),
    INDEX idx_hist_change_type      (change_type)
);





DROP PROCEDURE IF EXISTS sp_archive_crm_imp_person_accounts;

/*
-- Nach erfolgreichem SF-Import:
CALL sp_archive_crm_imp_person_accounts(
    'protel_2026-05-28_initial',
    'import_persons_bulk.py'
);
*/

CREATE PROCEDURE sp_archive_crm_imp_person_accounts(
    IN p_batch_id       VARCHAR(50),    -- z.B. 'protel_2026-05-28_initial'
    IN p_change_source  VARCHAR(100)    -- z.B. 'import_persons_bulk.py' oder 'oleg.danilov'
)
BEGIN
    DECLARE v_rows_archived INT DEFAULT 0;
    DECLARE v_rows_deleted  INT DEFAULT 0;

    /* ------------------------------------------------------------------ */
    /* Safety: batch_id muss gesetzt sein                                  */
    /* ------------------------------------------------------------------ */
    IF p_batch_id IS NULL OR p_batch_id = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'sp_archive_crm_imp_person_accounts: p_batch_id darf nicht leer sein';
    END IF;

    /* ------------------------------------------------------------------ */
    /* Transaktional: erst kopieren, dann löschen                          */
    /* ------------------------------------------------------------------ */
    START TRANSACTION;

    /* ---- 1. Snapshot in History übertragen ---- */
    INSERT INTO crm_imp_person_accounts_history (
        row_id, version_at, change_type, change_source,
        cluster_id, _excluded, _exclude_reason, _operation, _batch_id, _processed_at,
        sf_account_id, sf_person_contact_id, sf_loyalty_member_id, sf_cp_email_id,
        source, source_origin,
        external_id, entra_external_id,
        salutation, first_name, middle_name, last_name,
        birth_date, birth_place, gender,
        email, phone, preferred_language, nationality_country_code,
        address, postal_code, city, state, country,
        hotel_customer, camping_customer, residences_customer, invest_customer,
        primary_property_id,
        loyalty_program, loyalty_membership_number,
        loyalty_legacy_tier, loyalty_legacy_number,
        loyalty_points_balance, loyalty_enrollment_date,
        investment_status, investment_expiration_date,
        consent_central, consent_camping, consent_residences, consent_invest
    )
    SELECT
        row_id, NOW(), 'sf_push', p_change_source,
        cluster_id, _excluded, _exclude_reason, _operation, _batch_id, _processed_at,
        sf_account_id, sf_person_contact_id, sf_loyalty_member_id, sf_cp_email_id,
        source, source_origin,
        external_id, entra_external_id,
        salutation, first_name, middle_name, last_name,
        birth_date, birth_place, gender,
        email, phone, preferred_language, nationality_country_code,
        address, postal_code, city, state, country,
        hotel_customer, camping_customer, residences_customer, invest_customer,
        primary_property_id,
        loyalty_program, loyalty_membership_number,
        loyalty_legacy_tier, loyalty_legacy_number,
        loyalty_points_balance, loyalty_enrollment_date,
        investment_status, investment_expiration_date,
        consent_central, consent_camping, consent_residences, consent_invest
    FROM crm_imp_person_accounts
    WHERE _batch_id = p_batch_id;

    SET v_rows_archived = ROW_COUNT();

    /* ---- 2. Quelltabelle für diesen Batch leeren ---- */
    DELETE FROM crm_imp_person_accounts
    WHERE _batch_id = p_batch_id;

    SET v_rows_deleted = ROW_COUNT();

    /* ---- 3. Sicherheitscheck: gleiche Zahl muss raus wie rein ---- */
    IF v_rows_archived <> v_rows_deleted THEN
        ROLLBACK;
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Anzahl archivierter vs. gelöschter Zeilen weicht ab — Rollback';
    END IF;

    COMMIT;

    /* ---- 4. Ergebnis zurückgeben ---- */
    SELECT
        p_batch_id          AS batch_id,
        v_rows_archived     AS rows_archived,
        v_rows_deleted      AS rows_deleted,
        NOW()               AS finished_at;

END