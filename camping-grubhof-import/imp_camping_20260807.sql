/*
   imp_camping_20260807 - Camping Grubhof: Analyse + Batch-Aufbau

   Quelle:   2607_Data cleaned - Grubhof.xlsx, Sheet 2 (Sheet 1 ist kaputt - Excel-
             Transformationen, siehe AGENTS.md)
   Cleaning: data-quality-validation/person_account_export.ipynb (polars + pandera)
             -> local_data/csv/20260807_camping_grubhof_cleaned.csv (146.996 x 43)
   Staging:  stg_imp_camping_grubhof_20260807 (alle Spalten VARCHAR, leer = '' nicht NULL)
   Mirrors:  crm_*_sfid_prod, alle vom 2026-08-10 (ein Snapshot, inkl. Lead),
             via refresh_sf_mirrors.py (damals refresh_camping_comparison.py)
   Kontakt:  Carmen Marti (carmen.marti@falkensteiner.com)

   Entscheidungen (im Notebook kodiert):
   - Option 4 upload scope: first_name + last_name + valide email pflicht,
     birth_date optional, consent_camping pflicht -> import_ready = 96.933
   - kaputte telefonnummern geleert statt zeile verworfen
   - emails lowercased (haus-standard, gms_all_profiles_cleaning STEP 2)
   - dedup auf first_name/last_name/email/birth_date (334 zeilen entfernt)
   - flags data_issue / contract_valid / import_ready als '1'/'0'
     (data_issue = 113 nicht-personen: firmen, vereine, familien, platzhalter)

   Eigenheiten:
   - staging: fehlende werte sind '' nicht NULL -> col <> '' verwenden
   - collation case-insensitive, email im file bereits lowercase
   - external_id (G-nummern) existiert NICHT in SF -> email ist der EINZIGE join key
   - indizes verschwinden bei jedem reload der staging/mirror tabellen

   Basis-definitionen:
   - alle:  146.996 zeilen
   - valid: contract_valid = '1' (145.408)
   - ready: import_ready = '1' (96.933) - alle batch-zahlen auf dieser basis
*/


/* prepare table */

select * from stg_imp_camping_grubhof_20260807

-- nach jedem reload neu anlegen (duplicate-key fehler beim re-run ist ok):

CREATE INDEX idx_camping_email
    ON stg_imp_camping_grubhof_20260807 (email);

CREATE INDEX idx_person_email
    ON crm_person_account_sfid_prod (PersonEmail);

CREATE INDEX idx_person_external_id
    ON crm_person_account_sfid_prod (ExternalID__pc);

CREATE INDEX idx_cp_email_address
    ON crm_cp_email_sfid_prod (EmailAddress);

-- ohne diesen index laufen die consent-abfragen ~106s statt <1s
CREATE INDEX idx_consent_email
    ON crm_consent_sfid_prod (EmailAddress);

CREATE INDEX idx_lead_email
    ON crm_person_lead_sfid_prod (Email);


/* profil import file */

-- 146996 total / 113 data_issue / 145408 contract_valid / 96933 import_ready
-- 104941 mit email / 103733 distinct
-- consent_camping: 126341 J / 107 N / 20548 leer
select count(*)                                            as total,
       sum(data_issue = '1')                               as data_issue,
       sum(contract_valid = '1')                           as contract_valid,
       sum(import_ready = '1')                             as import_ready,
       sum(email <> '')                                    as mit_email,
       count(distinct case when email <> '' then email end) as distinct_emails,
       sum(consent_camping = '1')                          as consent_j,
       sum(consent_camping = '0')                          as consent_n,
       sum(consent_camping = '')                           as consent_leer
from stg_imp_camping_grubhof_20260807


/* profil import_ready */

-- vollstaendigkeit: 43163 geburtsdatum / 60342 telefon / 82728 adresse
select sum(birth_date <> '')  as mit_geburtsdatum,
       sum(phone <> '')       as mit_telefon,
       sum(address <> '')     as mit_adresse
from stg_imp_camping_grubhof_20260807
where import_ready = '1'

-- laender: DE 60868 / NL 17668 / AT 6467 / GB 3231 / BE 1756 / CZ 1511
select country, count(*)
from stg_imp_camping_grubhof_20260807
where import_ready = '1'
group by country order by 2 desc

-- sprachen: de 70247 / nl 18978 / en 4718 / cs 928 / da 604 / it 368 ...
-- (nl existiert als PreferredLanguage__pc in SF mit 1222 accounts, cz->cs im
--  notebook gemappt = dominante SF-schreibweise -> keine picklist-konflikte erwartet)
select preferred_language, count(*)
from stg_imp_camping_grubhof_20260807
where import_ready = '1'
group by preferred_language order by 2 desc


/* external_id ist KEIN join key */

-- 0 -> matching ausschliesslich ueber email
select count(*)
from stg_imp_camping_grubhof_20260807 c
inner join crm_person_account_sfid_prod a
	on a.ExternalID__pc = c.external_id
where c.external_id <> ''


/* duplikate im import file */

-- personen-duplikate (4 felder): 0 - per notebook-dedup entfernt (334 zeilen)
select first_name, last_name, email, birth_date, count(*)
from stg_imp_camping_grubhof_20260807
group by first_name, last_name, email, birth_date
having count(*) > 1

-- email-duplikate auf ready basis: 1077 gruppen / 2187 zeilen
-- (96933 ready zeilen -> nur 95823 distinct emails, 1110 zeilen ueberschuss)
-- ueberwiegend haushalts-emails: verschiedene personen, gleiche adresse.
-- OFFENE ENTSCHEIDUNG (Carmen): ein account pro person oder pro email?
-- bis zur entscheidung bleiben diese zeilen KOMPLETT aus dem insert batch
-- (siehe guard im INSERT unten), nichts wird automatisch zusammengefasst.
select email, count(*),
       GROUP_CONCAT(distinct first_name order by first_name separator ' | ') as vornamen
from stg_imp_camping_grubhof_20260807
where import_ready = '1'
group by email having count(*) > 1


/* duplikate in SF */

-- 7 ready-emails treffen mehr als einen account -> aus update batch raus,
-- detail fuer Carmen:
select c.email, count(distinct a.Id) as accounts,
       GROUP_CONCAT(a.Id order by a.Id separator ' | ')                as account_ids,
       GROUP_CONCAT(distinct a.SourceSystem__pc separator ' | ')       as quellsysteme,
       GROUP_CONCAT(distinct a.FirstName separator ' | ')              as sf_vornamen
from stg_imp_camping_grubhof_20260807 c
inner join crm_person_account_sfid_prod a
	on a.PersonEmail = c.email
where c.import_ready = '1'
group by c.email having count(distinct a.Id) > 1


/* email overlap mit SF */

-- 4720 ready zeilen / 4651 distinct emails treffen bestehende accounts
select count(*) as zeilen, count(distinct c.email) as emails
from stg_imp_camping_grubhof_20260807 c
where c.import_ready = '1'
and exists (select 1 from crm_person_account_sfid_prod a where a.PersonEmail = c.email)

-- nach quellsystem (pairs): apaleo 2707 (1908 bereits camping) / gms 1139 (43) /
-- protel 867 (125) / conda 10 (3) / null 5
-- -> overlap = ueberwiegend hotelgaeste die auch campen
select a.SourceSystem__pc, count(*) as pairs,
       sum(a.CampingCustomer__pc in (1,'true','True')) as bereits_camping
from stg_imp_camping_grubhof_20260807 c
inner join crm_person_account_sfid_prod a on a.PersonEmail = c.email
where c.import_ready = '1'
group by a.SourceSystem__pc order by pairs desc


/* namens-abgleich (ready overlap) */

-- 3733 vorname gleich / 307 SF-vorname NULL / 688 abweichend
-- abweichende = grossteils haushalts-emails (partner bucht unter gleicher adresse)
select sum(c.first_name = a.FirstName)                          as name_gleich,
       sum(a.FirstName is null)                                 as sf_name_null,
       sum(c.first_name <> a.FirstName and a.FirstName is not null) as name_abweichend
from stg_imp_camping_grubhof_20260807 c
inner join crm_person_account_sfid_prod a on a.PersonEmail = c.email
where c.import_ready = '1'

-- review-liste fuer Carmen (688):
select c.first_name as camping_first_name,
		a.FirstName as sf_first_name,
		c.last_name as camping_last_name,
		a.LastName as sf_last_name,
		c.email,
		a.SourceSystem__pc,
		a.CampingCustomer__pc,
		a.Id as account_id
from stg_imp_camping_grubhof_20260807 c
inner join crm_person_account_sfid_prod a
	on a.PersonEmail = c.email
where c.import_ready = '1'
and c.first_name <> a.FirstName
and a.FirstName is not null


/* geburtsdatums-abgleich (update-kandidaten) */

-- 341 beide vorhanden: 312 gleich / 29 abweichend
-- 2068 nur camping (potentieller gewinn) / 195 nur SF
-- ACHTUNG: der minimale update (nur CampingCustomer__pc) fasst geburtsdaten
-- NICHT an - die 2068 gewinne waeren ein eigener, spaeterer batch
select sum(c.birth_date <> '' and a.PersonBirthdate is not null)  as beide,
       sum(DATE(a.PersonBirthdate) = c.birth_date)                as gleich,
       sum(c.birth_date <> '' and a.PersonBirthdate is not null
           and DATE(a.PersonBirthdate) <> c.birth_date)           as abweichend,
       sum(c.birth_date <> '' and a.PersonBirthdate is null)      as nur_camping,
       sum(c.birth_date = ''  and a.PersonBirthdate is not null)  as nur_sf
from stg_imp_camping_grubhof_20260807 c
inner join crm_person_account_sfid_prod a on a.PersonEmail = c.email
where c.import_ready = '1'
and (c.first_name = a.FirstName or a.FirstName is null)


/* verdeckte kollisionen: leads */

-- 273 "neue" emails existieren bereits als Lead. die frueher separat gezaehlten
-- CPE-kollisionen sind DIESELBEN 273 emails (leads haengen an Individuals, die
-- ihre eigenen ContactPointEmails haben) -> EIN guard, nicht zwei.
-- Lead-mirror muss frisch sein: crm_person_lead_sfid_prod vom 2026-08-10.
select count(distinct c.email)
from stg_imp_camping_grubhof_20260807 c
where c.import_ready = '1'
and not exists (select 1 from crm_person_account_sfid_prod a where a.PersonEmail = c.email)
and exists (select 1 from crm_person_lead_sfid_prod l where l.Email = c.email)


/* consent-abgleich */

-- ready overlap (4651 emails): 4374 mit consent-record / 3892 mit OptIn / 516 mit OptOut
select count(*)            as overlap_emails,
       sum(cs.has_consent) as mit_consent,
       sum(cs.has_optin)   as mit_optin,
       sum(cs.has_optout)  as mit_optout
from (
  select c.email,
         max(cc.EmailAddress is not null)              as has_consent,
         max(cc.PrivacyConsentStatus = 'OptIn')        as has_optin,
         max(cc.PrivacyConsentStatus = 'OptOut')       as has_optout
  from (select distinct email from stg_imp_camping_grubhof_20260807 s
        where s.import_ready = '1'
        and exists (select 1 from crm_person_account_sfid_prod a where a.PersonEmail = s.email)) c
  left join crm_consent_sfid_prod cc on cc.EmailAddress = c.email
  group by c.email
) cs


/* opt-out guard */

-- 533 ready-emails haben irgendeinen OptOut in SF:
--   289 marketing_central / 485 Marketing_Property (ueberschneidung moeglich)
-- OFFENE ENTSCHEIDUNG: blockiert ein property-OptOut den camping consent,
-- oder nur ein zentraler? camping sagt bei diesen leuten "J" - wir duerfen
-- einen bestehenden OptOut NICHT mit einem neueren OptIn ueberschreiben,
-- solange das nicht geklaert ist. betrifft NUR den consent-schritt (schritt 4),
-- nicht den account-insert.
select count(distinct c.email)
from stg_imp_camping_grubhof_20260807 c
where c.import_ready = '1'
and exists (select 1 from crm_consent_sfid_prod cc
            where cc.EmailAddress = c.email and cc.PrivacyConsentStatus = 'OptOut')

-- aufschluesselung nach purpose:
select cc.Name, count(distinct c.email) as emails
from (select distinct email from stg_imp_camping_grubhof_20260807 where import_ready = '1') c
inner join crm_consent_sfid_prod cc
	on cc.EmailAddress = c.email and cc.PrivacyConsentStatus = 'OptOut'
group by cc.Name order by emails desc


/* funnel auf import_ready basis */

-- 96933 ready = 4720 overlap + 92213 neu
-- neu zerlegt: 89898 sauber (insert batch)
--            +  2042 haushalts-email gruppen (1007 gruppen, geparkt bis entscheidung)
--            +   273 lead-kollisionen (geparkt, manuelle pruefung / lead-konversion)
select sum(exists (select 1 from crm_person_account_sfid_prod a
                   where a.PersonEmail = c.email))     as overlap,
       sum(not exists (select 1 from crm_person_account_sfid_prod a
                       where a.PersonEmail = c.email)) as neu
from stg_imp_camping_grubhof_20260807 c
where c.import_ready = '1'


/*  insert into final import table - batch 1: neue accounts */

-- WICHTIG: eigener batch_id fuer insert vs. update - insert_person_accounts_bulk.py
-- filtert NICHT auf _operation und wuerde update-zeilen als neue accounts anlegen.

-- erwartet: 89898 zeilen
-- guards: (1) email nicht in SF accounts, (2) email nicht als lead,
--         (3) email eindeutig im ready-set (haushalts-gruppen komplett geparkt)

INSERT INTO crm_imp_person_accounts (
    /* Pipeline Control */
    _operation,
    _batch_id,

    /* SF IDs */
    sf_account_id,
    sf_person_contact_id,
    sf_loyalty_member_id,
    sf_cp_email_id,

    /* Source Tracking */
    source,
    source_origin,

    /* External IDs */
    external_id,

    /* Profile: Identity */
    salutation,
    first_name,
    last_name,
    birth_date,
    gender,

    /* Profile: Communication */
    email,
    phone,
    preferred_language,

    /* Address */
    address,
    postal_code,
    city,
    country,

    /* Business Unit Flags */
    hotel_customer,
    camping_customer,
    residences_customer,
    invest_customer,

    /* Consent */
    consent_central,
    consent_camping,
    consent_residences,
    consent_invest
)
SELECT
    'insert'                                            AS _operation,
    '2026-08-10_new_camping_import'                     AS _batch_id,

    null                                                AS sf_account_id,
    null                                                AS sf_person_contact_id,
    null                                                AS sf_loyalty_member_id,
    null                                                AS sf_cp_email_id,

    cmp.source                                          AS source,
    cmp.source_origin                                   AS source_origin,

    cmp.external_id                                     AS external_id,

    NULLIF(cmp.salutation, '')                          AS salutation,
    cmp.first_name                                      AS first_name,
    cmp.last_name                                       AS last_name,
    NULLIF(cmp.birth_date, '')                          AS birth_date,
    NULLIF(cmp.gender, '')                              AS gender,

    cmp.email                                           AS email,
    NULLIF(cmp.phone, '')                               AS phone,
    NULLIF(cmp.preferred_language, '')                  AS preferred_language,

    NULLIF(cmp.address, '')                             AS address,
    NULLIF(cmp.postal_code, '')                         AS postal_code,
    NULLIF(cmp.city, '')                                AS city,
    NULLIF(cmp.country, '')                             AS country,

    0                                                   AS hotel_customer,
    1                                                   AS camping_customer,
    0                                                   AS residences_customer,
    0                                                   AS invest_customer,

    0                                                   AS consent_central,
    1                                                   AS consent_camping,
    0                                                   AS consent_residences,
    0                                                   AS consent_invest
-- select count(*)
FROM stg_imp_camping_grubhof_20260807 cmp
WHERE cmp.import_ready = '1'
  AND NOT EXISTS (
      SELECT 1
      FROM crm_person_account_sfid_prod acc
      WHERE acc.PersonEmail = cmp.email
  )
  AND NOT EXISTS (
      SELECT 1
      FROM crm_person_lead_sfid_prod l
      WHERE l.Email = cmp.email
  )
  AND NOT EXISTS (
      SELECT 1
      FROM stg_imp_camping_grubhof_20260807 dup
      WHERE dup.email = cmp.email
        AND dup.import_ready = '1'
        AND dup.external_id <> cmp.external_id
  );

-- kontrolle: 89898
select count(*) from crm_imp_person_accounts
where _batch_id = '2026-08-10_new_camping_import'


/*  insert into final import table - batch 2: camping-flag update */

-- !!! NICHT AUSFUEHREN bevor update_contacts_bulk.py angepasst ist !!!
-- der mapper schreibt aktuell ExternalID__pc, SourceSystem__pc, PersonBirthdate,
-- InvestCustomer__pc (setzt investoren auf False!), InvestmentStatus/-Expiration.
-- fuer camping muss der payload auf Id + CampingCustomer__pc reduziert werden
-- (alle anderen felder im mapper auskommentieren, CampingCustomer__pc rein).
-- 9 der update-kandidaten sind investoren - deshalb ist der minimale payload pflicht.
--
-- wegen tinyint-defaults explizit: NICHT gesetzte flag-spalten landen als 0, und
-- sf_bool(0) = False wird nach SF GESCHRIEBEN (nicht uebersprungen) ->
-- invest_customer etc. hier explizit auf NULL setzen.

-- erwartet: 4034 zeilen (4040 sichere pairs = 3733 name gleich + 307 SF-vorname
-- NULL, minus 6 pairs an den 7 multi-account emails), ~1762 bereits
-- CampingCustomer (idempotent, schadet nicht)
-- ausgeschlossen: 688 namens-abweichungen (review-liste oben), multi-account emails

INSERT INTO crm_imp_person_accounts (
    _operation,
    _batch_id,

    sf_account_id,
    sf_person_contact_id,

    source,
    source_origin,
    external_id,

    first_name,
    last_name,
    email,

    hotel_customer,
    camping_customer,
    residences_customer,
    invest_customer,

    consent_central,
    consent_camping,
    consent_residences,
    consent_invest
)
SELECT
    'update'                                            AS _operation,
    '2026-08-10_camping_flag_update'                    AS _batch_id,

    acc.Id                                              AS sf_account_id,
    acc.PersonContactId                                 AS sf_person_contact_id,

    cmp.source                                          AS source,
    cmp.source_origin                                   AS source_origin,
    cmp.external_id                                     AS external_id,

    cmp.first_name                                      AS first_name,
    cmp.last_name                                       AS last_name,
    cmp.email                                           AS email,

    null                                                AS hotel_customer,
    1                                                   AS camping_customer,
    null                                                AS residences_customer,
    null                                                AS invest_customer,

    0                                                   AS consent_central,
    1                                                   AS consent_camping,
    0                                                   AS consent_residences,
    0                                                   AS consent_invest
-- select count(*)
FROM crm_person_account_sfid_prod acc
INNER JOIN stg_imp_camping_grubhof_20260807 cmp
    ON cmp.email = acc.PersonEmail
WHERE cmp.import_ready = '1'
  AND (cmp.first_name = acc.FirstName OR acc.FirstName IS NULL)
  AND NOT EXISTS (
      SELECT 1
      FROM crm_person_account_sfid_prod acc2
      WHERE acc2.PersonEmail = cmp.email
        AND acc2.Id <> acc.Id
  );

-- kontrolle: 4034
select count(*) from crm_imp_person_accounts
where _batch_id = '2026-08-10_camping_flag_update'


/* naechste schritte */

-- 1. insert_person_accounts_bulk.py: CampingCustomer__pc einkommentieren, dann
--    python insert_person_accounts_bulk.py 2026-08-10_new_camping_import
--    (89898 zeilen = ~180 bulk jobs a 500 - laeuft laenger; bekannte schwaechen:
--     kein _operation filter, MySQL-writeback erst am ende, 'Aborted' jobs werden
--     als erfolg gewertet - bei abbruch NICHT blind neu starten, erst
--     _account_processed_at gegen SF abgleichen)
-- 2. update_contacts_bulk.py auf minimalen payload trimmen, dann
--    python update_contacts_bulk.py 2026-08-10_camping_flag_update
-- 3. loyalty + punkte: entfallen fuer camping
-- 4. consent: purpose ENTSCHIEDEN (arsal.jabbar, 2026-08-10): camping_central
--    (0ZWTe0000000X41OAE). offen bleibt nur noch:
--    b) opt-out regel (533 betroffene emails, s.o.)
--    danach: CPE-mirror refreshen, sf_cp_email_id per EmailAddress-join backfillen
--    (CPE haengt am Individual, NICHT am contact!), insert_consents_bulk.py
--    konstanten setzen (CONSENT_FLAG_COLUMN = 'consent_camping'), laufen lassen
-- 5. geparkte gruppen nach entscheidungen: 2042 haushalts-email zeilen,
--    273 lead-kollisionen, 688 namens-abweichungen, 7 multi-account emails
-- 6. archiv erst wenn batch komplett:
-- CALL sp_archive_crm_imp_person_accounts(
--     '2026-08-10_new_camping_import',
--     'arsal.jabbar')


/* STATUS 2026-08-17 (arsal.jabbar) - accounts + consents GELAUFEN

   Schritt 1 (accounts, 2026-08-10): 89887 von 89898 person accounts angelegt,
   11 fehler (INVALID_EMAIL_ADDRESS, umlaut-local-parts - siehe
   local_data/batch/all_failed_person_accounts_insert_2026-08-10_new_camping_import.json).
   sf_account_id / sf_person_contact_id / sf_cp_email_id backfilled (89887).

   Schritt 4 (consents, 2026-08-17): purpose-entscheidung REVIDIERT:
   NICHT camping_central - Christoph Crepaz (2026-08-11, mail-thread Carmen):
   kein central consent fuer grubhof-uploads, nur PROPERTY consent.
   -> Marketing_Property (0ZWTe0000000X8rOAE) + Property__c a0QTe00000La2dVMAR
      (Camping Grubhof) + HotelName__c 'Camping Grubhof' + Region__c 'Saalachtal'
      + ConsentKey__c = CPE|purpose|property (wie apaleo-migration).
   ConsentKey__c ist NICHT SF-automatisiert - empirisch verifiziert am test-record
   (REST-insert ohne key blieb leer; scripts in temp_consent_test/).
   HotelName__c/Region__c per feld-diff gegen migrations-record 0ZXTe000000017VOAQ
   gefunden (UI-spalte 'Hotel Name' liest HotelName__c, nicht Property__c!).
   Lauf: insert_consents_bulk.py 2026-08-10_new_camping_import
   -> 18/18 jobs JobComplete, 89887 erfolgreich, 0 fehler,
      _consent_processed_at gesetzt, live SF count = 89887. Carmen approved.

   Opt-out frage fuer die 89887 GEGENSTANDSLOS: alle 533 opt-out emails haengen
   an bestehenden accounts/leads (516 accounts, 355 leads) und sind per NOT
   EXISTS guards NICHT im batch. Relevant bleibt sie nur fuer batch 2
   (flag update 4034) und die geparkten gruppen.

   OFFEN: batch 2 (update_contacts_bulk.py mapper trimmen!), 11 email-fehler,
   geparkte gruppen (2042/273/688/7), consent-mirror refresh, archiv.
*/
