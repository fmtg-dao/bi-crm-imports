-- Indexes on the crm_*_sfid_prod mirrors for the invest pre-registration import.
-- A mirror refresh replaces the tables and drops these, so re-run this file
-- after every refresh_sf_mirrors.py run. A duplicate-key error on re-run is
-- fine, it means the index is still there.
-- Superset of mailing-address-backfill/01_create_mirror_indexes.sql: adds
-- PersonEmail + ExternalID__pc (the match map in 03 scans 1.5M accounts) and
-- the CPE/consent join columns for the recon and the backfill.

CREATE INDEX idx_acc_id
    ON crm_person_account_sfid_prod (Id);

CREATE INDEX idx_acc_person_contact_id
    ON crm_person_account_sfid_prod (PersonContactId);

CREATE INDEX idx_acc_person_email
    ON crm_person_account_sfid_prod (PersonEmail);

CREATE INDEX idx_acc_external_id
    ON crm_person_account_sfid_prod (ExternalID__pc);

CREATE INDEX idx_cpe_party
    ON crm_cp_email_sfid_prod (PartyID__c);

CREATE INDEX idx_cpe_party_email
    ON crm_cp_email_sfid_prod (PartyID__c, EmailAddress);

CREATE INDEX idx_cpc_contact_point
    ON crm_cp_consent_sfid_prod (ContactPointId);

CREATE INDEX idx_consent_account
    ON crm_consent_sfid_prod (AccountId);

CREATE INDEX idx_consent_name
    ON crm_consent_sfid_prod (Name);
