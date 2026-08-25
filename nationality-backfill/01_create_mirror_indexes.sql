-- Indexes on the crm_*_sfid_prod mirrors for the nationality backfill.
-- A mirror refresh replaces the tables and drops these, so re-run this file
-- after every refresh_sf_mirrors.py run. A duplicate-key error on re-run is
-- fine, it means the index is still there.
-- Same base file as billing-address-backfill/01_create_mirror_indexes.sql;
-- idx_acc_id and idx_acc_extid are the ones this backfill actually needs
-- (the staging join matches ExternalID__pc against the stg_imp_investors_*
-- tables), the rest keep the mirrors queryable.

CREATE INDEX idx_acc_id
    ON crm_person_account_sfid_prod (Id);

CREATE INDEX idx_acc_person_contact_id
    ON crm_person_account_sfid_prod (PersonContactId);

CREATE INDEX idx_acc_extid
    ON crm_person_account_sfid_prod (ExternalID__pc);

CREATE INDEX idx_cpe_party
    ON crm_cp_email_sfid_prod (PartyID__c);

CREATE INDEX idx_consent_account
    ON crm_consent_sfid_prod (AccountId);

CREATE INDEX idx_consent_name
    ON crm_consent_sfid_prod (Name);
