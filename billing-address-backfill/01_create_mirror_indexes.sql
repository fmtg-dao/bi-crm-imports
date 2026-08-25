-- Indexes on the crm_*_sfid_prod mirrors for the billing address backfill.
-- A mirror refresh replaces the tables and drops these, so re-run this file
-- after every refresh_sf_mirrors.py run. A duplicate-key error on re-run is
-- fine, it means the index is still there.
-- Same as invest-consent/01_create_mirror_indexes.sql; only idx_acc_id is
-- strictly needed here, the rest keep the shared mirrors queryable.

CREATE INDEX idx_acc_id
    ON crm_person_account_sfid_prod (Id);

CREATE INDEX idx_acc_person_contact_id
    ON crm_person_account_sfid_prod (PersonContactId);

CREATE INDEX idx_cpe_party
    ON crm_cp_email_sfid_prod (PartyID__c);

CREATE INDEX idx_consent_account
    ON crm_consent_sfid_prod (AccountId);

CREATE INDEX idx_consent_name
    ON crm_consent_sfid_prod (Name);
