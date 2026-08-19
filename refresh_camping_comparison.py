"""Refresh SF prod objects needed for the camping overlap comparison.

Downloads Account, ContactPointEmail, ContactPointConsent and Lead, builds the
merged Consent table, and mirrors each into MySQL (crm_*_sfid_prod tables).

Lead is included because the camping insert batch guards against creating a
Person Account for an email that already exists as a Lead — the same third-table
NOT EXISTS pattern the investor import used against the loyalty legacy table.
"""

from sf_objects_download import (
    ACCOUNT_PATH, ACCOUNT_QUERY, ACCOUNT_TABLE,
    CP_CONSENT_PATH, CP_CONSENT_QUERY, CP_CONSENT_TABLE,
    CP_EMAIL_PATH, CP_EMAIL_QUERY, CP_EMAIL_TABLE,
    LEAD_PATH, LEAD_QUERY, LEAD_TABLE,
    create_consent_df, save_object_in_mysqL, sf_query,
)


def refresh(query: str, path: str, table: str, label: str) -> None:
    df = sf_query(query)
    df.to_parquet(path, index=False)
    save_object_in_mysqL(path, table)
    print(f"{label} done: {len(df):,} rows -> {table}")


if __name__ == "__main__":
    refresh(ACCOUNT_QUERY, ACCOUNT_PATH, ACCOUNT_TABLE, "Account")
    refresh(CP_EMAIL_QUERY, CP_EMAIL_PATH, CP_EMAIL_TABLE, "CPE")
    refresh(CP_CONSENT_QUERY, CP_CONSENT_PATH, CP_CONSENT_TABLE, "CPC")
    refresh(LEAD_QUERY, LEAD_PATH, LEAD_TABLE, "Lead")

    df_consent = create_consent_df()
    df_consent.to_parquet("local_data/sf_object_data/consent_prod.parquet", index=False)
    save_object_in_mysqL("local_data/sf_object_data/consent_prod.parquet", "crm_consent_sfid_prod")
    print(f"Consent done: {len(df_consent):,} rows -> crm_consent_sfid_prod")
