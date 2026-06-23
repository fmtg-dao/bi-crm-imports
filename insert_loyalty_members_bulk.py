from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, timezone, time, date
from typing import Optional, Union
import json
import csv
import io
import time as time_module
import logging
from pathlib import Path

# --- Logging Setup ---
log_path = Path("logs/insert_loyalty_members_bulk.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
OBJECT_NAME = "LoyaltyProgramMember"
BATCH_SIZE = 500
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 60

# Steuert, welcher Batch aus crm_imp_person_accounts gezogen wird.
# Wird per CLI-Arg überschreibbar gemacht (siehe main()).
DEFAULT_BATCH_ID = "2026-06-22_new_investor_import"

# --- Loyalty-Konstanten für diese Migration ---
# TODO: SF-Id deines Loyalty Programs vor dem Lauf eintragen
LOYALTY_PROGRAM_ID  = "0lpTe000000004rIAA"   # Loyalty Program in Salesforce
MEMBER_STATUS       = "Active"               # MemberStatus Picklist: Active / Inactive / Suspended
ENROLLMENT_CHANNEL  = "Migration"            # falls in eurer Org Pflicht/sinnvoll


# --- Helpers ---
def sf_datetime(value: Optional[Union[datetime, date]]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min, tzinfo=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def sf_date(value: Optional[Union[date, datetime]]) -> Optional[str]:
    """Salesforce Date-Feld erwartet YYYY-MM-DD ohne Zeit."""
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    return value.isoformat()


def row_to_sf_record(row: dict) -> dict:
    """
    Mappt eine Zeile aus crm_imp_person_accounts auf einen
    Salesforce LoyaltyProgramMember-Insert.

    Insert: SF vergibt die Id, External-IDs gibt es nicht im klassischen Sinne.
    ContactId dient als Matchback-Key (1:1 Loyalty Member je Person Contact).
    None-Felder werden am Ende entfernt.
    """
    record = {
        # --- Pflicht / Stammdaten ---
        "ProgramId":                LOYALTY_PROGRAM_ID,
        "ContactId":                row.get("sf_person_contact_id"),
        "MemberStatus":             MEMBER_STATUS,
        "EnrollmentDate":           sf_date(row.get("loyalty_enrollment_date") or date.today()),
        #"EntraID__c":              row.get("entra_external_id"),
        #"EnrollmentChannel":        ENROLLMENT_CHANNEL,

        # --- Membership Nummer ---
        #"MembershipNumber":         row.get("loyalty_membership_number"),

        # --- Legacy / Migration Felder (Custom) ---
        "LegacyTier__c":            row.get("loyalty_legacy_tier"),
        "LegacyMemberId__c":          row.get("loyalty_legacy_number"),

        # --- Source Tracking (Custom) ---
        "SourceSystem__c":          row.get("source"),
    }

    # None-Werte entfernen
    return {k: v for k, v in record.items() if v is not None}


def records_to_csv(records: list[dict]) -> str:
    """Konvertiert eine Liste von Dicts in einen CSV-String für die Bulk API."""
    if not records:
        return ""

    all_keys = sorted({k for r in records for k in r})

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for record in records:
        writer.writerow(record)

    return buf.getvalue()


def chunked(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# --- Bulk API Helpers ---
def poll_job(sf: SalesforceClientCC, job_id: str) -> dict:
    url = f"{sf._base()}/jobs/ingest/{job_id}"
    for attempt in range(MAX_POLL_ATTEMPTS):
        r = sf._client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"Poll Job failed ({r.status_code}): {r.text}")
        status = r.json()
        state = status.get("state")
        print(f"  → [{attempt+1}/{MAX_POLL_ATTEMPTS}] state: {state} | "
              f"processed: {status.get('numberRecordsProcessed', 0)} | "
              f"failed: {status.get('numberRecordsFailed', 0)}")

        if state in ("JobComplete", "Failed", "Aborted"):
            return status

        time_module.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError(f"Job {job_id} hat nach {MAX_POLL_ATTEMPTS} Versuchen nicht abgeschlossen.")


def fetch_successful_records(sf: SalesforceClientCC, job_id: str) -> list[dict]:
    """Lädt die erfolgreich verarbeiteten Records (inkl. sf__Id) aus der Bulk API."""
    url = f"{sf._base()}/jobs/ingest/{job_id}/successfulResults"
    r = sf._client.get(url, headers={"Accept": "text/csv"})
    if r.status_code != 200:
        raise RuntimeError(f"Fetch successful records error ({r.status_code}): {r.text}")
    return list(csv.DictReader(io.StringIO(r.text)))


def fetch_failed_records(sf: SalesforceClientCC, job_id: str) -> list[dict]:
    url = f"{sf._base()}/jobs/ingest/{job_id}/failedResults"
    r = sf._client.get(url, headers={"Accept": "text/csv"})
    if r.status_code != 200:
        raise RuntimeError(f"Fetch failed records error ({r.status_code}): {r.text}")
    return list(csv.DictReader(io.StringIO(r.text)))


def save_failed_records(failed: list[dict], batch_index: int, batch_id: str) -> None:
    out_path = Path(f"local_data/failed_loyalty_members_insert_{batch_id}_batch_{batch_index}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def write_loyalty_member_ids(db: MySQLClient, batch_id: str, pairs: list[tuple[int, str]]) -> None:
    """
    Schreibt sf_loyalty_member_id zurück und setzt _loyalty_processed_at = NOW().
    pairs: (row_id, loyalty_member_id). sf_loyalty_member_id ist die Referenz für Schritt 3 (Punkte).
    """
    sql = """
        UPDATE crm_imp_person_accounts
        SET    sf_loyalty_member_id  = %s,
               _loyalty_processed_at = NOW()
        WHERE  _batch_id = %s
          AND  row_id    = %s
    """
    for row_id, member_id in pairs:
        db.execute(sql, (member_id, batch_id, row_id))


# --- Main ---
def main():
    import sys

    batch_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BATCH_ID

    print(f"insert_loyalty_members_bulk | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → batch_id: {batch_id}")

    # 1. Daten aus MySQL laden
    #    Gate: Account-Schritt muss durch sein (sf_person_contact_id vorhanden),
    #    Loyalty-Schritt noch offen.
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id              = %s
          AND  _excluded              = 0
          AND  _account_processed_at  IS NOT NULL
          AND  sf_person_contact_id   IS NOT NULL
          AND  _loyalty_processed_at  IS NULL
        """,
        (batch_id,),
    )
    print(f"  → {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  → Keine Records zu verarbeiten. Ende.")
        return

    # 2. Mapping — ContactId als Matchback-Key zu row_id behalten.
    #    (1:1 Loyalty Member je Person Contact → ContactId ist im Upload eindeutig.)
    records: list[dict] = []
    contactid_to_row_id: dict[str, int] = {}

    for row in rows:
        contact_id = row.get("sf_person_contact_id")
        if not contact_id:
            logging.error(f"row_id {row.get('row_id')} ohne sf_person_contact_id — übersprungen")
            continue
        records.append(row_to_sf_record(row))
        contactid_to_row_id[str(contact_id)] = row["row_id"]

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        inserted_pairs: list[tuple[int, str]] = []   # (row_id, loyalty_member_id)

        for i, start in enumerate(range(0, len(records), BATCH_SIZE)):
            batch_records = records[start : start + BATCH_SIZE]
            print(f"\nBatch {i+1}/{(len(records) + BATCH_SIZE - 1) // BATCH_SIZE} | "
                  f"{len(batch_records)} Records")

            try:
                job_id = sf.bulk_create_insert_job(OBJECT_NAME)
                print(f"  → Job erstellt: {job_id}")

                csv_data = records_to_csv(batch_records)
                sf.bulk_upload_csv(job_id, csv_data)

                sf.bulk_close_job(job_id)
                print(f"  → Job geschlossen (UploadComplete)")

                status = poll_job(sf, job_id)

                if status.get("state") == "Failed":
                    logging.error(f"Batch {i+1} | Job {job_id} komplett fehlgeschlagen: {status}")
                    continue

                # --- Erfolgreiche Records: ContactId → sf__Id → row_id ---
                if status.get("numberRecordsProcessed", 0) > status.get("numberRecordsFailed", 0):
                    succeeded = fetch_successful_records(sf, job_id)
                    for s in succeeded:
                        contact_id = s.get("ContactId")
                        member_id  = s.get("sf__Id")
                        row_id     = contactid_to_row_id.get(str(contact_id))
                        if row_id is not None and member_id:
                            inserted_pairs.append((row_id, member_id))

                # --- Fehlgeschlagene Records ---
                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1, batch_id)
                    total_failed.extend(failed)
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")

        # 4. sf_loyalty_member_id + _loyalty_processed_at zurückschreiben
        if inserted_pairs:
            print(f"\n  → Schreibe {len(inserted_pairs)} sf_loyalty_member_id zurück + _loyalty_processed_at = NOW()")
            write_loyalty_member_ids(db, batch_id, inserted_pairs)

    print(f"\ninsert_loyalty_members_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Erfolgreich:           {len(inserted_pairs)}")
    print(f"  → Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = Path(f"local_data/batch/all_failed_loyalty_members_insert_{batch_id}.json")
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()