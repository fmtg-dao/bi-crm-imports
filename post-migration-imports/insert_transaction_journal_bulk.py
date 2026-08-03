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
log_path = Path("logs/insert_transaction_journals_bulk.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
OBJECT_NAME = "TransactionJournal"
BATCH_SIZE = 500
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 60

# Steuert, welcher Batch aus crm_imp_person_accounts gezogen wird.
# Wird per CLI-Arg überschreibbar gemacht (siehe main()).
DEFAULT_BATCH_ID = "2026-05-28_invest_points_migration"

# --- TJ-Konstanten für diese Nachmigration ---
# TODO: SF-Ids vor dem Lauf eintragen
LOYALTY_PROGRAM_ID  = "0lpTe000000004rIAA"   # Loyalty Program in Salesforce
JOURNAL_TYPE_ID     = "0lEd10000001oafEAA"    # JournalType: Accrual (Punkte-Gutschrift)
#JOURNAL_SUBTYPE_ID  = "0lwTe000000000ABC"    # JournalSubType: Manual / Legacy Migration
SOURCE_SYSTEM       = "conda"


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
    Mappt eine Zeile aus crm_imp_person_accounts auf einen Salesforce TransactionJournal-Insert.

    Insert: SF vergibt die Id, External-IDs gibt es nicht.
    MemberId (= sf_loyalty_member_id) dient als Matchback-Key (1:1 TJ je Member).
    None-Felder werden am Ende entfernt.
    """
    record = {
        # --- Pflichtfelder ---
        "ActivityDate":             sf_datetime(datetime.now(timezone.utc)),
        "Status":                   "Pending",

        # --- Lookups (per SF-Id) ---
        "MemberId":                 row.get("sf_loyalty_member_id"),
        "JournalTypeId":            JOURNAL_TYPE_ID,
        #"JournalSubTypeId":         JOURNAL_SUBTYPE_ID,
        "LoyaltyProgramId":         LOYALTY_PROGRAM_ID,

        # --- Punkte (Kernfeld dieser Migration) ---
        "Points__c":                row.get("loyalty_points_balance"),

        # --- Tracking ---
        "SourceSystem__c":          SOURCE_SYSTEM,
        #"ExternalMemberId__c":      row.get("external_id"),

        # --- Optional: Beschreibung für Audit ---
        "Description__c":           "Investor Migration Points",
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
    out_path = Path(f"local_data/failed_transaction_journals_{batch_id}_batch_{batch_index}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def mark_points_processed(db: MySQLClient, batch_id: str, row_ids: list[int]) -> None:
    """
    Setzt _points_processed_at = NOW() für erfolgreich verarbeitete Records über row_id.
    (Keine sf_transaction_id-Spalte vorhanden → es wird nur der Status gesetzt.)
    """
    if not row_ids:
        return
    placeholders = ",".join(["%s"] * len(row_ids))
    sql = f"""
        UPDATE crm_imp_person_accounts
        SET    _points_processed_at = NOW()
        WHERE  _batch_id = %s
          AND  row_id    IN ({placeholders})
    """
    db.execute(sql, [batch_id, *row_ids])


# --- Main ---
def main():
    import sys

    batch_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BATCH_ID

    print(f"insert_transaction_journals_bulk | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → batch_id: {batch_id}")

    # 1. Daten aus MySQL laden
    #    Gate: Loyalty-Schritt muss durch sein (sf_loyalty_member_id vorhanden),
    #    Punkte-Schritt noch offen. Nur Zeilen mit tatsächlichem Punktestand.
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id              = %s
          AND  _excluded              = 0
          -- AND  _loyalty_processed_at  IS NOT NULL
          AND  sf_loyalty_member_id   IS NOT NULL
          AND  _points_processed_at   IS NULL
          AND  loyalty_points_balance IS NOT NULL
        """,
        (batch_id,),
    )
    print(f"  → {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  → Keine Records zu verarbeiten. Ende.")
        return

    # 2. Mapping — MemberId (= sf_loyalty_member_id) als Matchback-Key zu row_id behalten.
    #    (1:1 TJ je Member → MemberId ist im Upload eindeutig.)
    records: list[dict] = []
    memberid_to_row_id: dict[str, int] = {}

    for row in rows:
        member_id = row.get("sf_loyalty_member_id")
        if not member_id:
            logging.error(f"row_id {row.get('row_id')} ohne sf_loyalty_member_id — übersprungen")
            continue
        records.append(row_to_sf_record(row))
        memberid_to_row_id[str(member_id)] = row["row_id"]

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        succeeded_row_ids: list[int] = []

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

                # --- Erfolgreiche Records: MemberId → row_id (keine sf-Id-Rückschreibung nötig) ---
                if status.get("numberRecordsProcessed", 0) > status.get("numberRecordsFailed", 0):
                    succeeded = fetch_successful_records(sf, job_id)
                    for s in succeeded:
                        member_id = s.get("MemberId")
                        row_id    = memberid_to_row_id.get(str(member_id))
                        if row_id is not None:
                            succeeded_row_ids.append(row_id)

                # --- Fehlgeschlagene Records ---
                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1, batch_id)
                    total_failed.extend(failed)
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")

        # 4. _points_processed_at in MySQL setzen für erfolgreiche Records
        if succeeded_row_ids:
            print(f"\n  → Markiere {len(succeeded_row_ids)} Records als _points_processed_at = NOW()")
            mark_points_processed(db, batch_id, succeeded_row_ids)

    print(f"\ninsert_transaction_journals_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Erfolgreich:           {len(succeeded_row_ids)}")
    print(f"  → Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = Path(f"local_data/batch/all_failed_transaction_journals_{batch_id}.json")
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()