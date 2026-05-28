from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, timezone, time, date
from typing import Optional, Union
from tqdm import tqdm
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
BATCH_SIZE = 5000
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 60

# Steuert, welcher Batch aus crm_imp_person_accounts gezogen wird.
# Wird per CLI-Arg überschreibbar gemacht (siehe main()).
DEFAULT_BATCH_ID = "conda_2026-05-28_invest_enrichment"

# --- TJ-Konstanten für diese Nachmigration ---
# TODO: SF-Ids vor dem Lauf eintragen
JOURNAL_TYPE_ID     = "0lvTe000000000XYZ"   # JournalType: Accrual (Punkte-Gutschrift)
JOURNAL_SUBTYPE_ID  = "0lwTe000000000ABC"   # JournalSubType: Manual / Legacy Migration
SOURCE_SYSTEM       = "investor excel"


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
    None-Felder werden am Ende entfernt.
    """
    record = {
        # --- Pflichtfelder ---
        "ActivityDate":             sf_datetime(datetime.now(timezone.utc)),
        "Status":                   "Pending",

        # --- Lookups (per SF-Id) ---
        "MemberId":                 row.get("sf_loyalty_member_id"),
        "JournalTypeId":            JOURNAL_TYPE_ID,
        "JournalSubTypeId":         JOURNAL_SUBTYPE_ID,
        "LoyaltyProgramId":         "0lpTe000000004rIAA",  

        # --- Punkte (Kernfeld dieser Migration) ---
        "Points__c":                row.get("loyalty_points_balance"),

        # --- Tracking ---
        "SourceSystem__c":          SOURCE_SYSTEM,
        #"ExternalMemberId__c":      row.get("external_id"),

        # --- Optional: Beschreibung für Audit ---
        "Description__c":           f"Investor points migration"
                                    ,
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
def mark_batch_processed(db: MySQLClient, batch_id: str, row_ids: list[int]) -> None:
    """Setzt _processed_at = NOW() für erfolgreich verarbeitete Records über row_id."""
    if not row_ids:
        return
    placeholders = ",".join(["%s"] * len(row_ids))
    sql = f"""
        UPDATE crm_imp_person_accounts
        SET    _processed_at = NOW()
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
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id      = %s
          AND  _excluded      = 0
          AND  _processed_at  IS NULL
        """,
        (batch_id,),
    )
    print(f"  → {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  → Keine Records zu verarbeiten. Ende.")
        return

    # 2. Mapping — wir behalten die row_id parallel zur Reihenfolge,
    #    damit wir nach dem Bulk-Insert wissen, welche Records erfolgreich waren.
    records: list[dict] = []
    row_ids: list[int]  = []  # parallele Liste, gleiche Reihenfolge wie records

    for row in rows:
        rec = row_to_sf_record(row)
        records.append(rec)
        row_ids.append(row["row_id"])

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        total_succeeded_row_ids: list[int] = []

        # Records und row_ids gemeinsam batchen, damit sie synchron bleiben
        for i, start in enumerate(range(0, len(records), BATCH_SIZE)):
            batch_records = records[start : start + BATCH_SIZE]
            batch_row_ids = row_ids[start : start + BATCH_SIZE]
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

                # Failed-Row-Indizes innerhalb des Batches ermitteln.
                # Bei einem Insert haben wir keine SF-Id zum Zurück-Matchen,
                # ABER: die Bulk-API liefert in failedResults die Zeilen in
                # derselben Reihenfolge wie im Upload, mit dem Original-CSV-Inhalt.
                # Wir matchen daher über Position (siehe Hinweis unten im Chat).
                failed_positions: set[int] = set()

                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1, batch_id)
                    total_failed.extend(failed)
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

                    # Position der Failed-Records im Upload-Batch rekonstruieren.
                    # failedResults enthält die Original-Felder zurück — wir matchen
                    # auf die Kombination (MemberId, Points__c), die für diesen
                    # Migrations-Use-Case eindeutig sein sollte (1 TJ pro Member).
                    upload_keys = [
                        (r.get("MemberId"), str(r.get("Points__c"))) for r in batch_records
                    ]
                    for f in failed:
                        key = (f.get("MemberId"), str(f.get("Points__c")))
                        for pos, uk in enumerate(upload_keys):
                            if uk == key and pos not in failed_positions:
                                failed_positions.add(pos)
                                break

                if status.get("state") == "Failed":
                    logging.error(f"Batch {i+1} | Job {job_id} komplett fehlgeschlagen: {status}")
                    continue

                # Erfolgreiche row_ids = alle im Batch minus die failed_positions
                succeeded = [
                    rid for pos, rid in enumerate(batch_row_ids)
                    if pos not in failed_positions
                ]
                total_succeeded_row_ids.extend(succeeded)

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")

        # 4. _processed_at in MySQL setzen für erfolgreiche Records
        if total_succeeded_row_ids:
            print(f"\n  → Markiere {len(total_succeeded_row_ids)} Records als _processed_at = NOW()")
            mark_batch_processed(db, batch_id, total_succeeded_row_ids)

    print(f"\ninsert_transaction_journals_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Erfolgreich:           {len(total_succeeded_row_ids)}")
    print(f"  → Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = Path(f"local_data/batch/all_failed_transaction_journals_{batch_id}.json")
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()