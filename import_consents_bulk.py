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
log_path = Path("logs/import_consent_member_bulk.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
OBJECT_NAME = "ContactPointConsent"
BATCH_SIZE = 5000
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 60


# --- Helpers ---
def dev_email(email: str | None) -> str | None:
    if not email:
        return None
    return f"{email}.inactive"


def sf_datetime(value: Optional[Union[datetime, date]]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min, tzinfo=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def row_to_sf_record(row: dict) -> dict:
    """Mappt eine MySQL-Zeile auf ein Salesforce Consent."""
    record = {
        "Name": "marketing_central",
        "ContactPointId": row.get("cpe_id"),
        "CaptureDate": sf_datetime(date.today()),
        "CaptureSource": "gms" ,
        "PrivacyConsentStatus": "OptIn",
        "DataUsePurposeId": '0ZWTe0000000X7FOAU',
        "EffectiveFrom": sf_datetime(date.today()),
        #"EffectiveTo": None,
        "SourceSystem__c": row.get("source"),
    }

    # None-Werte entfernen
    return {k: v for k, v in record.items() if v is not None}


def records_to_csv(records: list[dict]) -> str:
    """Konvertiert eine Liste von Dicts in einen CSV-String für die Bulk API."""
    if not records:
        return ""

    all_keys = sorted({k for r in records for k in r})

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction='ignore', lineterminator="\n")
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


def save_failed_records(failed: list[dict], batch_index: int) -> None:
    out_path = Path(f"local_data/failed_consent_batch_{batch_index}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- Main ---
def main():
    print(f"import_consent_bulk | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Daten aus MySQL laden
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    accounts = db.fetch_all(""" error | select 'gustaffo' as 'source', cpe.id  as cpe_id
                                    from crm_cp_email_sfid_prod cpe
                                    inner join  gustaffo_newsletter_contacts gus 
                                        on gus.email = cpe.EmailAddress
                                    where not EXISTS ( 
                                                select 1 from crm_cp_consent_sfid_prod con where con.ContactPointId  = cpe.Id and con.Name = 'marketing_central'
                                    ) 
                                    and left(cpe.PartyID__c,3) = '003' 
                            """)
    
    print(f"  → {len(accounts)} Consents geladen")

    # 2. Mapping
    records = [row_to_sf_record(row) for row in accounts]

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        batches = list(chunked(records, BATCH_SIZE))

        for i, batch in enumerate(tqdm(batches, desc="Bulk Batches")):
            print(f"\nBatch {i+1}/{len(batches)} | {len(batch)} Records")

            try:
                job_id = sf.bulk_create_insert_job(OBJECT_NAME)
                print(f"  → Job erstellt: {job_id}")

                csv_data = records_to_csv(batch)
                sf.bulk_upload_csv(job_id, csv_data)

                sf.bulk_close_job(job_id)
                print(f"  → Job geschlossen (UploadComplete)")

                status = poll_job(sf, job_id)

                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1)
                    total_failed.extend(failed)
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

                if status.get("state") == "Failed":
                    logging.error(f"Batch {i+1} | Job {job_id} komplett fehlgeschlagen: {status}")

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")

    print(f"\nimport_consent_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Gesamt fehlgeschlagene Records: {len(total_failed)}")

    if total_failed:
        all_failed_path = Path("local_data/batch/all_failed_consent.json")
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()