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
log_path = Path("logs/update_person_accounts_bulk.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
OBJECT_NAME = "Account"
BATCH_SIZE = 5000
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 60

# Steuert, welcher Batch aus crm_imp_person_accounts gezogen wird.
# Wird per CLI-Arg überschreibbar gemacht (siehe main()).
DEFAULT_BATCH_ID = "conda_2026-05-28_invest_enrichment"


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


def sf_bool(value) -> Optional[bool]:
    """MySQL TINYINT(1) → echter Bool, NULL bleibt NULL (= Feld wird nicht überschrieben)."""
    if value is None:
        return None
    return bool(value)


def row_to_sf_record(row: dict) -> dict:
    """
    Mappt eine Zeile aus crm_imp_person_accounts auf ein Salesforce Account-Update.
    Update erfolgt per Salesforce Id (sf_account_id) → muss als 'Id' Spalte im CSV stehen.

    None-Felder werden am Ende entfernt, damit Bulk API sie NICHT überschreibt.
    """
    record = {
        # --- Pflicht: SF Id für Update-Operation ---
        "Id":                                   row.get("sf_account_id"),

        # --- External IDs ---
        "ExternalID__pc":                        row.get("external_id"),
        #"ClusterID__c":                         row.get("cluster_id"),
        #"EntraExternalID__c":                   row.get("entra_external_id"),

        # --- Source Tracking ---
        "SourceSystem__pc":                     row.get("source"),
        #"SourceOrigin__pc":                     row.get("source_origin"),

        # --- Profile: Identity ---
        #"Salutation":                           row.get("salutation"),
        #"FirstName":                            row.get("first_name"),
        #"MiddleName":                           row.get("middle_name"),
        #"LastName":                             row.get("last_name"),
        "PersonBirthdate":                      sf_date(row.get("birth_date")),
        #"BirthPlace__pc":                       row.get("birth_place"),
        #"PersonGenderIdentity":                 row.get("gender"),

        # --- Profile: Communication ---
        #"PersonEmail":                          row.get("email"),
        #"Phone":                                row.get("phone"),
        #"PreferredLanguage__pc":                row.get("preferred_language"),
        #"NationalityCountryCode__pc":           row.get("nationality_country_code"),

        # --- Profile: Address (Mailing) ---
        #"PersonMailingStreet":                  row.get("address"),
        #"PersonMailingPostalCode":              row.get("postal_code"),
        #"PersonMailingCity":                    row.get("city"),
        #"PersonMailingState":                   row.get("state"),
        #"PersonMailingCountry":                 row.get("country"),

        # --- Business Unit Flags ---
        #"HotelCustomer__pc":                    sf_bool(row.get("hotel_customer")),
        #"CampingCustomer__pc":                  sf_bool(row.get("camping_customer")),
        #"ResidencesCustomer__pc":               sf_bool(row.get("residences_customer")),
        "InvestCustomer__pc":                   sf_bool(row.get("invest_customer")),

        #"PrimaryProperty__pc":                  row.get("primary_property_id"),

        # --- Investment ---
        "InvestmentStatus__pc":                 row.get("investment_status"),
        "InvestmentExpirationDate__pc":         sf_date(row.get("investment_expiration_date")),
    }

    # None-Werte entfernen — bei Bulk API 2.0 bedeutet leerer String "Feld leeren",
    # None/weglassen bedeutet "Feld nicht anfassen". Wir wollen letzteres.
    return {k: v for k, v in record.items() if v is not None}


def records_to_csv(records: list[dict]) -> str:
    """Konvertiert eine Liste von Dicts in einen CSV-String für die Bulk API."""
    if not records:
        return ""

    # Id zuerst, Rest alphabetisch
    all_keys = ["Id"] + sorted({k for r in records for k in r if k != "Id"})

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
    out_path = Path(f"local_data/failed_person_accounts_{batch_id}_batch_{batch_index}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def mark_batch_processed(db: MySQLClient, batch_id: str, sf_ids: list[str]) -> None:
    """Setzt _processed_at = NOW() für erfolgreich verarbeitete Records."""
    if not sf_ids:
        return
    placeholders = ",".join(["%s"] * len(sf_ids))
    sql = f"""
        UPDATE crm_imp_person_accounts
        SET    _processed_at = NOW()
        WHERE  _batch_id     = %s
          AND  sf_account_id IN ({placeholders})
    """
    db.execute(sql, [batch_id, *sf_ids])


# --- Main ---
def main():
    import sys

    batch_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BATCH_ID

    print(f"update_person_accounts_bulk | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → batch_id: {batch_id}")

    # 1. Daten aus MySQL laden
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id      = %s
          AND  _operation     = 'update'
          AND  _excluded      = 0
          AND  _processed_at  IS NULL
          AND  sf_account_id  IS NOT NULL
        """,
        (batch_id,),
    )
    print(f"  → {len(rows)} Person Accounts zum Update geladen")

    if not rows:
        print("  → Keine Records zu verarbeiten. Ende.")
        return

    # 2. Mapping
    records = [row_to_sf_record(row) for row in rows]

    # Sanity-Check: alle Records müssen eine Id haben
    missing_id = [r for r in records if not r.get("Id")]
    if missing_id:
        raise RuntimeError(
            f"{len(missing_id)} Records ohne sf_account_id — Update nicht möglich. Abbruch."
        )

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        total_succeeded_ids: list[str] = []
        batches = list(chunked(records, BATCH_SIZE))

        for i, batch in enumerate(tqdm(batches, desc="Bulk Batches")):
            print(f"\nBatch {i+1}/{len(batches)} | {len(batch)} Records")

            batch_ids_in_chunk = [r["Id"] for r in batch]
            failed_ids_in_chunk: set[str] = set()

            try:
                job_id = sf.bulk_create_update_job(OBJECT_NAME)
                print(f"  → Job erstellt: {job_id}")

                csv_data = records_to_csv(batch)
                sf.bulk_upload_csv(job_id, csv_data)

                sf.bulk_close_job(job_id)
                print(f"  → Job geschlossen (UploadComplete)")

                status = poll_job(sf, job_id)

                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1, batch_id)
                    total_failed.extend(failed)
                    # Failed-Ids sammeln, damit wir sie NICHT als processed markieren
                    failed_ids_in_chunk = {f.get("sf__Id") or f.get("Id") for f in failed if f.get("sf__Id") or f.get("Id")}
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

                if status.get("state") == "Failed":
                    logging.error(f"Batch {i+1} | Job {job_id} komplett fehlgeschlagen: {status}")
                    # Bei komplettem Fehlschlag: alle als failed betrachten, keine als processed markieren
                    continue

                # Erfolgreiche Ids = alle im Batch minus die failed
                succeeded_in_chunk = [sf_id for sf_id in batch_ids_in_chunk if sf_id not in failed_ids_in_chunk]
                total_succeeded_ids.extend(succeeded_in_chunk)

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")

        # 4. _processed_at in MySQL setzen für erfolgreiche Records
        if total_succeeded_ids:
            print(f"\n  → Markiere {len(total_succeeded_ids)} Records als _processed_at = NOW()")
            mark_batch_processed(db, batch_id, total_succeeded_ids)

    print(f"\nupdate_person_accounts_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Erfolgreich:           {len(total_succeeded_ids)}")
    print(f"  → Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = Path(f"local_data/batch/all_failed_person_accounts_{batch_id}.json")
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()