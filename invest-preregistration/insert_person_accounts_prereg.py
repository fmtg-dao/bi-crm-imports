import sys
from pathlib import Path

# config, mysql_client und salesforce_client_prod liegen im Repo-Root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, timezone, time, date
from typing import Optional, Union
import json
import csv
import io
import time as time_module
import urllib.parse
import logging

# Alle Artefakte relativ zum Repo-Root, nicht zum cwd (Konvention wie
# insert_consents_invest.py / update_mailing_addresses.py).
REPO_ROOT = Path(__file__).resolve().parents[1]

# --- Logging Setup ---
log_path = REPO_ROOT / "logs/insert_person_accounts_prereg.log"
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
OBJECT_NAME = "Account"
BATCH_SIZE = 500
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 60

# Kein DEFAULT_BATCH_ID: die Batch-Id MUSS explizit kommen, ein stiller
# Fallback auf einen alten Batch war ein bekannter Footgun.

# Feld, in das external_id geschrieben wird — dient als Matchback-Key,
# um nach dem Insert sf__Id (Account.Id) der richtigen Staging-Zeile zuzuordnen.
# ExternalID__pc ist Text(40) → external_id (Pre-Reg GUID, 36 Zeichen) passt.
EXTERNAL_ID_FIELD = "ExternalID__pc"

# --- Person-Account-Konstanten für diese Migration ---
# TODO: RecordTypeId des Person-Account-RecordTypes vor dem Lauf eintragen.
# Ohne korrekten Person-Account-RecordType legt SF einen Business Account an!
PERSON_ACCOUNT_RECORD_TYPE_ID = "012Te0000018UgIIAU"


# --- Helpers ---
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
    Salesforce Account (Person Account) Insert.

    Insert: SF vergibt die Id. external_id geht in ExternalID__pc als Matchback-Key.
    Loyalty-Felder (Schritt 2) und Consent (Schritt 4) werden hier NICHT gemappt.
    None-Felder werden am Ende entfernt.
    """
    record = {
        # --- Pflicht: RecordType + Name ---
        "RecordTypeId":                 PERSON_ACCOUNT_RECORD_TYPE_ID,
        "LastName":                     row.get("last_name"),
        "FirstName":                    row.get("first_name"),
        "MiddleName":                   row.get("middle_name"),
        "Salutation":                   row.get("salutation"),               # Picklist (Mr./Ms.)

        # --- Identity / Profile ---
        "PersonEmail":                  row.get("email"),
        "PersonBirthdate":              sf_date(row.get("birth_date")),
        "BirthPlace__pc":               row.get("birth_place"),
        "PersonGenderIdentity":         row.get("gender"),                   # Picklist (Female/Male)
        "PersonMobilePhone":            row.get("phone"),
        "PreferredLanguage__pc":        row.get("preferred_language"),       # Picklist (de/sk)
        "NationalityCountryCode__pc":   row.get("nationality_country_code"), # Picklist

        # --- Mailing Address ---
        "PersonMailingStreet":          row.get("address"),
        "PersonMailingPostalCode":      row.get("postal_code"),
        "PersonMailingCity":            row.get("city"),
        "PersonMailingState":           row.get("state"),
        "PersonMailingCountry":         row.get("country"),

        # --- External / Source ---
        EXTERNAL_ID_FIELD:              row.get("external_id"),
        "ClusterID__c":                 row.get("cluster_id"),               # External ID
        "EntraExternalID__pc":          row.get("entra_external_id"),
        "SourceOrigin__pc":             row.get("source_origin"),
        "SourceSystem__pc":             row.get("source"),                   # Picklist

        # --- Business Unit Flags ---
        "InvestCustomer__pc":           bool(row.get("invest_customer") or 0),
        # "HotelCustomer__pc":          bool(row.get("hotel_customer") or 0),
        # "CampingCustomer__pc":        bool(row.get("camping_customer") or 0),
        # "ResidencesCustomer__pc":     bool(row.get("residences_customer") or 0),

        # --- Investment ---
        # Bewusst NICHT gemappt: die Pre-Reg-Liste sind Prospects, beide Spalten
        # sind in der Quelle durchgehend NULL (SQL-Check 2026-08-28).
        # "InvestmentStatus__pc":         row.get("investment_status"),
        # "InvestmentExpirationDate__pc": sf_date(row.get("investment_expiration_date")),
    }

    # None-Werte entfernen (Bulk API: leerer String = Feldlöschung, None = weglassen)
    return {k: v for k, v in record.items() if v is not None}


def records_to_csv(records: list[dict]) -> str:
    """Konvertiert eine Liste von Dicts in einen CSV-String für die Bulk API."""
    if not records:
        return ""

    # External ID zuerst, Rest alphabetisch (Union über alle Records)
    all_keys = [EXTERNAL_ID_FIELD] + sorted(
        {k for r in records for k in r if k != EXTERNAL_ID_FIELD}
    )

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
    out_path = REPO_ROOT / f"local_data/failed_prereg_insert_{batch_id}_batch_{batch_index}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


def fetch_person_contact_ids(sf: SalesforceClientCC, account_ids: list[str]) -> dict[str, str]:
    """
    Holt die auto-erzeugte PersonContactId je Account nach dem Insert.
    Beim Account-Insert gibt die Bulk API nur Account.Id zurück, nicht den Person Contact.
    """
    mapping: dict[str, str] = {}
    for chunk in chunked(account_ids, 200):
        ids = ",".join(f"'{i}'" for i in chunk)
        soql = f"SELECT Id, PersonContactId FROM Account WHERE Id IN ({ids})"
        url = f"{sf._base()}/query/?q={urllib.parse.quote(soql)}"

        while url:
            r = sf._client.get(url)
            if r.status_code != 200:
                raise RuntimeError(f"Query PersonContactId failed ({r.status_code}): {r.text}")
            data = r.json()
            for rec in data.get("records", []):
                mapping[rec["Id"]] = rec.get("PersonContactId")
            # Pagination, falls > 2000 Records (sollte bei 200er-Chunks nicht passieren)
            next_url = data.get("nextRecordsUrl")
            url = f"{sf._instance_url()}{next_url}" if next_url else None

    return mapping


# --- DB Helpers ---
def write_account_ids(db: MySQLClient, batch_id: str, pairs: list[tuple[int, str]]) -> None:
    """Schreibt sf_account_id zurück und setzt _account_processed_at = NOW(). pairs: (row_id, account_id)."""
    sql = """
        UPDATE crm_imp_person_accounts
        SET    sf_account_id        = %s,
               _account_processed_at = NOW()
        WHERE  _batch_id = %s
          AND  row_id    = %s
    """
    for row_id, account_id in pairs:
        db.execute(sql, (account_id, batch_id, row_id))


def write_person_contact_ids(db: MySQLClient, batch_id: str, pairs: list[tuple[str, str]]) -> None:
    """Schreibt sf_person_contact_id zurück. pairs: (account_id, contact_id)."""
    sql = """
        UPDATE crm_imp_person_accounts
        SET    sf_person_contact_id = %s
        WHERE  _batch_id     = %s
          AND  sf_account_id = %s
    """
    for account_id, contact_id in pairs:
        if contact_id:
            db.execute(sql, (contact_id, batch_id, account_id))


# --- Main ---
def main():
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("Aufruf: insert_person_accounts_prereg.py <batch_id> [--dry-run]")
    batch_id = args[0]

    print(f"insert_person_accounts_prereg | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → batch_id: {batch_id}{' | DRY-RUN' if dry_run else ''}")

    # 1. Daten aus MySQL laden — Gate: Account-Schritt noch offen.
    #    _operation = 'insert' zusätzlich zum Batch-Filter: der Insert darf
    #    nie Update-Zeilen einsammeln, auch nicht bei einem falschen Batch-Arg.
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id             = %s
          AND  _operation            = 'insert'
          AND  _excluded             = 0
          AND  _account_processed_at IS NULL
        """,
        (batch_id,),
    )
    print(f"  → {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  → Keine Records zu verarbeiten. Ende.")
        return

    # 2. Mapping — external_id als Matchback-Key zu row_id behalten
    records: list[dict] = []
    extid_to_row_id: dict[str, int] = {}

    for row in rows:
        ext_id = row.get("external_id")
        if not ext_id:
            logging.error(f"row_id {row.get('row_id')} ohne external_id — übersprungen (kein Matchback möglich)")
            continue
        records.append(row_to_sf_record(row))
        extid_to_row_id[str(ext_id)] = row["row_id"]

    if dry_run:
        # Kein SF-Kontakt: CSV bauen, wegschreiben, Zeilen zaehlen.
        csv_data = records_to_csv(records)
        out_path = REPO_ROOT / f"local_data/dry_run_prereg_insert_{batch_id}.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(csv_data, encoding="utf-8")
        print(f"  → DRY-RUN: {len(records)} Records, CSV: {out_path}")
        return

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        batch_errors = 0
        inserted_pairs: list[tuple[int, str]] = []   # (row_id, account_id) → für sf_account_id-Writeback
        all_account_ids: list[str] = []              # für PersonContactId-Nachladung

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

                if status.get("state") in ("Failed", "Aborted"):
                    logging.error(f"Batch {i+1} | Job {job_id} komplett fehlgeschlagen: {status}")
                    batch_errors += 1
                    continue

                # --- Erfolgreiche Records: external_id → Account.Id → row_id ---
                if status.get("numberRecordsProcessed", 0) > status.get("numberRecordsFailed", 0):
                    succeeded = fetch_successful_records(sf, job_id)
                    for s in succeeded:
                        ext_id = s.get(EXTERNAL_ID_FIELD)
                        acc_id = s.get("sf__Id")
                        row_id = extid_to_row_id.get(str(ext_id))
                        if row_id is not None and acc_id:
                            inserted_pairs.append((row_id, acc_id))
                            all_account_ids.append(acc_id)

                # --- Fehlgeschlagene Records ---
                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1, batch_id)
                    total_failed.extend(failed)
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")
                batch_errors += 1

        # 4. sf_account_id + _account_processed_at zurückschreiben
        if inserted_pairs:
            print(f"\n  → Schreibe {len(inserted_pairs)} sf_account_id zurück + _account_processed_at = NOW()")
            write_account_ids(db, batch_id, inserted_pairs)

        # 5. PersonContactId nachladen und sf_person_contact_id zurückschreiben
        if all_account_ids:
            print(f"  → Lade PersonContactId für {len(all_account_ids)} Accounts nach")
            contact_map = fetch_person_contact_ids(sf, all_account_ids)
            contact_pairs = [(acc_id, contact_map.get(acc_id)) for acc_id in all_account_ids]
            missing = [acc for acc, c in contact_pairs if not c]
            if missing:
                logging.error(f"{len(missing)} Accounts ohne PersonContactId (kein Person Account?): {missing[:10]}")
            write_person_contact_ids(db, batch_id, contact_pairs)

    print(f"\ninsert_person_accounts_prereg | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Erfolgreich (Account):  {len(inserted_pairs)}")
    print(f"  → Fehlgeschlagen:         {len(total_failed)}")

    if total_failed:
        all_failed_path = REPO_ROOT / f"local_data/batch/all_failed_prereg_insert_{batch_id}.json"
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")

    # Exit != 0 bei jedem Problem: der Notebook-Runner prueft nur den
    # Returncode und darf nach einem Teilerfolg nicht einfach weiterlaufen.
    if total_failed or batch_errors:
        raise SystemExit(
            f"Lauf unvollstaendig: {len(total_failed)} Record-Fehler, "
            f"{batch_errors} Batch-Fehler"
        )


if __name__ == "__main__":
    main()