import sys
from pathlib import Path

# config, mysql_client und salesforce_client_prod liegen im Repo-Root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, date
from typing import Optional, Union
import json
import csv
import io
import time as time_module
import logging
from collections import Counter

# Alle Artefakte relativ zum Repo-Root, nicht zum cwd (Konvention wie
# insert_consents_invest.py / update_mailing_addresses.py).
REPO_ROOT = Path(__file__).resolve().parents[1]

# --- Logging Setup ---
log_path = REPO_ROOT / "logs/update_accounts_prereg.log"
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

# Kein DEFAULT_BATCH_ID: die Batch-Id MUSS explizit kommen, ein stiller
# Fallback auf einen alten Batch war ein bekannter Footgun.


# --- Helpers ---
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

    BEWUSST NUR das Invest-Flag: die Pre-Reg-Liste sind PROSPECTS (Team-Aussage
    2026-08-28, per SQL bestätigt: investment_status und expiration_date sind in
    allen 3.192 Zeilen NULL). Kein InvestmentStatus__pc, kein
    InvestmentExpirationDate__pc, kein ExternalID__pc-Stempel. Bestehende
    Accounts werden sonst NICHT angefasst.

    None-Felder werden am Ende entfernt, damit Bulk API sie NICHT überschreibt.
    """
    record = {
        # --- Pflicht: SF Id für Update-Operation ---
        "Id":                 row.get("sf_account_id"),

        # --- Invest-Flag (einziges autorisiertes Feld für Bestandsaccounts) ---
        "InvestCustomer__pc": sf_bool(row.get("invest_customer")),
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
        print(f"  -> [{attempt+1}/{MAX_POLL_ATTEMPTS}] state: {state} | "
              f"processed: {status.get('numberRecordsProcessed', 0)} | "
              f"failed: {status.get('numberRecordsFailed', 0)}")

        if state in ("JobComplete", "Failed", "Aborted"):
            return status

        time_module.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError(f"Job {job_id} hat nach {MAX_POLL_ATTEMPTS} Versuchen nicht abgeschlossen.")


def fetch_successful_records(sf: SalesforceClientCC, job_id: str) -> list[dict]:
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
    out_path = REPO_ROOT / f"local_data/failed_prereg_update_{batch_id}_batch_{batch_index}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  -> {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


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
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("Aufruf: update_accounts_prereg.py <batch_id> [--dry-run]")
    batch_id = args[0]

    print(f"update_accounts_prereg | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> batch_id: {batch_id}{' | DRY-RUN' if dry_run else ''}")

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
    print(f"  -> {len(rows)} Person Accounts zum Update geladen")

    if not rows:
        print("  -> Keine Records zu verarbeiten. Ende.")
        return

    # 2. Matchback-Eindeutigkeit VOR allem anderen: markiert wird per
    #    sf_account_id, eine Doublette würde die falsche Zeile abhaken.
    doubles = sorted(
        a for a, n in Counter(str(r["sf_account_id"]) for r in rows).items() if n > 1
    )
    if doubles:
        raise SystemExit(f"sf_account_id mehrfach im Batch, Abbruch: {doubles[:10]}")

    records = [row_to_sf_record(row) for row in rows]

    missing_id = [r for r in records if not r.get("Id")]
    if missing_id:
        raise SystemExit(
            f"{len(missing_id)} Records ohne sf_account_id — Update nicht möglich. Abbruch."
        )

    if dry_run:
        csv_data = records_to_csv(records)
        out_path = REPO_ROOT / f"local_data/dry_run_prereg_update_{batch_id}.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(csv_data, encoding="utf-8")
        print(f"  -> DRY-RUN: {len(records)} Records, CSV: {out_path}")
        return

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []
        total_succeeded_ids: list[str] = []
        batch_errors = 0
        batches = list(chunked(records, BATCH_SIZE))

        for i, batch in enumerate(batches):
            print(f"\nBatch {i+1}/{len(batches)} | {len(batch)} Records")

            try:
                job_id = sf.bulk_create_update_job(OBJECT_NAME)
                print(f"  -> Job erstellt: {job_id}")

                csv_data = records_to_csv(batch)
                sf.bulk_upload_csv(job_id, csv_data)

                sf.bulk_close_job(job_id)
                print("  -> Job geschlossen (UploadComplete)")

                status = poll_job(sf, job_id)

                # 'Aborted' ist ein Fehlschlag, aber successfulResults zuerst
                # lesen: teilweise geschriebene Records muessen markiert werden.
                job_failed = status.get("state") in ("Failed", "Aborted")
                if job_failed:
                    logging.error(f"Batch {i+1} | Job {job_id} state {status.get('state')}: {status}")

                # Erfolgreiche Ids aus successfulResults, NICHT "Batch minus
                # failed": bei Failed/Aborted stimmt die Differenzmenge nicht.
                batch_succeeded: list[str] = []
                if status.get("numberRecordsProcessed", 0) > status.get("numberRecordsFailed", 0):
                    succeeded = fetch_successful_records(sf, job_id)
                    batch_succeeded = [s.get("sf__Id") or s.get("Id") for s in succeeded]
                    batch_succeeded = [s for s in batch_succeeded if s]

                # Writeback je Batch, nicht erst am Ende: bricht der Lauf ab,
                # steht in MySQL, was in SF wirklich schon geschrieben ist.
                if batch_succeeded:
                    mark_batch_processed(db, batch_id, batch_succeeded)
                    total_succeeded_ids.extend(batch_succeeded)
                    print(f"  -> {len(batch_succeeded)} Records als verarbeitet markiert")

                if job_failed or status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    if failed:
                        save_failed_records(failed, i + 1, batch_id)
                        total_failed.extend(failed)
                        logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

                if job_failed:
                    batch_errors += 1

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  x Batch {i+1} Fehler: {e}")
                batch_errors += 1

    print(f"\nupdate_accounts_prereg | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> Erfolgreich:           {len(total_succeeded_ids)}")
    print(f"  -> Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = REPO_ROOT / f"local_data/batch/all_failed_prereg_update_{batch_id}.json"
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  -> Alle Fehler gespeichert: {all_failed_path}")

    # Exit != 0 bei jedem Problem: der Notebook-Runner prueft nur den
    # Returncode und darf nach einem Teilerfolg nicht einfach weiterlaufen.
    if total_failed or batch_errors:
        raise SystemExit(
            f"Lauf unvollstaendig: {len(total_failed)} Record-Fehler, "
            f"{batch_errors} Batch-Fehler"
        )


if __name__ == "__main__":
    main()
