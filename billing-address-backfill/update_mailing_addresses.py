import sys
from pathlib import Path

# config, mysql_client und salesforce_client_prod liegen im Repo-Root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime
import json
import csv
import io
import re
import time as time_module
import logging
from collections import Counter, defaultdict

# Alle Artefakte relativ zum Repo-Root, nicht zum cwd: der Notebook-Runner und
# ein Direktaufruf aus billing-address-backfill/ muessen in denselben Ordnern landen.
REPO_ROOT = Path(__file__).resolve().parents[1]

# --- Logging Setup ---
log_path = REPO_ROOT / "logs/update_mailing_addresses.log"
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

# Staging-Spalte -> Salesforce-Feld. Quelle in der Staging-Tabelle sind die
# Billing*-Werte (address = BillingStreet, country = BillingCountryCode__c,
# beide Seiten ISO-2, siehe 02_recon_addresses.sql Query 4).
FIELD_MAP = {
    "address":     "PersonMailingStreet",
    "city":        "PersonMailingCity",
    "postal_code": "PersonMailingPostalCode",
    "country":     "PersonMailingCountry",
}


def row_to_sf_record(row: dict) -> dict:
    """
    Mappt eine Staging-Zeile auf ein Account-Update per Id. Leere/NULL-Felder
    werden ENTFERNT statt leer mitgeschickt: bei einem Bulk-API-UPDATE bedeutet
    eine leere CSV-Zelle "Feld leeren", eine fehlende Spalte "nicht anfassen".
    """
    record = {"Id": row.get("sf_account_id")}
    for col, sf_field in FIELD_MAP.items():
        v = row.get(col)
        if v is not None and str(v).strip() != "":
            record[sf_field] = v
    return record


def group_by_signature(records: list[dict]) -> dict[tuple, list[dict]]:
    """
    Gruppiert Records nach der Menge ihrer belegten Felder. Jede Gruppe wird
    als eigener Bulk-Job mit exakt diesen Spalten geschickt, damit NIE eine
    leere Zelle im CSV steht (leere Zelle = Feld wird in SF geleert).
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        sig = tuple(sorted(k for k in r if k != "Id"))
        groups[sig].append(r)
    return dict(groups)


def records_to_csv(records: list[dict]) -> str:
    if not records:
        return ""

    all_keys = ["Id"] + sorted({k for r in records for k in r if k != "Id"})

    # Verteidigung gegen den Leere-Zelle-Fall: innerhalb eines Jobs muessen
    # ALLE Records exakt dieselben Spalten belegen (group_by_signature).
    for r in records:
        if set(r) != set(all_keys):
            raise SystemExit(f"Record mit abweichenden Spalten im CSV: {sorted(r)} vs {all_keys}")

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


SF_ID_PATTERN = re.compile(r"^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$")


def accounts_with_mailing_street(sf: SalesforceClientCC, account_ids: list[str]) -> set[str]:
    """
    Live-Check vor dem Schreiben: Accounts, bei denen PersonMailingStreet
    inzwischen belegt ist (die Live-Integration schreibt laufend weiter),
    werden uebersprungen. Der Spiegel kann Stunden alt sein, die Wahrheit
    liegt in Salesforce.
    """
    bad = [a for a in account_ids if not SF_ID_PATTERN.match(a or "")]
    if bad:
        raise SystemExit(f"Keine gueltigen SF-Ids in sf_account_id: {bad[:10]}")

    found: set[str] = set()
    for chunk in chunked(account_ids, 500):
        ids = ",".join(f"'{a}'" for a in chunk)
        soql = (
            "SELECT Id FROM Account "
            f"WHERE Id IN ({ids}) AND PersonMailingStreet != null"
        )
        for rec in sf.query_all(soql).get("records", []):
            found.add(rec["Id"])
    return found


def save_skipped_rows(rows: list[dict], batch_id: str, run_stamp: str) -> None:
    out_path = REPO_ROOT / f"local_data/skipped_billing_backfill_{batch_id}_{run_stamp}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "row_id": r.get("row_id"),
            "sf_account_id": r.get("sf_account_id"),
            "email": r.get("email"),
            "reason": "PersonMailingStreet inzwischen live belegt",
        }
        for r in rows
    ]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  -> {len(rows)} uebersprungene Records gespeichert: {out_path}")


def save_failed_records(failed: list[dict], job_index: int, batch_id: str) -> None:
    out_path = REPO_ROOT / f"local_data/failed_billing_backfill_{batch_id}_job_{job_index}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  -> {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def mark_billing_processed(db: MySQLClient, batch_id: str, row_ids: list[int]) -> None:
    if not row_ids:
        return
    # In Schueben, damit das SQL-Statement bei 5.000 Ids nicht explodiert.
    for chunk in chunked(row_ids, 1000):
        placeholders = ",".join(["%s"] * len(chunk))
        sql = f"""
            UPDATE crm_imp_person_accounts
            SET    _billing_processed_at = NOW()
            WHERE  _batch_id = %s
              AND  row_id    IN ({placeholders})
        """
        db.execute(sql, [batch_id, *chunk])


# --- Main ---
def main():
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("Aufruf: update_mailing_addresses.py <batch_id> [--dry-run]")
    batch_id = args[0]

    print(f"update_mailing_addresses | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> batch_id: {batch_id}{' | DRY-RUN' if dry_run else ''}")

    # 1. Daten aus MySQL laden
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id             = %s
          AND  _operation            = 'update'
          AND  _excluded             = 0
          AND  _billing_processed_at IS NULL
          AND  sf_account_id         IS NOT NULL
          AND  address               IS NOT NULL
          AND  address               <> ''
        """,
        (batch_id,),
    )
    print(f"  -> {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  -> Keine Records zu verarbeiten. Ende.")
        return

    # 2. Matchback-Eindeutigkeit VOR allem anderen: die Bulk-Antwort traegt
    #    keine row_id, bei Account-Doubletten wuerde die falsche Zeile markiert.
    doubles = sorted(
        a for a, n in Counter(str(r["sf_account_id"]) for r in rows).items() if n > 1
    )
    if doubles:
        raise SystemExit(f"sf_account_id mehrfach im Batch, Abbruch: {doubles[:10]}")

    if dry_run:
        # Kein SF-Kontakt: CSVs je Feld-Signatur bauen und wegschreiben. Der
        # Live-Skip-Check laeuft nur im echten Lauf, direkt vor dem Update.
        records = [row_to_sf_record(row) for row in rows]
        groups = group_by_signature(records)
        out_dir = REPO_ROOT / "local_data"
        out_dir.mkdir(parents=True, exist_ok=True)
        for gi, (sig, recs) in enumerate(sorted(groups.items(), key=lambda kv: -len(kv[1])), 1):
            out_path = out_dir / f"dry_run_billing_backfill_{batch_id}_sig{gi}.csv"
            out_path.write_text(records_to_csv(recs), encoding="utf-8")
            print(f"  -> DRY-RUN Gruppe {gi}: {len(recs):,} Records, Felder {list(sig)}")
            print(f"     CSV: {out_path}")
        print(f"  -> DRY-RUN gesamt: {len(records):,} Records in {len(groups)} Signatur-Gruppen")
        return

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        # 4. Live-Skip-Check: Accounts, deren PersonMailingStreet seit dem
        #    Spiegel-Stand belegt wurde, nicht anfassen (Entscheidung: nur
        #    schreiben, wo das Ziel leer ist). Uebersprungene Zeilen behalten
        #    _billing_processed_at = NULL und tauchen im skipped_*-File auf.
        run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print("  -> Live-Check: PersonMailingStreet bereits belegt?")
        taken = accounts_with_mailing_street(sf, [str(r["sf_account_id"]) for r in rows])
        if taken:
            skipped = [r for r in rows if str(r["sf_account_id"]) in taken]
            rows = [r for r in rows if str(r["sf_account_id"]) not in taken]
            print(f"  -> {len(skipped)} Records uebersprungen (Ziel nicht mehr leer)")
            save_skipped_rows(skipped, batch_id, run_stamp)

        if not rows:
            print("  -> Nach dem Live-Check bleibt nichts zu schreiben. Ende.")
            return

        records = [row_to_sf_record(row) for row in rows]
        id_to_row_id = {str(row["sf_account_id"]): row["row_id"] for row in rows}
        groups = group_by_signature(records)
        print(f"  -> {len(records):,} Records in {len(groups)} Signatur-Gruppen werden geschrieben")

        total_failed = []
        succeeded_row_ids: list[int] = []
        batch_errors = 0
        job_index = 0
        n_jobs = sum((len(recs) + BATCH_SIZE - 1) // BATCH_SIZE for recs in groups.values())

        for sig, group_records in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            for batch_records in chunked(group_records, BATCH_SIZE):
                job_index += 1
                print(f"\nJob {job_index}/{n_jobs} | {len(batch_records)} Records | Felder: {list(sig)}")

                try:
                    job_id = sf.bulk_create_update_job(OBJECT_NAME)
                    print(f"  -> Job erstellt: {job_id}")

                    csv_data = records_to_csv(batch_records)
                    sf.bulk_upload_csv(job_id, csv_data)

                    sf.bulk_close_job(job_id)
                    print("  -> Job geschlossen (UploadComplete)")

                    status_json = poll_job(sf, job_id)

                    # 'Aborted' ist ein Fehlschlag, aber successfulResults zuerst
                    # lesen: teilweise geschriebene Records muessen markiert werden.
                    job_failed = status_json.get("state") in ("Failed", "Aborted")
                    if job_failed:
                        logging.error(f"Job {job_index} | {job_id} state {status_json.get('state')}: {status_json}")

                    job_row_ids: list[int] = []
                    if status_json.get("numberRecordsProcessed", 0) > status_json.get("numberRecordsFailed", 0):
                        succeeded = fetch_successful_records(sf, job_id)
                        for s in succeeded:
                            acc_id = s.get("sf__Id") or s.get("Id")
                            row_id = id_to_row_id.get(str(acc_id))
                            if row_id is not None:
                                job_row_ids.append(row_id)

                    # Writeback je Job, nicht erst am Ende: bricht der Lauf ab,
                    # steht in MySQL, was in SF wirklich schon geschrieben ist.
                    if job_row_ids:
                        mark_billing_processed(db, batch_id, job_row_ids)
                        succeeded_row_ids.extend(job_row_ids)
                        print(f"  -> {len(job_row_ids)} Records als verarbeitet markiert")

                    # failedResults auch bei Failed/Aborted lesen: ohne die
                    # Fehlermeldungen je Record ist ein halb geschriebener Job
                    # nicht diagnostizierbar.
                    if job_failed or status_json.get("numberRecordsFailed", 0) > 0:
                        failed = fetch_failed_records(sf, job_id)
                        if failed:
                            save_failed_records(failed, job_index, batch_id)
                            total_failed.extend(failed)
                            logging.error(f"Job {job_index} | {job_id} | {len(failed)} Fehler")

                    if job_failed:
                        batch_errors += 1

                except Exception as e:
                    logging.exception(f"Job {job_index} | Unerwarteter Fehler")
                    print(f"  x Job {job_index} Fehler: {e}")
                    batch_errors += 1

    print(f"\nupdate_mailing_addresses | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> Erfolgreich:    {len(succeeded_row_ids):,}")
    print(f"  -> Fehlgeschlagen: {len(total_failed):,}")

    if total_failed:
        all_failed_path = REPO_ROOT / f"local_data/batch/all_failed_billing_backfill_{batch_id}.json"
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  -> Alle Fehler gespeichert: {all_failed_path}")

    # Exit != 0 bei jedem Problem: der Notebook-Runner prueft nur den
    # Returncode und darf nach einem Teilerfolg nicht einfach weiterlaufen.
    if total_failed or batch_errors:
        raise SystemExit(
            f"Lauf unvollstaendig: {len(total_failed)} Record-Fehler, "
            f"{batch_errors} Job-Fehler"
        )


if __name__ == "__main__":
    main()
