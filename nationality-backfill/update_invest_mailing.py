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

# Alle Artefakte relativ zum Repo-Root, nicht zum cwd.
REPO_ROOT = Path(__file__).resolve().parents[1]

# --- Logging Setup ---
log_path = REPO_ROOT / "logs/update_invest_mailing.log"
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
OBJECT_NAME = "Account"
# ~6k Records gesamt: die ueblichen 5.000 je Job reichen, kein Grund fuer die
# 20k-Jobs des populationsweiten Billing-Backfills.
BATCH_SIZE = 5000
POLL_INTERVAL_SEC = 10
MAX_POLL_ATTEMPTS = 180
MAX_CONSECUTIVE_JOB_ERRORS = 3

# Staging-Spalte -> Salesforce-Feld. `country` traegt hier den ENGLISCHEN
# LAENDERNAMEN ("Austria"), nicht den ISO-2-Code - gemappt beim Staging ueber
# country_names.COUNTRY_NAMES (siehe 06_stage_mailing_country.ipynb).
# Population A belegt alle vier Felder, B und C nur `country`;
# group_by_signature trennt die beiden Formen automatisch in eigene Jobs.
FIELD_MAP = {
    "address":     "PersonMailingStreet",
    "city":        "PersonMailingCity",
    "postal_code": "PersonMailingPostalCode",
    "country":     "PersonMailingCountry",
}

# Population C ueberschreibt einen VORHANDENEN Wert (ISO-2-Code -> Name).
# Der Live-Skip-Check unterscheidet deshalb nach `source`:
#   invest_mailing_A / _B: Ziel-Block muss live noch komplett leer sein.
#   invest_mailing_C:      live PersonMailingCountry muss noch exakt der Code
#                          sein, aus dem wir konvertiert haben
#                          (_mailing_prev_country) - wurde er editiert, Finger weg.
SOURCE_A = "invest_mailing_A"
SOURCE_B = "invest_mailing_B"
SOURCE_C = "invest_mailing_C"


def row_to_sf_record(row: dict) -> dict:
    """
    Mappt eine Staging-Zeile auf ein Account-Update per Id. Leere/NULL-Felder
    werden ENTFERNT: group_by_signature garantiert, dass nie eine leere Zelle
    im CSV steht (Bulk 2.0 wuerde sie ignorieren, SOAP-Wege wuerden loeschen).
    """
    record = {"Id": row.get("sf_account_id")}
    for col, sf_field in FIELD_MAP.items():
        v = row.get(col)
        if v is not None and str(v).strip() != "":
            record[sf_field] = v
    return record


def group_by_signature(records: list[dict]) -> dict[tuple, list[dict]]:
    """
    Gruppiert Records nach der Menge ihrer belegten Felder; jede Gruppe wird
    als eigener Bulk-Job mit exakt diesen Spalten geschickt.
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

    for r in records:
        if set(r) != set(all_keys):
            raise SystemExit(f"Record mit abweichenden Spalten im CSV: {sorted(r)} vs {all_keys}")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, lineterminator="\n")
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


def fetch_live_mailing(sf: SalesforceClientCC, account_ids: list[str]) -> dict[str, dict]:
    """
    Live-Stand der vier Mailing-Felder je Account. Basis fuer den
    source-abhaengigen Skip-Check; der Spiegel kann Stunden alt sein,
    die Wahrheit liegt in Salesforce.
    """
    bad = [a for a in account_ids if not SF_ID_PATTERN.match(a or "")]
    if bad:
        raise SystemExit(f"Keine gueltigen SF-Ids in sf_account_id: {bad[:10]}")

    # 400 Ids je Chunk: die SOQL geht als GET raus, bei 500 Ids liegt die URI
    # schon nahe an der 16.384-Zeichen-Grenze von Salesforce.
    live: dict[str, dict] = {}
    for chunk in chunked(account_ids, 400):
        ids = ",".join(f"'{a}'" for a in chunk)
        soql = (
            "SELECT Id, PersonMailingStreet, PersonMailingCity, "
            "PersonMailingPostalCode, PersonMailingCountry "
            f"FROM Account WHERE Id IN ({ids})"
        )
        for rec in sf.query_all(soql).get("records", []):
            live[rec["Id"]] = rec
    return live


def skip_reason(row: dict, live_rec: dict | None) -> str | None:
    """
    None = schreiben. Sonst der Grund, warum die Zeile uebersprungen wird.
    """
    if live_rec is None:
        return "Account live nicht gefunden (geloescht/gemerged?)"

    def filled(field: str) -> bool:
        v = live_rec.get(field)
        return v is not None and str(v).strip() != ""

    src = row.get("source")
    if src in (SOURCE_A, SOURCE_B):
        taken = [f for f in ("PersonMailingStreet", "PersonMailingCity",
                             "PersonMailingPostalCode", "PersonMailingCountry") if filled(f)]
        if taken:
            return f"Mailing-Block inzwischen belegt: {', '.join(taken)}"
        return None

    if src == SOURCE_C:
        prev = (row.get("_mailing_prev_country") or "").strip()
        live_val = (live_rec.get("PersonMailingCountry") or "").strip()
        if live_val.upper() != prev.upper():
            return (f"PersonMailingCountry live '{live_val}' != gestagter "
                    f"Ausgangscode '{prev}' (editiert seit dem Spiegel)")
        return None

    return f"Unbekannte source '{src}' - Zeile wird nicht angefasst"


def write_json(out_path: Path, payload) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def save_skipped_rows(skipped: list[tuple[dict, str]], batch_id: str, run_stamp: str) -> None:
    out_path = REPO_ROOT / f"local_data/skipped_invest_mailing_{batch_id}_{run_stamp}.json"
    payload = [
        {
            "row_id": r.get("row_id"),
            "sf_account_id": r.get("sf_account_id"),
            "email": r.get("email"),
            "source": r.get("source"),
            "reason": reason,
        }
        for r, reason in skipped
    ]
    write_json(out_path, payload)
    print(f"  -> {len(skipped)} uebersprungene Records gespeichert: {out_path}")


def save_failed_records(failed: list[dict], job_index: int, batch_id: str) -> None:
    out_path = REPO_ROOT / f"local_data/failed_invest_mailing_{batch_id}_job_{job_index}.json"
    write_json(out_path, failed)
    print(f"  -> {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def mark_mailing_processed(db: MySQLClient, batch_id: str, row_ids: list[int]) -> None:
    if not row_ids:
        return
    # Eine Transaktion je Job: entweder ist der ganze Job-Writeback drin
    # oder gar nicht.
    with db.transaction() as conn:
        for chunk in chunked(row_ids, 1000):
            placeholders = ",".join(["%s"] * len(chunk))
            sql = f"""
                UPDATE crm_imp_person_accounts
                SET    _mailing_processed_at = NOW()
                WHERE  _batch_id = %s
                  AND  row_id    IN ({placeholders})
            """
            db.execute(sql, [batch_id, *chunk], conn=conn)


def mark_skipped_excluded(db: MySQLClient, batch_id: str, row_ids: list[int], run_stamp: str) -> None:
    """
    Live uebersprungene Zeilen als _excluded markieren, damit sie aus der
    offenen Menge verschwinden - sonst waere still_open = 0 nie erreichbar.
    """
    if not row_ids:
        return
    with db.transaction() as conn:
        for chunk in chunked(row_ids, 1000):
            placeholders = ",".join(["%s"] * len(chunk))
            sql = f"""
                UPDATE crm_imp_person_accounts
                SET    _excluded = 1,
                       _exclude_reason = %s
                WHERE  _batch_id = %s
                  AND  row_id    IN ({placeholders})
            """
            db.execute(sql, [f"live_drift_{run_stamp}", batch_id, *chunk], conn=conn)


# --- Main ---
def main():
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("Aufruf: update_invest_mailing.py <batch_id> [--dry-run]")
    batch_id = args[0]

    print(f"update_invest_mailing | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> batch_id: {batch_id}{' | DRY-RUN' if dry_run else ''}")

    # 1. Daten aus MySQL laden
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        """
        SELECT row_id, sf_account_id, email, source,
               address, city, postal_code, country, _mailing_prev_country
        FROM   crm_imp_person_accounts
        WHERE  _batch_id             = %s
          AND  _operation            = 'update'
          AND  _excluded             = 0
          AND  _mailing_processed_at IS NULL
          AND  sf_account_id         IS NOT NULL
          AND  (   (country IS NOT NULL AND country <> '')
                OR (address IS NOT NULL AND address <> ''))
        """,
        (batch_id,),
    )
    print(f"  -> {len(rows)} Records aus dem Batch geladen")
    by_source = Counter(r["source"] for r in rows)
    for src, n in sorted(by_source.items()):
        print(f"     {src}: {n:,}")

    if not rows:
        print("  -> Keine Records zu verarbeiten. Ende.")
        return

    # Der Name im country-Feld darf nie ein nackter ISO-2-Code sein - das
    # waere ein Staging-Fehler (Mapping nicht angewandt). A-Zeilen ohne
    # Billing-Code haben legitim KEIN country (Block ohne Land).
    bare_codes = [r for r in rows
                  if r["country"] and re.fullmatch(r"[A-Za-z]{2}", str(r["country"]).strip())]
    if bare_codes:
        raise SystemExit(
            f"{len(bare_codes)} Zeilen tragen einen ISO-2-Code statt eines Namens "
            f"in country (z.B. row_id {bare_codes[0]['row_id']}), Abbruch"
        )

    # 2. Matchback-Eindeutigkeit VOR allem anderen: die Bulk-Antwort traegt
    #    keine row_id, bei Account-Doubletten wuerde die falsche Zeile markiert.
    doubles = sorted(
        a for a, n in Counter(str(r["sf_account_id"]) for r in rows).items() if n > 1
    )
    if doubles:
        raise SystemExit(f"sf_account_id mehrfach im Batch, Abbruch: {doubles[:10]}")

    if dry_run:
        # Kein SF-Kontakt: CSVs je Feld-Signatur bauen und wegschreiben.
        records = [row_to_sf_record(row) for row in rows]
        groups = group_by_signature(records)
        out_dir = REPO_ROOT / "local_data"
        out_dir.mkdir(parents=True, exist_ok=True)
        for gi, (sig, recs) in enumerate(sorted(groups.items(), key=lambda kv: -len(kv[1])), 1):
            out_path = out_dir / f"dry_run_invest_mailing_{batch_id}_sig{gi}.csv"
            out_path.write_text(records_to_csv(recs), encoding="utf-8")
            print(f"  -> DRY-RUN Gruppe {gi}: {len(recs):,} Records, Felder {list(sig)}")
            print(f"     CSV: {out_path}")
        print(f"  -> DRY-RUN gesamt: {len(records):,} Records in {len(groups)} Signatur-Gruppen")
        return

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        # 4. Live-Skip-Check, source-abhaengig (siehe skip_reason).
        run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print("  -> Live-Check: Mailing-Felder gegen den aktuellen SF-Stand")
        live = fetch_live_mailing(sf, [str(r["sf_account_id"]) for r in rows])

        skipped: list[tuple[dict, str]] = []
        writable: list[dict] = []
        for r in rows:
            reason = skip_reason(r, live.get(str(r["sf_account_id"])))
            if reason is None:
                writable.append(r)
            else:
                skipped.append((r, reason))

        if skipped:
            print(f"  -> {len(skipped)} Records uebersprungen (Live-Drift)")
            save_skipped_rows(skipped, batch_id, run_stamp)
            mark_skipped_excluded(db, batch_id, [r["row_id"] for r, _ in skipped], run_stamp)
        rows = writable

        if not rows:
            print("  -> Nach dem Live-Check bleibt nichts zu schreiben. Ende.")
            return

        records = [row_to_sf_record(row) for row in rows]
        id_to_row_id = {str(row["sf_account_id"]): row["row_id"] for row in rows}
        groups = group_by_signature(records)
        print(f"  -> {len(records):,} Records in {len(groups)} Signatur-Gruppen werden geschrieben")

        total_failed = []
        n_succeeded = 0
        batch_errors = 0
        consecutive_errors = 0
        job_index = 0
        n_jobs = sum((len(recs) + BATCH_SIZE - 1) // BATCH_SIZE for recs in groups.values())

        for sig, group_records in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            for batch_records in chunked(group_records, BATCH_SIZE):
                job_index += 1
                print(f"\nJob {job_index}/{n_jobs} | {len(batch_records)} Records | Felder: {list(sig)}")

                if consecutive_errors >= MAX_CONSECUTIVE_JOB_ERRORS:
                    raise SystemExit(
                        f"{consecutive_errors} Job-Fehler in Folge - systemisches Problem "
                        f"(Berechtigung? Token? Validation Rule?), Abbruch vor Job {job_index}"
                    )

                try:
                    # Tokens laufen nach ~2h ab: vor jedem Job frisch anmelden.
                    sf.authenticate()
                    job_id = sf.bulk_create_update_job(OBJECT_NAME)
                    print(f"  -> Job erstellt: {job_id}")

                    csv_data = records_to_csv(batch_records)
                    sf.bulk_upload_csv(job_id, csv_data)

                    sf.bulk_close_job(job_id)
                    print("  -> Job geschlossen (UploadComplete)")

                    status_json = poll_job(sf, job_id)

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

                    # Writeback je Job, nicht erst am Ende.
                    if job_row_ids:
                        mark_mailing_processed(db, batch_id, job_row_ids)
                        n_succeeded += len(job_row_ids)
                        print(f"  -> {len(job_row_ids)} Records als verarbeitet markiert")

                    if job_failed or status_json.get("numberRecordsFailed", 0) > 0:
                        failed = fetch_failed_records(sf, job_id)
                        if failed:
                            save_failed_records(failed, job_index, batch_id)
                            total_failed.extend(failed)
                            logging.error(f"Job {job_index} | {job_id} | {len(failed)} Fehler")

                    if job_failed:
                        batch_errors += 1
                        consecutive_errors += 1
                    else:
                        consecutive_errors = 0

                except Exception as e:
                    logging.exception(f"Job {job_index} | Unerwarteter Fehler")
                    print(f"  x Job {job_index} Fehler: {e}")
                    batch_errors += 1
                    consecutive_errors += 1

    print(f"\nupdate_invest_mailing | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> Erfolgreich:    {n_succeeded:,}")
    print(f"  -> Fehlgeschlagen: {len(total_failed):,}")

    if total_failed:
        all_failed_path = REPO_ROOT / f"local_data/batch/all_failed_invest_mailing_{batch_id}.json"
        write_json(all_failed_path, total_failed)
        print(f"  -> Alle Fehler gespeichert: {all_failed_path}")

    if total_failed or batch_errors:
        raise SystemExit(
            f"Lauf unvollstaendig: {len(total_failed)} Record-Fehler, "
            f"{batch_errors} Job-Fehler"
        )


if __name__ == "__main__":
    main()
