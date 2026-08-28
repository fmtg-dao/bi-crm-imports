import sys
from pathlib import Path

# config, mysql_client und salesforce_client_prod liegen im Repo-Root.
# Per-Job-Kopie von invest-consent/insert_consents_invest.py (Pre-Reg-Lauf 2026-08):
# identischer Purpose (invest_central), identisches Payload — nur Artefakt-Namen
# und Batch-Ids unterscheiden sich.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, timezone
import json
import csv
import io
import re
import time as time_module
import logging
from collections import Counter

# Alle Artefakte relativ zum Repo-Root, nicht zum cwd: der Notebook-Runner und
# ein Direktaufruf aus invest-consent/ muessen in denselben Ordnern landen.
REPO_ROOT = Path(__file__).resolve().parents[1]

# --- Logging Setup ---
log_path = REPO_ROOT / "logs/insert_consents_prereg.log"
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

# --- Consent-Konstanten für DIESEN Lauf ---
# Zentraler Purpose: Name lowercase wie die bestehenden marketing_central-Records,
# ConsentKey ohne Property (dritter Slot = 'CENTRAL'), kein Property__c/HotelName__c/
# Region__c, kein CaptureContactPointType (D5/D6, phase-1-decision-memo.md).
CONSENT_NAME        = "invest_central"
DATA_USE_PURPOSE_ID = "0ZWTe0000000X5dOAE"   # invest_central (NICHT der typo 'marekting_property')
CONSENT_FLAG_COLUMN = "consent_invest"

# PrivacyConsentStatus ist konstant pro Lauf und kommt aus dem Batch-Suffix:
# *_optin -> OptIn, *_optout -> OptOut. Kein Default, kein CLI-Freitext —
# ein vertauschter Status wäre eine falsche Einwilligung, kein Tippfehler.
BATCH_SUFFIX_TO_STATUS = {"_optin": "OptIn", "_optout": "OptOut"}

# Abweichung zum August-Lauf: dort war die OptOut-Population eine Exclusion-Liste
# mit consent_invest=1. Im Pre-Reg-Lauf kommt der Status aus der CSV selbst —
# der Flag-Filter muss also zum Suffix passen (optin: =1, optout: =0), sonst
# lädt ein OptOut-Batch null Zeilen.
STATUS_TO_FLAG_VALUE = {"OptIn": 1, "OptOut": 0}


def sf_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def row_to_sf_record(row: dict, status: str, now: str) -> dict:
    """
    Mappt eine Zeile aus crm_imp_person_accounts auf einen
    ContactPointConsent-Insert. ContactPointId (= sf_cp_email_id) dient als
    Matchback-Key (1:1 Consent je CPE/Purpose/Lauf). None-Felder werden entfernt.
    """
    record = {
        "Name":                 CONSENT_NAME,
        "ContactPointId":       row.get("sf_cp_email_id"),
        "DataUsePurposeId":     DATA_USE_PURPOSE_ID,
        "PrivacyConsentStatus": status,
        "CaptureDate":          now,
        "EffectiveFrom":        now,
        "ConsentKey__c":        f"{row.get('sf_cp_email_id')}|{DATA_USE_PURPOSE_ID}|CENTRAL",
        # Nicht row.get("source") ('Migration'): Consents sollen wie die Accounts
        # (SourceOrigin__pc) auf das Conda-Exportsystem zurückführbar sein
        # (User-Entscheid 2026-08-28).
        "CaptureSource":        "conda-pre-reg",
        "SourceSystem__c":      "conda-pre-reg",
    }
    return {k: v for k, v in record.items() if v is not None}


def records_to_csv(records: list[dict]) -> str:
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


def existing_consents(sf: SalesforceClientCC, cp_ids: list[str]) -> dict[str, list[dict]]:
    """
    Vorhandene Consents für DIESEN Purpose, je ContactPointId. Purpose-only,
    kein Property-Filter: invest_central ist zentral. Gibt die ganzen Records
    zurück, ein Mensch muss OptIn-Duplikat und OptOut-Konflikt unterscheiden.
    """
    bad = [c for c in cp_ids if not SF_ID_PATTERN.match(c or "")]
    if bad:
        raise SystemExit(f"Keine gueltigen SF-Ids in sf_cp_email_id: {bad[:10]}")

    found: dict[str, list[dict]] = {}
    for chunk in chunked(cp_ids, 200):
        ids = ",".join(f"'{c}'" for c in chunk)
        soql = (
            "SELECT Id, ContactPointId, PrivacyConsentStatus, CaptureDate, "
            "EffectiveFrom, EffectiveTo FROM ContactPointConsent "
            f"WHERE DataUsePurposeId = '{DATA_USE_PURPOSE_ID}' "
            f"AND ContactPointId IN ({ids})"
        )
        for rec in sf.query_all(soql).get("records", []):
            rec.pop("attributes", None)
            found.setdefault(rec["ContactPointId"], []).append(rec)
    return found


def save_skipped_rows(rows: list[dict], found: dict[str, list[dict]], batch_id: str, run_stamp: str) -> None:
    out_path = REPO_ROOT / f"local_data/skipped_prereg_consents_{batch_id}_{run_stamp}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "row_id": r.get("row_id"),
            "email": r.get("email"),
            "sf_cp_email_id": r.get("sf_cp_email_id"),
            "existing_consents": found.get(str(r.get("sf_cp_email_id")), []),
        }
        for r in rows
    ]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  -> {len(rows)} übersprungene Records gespeichert: {out_path}")


def save_failed_records(failed: list[dict], batch_index: int, batch_id: str) -> None:
    out_path = REPO_ROOT / f"local_data/failed_prereg_consents_{batch_id}_batch_{batch_index}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  -> {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def mark_consent_processed(db: MySQLClient, batch_id: str, row_ids: list[int]) -> None:
    if not row_ids:
        return
    placeholders = ",".join(["%s"] * len(row_ids))
    sql = f"""
        UPDATE crm_imp_person_accounts
        SET    _consent_processed_at = NOW()
        WHERE  _batch_id = %s
          AND  row_id    IN ({placeholders})
    """
    db.execute(sql, [batch_id, *row_ids])


# --- Main ---
def main():
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry_run = "--dry-run" in sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("Aufruf: insert_consents_prereg.py <batch_id> [--dry-run]")
    batch_id = args[0]

    status = next(
        (s for suffix, s in BATCH_SUFFIX_TO_STATUS.items() if batch_id.endswith(suffix)),
        None,
    )
    if status is None:
        raise SystemExit(f"Batch-Id muss auf _optin oder _optout enden: {batch_id}")

    print(f"insert_consents_prereg | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> batch_id: {batch_id} | purpose: {CONSENT_NAME} | status: {status}"
          f"{' | DRY-RUN' if dry_run else ''}")

    # 1. Daten aus MySQL laden
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        f"""
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id             = %s
          AND  _excluded             = 0
          AND  sf_cp_email_id        IS NOT NULL
          AND  {CONSENT_FLAG_COLUMN} = %s
          AND  _consent_processed_at IS NULL
        """,
        (batch_id, STATUS_TO_FLAG_VALUE[status]),
    )
    print(f"  -> {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  -> Keine Records zu verarbeiten. Ende.")
        return

    # 2. Matchback-Eindeutigkeit VOR allem anderen: die Bulk-Antwort traegt
    #    keine row_id, bei CPE-Doubletten wuerde die falsche Zeile markiert.
    doubles = sorted(
        cp for cp, n in Counter(str(r["sf_cp_email_id"]) for r in rows).items() if n > 1
    )
    if doubles:
        raise SystemExit(f"sf_cp_email_id mehrfach im Batch, Abbruch: {doubles[:10]}")

    now = sf_datetime(datetime.now(timezone.utc))
    if dry_run:
        # Kein SF-Kontakt: CSV bauen, wegschreiben, Zeilen zaehlen. Der
        # Vorab-Skip-Check laeuft nur im echten Lauf, direkt vor dem Insert.
        records = [row_to_sf_record(row, status, now) for row in rows]
        csv_data = records_to_csv(records)
        out_path = REPO_ROOT / f"local_data/dry_run_prereg_consents_{batch_id}.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(csv_data, encoding="utf-8")
        print(f"  -> DRY-RUN: {len(records)} Records, CSV: {out_path}")
        return

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        # 4. Zeilen aussortieren, die für diesen Purpose schon einen Consent
        #    haben. Ohne diese Prüfung legt ein zweiter Lauf Duplikate an und
        #    ein bestehendes OptOut bekommt zusätzlich ein OptIn. Übersprungene
        #    Zeilen behalten _consent_processed_at = NULL.
        run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        found = existing_consents(sf, [str(r["sf_cp_email_id"]) for r in rows])
        status_conflicts = 0
        if found:
            skipped = [r for r in rows if str(r["sf_cp_email_id"]) in found]
            rows = [r for r in rows if str(r["sf_cp_email_id"]) not in found]
            optouts = sum(
                1 for recs in found.values()
                if any(rec["PrivacyConsentStatus"] == "OptOut" for rec in recs)
            )
            # Skip ist nur dann harmlos, wenn der vorhandene Status dem
            # gewuenschten entspricht. Ein OptOut-Batch-Row mit vorhandenem
            # OptIn (oder umgekehrt) ist ein KONFLIKT: die Person behaelt den
            # falschen Status. Wird gemeldet und der Lauf endet mit Exit 1.
            status_conflicts = sum(
                1 for recs in found.values()
                if not any(rec["PrivacyConsentStatus"] == status for rec in recs)
            )
            print(f"  -> {len(skipped)} Records übersprungen, davon {optouts} mit OptOut")
            if status_conflicts:
                print(f"  x  {status_conflicts} Skips mit ABWEICHENDEM Status "
                      f"(erwartet {status}) - siehe skipped_*-Datei, manuell klaeren!")
            save_skipped_rows(skipped, found, batch_id, run_stamp)

        if not rows:
            print("  -> Nach der Prüfung bleibt nichts zu schreiben. Ende.")
            if status_conflicts:
                raise SystemExit(f"{status_conflicts} Status-Konflikte, siehe skipped_*-Datei")
            return

        records = [row_to_sf_record(row, status, now) for row in rows]
        cpid_to_row_id = {str(row["sf_cp_email_id"]): row["row_id"] for row in rows}
        print(f"  -> {len(records)} Records werden geschrieben")

        total_failed = []
        succeeded_row_ids: list[int] = []
        batch_errors = 0

        for i, start in enumerate(range(0, len(records), BATCH_SIZE)):
            batch_records = records[start : start + BATCH_SIZE]
            print(f"\nBatch {i+1}/{(len(records) + BATCH_SIZE - 1) // BATCH_SIZE} | "
                  f"{len(batch_records)} Records")

            try:
                job_id = sf.bulk_create_insert_job(OBJECT_NAME)
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
                    logging.error(f"Batch {i+1} | Job {job_id} state {status_json.get('state')}: {status_json}")

                batch_row_ids: list[int] = []
                if status_json.get("numberRecordsProcessed", 0) > status_json.get("numberRecordsFailed", 0):
                    succeeded = fetch_successful_records(sf, job_id)
                    for s in succeeded:
                        cp_id  = s.get("ContactPointId")
                        row_id = cpid_to_row_id.get(str(cp_id))
                        if row_id is not None:
                            batch_row_ids.append(row_id)

                # Writeback je Batch, nicht erst am Ende: bricht der Lauf ab,
                # steht in MySQL, was in SF wirklich schon existiert.
                if batch_row_ids:
                    mark_consent_processed(db, batch_id, batch_row_ids)
                    succeeded_row_ids.extend(batch_row_ids)
                    print(f"  -> {len(batch_row_ids)} Records als verarbeitet markiert")

                # failedResults auch bei Failed/Aborted lesen: ohne die
                # Fehlermeldungen je Record ist ein halb geschriebener Job
                # nicht diagnostizierbar.
                if job_failed or status_json.get("numberRecordsFailed", 0) > 0:
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

    print(f"\ninsert_consents_prereg | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  -> Erfolgreich:           {len(succeeded_row_ids)}")
    print(f"  -> Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = REPO_ROOT / f"local_data/batch/all_failed_prereg_consents_{batch_id}.json"
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  -> Alle Fehler gespeichert: {all_failed_path}")

    # Exit != 0 bei jedem Problem: der Notebook-Runner prueft nur den
    # Returncode und darf nach einem Teilerfolg nicht einfach weiterlaufen.
    if total_failed or batch_errors or status_conflicts:
        raise SystemExit(
            f"Lauf unvollstaendig: {len(total_failed)} Record-Fehler, "
            f"{batch_errors} Batch-Fehler, {status_conflicts} Status-Konflikte"
        )


if __name__ == "__main__":
    main()
