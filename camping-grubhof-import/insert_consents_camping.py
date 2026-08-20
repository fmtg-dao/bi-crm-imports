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
import re
import time as time_module
import logging
from collections import Counter

# --- Logging Setup ---
log_path = Path("logs/insert_contact_point_consents_bulk.log")
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

# Steuert, welcher Batch aus crm_imp_person_accounts gezogen wird.
# Wird per CLI-Arg überschreibbar gemacht (siehe main()).
DEFAULT_BATCH_ID = "2026-08-10_new_camping_import"

# --- Consent-Konstanten für DIESEN Lauf ---
# WICHTIG: Pro Lauf genau EIN Purpose. Für andere Purposes (residences, invest)
# müssen ALLE DREI zusammen geändert werden, sonst stimmt Status nicht zum Flag:
#   CONSENT_NAME, DATA_USE_PURPOSE_ID  und  CONSENT_FLAG_COLUMN
# Camping Grubhof (Entscheidung Christoph Crepaz 2026-08-11: KEIN central consent,
# nur property consent): Name/Casing wie die bestehenden apaleo-Migrationsconsents.
CONSENT_NAME           = "Marketing_Property"
DATA_USE_PURPOSE_ID    = "0ZWTe0000000X8rOAE"   # marketing_property (NICHT der typo 'marekting_property' 0ZWTe0000000ZyfOAE)
PROPERTY_ID            = "a0QTe00000La2dVMAR"   # Camping Grubhof (crm_properties_sfid_prod, ApaleoID FCG)
HOTEL_NAME             = "Camping Grubhof"      # HotelName__c - denormalisierter Text, Spalte "Hotel Name" in der UI
REGION                 = "Saalachtal"           # Region__c    - denormalisiert von der Property, wie Migration
PRIVACY_CONSENT_STATUS = "OptIn"

# Spalte in crm_imp_person_accounts, die die Einwilligung für DIESEN Purpose hält.
# Nur Zeilen mit = 1 bekommen ein OptIn — sonst würden wir Einwilligungen
# für Personen schreiben, die nie zugestimmt haben.
CONSENT_FLAG_COLUMN    = "consent_camping"


# --- Helpers ---
def sf_datetime(value: Optional[Union[datetime, date]]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min, tzinfo=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def row_to_sf_record(row: dict) -> dict:
    """
    Mappt eine Zeile aus crm_imp_person_accounts auf einen
    Salesforce ContactPointConsent-Insert.

    Insert: SF vergibt die Id, External-IDs gibt es nicht.
    ContactPointId (= sf_cp_email_id) dient als Matchback-Key (1:1 Consent je CPE/Purpose/Lauf).
    None-Felder werden am Ende entfernt.
    """
    now = sf_datetime(datetime.now(timezone.utc))

    record = {
        "Name":                 CONSENT_NAME,
        "ContactPointId":       row.get("sf_cp_email_id"),   # CPE des Contacts/Accounts
        "DataUsePurposeId":     DATA_USE_PURPOSE_ID,
        "PrivacyConsentStatus": PRIVACY_CONSENT_STATUS,
        "CaptureDate":          now,                          # today
        "CaptureSource":        row.get("source"),
        "EffectiveFrom":        now,                          # today

        # Property-Consent (Grubhof): Property__c setzen, sonst greift Carmens
        # Segmentierung (Purpose + Property) nicht. ConsentKey__c wie die
        # apaleo-Migrationsconsents (CPE|Purpose|Property) — SF baut den Key
        # NICHT automatisch (empirisch verifiziert 2026-08-17 am Test-Record
        # 0ZXTe000000YtenOAC: ohne Key eingefuegt, blieb leer).
        # HotelName__c/Region__c: denormalisierte Property-Texte wie Migration
        # (Feld-Diff gegen 0ZXTe000000017VOAQ; "Hotel Name"-Spalte in der UI).
        "Property__c":          PROPERTY_ID,
        "ConsentKey__c":        f"{row.get('sf_cp_email_id')}|{DATA_USE_PURPOSE_ID}|{PROPERTY_ID}",
        "HotelName__c":         HOTEL_NAME,
        "Region__c":            REGION,

        # --- Tracking ---
        "SourceSystem__c":      row.get("source"),
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


SF_ID_PATTERN = re.compile(r"^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$")


def existing_consents(sf: SalesforceClientCC, cp_ids: list[str]) -> dict[str, list[dict]]:
    """
    Vorhandene Consents für DIESEN Purpose+Property, je ContactPointId.

    Gibt die ganzen Records zurück, nicht nur den Status: ein Mensch muss
    OptIn-Duplikat und OptOut-Konflikt unterscheiden können.
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
            f"AND Property__c = '{PROPERTY_ID}' "
            f"AND ContactPointId IN ({ids})"
        )
        for rec in sf.query_all(soql).get("records", []):
            rec.pop("attributes", None)
            found.setdefault(rec["ContactPointId"], []).append(rec)
    return found


def save_skipped_rows(rows: list[dict], found: dict[str, list[dict]], batch_id: str, run_stamp: str) -> None:
    out_path = Path(f"local_data/skipped_contact_point_consents_{batch_id}_{run_stamp}.json")
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
    print(f"  → {len(rows)} übersprungene Records gespeichert: {out_path}")


def save_failed_records(failed: list[dict], batch_index: int, batch_id: str) -> None:
    out_path = Path(f"local_data/failed_contact_point_consents_{batch_id}_batch_{batch_index}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- DB Helpers ---
def mark_consent_processed(db: MySQLClient, batch_id: str, row_ids: list[int]) -> None:
    """
    Setzt _consent_processed_at = NOW() für erfolgreich verarbeitete Records über row_id.
    (Keine sf_consent_id-Spalte vorhanden → es wird nur der Status gesetzt.)
    """
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
    batch_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BATCH_ID

    print(f"insert_contact_point_consents_bulk | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → batch_id: {batch_id} | purpose: {CONSENT_NAME} | flag: {CONSENT_FLAG_COLUMN}")

    # 1. Daten aus MySQL laden
    #    Gate: CPE muss vorhanden sein (ContactPointId-Pflicht), Consent-Schritt offen,
    #    und NUR Zeilen mit gesetztem Einwilligungs-Flag für diesen Purpose.
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    rows = db.fetch_all(
        f"""
        SELECT *
        FROM   crm_imp_person_accounts
        WHERE  _batch_id             = %s
          AND  _excluded             = 0
          AND  sf_cp_email_id        IS NOT NULL
          AND  {CONSENT_FLAG_COLUMN} = 1
          AND  _consent_processed_at IS NULL
        """,
        (batch_id,),
    )
    print(f"  → {len(rows)} Records aus dem Batch geladen")

    if not rows:
        print("  → Keine Records zu verarbeiten. Ende.")
        return

    # 2. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        # 3. Zeilen aussortieren, die für diesen Purpose+Property schon einen
        #    Consent haben. Ohne diese Prüfung legt ein zweiter Lauf Duplikate an
        #    und ein bestehendes OptOut bekommt zusätzlich ein OptIn.
        #    Übersprungene Zeilen behalten _consent_processed_at = NULL, damit ein
        #    Mensch über sie entscheidet.
        run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        found = existing_consents(sf, [str(r["sf_cp_email_id"]) for r in rows])
        if found:
            skipped = [r for r in rows if str(r["sf_cp_email_id"]) in found]
            rows = [r for r in rows if str(r["sf_cp_email_id"]) not in found]
            optouts = sum(
                1 for recs in found.values()
                if any(rec["PrivacyConsentStatus"] == "OptOut" for rec in recs)
            )
            print(f"  → {len(skipped)} Records übersprungen, davon {optouts} mit OptOut")
            save_skipped_rows(skipped, found, batch_id, run_stamp)

        if not rows:
            print("  → Nach der Prüfung bleibt nichts zu schreiben. Ende.")
            return

        # 4. Mapping — ContactPointId (= sf_cp_email_id) als Matchback-Key zu row_id.
        #    Der Matchback ist nur eindeutig, wenn kein CPE zweimal vorkommt: die
        #    Bulk-Antwort traegt keine row_id, also wuerde bei Doubletten die falsche
        #    Zeile als verarbeitet markiert.
        doubles = sorted(
            cp for cp, n in Counter(str(r["sf_cp_email_id"]) for r in rows).items() if n > 1
        )
        if doubles:
            raise SystemExit(f"sf_cp_email_id mehrfach im Batch, Abbruch: {doubles[:10]}")

        records = [row_to_sf_record(row) for row in rows]
        cpid_to_row_id = {str(row["sf_cp_email_id"]): row["row_id"] for row in rows}
        print(f"  → {len(records)} Records werden geschrieben")

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
                print("  → Job geschlossen (UploadComplete)")

                status = poll_job(sf, job_id)

                # 'Aborted' hiess in der Ursprungsversion Erfolg: der Job wurde
                # abgebrochen, die Records galten trotzdem als geschrieben.
                if status.get("state") in ("Failed", "Aborted"):
                    logging.error(f"Batch {i+1} | Job {job_id} state {status.get('state')}: {status}")
                    continue

                # --- Erfolgreiche Records: ContactPointId → row_id ---
                batch_row_ids: list[int] = []
                if status.get("numberRecordsProcessed", 0) > status.get("numberRecordsFailed", 0):
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
                    print(f"  → {len(batch_row_ids)} Records als verarbeitet markiert")

                # --- Fehlgeschlagene Records ---
                if status.get("numberRecordsFailed", 0) > 0:
                    failed = fetch_failed_records(sf, job_id)
                    save_failed_records(failed, i + 1, batch_id)
                    total_failed.extend(failed)
                    logging.error(f"Batch {i+1} | Job {job_id} | {len(failed)} Fehler")

            except Exception as e:
                logging.exception(f"Batch {i+1} | Unerwarteter Fehler")
                print(f"  ✗ Batch {i+1} Fehler: {e}")

    print(f"\ninsert_contact_point_consents_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  → Erfolgreich:           {len(succeeded_row_ids)}")
    print(f"  → Fehlgeschlagen:        {len(total_failed)}")

    if total_failed:
        all_failed_path = Path(f"local_data/batch/all_failed_contact_point_consents_{batch_id}.json")
        all_failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(all_failed_path, "w") as f:
            json.dump(total_failed, f, indent=2)
        print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()