from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client import SalesforceClientCC, load_salesforce_cc_config_from_env
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
log_path = Path("logs/import_reservations_bulk.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --- Constants ---
BULK_API_VERSION = "v62.0"
EXTERNAL_ID_FIELD = "ReservationID__c"   # <-- External ID Feld auf Reservation__c
OBJECT_NAME = "Reservation__c"
BATCH_SIZE = 50                         # Bulk API 2.0 max: 150MB / Job, ca. 5k–10k Zeilen empfohlen
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
    """Mappt eine MySQL-Zeile auf ein Salesforce Reservation__c Record Dict."""
    record = {
        EXTERNAL_ID_FIELD:              row.get('reservation_id'),

        # --- Reservation core ---
        "BookingID__c":                 row.get('booking_id'),
        "ReservationStatus__c":         row.get("reservation_status"),
        "MarketSegmentCode__c":         row.get('market_segment'),
        "SourceSystem__c":              row.get('source_system'),
        "Source__c":                    row.get('source_system'),
        "ClusterID__c":                 row.get('cluster_id'),
        "RateCode__c":                  row.get('rate_plan_code'),

        # --- Consent ---
        "ConsentCentral__c":            False,
        "ConsentProperty__c":           True,
        #  Obsolete--> "IsPrimaryBooker__c":           True,

        # --- Dates ---
        "BookingDate__c":               sf_datetime(row.get('booking_at')),
        "Arrival__c":                   sf_datetime(row.get('arrival_at')),
        "Departure__c":                 sf_datetime(row.get('departure_at')),
        "CheckIn__c":                   sf_datetime(row.get('checkin_at')),
        "CheckOut__c":                  sf_datetime(row.get('checkout_at')),
        "CancellationAt__c":            sf_datetime(row.get('cancelled_at')),
        "NoShowAt__c":                  sf_datetime(row.get('noshow_at')),
        "RoomNights__c":                row.get('room_nights'),

        # --- Guest / occupancy ---
        "Adults__c":                    row.get('adults_num'),
        "ChildrenCount__c":             row.get('children_num'),
        "Guest__c":                     row.get('person_account_id'),
        "Contact__c":                    row.get('person_contact_id'),
        "GuestRole__c":                 row.get('guest_role'),

        # --- Revenues ---
        # rr.revenue_room, rr.revenue_fnb, rr.revenue_total
        "TotalRevenue__c":              row.get('revenue_total'),
        "FBRevenue__c":                 row.get('revenue_fnb'),
        "RoomRevenue__c":               row.get('revenue_room'),
        "OtherRevenue__c":              row.get('revenue_extra'),



        # --- Property ---
        "Property__c":                  row.get('sf_property_id'),

        # --- Channel / CRS ---
        "ChannelCode__c":               row.get('market_channel'),
        "BookingGroupID__c":            row.get('group_name'),
        "CRSBookingID__c":              row.get('external_code'),

        # --- Travel info ---
        # -> ToDo: missing mapping 
        "TravelPurpose__c":             row.get('travel_purpose'),

        # --- Company ---
        "BookerCompany__c":             None,
        "CompanyName__c":               None,
        "CompanyTaxID__c":              None,
        "CompanyRegisterNumber__c":     None,
        "CompanyID__c":                 row.get('booker_company_id'),
        "CompanyDebitorID__c":          None,
        "CompanyIATACode__c":           None,
        "CompanyGDSID__c":              None,
        "CompanyiHotelierID__c":        None,
        "CompanyBillingEmail__c":       None,
        "CompanyBillingCountry__c":     None,

        # --- Profile identity ---
        "ProfileTitle__c":              row.get("salutation"),
        "ProfileFirstName__c":          row.get("first_name"),
        "ProfileMiddleName__c":         row.get("middle_name"),
        "ProfileLastName__c":           row.get("last_name"),
        "ProfileEmail__c":              dev_email(row.get("email")),
        "ProfileMobilePhone__c":        row.get("phone"),
        "ProfileBirthdate__c":          sf_datetime(row.get("birth_date")),
        "ProfileBirthPlace__c":         row.get("birth_place"),
        "ProfileGenderIdentity__c":     row.get("gender"),
        "ProfilePreferredLanguage__c":  row.get("preferred_language"),
        "ProfileNationalityCountryCode__c": row.get("nationality"),

        # --- Profile address ---
        "ProfileMailingStreet__c":      row.get("address"),
        "ProfileMailingPostalCode__c":  row.get("postal_code"),
        "ProfileMailingCity__c":        row.get("city"),
        "ProfileMailingCountry__c":     row.get("country"),

        # --- Matching ---
        "ProfileSourceSystem__c":       row.get('source_system'),
    }

    # None-Werte entfernen (Bulk API: leerer String = Feldlöschung, None = weglassen)
    return {k: v for k, v in record.items() if v is not None}


def records_to_csv(records: list[dict]) -> str:
    """Konvertiert eine Liste von Dicts in einen CSV-String für die Bulk API."""
    if not records:
        return ""

    # Alle Keys aus allen Records sammeln (Union), External ID zuerst
    all_keys = [EXTERNAL_ID_FIELD] + sorted(
        {k for r in records for k in r if k != EXTERNAL_ID_FIELD}
    )

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction='ignore', lineterminator="\n")
    writer.writeheader()
    for record in records:
        writer.writerow(record)

    return buf.getvalue()


def chunked(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# --- Bulk API 2.0 Logik ---
def create_bulk_job(sf: SalesforceClientCC) -> str:
    """Erstellt einen Bulk API 2.0 Upsert-Job und gibt die Job-ID zurück."""
    job_id = sf.bulk_create_job(
        object_name=OBJECT_NAME,
        external_id_field=EXTERNAL_ID_FIELD,
    )
    print(f"  → Job erstellt: {job_id}")
    return job_id


def upload_csv_batch(sf: SalesforceClientCC, job_id: str, csv_data: str) -> None:
    """Lädt einen CSV-Batch in den Job."""
    sf.bulk_upload_csv(job_id, csv_data)


def close_bulk_job(sf: SalesforceClientCC, job_id: str) -> None:
    """Setzt den Job auf UploadComplete."""
    sf.bulk_close_job(job_id)
    print(f"  → Job geschlossen (UploadComplete)")


def poll_job(sf: SalesforceClientCC, job_id: str) -> dict:
    """Pollt den Job-Status bis JobComplete oder Failed."""
    url = f"{sf._base()}/jobs/ingest/{job_id}"
    for attempt in range(MAX_POLL_ATTEMPTS):
        r = sf._client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"Poll Job failed ({r.status_code}): {r.text}")
        status = r.json()
        state = status.get("state")
        print(f"  → [{attempt+1}/{MAX_POLL_ATTEMPTS}] Job state: {state} | "
              f"processed: {status.get('numberRecordsProcessed', 0)} | "
              f"failed: {status.get('numberRecordsFailed', 0)}")

        if state in ("JobComplete", "Failed", "Aborted"):
            return status

        time_module.sleep(POLL_INTERVAL_SEC)

    raise TimeoutError(f"Job {job_id} hat nach {MAX_POLL_ATTEMPTS} Versuchen nicht abgeschlossen.")


def fetch_failed_records(sf: SalesforceClientCC, job_id: str) -> list[dict]:
    """Lädt die fehlgeschlagenen Records aus der Bulk API."""
    url = f"{sf._base()}/jobs/ingest/{job_id}/failedResults"
    r = sf._client.get(url, headers={"Accept": "text/csv"})
    if r.status_code != 200:
        raise RuntimeError(f"Fetch failed records error ({r.status_code}): {r.text}")
    return list(csv.DictReader(io.StringIO(r.text)))


def save_failed_records(failed: list[dict], batch_index: int) -> None:
    out_path = Path(f"local_data/failed_batch_{batch_index}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(failed, f, indent=2)
    print(f"  → {len(failed)} fehlgeschlagene Records gespeichert: {out_path}")


# --- Main ---
def main():
    print(f"import_reservations_bulk | start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Daten aus MySQL laden
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)
    reservations = db.fetch_all(
        """ select * from crm_reservation_import_20260407 where row_id < 500 """
    )
    print(f"  → {len(reservations)} Reservierungen geladen")

    # 2. Mapping
    records = [row_to_sf_record(row) for row in reservations]

    # 3. Salesforce Auth
    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        total_failed = []

        # 4. Batches verarbeiten
        batches = list(chunked(records, BATCH_SIZE))
        for i, batch in enumerate(tqdm(batches, desc="Bulk Batches")):
            print(f"\nBatch {i+1}/{len(batches)} | {len(batch)} Records")

            try:
                # Job erstellen
                job_id = create_bulk_job(sf)

                # CSV hochladen
                csv_data = records_to_csv(batch)
                upload_csv_batch(sf, job_id, csv_data)

                # Job abschließen & pollen
                close_bulk_job(sf, job_id)
                status = poll_job(sf, job_id)

                # Fehler abrufen
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

        # 5. Zusammenfassung
        print(f"\nimport_reservations_bulk | end: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  → Gesamt fehlgeschlagene Records: {len(total_failed)}")

        if total_failed:
            all_failed_path = Path("local_data/batch/all_failed_records.json")
            with open(all_failed_path, "w") as f:
                json.dump(total_failed, f, indent=2)
            print(f"  → Alle Fehler gespeichert: {all_failed_path}")


if __name__ == "__main__":
    main()