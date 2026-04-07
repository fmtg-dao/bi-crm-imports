
from config import load_mysql_config
from mysql_client import MySQLClient
from transform_data import map_picklist
from salesforce_client import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, timezone, time, date
from typing import Any, Dict, Optional, Union
from tqdm import tqdm
import json
from pathlib import Path
import urllib.parse
import random

# Error Logging
import logging
from pathlib import Path

# create logs folder
log_path = Path("logs/import_reservations.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def dev_email(email: str | None) -> str | None:
    if not email:
        return None
    return f"{email}.inactive"


def sf_datetime(value: Optional[Union[datetime, date]]) -> Optional[str]:
    if value is None:
        return None

    # If it's a date, convert to datetime at midnight UTC
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min, tzinfo=timezone.utc)

    # If it's a datetime without tz, assume UTC
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def reservation_row_to_payload() -> None:

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    reservations = db.fetch_all(
        """select * from crm_reservation_import_20260322 where row_id > 2000"""
    )
    
    #cnt = len(reservations)
    #print(cnt)


    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc ) as sf:
        sf.authenticate()
        fmtg_id = '500541'

        sf_properties = sf.properties()

    
    ls_payload = []


    for row in reservations:

        payload = { 
            'reservation_id': row.get('reservation_id'), 
                'payload':  {
                    # --- Reservation core ---
                    "BookingID__c": row.get('booking_id'),
                    "ReservationStatus__c": row.get("reservation_status"),
                    "MarketSegmentCode__c": row.get('market_segment'),
                    "SourceSystem__c": row.get('source_system'),
                    "Source__c": row.get('source_system'),
                    "ClusterID__c": row.get('cluster_id'),
                    "RateCode__c": row.get('rate_plan_code'),

                    # --- Consent --- #
                    'ConsentCentral__c': True,
                    'ConsentProperty__c': True,
                    'IsPrimaryBooker__c': True,

                    # --- Dates ---
                    "BookingDate__c": sf_datetime(row.get('booking_at')),
                    "Arrival__c": sf_datetime(row.get('arrival_at')),
                    "Departure__c": sf_datetime(row.get('departure_at')),
                    "CheckIn__c": sf_datetime(row.get('checkin_at')),
                    "CheckOut__c": sf_datetime(row.get('checkout_at')),
                    "CancellationAt__c": sf_datetime(row.get('cancelled_at')),
                    "NoShowAt__c": sf_datetime(row.get('noshow_at')),
                    "RoomNights__c": row.get('room_nights'),

                    # --- Guest / occupancy ---
                    "Adults__c": row.get('adults_num'),
                    "ChildrenCount__c":  row.get('children_num'),
                    "Guest__c": row.get('person_contact_id'),
                    "GuestRole__c": row.get('guest_role'),

                    # --- Property ---
                    "Property__c": row.get('sf_property_id'),
                    #"UnitID__c": None,

                    # --- Channel / CRS ---
                    "ChannelCode__c": row.get('market_channel'),
                    "BookingGroupID__c": row.get('group_name'),
                    "CRSBookingID__c": row.get('external_code'),

                    # --- Travel info ---
                    #"TravelHear__c": None,
                    "TravelPurpose__c": row.get('travel_purpose'),

                    # --- Company / business account ---
                    "BookerCompany__c": None,
                    "CompanyName__c": None,
                    "CompanyTaxID__c": None,
                    "CompanyRegisterNumber__c": None,
                    "CompanyID__c": None,
                    "CompanyDebitorID__c": None,
                    "CompanyIATACode__c": None,
                    "CompanyGDSID__c": None,
                    "CompanyiHotelierID__c": None,
                    "CompanyBillingEmail__c": None,
                    "CompanyBillingCountry__c": None,

                    # --- Profile identity ---
                    "ProfileTitle__c": row.get("salutation"),
                    "ProfileFirstName__c": row.get("first_name"),
                    "ProfileMiddleName__c": row.get("middle_name"),
                    "ProfileLastName__c": row.get("last_name"),
                    "ProfileEmail__c": dev_email(row.get("email")),
                    "ProfileMobilePhone__c": row.get("phone"),
                    "ProfileBirthdate__c": sf_datetime(row.get("birth_date")),
                    "ProfileBirthPlace__c": row.get("birth_place"),
                    "ProfileGenderIdentity__c": row.get("gender"),
                    "ProfilePreferredLanguage__c": row.get("preferred_language"),
                    "ProfileNationalityCountryCode__c": row.get("nationality"),

                    # --- Profile address ---
                    "ProfileMailingStreet__c": row.get("address"),
                    "ProfileMailingPostalCode__c": row.get("postal_code"),
                    "ProfileMailingCity__c": row.get("city"),
                    "ProfileMailingCountry__c": row.get("country"),

                    # --- Matching & integration ---
                    "ProfileSourceSystem__c": row.get('source_system'),

                }
            }
        
        # remove empty fields
        payload["payload"] = {
            k: v
            for k, v in payload["payload"].items()
            if v is not None
        }

        #print(payload)

        ls_payload.append(payload)

    return ls_payload





if __name__ == "__main__":

    print(f"import_reservations | start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    reservations = reservation_row_to_payload() #[:1]

    #print(reservations)

    print(f"import_reservations | number of records: {len(reservations)}")

    err_reservation_id = []

    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()       

        for res in tqdm(reservations, total=len(reservations)):


            try:
                # upsert reservation by reservation id
                res_id = sf.upsert_reservation_payload(res['reservation_id'], res['payload'] ) 

            except Exception as e:
                logging.exception(f"reservation_id: {res['reservation_id']} | payload: {res['payload']}")
                err_reservation_id.append(res['reservation_id'])

            finally:
                with open("local_data/err_reservation_id.json", "w") as f:
                    json.dump(err_reservation_id, f)


            #print(res_id)