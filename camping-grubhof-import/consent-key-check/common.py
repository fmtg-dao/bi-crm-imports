"""
Gemeinsame Helpers fuer den Single-Record-Consent-Test (ConsentKey__c-Automation-Check).

Alle Skripte von der Repo-Root ausfuehren:
    ./.venv/Scripts/python.exe temp_consent_test/01_pick_test_row.py
"""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

load_dotenv(REPO_ROOT / ".env")

from config import load_mysql_config                     # noqa: E402
from mysql_client import MySQLClient                     # noqa: E402
from salesforce_client_prod import (                     # noqa: E402
    SalesforceClientCC,
    load_salesforce_cc_config_from_env,
)

STATE_PATH = Path(__file__).resolve().parent / "test_state.json"

BATCH_ID            = "2026-08-10_new_camping_import"
CONSENT_NAME        = "Marketing_Property"
DATA_USE_PURPOSE_ID = "0ZWTe0000000X8rOAE"   # marketing_property
PROPERTY_ID         = "a0QTe00000La2dVMAR"   # Camping Grubhof (ApaleoID FCG)
HOTEL_NAME          = "Camping Grubhof"      # HotelName__c - denormalisiert wie Migration
REGION              = "Saalachtal"           # Region__c    - denormalisiert wie Migration


def get_db() -> MySQLClient:
    return MySQLClient(load_mysql_config())


def get_sf() -> SalesforceClientCC:
    sf = SalesforceClientCC(load_salesforce_cc_config_from_env())
    sf.authenticate()
    return sf


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nstate gespeichert: {STATE_PATH}")
