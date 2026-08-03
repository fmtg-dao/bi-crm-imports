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
log_path = Path("logs/import_loyalty_members.log")
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


def members_person_row_to_payload() -> None:

    #
    # SELECT cluster_id, _entity_id, first_name, last_name, email, birth_date, 
    #  salutation, gender, middle_name, address, city, postal_code, country, 
    #  phone, birth_place, nationality, `language`, member_id, member_tier, 
    #  source_system, has_active_loyalty, preferred_language
    # FROM int_crm_person_accounts;
    #


    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    members = db.fetch_all(
        """select a.cluster_id, i.PersonContactId, a.member_id, a.source_system, date('2026-01-01') as enrollment_date
                from int_crm_person_accounts a
                inner join crm_person_account_sfid_uat i
                    on a.cluster_id = i.ClusterID__pc
                where a.has_active_loyalty = 1 and a.cluster_id = 47519
                order by a.cluster_id asc"""
    )
    


    #cnt = len(accounts)
    #print(cnt)

    ls_payload = []


    for row in members:

        payload = { 
            'cluster_id': row.get('cluster_id'), 
                'payload':  {
                    # --- Source info ---
                    "SourceSystem__c": row.get('source_system'),
                    "ContactId": row.get('PersonContactId'),
                    "ProgramId": '0lpUD000000Dh0jYAC',
                    "MemberType": 'Individual',
                    "MemberStatus": 'Active',
                    "MembershipNumber": row.get('member_id'),
                    "EnrollmentDate": sf_datetime(row.get('enrollment_date')),
                    

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

    print(f"import_members | start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    members = members_person_row_to_payload()

    print(f"import_members | number of records: {len(members)}")
    #print(members[0])

    err_cluster_id = []

    
    cfg_sfcc = load_salesforce_cc_config_from_env()
    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        for c in tqdm(members, total=len(members)):
            

            try:
                # create account - will created dubs 
                membership_id = sf.create_loyalty_program_member(c['payload'])

            except Exception as e:
                logging.exception(f"cluster_id: {c['cluster_id']} | payload: {c['payload']}")
                err_cluster_id.append(c['cluster_id'])

            finally:
                with open("local_data/err_member_cluster_id.json", "w") as f:
                    json.dump(err_cluster_id, f)
        