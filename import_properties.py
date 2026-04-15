from config import load_mysql_config
from mysql_client import MySQLClient
from transform_data import map_picklist
#from salesforce_client import SalesforceClientCC, load_salesforce_cc_config_from_env
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
from datetime import datetime, timezone, time, date
from typing import Any, Dict, Optional, Union
from tqdm import tqdm
import json
from pathlib import Path
import urllib.parse
import random


def property_payload() -> None:

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    records = db.fetch_all(
        """select 
                    PAS_FMTG_ID as fmtg_id,
                    PAS_code3 as apaleo_id,
                    PAS_name_short as name_short,
                    PAS_name_long  as name_long,
                    PAS_Protel_ID as protel_id,
                    PAS_pms as pms
                    
            from V2D_Property_Attributes vdpa
            where is_active = 1;"""
    )

    cnt = len(records)
    print(cnt)

    ls_payload = []

    for row in records:

        payload = { 
            'FMTGID__c': row.get('fmtg_id'),
            'payload': { 
                        'Name': row.get('name_short'),
                        'LongName__c': row.get('name_long'),
                        'ApaleoID__c': row.get('apaleo_id'),
                        'ProtelID__c': row.get('protel_id'),
                        'PMS__c': row.get('pms'),
                        }
        }

            # remove empty fields
        payload['payload'] = {
                                k: v
                                for k, v in payload['payload'].items()
                                if v is not None
                            }

        ls_payload.append(payload)
        #print(payload)

    return ls_payload


if __name__ == "__main__":

    payload = property_payload()


    cfg_sfcc = load_salesforce_cc_config_from_env()
    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        for property in tqdm(payload, total=len(payload)):
        
            property_id = sf.upsert_object_by_external_id('Property__c', 'FMTGID__c', property['FMTGID__c'], property['payload']) 
            print(property_id)
