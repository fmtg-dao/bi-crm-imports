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


def ratescodes_payload() -> None:

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    records = db.fetch_all(
        """select 
                coalesce(RCO_plan_name, RCO_crs_code) as Name,
                RCO_crs_code as RateCode__c,
                RCO_category as RateCodeCategory__c,
                RCO_market_segment as RateType__c,
                RCO_thematik as RateDescription__c
           from V2I_RateCodeOverview;"""
    )

    cnt = len(records)
    print(cnt)

    ls_payload = []

    for row in records:

        payload = { 
            'RateCode__c': row.get('RateCode__c'),
            'payload': { 
                        'Name': row.get('Name'),
                        'RateCodeCategory__c': row.get('RateCodeCategory__c'),
                        'RateType__c': row.get('RateType__c'),
                        'RateDescription__c': row.get('RateDescription__c')
                        }
        }

            # remove empty fields
        payload['payload'] = {
                                k: v
                                for k, v in payload['payload'].items()
                                if v is not None
                            }

        ls_payload.append(payload)
        print(payload)

    return ls_payload


if __name__ == "__main__":

    rates_payload = ratescodes_payload()


    cfg_sfcc = load_salesforce_cc_config_from_env()
    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        for rate in tqdm(rates_payload, total=len(rates_payload)):
        
            rate_id = sf.upsert_object_by_external_id('RateCode__c', 'RateCode__c', rate['RateCode__c'], rate['payload']) 
            #print(rate_id)
