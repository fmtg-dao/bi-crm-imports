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
import pandas as pd




def sf_query_accounts():
    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        q = sf.query_all("""
            SELECT Id, PersonContactId, PersonEmail, FirstName, LastName, ClusterID__pc
            FROM Account
        """)

        records = q.get("records", [])

        # remove Salesforce metadata
        cleaned = [
            {k: v for k, v in r.items() if k != "attributes"}
            for r in records
        ]

        df = pd.DataFrame(cleaned)

        return df

def sf_query_properties():
    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        q = sf.query_all("""
            SELECT FIELDS(All) FROM PROPERTY__C LIMIT 200
        """)

        records = q.get("records", [])

        # remove Salesforce metadata
        cleaned = [
            {k: v for k, v in r.items() if k != "attributes"}
            for r in records
        ]

        df = pd.DataFrame(cleaned)

        return df


def save_accounts_in_mysqL():

    df = pd.read_parquet("local_data/accounts.parquet")


    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    db.create_table_from_df(df,'crm_person_account_sfid_uat', 'replace')

def save_properties_in_mysqL():

    df = pd.read_parquet("local_data/properties.parquet")


    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    db.create_table_from_df(df,'crm_properties_sfid_uat', 'replace')

if __name__ == "__main__":

    save_properties_in_mysqL()

    # save_accounts_in_mysqL()

    # df_acc = sf_query_accounts()

    # print(df_acc.head(5))

    # df_acc.to_parquet("local_data/accounts.parquet", index=False)

    #df_pro = sf_query_properties()

    # print(df_pro.head(5))

    #df_pro.to_parquet("local_data/properties.parquet", index=False)

