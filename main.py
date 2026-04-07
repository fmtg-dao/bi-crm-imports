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


def SalesForceBusinessAccount():

    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        # Create (always creates, duplicates possible)
        new_id = sf.create_account_business(
            
            Name = "cUstomer GMBH",
            Phone = "+43 1 456841",
            BillingCity="Berlin",
            BillingCountry="Germany",
            BillingCountryCode__c= "AT",
        )
        print("Created Account:", new_id)


def SalesForceAccountPerson():

    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        # Create (always creates, duplicates possible)
        new_id = sf.create_account_person(
            
            Phone="+34 1 23456733",
            Type = "Other" ,
            BillingCity="Vienna",
            BillingCountry="Austria",
            BillingCountryCode__c= "AT",
            LastName= "Smith",
            FirstName= "John",
            Salutation= "Mr.",
            MiddleName= "D.",
            Suffix= "MBA",
        )
        print("Created Account:", new_id)




def AccountInfo():
    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        desc = sf.describe_account()
        print(desc)




def QueryAccount(account_id:str):

    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        # Query back (optional)
        q = sf.query(f"SELECT FIELDS(All) FROM Account WHERE ID = '{account_id}' LIMIT 1")

        # soql = (
        # "SELECT Id, PersonContactId, FirstName, LastName, PersonEmail, PersonBirthdate "
        # "FROM Account "
        # f"WHERE Id = '{account_id}' PersonEmail != null AND PersonContact.IndividualId = null "
        # "LIMIT 10"
        # )

        # q = sf.query(soql)

        print(json.dumps( q.get("records"), indent=2, ensure_ascii=False))


def QueryReservation():

    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        # Query back (optional)
        q = sf.query(f"SELECT FIELDS(All) FROM Reservation__c WHERE ReservationID__c = 'XKSHD-1' LIMIT 1")
        print("Query result:", q.get("records"))


def QueryProperties():

    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        # Query back (optional)
        q = sf.query(f"SELECT FIELDS(All) FROM Property__c LIMIT 100")
        print("Query result:", q.get("records"))

        fmtg_id = '500541'

        Property__c = sf.properties()[fmtg_id].sf_id

        print(Property__c)



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


def dev_email(email: str | None) -> str | None:
    if not email:
        return None
    return f"{email}.inactive"

def account_person_row_to_payload() -> None:

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    accounts = db.fetch_all(
        """WITH base AS (
                SELECT *

                FROM crm_reservation_test rs
                -- your joins here (pro/rs/chl/cls etc.)
                WHERE rs.arrival_at <= CURRENT_DATE()
                ),
                ranked AS (
                    SELECT
                        base.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY base.cluster_id
                            ORDER BY base.arrival_at DESC, base.reservation_id DESC
                        ) AS rn
                    FROM base
                )
        SELECT *
        FROM ranked
        WHERE rn = 1;"""
    )
    cnt = len(accounts)

    print(cnt)

    ls_payload = []


    for row in accounts:

        payload = { 
            'cluster_id': row.get('cluster_id'), 
                'payload':  {
                    # --- Source info ---
                    "SourceSystem__c": row.get('pms_name'),

                    # --- Contact (Person Account core fields) ---
                    "FirstName": row.get("pg_first_name"),
                    "LastName": row.get("pg_last_name"),
                    "Salutation": row.get("pg_title"),
                    "MiddleName": row.get("pg_middle_name"),
                    "Suffix": None,

                    "PersonEmail": dev_email(row.get("pg_email")),
                    "PersonMobilePhone": row.get("pg_phone"),
                    "PersonHomePhone": None,
                    "PersonOtherPhone": None,
                    "PersonAssistantPhone": None,

                    "PersonTitle": row.get("pg_title"),
                    "PersonDepartment": None,
                    "PersonAssistantName": None,

                    # --- Person demographics ---
                    "PersonBirthdate": row.get("pg_birth_date"),
                    "PersonGenderIdentity": row.get("pg_gender"),
                    #"PersonPronouns": None,

                    # --- Person address (MAILING, not compound) ---
                    "PersonMailingStreet": row.get("pg_mailing_street"),
                    "PersonMailingCity": row.get("pg_mailing_city"),
                    "PersonMailingState": None,
                    "PersonMailingPostalCode": row.get("pg_mailing_postal_code"),
                    "PersonMailingCountry": None,

                    # --- Billing Address (Account-level, still valid for Person Accounts) ---
                    "BillingStreet": row.get("pg_mailing_street"),
                    "BillingCity": row.get("pg_mailing_city"),
                    "BillingState": None,
                    "BillingPostalCode": row.get("pg_mailing_postal_code"),
                    "BillingCountry": None,
                    "BillingCountryCode__c": row.get("pg_mailing_country_code"),

                    # --- Contact fallback ---
                    "Phone": row.get("pg_phone"),

                    # --- Custom Person fields (__pc) ---
                    "BirthPlace__pc": row.get("pg_birth_place"),
                    "NationalityCountryCode__pc": row.get("pg_nationality_country_code"),
                    "PreferredLanguage__pc": row.get("pg_preferred_language"),
                    #"RegionCode__pc": None,
                    #"SourceSystem__pc": row.get('pms_name'),

                    # --- Custom Account / Business metadata ---
                    "BillingEmail__c":  dev_email(row.get("pg_email")),
                    #"CompanyDebitorID__c": None,
                    #"CompanyID__c": None,
                    #"CompanyRegisterNumber__c": None,
                    #"CompanyTaxID__c": None,
                    #"IATACode__c": None,
                    #"GDSID__c": None,
                    #"iHotelierID__c": None,

                    # --- Flags / integration ---
                    #"IsMasterAccount__c": False,
                    

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


def preferences_row_to_payload(data:Dict) -> dict:

    return {
            "allOrNone": True,
            "compositeRequest": [
                {
                    "method": "POST",
                    "url": "/services/data/v60.0/sobjects/Preferences__c",
                    "referenceId": "refPreferences",
                    "body": {
                        "Name": data.get("preference_name"),
                        "Category__c": data.get("preference_category"),
                        "Contact__c": data.get("contact_id"),
                        "ExternalID__c": data.get("external_id"),
                        "IsActive__c": True,
                        "SourceSystem__c": data.get("source_system"),
                    },
                }
            ],
        }


def sync_preference_data() -> None:

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    preferences = db.fetch_all(
        "SELECT * FROM crm_preferences_test WHERE 1 = %s",
        (1,),
    )
    cnt = len(preferences)
    print(f"Count preferences: {cnt}")


    
    cfg_sfcc = load_salesforce_cc_config_from_env()
    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        preferences_to_sync = []

        #enrich data with sf account id
        print("collect contact ids and create payload")
        for p in tqdm(preferences[5:], total=len(preferences[5:])):

            soql_acc = (
            "SELECT PersonContactId "
            "FROM Account "
            f"WHERE ClusterID__c = '{p.get('cluster_id')}' AND PersonContactId != null "
            "LIMIT 1"
            )


            q_acc = sf.query(soql_acc)

            
            #print(q_acc)

            records = q_acc.get("records") or [{}]
            person_contact_id = records[0].get("PersonContactId")


            if person_contact_id:
                p["contact_id"] = person_contact_id
                preferences_to_sync.append(p)

            #print(p)
        

        #create preferences for respective accounts
        print("start preferences sync")
        for p in tqdm(preferences_to_sync, total=len(preferences_to_sync)):

            if p.get("contact_id"):

                payload = preferences_row_to_payload(p)
                r = sf.composite(payload)
                #print(r)

    







def reservation_row_to_payload() -> None:
    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    # SELECT multiple rows
    reservations = db.fetch_all(
        "SELECT * FROM crm_reservation_test WHERE 1 = %s",
        (1,),
    )
    cnt = len(reservations)

    print(cnt)


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
                    #"ReservationStatus__c": map_picklist("ReservationStatus__c", row.get('reservation_status'), on_unknown = None),
                    #"MarketSegmentCode__c": map_picklist("MarketSegmentCode__c", row.get('market_segment'), on_unknown = "Other"),
                    "ReservationStatus__c": "cancelled" if row.get("reservation_status") == "canceled" else row.get("reservation_status"),
                    "MarketSegmentCode__c": row.get('market_segment'),
                    "SourceSystem__c": row.get('pms_name'),
                    "Source__c": row.get('pms_name'),
                    "ClusterID__c": row.get('cluster_id'),

                    # --- Dates ---
                    "BookingDate__c": sf_datetime(row.get('booking_at')),
                    "Arrival__c": sf_datetime(row.get('arrival_at')),
                    "Departure__c": sf_datetime(row.get('departure_at')),
                    "CheckIn__c": sf_datetime(row.get('checkin_at')),
                    "CheckOut__c": sf_datetime(row.get('checkout_at')),
                    "CancellationAt__c": sf_datetime(row.get('cancelled_at')),
                    "NoShowAt__c": sf_datetime(row.get('noshow_at')),

                    # --- Guest / occupancy ---
                    "Adults__c": row.get('adults_num'),
                    "ChildrenCount__c":  row.get('children_num'),
                    "Guest__c": None,
                    "GuestRole__c": row.get('guest_role'),

                    # --- Property ---
                    "Property__c": sf_properties[row.get('property_fmtg_id')].sf_id,
                    "UnitID__c": None,

                    # --- Channel / CRS ---
                    "ChannelCode__c": row.get('market_channel'),
                    "BookingGroupID__c": row.get('group_name'),
                    "CRSBookingID__c": row.get('external_code'),

                    # --- Travel info ---
                    "TravelHear__c": None,
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
                    "ProfileTitle__c": row.get("pg_title"),
                    "ProfileFirstName__c": row.get("pg_first_name"),
                    "ProfileMiddleName__c": row.get("pg_middle_name"),
                    "ProfileLastName__c": row.get("pg_last_name"),
                    "ProfileEmail__c": dev_email(row.get("pg_email")),
                    "ProfileMobilePhone__c": row.get("pg_phone"),
                    "ProfileBirthdate__c": row.get("pg_birth_date"),
                    "ProfileBirthPlace__c": row.get("pg_birth_place"),
                    "ProfileGenderIdentity__c": row.get("pg_gender"),
                    "ProfilePreferredLanguage__c": row.get("pg_preferred_language"),
                    "ProfileNationalityCountryCode__c": row.get("pg_nationality_country_code"),

                    # --- Profile address ---
                    "ProfileMailingStreet__c": row.get("pg_mailing_street"),
                    "ProfileMailingPostalCode__c": row.get("pg_mailing_postal_code"),
                    "ProfileMailingCity__c": row.get("pg_mailing_city"),
                    "ProfileMailingCountry__c": row.get("pg_mailing_country_code"),

                    # --- Matching & integration ---
                    "ProfileSourceSystem__c": "apaleo",

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


def reservation_sync():

    res_payload = reservation_row_to_payload()

    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()       

        for res in tqdm(res_payload, total=len(res_payload)):
        
            res_id = sf.upsert_reservation_payload(res['reservation_id'], res['payload'] ) 

            #print(res_id)

def accounts_sync():

    # write mapping to file
    mapping: dict[str, str] = {}
    out_file = Path("cluster_to_account_id.json")
    
    acc_payload = account_person_row_to_payload()

    cfg_sfcc = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()       

        for acc in tqdm(acc_payload, total=len(acc_payload)):
        
            acc_id = sf.upsert_account_payload( sf.RecordTypes.Account.Person, acc['cluster_id'], acc['payload'] ) 

            #print(acc_id)
            


            if acc_id:
                mapping[acc['cluster_id']] = acc_id

                relate_payload = {
                        'Guest__c': acc_id['id'],
                }

                soql = (
                            f"SELECT ReservationID__c FROM Reservation__c WHERE ClusterID__c = '{acc['cluster_id']}'"
                        )
                
                q = sf.query(soql)

                reservations = [r["ReservationID__c"] for r in q.get("records", [])]

                for res in reservations:

                    sf.upsert_reservation_payload(res, relate_payload)

        # persist to json
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    return mapping


def build_individual_consent_composite(
            account_id: str,
            data_use_purpose_id_central: str,
            data_use_purpose_id_hotel: str,
            property_id: str,
            source_system: str = "apaleo"
        ) -> dict:

    soql = (
        "SELECT Id, PersonContactId, FirstName, LastName, PersonEmail, PersonBirthdate "
        "FROM Account "
        f"WHERE Id = '{account_id}' "
        "LIMIT 1"
    )

    soql_encoded = urllib.parse.quote(soql, safe="")


    return {
        "allOrNone": True,
        "compositeRequest": [
            {
                "method": "GET",
                "url": f"/services/data/v60.0/query?q={soql_encoded}",
                "referenceId": "refAccount",
            },
            {
                "method": "POST",
                "url": "/services/data/v60.0/sobjects/Individual",
                "referenceId": "refIndividual",
                "body": {
                    "FirstName": "@{refAccount.records[0].FirstName}",
                    "LastName": "@{refAccount.records[0].LastName}",
                    "Contact__c": "@{refAccount.records[0].PersonContactId}",
                    #"BirthDate": "@{refAccount.records[0].PersonBirthdate}",
                    "SourceSystem__c": source_system,
                },
            },
            {
                "method": "POST",
                "url": "/services/data/v60.0/sobjects/ContactPointEmail",
                "referenceId": "refCPE",
                "body": {
                    "ParentId": "@{refIndividual.id}",
                    "Contact__c": "@{refAccount.records[0].PersonContactId}",
                    "EmailAddress": "@{refAccount.records[0].PersonEmail}",
                    "ActiveFromDate": sf_datetime(date.today()),
                    #"ActiveToDate": None,
                    "SourceSystem__c": source_system,


                },
            },
            # Contact Point Consent for Marketing Central  
            {
                "method": "POST",
                "url": "/services/data/v60.0/sobjects/ContactPointConsent",
                "referenceId": "refConsentCentral",
                "body": {
                    "Name": "marketing_central",
                    "ContactPointId": "@{refCPE.id}",
                    "CaptureDate": sf_datetime(date.today()),
                    "CaptureSource": "migration" ,
                    "PrivacyConsentStatus": "OptIn",
                    "DataUsePurposeId": data_use_purpose_id_central,
                    "EffectiveFrom": sf_datetime(date.today()),
                    #"EffectiveTo": None,
                    "SourceSystem__c": source_system,
                },
            },
            # Contact Point Consent for Hotel  
            {
                "method": "POST",
                "url": "/services/data/v60.0/sobjects/ContactPointConsent",
                "referenceId": "refConsentHotel",
                "body": {
                    "Name": "marketing_hotel",
                    "ContactPointId": "@{refCPE.id}",
                    "CaptureDate": sf_datetime(date.today()),
                    "CaptureSource": "migration" ,
                    "PrivacyConsentStatus": "OptIn",
                    "DataUsePurposeId": data_use_purpose_id_hotel,
                    "Property__c": property_id,
                    "EffectiveFrom": sf_datetime(date.today()),
                    #"EffectiveTo": None,
                    "SourceSystem__c": source_system,
                },
            },
        ],
    }

def create_individual_consent_composite():

    # marketing_central '0ZWUD000000A0y14AC'
    data_use_purpose_id_central = '0ZWUD000000A0y14AC'
    data_use_purpose_id_hotel = '0ZWUD0000009zvV4AQ'

    # with open("cluster_to_account_id.json", "r", encoding="utf-8") as f:
    #     data = json.load(f)

    # acc_ids = {
    #     v["id"]
    #     for v in data.values()
    #     if v.get("id")
    # }


    cfg_sfcc = load_salesforce_cc_config_from_env()
    with SalesforceClientCC(cfg_sfcc) as sf:
        sf.authenticate()

        ls_payload = []


        soql_acc = (
        "SELECT Id "
        "FROM Account "
        "WHERE PersonContactId IN (SELECT Id FROM Contact WHERE IndividualId = null) AND PersonEmail != null AND CreatedById = '005UD00000MnVVqYAN' "  
        #f"WHERE PersonEmail != null AND PersonContact.IndividualId = null AND CreatedById = '005UD00000MnVVqYAN' and Id ='001UD00000SYIuNYAX' "
        "LIMIT 500"
        )

        q_acc = sf.query(soql_acc)

        acc_ids = [
            r["Id"]
            for r in q_acc.get("records", [])
        ]



        q_properties = sf.query(f"SELECT Id FROM Property__c LIMIT 100")

        property_ids = [
            r["Id"]
            for r in q_properties.get("records", [])
]
            

        for acc in list(acc_ids):

            property_id = random.choice(property_ids)

            #print("-------------------")
            #print(f"account_id: {acc}")
            #print(f"property_id: {property_id}")
            #print("-------------------")

            ls_payload.append(
                
                    build_individual_consent_composite(acc,
                                                    data_use_purpose_id_central,
                                                    data_use_purpose_id_hotel,
                                                    property_id, 
                                                    "apaleo" )
            )
            #print("-------------------")   
            #print(ls_payload)
            #print("-------------------") 

        for payload in ls_payload:
           
           print(payload)
           r = sf.composite(payload)

           print(r)






if __name__ == "__main__":

    #QueryAccount('001UD00000SajNZYAZ')

    sync_preference_data()

    #create_individual_consent_composite()
   
    #reservation_sync()

    #mapping = accounts_sync()

    #print(mapping)

    #QueryAccount()

   #QueryProperties()
   #QueryReservation()
   #QueryAccount()
   
   #SalesForceAccountPerson()
   #SalesForceBusinessAccount()
   #CreateReservation()
   #UpdateReservation()
   
   # main()

   #QueryAccount()

   #SalesForceTest()

   #AccountInfo()