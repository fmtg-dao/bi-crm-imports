from config import load_mysql_config
from mysql_client import MySQLClient
from salesforce_client_prod import SalesforceClientCC, load_salesforce_cc_config_from_env
import pandas as pd



SF_DATE_FIELDS = {
        'CreatedDate', 'LastModifiedDate', 'LastActivityDate', 
        'LastViewedDate', 'LastReferencedDate', 'SystemModstamp',
        'EmailBouncedDate', 'ConvertedDate', 'CaptureDate',
        'EffectiveFrom', 'EffectiveTo', 'ActivityDate',
        'CloseDate', 'StartDate', 'EndDate', 'Birthdate', 'PersonBirthdate'
    }

# ACCOUNT 
ACCOUNT_QUERY = """
        SELECT Id, PersonContactId, PersonIndividualId, IsPersonAccount, RecordTypeId, OwnerId, CreatedDate, LastModifiedDate, LastSourceSystemUpdate__c,
        SourceSystem__pc, SourceSystem__c, SourceOrigin__pc, SourceSystemIdentifier, ExternalID__pc, EntraExternalID__pc, ClusterID__pc, ClusterID__c,
        Salutation, FirstName, MiddleName, LastName, Suffix, PersonTitle, PersonGenderIdentity, PersonPronouns, PersonBirthdate, BirthdateString__pc,
        BirthPlace__pc, NationalityCountryCode__pc, PreferredLanguage__pc, RegionCode__pc,
        PersonEmail, PersonEmailBouncedDate, PersonEmailBouncedReason, PersonHasOptedOutOfEmail, PersonMobilePhone, PersonHomePhone, PersonOtherPhone,
        PersonAssistantPhone, PersonAssistantName, PersonDepartment, PersonDoNotCall, Phone, Fax, PersonHasOptedOutOfFax,
        BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingCountryCode__c, BillingEmail__c,
        PersonMailingStreet, PersonMailingCity, PersonMailingState, PersonMailingPostalCode, PersonMailingCountry,
        PersonOtherStreet, PersonOtherCity, PersonOtherState, PersonOtherPostalCode, PersonOtherCountry,
        HotelCustomer__pc, CampingCustomer__pc, ResidencesCustomer__pc, InvestCustomer__pc, InvestmentStatus__pc, InvestmentExpirationDate__pc,
        IsMasterAccount__c, CustomerSinceDate__c,
        SyncToMcHotelCampingResidences__pc, SyncToMcInvest__pc, StopSalesEmail__c,
        PrimaryProperty__pc, PrimarySalesOwner__c, ParentId
        FROM Account
        WHERE IsDeleted = false
    """

ACCOUNT_PATH =  'local_data/sf_object_data/accounts_prod.parquet'

ACCOUNT_TABLE = 'crm_person_account_sfid_prod'

# LEAD

LEAD_QUERY = """
                SELECT id, FirstName, LastName, Title, Email, Country, CountryCode__c, BillingEmail__c, HotelCustomer__c,  
                        ClusterID__c, Status, PreferredLanguage__c, BirthdateString__c, CreatedDate, MatchBestConfidence__c, MatchCandidateCount__c, MatchCheckDue__c, MatchCheckNotBefore__c, 
                        MatchCheckRequested__c, MatchCheckStatus__c, MatchResult__c, MatchCheckedBy__c, RelatedPersonAccount__c, RelatedContact__c, LastMatchCheckedAt__c, MatchCheckAttempts__c, MatchedContact__c, MatchManualReviewResolvedAt__c, MatchManualReviewStatus__c 
                FROM Lead
                WHERE IsDeleted = false
            """

LEAD_PATH = 'local_data/sf_object_data/leads_prod.parquet'

LEAD_TABLE = 'crm_person_lead_sfid_prod'


# Loyality 
LOYALITY_QUERY = """
                SELECT Id, MembershipNumber, ExternalMemberId__c, MemberStatus, ContactId, 
                    ProgramId,  LegacyMemberId__c, LegacyTier__c, TierName__c, LastActivityDate,
                    EntraID__c, CreatedDate    
                FROM LoyaltyProgramMember
                WHERE IsDeleted = false
                """

LOYALITY_PATH = 'local_data/sf_object_data/loyality_prod.parquet'

LOYALITY_TABLE = 'crm_loyality_sfid_prod'


# PROPERTY 
PROPERTY_QUERY = """
            SELECT FIELDS(All) FROM PROPERTY__C LIMIT 200
        """

PROPERTY_PATH =  'local_data/sf_object_data/properties_prod.parquet'
PROPERTY_TABLE = 'crm_properties_sfid_prod'


# CONTACT POINT EMAIL

CP_EMAIL_QUERY = """ 
                SELECT EmailAddress, Id, ParentId, PartyID__c, LastModifiedDate, CreatedDate, SourceSystem__c
                FROM ContactPointEmail
                WHERE IsDeleted = false
            """

CP_EMAIL_PATH = 'local_data/sf_object_data/cp_email_prod.parquet'
CP_EMAIL_TABLE = 'crm_cp_email_sfid_prod'


# CONTACT POINT CONSENT
CP_CONSENT_QUERY = """SELECT Id, Name, ContactPointId, PrivacyConsentStatus, CaptureSource, CaptureDate, 
                            EffectiveFrom, EffectiveTo, PartyId, ConsentKey__c, Property__c, SourceSystem__c, LastModifiedDate, CreatedDate
                    FROM ContactPointConsent
                    WHERE IsDeleted = false
                    """

CP_CONSENT_PATH = 'local_data/sf_object_data/cp_consent_prod.parquet'
CP_CONSENT_TABLE = 'crm_cp_consent_sfid_prod'

# RESERVATIONS 
RESERVATION_QUERY = """SELECT
                            Id, Name, IsDeleted, CreatedDate, LastModifiedDate,
                            ReservationID__c, ClusterID__c, SFID__c,
                            SourceSystem__c, Source__c, ReservationStatus__c,
                            BookingID__c, CRSBookingID__c, BookingGroupID__c,
                            Property__c, PropertyType__c, UnitID__c, RoomNights__c,
                            RateCode__c, MarketSegmentCode__c, ChannelCode__c,
                            BookingDate__c, Arrival__c, Departure__c,
                            CheckIn__c, CheckOut__c, CancellationAt__c, NoShowAt__c,
                            Adults__c, ChildrenCount__c,
                            Guest__c, Contact__c, Lead__c, BookerCompany__c,
                            GuestRole__c, IsPrimaryBooker__c,
                            ConsentCentral__c, ConsentProperty__c,
                            TotalRevenue__c, RoomRevenue__c, FBRevenue__c, OtherRevenue__c,
                            ProfileTitle__c, ProfileFirstName__c, ProfileMiddleName__c, ProfileLastName__c,
                            ProfileEmail__c, ProfileMobilePhone__c,
                            ProfileBirthdate__c, ProfileBirthPlace__c,
                            ProfileGenderIdentity__c, ProfilePreferredLanguage__c,
                            ProfileNationalityCountryCode__c, ProfileSourceSystem__c,
                            ProfileMailingStreet__c, ProfileMailingPostalCode__c,
                            ProfileMailingCity__c, ProfileMailingCountry__c,
                            ProfileMatchingStatus__c, ProfileMatchingMethod__c, ProfileMatchingConfidence__c,
                            TravelPurpose__c
                        FROM Reservation__c
                        WHERE IsDeleted = false
                    """

RESERVATION_PATH = 'local_data/sf_object_data/reservation_prod.parquet'

RESERVATION_TABLE = 'crm_reservation_sfid_prod'




def sf_query(soql_query:str) -> pd.DataFrame:
    cfg = load_salesforce_cc_config_from_env()

    with SalesforceClientCC(cfg) as sf:
        sf.authenticate()

        q = sf.query_all(soql_query)

        records = q.get("records", [])

        # remove Salesforce metadata
        cleaned = [
            {k: v for k, v in r.items() if k != "attributes"}
            for r in records
        ]

        df = pd.DataFrame(cleaned)

        return df
    
def save_object_in_mysqL(local_path:str, table_name:str, if_exists:str='replace'):

    df = pd.read_parquet(local_path)

  
    for col in df.columns:
        if col in SF_DATE_FIELDS:
            df[col] = pd.to_datetime(df[col], utc=True)
        else:
            df[col] = df[col].astype('string')

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    db.create_table_from_df(df, table_name, if_exists)


def create_consent_df() -> pd.DataFrame:

    df_cpc = pd.read_parquet(CP_CONSENT_PATH)
    df_cpe = pd.read_parquet(CP_EMAIL_PATH)
    df_acc = pd.read_parquet(ACCOUNT_PATH)

    df_acc_slim = df_acc[['Id', 'PersonContactId', 'FirstName', 'LastName', 'ClusterID__pc']].rename(columns={'Id': 'AccountId', 'ClusterID__pc': 'ClusterID' })
    
    df_cpe_slim = df_cpe[['Id', 'EmailAddress', 'PartyID__c', ]]

    
    df_cpc_slim = df_cpc[['Id', 'Name', 'ContactPointId', 'PrivacyConsentStatus', 'CaptureSource', 'CaptureDate',
                        'EffectiveFrom', 'EffectiveTo', 'PartyId', 'ConsentKey__c', 'Property__c', 'SourceSystem__c',
                        'LastModifiedDate', 'CreatedDate']]


    df_merged = (
        df_cpc_slim
        .merge(df_cpe_slim, left_on='ContactPointId', right_on='Id', suffixes=('', '_cpe'), how='left')
        .merge(df_acc_slim, left_on='PartyID__c', right_on='PersonContactId', how='left')
    )
    
    df_merged['PartyType'] = df_merged['PartyID__c'].str[:3].map({ 
                                                            '001': 'Account',
                                                            '003': 'Contact',
                                                            '00Q': 'Lead'
                                                        })

    date_cols = ['CaptureDate', 'EffectiveFrom', 'EffectiveTo', 'LastModifiedDate', 'CreatedDate']

    for col in df_merged.columns:
        if col in date_cols:
            df_merged[col] = pd.to_datetime(df_merged[col], utc=True)
        else:
            df_merged[col] = df_merged[col].astype('string')

    return df_merged





if __name__ == "__main__":

    # # # # # Account 
    df_acc = sf_query(ACCOUNT_QUERY)
    df_acc.to_parquet(ACCOUNT_PATH, index=False)
    save_object_in_mysqL(ACCOUNT_PATH,ACCOUNT_TABLE)

    print('Account done')

    # # # # Loyality
    df_loy = sf_query(LOYALITY_QUERY)
    df_loy.to_parquet(LOYALITY_PATH, index=False)
    save_object_in_mysqL(LOYALITY_PATH,LOYALITY_TABLE)

    print('Loyality done')

    # # # # Lead
    df_lea = sf_query(LEAD_QUERY)
    df_lea.to_parquet(LEAD_PATH, index=False)
    save_object_in_mysqL(LEAD_PATH,LEAD_TABLE)

    print('Lead done')

    # # # # Reservation
    df_res = sf_query(RESERVATION_QUERY)
    df_res.to_parquet(RESERVATION_PATH, index=False)
    save_object_in_mysqL(RESERVATION_PATH, RESERVATION_TABLE)

    print('Reservation done')

    # # # # # CPE  
    df_cpe = sf_query(CP_EMAIL_QUERY)
    df_cpe.to_parquet(CP_EMAIL_PATH, index=False)
    save_object_in_mysqL(CP_EMAIL_PATH,CP_EMAIL_TABLE)

    print('CPE done')
 
    # # # # # CPC  
    df_cpc = sf_query(CP_CONSENT_QUERY)
    df_cpc.to_parquet(CP_CONSENT_PATH, index=False)
    save_object_in_mysqL(CP_CONSENT_PATH,CP_CONSENT_TABLE)

    print('CPC done')

    # # # # # # Consent 
    df_consent = create_consent_df()
    df_consent.to_parquet('local_data/sf_object_data/consent_prod.parquet', index=False)
    save_object_in_mysqL('local_data/sf_object_data/consent_prod.parquet','crm_consent_sfid_prod')

    print('Consent done')

    # # # print(df_consent.dtypes)
