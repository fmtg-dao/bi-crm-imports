from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, NewType, List
import httpx


RecordTypeId = NewType("RecordTypeId", str)



@dataclass(frozen=True)
class AccountRecordTypes:
    Business: RecordTypeId
    Person: RecordTypeId

@dataclass(frozen=True)
class RecordTypes:
    Account: AccountRecordTypes

@dataclass(frozen=True)
class SalesforceCCConfig:
    token_url: str
    client_id: str
    client_secret: str
    api_version: str = "60.0"

@dataclass(frozen=True)
class PropertyData:
    sf_id: str
    name: Optional[str]
    protel_id: Optional[str]
    apaleo_id: Optional[str]




def _require_record_type_env(name: str) -> RecordTypeId:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return RecordTypeId(value)


class SalesforceClientCC:
    """
    Salesforce REST API client using OAuth 2.0 Client Credentials flow and httpx.

    Typical endpoints:
      - Token:  POST {token_url} with grant_type=client_credentials
      - Base:   {instance_url}/services/data/v{api_version}

    Notes:
      - Salesforce update via PATCH often returns 204 No Content (success, no body).
      - Token response usually contains instance_url. If not, we derive it from token_url.
    """

    def __init__(self, cfg: SalesforceCCConfig, timeout_s: float = 30.0) -> None:
        self.cfg = cfg
        self.timeout_s = timeout_s
        self.access_token: Optional[str] = None
        self.instance_url: Optional[str] = None
        self._properties: Dict[str, PropertyData] | None = None

        # RecordTypeIds from env, exposed as namespace:
        # client.record_types.account.business
        # client.record_types.account.person
        self.RecordTypes = RecordTypes(
            Account=AccountRecordTypes(
                Business= _require_record_type_env("SF_ACCOUNT_RT_BUSINESS"),
                Person= _require_record_type_env("SF_ACCOUNT_RT_PERSONAL"),
    )
        )

        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout_s),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SalesforceClientCC":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def authenticate(self) -> None:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
        }

        r = self._client.post(self.cfg.token_url, data=data)
        if r.status_code != 200:
            raise RuntimeError(f"Salesforce auth failed ({r.status_code}): {r.text}")

        payload = r.json()
        self.access_token = payload["access_token"]
        self.instance_url = payload.get("instance_url") or self._derive_instance_url_from_token_url()

        # Switch client headers to Bearer for API calls
        self._client.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        })

    def _derive_instance_url_from_token_url(self) -> str:
        # token_url like https://X/services/oauth2/token -> instance_url https://X
        marker = "/services/oauth2/token"
        if marker in self.cfg.token_url:
            return self.cfg.token_url.split(marker, 1)[0]
        # Fallback: just strip path (best-effort)
        u = httpx.URL(self.cfg.token_url)
        return f"{u.scheme}://{u.host}"

    def _base(self) -> str:
        if not self.instance_url:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return f"{self.instance_url}/services/data/v{self.cfg.api_version}"
    
    def load_properties_by_fmtg_id(self) -> Dict[str, PropertyData]:
        soql = """
            SELECT
                Id,
                Name,
                ProtelID__c,
                ApaleoID__c,
                FMTGID__c
            FROM Property__c
            LIMIT 100
        """

        result = self.query(soql)

        properties: Dict[str, PropertyData] = {}

        for r in result.get("records", []):
            fmtg_id = r.get("FMTGID__c")
            if not fmtg_id:
                continue

            properties[fmtg_id] = PropertyData(
                sf_id=r["Id"],
                name=r.get("Name"),
                protel_id=r.get("ProtelID__c"),
                apaleo_id=r.get("ApaleoID__c"),
            )

        return properties
    

    def properties(self) -> Dict[str, PropertyData]:
        if self._properties is None:
            self._properties = self.load_properties_by_fmtg_id()
        return self._properties
    

    def create_account(self, name: str, record_type: RecordTypeId, **fields: Any) -> str:
        url = f"{self._base()}/sobjects/Account/"
        body: Dict[str, Any] = {"Name": name, "RecordTypeId": record_type, **fields}
        print(body)
        r = self._client.post(url, json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create Account failed ({r.status_code}): {r.text}")

        return r.json()["id"]

    def create_account_bus(self, name: str, **fields: Any) -> str:
        url = f"{self._base()}/sobjects/Account/"
        body: Dict[str, Any] = {"Name": name, **fields}

        r = self._client.post(url, json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create Account failed ({r.status_code}): {r.text}")

        return r.json()["id"]
    
    def create_account_business(self, **fields: Any) -> str:
        record_type = self.RecordTypes.Account.Business
        url = f"{self._base()}/sobjects/Account/"
        #body: Dict[str, Any] = {"SourceOrigin__pc": record_type, **fields}
        body: Dict[str, Any] = {"RecordTypeId": record_type, **fields}

        r = self._client.post(url, json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create Account failed ({r.status_code}): {r.text}")

        return r.json()["id"]
    

    def create_loyalty_program_member(self, payload: Dict[str, Any]) -> str:

        url = f"{self._base()}/sobjects/LoyaltyProgramMember/"

        r = self._client.post(url, json=payload)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create LoyaltyProgramMember failed ({r.status_code}): {r.text}")

        return r.json()["id"]
    
    def create_account_person(self, payload: Dict[str, Any]) -> str:
        record_type = self.RecordTypes.Account.Person
        url = f"{self._base()}/sobjects/Account/"

        r = self._client.post(url, json=payload)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create Account failed ({r.status_code}): {r.text}")

        return r.json()["id"]

    def upsert_object_by_external_id(
        self,
        object_name: str,
        external_id_field: str,
        external_id_value: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        True upsert:
          PATCH /sobjects/<ObjectName>/<ExternalIdField>/<ExternalIdValue>

        Returns a small result dict indicating created/updated and maybe id.
        """
        url = f"{self._base()}/sobjects/{object_name}/{external_id_field}/{external_id_value}"

        r = self._client.patch(url, json=payload)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Upsert failed ({r.status_code}): {r.text}")

        if r.status_code == 201:
            data = r.json()
            return {"action": "created", "id": data.get("id")}
        if r.status_code == 204:
            return {"action": "updated", "id": None}

        # Sometimes Salesforce returns 200 with a body
        try:
            data = r.json()
        except Exception:
            data = {}
        return {"action": "success", "id": data.get("id")}
    
    

    def upsert_account_by_external_id(
        self,
        external_id_field: str,
        external_id_value: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        True upsert:
          PATCH /sobjects/Account/<ExternalIdField>/<ExternalIdValue>

        Returns a small result dict indicating created/updated and maybe id.
        """
        url = f"{self._base()}/sobjects/Account/{external_id_field}/{external_id_value}"

        r = self._client.patch(url, json=payload)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Upsert failed ({r.status_code}): {r.text}")

        if r.status_code == 201:
            data = r.json()
            return {"action": "created", "id": data.get("id")}
        if r.status_code == 204:
            return {"action": "updated", "id": None}

        # Sometimes Salesforce returns 200 with a body
        try:
            data = r.json()
        except Exception:
            data = {}
        return {"action": "success", "id": data.get("id")}
    
    def update_account_by_id(
        self,
        account_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Update an existing Account by Salesforce Id.

        PATCH /sobjects/Account/<Id>

        - Requires the record to exist
        - Will NOT create a new record
        - Successful update usually returns 204 No Content

        Returns:
            {
                "action": "updated",
                "id": <account_id>
            }
        """
        if not account_id:
            raise ValueError("account_id must be provided")

        url = f"{self._base()}/sobjects/Account/{account_id}"

        r = self._client.patch(url, json=payload)
        if r.status_code not in (200, 204):
            raise RuntimeError(
                f"Update Account failed ({r.status_code}): {r.text}"
            )

        # Salesforce usually returns 204 No Content on success
        if r.status_code == 204:
            return {"action": "updated", "id": account_id}

        # Rarely, Salesforce returns 200 with a body
        try:
            data = r.json()
        except Exception:
            data = {}

        return {"action": "updated", "id": data.get("id", account_id)}
    

    def create_reservation(self, reservation_id: str, **fields: Any) -> str:
        url = f"{self._base()}/sobjects/Reservation__c/"
        body: Dict[str, Any] = {"ReservationID__c": reservation_id, **fields}

        r = self._client.post(url, json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Create Reservation failed ({r.status_code}): {r.text}")

        return r.json()["id"]
    


    def composite(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._base()}/composite/"

        r = self._client.post(url, json=payload)
        data = {}
        try:
            data = r.json()
        except Exception:
            pass

        if r.status_code not in (200, 201):
            raise RuntimeError(f"Composite request failed ({r.status_code}): {r.text}")

        results: Dict[str, Any] = {}
        errors = []

        for item in data.get("compositeResponse", []):
            ref_id = item.get("referenceId")
            status = item.get("httpStatusCode")
            body = item.get("body", {})

            if status is None:
                continue

            if status >= 400:
                errors.append({"referenceId": ref_id, "httpStatusCode": status, "body": body})
            else:
                # return id if present, otherwise body
                if isinstance(body, dict) and "id" in body:
                    results[ref_id] = body["id"]
                else:
                    results[ref_id] = body

        if errors:
            # Show the *real* failing subrequest(s)
            raise RuntimeError(f"Composite subrequest(s) failed: {errors}")

        return results
        
    
    def upsert_reservation_payload(self, reservation_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:

        url = f"{self._base()}/sobjects/Reservation__c/ReservationID__c/{reservation_id}"
       
        r = self._client.patch(url, json=payload)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Upsert failed ({r.status_code}): {r.text}")

        if r.status_code == 201:
            data = r.json()
            return {"action": "created", "id": data.get("id")}
        if r.status_code == 204:
            return {"action": "updated", "id": None}

        # Sometimes Salesforce returns 200 with a body
        try:
            data = r.json()
        except Exception:
            data = {}
        return {"action": "success", "id": data.get("id")}
    
    def upsert_reservation_cluster_id(self, cluster_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:

        url = f"{self._base()}/sobjects/Reservation__c/ClusterID__c/{cluster_id}"
       
        r = self._client.patch(url, json=payload)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Upsert failed ({r.status_code}): {r.text}")

        if r.status_code == 201:
            data = r.json()
            return {"action": "created", "id": data.get("id")}
        if r.status_code == 204:
            return {"action": "updated", "id": None}

        # Sometimes Salesforce returns 200 with a body
        try:
            data = r.json()
        except Exception:
            data = {}
        return {"action": "success", "id": data.get("id")}
    

    # ------------------------------------------------------------------
        # Bulk API 2.0
    # ------------------------------------------------------------------

    def bulk_create_job(self, object_name: str, external_id_field: str) -> str:
        """
        Erstellt einen Bulk API 2.0 Upsert-Job.
        Gibt die Job-ID zurück.
        """
        url = f"{self._base()}/jobs/ingest"
        body = {
            "object": object_name,
            "operation": "upsert",
            "externalIdFieldName": external_id_field,
            "contentType": "CSV",
            "lineEnding": "LF",
        }
        r = self._client.post(url, json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Bulk create job failed ({r.status_code}): {r.text}")
        return r.json()["id"]

    def bulk_upload_csv(self, job_id: str, csv_data: str) -> None:
        """
        Lädt die CSV-Daten in einen offenen Bulk-Job.
        Muss vor bulk_close_job aufgerufen werden.
        """
        url = f"{self._base()}/jobs/ingest/{job_id}/batches"
        r = self._client.put(
            url,
            content=csv_data.encode("utf-8"),   
            headers={
                "Content-Type": "text/csv",
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Bulk upload CSV failed ({r.status_code}): {r.text}")

    def bulk_close_job(self, job_id: str) -> None:
        """
        Setzt den Job-Status auf UploadComplete.
        Salesforce startet danach die Verarbeitung.
        """
        url = f"{self._base()}/jobs/ingest/{job_id}"
        r = self._client.patch(url, json={"state": "UploadComplete"})
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Bulk close job failed ({r.status_code}): {r.text}")
    
    
    def upsert_account_payload(self, record_type: AccountRecordTypes, cluster_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:

        #record_type = self.RecordTypes.Account.Business
        url = f"{self._base()}/sobjects/Account/ClusterID__c/{cluster_id}"
       
        r = self._client.patch(url, json=payload)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Upsert failed ({r.status_code}): {r.text}")

        if r.status_code == 201:
            data = r.json()
            return {"action": "created", "id": data.get("id")}
        if r.status_code == 204:
            return {"action": "updated", "id": None}

        # Sometimes Salesforce returns 200 with a body
        try:
            data = r.json()
        except Exception:
            data = {}
        return {"action": "success", "id": data.get("id")}



    def query(self, soql: str) -> Dict[str, Any]:
        url = f"{self._base()}/query"
        r = self._client.get(url, params={"q": soql})
        if r.status_code != 200:
            raise RuntimeError(f"SOQL query failed ({r.status_code}): {r.text}")
        return r.json()
    
    def query_all(self, soql: str) -> Dict[str, Any]:
        url = f"{self._base()}/query"
        params = {"q": soql}

        all_records: List[Dict[str, Any]] = []
        done = False
        next_url = None
        last_response: Dict[str, Any] = {}

        while not done:
            if next_url:
                r = self._client.get(next_url)
            else:
                r = self._client.get(url, params=params)

            if r.status_code != 200:
                raise RuntimeError(f"SOQL query failed ({r.status_code}): {r.text}")

            data = r.json()
            last_response = data

            all_records.extend(data.get("records", []))
            done = data.get("done", True)

            next_records_url = data.get("nextRecordsUrl")
            next_url = f"{self.instance_url}{next_records_url}" if next_records_url else None

        return {
            **last_response,
            "records": all_records,
            "totalSize": len(all_records),
            "done": True,
        }



    def describe_account(self) -> Dict[str, Any]:
        url = f"{self._base()}/sobjects/Account/describe"
        r = self._client.get(url)
        if r.status_code != 200:
            raise RuntimeError(f"Describe Account failed ({r.status_code}): {r.text}")
        return r.json()


def load_salesforce_cc_config_from_env() -> SalesforceCCConfig:
    required = ["SF_TOKEN_URL", "SF_CLIENT_ID", "SF_CLIENT_SECRET"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    return SalesforceCCConfig(
        token_url=os.getenv("SF_TOKEN_URL", ""),
        client_id=os.getenv("SF_CLIENT_ID", ""),
        client_secret=os.getenv("SF_CLIENT_SECRET", ""),
        api_version=os.getenv("SF_API_VERSION", "60.0"),
    )



