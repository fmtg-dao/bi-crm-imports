"""Download the Camping workbook from SharePoint and load its first worksheet."""

import os
from pathlib import Path
from urllib.parse import quote, urlparse

import polars as pl
import requests
from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).parent
LOCAL_WORKBOOK = PROJECT_DIR / "data" / "source" / "2607_Data cleaned - Grubhof.xlsx"
SHAREPOINT_PATH = (
    "02-Business_Intelligence/1-Shared_Data/Camping/"
    "2607_Data cleaned - Grubhof.xlsx"
)
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"


def get_access_token() -> str:
    response = requests.post(
        f"https://login.microsoftonline.com/{os.environ['SHAREPOINT_TENANT_ID']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": os.environ["SHAREPOINT_CLIENT_ID"],
            "client_secret": os.environ["SHAREPOINT_CLIENT_SECRET"],
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def download_workbook() -> Path:
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    site_url = urlparse(os.environ["SHAREPOINT_SITE_URL"])

    site_response = requests.get(
        f"{GRAPH_ROOT}/sites/{site_url.hostname}:{site_url.path.rstrip('/')}",
        headers=headers,
        timeout=60,
    )
    site_response.raise_for_status()
    site_id = site_response.json()["id"]

    drive_response = requests.get(
        f"{GRAPH_ROOT}/sites/{site_id}/drive",
        headers=headers,
        timeout=60,
    )
    drive_response.raise_for_status()
    drive_id = drive_response.json()["id"]

    encoded_path = quote(SHAREPOINT_PATH, safe="/")
    content_url = (
        f"{GRAPH_ROOT}/drives/{drive_id}/root:/"
        f"{encoded_path}:/content"
    )

    LOCAL_WORKBOOK.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(
        content_url,
        headers=headers,
        stream=True,
        timeout=(30, 600),
    ) as response:
        response.raise_for_status()
        with LOCAL_WORKBOOK.open("wb") as output:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    output.write(chunk)

    return LOCAL_WORKBOOK


def main() -> None:
    load_dotenv(PROJECT_DIR / ".env")
    workbook_path = download_workbook()
    dataframe = pl.read_excel(workbook_path, sheet_id=1)

    print(f"Workbook: {workbook_path}")
    print(f"Shape: {dataframe.shape}")
    print(f"Columns: {dataframe.columns}")


if __name__ == "__main__":
    main()
