"""
Schritt 2 (EIN PROD-WRITE, nur nach explizitem GO):
Einen ContactPointConsent per REST anlegen - VOLLES Payload, identisch zum
Bulk-Mapper in insert_consents_bulk.py (inkl. ConsentKey__c/HotelName__c/Region__c).

Historie: Lauf 1 (2026-08-17, Record 0ZXTe000000YtenOAC) war der Automation-Test
OHNE ConsentKey__c -> blieb leer (NOT_AUTOMATED), Record danach im UI geloescht.
Lauf 2 zeigt das finale Bild, wie alle 89.887 Bulk-Records aussehen werden.
"""
from datetime import datetime, timezone

from common import (
    CONSENT_NAME, DATA_USE_PURPOSE_ID, HOTEL_NAME, PROPERTY_ID, REGION,
    get_sf, load_state, save_state,
)


def main():
    state = load_state()
    if state.get("step") != "01_picked":
        raise SystemExit(f"Unerwarteter State '{state.get('step')}' - erst 01 laufen lassen.")
    if state.get("consent_id"):
        raise SystemExit(f"Consent existiert bereits ({state['consent_id']}) - Abbruch.")

    sf = get_sf()

    # Idempotenz-Guard: existiert schon ein Consent fuer CPE+Purpose+Property?
    dup = sf.query(
        f"SELECT Id FROM ContactPointConsent "
        f"WHERE ContactPointId = '{state['sf_cp_email_id']}' "
        f"AND DataUsePurposeId = '{DATA_USE_PURPOSE_ID}' "
        f"AND Property__c = '{PROPERTY_ID}'"
    ).get("records", [])
    if dup:
        sf.close()
        raise SystemExit(f"Consent existiert bereits in SF ({dup[0]['Id']}) - Abbruch.")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    record = {
        "Name":                 CONSENT_NAME,
        "ContactPointId":       state["sf_cp_email_id"],
        "DataUsePurposeId":     DATA_USE_PURPOSE_ID,
        "Property__c":          PROPERTY_ID,
        "PrivacyConsentStatus": "OptIn",
        "CaptureDate":          now,
        "EffectiveFrom":        now,
        "CaptureSource":        "camping",
        "SourceSystem__c":      "camping",
        "ConsentKey__c":        f"{state['sf_cp_email_id']}|{DATA_USE_PURPOSE_ID}|{PROPERTY_ID}",
        "HotelName__c":         HOTEL_NAME,
        "Region__c":            REGION,
    }
    print("Payload:")
    for k, v in record.items():
        print(f"  {k}: {v}")

    url = f"{sf._base()}/sobjects/ContactPointConsent/"
    r = sf._client.post(url, json=record)
    if r.status_code not in (200, 201):
        sf.close()
        raise RuntimeError(f"Insert fehlgeschlagen ({r.status_code}): {r.text}")
    consent_id = r.json()["id"]

    print(f"\nConsent angelegt: {consent_id}")
    print(f"UI: <org-url>/lightning/r/ContactPointConsent/{consent_id}/view")

    # Read-back-Verifikation (Schritt 3/4 entfallen bei vollem Payload)
    rec = sf.query(
        f"SELECT Id, Name, ConsentKey__c, HotelName__c, Region__c, Property__c, "
        f"PrivacyConsentStatus, CaptureSource, SourceSystem__c "
        f"FROM ContactPointConsent WHERE Id = '{consent_id}'"
    )["records"][0]
    sf.close()
    print("\nRead-back:")
    for k, v in rec.items():
        if k != "attributes":
            print(f"  {k}: {v}")

    state.update({"step": "02_inserted_full", "consent_id": consent_id, "capture_date": now})
    save_state(state)
    print("-> UI pruefen, dann Schritt 5 (MySQL mark processed) nach GO.")


if __name__ == "__main__":
    main()
