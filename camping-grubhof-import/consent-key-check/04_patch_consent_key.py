"""
Schritt 4 (WRITE, nur wenn Schritt 3 = NOT_AUTOMATED, nur nach explizitem GO):
Denormalisierte Felder auf dem Test-Record nachtragen, damit er den 89.886
Bulk-Records gleicht: ConsentKey__c, HotelName__c, Region__c
(Feld-Diff gegen Migrations-Record 0ZXTe000000017VOAQ, 2026-08-17).
"""
from common import (
    DATA_USE_PURPOSE_ID, HOTEL_NAME, PROPERTY_ID, REGION,
    get_sf, load_state, save_state,
)


def main():
    state = load_state()
    if state.get("consent_key_verdict") != "NOT_AUTOMATED":
        raise SystemExit(f"Verdict ist '{state.get('consent_key_verdict')}' - Patch nicht noetig/erlaubt.")

    key = f"{state['sf_cp_email_id']}|{DATA_USE_PURPOSE_ID}|{PROPERTY_ID}"
    payload = {"ConsentKey__c": key, "HotelName__c": HOTEL_NAME, "Region__c": REGION}
    print("Patch:")
    for k, v in payload.items():
        print(f"  {k}: {v}")

    sf = get_sf()
    url = f"{sf._base()}/sobjects/ContactPointConsent/{state['consent_id']}"
    r = sf._client.patch(url, json=payload)
    if r.status_code not in (200, 204):
        sf.close()
        raise RuntimeError(f"Patch fehlgeschlagen ({r.status_code}): {r.text}")

    rec = sf.query(
        f"SELECT Id, ConsentKey__c, HotelName__c, Region__c FROM ContactPointConsent "
        f"WHERE Id = '{state['consent_id']}'"
    )["records"][0]
    sf.close()
    print(f"Bestaetigt: ConsentKey__c={rec['ConsentKey__c']} | "
          f"HotelName__c={rec['HotelName__c']} | Region__c={rec['Region__c']}")
    if rec["ConsentKey__c"] != key or rec["HotelName__c"] != HOTEL_NAME or rec["Region__c"] != REGION:
        raise SystemExit("ACHTUNG: Felder stimmen nicht mit erwarteten Werten ueberein!")

    state.update({"step": "04_patched", "consent_key_observed": rec["ConsentKey__c"]})
    save_state(state)
    print("-> Schritt 5 (MySQL mark processed) nach GO.")


if __name__ == "__main__":
    main()
