"""
Schritt 3 (READ-ONLY): ConsentKey__c des Test-Records pruefen - sofort und nach 60s
(Fenster fuer asynchrone Flows).

- Key befuellt  -> SF-seitige Automation existiert -> ConsentKey__c-Zeile aus dem
                   Mapper in insert_consents_bulk.py ENTFERNEN
- Key leer      -> keine Automation -> Mapper-Zeile bleibt; Schritt 4 patcht den Test-Record
"""
import time

from common import get_sf, load_state, save_state

FIELDS = "Id, ConsentKey__c, Property__c, PrivacyConsentStatus, CreatedDate, LastModifiedDate"


def fetch(sf, consent_id: str) -> dict:
    recs = sf.query(
        f"SELECT {FIELDS} FROM ContactPointConsent WHERE Id = '{consent_id}'"
    ).get("records", [])
    if not recs:
        raise SystemExit(f"Consent {consent_id} nicht gefunden!")
    return recs[0]


def show(label: str, rec: dict) -> None:
    print(f"\n{label}:")
    for k in ("Id", "ConsentKey__c", "Property__c", "PrivacyConsentStatus",
              "CreatedDate", "LastModifiedDate"):
        print(f"  {k}: {rec.get(k)}")


def main():
    state = load_state()
    if not state.get("consent_id"):
        raise SystemExit("Kein consent_id im State - erst 02 laufen lassen.")

    sf = get_sf()
    rec1 = fetch(sf, state["consent_id"])
    show("Check sofort", rec1)

    key = rec1.get("ConsentKey__c")
    if not key:
        print("\nKey leer - warte 60s auf evtl. asynchrone Automation ...")
        time.sleep(60)
        rec2 = fetch(sf, state["consent_id"])
        show("Check nach 60s", rec2)
        key = rec2.get("ConsentKey__c")
    sf.close()

    if key:
        verdict = "AUTOMATED"
        print(f"\nERGEBNIS: ConsentKey__c wurde automatisch befuellt: {key}")
        print("-> Oleg hat recht. ConsentKey__c-Zeile aus insert_consents_bulk.py entfernen!")
    else:
        verdict = "NOT_AUTOMATED"
        print("\nERGEBNIS: ConsentKey__c bleibt leer - keine SF-seitige Automation.")
        print("-> Mapper-Zeile bleibt. Schritt 4 patcht diesen Record (nach GO).")

    state.update({"step": "03_checked", "consent_key_verdict": verdict,
                  "consent_key_observed": key})
    save_state(state)


if __name__ == "__main__":
    main()
