"""
Schritt 1 (READ-ONLY): Test-Zeile aus dem Camping-Batch waehlen und live gegen SF verifizieren.

- Kandidat: erste Zeile des Batches mit plain-ASCII-Email, CPE vorhanden, Consent offen
- Live-Checks: CPE-EmailAddress == Zeilen-Email, Account existiert mit gleicher PersonEmail
- Ergebnis landet in test_state.json -> User reviewt, bevor Schritt 2 laeuft
"""
from common import BATCH_ID, get_db, get_sf, save_state


def main():
    db = get_db()
    rows = db.fetch_all(
        """
        SELECT row_id, first_name, last_name, email,
               sf_account_id, sf_person_contact_id, sf_cp_email_id
        FROM   crm_imp_person_accounts
        WHERE  _batch_id             = %s
          AND  _excluded             = 0
          AND  consent_camping       = 1
          AND  sf_cp_email_id        IS NOT NULL
          AND  _consent_processed_at IS NULL
          AND  email REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+$'
        ORDER BY row_id
        LIMIT 1
        """,
        (BATCH_ID,),
    )
    if not rows:
        raise SystemExit("Kein Kandidat gefunden - Abbruch.")
    row = rows[0]
    print("Kandidat aus MySQL:")
    for k, v in row.items():
        print(f"  {k}: {v}")

    sf = get_sf()
    problems = []

    cpe = sf.query(
        f"SELECT Id, EmailAddress, ParentId FROM ContactPointEmail "
        f"WHERE Id = '{row['sf_cp_email_id']}'"
    ).get("records", [])
    if not cpe:
        problems.append("CPE nicht in SF gefunden!")
    else:
        print(f"\nCPE live: Id={cpe[0]['Id']} EmailAddress={cpe[0]['EmailAddress']} "
              f"ParentId={cpe[0]['ParentId']}")
        if (cpe[0]["EmailAddress"] or "").lower() != row["email"].lower():
            problems.append(
                f"CPE-Email '{cpe[0]['EmailAddress']}' != Batch-Email '{row['email']}'"
            )

    acc = sf.query(
        f"SELECT Id, FirstName, LastName, PersonEmail FROM Account "
        f"WHERE Id = '{row['sf_account_id']}'"
    ).get("records", [])
    if not acc:
        problems.append("Account nicht in SF gefunden!")
    else:
        print(f"Account live: Id={acc[0]['Id']} Name={acc[0]['FirstName']} {acc[0]['LastName']} "
              f"PersonEmail={acc[0]['PersonEmail']}")
        if (acc[0]["PersonEmail"] or "").lower() != row["email"].lower():
            problems.append(
                f"Account-PersonEmail '{acc[0]['PersonEmail']}' != Batch-Email '{row['email']}'"
            )

    existing = sf.query(
        f"SELECT Id, Name, PrivacyConsentStatus FROM ContactPointConsent "
        f"WHERE ContactPointId = '{row['sf_cp_email_id']}'"
    ).get("records", [])
    print(f"Bestehende Consents an diesem CPE: {len(existing)}")
    if existing:
        problems.append(f"CPE hat bereits {len(existing)} Consent(s) - anderen Kandidaten waehlen.")

    sf.close()

    if problems:
        print("\nPROBLEME:")
        for p in problems:
            print(f"  - {p}")
        raise SystemExit("Kandidat NICHT ok - Abbruch, nichts gespeichert.")

    save_state({
        "step": "01_picked",
        "row_id": row["row_id"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "email": row["email"],
        "sf_account_id": row["sf_account_id"],
        "sf_cp_email_id": row["sf_cp_email_id"],
    })
    print("\nKandidat verifiziert. -> User-Review, dann Schritt 2 (Insert) nach explizitem GO.")


if __name__ == "__main__":
    main()
