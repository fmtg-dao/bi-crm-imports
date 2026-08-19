"""
Schritt 5 (MySQL-WRITE, nur nach explizitem GO):
_consent_processed_at fuer die Test-Zeile setzen, damit der Bulk-Lauf sie ueberspringt.
Danach muss der Bulk-SELECT genau 89.886 Zeilen liefern.
"""
from common import BATCH_ID, get_db, load_state, save_state


def main():
    state = load_state()
    if state.get("step") not in ("02_inserted_full", "04_patched"):
        raise SystemExit(f"Unerwarteter State '{state.get('step')}' - Test-Consent muss vollstaendig sein.")

    db = get_db()
    db.execute(
        """
        UPDATE crm_imp_person_accounts
        SET    _consent_processed_at = NOW()
        WHERE  _batch_id = %s
          AND  row_id    = %s
          AND  _consent_processed_at IS NULL
        """,
        (BATCH_ID, state["row_id"]),
    )

    check = db.fetch_all(
        "SELECT row_id, email, _consent_processed_at FROM crm_imp_person_accounts "
        "WHERE _batch_id = %s AND row_id = %s",
        (BATCH_ID, state["row_id"]),
    )[0]
    print(f"row_id {check['row_id']} ({check['email']}): "
          f"_consent_processed_at = {check['_consent_processed_at']}")

    remaining = db.fetch_all(
        """
        SELECT COUNT(*) AS n FROM crm_imp_person_accounts
        WHERE _batch_id = %s AND _excluded = 0 AND consent_camping = 1
          AND sf_cp_email_id IS NOT NULL AND _consent_processed_at IS NULL
        """,
        (BATCH_ID,),
    )[0]["n"]
    print(f"Verbleibend fuer den Bulk-Lauf: {remaining} (erwartet: 89886)")

    state.update({"step": "05_marked_processed"})
    save_state(state)


if __name__ == "__main__":
    main()
