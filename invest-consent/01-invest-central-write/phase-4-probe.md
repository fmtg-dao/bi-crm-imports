# Phase 4: probe record

[Overview](overview.md)

## Goal

One real invest_central ContactPointConsent in production, verified in the UI
and by SOQL, before any bulk job. Camping did the same (single REST insert,
then Carmen signed off a template record).

## Changes

- Insert one consent via the existing REST path for a single agreed test
  account from the OptIn batch.
- SOQL probes: the CPC by Id (status, purpose, CaptureDate, Name,
  ConsentKey__c, CaptureContactPointType as Salesforce stored them), the CPE
  it hangs on (`ParentId`, `EmailAddress`), and a count of consents on that
  CPE.
- Business sign-off on the probe record (Carmen or whoever Oleg names) before
  phase 5.

## Verification

Probe record looks exactly like the D5/D6 expectations; sign-off recorded in
the load-day log. If anything differs, back to phase 3, not onward.
