# Phase 1: decision memo

[Overview](overview.md)

## Goal

Every open decision resolved, by Arsal, by Oleg, or by a query, before any
staging happens. Each item below names the decider and a recommendation.

## Decisions

**D1. Who executes the write.** Oleg's mail describes his own API job writing
invest_central. If his job runs, this plan stops after the audit handover; if
we write, his job must not. Decider: Oleg. Blocking for phase 5.

**D2. Population selection, OR vs AND.** His mail reads ambiguously:
investment fields OR the InvestCustomer flag gives 7,057, AND minus Owners
gives 6,146. Working assumption is AND minus Owners per Arsal. Decider: Oleg.
Blocking for phase 2.

**D3. One consent per account or per ContactPointEmail.** 6,071 accounts have
exactly one CPE, 74 have two, 1 has none (recon query 16). Camping wrote per
CPE. Recommendation: per CPE, 6,219 records. Decider: Arsal.

**D4. Exclusion-list semantics outside the population.** 11 of the 108
excluded emails are in the population (OptOut). Of the rest: 46 match other
person accounts, 10 match only a lead, 49 match nothing. Pre-emptive OptOut
for the 46, or ignore everyone outside the population? Decider: Oleg.

**D5. ConsentKey__c and Name for a central consent.** Camping's key/property
fields are property-consent machinery; the shared central payload omits them.
Resolve by query: read one existing `marketing_central` ContactPointConsent
from the mirror and copy its Name convention and whether ConsentKey__c is set.
Recommendation: match marketing_central exactly. Decider: query, then Arsal
nods.

**D6. CaptureContactPointType.** Never sent by any previous import; unknown
whether Salesforce defaulted it. Resolve by query against an existing camping
ContactPointConsent. Decider: query.

**D7. The one population account with no ContactPointEmail.** It cannot
receive a consent. Create a CPE from PersonEmail first, or leave it out and
note it. Recommendation: leave it out, add it to the data-fix track. Decider:
Arsal.

**D8. Lead twins.** 2,537 New/Open leads share an email with the population
and have no RelatedPersonAccount__c link. The consent write ignores leads, but
if invest marketing selects leads too, these have no consent record. Decider:
Oleg, as a flagged risk, not a blocker.

## Verification

Memo answered in writing (Oleg's reply plus Arsal's picks recorded here), D5
and D6 queries run and their results pasted into this file.
