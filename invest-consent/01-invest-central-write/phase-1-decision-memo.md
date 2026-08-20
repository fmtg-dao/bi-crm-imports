# Phase 1: decision memo

[Overview](overview.md)

## Goal

Every open decision resolved, by Arsal, by Oleg, or by a query, before any
staging happens. Each item below names the decider and a recommendation.

## Decisions

**D1. Who executes the write.** RESOLVED (Arsal, 2026-08-20): we execute.
Remaining action: tell Oleg his API job must not also write invest_central,
or everyone gets the consent twice.

**D2. Population selection, OR vs AND.** RESOLVED (Arsal, 2026-08-20): the
6,146 (InvestCustomer__pc = 'True' AND InvestmentStatus__pc set AND
<> 'Owner').

**D3. One consent per account or per ContactPointEmail.** DEMOTED to a note
(Arsal, 2026-08-20: not a real decision). Per CPE is what the pipeline does
with no extra code; per account would mean winner-picking logic for the 74
dual-CPE accounts that nobody asked for. Only consequence worth knowing: the
record count is 6,219, not 6,145. 6,071 accounts have one CPE, 74 have two,
1 has none (recon query 16).

**D4. Exclusion-list semantics outside the population.** 11 of the 108
excluded emails are in the population (OptOut). Of the rest: 46 match other
person accounts, 10 match only a lead, 49 match nothing. Pre-emptive OptOut
for the 46, or ignore everyone outside the population? Decider: Oleg.

**D5. ConsentKey__c and Name for a central consent.** RESOLVED by query
(2026-08-20). Name = the lowercase purpose name (`invest_central`), no
Property__c, and ConsentKey__c = `{CPE Id}|0ZWTe0000000X5dOAE|CENTRAL`.
Evidence: of 361,261 keyed marketing_central rows in the mirror, 361,260 have
ConsentKey__c starting with the CPE Id and ending
`|0ZWTe0000000X7FOAU|CENTRAL`; the 569k legacy gms rows carry no key, every
newer system-written row does. Camping's live records show the same key
structure with the property Id in the third slot.

**D6. CaptureContactPointType.** RESOLVED by live SOQL (2026-08-20): null on
940,117 of 940,118 marketing_central records and on the camping records.
Salesforce does not default it. Omit the field.

**D7. The one population account with no ContactPointEmail.** DEMOTED to a
note: the script requires `sf_cp_email_id IS NOT NULL`, so the account is
skipped by construction. It goes on the data-fix track; no consent-write
decision exists here.

**D8. Lead twins.** 2,537 New/Open leads share an email with the population
and have no RelatedPersonAccount__c link. The consent write ignores leads, but
if invest marketing selects leads too, these have no consent record. Decider:
Oleg, as a flagged risk, not a blocker.

## Verification

Memo answered in writing (Oleg's reply plus Arsal's picks recorded here), D5
and D6 queries run and their results pasted into this file.
