# Day 19a — GATE Step 1 of 4: Credentials (the keys)

**Sprint:** Week 4.5 — Convert capability into access and revenue
**Status:** 🟡 In Progress — both credentials confirmed live (IBD registered, CCIB membership approved); CIB certification approval and shareholder registry signature still open
**Depends on:** Days 15–19 traction work
**Part of:** [gate_credentials_and_funding.md](gate_credentials_and_funding.md) — **Step 1 of 4**
**Next:** [day19b_gate_reference.md](day19b_gate_reference.md)
**Runs in parallel:** [day19e_two_rivers_local_network.md](day19e_two_rivers_local_network.md) —
Two Rivers CDC walk-in (local network + AEP fit); don't serialize it behind this file
**Blocks:** Day 20+ (MCP/agent build)

---

## Objective
Get the keys that open procurement doors. Nothing in federal/provincial procurement unlocks without these. The two certifications are independent processes — start both, they run in parallel.

## Why This Matters
- IBD = the **federal** door (PSIB set-asides + the mandatory 5% target). Buyers search the directory by NAICS — wrong codes = invisible.
- CCAB = the **Ontario + Crown-corp** door.
- ISC runs **post-award ownership audits**. You are legitimately 100% Indigenous-owned — keep it airtight and it becomes an advantage.

## Steps — broken into 4 small sessions (added 2026-07-14)

> Do these as separate sittings if needed. Session 1 unblocks everything else — do it first.

### Session 1 — One folder, all documents (~30 min, no websites)

Create a folder (e.g. `~/Documents/GeminiMatrix-Credentials/`) and drop scanned PDFs of:

- [ ] **Certificate / Articles of Incorporation** — Gemini Matrix Consulting Inc.
- [ ] **Shareholder register or share certificate** showing Mike = 100% owner
- [ ] **Proof of Indigenous identity** — status card (both sides) or equivalent
- [ ] **CRA Business Number** (any CRA correspondence showing the BN)
- [ ] Director register / corporate profile report if you have one
- [ ] A 2–3 sentence business description you'll reuse verbatim in both portals, e.g.:
  > *"Gemini Matrix Consulting Inc. is a 100% Indigenous-owned AI/ML systems integrator based in Grand River territory, Ontario. We build data pipelines, machine-learning anomaly detection, APIs, and dashboards for government and Indigenous organizations. Flagship product: Water-Intel, a source-water intelligence platform (water.geminimatrixinc.com)."*

**Done when:** every file above is in the folder. If one is missing (e.g. can't find the shareholder register), note it and order/request it today — that's the long pole.

### Session 2 — IBD verification (~30–45 min, one portal) — ✅ registration confirmed

Portal: https://services.sac-isc.gc.ca/REA-IBD/

**Confirmed 2026-07-22:** IBD registration is active and verified (Gemini Matrix Consulting Inc., postal code L9T7M7). Address updated to 1574 Chiefswood Rd, PO Box 846, Ohsweken, ON N0A 1M0.

- [x] Log in and pull up the existing Gemini Matrix listing
- [x] Confirm the ownership/control attestation is current (100% Indigenous-owned)
- [ ] Update remaining basics: phone, email, **website = geminimatrixinc.com** (buyers click this) — address already updated, others not yet confirmed
- [ ] Replace/confirm the business description with the Session 1 blurb (keywords buyers search: *artificial intelligence, machine learning, data analytics, software development, dashboards*)
- [ ] **Fix the NAICS codes** — this is the entire point. You want the codes buyers search for AI/software work:
  - **541514 / 541511 — Custom computer programming services** (primary)
  - **541512 — Computer systems design services** (primary)
  - **541690 or 541611 — consulting** (keep as secondary only)
  - Add, don't just replace — multiple codes are allowed; more codes = more search hits
- [ ] Save + screenshot the updated listing for your records

**Done when:** listing shows correct NAICS + website + AI/ML description. (Registration itself is done; this remaining list is the optimization pass.)

### Session 3 — CCIB application (~45–60 min) — ✅ membership approved

Portal: https://www.ccab.com/main/ccab_member/certification/ (CCAB rebranded to CCIB — Canadian Council for Indigenous Business)

**Confirmed 2026-07-22:** CCIB membership application submitted, fee paid, and **membership approved**. CIB (Certified Indigenous Business) certification application submitted as the next step beyond membership.

- [x] Create an account / confirm CCIB membership is required before CIB certification — confirmed required, membership now approved
- [x] Start the Certified Indigenous Business (CIB) certification application
- [x] Upload the Session 1 documents
- [x] Paste the same business description
- [x] Pay the membership fee
- [x] **Submit** the CIB certification application

**Still open:**
- [ ] Confirm whether CIB *certification* approval is complete — this is separate from and comes after membership approval
- [ ] Sign and date the Shareholders Registry (drafted, unsigned — this is likely a blocker on certification approval)
- [ ] Optional consistency follow-up: update the registered office address with ServiceOntario if the Articles still show the old Milton address

**Done when:** CIB certification status = approved (not just membership).

### Session 4 — File + follow-up hooks (~15 min)

- [ ] Save all confirmation numbers/emails into the credentials folder
- [ ] Calendar reminder **+2 weeks**: check CCAB application status
- [ ] Calendar reminder **+2 weeks**: confirm IBD listing changes went live (search yourself in the public directory)
- [ ] Note in `PROGRESS.md`: credentials in flight

### Integrity housekeeping (standing rules)
- [ ] Credentials folder = the audit-ready ownership record; keep it current
- [ ] Never overstate certification status in any bid — "CCAB application submitted" until it's granted
- [ ] (Later, not now) CAMSC certification for corporate supply chains

## Acceptance Criteria (Step 1 done when…)
- ✅ IBD listing verified current; active registration confirmed and address updated (2026-07-22)
- ✅ CCIB membership approved (2026-07-22); CIB certification application submitted, approval still pending explicit confirmation
- 🟡 Ownership/integrity documents are substantially assembled; shareholder registry still needs signature/date — likely gates CIB certification approval, so this is now the long pole on Step 1

## Notes
- ✅ IBD status verified: Gemini Matrix Consulting Inc. is Active Registration (postal code L9T7M7).
- This is a business task, not engineering. The product is good enough to pitch.
- Reference: `planning/indigenous-track/00_strategy_overview.md` (credentials map + source URLs).
- These two applications can be in flight simultaneously — don't serialize them.
