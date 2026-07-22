# Day 19d — GATE Step 4 of 4: First Revenue Path (the fastest door)

**Sprint:** Week 4.5 — Convert capability into access and revenue
**Status:** 🔲 Not Started
**Depends on:** [day19a_gate_credentials.md](day19a_gate_credentials.md) (credentials open the door)
**Part of:** [gate_credentials_and_funding.md](gate_credentials_and_funding.md) — **Step 4 of 4**
**Next:** Gate cleared → [day20_mcp_server.md](day20_mcp_server.md)
**Blocks:** Day 20+ (MCP/agent build)

---

## Objective
Open one real procurement conversation that can become first revenue — the fastest path is a **sole-source award under the PSIB threshold (~$40K)**, which needs no competition.

## Why This Matters
- Sole-source under threshold is the single fastest route to first dollars as a 100% Indigenous-owned firm.
- Every department under 5% pressure has an **Indigenous Procurement Coordinator** who answers email — because their numbers depend on it.
- First revenue (or a live procurement conversation) is what makes the company real, not just fundable.

## Steps

### Identify a sole-source opportunity
- [ ] Shortlist depts under 5% pressure with data/AI needs that fit the reusable engine: **ECCC, ISC, CIRNAC, DFO**
- [ ] Find the named **Indigenous Procurement Coordinator** for the target dept
- [ ] Draft a one-paragraph capability note tying the vertical engine to a concrete dept need
- [ ] Send the outreach; track in `docs/AI_OUTREACH_PLAYBOOK.md`

### Watch the tender feeds
- [ ] Set up monitoring on CanadaBuys for set-aside tenders matching the vertical: https://canadabuys.canada.ca/en
- [ ] Note the three doors: Set-Aside Program · Indigenous Participation Plan (IPP) · Sole-source under threshold
- [ ] **Automation (added 2026-07-15):** grant-intel (C:\_GIT\grant-intel, WORKPLAN Days 3–12)
      replaces this manual monitoring with a nightly ranked match feed for Gemini Matrix's
      profile — its Day 10 dogfood output is this step's shortlist. Manual alerts are the
      stopgap until then.

## Acceptance Criteria (Step 4 done when…)
- [ ] At least one sole-source / set-aside conversation is **open with a named procurement contact**

## Guardrails
- If you ever subcontract to a non-Indigenous tech firm, **you** retain control and lead the work — never become a flow-through (ISC audits this).
- Never overstate certification status in a bid.

## Notes
- Sequence reminder: credentials (19a) must be in place for set-aside eligibility to mean anything.
- Reference: `planning/indigenous-track/00_strategy_overview.md` (the three doors + buyer tiers).
- **When this clears, the gate is complete → proceed to `day20_mcp_server.md`.**
