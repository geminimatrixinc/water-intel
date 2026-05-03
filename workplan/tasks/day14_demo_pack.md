# Day 14 — Demo Pack + Pilot Artifact

**Sprint:** Week 3 — API + UI + Demo Pack  
**Status:** 🟡 In Progress  
**Depends on:** Day 13 (dashboard) ✅  
**Blocked by:** None

---

## Objective
Package the MVP into a demo-ready artifact: scripted walkthrough, pitch one-pager, and screenshots. This is what we show to funders and pilot partners.

## Deliverables

### `docs/demo_script.md`
- 60–90 second scripted walkthrough
- Steps: start API → open dashboard → pick site → show risk score → show anomaly table → explain "what triggered it"
- Include which commands to run

### `docs/PILOT_ONE_PAGER.md`
- **Problem:** X First Nations communities under long-term drinking water advisories
- **Solution:** Early-warning system that flags water quality deterioration before it becomes a crisis
- **How it works:** AI anomaly detection on water quality monitoring data from the Grand River watershed
- **What we've built:** Working MVP analyzing 5 years of data from 8 stations near Six Nations (2019–2024, 103 water quality parameters)
- **Data note:** Trend analysis using Ontario PWQMN historical data; real-time monitoring is Phase 2
- **What Phase 2 needs:** Access to potable water system data, pilot community, $X funding
- **Ask:** 30-minute call or letter of support

### `docs/screens/` — 3+ screenshots
- Dashboard overview (site list with risk badges)
- Site detail (risk card + anomaly table)
- Anomaly timeline chart

## Acceptance Criteria
- [ ] Demo script works end-to-end without fumbling
- [ ] One-pager fits on one page (printable)
- [ ] Screenshots are current and well-cropped
- [ ] Non-technical person can understand the one-pager

## Commit Message
```
docs: pilot one-pager + demo script + screenshots
```

## Notes
- The one-pager is the most important non-code deliverable in Phase 1
- Test the demo with someone who hasn't seen the project — if they don't "get it" in 90 seconds, simplify
- Dashboard clarity improvements are already in place: risk legend, recommended actions, timeline caption, and plain-language driver hints are live in the app
- Dashboard disclosure copy is already in place: the app now clearly states it uses public historical PWQMN data and that real-time monitoring requires Phase 2 access
- Raw Ontario source files were refreshed from the latest published 2023/2024 resources; current processed coverage now reaches as late as Dec 2024
- Remaining Day 14 work is the packaging layer: demo script, one-pager, and current screenshots
- Diminishing returns reminder: do not reopen dashboard/detail polishing unless the packaging work reveals a real bug or a serious clarity problem

---

## Summit Addendum — Forward Summit East 2026

Use this section if full demo-pack polishing is temporarily parked and the immediate goal is relationship-building.

### 1) Founder Talk Track

**30-second version**

"I'm Mike, an Indigenous founder building Water-Intel through Gemini Matrix Consulting. I built an early-warning approach for water quality that helps interpret upstream risk patterns earlier. I am here to learn from people doing the work and build the right relationships for pilot and community-aligned next steps."

**60-second version**

"I'm Mike, an Indigenous founder with Gemini Matrix Consulting. I'm building Water-Intel, an early-warning project for water quality that uses historical watershed data and machine learning anomaly detection to surface risk patterns earlier. Right now it's a working Phase 1 prototype using Ontario PWQMN Grand River data, so this is not a replacement for plant systems or a finished product. The goal is to validate whether this type of interpretation can help communities make earlier, better-informed decisions. I'm here to listen, get grounded feedback, and connect with people open to pilot conversations or support-letter pathways."

### 2) Three Ask Variants

**Community / pilot ask**
- "Would you be open to a 20-minute feedback call on whether this is useful in your operational reality?"

**Funder / support-letter ask**
- "Would you be open to reviewing a one-page brief and, if aligned, discussing a support letter or funding fit?"

**Technical / advisor ask**
- "Could you point me to one or two people who understand source-water interpretation and would give candid technical feedback?"

### 3) Conversation Framework (simple)

1. Open: Who you are and why you care.
2. Problem: Water safety decisions need clearer early signals.
3. Proof: Working prototype on real Grand River watershed historical data.
4. Ask: One specific next step (call, intro, or brief review).
5. Confirm: Set timeline and best follow-up channel.

### 4) Contact Capture Template

Use immediately after each conversation:

- Name:
- Organization:
- Role:
- Why relevant:
- What they said matters most:
- Next step promised:
- Follow-up owner: Mike
- Follow-up date:
- Channel: LinkedIn / email / phone
- Status: New / Followed up / Scheduled / Closed

### 5) Summit Targets (minimum)

- [ ] 5 meaningful conversations
- [ ] 3 concrete follow-ups secured
- [ ] 24-hour follow-up sent for every meaningful contact

### 6) Post-Summit Execution Checklist

**Within 48 hours**
- [ ] Send all follow-up messages
- [ ] Share one-page brief and demo link where promised
- [ ] Book at least 2 next-step calls

**Within 7 days**
- [ ] Reconnect with unresponsive high-value contacts
- [ ] Ask warm-network contacts for 2-3 targeted introductions
- [ ] Update outreach tracking in `docs/pilot_targets.md` and `PROGRESS.md`
