# THIS WEEK — Action Summary (July 14–21)

> **Status check:** One meeting with Colin/Sara/intern (June 25), then **silence on their end** — no expert-meeting coordination yet. That's ~3 weeks, which is normal for academia in summer, not a dead signal. But it changes the play: instead of waiting for Colin's email, **the technical brief becomes the follow-up itself.** Finish it, then send it — "here's the brief I offered, feel free to forward it to the expert" is a value-delivering nudge, not a needy check-in.

---

## Carry-over status (from June 25–July 2 week)

- [x] Thank-you emails sent (Colin, Sara, intern)
- [x] NRC IRAP call completed — **ineligible** (requires T4 employee)
- [ ] Technical brief — **OVERDUE, now P1**
- [ ] Innovations Canada inquiry — approach corrected, see below
- [ ] SR&ED log — not started, see reality-check below
- [x] IBD/CCIB credentials — IBD registered, CCIB membership approved (2026-07-22); CIB certification approval + shareholder registry signature still open

---

## P1 — Finish the Technical Brief (Mon–Tue, ~3 hours)

This was due July 2, and it's now doing double duty: it's both the expert-review material **and** your re-engagement move with Colin. You offered it in the thank-you email — delivering on that offer unprompted is the strongest possible follow-up after three weeks of silence.

Full outline already exists in [day19_5_post_meeting_expert_prep.md](tasks/day19_5_post_meeting_expert_prep.md) (5 sections, 2–3 pages). Write it to `docs/TECHNICAL_BRIEF.md`.

**Do not gold-plate it.** The outline is complete; this is 2–3 hours of writing, not a week of research.

## P1b — Mini Validation Memo (Tue–Wed, ~2–3 hours)

The expert's first serious question will be some form of *"how do you know the flags are real?"* Your prepared answer ("false-positive rate is unknown, that's Phase 2") is honest but weak on its own. Strengthen it with a lightweight backtest:

- [ ] Take the top 5–10 highest-scoring anomalies from `outputs/anomalies.csv`
- [ ] Cross-reference each against public records: GRCA flood/flow bulletins, Ontario spills registry (Environmental Occurrences), ECCC hydrometric records, news archives for the date/location
- [ ] Produce a half-page table: date · station · what the model flagged · what actually happened (or "no corroborating record found" — honesty included)
- [ ] Append it to the technical brief as an annex

Even 3 corroborated events out of 10 turns "unknown false-positive rate" into "preliminary event correlation looks promising, formal validation is Phase 2." That is a categorically stronger position in an expert review.

## P2 — Credentials time-box — ✅ core status confirmed 2026-07-22

IBD registration is confirmed active, and CCIB membership is confirmed **approved** ([day19a](tasks/day19a_gate_credentials.md) updated). Remaining:

- [ ] Fix/confirm IBD NAICS codes + website/phone/email (registration itself is done; this is the optimization pass)
- [ ] Sign and date the Shareholders Registry — likely blocks CIB certification approval, now the long pole
- [ ] Confirm CIB certification approval status (separate from, and after, membership approval)

**Done = both processes in flight.** ✅ Both are in flight; certification (not just membership) is the remaining gate.

## P3 — Innovations Canada (corrected approach)

⚠️ **Correction to the previous plan:** Innovative Solutions Canada is **challenge-based procurement**, not an open grant. You don't apply with your own project — federal departments post specific challenges and companies propose solutions. "Calling to apply" isn't how it works.

What to actually do (30 min):
- [ ] Check the current ISC challenge portal for open challenges that fit (water, environmental monitoring, AI/data): https://ised-isde.canada.ca/site/innovative-solutions-canada/en
- [ ] Verify the program's current status and both streams (Challenge Stream + Testing Stream — Testing Stream buys pre-commercial prototypes, potentially a direct fit for Water-Intel)
- [ ] Set a monthly 15-min recurring check of the portal rather than a one-time call
- [ ] If an inquiry contact exists, one email asking whether water-quality/environmental-monitoring challenges are in the pipeline

## P3 — SR&ED log + reality check (30 min setup)

⚠️ **Reality check the plan missed:** SR&ED refunds **eligible expenditures actually incurred** — T4 salaries, contractor invoices, materials. **Unpaid founder labour is not claimable, and dividends don't count.** If you haven't paid yourself a salary or paid contractors for Water-Intel work, the retroactive claim may be close to $0 regardless of how good the log is.

Still do the log (15–20 min to set up) because:
1. It costs nothing and becomes valuable the moment you draw a T4 salary or hire (post-funding)
2. It doubles as the evidence base for grant applications and the expert meeting
3. Confirm the CCPC status question while you're at it

- [ ] Confirm with your accountant: any eligible expenditures to date? (software subscriptions/hosting generally don't qualify; labour does)
- [ ] Set up the log (table format already in the old plan) and backfill 4–5 entries from PROGRESS.md
- [ ] Decision: if no eligible expenditures exist yet, SR&ED moves from "retroactive money" to "future hygiene" — adjust expectations, keep logging

## THIS WEEK — Powwow + Water Festival (10 min)

Grand River Champion of Champions Powwow: **confirmed July 24–26, 2026** (verify venue — Chiefswood Park vs Ohsweken Speedway — at https://www.grpowwow.ca).
- [ ] Block the weekend in your calendar
- [ ] Re-read the "how to show up" notes in [days15-19_traction.md](tasks/days15-19_traction.md) — community member, not vendor; any Ohneganos encounter there is luck, not a plan
- [ ] **Also:** the **Six Nations Water Festival** (Ohneganos co-plans it, runs booths — the *deliberate* place to meet them) has **no published 2026 date** and its timing has varied (Aug 2024, early July 2025 — it may even have happened already). 2-min check: Dept of Well-Being Facebook + sixnations.ca events calendar; or call (519) 445-2418 and ask if one is planned this year

## BACKGROUND (only if P1/P1b done) — Real-time data spike

The demo's biggest credibility gap is the 15-month-stale data. There is public **real-time** data for the Grand River you can integrate without any community agreement: ECCC hydrometric (water level/flow, near-real-time) and GRCA river data. See new task: [day24_realtime_data_spike.md](tasks/day24_realtime_data_spike.md).

This also proves the "new vertical = new loader" architecture claim with a second live loader — worth more than any slide.

---

## Checklist for the week

- [x] Technical brief drafted, reviewed, rendered to PDF → `docs/TECHNICAL_BRIEF.md` + `Water-Intel_Technical_Brief_July2026.pdf` ✅ July 14
- [x] **Follow-up email sent to Colin with brief attached** ✅ July 14 — loop closed, ball in his court. Do not nudge again; ~Aug 4 fallback stands.
- [ ] Validation memo annex (5–10 anomalies cross-referenced) — now doubles as the natural *second* touchpoint if Colin stays silent
- [x] IBD verified + CCIB membership approved (2026-07-22) — CIB certification approval + shareholder registry signature still open
- [ ] Innovations Canada portal checked (corrected model: challenges, not applications)
- [ ] SR&ED log set up + expenditure reality-check done
- [ ] Powwow July 24–26 blocked in calendar (verify venue at grpowwow.ca)
- [ ] (Stretch) Real-time data spike started

---

## The follow-up play (this replaces waiting)

Colin has been silent since June 25. Do **not** send a bare "any update?" email. Instead:

1. **Finish the brief + validation annex first** (P1/P1b above)
2. **Then send one email to Colin (target: July 17–18).** Note: your June 26 offer was conditional — *"I'm happy to send a technical brief … if that would help … Just let me know"* — and Colin never replied yes. So frame the send as closing the loop on your own offer, not as if he asked:
   > "Hi Colin — I went ahead and finished the technical brief I mentioned, attached. If you do end up connecting with the hydrology expert you mentioned, it should give them everything they'd need to assess the approach — the pipeline, why it suits water systems, and the honest limitations. Happy to adjust anything. No rush, I know summer schedules are chaos."

   **Why this framing:** the expert meeting was only *mentioned* on June 25 — never scheduled. Verbal intentions die without an artifact; a forwardable 3-page brief is what converts "we should have someone look at this" into an email Colin can send the expert in 30 seconds. You're not preparing for a meeting — you're making the meeting easy to create.
3. **Then let it rest.** One touch, value attached, zero pressure. That email is impossible to read as pestering because it delivers something you promised.

**Fallback decision point:** if there's still no response by **~August 4** (six weeks post-meeting), the hold on parallel McMaster outreach (Dr. Li, Dr. Jawed) lifts — the Ohneganos path stays warm but stops being exclusive. Note the **powwow (late July)** may resolve this organically: Ohneganos/McMaster people are often present at Six Nations community events, and an in-person "good to see you" beats any email.

- **Dawn Martin-Hill / Six Nations Environment intro:** still let it land — no direct contact.

## Why this week matters

The expert meeting is the gate on all of Track B — Mitacs, the reference letter, and Phase 2 scope all flow from it. Right now that thread is silent, and the brief is the one move that can restart it while making you look *more* credible, not more eager. And regardless of what Colin does, credentials, the powwow, and the validation work all advance the company without needing anyone to reply.
