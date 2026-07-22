# Days 15–19 — Traction Week (Company Presence + Outreach + Funding)

**Sprint:** Week 4 — Convert MVP into Credibility  
**Status:** 🟡 In Progress  
**Depends on:** Day 14 packaging is still useful, but it is no longer blocking website/deployment or outreach prep  
**Blocked by:** None

---

## Philosophy
> Stop building new features unless a partner requests them.  
> Focus: company credibility, public presence, pilot conversations, funding readiness, and a first-impression story that makes Water-Intel feel meaningfully different from an existing monitoring portal.

**Discipline reminder:** the dashboard/detail build is good enough for this phase. Do not slip back into UI polishing unless a real bug or credibility issue blocks packaging or outreach.

---

## Day 15 — Record Demo Video + Mission Statement Review
- [ ] Record or script a 90-second walkthrough (screen recording or narrated screenshots)
- [ ] Save link/file in `docs/demo_video_link.md`
- [ ] Review and finalize `docs/MISSION_STATEMENT.md` and `docs/BUSINESS_PLAN.md`
- [ ] Test that a non-technical viewer understands the value

## Day 16 — Gemini Matrix Consulting Website (Next.js)
> **See dedicated task file:** [done/day16_company_website.md](done/day16_company_website.md)
- [x] Build full company website (Home, About, Services, Water-Intel, Contact, Funding)
- [ ] Use Gemini Pro for copy generation and design polish
- [ ] Indigenous-inspired design (earth tones, water imagery, community focus)
- [x] Corporate site now links to the live Water-Intel dashboard
- [ ] Water-Intel product page with screenshots, "Request a Pilot" CTA, and Ask Water-Intel teaser copy
- [ ] Add a short technical brief / pilot brief link so serious reviewers have a deeper credibility document

## Day 17 — Hostinger Deployment + Domain
> **See dedicated task file:** [done/day17_hostsigner_deploy.md](done/day17_hostsigner_deploy.md)
- [x] Activate Hostinger account/access
- [x] Deploy company website to production
- [x] Configure custom domain + SSL
- [x] Deploy Water-Intel dashboard as a live subdomain on Hostinger
- [ ] Ensure the public-facing Water-Intel page distinguishes interpretation from charting in the first screenful

## Day 18 — Pilot Outreach + Grant Research
- [ ] Identify 3–5 target organizations (tribal councils, ISC regional, health authorities, NGOs)
- [ ] Document in `docs/pilot_targets.md`
- [ ] Draft outreach email template: `docs/outreach_email.md`
- [x] First outreach: sent LinkedIn message to Michael Montour (Six Nations Director of Public Works) — 2026-05-04
- [ ] **MUST DO May 11** — Follow up Montour on LinkedIn (one line + demo link: https://water.geminimatrixinc.com/dashboard)
- [ ] **MUST DO this week** — Submit NRC IRAP intake form (https://nrc-cnrc.canada.ca/en/support-technology-innovation) — gets you an Industrial Technology Advisor (ITA), no partner required, takes 10 min
- [ ] Second outreach: email to crc@sixnations.ca (Rachel VanEvery, Community Research Coordinator, DA&I) — not yet sent
- [ ] Send Rachel email when ready (template in Six Nations section below)
- [x] Third outreach: ohneganos@gmail.com (Ohneganos team) — 2026-05-07
- [x] **MUST DO May 14** — Follow up Ohneganos → Colin Gibson directly (gibsoc13@mcmaster.ca / 647-215-5768) — sent May 24
- [ ] **MUST DO May 14+** — Email Dr. Zoe Li, McMaster (zoeli@mcmaster.ca) — ML + water risk management, direct fit
- [ ] **MUST DO May 14+** — Email Dr. Zobia Jawed, McMaster (jawedz@mcmaster.ca) — AI water systems, Mitacs Engage experience, mention Mitacs partnership model explicitly
- [ ] Follow up WELL Lab / Dr. Papangelakis — real-time sensor network, predictive layer pitch
- [ ] Research Indigenous water funding programs (see `docs/FUNDING_STRATEGY.md`)
- [ ] Track responses in `docs/AI_OUTREACH_PLAYBOOK.md` (Outreach Log section)
- [ ] Outreach language positions the dashboard as proof and Ask Water-Intel as the next differentiation layer

### Funding Tier 1 — Active Pipeline

These three programs are designed to stack. Don't pick one — design Phase 2 so all three fit.

| Program | Status | Requires | Action |
|---------|--------|----------|--------|
| **NRC IRAP — AI Assist** | Start now | None for intake | Submit intake form this week → ITA call in 2–3 weeks |
| **Mitacs Engage** (~$15K) | Wait | Faculty partner (Jawed/Li) | Apply once a McMaster prof signs on |
| **FedDev RAII** (Regional AI Initiative, Ontario) | Wait | 1+ partner letter + project scope | Apply after first warm partner reply |

**Why this order:** IRAP intake costs nothing and can run in parallel. A live IRAP conversation makes McMaster outreach more credible ("I'm in early discussions with NRC IRAP"). Mitacs and RAII both need partner letters that come from successful outreach.

**Outreach pipeline summary (as of May 26):**
| Contact | Org | Channel | Status | Due |
|---------|-----|---------|--------|-----|
| Michael Montour | Six Nations Public Works | LinkedIn | Sent May 4 — no reply | Let it rest |
| Ohneganos team | McMaster/Six Nations | Email | Sent May 7 → Colin replied May 26 | Follow Colin thread |
| Rachel VanEvery | Six Nations DA&I | Email | Not yet sent | HOLD — wait for Dawn response |
| Dr. Colin Gibson | McMaster / Ohneganos | Email | **Replied May 26 — forwarded to Dawn Martin-Hill (PI) + Six Nations Environment Dept.** Thank-you reply sent same day. | Wait. Re-check ~June 9 if silent. |
| Dr. Zoe Li | McMaster | Email | Not yet sent | **HOLD** — don't run parallel McMaster outreach while Dawn intro is active |
| Dr. Zobia Jawed | McMaster | Email | Not yet sent | **HOLD** — same reason |
| WELL Lab / Papangelakis | McMaster | Email | Not yet sent | HOLD |

**🟢 Active warm intro:** Colin Gibson forwarded Water-Intel to **Dr. Dawn Martin-Hill (Ohneganos PI)** and **Six Nations Environment Department**. This is the warm path into both organizations. Do not contact them directly — let the intro land.

**Next-step prep while waiting on Dawn:**
- Have a Phase 2 one-pager ready: scope of pilot, what data would be needed from community, OCAP® commitments in writing, timeline, what Mitacs Engage would fund
- Have a 15-min demo flow rehearsed for the dashboard (anomaly view → driver hints → caveats about historical-only data)
- Be ready to answer: "What would a pilot look like with us?"

### Events Strategy

**Principle:** Email outreach is the primary channel until a community partner is signed. Paid conferences are deferred until there's a pilot to talk about and a specific ask. Focus events on free/low-cost, high-trust, local opportunities.

**⭐ KEY EVENT — Grand River Champion of Champions Powwow (Six Nations)**
- **When:** Last weekend of July 2026 (typically 4th weekend — confirm exact dates at https://www.snpowwow.com)
- **Where:** Chiefswood Park, Six Nations of the Grand River
- **Why this matters most:**
  - Largest powwow in the territory — draws people from across Six Nations and beyond
  - Highest-density in-person opportunity to be present in community as a member, not a vendor
  - Builds the kind of trust no email can build
- **How to show up:**
  - Go as a community member. Bring family if possible.
  - Do NOT pitch. Do NOT hand out cards unprompted.
  - If conversations come up about what you do, keep it short and human ("I'm building a water-quality tool — trying to find the right people to talk to about it")
  - Wear something with the Gemini Matrix logo only if it's subtle
- **Prep before going:**
  - Confirm dates and gate fee on the official site
  - Have one-line answer ready for "what brings you here?"
  - Note any Six Nations Public Works / DA&I / Ohneganos people you recognize — follow up by email afterward, not at the event
- **Action:** Block the weekend in calendar now. Confirm dates in June.

**Go (low-cost, high-fit):**
| Event | When | Why | Cost | Action |
|---|---|---|---|---|
| Six Nations community events (Public Works open houses, powwows, Grand River Champion of Champions Powwow in July, Fall Fair in September) | Ongoing — Bread & Cheese already passed (May 17) | Show up as community member, not vendor. Highest trust-building per dollar. | Free | Check Six Nations event calendar monthly; aim for July powwow |
| ⭐ **Six Nations Water Festival** | **2026 unconfirmed** — date has varied (Aug 27 2024, early July 2025); announced via community channels, not the open web | **Ohneganos co-plans this with the Dept of Well-Being and runs booths** — the one event where walking up and talking about water tools is the literal point. Far better targeted than the powwow for meeting Ohneganos people deliberately. | Free | Check Dept of Well-Being Facebook + sixnations.ca events calendar; if unclear, call Dept of Well-Being (519) 445-2418 and ask if one is planned this year. If it already happened this July, note the pattern for 2027. |
| Ohneganos / McMaster Water Network public talks | Check calendar | In-person with Gibson / Li / Jawed without cold email | Free | Subscribe to McMaster Water Network mailing list |
| Communitech (Waterloo) — Indigenous founder programming | Ongoing | 20 min away. Has Indigenous entrepreneurship lead. Real intros. | Free–$30 | Book one intro coffee visit |
| CWN (Canadian Water Network) webinars | Monthly | Learn sector language, spot funding/partners | Free | Subscribe to newsletter |
| Mitacs networking events | Check site | Direct line to faculty doing Mitacs Engage — your funding path | Often free | Watch Mitacs Ontario events page |

**Defer until pilot signed:**
| Event | When | Why deferring |
|---|---|---|
| First Nations Investment Forum (FNIF) | Nov 29 – Dec 1, 2026, Toronto | $1,100 + travel. Audience wants investable projects with revenue. Revisit for 2027. |
| AFOA Canada conference | Annual | Right buyer persona (Indigenous finance officers) but no offer to sell yet. |
| CANDO | Annual | Economic development officers — useful once there's a pilot story. |
| Indigenous Innovation Summit | Fall | Go if community partner is signed by then. |

**Re-evaluate events:** September 2026 (after summer outreach cycle).

### Six Nations Outreach — Email Template (crc@sixnations.ca)

Use this template when regenerating or following up. Adjust dates/status as needed.

```
Subject: Attn: Community Research Coordinator — Early Warning Water Intelligence System Inquiry

Hi,

I'm Mike, an Indigenous developer and owner of Gemini Matrix Consulting. I'm reaching out
regarding Water-Intel, an ML-based early warning system for water quality — and I believe it
may be relevant to Six Nations' current monitoring priorities.

Phase 1 (where we are now): I've built a working proof of concept using historical Canadian
public water-quality data from the Grand River watershed. The system uses machine learning to
flag periods and locations of elevated risk. You can explore it at geminimatrix.ca.

Phase 2 (what I'm looking for): I'm seeking a community partnership to refine and ground-truth
the model using local data, so it is accurate and meaningful specifically for Grand River
territory — not just a generic tool.

Data sovereignty: Any data shared by Six Nations in that process would remain under the
community's full Ownership and Control, consistent with OCAP® principles. I understand formal
proposals go through the Cayuse platform and the Research Ethics Committee, and I'm prepared
for that process.

Given the June 2025 flooding emergency and current river monitoring modernization, I hope this
is timely. Could we schedule a short 20-minute call to assess fit?

Thank you,
Mike
Gemini Matrix Consulting
[phone]
geminimatrix.ca
```

**Context notes for this template:**
- June 2025: Six Nations declared State of Emergency due to flooding
- River gauge modernization underway (Turtle Island News, Mar 4 2026)
- Cayuse platform = Six Nations' formal research/tech proposal intake system
- OCAP® compliance is a formal requirement, not optional
- crc@sixnations.ca is a role-based inbox — write as if it may be screened

---

### Ohneganos Outreach — Email Template (ohneganos@gmail.com)

**Why this is a priority target:**
- Already have creek and chlorine sensors deployed in Grand River territory — you're offering a brain for infrastructure they built
- McMaster University + Six Nations co-creation model — academically credible, community-trusted
- Funded through Global Water Futures
- Core value is Two-Eyed Seeing (Indigenous Knowledge + Western Science) — your OCAP® + ML framing maps directly
- Water Caretakers and youth leaders already engaged — youth demo offer is a concrete differentiator
- Mike has an office in Grand River — local presence is a strong credibility signal

**Contact sequence:**
1. `ohneganos@gmail.com` — first, lowest barrier
2. Day 7 no reply → Colin Gibson `gibsoc13@mcmaster.ca` (Project Officer, handles tools/integration)
3. Only after warm reply → Dr. Dawn Martin-Hill `dawnm@mcmaster.ca`

```
Subject: Water quality early warning project — possible collaboration with Ohneganos

Hi,

My name is Mike. I'm an Indigenous developer with an office in Grand River — I teach
technology at George Brown Polytechnic and I've been working through a machine learning
program at the University of Waterloo. Over the past several months I've been putting that
together into something practical: a water quality early warning system for the Grand River
watershed called Water-Intel.

The short version is that I trained a model on five years of historical public water-quality
data from monitoring stations near Six Nations, and it flags anomalies and elevated-risk
periods across 103 parameters. There's a working prototype at geminimatrix.ca if you want
to take a look before reading further.

I came across Ohneganos while researching who was already doing serious work in this space,
and honestly it made me think I should reach out rather than keep building in isolation. You
already have sensors in the water. I have a model that could potentially make sense of
patterns in that kind of data. That feels like a conversation worth having.

What I'm looking for in Phase 2 is a community partner who can help ground-truth the model
with local data — things like flow rates and contaminant profiles specific to Grand River —
so the predictions are actually meaningful for this territory rather than generic. Any data
the community shares would stay under community ownership and control, consistent with OCAP®
principles. I'm also open to working through whatever ethics and governance process makes
sense for your program.

And if it would be useful, I'd genuinely enjoy showing the prototype to your Water Caretakers
or youth team and walking through how the model works. That kind of feedback would actually
help me build something better.

Would you be open to a short call to see if there's a fit?

Thanks for the work you're doing,

Mike
Gemini Matrix Consulting
[phone]
geminimatrix.ca
```

**Day-7 follow-up to Colin Gibson (gibsoc13@mcmaster.ca) if no reply:**
```
Subject: Follow-up: Water-Intel early warning layer — Ohneganos collaboration

Hi Colin,

I sent a note to the Ohneganos team last week and wanted to follow up directly given your
work on water quality tools and Indigenous STEM pathways.

Short version: I've built a historical ML model for Grand River water-quality anomaly
detection — trained on five years of public monitoring data from stations near Six Nations.
The dashboard is designed so non-technical community members can interpret what it flags
without needing to understand the model. Data sovereignty is built in: any community data
stays under community ownership and control. Live prototype:
https://water.geminimatrixinc.com/dashboard

I'm in Phase 2 planning and looking for a community partner to ground-truth it with local
data. Given your sensor work and the Co-Creation mandate, I thought it was worth asking
directly.

Happy to compare notes if useful.

Mike
Gemini Matrix Consulting
[phone]
geminimatrixinc.com
```

---

### McMaster Research Outreach — Email Templates

#### Dr. Zoe Li (zoeli@mcmaster.ca) — ML + Water Risk Management

```
Subject: ML water quality risk model — possible collaboration or feedback

Hi Dr. Li,

My name is Mike. I'm an Indigenous developer with an office in Grand River, currently
building Water-Intel — a machine learning system that uses historical Grand River watershed
data to detect water quality anomalies and flag elevated-risk periods.

I came across your group's work on ML for water and environmental risk management and wanted
to reach out directly. I've trained the model on five years of public monitoring data across
103 parameters from stations near Six Nations. The current prototype is live at
https://water.geminimatrixinc.com/dashboard.

I'm in Phase 2 planning and looking for either a technical collaborator or honest feedback on
the modelling approach. If your group is working on anything adjacent — flood forecasting,
pollutant transport, source water risk — I'd be curious whether there's a connection worth
exploring.

Even a 15-minute conversation would be useful.

Mike
Gemini Matrix Consulting
[phone]
geminimatrixinc.com
```

---

#### Dr. Zobia Jawed (jawedz@mcmaster.ca) — AI Water Systems + Mitacs Engage

**Key context:** She led the ARGoN/RainGrid Mitacs Engage partnership — AI for stormwater
management with a small company. She already knows how to structure this kind of collaboration.
Mention Mitacs explicitly. She'll recognize the path immediately.

```
Subject: AI water quality early warning system — Mitacs Engage collaboration inquiry

Hi Dr. Jawed,

My name is Mike. I'm an Indigenous developer with an office in Grand River building
Water-Intel — an ML-based early warning system for water quality on the Grand River
watershed.

I came across the ARGoN/RainGrid Mitacs Engage project and the structure of that
collaboration is exactly what I'm looking for. I've built a working prototype using five
years of historical public monitoring data — anomaly detection across 103 parameters, with
a risk-scoring dashboard designed for non-technical operators. Live:
https://water.geminimatrixinc.com/dashboard

I'm in Phase 2 planning — looking for a faculty partner to help ground-truth the model with
real local data and potentially structure a Mitacs Engage collaboration. The community angle
is built in: I'm also working with Ohneganos (McMaster/Six Nations) on the Indigenous data
sovereignty side.

Would you be open to a short conversation to see if there's a fit?

Mike
Gemini Matrix Consulting
[phone]
geminimatrixinc.com
```

## Day 19 — Funding Application Skeleton
- [ ] Create `docs/funding_application_skeleton.md`
  - Outcomes and impact metrics
  - Budget categories (development, hosting, community engagement, travel)
  - Timeline (Phase 1 → Phase 2 → Pilot → Scale)
  - Evaluation plan
- [ ] Identify top 3 funding programs and map requirements
- [ ] Review `docs/BUSINESS_PLAN.md` against funding criteria

---

## Exit Criteria (Phase 1 Complete)
All of these must be true:
- [ ] 90-second demo works without explaining ML
- [x] Dashboard shows real anomaly/risk output
- [ ] Clear Phase 2 plan exists (pilot + calibration + governance)
- [ ] Guardrails are explicit (2A proxy, not 2B advisory prediction)
- [ ] At least 1 outreach conversation initiated

---

## Notes
- These are business tasks, not engineering tasks — the product must work before we pitch
- Don't be afraid to simplify the demo if the full pipeline isn't stable
- Letters of support are more valuable than fancy features at this stage
- The public-facing Week 4 story should be: dashboard proof now, Ask Water-Intel next, MCP build after that.
- The brief should support the website, not compete with it: concise, credible, and clearly tied to the product page.
- Keep moving forward: the next gains come from packaging, credibility, and interaction design, not from squeezing a little more polish out of the current dashboard.
