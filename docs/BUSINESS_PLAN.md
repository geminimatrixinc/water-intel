# Business Plan — Gemini Matrix Consulting

> **Draft v1** — 2026-03-14  
> Owner: Mike | Gemini Matrix Consulting  
> Status: Living document — review and update at each milestone

---

## 1. Executive Summary

**Gemini Matrix Consulting is an Indigenous-owned AI/ML systems integrator.** We build modern data, machine-learning, and agent systems for governments, Crown corporations, and Indigenous organizations — and we ship working software, not PDFs. **Water-Intel is our flagship vertical, not the whole company:** it is the proof that we can take a real-world data problem from raw feed to deployed, decision-ready intelligence.

**Why this company, why now.** A mandatory minimum **5% of federal contract value** must now go to Indigenous business, in force across 96 departments — and buyers are *actively searching* for suppliers, especially in under-served categories like AI/ML. Most firms in the Indigenous Business Directory are construction, staffing, and facilities. The pool of Indigenous suppliers who can credibly ship modern AI is thin. That gap — Indigenous ownership *plus* real AI capability — is our structural moat.

**The reusable engine.** Our core pipeline — ingest → anomaly detection → driver hints → risk score → API → dashboard → (MCP/agent layer) — is domain-agnostic after the data-loader stage. Each new vertical is a new loader and a new framing, not a new company. This is what makes Gemini Matrix *fundable as a company*, not just contractable for one project.

**The flagship problem (Water-Intel).** Dozens of First Nations communities remain under long-term drinking water advisories. Monitoring today is largely reactive — advisories are issued *after* failure. Water-Intel ingests water-quality monitoring data, detects multi-parameter anomalies with machine learning, and produces per-site risk scores and plain-language driver explanations, so operators and decision-makers have source-water context earlier and with more confidence.

- **Phase 1 (current, live):** Working MVP on public Ontario PWQMN / ECCC data — deployed dashboard at `water.geminimatrixinc.com`. A 2A proxy: it flags anomalies and trends; it does **not** predict ISC drinking water advisories.
- **Phase 2 (target):** Integration with community-governed potable-system data, under OCAP®-compliant agreements, for grounded validation and operator-facing early warning.

**What we're seeking:** the credentials and first references that turn capability into recurring public-sector revenue — verified IBD/CCAB standing, a reference pilot (Six Nations / OFNTSC), one funded validation partnership (Mitacs with McMaster), and the first sole-source federal engagement.

---

## 2. Company Overview

| | |
|---|---|
| **Legal Name** | Gemini Matrix Consulting Inc. |
| **Type** | Indigenous-owned (100%) AI/ML systems integrator |
| **Flagship vertical** | Water-Intel — source-water intelligence & decision support |
| **Founder** | Mike |
| **Location** | Grand River territory, Ontario, Canada |
| **Website** | geminimatrixinc.com (live) · demo: water.geminimatrixinc.com |
| **Founded** | 2025 |

### What We Are
Not a single-product water startup, and not a generic consultancy. We are the rare Indigenous supplier that can architect and deliver a full modern AI stack — ingestion, ML, APIs, dashboards, and agent/MCP integration — and stand behind it in production. Water-Intel is the reference implementation of that capability.

### Core Competencies
- **AI/ML systems delivery** — data ingestion, anomaly detection, risk scoring, model explainability, end-to-end pipelines
- **Agent & MCP architecture** — exposing data and tools to AI agents via the Model Context Protocol (Phase 3 capability)
- **Full-stack engineering** — Python, FastAPI, React/Next.js, deployment and ops
- **Indigenous data governance** — OCAP®-aligned design; data sovereignty as an architectural default
- **Public-sector & Indigenous procurement fluency** — IBD/CCAB pathways, set-asides, IPP, and grant navigation

### Vertical Roadmap (same engine, many buyers)
| # | Vertical | Status |
|---|----------|--------|
| 1 | **Source / drinking-water intelligence** (Water-Intel) | ✅ Built & deployed |
| 2 | Traditional-territory & environmental monitoring | Drop-in (reuses ingest + anomaly stack) |
| 3 | Agentic case-management workflows (e.g. Jordan's Principle) | Reuses MCP/agent layer |
| 4 | Indigenous language / heritage AI tooling | Adjacent capability |
| 5 | Procurement & compliance AI (5%-target / IPP tracking) | Adjacent capability |

> Lead with #1 — it is built and it hits a named federal priority. Keep #2 architecturally drop-in ready — that is what makes the company fundable, not just contract-able. See `planning/indigenous-track/00_strategy_overview.md` for the full procurement thesis.

### Credentials Status
| Credential | Unlocks | Status |
|-----------|---------|--------|
| IBD listing (Indigenous Business Directory) | Federal PSIB set-asides + the 5% target | Registered — verify current + correct NAICS |
| CCAB Certified Aboriginal Business | Ontario provincial + Crown-corp set-asides | To apply |
| ProServices / TBIPS (Indigenous supplier) | Federal professional-services call-ups | Later phase |

---

## 3. Market Opportunity

### The Crisis (by the numbers)
- **28+** long-term drinking water advisories remain active in First Nations communities (as of 2024)
- **$8.5B+** committed by the federal government to First Nations water infrastructure since 2016
- Average advisory duration: **7+ years** — some exceeding 25 years
- Root causes are systemic: infrastructure, monitoring, operator capacity, and data gaps

### The Gap We Fill
Current monitoring is largely **reactive** — advisories are issued *after* contamination is detected or systems fail. Existing SCADA systems watch water *inside the treatment plant*. Nobody is systematically watching the river *upstream* — where contamination events originate.

**Water-Intel fills that gap.** Their SCADA says "turbidity is 4.2 right now." Our system says "conductivity at the upstream station jumped 3x its 30-day average while iron spiked simultaneously — this pattern matches contamination events we've flagged before."

> **Positioning:** "Your SCADA watches the water inside your plant. We watch the river upstream — so you know what's coming before it arrives."

Water-Intel provides the intelligence layer that doesn't exist today:
- **Upstream source water monitoring** — tracking what's coming *before* it reaches the intake
- **Multi-parameter anomaly detection** — ML models that see patterns across 100+ parameters simultaneously
- **Risk scoring** that surfaces deteriorating trends before they trigger advisories
- **Operator-friendly dashboard** that doesn't require ML expertise to use

### Target Market
| Segment | # of Potential Clients | Approach |
|---------|----------------------|----------|
| First Nations communities with active advisories | 28+ | Direct outreach via ISC regional |
| First Nations communities with lifted advisories (prevention) | 100+ | Tribal council partnerships |
| Tribal councils / health authorities | ~60 | Pilot partnerships |
| ISC regional offices | 7 | Technology advisory / procurement |
| Provincial health authorities | 13 | Data integration partnerships |

---

## 4. Product: Water-Intel Platform

### Phase 1 — Proof of Concept (Current)
- **Data:** Public ECCC surface water quality monitoring
- **ML:** Unsupervised anomaly detection (Isolation Forest) + risk scoring
- **Output:** Per-site risk scores (0–100), anomaly timelines, driver explanations
- **Stack:** Python + pandas + scikit-learn | FastAPI | Next.js + React + Tailwind
- **Limitation:** 2A proxy data — does not predict ISC advisories directly

### Phase 2 — Pilot Deployment (Target)
- **Data:** Potable water system monitoring (treatment plant, distribution)
- **ML:** Supervised models trained on advisory outcomes + operational data
- **Output:** Community-specific early-warning alerts
- **Requirement:** Data-sharing agreements, OCAP® compliance, community consent
- **Timeline:** 6–12 months after pilot funding secured

### Phase 3 — Scale (Vision)
- Multi-community deployment
- Real-time sensor integration
- Operator mobile alerts
- National dashboard for advocacy and policy

---

## 5. Revenue Model

### Near-term (Year 1)
| Stream | Description | Target |
|--------|-------------|--------|
| **Grants** | Indigenous water/tech funding (ISC, CIRNAC, FNHA, innovation funds) | $50K–$200K |
| **Consulting** | Data strategy + tech advisory for Indigenous organizations | $25K–$75K |
| **Pilot fees** | Subsidized pilot deployments (co-funded with grants) | $10K–$25K per community |

### Medium-term (Year 2–3)
| Stream | Description | Target |
|--------|-------------|--------|
| **SaaS licensing** | Per-community subscription for ongoing monitoring | $500–$2,000/month/community |
| **Integration services** | Custom data pipelines for tribal councils / health authorities | $25K–$100K |
| **Training** | Operator training and capacity building | $5K–$15K per engagement |

### Long-term
- National platform licensing
- Provincial/federal procurement
- International expansion (Indigenous communities globally face similar challenges)

---

## 6. Funding Strategy

### Immediate Targets
1. **ISC First Nations Infrastructure Fund** — Water and wastewater category
2. **Innovation Stream — First Nations Water** — New approaches to water safety
3. **EcoAction Freshwater Stream** — Watershed, freshwater, and monitoring-aligned support
4. **Mitacs** — Industry-academic research partnerships
5. **NACCA / Indigenous business development** — Founder and business growth funding
6. **Canada Water Agency opportunities** — Track closely for freshwater-aligned future funding or partnerships

### What Funding Enables
| Amount | What It Unlocks |
|--------|----------------|
| **$25K** | Complete Phase 1 MVP + 1 pilot outreach trip |
| **$75K** | Phase 2 development + 2 community pilot deployments |
| **$200K** | Full platform build + 5 community pilots + operator training program |
| **$500K+** | National deployment infrastructure + team expansion |

### Grant Positioning
- **Indigenous-owned** company building technology for Indigenous communities
- **Water safety and freshwater resilience** are stronger funding frames than generic AI innovation
- **Data sovereignty** aligned (OCAP® principles, community consent model)
- **Source-water interpretation** complements plant systems rather than replacing SCADA
- **Measurable outcomes:** advisory days prevented, response time improved, operator confidence

---

## 7. Competitive Landscape

| Competitor/Alternative | What They Do | Limitation | Our Advantage |
|----------------------|-------------|------------|---------------|
| **Plant SCADA systems** | Real-time sensors at treatment plant | Watches water *inside* the plant only — no upstream visibility, threshold alerts only (no ML) | We watch the **river upstream** — early warning before contamination reaches the plant |
| **Manual grab sampling** | Weekly/monthly lab samples | Results take days; no trend analysis; data sits in spreadsheets | Real-time anomaly detection across 100+ params with 5-year trend context |
| **Government reporting (ISC/PWQMN)** | Annual provincial reports | Data published 12–18 months late; tracks advisories after the fact | Same data, analyzed *now* with ML — turns annual reports into actionable intelligence |
| **Large consulting firms (Deloitte, WSP)** | Water infrastructure consulting | Expensive, not Indigenous-led, generic reports | Indigenous-owned, purpose-built, community-focused, ships software not PDFs |
| **Regulatory compliance software (Hach WIMS, CityWorks)** | Track readings against legal limits | Simple pass/fail against fixed thresholds — no pattern detection, no predictive capability | Multi-parameter anomaly detection finds patterns that fixed thresholds miss |
| **Academic research projects** | Publish papers on water ML | Don't ship products, don't engage communities | We ship working software and walk into the plant |
| **No solution (status quo)** | Nothing | Communities stay on advisories | Any improvement > status quo |

### What Existing Systems Miss

Most water plants operate at one of three levels:

| Level | Typical Setup | Gap |
|-------|--------------|-----|
| **Level 1 — Small/rural/First Nations** | Manual grab samples, basic SCADA, spreadsheets | No trend analysis, no upstream monitoring, no ML |
| **Level 2 — Mid-size municipal** | Automated SCADA + compliance software | Threshold alerts only — no anomaly detection, no source water intelligence |
| **Level 3 — Large cities** | Sophisticated SCADA + historian databases | Some experimenting with statistics, but almost none use ML on source water |

**Water-Intel sits in the gap between Level 1 infrastructure and Level 3 intelligence** — bringing upstream ML-powered monitoring to communities that currently have nothing.

---

## 8. Team

| Role | Person | Responsibility |
|------|--------|----------------|
| **Founder / CEO** | Mike | Vision, strategy, Indigenous community relationships, funding |
| **CTO / Architect** | AI-assisted (Copilot) + Mike | System design, ML pipeline, full-stack development |
| **Advisory (future)** | TBD | Indigenous governance, water engineering, public health |

### Hiring Plan (with funding)
- **Year 1:** 1 full-stack developer, 1 community engagement coordinator
- **Year 2:** 1 ML engineer, 1 Indigenous data governance advisor, 1 operations/deployment

---

## 9. Timeline

| Phase | Timeline | Status / Milestone |
|-------|----------|--------------------|
| **Phase 1 MVP** | Mar–May 2026 | ✅ Done — working demo deployed at water.geminimatrixinc.com |
| **Traction** | May–Jul 2026 | 🟡 In progress — website live, McMaster/Six Nations relationship active, expert validation pending |
| **Expert validation** | Jul–Aug 2026 | Hydrological expert review of methodology (McMaster-coordinated) — gates Phase 2 scope |
| **Pilot Funding** | Q3–Q4 2026 | Mitacs + freshwater grant application in active progress (needs partner letter) |
| **Phase 2 Build** | ~2 quarters post-funding | Community-governed data integration, supervised models |
| **Pilot Deploy** | Following 2 quarters | 1–2 community pilots with real data |
| **Phase 3 Scale** | Year 2+ | Multi-community, national platform |

### Key Events

| Event | Timing | Why It Matters |
|-------|--------|----------------|
| **Grand River Champion of Champions Powwow (Six Nations)** | Late July 2026 | Highest trust-per-dollar community presence — attend as community member, not vendor |
| **First Nations Investment Forum (Toronto)** | Deferred to 2027 | Right audience, but wants investable projects with revenue; revisit once a pilot is signed ($1,100 + travel) |

---

## 10. Key Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Can't access 2B potable data | Medium | High | Phase 1 demonstrates methodology; build trust first |
| Funding not secured | Medium | High | Multiple applications, lean operations, consulting revenue bridge |
| Community adoption resistance | Low–Medium | Medium | Indigenous-led, community-first approach, pilot with willing partners |
| Technical: model accuracy insufficient | Low | Medium | Start with anomaly detection (proven), supervised models in Phase 2 |
| Scope creep / feature bloat | Medium | Medium | Strict daily plan, scrum discipline, "ship then polish" |
| Liability exposure (tool informs drinking-water decisions) | Medium | High | Decision-support framing in every contract (never certification/compliance); obtain E&O / professional liability insurance before first paid pilot; keep 2A guardrail language in all deliverables |
| Indigenous procurement program changes (post-ArriveCan scrutiny of PSIB / 5% target) | Low–Medium | Medium | Keep ownership documentation audit-ready; diversify across federal, Ontario, Crown-corp, and Tier 3 (Nation-direct) buyers so no single program is load-bearing |
| Solo-founder single point of failure | Medium | Medium | Everything documented in-repo (plans, runbooks, pipeline); prioritize first hire on funding; automate deploys so operations don't live in one person's head |

---

## 11. Success Metrics

### Phase 1 (MVP)
- [ ] Working end-to-end demo in 90 seconds
- [ ] Company website live
- [ ] 3+ outreach conversations initiated
- [ ] 1 funding application submitted

### Phase 2 (Pilot)
- [ ] 1 community pilot deployed with real data
- [ ] Anomaly detection validated against known events
- [ ] Operator feedback collected and incorporated
- [ ] Letter of support from pilot community

### Phase 3 (Scale)
- [ ] 5+ communities using the platform
- [ ] Measurable reduction in advisory response time
- [ ] Sustainable revenue (grants + SaaS)
- [ ] Team of 4+

---

## 12. The Ask

**For pilot partners:** 30-minute discovery call + access to anonymized water quality data for validation.

**For funders:** $75K–$200K to advance from working prototype to community-ready pilot deployment covering:
- Platform development (Phase 2 features)
- 2 community pilot deployments
- Operator training materials
- Community engagement and governance alignment
- Hosting and infrastructure (12 months)

**For supporters:** A letter of support that we can include in funding applications.

---

*This plan is a living document. Updated at each project milestone.*
