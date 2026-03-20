# Business Plan — Gemini Matrix Consulting

> **Draft v1** — 2026-03-14  
> Owner: Mike | Gemini Matrix Consulting  
> Status: Living document — review and update at each milestone

---

## 1. Executive Summary

Gemini Matrix Consulting is an Indigenous-owned technology consulting company building **Water-Intel**, an AI-powered early-warning platform for drinking water safety in First Nations communities across Canada.

**The problem:** Dozens of Indigenous communities remain under long-term drinking water advisories due to aging infrastructure, insufficient monitoring, and gaps in early-warning capability.

**Our solution:** A data-driven platform that ingests water quality monitoring data, detects anomalies using machine learning, and provides risk scores and actionable alerts — giving operators and decision-makers the information they need *before* a crisis escalates.

**Phase 1 (current):** MVP using public ECCC water quality data to demonstrate the technology and methodology.  
**Phase 2 (target):** Integration with actual potable water system data, with community consent and data governance agreements in place.

**What we're seeking:** Pilot community partnerships, letters of support, and funding to advance from prototype to field-ready deployment.

---

## 2. Company Overview

| | |
|---|---|
| **Legal Name** | Gemini Matrix Consulting |
| **Type** | Indigenous-owned technology consulting |
| **Founder** | Mike |
| **Location** | Canada |
| **Website** | TBD (Day 16 — deploying to HostSigner) |
| **Founded** | 2025 |

### Core Competencies
- Water quality data analysis and ML modeling
- Full-stack web application development (Python, React, Next.js, FastAPI)
- Indigenous data governance advisory (OCAP® aligned)
- Grant writing and funding navigation for Indigenous tech projects

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
2. **CIRNAC Northern/Indigenous Innovation** — Technology projects
3. **FNHA (First Nations Health Authority)** — Health-tech grants (BC-specific)
4. **Mitacs** — Industry-academic research partnerships
5. **ISED Innovation Canada** — Indigenous entrepreneurship stream
6. **NACCA / Indigenous business development** — Business growth funding

### What Funding Enables
| Amount | What It Unlocks |
|--------|----------------|
| **$25K** | Complete Phase 1 MVP + 1 pilot outreach trip |
| **$75K** | Phase 2 development + 2 community pilot deployments |
| **$200K** | Full platform build + 5 community pilots + operator training program |
| **$500K+** | National deployment infrastructure + team expansion |

### Grant Positioning
- **Indigenous-owned** company building technology for Indigenous communities
- **Water safety** is a top federal priority with dedicated funding streams
- **Data sovereignty** aligned (OCAP® principles, community consent model)
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

| Phase | Timeline | Milestone |
|-------|----------|-----------|
| **Phase 1 MVP** | Weeks 1–3 (now) | Working demo: CSV → ML → Dashboard |
| **Traction** | Week 4 | Company website, outreach, pitch materials |
| **Pilot Funding** | Months 2–3 | First grant application submitted |
| **Phase 2 Build** | Months 3–6 | Potable data integration, supervised models |
| **Pilot Deploy** | Months 6–9 | 1–2 community pilots with real data |
| **Evaluation** | Months 9–12 | Pilot results documented, scale plan |
| **Phase 3 Scale** | Year 2+ | Multi-community, national platform |

---

## 10. Key Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Can't access 2B potable data | Medium | High | Phase 1 demonstrates methodology; build trust first |
| Funding not secured | Medium | High | Multiple applications, lean operations, consulting revenue bridge |
| Community adoption resistance | Low–Medium | Medium | Indigenous-led, community-first approach, pilot with willing partners |
| Technical: model accuracy insufficient | Low | Medium | Start with anomaly detection (proven), supervised models in Phase 2 |
| Scope creep / feature bloat | Medium | Medium | Strict daily plan, scrum discipline, "ship then polish" |

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
