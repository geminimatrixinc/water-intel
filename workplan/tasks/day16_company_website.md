# Day 16 — Gemini Matrix Consulting Website (Next.js)

**Sprint:** Week 4 — Traction + Company Presence  
**Status:** 🔲 Not Started  
**Depends on:** Day 14 (demo pack — screenshots and content ready)  
**Blocked by:** Day 14 minimum; can scaffold earlier

---

## Objective
Build the Gemini Matrix Consulting company website using Next.js. This is the public face of the company — it must look professional, communicate Indigenous values, and include a dedicated section for the Water-Intel product. Hosted on HostSigner (dormant account — activate and configure).

## Architecture Decision
- **Single Next.js app** (separate from the water-intel dashboard)
- Repo: Can live in a new repo (`gemini-matrix-web`) or a `company-site/` folder — decide at build time
- Use **Gemini Pro API** for AI-assisted copy generation, design suggestions, and content polish
- Design: modern, clean, Indigenous-inspired visual identity (earth tones, water imagery, community focus)

## Deliverables

### Pages
1. **Home** — Hero with mission statement, value proposition, key stats
2. **About** — Company story, Indigenous ownership, team/founder, values
3. **Services** — Indigenous consulting offerings (water safety, data, technology)
4. **Water-Intel** — Product page:
   - Problem statement (BWA/DNC/DNU crisis)
   - Solution overview (early-warning system)
   - Screenshots/demo from Phase 1 MVP
   - "Request a Pilot" CTA
   - Live embedded dashboard view OR iframe of working app (stretch goal)
5. **Contact** — Contact form, email, social links
6. **Funding / Partners** — For potential funders: what we're building, what support enables

### Technical
- Next.js 14+ with App Router
- Tailwind CSS + custom theme (Indigenous design palette)
- Responsive (mobile-first)
- SEO meta tags, Open Graph
- Accessibility (WCAG 2.1 AA minimum)

### Hosting
- Deploy to **HostSigner** (activate dormant account)
- Configure custom domain
- SSL/HTTPS enabled

## Acceptance Criteria
- [ ] Site loads on HostSigner with custom domain
- [ ] All 6 pages render correctly on desktop + mobile
- [ ] Water-Intel product page includes real screenshots
- [ ] Contact form works (or mailto fallback)
- [ ] Design passes "would you trust this company?" test

## Commit Message
```
feat: gemini matrix consulting website v1
```

## Design Direction
- **Color palette:** Deep blues (water), forest greens, earth browns, white space
- **Typography:** Clean, modern sans-serif (Inter or similar)
- **Imagery:** Water, rivers, community, nature — no stock photos of white people in boardrooms
- **Tone:** Professional but warm, community-focused, solution-oriented
- **Gemini Pro usage:** Generate/refine copy for About, Services, and product descriptions

## Notes
- This is a credibility multiplier — funders will Google the company
- The Water-Intel page is the bridge between "consulting company" and "funded product"
- Don't over-build: 6 clean pages > 20 half-finished pages
- HostSigner deployment docs should be followed during setup
