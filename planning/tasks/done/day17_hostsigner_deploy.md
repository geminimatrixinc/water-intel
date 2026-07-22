# Day 17 — Hostinger Deployment + Domain Configuration

**Sprint:** Week 4 — Traction + Company Presence  
**Status:** ✅ Core deliverable shipped  
**Depends on:** Day 16 (company website built)  
**Blocked by:** None

---

## Objective
Activate the Hostinger environment, deploy both the company website and (optionally) the Water-Intel dashboard, configure domains, and ensure production readiness. Use the hosted product page as the public credibility surface for Water-Intel: dashboard proof, screenshots, and an Ask Water-Intel guided narrative that distinguishes the product from an ordinary chart monitor.

## Progress Update — 2026-05-03
- [x] Hostinger environment activated and used for live deployment work
- [x] Corporate website deployed with custom domain and HTTPS
- [x] Water-Intel dashboard and FastAPI stack deployed to Hostinger
- [x] Corporate site now links to the live Water-Intel dashboard
- [ ] Deployment hardening/docs can still be added later if needed

## Deliverables

### Hostinger Setup
- [x] Log into Hostinger account/terminal environment
- [ ] Review current plan/resources available
- [ ] Configure Node.js hosting environment for Next.js

### Company Website Deployment
- [ ] Build production bundle (`next build && next export` or SSR depending on host)
- [x] Deploy to Hostinger
- [x] Configure custom domain (geminimatrix.ca or similar)
- [x] SSL certificate enabled
- [ ] Verify all pages load correctly in production

### Water-Intel Product Experience
- [ ] Publish a Water-Intel product page that explains the difference between charting and interpretation
- [ ] Add an Ask Water-Intel section with guided prompts or a mock conversational demo
- [ ] Keep the 2A proxy / no advisory-prediction guardrail visible on the public page
- [ ] Make it clear what exists now (dashboard) versus what is coming next (MCP-backed conversational layer)

### Water-Intel Dashboard (stretch)
- [x] Deploy dashboard app as subdomain or path (`app.geminimatrix.ca` or `/water-intel`)
- [x] Configure FastAPI backend as API service
- [ ] OR: Keep as demo-only (localhost) and use screenshots on company site

### DNS + Domain
- [x] Point domain DNS to Hostinger
- [ ] Configure www redirect
- [ ] Test from external network

## Acceptance Criteria
- [x] Company website live at production URL
- [x] HTTPS working
- [ ] No broken links or missing assets
- [ ] Public Water-Intel page makes the product feel distinct from a chart dashboard within the first screenful
- [ ] Page load time < 3 seconds

## Commit Message
```
ops: hostsigner deployment + domain config
```

## Notes
- If Hostinger doesn't support Next.js SSR natively, consider static export
- Alternative: Vercel free tier for the company site, Hostinger for the API/dashboard
- Document the deployment process in `docs/DEPLOYMENT.md` for future reference
