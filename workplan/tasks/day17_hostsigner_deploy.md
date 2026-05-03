# Day 17 — HostSigner Deployment + Domain Configuration

**Sprint:** Week 4 — Traction + Company Presence  
**Status:** 🔲 Not Started  
**Depends on:** Day 16 (company website built)  
**Blocked by:** Day 16, HostSigner account access

---

## Objective
Activate the dormant HostSigner account, deploy both the company website and (optionally) the Water-Intel dashboard, configure domains, and ensure production readiness. Use the hosted product page as the public credibility surface for Water-Intel: dashboard proof, screenshots, and an Ask Water-Intel guided narrative that distinguishes the product from an ordinary chart monitor.

## Deliverables

### HostSigner Setup
- [ ] Log into dormant HostSigner account
- [ ] Review current plan/resources available
- [ ] Configure Node.js hosting environment for Next.js

### Company Website Deployment
- [ ] Build production bundle (`next build && next export` or SSR depending on host)
- [ ] Deploy to HostSigner
- [ ] Configure custom domain (geminimatrix.ca or similar)
- [ ] SSL certificate enabled
- [ ] Verify all pages load correctly in production

### Water-Intel Product Experience
- [ ] Publish a Water-Intel product page that explains the difference between charting and interpretation
- [ ] Add an Ask Water-Intel section with guided prompts or a mock conversational demo
- [ ] Keep the 2A proxy / no advisory-prediction guardrail visible on the public page
- [ ] Make it clear what exists now (dashboard) versus what is coming next (MCP-backed conversational layer)

### Water-Intel Dashboard (stretch)
- [ ] Deploy dashboard app as subdomain or path (`app.geminimatrix.ca` or `/water-intel`)
- [ ] Configure FastAPI backend as API service
- [ ] OR: Keep as demo-only (localhost) and use screenshots on company site

### DNS + Domain
- [ ] Point domain DNS to HostSigner
- [ ] Configure www redirect
- [ ] Test from external network

## Acceptance Criteria
- [ ] Company website live at production URL
- [ ] HTTPS working
- [ ] No broken links or missing assets
- [ ] Public Water-Intel page makes the product feel distinct from a chart dashboard within the first screenful
- [ ] Page load time < 3 seconds

## Commit Message
```
ops: hostsigner deployment + domain config
```

## Notes
- If HostSigner doesn't support Next.js SSR natively, consider static export
- Alternative: Vercel free tier for the company site, HostSigner for the API/dashboard
- Document the deployment process in `docs/DEPLOYMENT.md` for future reference
