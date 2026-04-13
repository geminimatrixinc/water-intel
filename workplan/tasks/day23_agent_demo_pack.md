# Day 23 — Agent Demo Pack + Federated Architecture Design

**Sprint:** Week 5 — MCP Agent Architecture  
**Status:** 🔲 Not Started  
**Depends on:** Day 22 (water agent working)

---

## Objective
Package the MCP agent architecture into a compelling demo, document the federated agent mesh design for Phase 3D, and update all business/funding docs to reflect the agent capability.

## Why This Matters
- The agent demo is potentially the most compelling piece for funders
- "Watch the AI agent analyze your water" beats a static dashboard every time
- Federated design document shows the long-term network vision
- Updated business docs position us for larger funding asks

## Deliverables

### Agent Demo Script (`docs/agent_demo_script.md`)
90-second live demo:
1. **Ask the agent** a natural language question about water safety
2. **Watch it reason** — calls risk score, pulls anomalies, checks weather
3. **Read the briefing** — plain-language summary with recommendations
4. **Show the mesh** — diagram of how multiple agents would interconnect

### Federated Architecture Design (`docs/FEDERATED_AGENT_DESIGN.md`)
- Architecture diagram: community agents ↔ regional agents ↔ national intelligence
- Data sovereignty model: what stays local, what gets shared, consent mechanism
- OCAP® alignment matrix for each data flow
- Technical: MCP transport options (stdio local, SSE remote, WebSocket future)
- Security: agent authentication, encrypted transport, audit logging

### Updated Business Docs
- `docs/BUSINESS_PLAN.md` — add Phase 3 agent platform as product evolution
- `docs/FUNDING_STRATEGY.md` — add agent/AI-specific funding targets (ISED AI, NRC IRAP)
- `docs/PILOT_ONE_PAGER.md` — mention agent capability as Phase 3 horizon

### Screenshots / Recordings
- Terminal screenshot: agent answering a water question
- Briefing output screenshot
- Architecture diagram (Mermaid → PNG)
- Save to `docs/screens/`

## Acceptance Criteria
- [ ] Agent demo script runs reproducibly in 90 seconds
- [ ] Federated architecture document complete with diagrams
- [ ] Business plan updated with Phase 3 agent vision
- [ ] Funding strategy includes AI/agent-specific programs
- [ ] At least 2 screenshots in `docs/screens/`
- [ ] README updated to mention MCP agent capability

## Commit Message
```
docs: mcp agent demo pack + federated architecture design
```

## Notes
- Phase 3D design is documented now, built later (requires multiple community partners)
- The federated model is the long-term moat — hard to replicate, massive network effects
- Keep the demo script tight — funders have short attention spans
- The agent demo + the dashboard demo are complementary: dashboard for operators, agent for decision-makers
