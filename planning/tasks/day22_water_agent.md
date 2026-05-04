# Day 22 — Autonomous Water-Intel Agent (LLM + MCP Orchestration)

**Sprint:** Week 5 — MCP Agent Architecture  
**Status:** 🔲 Not Started  
**Depends on:** Day 20 (MCP server), Day 21 (MCP clients)

---

## Objective
Build an LLM-powered agent that orchestrates Water-Intel's own MCP tools + external MCP sources to answer natural language questions, generate briefings, and proactively detect converging risk factors.

## Why This Matters
- Operators shouldn't need to read dashboards — they should get plain-language briefings
- An agent that reasons across sources catches things individual systems miss
- "Ask the water agent" is a compelling demo that non-technical stakeholders immediately understand
- This is the foundation for automated daily briefings and proactive alerts

## Deliverables

### `ai/agents/water-intel/workflows/water_agent.py` (Reasoning Agent)
Capabilities:
- Answer natural language questions: *"What's happening on the Grand River today?"*
- Generate daily operator briefings: risk summary, anomalies, weather context
- Cross-reference multiple MCP sources to detect converging risk factors
- Explain its reasoning in plain language (not ML jargon)

### `ai/agents/water-intel/workflows/briefing_generator.py`
- Automated daily briefing template
- Pulls from: Water-Intel MCP (risk + anomalies) + Weather MCP + Water Level MCP
- Output format: structured markdown or JSON
- Example:
  ```
  DAILY WATER BRIEFING — Grand River Watershed — 2026-04-10
  
  OVERALL STATUS: WATCH (2 sites elevated)
  
  SITE UPDATES:
  • GRAND_RIVER_01 (Bridgeport): Risk 42 → WATCH
    - Turbidity z-score elevated (2.3σ above 30-day mean)
    - Correlates with 28mm rainfall at Cambridge station yesterday
    - Trend: likely to normalize within 48h if no further rain
    
  • GRAND_RIVER_05 (Glen Morris): Risk 28 → SAFE
    - All parameters within normal range
    - No upstream anomalies propagating
  
  WEATHER OUTLOOK: Clearing. No rain expected 72h. 
  RECOMMENDATION: Continue standard monitoring. Re-check GRAND_RIVER_01 tomorrow.
  ```

### `ai/agents/water-intel/prompts/system_prompt.md`
- Agent system prompt with:
  - Role: Water safety intelligence analyst
  - Available tools (MCP tools it can call)
  - Guardrails: 2A proxy data, not advisory prediction
  - Tone: clear, practical, operator-friendly

## Technical Approach
1. Use Claude API (or OpenAI) with tool use / function calling
2. Register Water-Intel MCP tools as available functions
3. Agent decides which tools to call based on the question
4. Chain-of-thought reasoning: gather data → analyze → synthesize → respond
5. Briefing generator runs on schedule (cron) or on-demand

## Acceptance Criteria
- [ ] Agent answers: "What's the risk at GRAND_RIVER_01?" with correct data
- [ ] Agent generates a multi-site daily briefing
- [ ] Agent cross-references weather + water data in its reasoning
- [ ] All responses include 2A proxy disclaimer
- [ ] Agent explains *why* in plain language, not statistical jargon
- [ ] Briefing can be output as markdown file or printed to console

## Stretch Goals
- Slack/Teams webhook for daily briefing delivery
- Agent detects converging risk factors proactively (not just when asked)
- Voice interface prototype (operator asks verbally on mobile)

## Commit Message
```
feat: autonomous water-intel agent with LLM reasoning + daily briefings
```

## Notes
- Phase 3C from the Roadmap
- Start with Claude API (Anthropic) — best tool-use support for MCP
- Keep the system prompt tight — this agent should be helpful, not chatty
- Cost: ~$0.01–0.05 per briefing generation (Claude Haiku/Sonnet)
- This is the showstopper demo piece for funding conversations
