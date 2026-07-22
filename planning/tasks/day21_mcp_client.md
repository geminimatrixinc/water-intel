# Day 21 — MCP Client: Consume External Data Agents

**Sprint:** Week 5 — MCP Agent Architecture  
**Status:** 🔲 Not Started  
**Depends on:** Day 20 (MCP server working)  
**🚧 DEFERRED:** This and Days 22–23 (autonomous agent, federated mesh) are the *expensive, speculative* agent work. **Do not start until a funded pilot or buyer asks for it** — see [gate_credentials_and_funding.md](gate_credentials_and_funding.md). The lean Day 20 server is enough for funding demos.

---

## Objective
Build MCP client adapters so Water-Intel can pull real-time data from external sources (weather, water levels, advisories) and enrich its risk scoring with cross-domain context.

## Why This Matters
- Water risk doesn't exist in isolation — weather, flows, and upstream events drive it
- Agent-to-agent data enrichment is the real power of MCP
- Demonstrates the "mesh of specialized agents" vision to funders
- Even simulating these connections shows the architecture's potential

## Deliverables

### `ai/mcp/clients/weather_client.py`
- Connect to Environment Canada weather data (or simulated MCP server for demo)
- Pull: current conditions, precipitation forecast, flood warnings
- Methods: `get_current_weather(station)`, `get_precip_forecast(station, hours)`

### `ai/mcp/clients/water_level_client.py`
- Connect to ECCC hydrometric data (or simulated MCP server for demo)
- Pull: real-time water levels, discharge rates, flood stage
- Methods: `get_water_level(station)`, `get_discharge(station)`

### `ai/mcp/clients/advisory_client.py`
- Connect to ISC drinking water advisory data (or simulated)
- Pull: active advisories, advisory history, community status
- Methods: `get_active_advisories(region)`, `get_advisory_history(community)`

### `ai/mcp/server/demo_external_server.py` (simulated external MCP server for demo)
- Returns realistic mock data for weather, water levels, advisories
- Used when real external MCP servers don't exist yet
- Clearly labeled as demo/simulated

### Integration with Risk Scoring
- Modify `ml/src/models/risk_score.py` to accept optional weather/level context
- Weather correlation: heavy rain → turbidity spike expected → risk modifier
- Water level anomaly: sudden discharge change → upstream event → risk modifier

## Acceptance Criteria
- [ ] Weather client connects and returns weather data (real or simulated)
- [ ] Water level client connects and returns hydrometric data
- [ ] Advisory client returns active advisory status
- [ ] Risk score can optionally incorporate external context
- [ ] Demo: "Water-Intel detected turbidity spike + heavy rain upstream = correlated event"
- [ ] All external data clearly labeled with source + freshness timestamp

## Demo Script
> "Watch this — I'll ask Water-Intel about the Grand River. It pulls its own anomaly data, cross-references with the weather agent which reports heavy rain, checks the water level agent which shows elevated discharge, and concludes: *'Turbidity spike correlated with upstream rain event — elevated but expected. Monitor, don't escalate.'* That's multi-agent reasoning."

## Commit Message
```
feat: mcp clients — weather, water level, advisory data agents
```

## Notes
- Phase 3B from the Roadmap
- Start with simulated external servers — real integrations come when APIs exist as MCP
- Environment Canada has REST APIs that could be wrapped as MCP servers later
- The demo value is the architecture pattern, not the specific data feeds
