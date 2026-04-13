# Day 20 — MCP Server: Expose Water-Intel as Agent Tools

**Sprint:** Week 5 — MCP Agent Architecture  
**Status:** 🔲 Not Started  
**Depends on:** Day 12 (FastAPI running), Day 14 (demo pack shipped)

---

## Objective
Wrap the existing Water-Intel FastAPI endpoints as MCP tools so that any MCP-compatible AI agent (Claude, Copilot, GPT, custom agents) can query water safety data in real time.

## Why This Matters
- Positions Water-Intel as **infrastructure**, not just a dashboard
- Any AI agent can now ask: "What's the water risk at Six Nations?"
- Differentiator for funding conversations — shows network-ready architecture
- Low lift: we're wrapping endpoints that already exist

## Deliverables

### `mcp/water_intel_server.py` (MCP Server)
MCP tools (wrapping existing API logic):
- `get_risk_score(site_id)` → `{score, label, last_updated}`
- `get_anomalies(site_id, days=30)` → anomaly list with drivers
- `get_site_summary(region?)` → all sites with risk status
- `get_site_list()` → monitored station IDs + names
- `get_parameter_trend(site_id, parameter, window_days)` → time-series values

MCP resources (static/slow-changing data):
- `water://sites` → list of monitored stations with metadata
- `water://data-dictionary` → parameter definitions + units

### `mcp/config.json` (MCP server config for Claude Desktop / VS Code)
```json
{
  "mcpServers": {
    "water-intel": {
      "command": "python",
      "args": ["mcp/water_intel_server.py"],
      "env": {}
    }
  }
}
```

### Dependencies
- `mcp` (Python MCP SDK)
- `pandas` (already installed)

## Technical Approach
1. Install `mcp` SDK: `pip install mcp`
2. Reuse data-loading logic from `api/main.py` (import, don't duplicate)
3. Define tools with `@server.tool()` decorators
4. Define resources with `@server.resource()` decorators
5. Use stdio transport (standard for local MCP servers)
6. Test by connecting from Claude Desktop or VS Code Copilot

## Acceptance Criteria
- [ ] MCP server starts and registers tools
- [ ] `get_risk_score("GRAND_RIVER_01")` returns valid JSON via MCP
- [ ] `get_anomalies("GRAND_RIVER_01")` returns anomaly list with driver hints
- [ ] `get_site_summary()` returns all sites with risk scores
- [ ] Resources resolve: `water://sites`, `water://data-dictionary`
- [ ] Works from Claude Desktop (add to `claude_desktop_config.json`)
- [ ] Works from VS Code Copilot (add to `.vscode/mcp.json`)

## Guardrails
- All responses include `"data_class": "2A_proxy"` — not advisory prediction
- Community-scoped data only (no cross-community leakage in future multi-tenant)
- No PII in any MCP responses

## Commit Message
```
feat: mcp server — expose water-intel tools for agent integration
```

## Notes
- This is Phase 3A from the Roadmap
- MCP tools are essentially typed function calls — our FastAPI logic maps 1:1
- The MCP server runs as a subprocess (stdio), separate from the FastAPI HTTP server
- Both can coexist: HTTP for the dashboard, MCP for agent-to-agent
