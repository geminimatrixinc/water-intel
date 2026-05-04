# Tool-use guidance

- Prefer Water-Intel's own structured outputs before external narrative sources.
- Reuse canonical schemas from `ai\mcp\schemas\` and shared contracts from `packages\contracts\` when they exist.
- Keep prompts and workflows aligned to the canonical backend in `services\api\`.
- Treat `web\app\` as a presentation surface, not the source of truth for agent logic.
