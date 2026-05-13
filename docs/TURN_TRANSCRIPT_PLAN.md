# Turn Transcript Plan

## Goal

Make the v1 `conversation.get` API return a canonical execution transcript built
from persisted agent turns, not a provider transcript blob scraped out of
`agent_sessions.conversation`.

## Implementation Order

1. Define a canonical per-turn transcript model.
   - Ordered blocks.
   - Minimum block kinds:
     - `dispatch`
     - `tool_use`
     - `tool_result`
     - `assistant_text`
     - `outcome`
     - `reasoning`
   - `progress` stays optional until it becomes a first-class runtime concept.

2. Persist normalized blocks with `agent_turns`.
   - Add `turn_blocks JSONB`.
   - Keep existing fields:
     - `tool_calls`
     - `emitted_events`
     - `request_payload`
     - `response_payload`

3. Normalize at turn ingest time.
   - Parse provider output once when the turn completes.
   - Reuse the CLI stream structure already captured by the runtime.
   - Keep raw provider payload only as debug/supporting data.

4. Define extraction rules.
   - `dispatch`: from request payload / trigger event context.
   - `tool_use`: from streamed tool-use blocks or parsed tool call list.
   - `tool_result`: from `user` tool_result messages.
   - `assistant_text`: final visible assistant output.
   - `outcome`: final assistant-visible summary.
   - `reasoning`: only explicit provider thinking blocks.

5. Expose the canonical transcript through the v1 conversation API.
   - Keep `messages[]` from `agent_sessions` for chat continuity.
   - Add `turns[]` with `turn_blocks`.
   - Frontend should render execution from `turns[]`, not scrape `messages[]`.

6. Backward compatibility.
   - New turns use persisted `turn_blocks`.
   - Old turns fall back to best-effort derivation from `response_payload.raw`.

7. Tests.
   - CLI stream fixture covering:
     - thinking
     - tool_use
     - tool_result
     - final result
   - Store tests for `turn_blocks` persistence.
   - Conversation API tests for `turns[]` ordering and block shape.

## Definition of Done

- New turns persist ordered normalized blocks.
- v1 `conversation.get` returns those blocks directly.
- Frontend no longer depends on scraping provider transcript blobs for normal
  execution rendering.
