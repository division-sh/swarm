# EPO Investigation Surfaces

API authority note: retired `/api/*`, `/rpc`, and `/api/rpc` references in this
document are historical only. They are not supported operator API surfaces and
are not competing API specs. The canonical user-facing API contract lives in
`docs/specs/swarm-platform/platform/contracts/platform-spec.yaml`
`api_specification`, and `openrpc.json` is generated from that section.

## Purpose

This document lists the investigation surfaces and evidence sources available to the Empire Product Compatibility Owner.

The role should use the cheapest surface that can falsify the risky assumption before escalating to broader or more expensive runtime investigation.

## Default Working Assumptions

- The platform spec is authoritative.
- Product behavior is evidence, not semantic truth.
- Product-exposed failures should be translated into generic runtime/spec terms before they are handed off.
- Raw product contracts should not be shared with implementers unless strictly necessary.
- Logs may be used as evidence, but sanitized excerpts are preferred.

## Investigation Ladder

Use these in roughly this order:

1. targeted contract verification
2. supported startup smoke
3. supported run start and status surfaces
4. persisted runtime evidence
5. provider transcript inspection
6. full live runtime monitoring

## Core Surfaces

### 1. Current Head And Workspace State

Use this first to avoid investigating stale code.

Typical checks:
- `git fetch origin`
- `git rev-parse HEAD`
- `git rev-parse origin/master`
- `git status --short`

Preferred current-mainline workspace:
- `/Users/youmew/dev/swarm/worktrees/origin-master-current`

### 2. Targeted Contract Verify

Use this as the default cheap compatibility gate.

Command:

```bash
go run ./cmd/swarm verify \
  --contracts /Users/youmew/swarm/empire/contracts \
  --platform-spec docs/specs/swarm-platform/platform/contracts/platform-spec.yaml
```

Best for:
- contract validity
- verifier false positives
- event topology coherence
- tool declaration coherence
- boot-time semantic checks that should be caught before runtime

### 3. Supported Full-Run Helper

Use this when verify is green or when the suspected issue is runtime-only.

Command:

```bash
SWARM_TOOL_GATEWAY_URL=http://127.0.0.1:8081 \
SWARM_TOOL_GATEWAY_CONTAINER_URL=http://host.docker.internal:8081 \
make run-clear
```

Best for:
- startup readiness
- boot/runtime parity
- operator/helper path failures
- real run initialization problems

### 4. Runtime Status Surface

Use this to distinguish active progress from a stalled or incoherent run lifecycle.

Command:

```bash
go run ./cmd/swarm status
```

Best for:
- run operational state
- blocking layer
- blocking reason
- agent state snapshot
- whether a run is truly progressing or only marked `running`

### 5. Health And Control Plane APIs

Use these for lightweight runtime inspection without attaching to the database first.

Common surfaces:
- `/healthz`
- `/readyz`
- `/v1/rpc` `agent.list` / `agent.get`
- `/v1/rpc` `agent.send_directive`

Best for:
- readiness confirmation
- active agent discovery
- live directive-path reproduction through the canonical v1 owner
- builder/operator surface mismatches

Retired dashboard `/api/agents*` and Builder `/api/rpc` routes are historical
references only; they are not supported investigation surfaces.

### 6. Runtime Process Log

Current log location:
- `/tmp/swarm-empire.log`

Best for:
- boot sequence
- startup failures
- top-level runtime errors
- helper/operator failures

Important limitation:
- this log is not the best source for detailed per-turn LLM progress
- absence of visible progress here does not mean the agent is idle

### 7. Persisted Runtime Evidence

Use persisted store surfaces when the plain process log is insufficient.

Common tables:
- `runs`
- `events`
- `event_deliveries`
- `agent_turns`
- `agent_conversation_audits`
- `agent_sessions`
- `event_receipts`
- `entity_state`
- `entity_mutations`
- `flow_instances`
- `dead_letters`
- `timers`
- `mailbox`

Best for:
- run state contradictions
- event progression
- delivery completion vs stalled runs
- turn timing
- turn payload and block inspection

Useful patterns:
- check whether the run row says `running`, `failed`, or `completed`
- compare that with same-run event and delivery activity
- inspect `agent_turns.turn_blocks` when `status` output is too coarse

### 7a. Live Database Shape

The current live database exposes these public tables:

- `agent_conversation_audits`
- `agent_sessions`
- `agent_turns`
- `agents`
- `dead_letters`
- `derivation`
- `discovery_phase`
- `entity_mutations`
- `entity_state`
- `event_deliveries`
- `event_receipts`
- `events`
- `flow_instances`
- `identity`
- `mailbox`
- `metadata`
- `operating_phase`
- `routing_rules`
- `runs`
- `scan_accumulators`
- `schema_version`
- `scoring_phase`
- `search_campaign_state`
- `spend_ledger`
- `timers`
- `validation_phase`
- `validation_pipelines`
- `workflow_state`

Treat these as two broad classes:

- generic runtime investigation tables:
  - `runs`
  - `events`
  - `event_deliveries`
  - `event_receipts`
  - `agent_sessions`
  - `agent_turns`
  - `agent_conversation_audits`
  - `agents`
  - `flow_instances`
  - `entity_state`
  - `entity_mutations`
  - `dead_letters`
  - `timers`
  - `mailbox`
- domain- or flow-specific state tables:
  - `discovery_phase`
  - `scoring_phase`
  - `validation_phase`
  - `operating_phase`
  - `search_campaign_state`
  - `scan_accumulators`
  - `validation_pipelines`
  - `workflow_state`
  - `spend_ledger`
  - `derivation`
  - `identity`
  - `routing_rules`
  - `metadata`

For generic runtime bug intake, start with the generic runtime investigation tables first.

### 7b. High-Value Table Fields

The current live schema makes these fields especially useful:

- `runs`
  - `run_id`
  - `status`
  - `trigger_event_id`
  - `trigger_event_type`
  - `entity_count`
  - `event_count`
  - `error_summary`
  - `started_at`
  - `ended_at`
- `events`
  - `event_id`
  - `run_id`
  - `event_name`
  - `entity_id`
  - `flow_instance`
  - `scope`
  - `payload`
  - `produced_by`
  - `produced_by_type`
  - `handler_node`
  - `source_event_id`
  - `created_at`
- `event_deliveries`
  - `delivery_id`
  - `run_id`
  - `event_id`
  - `subscriber_type`
  - `subscriber_id`
  - `status`
  - `retry_count`
  - `reason_code`
  - `last_error`
  - `active_session_id`
  - `started_at`
  - `delivered_at`
- `event_receipts`
  - `receipt_id`
  - `event_id`
  - `subscriber_type`
  - `subscriber_id`
  - `outcome`
  - `reason_code`
  - `state_before`
  - `state_after`
  - `side_effects`
  - `duration_ms`
  - `processed_at`
- `agent_sessions`
  - `session_id`
  - `run_id`
  - `agent_id`
  - `entity_id`
  - `flow_instance`
  - `scope_key`
  - `scope`
  - `turn_count`
  - `runtime_mode`
  - `runtime_state`
  - `lease_holder`
  - `lease_expires_at`
  - `status`
  - `termination_reason`
  - `termination_detail`
  - `successor_session_id`
  - `terminated_at`
- `agent_turns`
  - `turn_id`
  - `run_id`
  - `agent_id`
  - `session_id`
  - `trigger_event_id`
  - `trigger_event_type`
  - `available_tools`
  - `tool_calls`
  - `emitted_events`
  - `mcp_servers`
  - `mcp_tools_listed`
  - `mcp_tools_visible`
  - `request_payload`
  - `response_payload`
  - `parse_ok`
  - `latency_ms`
  - `error`
  - `turn_blocks`
- `agent_conversation_audits`
  - `session_id`
  - `run_id`
  - `agent_id`
  - `conversation`
  - `turn_count`
  - `runtime_mode`
  - `runtime_state`
  - `status`
  - `created_at`
  - `updated_at`
- `agents`
  - `agent_id`
  - `flow_instance`
  - `conversation_mode`
  - `subscriptions`
  - `emit_events`
  - `tools`
  - `permissions`
  - `status`
  - `turn_count`
  - `last_active_at`
  - `runtime_descriptor`
- `entity_state`
  - `entity_id`
  - `flow_instance`
  - `entity_type`
  - `slug`
  - `name`
  - `current_state`
  - `gates`
  - `fields`
  - `accumulator`
  - `revision`
  - `entered_state_at`
- `entity_mutations`
  - `mutation_id`
  - `run_id`
  - `entity_id`
  - `field`
  - `old_value`
  - `new_value`
  - `caused_by_event`
  - `writer_type`
  - `writer_id`
  - `handler_step`
  - `created_at`
- `flow_instances`
  - `instance_id`
  - `flow_template`
  - `mode`
  - `parent_instance`
  - `config`
  - `status`
  - `created_at`
  - `terminated_at`
- `dead_letters`
  - `dead_letter_id`
  - `original_event_id`
  - `original_event`
  - `original_payload`
  - `entity_id`
  - `flow_instance`
  - `failure_type`
  - `error_message`
  - `retry_count`
  - `handler_node`
  - `created_at`
- `timers`
  - `timer_id`
  - `timer_name`
  - `entity_id`
  - `flow_instance`
  - `fire_event`
  - `fire_payload`
  - `fire_at`
  - `owner_node`
  - `owner_agent`
  - `status`
  - `fired_at`
- `mailbox`
  - `item_id`
  - `entity_id`
  - `flow_instance`
  - `scope`
  - `item_type`
  - `source_event_id`
  - `from_agent`
  - `severity`
  - `summary`
  - `payload`
  - `status`
  - `decision`
  - `decided_by`
  - `decided_at`

### 7c. First Tables To Query By Failure Class

Use this as a quick triage map:

- run stuck, premature terminal state, or incoherent lifecycle:
  - `runs`
  - `events`
  - `event_deliveries`
  - `event_receipts`
- agent appears idle or stuck:
  - `agents`
  - `agent_sessions`
  - `agent_turns`
  - provider transcript files
- missing emitted event, delivery drift, or routing confusion:
  - `events`
  - `event_deliveries`
  - `event_receipts`
  - `dead_letters`
- entity state contradiction:
  - `entity_state`
  - `entity_mutations`
  - `events`
- template/static flow startup confusion:
  - `flow_instances`
  - `events`
  - `agents`
  - `agent_sessions`
- human task / escalation / notification issues:
  - `mailbox`
  - `events`
  - `event_deliveries`

### 8. Docker And Container Inspection

Use container inspection when you need to understand the actual runtime environment seen by the agent.

Typical checks:
- `docker ps`
- `docker inspect <container>`
- `docker exec <container> sh -lc 'pwd && ls'`

Best for:
- mount verification
- environment verification
- confirming where `/workspace`, `/data`, and contracts are mounted
- checking whether a failure is runtime behavior or container/config drift

### 9. Provider Transcript Files

These are often the best live source for LLM turn progress.

Current known location inside the agent container:
- `/home/agent/.claude/projects/-workspace/*.jsonl`

Best for:
- actual assistant/tool conversation
- queued prompt flow
- tool-use attempts
- tool-result errors
- distinguishing "LLM is still working" from "runtime is idle"

Important notes:
- these files may exist even when the DB or process log looks quiet
- they can contain product-specific details, so sanitize before sharing

### 10. Workspace And Data Mounts

Inspect these when agent behavior suggests path confusion, missing inputs, or file-discovery drift.

Common mounts:
- `/workspace`
- `/data`
- `/opt/swarm/contracts`

Best for:
- confirming expected files actually exist
- understanding why an agent is reading or writing the wrong place
- separating runtime tool failure from bad mounted input assumptions

## Evidence Handling Rules

When escalating to implementers:

- prefer a generic reproducer over raw product context
- prefer sanitized logs over raw logs
- include exact observed path and current-head repro status
- do not overclaim root cause at intake

Use raw logs only when:
- the symptom cannot be reproduced otherwise
- the logs are necessary for repro/classification
- the audience is minimal

## Practical Monitoring Sequence

For a live run investigation:

1. confirm current head and workspace cleanliness
2. run targeted verify
3. start the supported run path
4. check `status`
5. inspect `/tmp/swarm-empire.log`
6. inspect `runs`, `events`, `event_deliveries`, and `agent_turns`
7. if LLM progress is unclear, inspect provider transcript files in the container
8. only then decide whether the symptom is runtime, contract, config, or still unknown

## Output Standard

Every investigation should aim to produce:

- observed symptom
- exact observed path or surface
- evidence
- current-head repro status
- tentative hypotheses
- explicit unknowns

That is enough for symptom intake. Final classification comes later.
