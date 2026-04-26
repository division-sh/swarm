# Runtime Improvements And Watchlist

## Purpose

This is a living document for recurring runtime issues, debugging patterns, and architectural pressure notes.

It is expected to grow as new runtime failure patterns are discovered, but it is no longer the canonical backlog.

Source of truth:

- GitHub issues are the canonical system for:
  - open work
  - prioritization
  - ownership
  - completion tracking
- this document is for:
  - incident memory
  - symptom-to-root-cause guidance
  - architectural pressure notes
  - links to the GitHub issues that own the corresponding work

Implementation baseline:

- apply the rules in [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md) when designing fixes and refactors
- use this watchlist to record where the codebase still violates those rules or proves they matter

It should be updated whenever:

- a failure pattern reappears
- we discover a brittle architectural seam
- we add a mitigation but not a full fix
- we identify a symptom that should immediately point operators to a likely root cause

This is not limited to one subsystem. It should cover:

- runtime boot
- MCP/tool transport
- agent launch/execution
- event routing
- entity/state handling
- dashboard/transcript surfaces
- test harness reliability

## Maintenance Protocol

Follow this protocol whenever a runtime debugging session finds a real issue.

1. Log every real incident.
   - Add entries for:
     - real bugs
     - recurring false positives
     - meaningful observability blind spots
   - Do not add every minor annoyance.

2. Record the issue in two places.
   - Add or update the relevant symptom/root-cause section here.
   - Link the corresponding GitHub issue(s) that own the work.

3. Keep entries concrete.
   - Capture:
     - symptom
     - likely cause
     - actual root cause
     - current fix or mitigation
     - what signal or invariant should have caught it

4. Update GitHub issue links only when incidents validate them.
   - Add short notes like:
     - `tracked in: #...`
     - `proved critical by: ...`
   - Avoid turning this document back into a parallel issue tracker.

5. Prefer appending over rewriting history.
   - Preserve institutional memory.
   - Only merge entries when they are clearly the same failure class.

6. Periodically collapse duplicates.
   - If several incidents reduce to the same architectural flaw, merge them into one stronger pattern.

7. Treat this as an incident/triage ledger, not polished product documentation.
   - Continuity and correctness matter more than presentation.

8. Update this file at the end of any debugging session that took real effort.
   - Default habit:
     - spend 2 minutes updating this file before moving on

9. Treat unexpectedly hard test-fixing as an escalation signal.
   - If getting tests green starts taking disproportionately long, evaluate whether that difficulty points to a missing abstraction or duplicated semantics.
   - If yes, record the architectural seam here instead of treating it as “just test cleanup”.

10. Record product leakage and semantic duplication explicitly.
   - If a debugging session reveals:
     - product-specific behavior leaking into shared runtime layers
     - or more than one semantically meaningful implementation of the same rule
   - record that here as an architectural issue, not as an isolated bug only.

11. Do not maintain duplicate backlog state here.
   - If work needs to be tracked, create or link the GitHub issue.
   - If an item is only a pressure note or debugging heuristic, keep it here without pretending it is an assigned task.

## Incident Template

Use this shape whenever adding a new incident:

- `Symptom`
- `Likely cause`
- `Actual root cause`
- `Fix`
- `What should have caught it`
- `GitHub links`

## Symptom Watchlist

### Tool visible but rejected at execution time

Symptoms:

- agent sees a tool in Claude
- `available_tools` / `mcp_tools_visible` include it
- runtime still returns:
  - `tool not allowed for agent`
  - `tool is not allowed for this agent`

Likely causes:

- tool name alias mismatch:
  - canonical local name vs MCP-prefixed name
- scoped event name vs local event name mismatch
- duplicated auth checks in gateway and executor disagree
- request allowlist differs from executor-side authorization

Current mitigations:

- canonical emit-event equivalence in [`emit_runtime.go`](/Users/youmew/dev/swarm/internal/runtime/tools/emit_runtime.go)
- gateway alias normalization in [`gateway.go`](/Users/youmew/dev/swarm/internal/runtime/mcp/gateway.go)

Still brittle because:

- authorization still exists in multiple layers
- transport choice can still change whether canonical turn context is required

Improvement items:

- centralize canonical tool identity
- centralize canonical event identity
- share one authorization predicate across gateway and executor
- unify `/mcp` and `/tools/*` context-token policy so the same tool call fails or succeeds for the same reason across transports

### Agent delivery marked in progress but no useful turn output appears

Symptoms:

- delivery status stays `in_progress`
- `agent_turns` do not complete
- container exists but activity is unclear

Likely causes:

- parent Swarm runtime died after dispatch
- detached launcher/process handling bug
- Claude session never actually started
- Claude is running but blocked early on input/tool access

Current mitigations:

- `run_clear.sh` now launches Swarm detached correctly
- `run_clear.sh` kills aggressively before restart

Still brittle because:

- process lifecycle is still shell-script based

Improvement items:

- move local runtime lifecycle into a more explicit supervisor model
- expose agent-launch lifecycle state directly in status APIs

### Run starts successfully but dies on first agent turn

Symptoms:

- root event persists
- first assignment event persists
- first agent dead-letters immediately

Likely causes:

- invalid auth token
- static agents not persisted before first turn
- stale process holding old runtime state during DB reset
- MCP/tool path misconfigured for the launched session

Current mitigations:

- aggressive `run-clear`
- startup MCP validation
- container-only Claude execution

Improvement items:

- stronger startup invariants around static agent persistence
- explicit launch-time verification per managed agent

### Run says running but no active deliveries

Symptoms:

- `runs.status = running`
- no `pending` or `in_progress` deliveries
- no recent events or turn completions

Likely causes:

- stale run bookkeeping
- delivery failed silently or was already exhausted
- runtime died after partial progress

Current mitigations:

- manual status inspection using deliveries and dead letters

Improvement items:

- derive run liveness from active deliveries and recent event activity
- introduce a first-class `stalled` run state
- surface stalled-run detection in `swarm status`

### Delivery lifecycle is unclear or misleading

Symptoms:

- delivery says `in_progress` but no launch actually happened
- delivery retries without a clear explanation
- delivery looks complete but downstream state never changed
- recovery skips an event that never actually reached subscribers

Likely causes:

- launch state is inferred indirectly
- retry lifecycle is not surfaced clearly
- delivery bookkeeping is weaker than turn/session bookkeeping
- the authoritative delivered marker is recorded earlier than real subscriber fan-out

Current mitigations:

- manual correlation of deliveries, turns, and session logs

Improvement items:

- align the authoritative delivered marker with real subscriber fan-out
- align recovery replay semantics with the spec's undelivered-event contract
- define explicit delivery states such as:
  - `queued`
  - `launching`
  - `active`
  - `retrying`
  - `delivered`
  - `exhausted`
- expose delivery status transitions in runtime/debug APIs

### Boot succeeds but runtime behavior later proves MCP/tooling was incomplete

Symptoms:

- runtime reaches ready state
- agents later say emit/tool infrastructure is unavailable
- tool catalogs are non-empty but required tools are missing

Likely causes:

- boot only checked `tools/list` non-empty
- boot did not verify required `emit_*` tools for each agent
- tool generation mismatch from schema/scoping issues

Current mitigations:

- startup now fails if required emit tools are missing for managed agents

Improvement items:

- preserve end-to-end boot assertions per agent capability set
- dry-run required tool auth at boot

### Spec-valid contracts still fail boot or behave differently at runtime

Symptoms:

- a contract that appears valid against the authoritative YAML still fails boot
- a contract using documented values is rejected while undocumented values are accepted
- runtime semantics differ from what the authoritative spec describes

Likely causes:

- boot/loader/runtime widened or narrowed contract vocabulary independently of the spec
- validation severity does not match the spec's boot-failure contract
- runtime fallback behavior still exists outside explicit spec-owned actions

Current mitigations:

- ad hoc conformance and boot tests for selected seams
- manual comparison against the authoritative YAML

Improvement items:

- reconcile boot and loader vocabulary with the authoritative spec
- make invalid event payloads fail closed before persistence in canonical runtime paths
- align delivery receipts and restart recovery semantics with the spec

### Flow-owned session-scope proof drifts across supported consumers

Symptoms:

- package-backed flow-owned agents verify cleanly on one surface but fail on another
- `session_scope flow/entity` is accepted in boot verification but later rejected during runtime startup/static bootstrap
- supported consumers disagree about whether an agent is flow-owned or root-level

Likely causes:

- loader/tree ownership proof is preserved in one carrier but not another
- semantic view, boot verification, and static bootstrap consume different flow-ownership carriers
- runtime startup derives scope from contract-source locality while boot verification uses a stronger owning-flow proof

Current mitigations:

- package-backed project scopes now carry `OwningFlowID` in semantic view
- boot verification consumes that owning-flow proof for `session_scope` validation

Still brittle because:

- runtime startup/static bootstrap can still bypass that proof model and classify the same agent through weaker local metadata

Improvement items:

- define one canonical agent-level owning-flow/session-scope proof model
- make boot verification and static bootstrap consume the same proof owner
- keep targeted verify and supported startup smoke as closure gates for this class
- tracked in: `#253`

### Host/container path or network split causes environment confusion

Symptoms:

- path works in container but not on host, or vice versa
- MCP URL is correct in one execution shape and wrong in another
- debugging from the host suggests a file or endpoint is missing even though the agent can see it

Likely causes:

- host and container path spaces differ
- host-local and container-local runtime paths are mixed mentally or operationally
- environment-specific defaults leak into the wrong execution path

Current mitigations:

- container-only Claude runtime
- explicit gateway URL config

Improvement items:

- document host vs container path expectations clearly
- add startup diagnostics that print effective container-visible paths and MCP endpoint
- avoid ambiguous defaults across host/container boundaries

### Tool call succeeds once then later the same family fails differently

Symptoms:

- one emit/tool call succeeds
- later emit/tool calls in the same turn or session fail for a different reason

Likely causes:

- payload/schema drift across calls
- context loss mid-turn
- per-call normalization or authorization mismatch
- publish path rejecting only certain payload shapes

Current mitigations:

- manual log inspection

Improvement items:

- log canonical tool id, canonical event id, payload validation result, and publish result per tool call
- expose tool-call outcomes as structured turn diagnostics

### Tool exists and is authorized, but payload/schema handling fails late

Symptoms:

- tool is generated
- tool is visible
- tool is authorized
- failure happens only at payload validation or publish time

Likely causes:

- schema drift between contracts and tool execution
- JSON serialization inconsistencies
- context enrichment producing invalid final payloads

Current mitigations:

- schema validation errors
- ad hoc log inspection

Improvement items:

- expose pre-validation and post-enrichment payload snapshots in debug diagnostics
- separate payload-shape failures from downstream publish failures explicitly

### Session starts but produces no completed turn for too long

Symptoms:

- `turn.start` exists
- no `turn.end` within a reasonable threshold
- container/process may still be alive

Likely causes:

- Claude blocked on input handling
- tool interaction loop stuck
- runtime lost visibility into a still-running session

Current mitigations:

- manual process and log inspection

Improvement items:

- add watchdog events such as:
  - `turn_long_running`
  - `session_no_output`
- include session id, agent id, and container name in watchdog output

### Retry starts another session without a clear lineage trail

Symptoms:

- one delivery results in multiple Claude session ids
- old sessions may still be present
- operator cannot easily tell why a retry happened

Likely causes:

- auth failures
- process reuse errors
- opaque retry logic

Current mitigations:

- manual log archaeology

Improvement items:

- record explicit retry reasons
- record session lineage fields such as `retries_from_session_id`
- expose retry lineage in status/debug APIs

### Runtime restart happens mid-turn and recovery behavior is unclear

Symptoms:

- runtime restarts while a turn is active
- old Claude/container process may still exist
- delivery/session ownership after restart is ambiguous
- replayed events come back under a fresh run lineage
- some recovered work is replayed with weaker identity than what was originally persisted
- a run or template flow appears finished operationally but has no durable terminal proof

Likely causes:

- recovery model is not explicit enough
- stale external processes can outlive runtime ownership
- recovery loaders do not always preserve the full persisted event envelope
- persisted exact identity is tighter than the in-memory recovery key used to restore it
- completion is still inferred from quiescence or teardown side effects instead of one persisted terminal owner

Current mitigations:

- aggressive reset scripts
- manual orphan checks

Improvement items:

- preserve full persisted correlation ancestry during recovery replay
- unify exact timer/schedule identity across persistence, recovery, and in-memory scheduler keys
- expose recovery decisions and dropped identity/context in logs/status APIs
- persist canonical run completion instead of narrating it from quiescence
- persist canonical terminal lifecycle for template flow instances instead of inferring it from teardown

Improvement items:

- define restart/recovery invariants for in-flight turns
- detect and mark orphaned or superseded sessions explicitly after restart
- expose recovery decisions in logs/status

### Entity appears to be in two states depending on where you look

Symptoms:

- operator tools show one state
- runtime behavior reflects another
- dashboards and agents disagree on state

Likely causes:

- shared mutable entity across multiple flows
- flow-local state and root state semantics mixed together
- stale legacy state exposure

Current mitigations:

- flow-scoped entity model
- explicit business-correlation payload/config fields
- source-event causal-chain proof

Improvement items:

- keep all tooling aligned on flow-local state + subject lineage
- avoid reintroducing shared-row state shortcuts

### `on_complete` appears to emit successfully but state does not advance

Symptoms:

- `on_complete` side effects appear to happen:
  - downstream events emitted
  - logs suggest the handler finished
- but the entity state does not advance as expected
- downstream behavior looks inconsistent with the emitted event stream

Likely causes:

- state transition and side effects are not committing atomically
- condition evaluation inside `on_complete` failed late
- rollback happened after partial-looking side effects were observed indirectly

Current mitigations:

- flow-scoped entity model reduced one historical cause of split state visibility
- stricter incident tracking in this document

Still brittle because:

- this symptom can look like an eventing bug when the actual problem is state-transition atomicity

Improvement items:

- keep `on_complete` atomicity as an explicit invariant:
  - emits
  - `advances_to`
  - mutation logging
  - all in one transaction or none
- add dedicated conformance coverage for:
  - emit succeeds and state advances
  - condition failure causes neither emit nor advance
- expose transaction outcome clearly in the flight recorder

### CEL missing-key errors on optional or not-yet-written fields

Symptoms:

- guard or expression evaluation fails with:
  - `no such key: ...`
- failure happens before a field has ever been written
- operators may assume the field should be nullable, but runtime treats it as absent

Likely causes:

- expressions read optional or not-yet-materialized JSONB fields directly
- wrong entity targeting causes evaluation against an entity shape that never had the field
- contracts use existence checks that rely on implicit null semantics

Current mitigations:

- contracts can guard on built-in fields that always exist, such as `entity.current_state`, when that matches intent
- wrong-entity-routing cases are tracked separately and fixed as routing bugs, not papered over

Still brittle because:

- missing-field CEL errors can indicate either:
  - a legitimate optional-field pattern
  - or a real runtime/context bug

Improvement items:

- make optional-field handling explicit in contracts and runtime diagnostics
- preserve strict missing-key behavior by default so wrong-entity/context bugs stay visible
- add clearer diagnostics that distinguish:
  - missing optional field
  - missing required field
  - wrong entity/context selection

### Flow entity is created without schema defaults, then handlers fail on first read

Symptoms:

- flow-scoped entity exists and is the correct target
- `fields` is still empty (`{}`)
- first guard or accumulation expression that reads a defaulted field fails with:
  - `no such key: ...`
- package or flow schema declares a default, but runtime-created entity does not contain it

Likely causes:

- entity creation path is not seeding defaults from package/entity schema
- contract assumes a field starts at `0` or another default value, but runtime only materializes fields after the first explicit write

Current mitigations:

- incident is documented here so operators can recognize it quickly
- contracts can sometimes avoid the immediate failure by guarding on always-present built-ins, but that is only a workaround when business semantics allow it

Still brittle because:

- the contract and runtime disagree on whether declared defaults are real persisted initial values
- the symptom looks like a CEL/nullability issue, but the actual bug is missing entity initialization

Improvement items:

- seed declared schema defaults when creating a new flow-scoped entity
- add conformance coverage proving defaulted fields exist on newly created entities before any handler writes
- make the flight recorder and entity-creation diagnostics show which defaults were materialized at creation time

### Accumulator appears to stall or drop the final item, but the real failure is elsewhere

Symptoms:

- accumulator looks stuck at a value like `10/11`
- one final expected item never seems to land
- repeated investigation points at accumulation logic, but the actual bug is outside the accumulator

Likely causes:

- downstream `on_complete` condition evaluation failed and rolled back the final write path
- transaction outcome made it look like accumulation was incomplete, even though the apparent symptom was one layer removed
- subsystem B failed while subsystem A got blamed by the surface symptom

Current mitigations:

- this failure pattern is now explicitly documented here

Still brittle because:

- this is a misleading cross-subsystem symptom that can waste multiple debugging cycles

Improvement items:

- make accumulator completion, `on_complete` condition evaluation, and state-transition commit outcomes visible in one place
- extend the flight recorder to show:
  - accumulator write count
  - `on_complete` evaluation result
  - commit / rollback result
- add regression coverage for final-item accumulation with `on_complete` transitions

### Cross-flow write rejection surprises agents

Symptoms:

- agent calls `save_entity_field`
- runtime returns `cross_flow_write_forbidden`
- a flow-control event is emitted successfully, but the receiving flow tries to transition an entity owned by another flow
- the write guard is the first visible failure, but the deeper issue is often earlier in the event semantics

Likely causes:

- flow ownership is not obvious to the agent
- prompts/tool docs do not make write scope explicit
- an agent emitted a flow-control event that only makes sense when a target flow instance already exists
- a control event reused the current subject/scoring entity id instead of resolving the correct target flow entity

Current mitigations:

- runtime enforcement blocks the write

Improvement items:

- make flow ownership explicit in prompts and tool docs
- expose helper context for "what entity is writable from this turn"
- consider a read-only helper to resolve writable target state
- surface when a control event targets a subject with no matching target flow instance
- distinguish in diagnostics:
  - wrong entity target
  - wrong flow-control event for the lifecycle phase
  - legitimate cross-flow write attempt

### Cross-flow routing leaks topology into contracts

Symptoms:

- contracts must manually qualify subscriptions with flow-scoped names like `scoring/vertical.shortlisted`
- contract authors need to know internal routing topology to wire flows together
- it is difficult to tell whether a failure is:
  - a contract routing mistake
  - or a runtime routing bug

Likely causes:

- cross-flow event wiring is expressed directly in node subscriptions
- output/input pins are not treated as the authoritative cross-flow interface
- routing semantics are split between contract author intent and runtime scoping details

Current mitigations:

- completed at the 2026-04-05 checkpoint via issue `#23` / PR `#30`

Still brittle because:

- topology details leak into contracts
- refactors require widespread subscription rewrites
- boot validation cannot cleanly distinguish interface mismatch from naming mismatch

Improvement items:

- completed at the 2026-04-05 checkpoint via issue `#23` / PR `#30`:
  - moved to pin-based cross-flow auto-wiring by default in the touched seams
  - kept node `subscribes_to` and handler keys local-name oriented for the default path
  - resolved cross-flow routes from producer/output and consumer/input pin interfaces
  - treated manual scoped subscriptions as an explicit escape hatch
  - failed boot on ambiguous wiring unless the contract disambiguated explicitly

### Cross-flow event is emitted and subscribed, but target handler never runs

Symptoms:

- cross-flow event is visibly emitted and persisted
- target flow subscribes to that cross-flow qualified event
- no dead letter is created
- no runtime error is emitted
- downstream flow simply never starts

Likely causes:

- runtime matches subscription names but fails to localize the routed event name back to the target flow's local handler key
- handler lookup expects local flow event names while incoming routed events still carry cross-flow qualification

Current mitigations:

- runtime now localizes cross-flow qualified input events to matching declared local flow inputs before handler lookup

Still brittle because:

- cross-flow routing still mixes:
  - external routed event names
  - local handler keys
  - contract scoping knowledge

Improvement items:

- keep routed-event-to-local-handler translation explicit and tested
- surface routing decisions in the flight recorder:
  - incoming event name
  - localized handler name
  - target node id
- continue moving toward pin-based auto-wiring so contracts encode interface intent, not route topology

### Prompt/runtime contract mismatch

Symptoms:

- agents keep attempting tools they should not use
- prompts assume capabilities or writable state that runtime forbids
- model behavior looks irrational but is actually induced by prompt/tool mismatch

Likely causes:

- prompt templates lag runtime semantics
- tool visibility and prompt instructions diverge

Current mitigations:

- runtime rejects invalid operations

Improvement items:

- keep prompt templates aligned with tool visibility and state-ownership rules
- include explicit writable/readable scope cues in system prompts where needed

### Cross-flow handoff payload carries ambiguous source vs target entity identity

Symptoms:

- receiving flow entity exists and is correct
- cross-flow entry payload still looks like source-flow data
- prompts or handlers say “read source entity_id from payload”
- but `payload.entity_id` is actually the target flow entity and the source-flow entity is in another field such as `vertical_id`

Likely causes:

- cross-flow payload contract does not make source-vs-target identity explicit
- prompt text lags the actual event payload shape
- source flow context is copied into the target flow payload without a clean identity boundary

Current mitigations:

- manual SQL / transcript inspection

Improvement items:

- standardize cross-flow payload fields for:
  - target entity id
  - source entity id
  - source flow id
- align prompts to the real payload contract
- show source and target entity ids explicitly in the flight recorder

### Conversation API returns unusable assistant blobs

Symptoms:

- frontend receives one mixed assistant string
- reasoning, tool use, progress, and outcome are interleaved

Likely causes:

- API backed only by conversation snapshot
- execution transcript not normalized at persistence time

Current mitigations:

- `turns[]` from `agent_turns`
- canonical `turn_blocks` support

Improvement items:

- keep transcript normalization at ingest time
- avoid sliding back to blob parsing in API readers

### Optional infrastructure is missing but tools remain visible

Symptoms:

- agent sees or calls a tool
- runtime fails with dependency errors like:
  - `mailbox store is not configured`

Likely causes:

- tool visibility is not dependency-aware
- runtime relies on execution-time rejection instead of hiding unavailable tools

Current mitigations:

- runtime returns dependency errors

Improvement items:

- suppress unavailable tools from visible tool catalogs when required backing services are absent
- make boot/startup surface active dependency gaps clearly

### Large-file access degrades early-turn behavior

Symptoms:

- agent repeatedly tries to read an entire large file
- token-limit errors occur before useful progress starts

Likely causes:

- generic file tools with no large-input guidance
- prompts do not nudge chunked access for known large inputs

Current mitigations:

- model often recovers by switching to chunked reads manually

Improvement items:

- add prompt guidance for large known inputs
- consider purpose-built corpus/chunk reader tools

### Turn observability still requires raw log archaeology

Symptoms:

- root cause of a failure can only be understood from raw session logs
- status APIs do not explain the failing layer

Likely causes:

- turn records do not yet expose enough structured diagnostics

Current mitigations:

- transcript persistence and turn blocks

Improvement items:

- add structured turn fields for:
  - `auth_denials`
  - `tool_exec_failures`
  - `context_fallbacks`
  - `publish_failures`
  - `retry_reasons`

### Flight recorder is missing or incomplete

Status:

- materially addressed at the 2026-04-04 checkpoint via issue `#4` / PR `#13`
- canonical per-turn flight-recorder entries now flow through `platform.runtime_log` and `agent_turns.turn_blocks`
- keep this section as a regression guard and for remaining spec/debug-surface follow-up, not as an unstarted initiative

Symptoms:

- root cause can only be reconstructed from scattered logs
- it is hard to correlate in one place:
  - offered tools
  - visible tools
  - allowed tools
  - tool calls
  - tool denials
  - payload validation outcomes
  - publish results
  - retries
  - session lineage

Likely causes:

- runtime diagnostics are split across logs, DB rows, and partial APIs
- no authoritative per-turn execution trace exists

Current mitigations:

- transcript persistence
- turn blocks
- raw runtime logs

Improvement items:

- persist a structured per-turn flight recorder timeline
- make it queryable via API
- make it the primary debugging surface instead of raw log scraping
- ensure it captures both success and denial/failure transitions

### Spec-level flight recorder and run-debug API surface is ahead of runtime

Symptoms:

- spec describes run-debug and flight-recorder query surfaces
- operators still have to combine raw SQL, logs, and partial APIs manually

Likely causes:

- runtime implemented transcript improvements first
- run-level debug endpoints were not completed to spec depth

Current mitigations:

- `turn_blocks`
- conversation API improvements
- direct DB inspection

Improvement items:

- implement spec-aligned run-debug surfaces for:
  - run events
  - run mutations
  - fork/pause/resume/cancel lifecycle where intended
- make the flight recorder usable without raw log scraping or ad hoc SQL joins

### Status APIs under-report the failing layer

Symptoms:

- `swarm status` says little beyond counts
- the actual failing layer has to be inferred from logs

Likely causes:

- status surfaces aggregate counts but not failure classification

Current mitigations:

- manual status plus log inspection

Improvement items:

- make status output name the current blocking layer:
  - launch
  - auth
  - schema
  - publish
  - retry
  - stalled

### Context-token fallback may mask real bugs

Symptoms:

- tool still executes after token lookup misses or falls back
- runtime appears healthy while turn context integrity is degraded

Likely causes:

- permissive fallback behavior for convenience

Current mitigations:

- context fallback logging

Improvement items:

- make fallback use highly visible in diagnostics
- reduce or eliminate fallback for mutating tools over time

### Concurrency or duplicate-work races

Symptoms:

- duplicate emits
- duplicate sessions
- overlapping retries
- the same work appears to run twice with slightly different outcomes

Likely causes:

- weak coordination between retries, context dedupe, and session ownership
- race conditions around turn recovery or duplicate launch

Current mitigations:

- emit dedupe
- manual inspection

Improvement items:

- track duplicate-work incidents explicitly
- make retry/session ownership coordination easier to inspect
- extend conformance tests for duplicate-launch and duplicate-emit scenarios

### Contract/runtime annotation drift

Symptoms:

- contracts add or rely on fields like:
  - `_source`
  - `_producer`
  - `_consumer`
  - `gate_state`
- runtime/verifier initially lags behind

Likely causes:

- contract evolution without mirrored runtime support

Current mitigations:

- manual follow-up fixes

Improvement items:

- maintain a standing checklist for new contract annotations
- require runtime/verifier coverage whenever a new annotation is introduced

### Mutation-log completeness is not yet continuously proven

Symptoms:

- spec requires every `entity_state` write to emit an `entity_mutations` row in the same transaction
- operators cannot easily prove this invariant for every write path

Likely causes:

- write paths evolved across:
  - system handlers
  - agent tools
  - recovery/state-fix flows
- no standing end-to-end audit currently proves full coverage

Current mitigations:

- `entity_mutations` table exists
- some write paths are covered
- spec defines the invariant clearly

Improvement items:

- audit every `entity_state` write path end-to-end
- add conformance tests that verify mutation-log emission for:
  - data accumulation writes
  - compute/store writes
  - sets_gate
  - advances_to
  - clear/reset operations
  - create_entity initial field population
  - `save_entity_field`
  - recovery/state-fix writes
- add a drift-detection command or test path that reconstructs entity state from `entity_mutations` and compares it to `entity_state`
- treat any write path that bypasses mutation logging as a correctness bug, not observability debt

Current extracted slices:

### Entity state changes occur but `entity_mutations` stays empty

Symptoms:

- entities clearly change state or fields during a run
- `entity_mutations` has zero rows for that run or entity
- mutation-based debugging and state reconstruction become impossible

Likely causes:

- one or more write paths bypass mutation logging entirely
- mutation logging is not in the same transactional path as state persistence
- tests prove final `entity_state` but do not assert mutation rows

Current mitigations:

- manual SQL checks

Improvement items:

- treat “state changed but no mutation row exists” as a hard conformance failure
- add end-to-end tests that assert both final state and emitted mutation rows
- add a standing audit command for run-level mutation completeness

### Observability fields drift from operator needs

Symptoms:

- fields exist in DB/logs but not in APIs
- APIs exist but do not surface the most actionable runtime state

Likely causes:

- observability evolves reactively without a stable operator contract

Current mitigations:

- ad hoc API additions

Improvement items:

- define a small operator-facing runtime observability contract
- ensure status, conversations, and debug endpoints all align to it

### Local dev lifecycle drift

Symptoms:

- local one-shot commands regress silently
- runtime appears to start but dies after the wrapper exits

Likely causes:

- shell detachment/process-lifecycle regressions
- stale process cleanup gaps

Current mitigations:

- hardened `run-clear`

Improvement items:

- keep a smoke test for local lifecycle:
  - runtime survives wrapper exit
  - ready endpoint remains up
  - default run persists expected rows

### Catalog or harness failures hide real runtime semantics

Symptoms:

- broad suite fails with infra-looking DB errors
- tier fixtures fail before semantic assertions run
- confidence in runtime changes is low because harness noise dominates

Likely causes:

- harness DB connection instability
- expectation schema drift
- old fixture assumptions surviving model changes

Current mitigations:

- harness connection fixes
- expectation schema updates

Improvement items:

- keep harness reliability treated as first-class work
- add smaller invariant tests so giant fixtures are not the only signal

### One correct identity fix exposes another hidden coupling

Symptoms:

- a focused fix makes one failing test pass
- nearby fixtures that used to pass now fail differently
- each failure looks unrelated at first:
  - wrong entity target
  - root state not advancing
  - child gate not visible
  - root output boundary warning appears

Likely causes:

- event identity, entity identity, gate scope, and flow-instance scope are implemented in different places
- one layer uses:
  - semantic flow scope such as `child`
- while another uses:
  - concrete instance paths such as `child/<instance-id>`
- behavior is still controlled by string heuristics instead of one typed model

Current mitigations:

- event identity logic is more centralized than before
- focused regressions now cover:
  - cross-flow localization
  - flow-entity id creation
  - child gate projection
  - root output boundaries

Still brittle because:

- event names, entity ids, flow scope, and flow-instance paths are still not modeled as one coherent typed subsystem
- fixing one seam can expose another seam that was only passing accidentally

Improvement items:

- introduce a first-class flow-instance descriptor covering:
  - template id
  - scope key
  - instance path
  - entity id
  - parent entity id
  - subject id
- stop using instance paths as a proxy for semantic flow scope
- centralize entity-target resolution and child/parent retargeting rules
- add direct conformance tests for each identity/scope rule so catalog fixtures are not the first place these interactions appear

### Emitted event targets the wrong entity

Symptoms:

- handler completes successfully but downstream handlers evaluate against the wrong entity
- runtime errors mention missing entity fields that should exist on the intended flow-local entity
- emitted payload contains mixed identity fields, for example:
  - `vertical_id = child flow entity`
  - `entity_id = root/parent entity`

Likely causes:

- event target selection is using heuristic parent-retargeting
- top-level flow outputs are being treated like child-flow exits
- lineage metadata such as `parent_entity_id` is present, but the entity is not actually in a child flow-instance context

Current mitigations:

- parent retargeting only applies when the current entity is in a real flow-instance context (`flow_path` present)
- focused regression tests cover:
  - child-flow output stays parent-targeted
  - root-flow output stays local

Improvement items:

- make emitted target-entity selection explicit and observable
- record for each emitted event:
  - source entity id
  - chosen target entity id
  - whether parent retargeting was applied
  - why it was applied
- add this decision stream to the flight recorder / run-debug surface

### Flow-control event is emitted for the wrong lifecycle phase

Symptoms:

- a control event like `opco.teardown_requested` is emitted successfully
- the receiving flow either has no matching instance for that subject or tries to mutate an entity owned by another flow
- runtime may only surface a later protection error such as `cross_flow_write_forbidden`

Likely causes:

- contract/agent semantics allow an event that assumes an existing downstream flow instance to be emitted before that instance exists
- event naming collapses:
  - subject/business-object identity
  - target flow entity identity
  - lifecycle phase intent
- the emitting layer chooses a valid event name without validating whether that event is meaningful in the current phase

Current mitigations:

- cross-flow write protection blocks the illegal mutation

Still brittle because:

- the visible failure happens at the write boundary, not at the earlier semantic mistake
- a semantically invalid control event can look like a routing or entity-targeting problem

Improvement items:

- validate control-event preconditions before publish when feasible:
  - target flow instance exists
  - target lifecycle phase is valid
- make subject-level control intent explicit instead of overloading flow-control events
- record in the flight recorder:
  - emitted control event
  - current subject state
  - whether a target flow instance existed
  - chosen target entity / flow instance

### Agents advance milestones without persisting required entity fields first

Symptoms:

- agent emits a progression event such as:
  - `research.completed`
  - `spec.draft_ready`
- downstream contracts assume required heavy content is already in entity state
- the required fields are still absent from `entity_state.fields`
- later agents report blocking issues like:
  - `MISSING_BUSINESS_BRIEF`
  - `MISSING_MVP_SPEC`

Likely causes:

- prompt instructions about save-before-emit are too weak
- runtime does not enforce persistence prerequisites for milestone events
- agents degrade into “emit anyway” behavior after earlier tool failures

Current mitigations:

- completed at the 2026-04-05 checkpoint via issue `#24` / PR `#29`

Improvement items:

- completed at the 2026-04-05 checkpoint via issue `#24` / PR `#29`:
  - made save-before-emit a runtime invariant for the touched milestone paths
  - added conformance tests asserting required field persistence before emit
  - surfaced missing prerequisite fields at the first invalid emit
  - captured the failure through the engine rejection path before post-commit dispatch

### Agent session gets contaminated by prior failure and stops acting usefully

Symptoms:

- early turns perform real work
- later turns in the same session degrade into:
  - “no action”
  - “standing by”
  - “operator must inject fields manually”
- tools are still available but the model no longer attempts them

Likely causes:

- session memory anchors on an earlier infrastructure failure
- retries/continuations reuse stale failure context
- runtime does not distinguish recoverable tool failure from hard-stop operator intervention clearly enough

Current mitigations:

- manual transcript inspection

Improvement items:

- track session contamination / stale failure memory explicitly
- consider restarting or quarantining sessions after certain infrastructure failures
- expose session reuse lineage and rationale
- keep flight-recorder summaries concise and current so agents do not overfit stale failures

### Validation-owned entity is incorrectly denied as foreign on agent writes

Symptoms:

- validation entity exists and is owned by the validation flow
- validation agents call `save_entity_field` on that entity
- runtime returns `cross_flow_write_forbidden`
- agents then fall back to embedding content in events or free-text summaries

Likely causes:

- write-authorization resolves ownership against the wrong flow context
- turn/entity context given to tools does not align with the entity’s owning flow instance
- source-flow identity leaks into the authorization decision after cross-flow handoff

Current mitigations:

- none beyond manual detection; the guard is firing, but on a legitimate write

Improvement items:

- audit write authorization for flow-scoped entities reached via cross-flow handoff
- add direct tests proving an agent in flow `X` can write an entity owned by flow `X` even when triggered from flow `Y`
- record in diagnostics:
  - turn flow
  - entity owner flow
  - why the write was classified as foreign

### Store behavior depends on implicit schema-shape fallbacks

Status:

- core initiative completed at the 2026-04-04 checkpoint via issue `#1` / PR `#5`
- keep this section as historical context and as a guard against regression, not as an active architecture initiative

Symptoms:

- persistence behavior changes based on which columns happen to exist
- store paths retry with older SQL after apparently unrelated query errors
- operational debugging depends on knowing whether a spec path or legacy path actually ran

Likely causes:

- store code probes schema capability on the fly
- some persistence paths still fall back by matching SQL error strings
- schema compatibility is embedded in hot runtime paths instead of one explicit boundary

Current mitigations:

- repeated `columnExists(...)` probes
- legacy fallback helpers like:
  - `shouldFallbackLegacyEventsSchema(...)`
  - `shouldFallbackLegacyAgentsSchema(...)`
  - `shouldFallbackLegacyTimersSchema(...)`

Still brittle because:

- runtime behavior is influenced by implicit database shape detection
- spec and legacy persistence semantics can drift independently

Improvement items:

- introduce one explicit store schema-capability/version boundary
- stop using SQL error-string matching as a runtime compatibility mechanism
- negotiate schema behavior once per store/runtime startup, not ad hoc inside write paths

### Expression evaluation still mixes explicit semantics with fallback interpretation

Symptoms:

- the same declarative string may be treated as:
  - a CEL expression
  - a ref
  - a literal
- fixes in expression handling often reveal hidden precedence assumptions
- contract behavior is harder to predict than it looks from the YAML

Likely causes:

- engine helpers still interpret strings through layered fallback paths
- unqualified refs search multiple buckets in order
- failed CEL evaluation can degrade into non-CEL interpretation

Current mitigations:

- targeted fixes for arithmetic `data_accumulation`
- focused tests around specific expression cases

Still brittle because:

- the runtime effectively has a mini language with implicit precedence
- evaluation semantics are spread across helper functions rather than one typed expression model

Improvement items:

- make expression classes explicit:
  - literal
  - path ref
  - CEL
- remove semantic fallback from one expression class into another
- make bucket lookup precedence explicit and test it directly

### Opaque agent config still acts as a runtime control plane

Symptoms:

- runtime-significant behavior is reconstructed from raw agent config JSON
- different subsystems decode the same config for:
  - flow path
  - subscriptions
  - manager fallback
  - workspace behavior
  - tool/runtime mode
- a config-shape change risks silent drift across manager, authority, workspace, and store code

Likely causes:

- agent config is carrying both:
  - arbitrary agent payload
  - typed control-plane semantics
- there is no separate persisted runtime descriptor for agents

Current mitigations:

- completed at the 2026-04-05 checkpoint via issue `#10` / PR `#26`

Still brittle because:

- historical risk: the real control plane was implicit inside opaque JSON
- historical risk: subsystems could stay “individually correct” while disagreeing semantically

Improvement items:

- completed at the 2026-04-05 checkpoint via issue `#10` / PR `#26`:
  - introduced a typed persisted runtime descriptor for agent control semantics
  - separated opaque agent config payload from runtime-owned fields
  - stopped re-deriving subscriptions/flow path/fallback semantics from arbitrary JSON blobs in the touched seams

### Tool invocation semantics are still distributed across multiple layers

Symptoms:

- the same tool call can be:
  - visible but rejected
  - allowed only after normalization
  - accepted at one layer and denied at another
- context requirements are inferred differently by gateway, validator, authorizer, and executor
- emit-event equivalence and native-tool registration still carry local compatibility logic

Likely causes:

- tool identity is still normalized in multiple places
- transport visibility, authorization, validation, and execution each own part of the semantics
- raw and canonical tool ids are treated as if they are interchangeable

Current mitigations:

- repeated normalization in gateway, validator, authorizer, and executor
- context fallback for selected tools
- duplicated payload/input compatibility rewrites

Still brittle because:

- one concept, “can this actor call this tool here with this payload,” has no single owner
- failures surface far from the layer that silently changed semantics

Improvement items:

- introduce one canonical tool invocation policy / capability registry
- move visibility, identity, auth, context requirements, and compatibility normalization behind that registry
- stop allowing transport and execution layers to normalize tool semantics independently

### Stateless turns are still back-projected into `agent_sessions`

Symptoms:

- task-mode turns still produce `agent_sessions` rows
- dashboards and readers must infer which rows are true live sessions versus audit shims
- session state and transcript/audit state are mixed in the same persistence surface

Likely causes:

- task mode still writes synthetic session-shaped rows for observability
- readers depend on runtime mode and scope-key conventions to recover semantics after the fact

Current mitigations:

- issue `#3` introduces a first-class audit persistence surface for task-mode conversation snapshots
- dashboard readers now label live-session versus turn-audit rows explicitly instead of inferring from `agent_sessions`

Still brittle because:

- the authoritative YAML platform spec still needs an explicit update to describe the split model
- older databases may still contain pre-split task rows until migrated forward

Improvement items:

- keep `agent_sessions` reserved for live leaseable session state only
- keep task-mode snapshots in the dedicated audit persistence surface
- remove remaining stale documentation/spec drift describing the mixed model

### Timer and accumulation-timeout identity still depends on string mini-languages

Symptoms:

- timer lifecycle keys use raw trigger strings like `state:...` and `event:...`
- timeout and accumulation bucket identity is assembled through concatenated strings
- bucket identity can be recovered heuristically when payload identity is incomplete

Likely causes:

- timer trigger identity is not modeled as a typed runtime object
- accumulation bucket references are still encoded and decoded by convention

Current mitigations:

- local parsing helpers
- “best bucket” recovery when direct identity is missing

Still brittle because:

- correctness depends on string format stability and collision-free naming conventions
- new timer modes increase the exception matrix rather than extending a typed model

Improvement items:

- introduce typed `TimerTrigger`, `TimerHandle`, and `AccumulatorBucketRef`
- stop recovering timer/bucket identity via concatenated string conventions
- make cancellation and timeout targeting consume canonical typed identity

### Conversation summaries are still reconstructed heuristically from raw payloads

Symptoms:

- dashboard summaries replay raw transcript payloads and infer assistant output from multiple payload shapes
- progress summaries depend on English prefix matching
- observability silently degrades when transcript-related columns are absent

Likely causes:

- the dashboard is reconstructing runtime truth instead of reading a canonical persisted turn outcome model
- transcript persistence still exposes raw material more reliably than explicit structured summary data

Current mitigations:

- completed at the 2026-04-05 checkpoint via issue `#31` / PR `#34`

Still brittle because:

- historical risk: debugging views could diverge from actual runtime behavior even when rows existed
- historical risk: the observability layer could become another heuristic interpreter

Improvement items:

- completed at the 2026-04-05 checkpoint via issue `#31` / PR `#34`:
  - persisted one canonical turn-summary read-model surface from the runtime
  - stopped reconstructing operator-facing summaries from raw transcript wire formats in the touched seams
  - made dashboard readers consumers of canonical persisted turn summaries rather than heuristic parsers

### Residual entity-id derivation is still split across namespaces

Symptoms:

- the same logical flow-instance ref can still produce different entity ids depending on which derivation path is used
- persistence/store helpers and centralized flow identity do not yet share one unconditional entity-id derivation path

Likely causes:

- store-side row-id derivation still has a residual local namespace branch
- centralized `flowidentity.EntityID(...)` is not yet the only authority for non-UUID flow-instance refs

Current mitigations:

- most major runtime identity seams now use shared flow identity
- tests cover canonical flow-instance identity at the runtime level

Still brittle because:

- one remaining split derivation path can silently break joins, lookups, deduplication, or cleanup logic
- this is exactly the same distributed-identity pattern we just spent time removing elsewhere

Improvement items:

- collapse residual store-side entity-id derivation onto `flowidentity.EntityID(...)` or one shared derivation helper
- remove alternate UUID namespaces for flow-instance-derived entity ids
- add direct tests proving all non-UUID flow-instance refs converge to the same entity id across runtime and store helpers

Status:

- completed at 2026-04-04 checkpoint via issue `#7` / PR `#9`

### Instance path resolution still has conflicting fallback precedence

Symptoms:

- different helpers can return different “canonical instance paths” for the same row if `flow_path` and `storage_ref` disagree
- identity-oriented callers and persistence-oriented callers still prioritize different fields first

Likely causes:

- path resolution remains split between pipeline identity helpers and store/persistence helpers
- the precedence order is still implicit and local to each function

Current mitigations:

- shared flow identity now covers many path/scope operations
- full-suite tests protect the main runtime semantics

Still brittle because:

- identity and persistence can silently disagree about which instance they are targeting
- divergent precedence reintroduces hidden semantics at the storage boundary

Improvement items:

- collapse instance-path resolution onto one canonical helper with explicit documented precedence
- return provenance or fail if persisted fields disagree in impossible ways
- remove path-resolution fallback ladders that can yield different answers for the same row

### Expression variable context still depends on silent overwrite precedence

Symptoms:

- expression variables are built by merging multiple maps with different overwrite rules
- collisions between `vars`, `policy`, `entity`, and `payload` can silently change expression meaning
- the effective precedence hierarchy is implicit rather than contract-visible

Likely causes:

- expression context is still modeled as a flat merged map
- different sources use different overwrite policies when added to the map

Current mitigations:

- namespaced access such as `entity.*` and `payload.*` exists in many places
- targeted fixes and tests cover some specific expression bugs

Still brittle because:

- unqualified names can silently resolve to the “winning” source instead of failing or requiring namespacing
- readers and contract authors cannot easily predict which source owns a colliding key

Improvement items:

- move toward explicit variable scopes:
  - `entity.*`
  - `payload.*`
  - `policy.*`
  - `vars.*`
- detect and warn on conflicting flat merges until namespacing is complete
- stop relying on undocumented overwrite precedence as expression semantics

### Tool execution telemetry is still effectively absent

Symptoms:

- tool execution already captures actor, tool name, latency, input, result, and error at call sites
- but runtime discards that data instead of emitting a canonical tool-execution record
- tool debugging still starts from indirect symptoms rather than direct execution evidence

Likely causes:

- the execution hook exists but remains a stub
- tool observability has not yet been treated as a first-class runtime surface

Current mitigations:

- partial turn/transcript evidence
- indirect logs and downstream workflow symptoms

Still brittle because:

- operators cannot easily distinguish:
  - tool never called
  - tool called and failed
  - tool succeeded but returned unexpected data
- this makes tool-heavy workflow failures much more expensive to investigate

Improvement items:

- implement canonical tool-execution telemetry immediately, even if first version is structured logging
- include:
  - actor
  - tool identity
  - latency
  - success/failure
  - normalized input/output summary
- wire this into the broader flight-recorder / observability initiative

### Semantic source access still relies on a package-level ambient lookup

Symptoms:

- multiple subsystems retrieve semantic identity/routing state through a package-level default accessor
- manager, workspace, and bus do not all share the same explicit injected semantic dependency

Likely causes:

- status: completed at the 2026-04-05 checkpoint via issue `#28` / PR `#33`
- historical risk: semantic-source ownership was ambient rather than constructor-injected
- some subsystems treat missing semantic source as “skip logic” while others keep going differently

Current mitigations:

- semantics appear effectively stable after boot in normal operation

Still brittle because:

- if semantic source mutability or reload ever changes, consumers can diverge in hard-to-see ways
- ambient access makes subsystem contracts less explicit than they should be

Improvement items:

- verify whether semantic source is truly immutable after boot
- if yes, still prefer explicit injection over package-level ambient access
- if not, treat this as a higher-severity architecture bug and version/inject the dependency explicitly

### Agent-to-entity scope still leaks through string suffix conventions

Symptoms:

- some routing/exclusion behavior still infers entity scoping from agent ids that end with `-{entityID}`
- routing semantics depend on id naming conventions rather than explicit metadata

Likely causes:

- entity scope is not yet represented as first-class subscription/registration metadata

Current mitigations:

- current id formats make accidental collisions unlikely

Still brittle because:

- changing id format or adding unexpected suffix collisions can silently alter routing behavior
- the actual scope rule is invisible at registration sites

Improvement items:

- introduce explicit entity-scope metadata on agent registration/subscription surfaces
- stop inferring scope from string suffix patterns

## Improvement Backlog

The flat backlog has been split into themed watchlists so this file can stay an index instead of the day-to-day working surface.

### Active Watchlists

- [Semantic Correctness](./watchlists/semantic-correctness.yaml)
  - spec drift, persistence/recovery truth, lifecycle proof, conformance, and fail-closed boundary semantics
- [Operator Surfaces](./watchlists/operator-surfaces.yaml)
  - dashboard, CLI, builder, API/read-models, observability, and canonical run-scoped query ownership
- [Runtime Operations](./watchlists/runtime-operations.yaml)
  - execution-boundary, security, concurrency, replay, shutdown, multi-runtime, and operational config work
- [Maintenance And Cleanup](./watchlists/maintenance-and-cleanup.yaml)
  - decomposition, dead code, conformance-fixture coverage, and legacy compatibility removal

### Current Active Wave

- `#115` Preserve full persisted correlation envelope during pipeline recovery replay.
- `#116` Make workflow instance mutation atomic across load, mutate, and upsert.
- `#118` Add a reusable cross-boundary delivery lifecycle conformance suite.
- `#119` Persist canonical terminal lifecycle for template flow instances.
- `#120` Persist canonical run completion instead of inferring it from quiescence.
- `#159` Legacy compatibility removal wave.

### Notes

- This file still owns the maintenance protocol, incident watchlist, recent root causes, and healthy-state definition.
- Each open backlog item should now have one canonical home in the themed watchlists above.
- Treat the themed watchlists as maintained failure-class maps, not passive issue lists.
- For failure-class / parity / semantic-drift work, issues should map to a watchlist node during intake or pre-audit.
- Review close-out should explicitly decide whether the relevant watchlist node needs no change, refinement, a new manifestation, or a new branch.
- Completed checkpoints remain visible through the issue tracker, merged PRs, and git history rather than an ever-growing flat list here.
## Known Recent Root Causes

### Scoped emit auth mismatch

Root cause:

- agent config stored scoped emit events
- executor auth compared them too literally against local emit tool names

Fix:

- canonical equivalence matching for emit event identities

### MCP allowlist alias mismatch

Root cause:

- gateway compared raw MCP-prefixed tool names against local allowlist names

Fix:

- normalize gateway tool name before allowlist check

### `run-clear` launcher orphaning runtime children

Root cause:

- Swarm was launched as a background child of the `make` shell and died when the script exited

Fix:

- detached launcher using `subprocess.Popen(..., start_new_session=True)`

### Stale runtime surviving DB reset

Root cause:

- reset script failed to kill the actual serving Swarm binary

Fix:

- aggressive process kill by port, PID, and binary patterns

### Over-broad parent retargeting rewrote top-level flow outputs

Root cause:

- generic “flow output targets parent entity” logic was correct for child-flow exits
- but it was also applied to top-level flow outputs like `scoring.requested`
- once flow-scoped lineage metadata existed on normal entities, this rewrote emitted `entity_id` to the root entity and poisoned downstream handler context

Fix:

- only apply parent retargeting when the current entity is actually in a flow-instance context (`flow_path` present)
- keep child-flow output behavior unchanged
- add regressions for:
  - child-flow output targeting parent entity
  - root-flow output staying on the local entity

### Flow scope and flow-instance path were treated as interchangeable

Root cause:

- flow-scoped semantics use keys like:
  - `child/g_validated`
- but some runtime paths filtered and projected using concrete instance paths like:
  - `child/<instance-id>`
- after canonical flow-entity ids were introduced for flow-scoped entities, this mismatch became visible:
  - child completion fired
  - child gate existed
  - root still did not advance because subject-gate projection looked under the wrong prefix
- the same family also affected root output-boundary handling, where root outputs were not treated consistently with flow outputs

Fix:

- project scoped child gates using the semantic flow scope key, not the concrete instance path
- treat root workflow outputs as legitimate output boundaries during boot verification
- add focused regressions for:
  - instanced child-flow gate projection
  - gated child-flow completion advancing the root
  - root output boundary behavior in boot fixtures

### Root terminal workflow rows were misclassified as flow-instance teardowns

Root cause:

- terminal-state deactivation logic loaded any workflow row by `entity_id`
- root workflow rows and flow-instance rows share the same store
- the old code then fell back from missing flow-instance metadata to generic `StorageRef`
- for root entities, that meant plain entity ids like `ent-001` were treated as if they were flow-instance paths
- manager teardown then tried to derive semantic scope from a non-flow path and killed otherwise-valid root handlers

Fix:

- only allow terminal flow-instance deactivation for explicit template flow instances
- require materialized flow-instance identity (`flow_path`, template-mode schema, logical instance id) before teardown
- root workflow rows no longer participate in flow-instance deactivation

### Nested template subscribers were materialized against semantic scope instead of concrete instance path

Root cause:

- template-flow subscriber patterns were pre-resolved once against semantic scope like:
  - `child/grandchild/...`
- when a concrete template instance was later materialized, the runtime reused those pre-resolved patterns unchanged
- instance-local subscriptions therefore stayed on the template namespace instead of the real instance namespace like:
  - `child/grandchild/<instance-id>/...`
- nested template instances could then exist correctly but still fail local delivery because their subscriptions were attached to the wrong path

Fix:

- keep template subscriber intent in raw form
- resolve subscriber patterns at `AddFlowInstance(...)` time against the exact concrete instance path
- persist and remove materialized instance routes using semantic scope key + logical instance id, not first-path-segment heuristics

### Cross-flow qualified event reached the flow but failed local handler lookup

Root cause:

- validation subscribed to `scoring/vertical.shortlisted`
- scoring emitted `scoring/vertical.shortlisted`
- but runtime handler lookup still searched with the fully qualified routed event name
- the local handler key in the target node was `vertical.shortlisted`
- runtime failed to translate:
  - external routed event name
  - local handler key

Fix:

- localize routed cross-flow event names against the target flow's declared input interface before handler lookup

### Flow-control event emitted without a valid target flow instance

Root cause:

- `lifecycle-coordinator` emitted `opco.teardown_requested` from a scoring marginal-review context
- the subject only had a scoring flow entity; there was no operating flow instance for that subject
- `lifecycle-orchestrator` then handled the event on the scoring entity id and attempted an operating-state transition there
- runtime correctly blocked the write with `cross_flow_write_forbidden`

Fix:

- not fixed at the runtime architecture level yet
- immediate operational conclusion:
  - this event is semantically wrong for that lifecycle phase
  - the emitter must not use `opco.teardown_requested` for a scoring-only subject

### Cross-flow validation handoff used mismatched source and target entity semantics

Root cause:

- validation flow instance `153e74e5-...` was created correctly with parent scoring entity `7da65fa3-...`
- `validation.started` used:
  - `entity_id = 153e74e5-...` for the validation entity
  - `vertical_id = 7da65fa3-...` for the source scoring entity
- validation prompts still instructed agents to treat payload `entity_id` as the source scoring entity
- agents then misread the handoff contract and made wrong assumptions about what they should read or write

Fix:

- not fixed yet in runtime/contract alignment
- required direction:
  - make source vs target entity identity explicit in cross-flow payloads
  - align prompts to the actual payload shape

### Validation agents were denied writes to their own validation entity

Root cause:

- validation entity `153e74e5-...` belonged to flow instance `153e74e5-...` with `flow_template = validation`
- both `business-research-agent` and `lightweight-spec-agent` attempted `save_entity_field` against that validation entity
- runtime returned `cross_flow_write_forbidden`
- this caused agents to emit progression events and fallback summaries without persisting:
  - `business_brief`
  - `mvp_spec`

Fix:

- not fixed yet
- required direction:
  - audit flow ownership resolution in `save_entity_field` / agent write authorization for cross-flow-triggered validation turns
  - add direct regression tests for same-flow writes after cross-flow handoff

### Milestone events were emitted without required persisted fields

Root cause:

- validation contracts require:
  - `business_brief` before `research.completed`
  - `mvp_spec` before `spec.draft_ready`
- in run `51b45b57-d82a-4d89-84c7-d0a3a7222fef`, agents emitted those events anyway after write failures
- downstream validation then looped on absent entity fields instead of stopping at the first invalid milestone emit

Fix:

- not fixed yet
- required direction:
  - strengthen prompt/runtime enforcement of save-before-emit invariants
  - detect and reject milestone emits when prerequisite entity fields are missing

### Agent sessions anchored on stale failure context and stopped acting

Root cause:

- after early write denials, later validation turns in the same sessions degraded into:
  - `No action. Standing by.`
  - `Operator: the fix is in event ... payload`
- tools were still available, but session memory had shifted from “try work” to “assume infrastructure is broken”
- this amplified the original write problem into a persistent loop with no further useful action

Fix:

- not fixed yet
- required direction:
  - make recoverable vs terminal infrastructure failures clearer
  - consider session reset/quarantine after certain failure classes

### Flow entity creation skipped declared schema defaults

Root cause:

- validation handlers correctly ran on the validation flow entity
- but the entity was created with empty `fields`
- package-level defaults such as:
  - `revision_count: integer default 0`
  - `brand_revision_count: integer default 0`
  were not materialized into the flow-scoped entity row
- later guards and accumulation expressions read `entity.revision_count` and failed with `no such key: revision_count`

Fix:

- pending runtime fix:
  - seed declared schema defaults during flow-entity creation
- until then, this class of issue should be recognized as an entity-initialization bug, not a generic CEL/nullability bug

### Mutation logging was absent despite visible state transitions

Root cause:

- on run `51b45b57-d82a-4d89-84c7-d0a3a7222fef`, entities changed state, including:
  - scoring entity `7da65fa3-...` reaching `shortlisted`
  - validation entity `153e74e5-...` reaching `mvp_speccing`
- `entity_mutations` still contained zero rows for the entire run
- this means state persistence and mutation logging are not continuously proven to be coupled

Fix:

- not fixed yet
- required direction:
  - audit all state-write paths
  - add conformance tests that fail when state changes occur without mutation rows

### `data_accumulation` arithmetic expressions did not mutate persisted state

Root cause:

- validation revision counters were initialized correctly and present in payloads as:
  - `revision_count: 0`
  - `brand_revision_count: 0`
- declarative handlers then used:
  - `expression: entity.revision_count + 1`
- runtime `data_accumulation` writes do support:
  - direct refs
  - literals
- but the execution path was narrower than contracts assumed and did not evaluate full arithmetic CEL there
- result:
  - the write did not increment the stored field
  - loop-brake counters stayed at `0`
  - `validation/spec.requested` kept cycling until the downstream agent exhausted its turn budget

Fix:

- pending runtime fix:
  - make `data_accumulation.writes[].expression` use the CEL-capable evaluation path expected by contracts
- also add focused tests proving:
  - `entity.revision_count + 1`
  mutates persisted state from `0 -> 1`

What should have caught it:

- a direct runtime test for arithmetic `data_accumulation` expressions
- flight-recorder output showing:
  - requested expression
  - evaluated value
  - final persisted field delta

### `on_complete` atomicity failure can masquerade as an eventing or accumulator bug

Root cause:

- state transition atomicity and downstream side effects can fail in a way that makes emitted work appear partially successful while the owning entity does not advance
- this can present as:
  - “emit succeeded but state stayed behind”
  - or “accumulator stalled at 10/11”
  even when the real failure is condition evaluation or rollback around `on_complete`

Fix:

- treat `on_complete` atomicity as a first-class invariant
- keep incident references linked to:
  - flight recorder
  - mutation-log completeness
  - accumulator / completion observability

## Definition Of Healthy State

We should consider this area healthy when:

- tool identity is canonical everywhere
- authorization cannot disagree between gateway and executor
- boot catches required capability gaps before ready
- run status and turn status explain launch failures directly
- recurring symptoms in this document map quickly to one narrow root-cause class
