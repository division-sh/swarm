# EmpireAI Spec Writer Agent Guide

This document contains everything a new spec writer agent needs to maintain and evolve the EmpireAI specification. It assumes no prior knowledge of the project or process.

---

## 1. What You Are Maintaining

EmpireAI is an autonomous AI holding company — a Go runtime that orchestrates 28 AI agents across a Factory (discovers and validates business verticals) and a Portfolio (operates companies via 13-agent OpCo teams). The system has 172 event types, 37 database tables, and dual LLM runtime (Claude API + CLI).

The specification is a three-layer system:

| Layer | Purpose | Audience |
|-------|---------|----------|
| **Prose spec** (`empireai-v2_0_XX.md`) | Explains WHY — design rationale, architecture, history | Humans understanding the system |
| **Contracts** (`contracts-v20XX/*.yaml`, `*.sql`) | Defines WHAT — machine-readable source of truth | Implementer, CI, verification tools |
| **Changelogs** (`CHANGELOG-v2.0.XX.md`) | Defines WHAT CHANGED — typed actions per version | Implementer's checklist |

**The #1 rule: Contracts are the source of truth. Prose explains why; contracts define what.**

When prose and contracts disagree, the contract wins. When the implementer builds from the spec, they read the contracts, not the prose. Prose errors are annoying; contract errors cause broken code.

The platform is separating into three architectural layers:

| Layer | Files | What it defines |
|-------|-------|----------------|
| **Platform** | workflow engine (Go) | Event routing, tool injection, state persistence, timer execution, schema validation |
| **Workflow** | workflow-schema.yaml, guard-action-registry.yaml | Stages, transitions, guards, actions for the vertical pipeline |
| **Policy** | prompt-variables.yaml, system-nodes.yaml scoring config | EmpireAI-specific thresholds, enum values, business rules |

---

## 2. The Contract Files (16 files)

### 2.1 agent-tools.yaml

Defines every agent's wiring: id, type, role, model_tier, conversation_mode, max_turns_per_task, subscriptions, tools, emit_events.

**Critical rules:**
- `agent_message`, `mailbox_send`, and `emit_*` tools are universal — injected into ALL agents. Never list them per-agent.
- `subscriptions` = static EventBus delivery (holding/factory + OpCo leadership — static only, no bootstrap)
- `subscriptions_bootstrap` = routing_rules table (OpCo worker agents ONLY — dynamic routing, REQUIRED)
- `emit_events` must include every event the agent can emit. Gate 3 enforces this for ALL emitter types including `opco_agent`.
- System nodes (like `scoring-node`) are also here with `node_type: system`

### 2.2 event-catalog.yaml

Defines every event: emitter, consumer, delivery_channel, payload with types. 172 events.

**Critical rules:**
- Every event in any agent's `emit_events` MUST have a catalog entry
- Every event in any agent's `subscriptions` MUST have a catalog entry
- Payload fields have types (string, integer, object, array) and many have `required` lists
- `consumer_type` values: `agent`, `system_component`, `dynamic` (runtime-resolved), `hybrid` (static + dynamic)
- `score.dimension_complete.score` has `minimum: 0, maximum: 100`

### 2.3 ddl-canonical.sql

Canonical database schema. 37 tables. `empire init` executes this directly. NEVER invent schemas — derive from Go structs (see Lesson 1).

### 2.4 system-nodes.yaml

System node definitions. Each node declares: subscribes_to, produces, owned_transitions (from workflow-schema.yaml), execution_type, implementation path, state table, idempotency table.

Current nodes: `scoring-node` (workflow_node), `pipeline system nodes` (runtime_interceptor).

Also contains: universal scoring rubric (11 dimensions), derivation pre-filter, analyst anti-bias assignment.

### 2.5 workflow-schema.yaml

Declarative state machine for the vertical pipeline. 18 stages, 27 transitions, 5 timers.

**Critical rules:**
- Every transition trigger event must exist in event-catalog.yaml
- Every guard/action ID must resolve in guard-action-registry.yaml
- Every transition node must exist in agent-tools.yaml or system-nodes.yaml
- Terminal stages have no outgoing transitions (except explicit overrides)
- **Transition triggers MUST match the actual runtime event names** (see Lesson 7)

### 2.6 guard-action-registry.yaml

Named guards (22) and actions (19) referenced by workflow-schema.yaml transitions. Each is categorized as `platform` (generic) or `empire` (business-specific). Empire guards reference prompt-variables.yaml via `policy_ref`.

### 2.7 tool-schemas.yaml

Input schemas for all 21 Tier 2 tools. MCP tool gateway generates tool definitions from this file. Enum values in schemas are the single source of truth — prompts must NOT repeat them.

### 2.8 prompt-variables.yaml

Single source of truth for all values appearing in multiple prompts. 44 variables. Prompts use `{{variable_name}}` syntax; runtime substitutes before sending to LLM. When changing any threshold or enum list, update HERE — not individual prompts.

### 2.9 prompts/ directory (20 files) + prompt-manifest.sha256

One markdown file per agent. Runtime loads directly.

**Critical rules:**
- Prompts tell agents WHEN and WHY to use tools. Tool definitions (from tool-schemas.yaml and EventSchemaRegistry) tell them HOW.
- Prompts must NOT describe payload structures inline — say "see tool schema"
- When updating prompts, regenerate `prompt-manifest.sha256`

### 2.10 upgrade-actions.yaml

Typed migration actions per version. Every implementation-impacting change needs an action with a `verify` command.

### 2.11 verification-gates.yaml

58 gates. `must_pass` gates block release. ~20 are automated via `TestContractCompliance`.

### 2.12 platform/platform-spec.yaml

MAS orchestration platform specification. Defines contract formats, vocabulary (6 primitives), compliance rules (16 rules), built-in hooks (5 guards + 5 actions), workflow state model, and file layout convention.

**Critical rules:**
- This is the schema for schemas — it defines the format that workflow-specific files must follow
- Platform builtins (has_entity_id, not_in_terminal_stage, etc.) are available to all workflows without declaration
- Implicit actions (record_transition, update_stage, timer management) run on every transition automatically
- When adding a new contract format to the platform: update this file FIRST

### 2.13 Other files

- `agent-config-map.yaml` — agent ID → config file path mapping
- `tooling.lock` — contract format version, required tooling versions
- `CHANGELOG-v2.0.XX.md` — per-version changelogs (8 exist as of v2.2.0)
- `spec-writer-guide.md` — this document

---

## 3. The Authority Hierarchy

When sources conflict:

1. **Implementer's runtime** — wins when the question is "what does the code actually do"
2. **contracts/*.yaml and contracts/*.sql** — wins over prose for "what should the code do"
3. **Prose spec** — explains why, defers to contracts for what

**The implementer builds to the spec, not the other way around.** The spec defines event names, payload structures, transition triggers. The implementer implements them.

**Exception — retroactive formalization:** When the spec is documenting behavior that already exists in the runtime (e.g. workflow-schema.yaml formalizing an existing pipeline), the contract must match reality. In this case, ask the implementer what the runtime uses. This is a one-time catch-up, not the ongoing process.

**Within contracts:**
- `ddl-canonical.sql` > any inline DDL in prose
- `agent-tools.yaml` > any agent tables in prose
- `event-catalog.yaml` > any event tables in prose
- `system-nodes.yaml` > any system node descriptions in prose
- `workflow-schema.yaml` > any stage/transition descriptions in prose

---

## 4. The Version Bump Process

**Every batch of spec changes must include a version bump and a changelog entry. This is a hard requirement.**

### Step 1: Edit contracts FIRST

If your changes affect structured data (events, agent wiring, DDL, thresholds, enums), edit the contract YAML/SQL file first. Then update prose to match.

### Step 2: Write the changelog

Create `CHANGELOG-v2.0.XX.md`. The Touches block must list every contract file — either changed or "no change needed."

### Step 3: Write upgrade-actions

Add typed actions to `upgrade-actions.yaml`. Each action needs a `verify` command.

### Step 4: Add verification gates if needed

New invariants → new gates in `verification-gates.yaml`.

### Step 5: New File Checklist

**When adding a new contract file, you MUST complete ALL of these:**
- [ ] YAML parse gate entry in verification-gates.yaml (add to parse gate file list)
- [ ] Cross-validation gate (if the file cross-references other contracts)
- [ ] Prose section in spec (e.g. §5.9 for workflow architecture)
- [ ] §16 directory listing updated
- [ ] §17.1 contract file description added
- [ ] spec-writer-guide section added (§2.X)
- [ ] Archive template in Step 8 updated
- [ ] File locations in §10 updated

This checklist exists because every release from v2.0.47 to v2.0.50 added contract files without updating all documentation surfaces. The auditor found this pattern in every single review.

### Step 6: Update version stamps

Files that need version bumps every release:
- All contract YAML file headers
- `upgrade-actions.yaml` → `previous_version` field (must be exactly N-1)
- `tooling.lock` → `contract_format_version`
- `verification-gates.yaml` → `spec_version` on new gates
- `version-field-consistency` gate → hardcoded version string

This list exists because version stamps were wrong in v2.0.45, v2.0.48, v2.0.49, AND v2.0.50.

### Step 7: Run cross-validation

Before finalizing, verify:
- Every `emit_events` entry has a catalog entry
- Every `subscriptions` entry has a catalog entry
- Every catalog emitter exists as an agent or system component
- Every workflow transition trigger exists in event-catalog.yaml
- Every workflow guard/action resolves in guard-action-registry.yaml
- All `{{variable}}` tokens in prompts resolve in prompt-variables.yaml
- All version stamps are bumped

### Step 8: Package the archive

```
empireai-v2_0_XX/
├── empireai-v2_0_XX.md
└── contracts-v20XX/
    ├── agent-tools.yaml
    ├── event-catalog.yaml
    ├── ddl-canonical.sql
    ├── system-nodes.yaml
    ├── workflow-schema.yaml
    ├── guard-action-registry.yaml
    ├── tool-schemas.yaml
    ├── prompt-variables.yaml
    ├── upgrade-actions.yaml
    ├── verification-gates.yaml
    ├── agent-config-map.yaml
    ├── prompt-manifest.sha256
    ├── tooling.lock
    ├── spec-writer-guide.md
    ├── CHANGELOG-v2.0.XX.md (all versions)
    ├── platform/
    │   └── platform-spec.yaml
    └── prompts/             (20 files)
```

---

## 5. Critical Lessons (Learned the Hard Way)

### Lesson 1: NEVER Invent Schemas — Derive from Go Structs

In v2.0.33, the spec writer designed DDL from scratch instead of reading Go struct definitions. Every column name was wrong.

**Rule:** Find the Go struct first. Translate field-by-field. Never add columns the Go code doesn't reference.

### Lesson 2: Ghost Removal Requires Grep-Level Thoroughness

The Scoring Coordinator was removed in v2.0.19. It survived in ASCII diagrams and agent tables through SIX review cycles.

**Rule:** Grep the entire spec. Remove all references atomically.

### Lesson 3: Prose Duplicating Contract Data Will Drift

Every audit finds "prose says X, contract says Y." Remove structured data from prose. Replace with pointers to contracts.

### Lesson 4: The Event Catalog Is Built from Agent Configs, Not Prose

Start from agent emit_events + subscriptions → then check prose. Agent configs are closer to ground truth.

### Lesson 5: Empty `subscriptions_bootstrap` Is Almost Always Wrong

OpCo workers with `subscriptions_bootstrap: []` but real subscriptions in config YAML are a common error.

### Lesson 6: Glob Patterns Don't Work

The EventBus does not support `opco.*.escalation`. Every subscription must be a literal event name.

### Lesson 7: The Spec Defines Event Names — Except When Retroactively Documenting

**Normal flow:** The spec defines a new event name → the implementer builds it. The spec is the authority on naming. If the runtime uses a different name, the runtime is wrong.

**Exception:** When retroactively formalizing behavior that already exists in the runtime (like workflow-schema.yaml in v2.0.50), the contract must match the runtime. You cannot invent event names for transitions that are already built — you must discover what the runtime actually uses.

In v2.0.50, the spec writer built workflow-schema.yaml top-down from prose. The prose said "spec review passes then CTO reviews" so the writer invented `spec_review.passed` as a transition trigger. **That event never existed in the runtime.** The actual event is `spec.approved` (emitted by the BRA). This class of bug — **spec-led fiction** — wasted an entire audit cycle.

**Rules:**
- For NEW features: spec defines the event name. Implementer builds to match.
- For EXISTING behavior being formalized: ask the implementer what the runtime uses. Grep `case "..."` in the Go code.
- Never guess. If you're unsure whether something is new or existing, ask.

### Lesson 8: Prompts Describe WHEN/WHY, Tool Schemas Describe HOW

When a prompt describes payload structure ("derivation_rationale: 1-2 sentences"), it competes with the tool schema and causes shape mismatches. The agent sent a string for a field the schema defined as an object.

**Rule:** Prompts say "see tool schema." The MCP tool gateway serves the schema. Never repeat structure in prompts.

### Lesson 9: Every New Contract File Needs the Full Checklist

From v2.0.47 to v2.0.50, every release added contract files without updating all documentation surfaces. The auditor found it every time. Use Step 5.

### Lesson 10: Version Stamps Drift Every Release

Wrong in v2.0.45, v2.0.48, v2.0.49, v2.0.50. Use the checklist in Step 6. Push for CI automation.

---

## 6. The Codebase (What You Need to Know)

| File | What It Contains | Contract It Maps To |
|------|-----------------|-------------------|
| `internal/commgraph/registry.go` | `agentProducerEvents` map | agent-tools.yaml `emit_events` |
| `internal/runtime/event_emit_tools.go` | `EventSchemaRegistry` | event-catalog.yaml `payload` |
| `internal/runtime/pipeline/coordinator.go` | Pipeline coordinator — stage transitions | system-nodes.yaml pipeline system nodes |
| `internal/runtime/pipeline/coordinator_validation.go` | Validation flow handlers | workflow-schema.yaml validation transitions |
| `internal/runtime/pipeline/scoring_node.go` | ScoringNode | system-nodes.yaml scoring-node |
| `internal/runtime/manager.go` | AgentManager — OpCo roster, routes | agent-tools.yaml |
| `configs/agents/*.yaml` | Agent config files | agent-tools.yaml + agent-config-map.yaml |
| `configs/agents/templates/routes.yaml` | Bootstrap routing rules | agent-tools.yaml `subscriptions_bootstrap` |

### Key: The BRA orchestrates the validation middle

The BRA (business-research-agent) is more than a research agent. It drives the validation pipeline:
1. PC emits `validation.started` → BRA subscribes
2. BRA does research, emits `research.completed`
3. BRA emits `spec.requested` → lightweight-spec-agent
4. LSA emits `spec.draft_ready` → BRA
5. BRA emits `spec.approved` → PC does stage projection to cto_spec_review
6. PC emits `cto.spec_review_requested` → factory-cto
7. CTO emits `cto.spec_approved` or `cto.spec_revision_needed` or `cto.spec_vetoed`

**The workflow-schema.yaml transition triggers must match these exact events.** This is why Lesson 7 exists.

---

## 7. The Review Process

```
You write spec → Auditor checks contracts → You fix findings → Repeat
```

Three reviewers may audit:
- **Claude auditor** — contract consistency, cross-validation, prose alignment
- **Gemini reviewer** — naming mismatches, coverage gaps, structural issues
- **Implementer** — the ultimate authority on what the runtime actually does

**When the implementer says the runtime doesn't match the spec:** determine whether the spec is defining new behavior (implementer should update runtime) or retroactively documenting existing behavior (spec should update to match). This distinction matters.

---

## 8. Common Mistakes and Prevention

| Mistake | Prevention |
|---------|-----------|
| Updating prose but not contracts | Edit contracts FIRST |
| Inventing DDL from imagination | Derive from Go structs (Lesson 1) |
| Inventing event names from prose | For new events: spec defines the name. For existing behavior: ask implementer (Lesson 7) |
| Adding contract file without docs | New File Checklist (Step 5) |
| Version stamps not bumped | Version Stamp Checklist (Step 6) |
| Prompts describing payload structures | Say "see tool schema" (Lesson 8) |
| Hardcoded thresholds in prompts | Use `{{variable}}` from prompt-variables.yaml |
| Orphaned events in catalog | Gate 3 checks all emitter types including opco_agent |
| Missing handler for subscription | Gate 8 checks subscription-handler coverage |
| OpCo leadership using bootstrap | Leadership uses static subscriptions ONLY |

---

## 9. Quick Reference: Adding Common Things

### Adding a new event
1. Add to `event-catalog.yaml` with all fields including `required` list
2. Add to emitter's `emit_events` in `agent-tools.yaml`
3. Add to consumer's `subscriptions` or `subscriptions_bootstrap`
4. If it triggers a workflow transition, add to `workflow-schema.yaml`
5. Add upgrade action, note in changelog

### Adding a workflow transition
1. Add transition to `workflow-schema.yaml` — get trigger event name from implementer
2. Add any new guards/actions to `guard-action-registry.yaml`
3. Verify trigger event exists in `event-catalog.yaml`
4. Verify node exists in `agent-tools.yaml` or `system-nodes.yaml`

### Changing scoring thresholds
1. Edit `prompt-variables.yaml` — single source of truth
2. Prompts using `{{variable}}` auto-update at runtime
3. If system-nodes.yaml has the same value, update there too

---

## 10. File Locations

```
contracts/                             # In-repo live copy (implementer maintains)
  agent-tools.yaml                    # 28 agents
  event-catalog.yaml                  # 172 events
  ddl-canonical.sql                   # 37 tables
  system-nodes.yaml                   # 5 system nodes (scoring, scan, discovery, validation, lifecycle)
  workflow-schema.yaml                # 18 stages, 27 transitions
  guard-action-registry.yaml          # 22 guards, 13 actions
  tool-schemas.yaml                   # 21 Tier 2 tool schemas
  prompt-variables.yaml               # 44 template variables
  upgrade-actions.yaml                # migration actions per version
  verification-gates.yaml             # 58 gates
  agent-config-map.yaml               # agent → config path
  prompt-manifest.sha256              # hash manifest for 20 prompts
  tooling.lock                        # format version, tooling reqs
  spec-writer-guide.md                # this document
  CHANGELOG-v2.0.XX.md               # 10 changelogs (v2.0.43–v2.2.0)
  platform/                           # Platform specification
    platform-spec.yaml                # MAS orchestration platform spec
  prompts/                            # 20 agent prompt files
```

---

## 11. Current State (as of v2.2.0)

**What's working:**
- Full corpus discovery pipeline: signals → pre-filter → scoring → derivation loop → EC digest
- SubDoc scored 70.75 — first vertical to clear all hard gates, pending human approval in mailbox
- Two derived verticals (PayGate signal 73, Lite signal 61) proposed by derivation loop
- 20 agent prompts with template variables, all `{{}}` tokens resolve
- 57 verification gates, ~20 automated
- Workflow state machine formalized (18 stages, 27 transitions matching actual runtime)

**Known gaps (deferred, not blocking):**
- 9 OpCo worker agent prompts missing — fix before OpCo spinup
- Migration 026 for 4 DDL indexes — implementer task
- EventSchemaRegistry not yet generated from event-catalog.yaml
- Prompt-schema guard test not yet built

**Pending implementer tasks:**
- Wire MCP tool gateway to read tool-schemas.yaml
- Wire prompt template renderer (`{{variable}}` substitution)
- Create `empireai-current.md` symlink
- Build Gate 8 (subscription-handler coverage)
- Remove `opco_agent` from Gate 3 skip list
- Generate emit tool schemas from event-catalog.yaml (eliminates dual source of truth)
