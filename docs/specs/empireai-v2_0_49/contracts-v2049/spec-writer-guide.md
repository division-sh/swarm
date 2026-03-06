# EmpireAI Spec Writer Agent Guide

This document contains everything a new spec writer agent needs to maintain and evolve the EmpireAI specification. It assumes no prior knowledge of the project or process.

---

## 1. What You Are Maintaining

EmpireAI is an autonomous AI holding company — a Go runtime that orchestrates 28 AI agents across a Factory (discovers and validates business verticals) and a Portfolio (operates companies via 13-agent OpCo teams). The system has 160+ event types, 37 database tables, and dual LLM runtime (Claude API + CLI).

The specification is a three-layer system:

| Layer | Purpose | Audience |
|-------|---------|----------|
| **Prose spec** (`empireai-v2_0_XX.md`) | Explains WHY — design rationale, architecture, history | Humans understanding the system |
| **Contracts** (`contracts-v20XX/*.yaml`, `*.sql`) | Defines WHAT — machine-readable source of truth | Implementer, CI, verification tools |
| **Changelogs** (`CHANGELOG-v2.0.XX.md`) | Defines WHAT CHANGED — typed actions per version | Implementer's checklist |

**The #1 rule: Contracts are the source of truth. Prose explains why; contracts define what.**

When prose and contracts disagree, the contract wins. When the implementer builds from the spec, they read the contracts, not the prose. Prose errors are annoying; contract errors cause broken code.

---

## 2. The Contract Files

### 2.1 agent-tools.yaml (~700 lines)

Defines every agent's wiring. Per agent:

```yaml
empire-coordinator:
  id: empire-coordinator
  type: holding                    # holding | factory | operating
  role: empire_coordinator
  model_tier: sonnet               # sonnet | haiku | haiku_or_sonnet
  conversation_mode: session       # session | session_per_vertical | task | stateless
  max_turns_per_task: 40
  subscriptions:                   # Static EventBus subscriptions (holding/factory agents)
    - vertical.shortlisted
    - vertical.approved
    # ...
  subscriptions_bootstrap: []      # Routes via routing_rules table (OpCo workers only)
  tools_tier2:                     # Domain-specific tools (agent_message and emit_* are universal, never list them here)
    - mailbox_send
    - schedule
    - human_task_decide
  emit_events:                     # Events this agent may emit
    - board.directive
    - vertical.resumed
    # ...
```

System nodes (like `scoring-node`) are also in this file with `node_type: system`.

**Critical rules:**
- `agent_message` and `emit_*` tools are universal — injected into ALL agents. Never list them per-agent.
- `subscriptions` = static EventBus delivery (holding/factory agents and OpCo leadership)
- `subscriptions_bootstrap` = routing_rules table (OpCo worker agents only)
- `emit_events` must match `agentProducerEvents` in `internal/commgraph/registry.go`

### 2.2 event-catalog.yaml (~2100 lines)

Defines every event in the system. Per event:

```yaml
category.assessed:
  emitter: market-research-agent
  consumer: [scoring-node]
  consumer_type: system_component
  intercepted: false
  passthrough: true
  routing: static
  delivery_channel: eventbus_static    # eventbus_static | eventbus_routing_table | runtime | agent_message | mailbox | audit
  payload:
    - scan_id
    - category
    - subcategory
    - geography
    - signal_strength
    - opportunity_name
    # ...
```

**Critical rules:**
- Every event in any agent's `emit_events` MUST have a catalog entry
- Every event in any agent's `subscriptions` MUST have a catalog entry
- Every event must have a `delivery_channel` — no exceptions
- `intercepted: true` means PipelineCoordinator consumes the event before/instead of delivering to subscribers
- `runtime_handling: projection` means runtime does work on the event but still delivers it (not intercepted)
- Payload fields must match the Go `EventSchemaRegistry` properties

### 2.3 ddl-canonical.sql (~793 lines)

Canonical database schema. 37 CREATE TABLE statements with indexes and constraints.

```sql
CREATE TABLE verticals (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT NOT NULL,
    geography       TEXT NOT NULL DEFAULT 'global',
    -- ...
);
CREATE UNIQUE INDEX idx_verticals_slug_geo ON verticals(slug, geography);
```

**Critical rules:**
- This file IS the schema. `empire init` executes it directly.
- If prose disagrees, this file wins.
- NEVER invent schemas. Derive from Go structs. See Lesson 1 below.
- Column names must be EXACT — `g2_spec` is not `g2_spec_approved`. Go's `db.Scan()` reads by name.
- `agents.id` is TEXT, not UUID. Any FK referencing agents must use TEXT.

### 2.4 system-nodes.yaml (~400 lines)

Defines deterministic Go system nodes that participate in EventBus alongside agents. Currently only `scoring-node`.

```yaml
scoring-node:
  subscribes_to: [vertical.discovered, vertical.derived, score.dimension_complete, scoring.contest_resolved]
  produces: [scoring.requested, vertical.scored, vertical.shortlisted, vertical.marginal, vertical.rejected, scoring.contested, pipeline.dead_letter]
  state_table: scoring_digest_buffer
  implementation: internal/runtime/scoring_node.go
```

Contains the universal scoring rubric definition (11 dimensions, thresholds, rejection cascade).

### 2.5 upgrade-actions.yaml (~700 lines)

Typed migration actions per version. Per action:

```yaml
- id: v2044-category-assessed-new-fields
  type: code                       # add | edit | drop | rename | migrate | grep_kill | code | test | prompt | contract
  priority: must                   # must | should | optional
  target: internal/runtime/event_emit_tools.go
  description: "Add opportunity_pattern, signal_sources, required_capabilities to category.assessed schema"
  verify: "go test ./internal/runtime/ -run TestCategoryAssessedSchema"
```

**Note:** The file has two structural formats due to historical evolution. Versions v2.0.35-41 use YAML list items (`- id: v2037-...`). Versions v2.0.42+ use top-level map keys (`v2042-mailbox-type-enum:`). Both are functionally equivalent but the file does not parse as valid YAML due to pre-existing quoting issues in grep verify strings. Treat as a human-readable checklist, not a machine-parseable contract. If reformatting, standardize on list-item syntax to match verification-gates.yaml.

### 2.6 verification-gates.yaml (~730 lines)

Test gate manifest. Per gate:

```yaml
- id: wiring-verifier-clean
  name: "Wiring verifier passes cleanly"
  priority: must_pass              # must_pass | should_pass | informational
  category: wiring                 # ddl | wiring | events | agents | integration
  command: "go test ./internal/runtime/ -run TestSpecRuntimeWiringVerification -v"
  pass_criteria: "fail=0, warn=0"
  introduced: "2.0.36"
```

**Gate priorities:**
- `must_pass` — Failure = broken system. Blocks release.
- `should_pass` — Expected green. Failure = tracked debt.
- `informational` — Never blocks. Tracks trends.

**Automation (v2.0.44):** Each gate has an `automated` field. If non-null, it names the Go test subfunction that implements the gate (e.g., `TestContractCompliance/agent_config`). Currently 17 of 50 gates are automated across 6 test functions in `internal/runtime/contract_compliance_test.go`. These run with `go test ./internal/runtime/ -run TestContractCompliance` and enforce contract-code consistency in CI. Manual gates (`automated: null`) are checked during audits.

| Test Function | Gates Covered | What It Catches |
|--------------|---------------|-----------------|
| `agent_config` | 2 | model_tier, max_turns, conversation_mode, tools mismatch |
| `subscriptions` | 3 | Subscription wiring drift (config, bootstrap, roster) |
| `emit_events` | 3 | CommGraph emit_events ↔ contract mismatch |
| `schema_payload` | 4 | EventSchemaRegistry missing catalog payload fields |
| `ddl_tables` | 4 | Table count, column names, new DDL columns |
| `version_constants` | 1 | Stale runtimeSpecVersion or TemplateVersion |

### 2.7 agent-config-map.yaml (~112 lines)

Maps contract agent IDs to config file paths. Authoritative for filename resolution.

```yaml
agents:
  empire-coordinator: configs/agents/empire-coordinator.yaml
  opco-ceo: configs/agents/templates/opco-ceo.yaml
  opco-head-of-product: configs/agents/templates/vp-product.yaml   # Note: filename differs from ID
  scanner-agent: null   # ephemeral, no config file
```

### 2.8 prompts/ directory (19 files) + prompt-variables.yaml

Canonical system prompts. One markdown file per agent. Runtime loads directly from these files.

- `{agent-id}.md` — default prompt
- `{agent-id}.{mode}.md` — mode variant (e.g. `market-research-agent.corpus.md`)
- `prompt-manifest.sha256` — hash manifest for verification
- `prompt-variables.yaml` — single source of truth for all shared values (thresholds, enum lists, capability tiers). Prompts use `{{variable_name}}` syntax; runtime substitutes before sending to LLM.

**Critical rules:**
- Every agent in agent-tools.yaml must have a corresponding prompt file
- Prompt must reference every event in the agent's `emit_events` list
- Prompt must not describe payload structures inline — defer to tool schema ("see tool schema")
- When updating prompts, regenerate `prompt-manifest.sha256`
- When changing any threshold, enum list, or capability, update `prompt-variables.yaml` — not individual prompts
- `TestContractCompliance/agent_prompts` verifies hash parity
- `TestPromptVariablesComplete` verifies all `{{}}` tokens resolve

### 2.9 tool-schemas.yaml

Input schemas for all Tier 2 tools. The MCP tool gateway generates tool definitions from this file. 21 tools across 2 universal + 19 per-agent.

**Critical rules:**
- Every tool in any agent's `tools_tier2` list must have a schema here
- Schemas follow JSON Schema draft 2020-12 with `additionalProperties: false`
- Enum values in schemas are the single source of truth — prompts must NOT repeat them
- `TestContractCompliance/tool_schemas` verifies every tools_tier2 entry has a matching schema
- When adding a new tool: add schema here FIRST, then add to agent's tools_tier2 list

### 2.10 CHANGELOG-v2.0.XX.md

Standalone changelog per version. Required format:

```markdown
# v2.0.XX — Title

## Summary
What changed and why.

## Spec Changes
- Bullet points of what was modified

## Implementation Actions
- Typed actions (ADD/EDIT/DROP/MIGRATE/VERIFY)

## Touches
- event-catalog.yaml: reason for change
- agent-tools.yaml: no change needed (wiring unchanged)
- ddl-canonical.sql: ADD 3 columns to verticals
- system-nodes.yaml: reason for change
- upgrade-actions.yaml: 10 new actions
- verification-gates.yaml: 14 new gates
```

**Every contract file must appear in the Touches block** — either with a change reason or "no change needed" with explanation.

---

## 3. The Authority Hierarchy

When sources conflict, this is the resolution order:

1. **contracts/*.yaml and contracts/*.sql** — always wins
2. **§4 Runtime Architecture prose** — authoritative for design rationale
3. **§5 Communication Model prose** — was historically authoritative for events, now defers to event-catalog.yaml
4. **§3 Actor Hierarchy prose** — overview level, defers to agent-tools.yaml for specifics
5. **§6/§8 Data Model prose** — defers to ddl-canonical.sql for schema

**Within contracts:**
- `ddl-canonical.sql` > any inline DDL in prose
- `agent-tools.yaml` > any agent tables in prose
- `event-catalog.yaml` > any event tables in prose
- `system-nodes.yaml` > any system node descriptions in prose

---

## 4. The Version Bump Process

This is the mechanical process for every spec revision. Never skip steps.

### Step 1: Edit contracts FIRST

If your changes affect structured data (events, agent wiring, DDL, thresholds, enums), edit the contract YAML/SQL file first. Then update prose to match. Not the other way around.

### Step 2: Write the changelog

Create `CHANGELOG-v2.0.XX.md` with all three sections: Summary, Implementation Actions, Touches. The Touches block must list every contract file.

### Step 3: Write upgrade-actions

Add typed actions to `upgrade-actions.yaml` for every implementation-impacting change. Each action needs a `verify` command.

### Step 4: Add verification gates if needed

If the change introduces new invariants (new table, new event, new system node behavior), add gates to `verification-gates.yaml`.

### Step 5: Run cross-validation

Before finalizing, verify:
- Every `emit_events` entry has a catalog entry
- Every `subscriptions` entry has a catalog entry
- Every catalog emitter exists as an agent or system component
- Every event has `delivery_channel`
- All payload fields in catalog match EventSchemaRegistry
- All numeric thresholds match between prose and contracts
- All enum values match between prose and contracts
- All version stamps in contract headers are bumped

### Step 6: Update version stamps

Every contract file header must show the current spec version.

### Step 7: Build the archive

```
empireai-v2_0_XX/
├── empireai-v2_0_XX.md
├── CHANGELOG-v2.0.XX.md
└── contracts-v20XX/
    ├── agent-tools.yaml
    ├── event-catalog.yaml
    ├── ddl-canonical.sql
    ├── system-nodes.yaml
    ├── upgrade-actions.yaml
    ├── verification-gates.yaml
    ├── agent-config-map.yaml
    ├── prompt-manifest.sha256
    ├── prompt-variables.yaml
    ├── tool-schemas.yaml
    ├── tooling.lock
    └── prompts/
        ├── empire-coordinator.md
        ├── market-research-agent.md
        ├── market-research-agent.corpus.md
        ├── analysis-agent.md
        └── ... (19 prompt files total)
```

Package as `empireai-v2_0_XX.tar`. Spec filename uses underscores (some tools choke on dots).

---

## 5. Critical Lessons (Learned the Hard Way)

### Lesson 1: NEVER Invent Schemas — Derive from Go Structs

In v2.0.33, the spec writer was asked to add 7 runtime tables to the DDL. Instead of reading the Go struct definitions, they designed idealized schemas from scratch. Every column name was wrong. Every query in the Go code would have failed.

**When creating or modifying DDL:**
1. Find the Go struct: `grep "type TableName struct" internal/**/*.go`
2. Translate field-by-field: Go `bool` → `BOOLEAN`, `json.RawMessage` → `JSONB`, `uuid.UUID` → `UUID`, `string` → `TEXT`
3. Check the code that reads/writes the table — the actual SQL queries tell you the real columns
4. Never add columns the Go code doesn't reference

### Lesson 2: Ghost Removal Requires Grep-Level Thoroughness

The Scoring Coordinator was removed in v2.0.19. It survived in ASCII diagrams, directory trees, and agent tables through SIX review cycles until v2.0.34.

**When removing anything (agent, event, table, field):**
1. Grep the entire spec for every reference
2. Classify as active (must remove) or historical (preserve in changelog)
3. Remove all active references in one atomic version
4. Add a VERIFY step proving zero remaining references

### Lesson 3: Prose Duplicating Contract Data Will Drift

The spec historically contained:
- Full EventSchemaRegistry with JSON Schema (~500 lines) — duplicated event-catalog.yaml AND Go code
- Event tables with payload columns (~800 lines) — duplicated event-catalog.yaml
- Inline DDL (~400 lines) — duplicated ddl-canonical.sql
- Scoring threshold tables (~150 lines) — duplicated system-nodes.yaml

Every audit found "prose says X, contract says Y." The fix: **remove structured data from prose, replace with pointers to contracts.** Prose should say "See contracts/event-catalog.yaml for complete event definitions" — not repeat the data.

### Lesson 4: The Event Catalog Is Built from Agent Configs, Not Prose

When building the event catalog, start from:
1. Every agent's `emit_events` → those events must exist
2. Every agent's `subscriptions` → those events must exist
3. PipelineCoordinator's interceptor cases → those events must exist
4. Runtime's deferred emissions → those events must exist
5. THEN check prose for anything missed

Agent configs are closer to ground truth than prose tables.

### Lesson 5: Empty `subscriptions_bootstrap` Is Almost Always Wrong

OpCo worker agents that show `subscriptions_bootstrap: []` in the contract but have real subscriptions in their config YAML files are a common error. Cross-reference `configs/agents/templates/routes.yaml` when populating bootstrap subscriptions.

### Lesson 6: Glob Patterns Don't Work

The EventBus does not support `opco.*.escalation` or similar glob patterns. Every subscription must be a literal event name. If you write a wildcard, you're writing a bug.

---

## 6. The Codebase (What You Need to Know)

You don't write code, but you need to understand what these files contain to write accurate contracts.

| File | What It Contains | Contract It Maps To |
|------|-----------------|-------------------|
| `internal/commgraph/registry.go` | `agentProducerEvents` map — which agents emit which events | agent-tools.yaml `emit_events` |
| `internal/runtime/event_emit_tools.go` | `EventSchemaRegistry` — JSON Schema per event | event-catalog.yaml `payload` |
| `internal/runtime/pipeline_coordinator.go` | `interceptPolicy()` — which events are intercepted | event-catalog.yaml `intercepted` |
| `internal/runtime/manager.go` | `defaultOpCoRoster()` and `defaultOpCoRoutes()` — hardcoded OpCo agents and routes | agent-tools.yaml subscriptions + subscriptions_bootstrap |
| `internal/runtime/scoring_node.go` | ScoringNode subscriptions and emissions | system-nodes.yaml |
| `configs/agents/*.yaml` | Agent config files (model_tier, subscriptions, tools, constraints, system_prompt) | agent-tools.yaml + agent-config-map.yaml |
| `configs/agents/templates/routes.yaml` | Bootstrap and seeded routing rules | agent-tools.yaml `subscriptions_bootstrap` |
| `migrations/*.sql` | Incremental database migrations | ddl-canonical.sql |
| `contracts/` | In-repo copy of contract files | Your spec contracts |

### Key Runtime Constants
- `runtimeSpecVersion` in manager.go — must match your spec version
- `TemplateVersion` in `defaultOpCoRoster()` — must match your spec version

---

## 7. The Review Process

After you produce a spec revision, it goes through this cycle:

```
You write spec → Reviewer audits contracts against code → You fix findings → Repeat until clean
```

**What the reviewer checks:**

*Note: Items 1-7 below are now partially automated by `TestContractCompliance` (§2.6). The reviewer focuses on what the automated tests don't cover: interceptor policy, ghost events, design rationale, and cross-contract semantic consistency.*
1. Every agent config field matches agent-tools.yaml (model_tier, subscriptions, tools, max_turns, conversation_mode)
2. CommGraph registry matches agent-tools.yaml emit_events
3. EventSchemaRegistry payload fields match event-catalog.yaml
4. DDL matches ddl-canonical.sql
5. Pipeline coordinator intercept policy matches event-catalog.yaml
6. Manager roster/routes match agent-tools.yaml
7. Version constants match spec version
8. No ghost events or agents in code
9. No glob subscription patterns

**What makes the reviewer's life easier:**
- Fix ALL findings in one version, not incrementally
- Add VERIFY steps for each fix
- Never silently skip a finding — mark it DEFERRED with a reason if you can't fix it
- The reviewer's time is expensive. Making them find the same issue three times erodes trust.

---

## 8. Common Mistakes and How to Avoid Them

| Mistake | How It Manifests | Prevention |
|---------|-----------------|------------|
| Updating prose but not contracts | "Prose says X, contract says Y" in every audit | Edit contracts FIRST, then prose |
| Inventing DDL from imagination | Every Go query fails against your schema | Always derive from Go structs |
| Forgetting `delivery_channel` on new events | Wiring verifier flags it | Template every new event from an existing one |
| Payload fields in prose but not catalog | Implementer builds wrong schema | Touches block forces explicit acknowledgment |
| Version stamps not bumped | Stale metadata confuses tooling | Last step of every version bump |
| `additionalProperties: false` in schemas | Future field additions rejected at runtime | Keep `false` as deliberate strict-schema policy, but ensure every contract payload field is registered in Go `EventSchemaRegistry`. The `TestContractCompliance/schema_payload` gate catches mismatches. |
| Naming mismatches (`prebrand` vs `pre-brand`) | Config files not found | Always check agent-config-map.yaml |
| Empty `subscriptions_bootstrap` for workers | Worker agents never receive events | Cross-reference routes.yaml |
| Scoring thresholds differ between files | Marginal/shortlist boundaries wrong | Single source in system-nodes.yaml, prose points to it |
| Changelog without Touches block | Contract drift goes undetected | Template enforces the block |

---

## 9. Quick Reference: Adding Common Things

### Adding a new event
1. Add entry to `event-catalog.yaml` with all fields (emitter, consumer, intercepted, passthrough, routing, delivery_channel, payload)
2. Add to emitter agent's `emit_events` in `agent-tools.yaml`
3. If consumed via subscription, add to consumer agent's `subscriptions` or `subscriptions_bootstrap`
4. Add upgrade action to `upgrade-actions.yaml`
5. Update prose if it has an event summary table
6. Note in changelog Touches block

### Adding a new agent
1. Add full entry to `agent-tools.yaml`
2. Add file path mapping to `agent-config-map.yaml`
3. Add to event-catalog.yaml as emitter/consumer where applicable
4. Add upgrade actions for config file creation and CommGraph registration
5. Add verification gate for agent config compliance

### Modifying DDL
1. Edit `ddl-canonical.sql` — this is the canonical source
2. Add a `migrate` type action to `upgrade-actions.yaml` with the ALTER/CREATE SQL
3. Note the migration number the implementer should create
4. If adding columns, verify the Go struct has corresponding fields
5. If adding CHECK constraints, list the valid enum values

### Changing scoring thresholds or rubric
1. Edit `system-nodes.yaml` — this is the single source of truth
2. Remove or update any threshold values in prose that duplicated the old values
3. Prose should point to system-nodes.yaml, not repeat the numbers

---

## 10. File Locations

```
docs/specs/empireai-v2_0_XX/           # Spec archive per version
  empireai-v2_0_XX.md                  # Main prose spec
  contracts-v20XX/                     # Contract files for this version
    agent-tools.yaml
    event-catalog.yaml
    ddl-canonical.sql
    system-nodes.yaml
    upgrade-actions.yaml
    verification-gates.yaml
    agent-config-map.yaml
    prompt-variables.yaml
    tool-schemas.yaml
    tooling.lock
    prompt-manifest.sha256
    CHANGELOG-v2.0.XX.md
    prompts/                           # 20 agent prompt files
      empire-coordinator.md
      market-research-agent.md
      market-research-agent.corpus.md
      analysis-agent.md
      # ... (20 prompt files total)

contracts/                              # In-repo live copy (implementer maintains)
  agent-tools.yaml
  event-catalog.yaml
  ddl-canonical.sql
  system-nodes.yaml
  upgrade-actions.yaml
  verification-gates.yaml
  prompt-variables.yaml
  tool-schemas.yaml
  prompts/                             # Runtime loads prompts from here

configs/agents/                         # Agent config files
  empire-coordinator.yaml
  factory-cto.yaml
  # ... (16 holding/factory agents)
  templates/
    opco-ceo.yaml
    chief-of-staff.yaml
    vp-product.yaml
    # ... (13 OpCo agents)
    routes.yaml                        # Bootstrap + seeded routing rules

docs/spec-template.md                  # Reusable spec template for other projects
docs/spec-writer-guide.md              # This document
```
