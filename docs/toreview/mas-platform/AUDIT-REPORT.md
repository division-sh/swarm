# MAS Platform Specification — Comprehensive Audit Report

**Date**: 2026-03-10
**Scope**: 90 files across `platform/`, `empire/`, `docs/` (652KB)
**Spec Version**: Platform v1.1.0 / Empire v3.0.1
**Auditor**: Claude Opus 4.6

---

## Executive Summary

The MAS Platform specification is **architecturally ambitious and structurally well-organized** — a declarative multi-agent orchestration platform with flow composition, event-driven state machines, and a clean separation between platform engine and application (empire) layers. The contract-first approach (YAML + prose) is sound.

However, the audit uncovered **significant issues across all layers**:

| Severity | Platform | Empire Contracts | Flow Contracts | Changelogs | **Total** |
|----------|----------|-----------------|----------------|------------|-----------|
| CRITICAL | 1 | 2 | 3 | 4 | **10** |
| HIGH | 4 | 7 | 8 | 6 | **25** |
| MEDIUM | 13 | 13 | 15 | 8 | **49** |
| LOW | 10 | 8 | — | 5 | **23** |
| **Total** | **28** | **30** | **26** | **23** | **107** |

**Top systemic risks:**
1. **Operating flow is dangerously underspecified** — 8/13 agent prompts are TODO stubs, no build pipeline system node, critically sparse policies
2. **Count instability** — handler counts (5 different values), event counts (4 conflicting totals), and compliance checks (3 different totals) across changelogs
3. **Cross-flow handoffs are implicit** — no formal contracts between Discovery→Scoring→Validation→Operating
4. **Security gaps** — no external event authentication, no secrets management, no data classification
5. **Broken event chains** — multiple events with no producer or no consumer

---

## Part 1: Platform Core Audit

### CRITICAL (1)

**P-C1: No authentication/authorization for external event sources**
`platform-spec.yaml` lines 997-998: Event sources include "external API" and "human input" but no authentication mechanism is specified. Any caller that can reach the event endpoint can inject arbitrary events — including state transitions, agent actions, or flow instance creation. No API keys, JWT, mTLS, or allowlists specified anywhere.

### HIGH (4)

**P-H1: Vocabulary `flow.required_files` contradicts 3 other locations**
`platform-spec.yaml` lines 91-94 says `nodes.yaml` and `events.yaml` are required. But `flow_tree_walker` (line 797-808) says only `package.yaml` and `schema.yaml` are required. `flow_package.files` (line 440-448) marks both as optional. CHANGELOG line 66-76 marks both as optional. An implementer reading the vocabulary first would enforce wrong file requirements at boot.

**P-H2: Two separate permissions lists with 13 combined unique entries**
`platform-spec.yaml` `platform_permissions` (lines 337-345) lists 10 permissions. A separate `permissions` field (lines 352-355) lists 4 more (`create_flow_instance`, `schedule`, `human_task_decide`, `human_task_request`). Only `human_task_request` appears in both. No documentation explains their relationship.

**P-H3: Circular event chains have no depth guard**
`platform-spec.yaml` lines 1022-1023: Events are processed in separate cycles (preventing stack recursion) but nothing prevents logical infinite loops (A emits X → B emits Y → A emits X → ...). No max-depth, cycle detection, or emission budget specified. Error model (lines 1195-1215) covers handler failures but not runaway chains.

**P-H4: No observability specification**
Zero mention of metrics, tracing, structured logging, or health checks across all platform files. No event throughput metrics, handler latency histograms, entity lock contention tracking, agent token cost monitoring, or dead letter queue depth visibility.

### MEDIUM (13)

| ID | Finding | Location |
|----|---------|----------|
| P-M1 | `package.yaml` missing from vocabulary `required_files` — the most important file omitted | `platform-spec.yaml:91-94` |
| P-M2 | `agents.yaml` optional in `flow_package` but `required_agents` validation expects it | `platform-spec.yaml:443, 963` |
| P-M3 | Stale handoff compliance rule references removed "declarative handoff" concept | `platform-spec.yaml:298`, CHANGELOG:22-23 |
| P-M4 | `on_complete` interaction with `advances_to`/`emits` underspecified — two interpretations possible | `platform-spec.yaml:761-762, 772` |
| P-M5 | `rules` vs `advances_to`/`emits` mutual exclusivity not enforced at boot | `platform-spec.yaml:776` |
| P-M6 | `clear_gates` not in 10-step handler execution order — timing unknown | `platform-spec.yaml:1394-1397` |
| P-M7 | `payload_transform` not in execution order — must execute before `emits` but not stated | `platform-spec.yaml:1385-1393` |
| P-M8 | Fail-open prompt templating — typos in `{{variables}}` silently produce broken prompts | `platform-spec.yaml:428` |
| P-M9 | `agent_reconfigure` permission allows prompt injection, tool escalation, subscription hijacking | `platform-spec.yaml:341` |
| P-M10 | `configure_routing` allows runtime event hijacking of workflow state machine | `platform-spec.yaml:340` |
| P-M11 | No secrets management — API keys, credentials stored in plain YAML | `platform-spec.yaml:426-432` |
| P-M12 | Per-entity serialization bottlenecks hot entities (50 fan-out results queued sequentially) | `platform-spec.yaml:1029` |
| P-M13 | Event store unbounded growth — no retention policy, compaction, or archival | `platform-spec.yaml:1005, 1026` |

### LOW (10)

P-L1: Prose spec missing coverage for timer model, CEL, error model, conversation modes, subscription types, emit tool convention (all in YAML only). P-L2: Dynamic instance cleanup deferred — unbounded terminated state accumulation. P-L3: Compliance handler execution order differs from normative 10-step order. P-L4: Obsolete "project" terminology in `project_coherence` compliance rules. P-L5: `fan_out.side_effect` writes to "entity state" in one location, "handler context" in another. P-L6: `from` filter not in execution order. P-L7: Template `instance_id` uniqueness scope unclear. P-L8: `session_per_entity` contradicts "short-lived, no persistent memory" statement. P-L9: Dead letter events contain full payloads — sensitive data leakage risk. P-L10: No horizontal scaling path designed (acknowledged as future concern).

### POSITIVE

- No TBDs, TODOs, or placeholder content anywhere in platform files
- All agent-to-system communication goes through validated channels — no backdoor access
- YAML faithfully encodes prose for all core concepts

---

## Part 2: Empire Contracts Audit

### CRITICAL (2)

**E-C1: `query_entities` tool referenced by operations-analyst but completely undefined**
`agents.yaml` operations-analyst tool list includes `query_entities`. No definition exists in `tools.yaml` or `runtime/tools.yaml`. The agent cannot function without it.

**E-C2: `deploy.completed` vs `devops.deploy_complete` naming mismatch leaves opco-qa deaf to deploys**
`agents.yaml` opco-qa subscribes to `deploy.completed`. The opco-devops agent emits `devops.deploy_complete`. Different event names — QA never receives deploy notifications, breaking the build→QA→deploy pipeline.

### HIGH (7)

| ID | Finding | Location |
|----|---------|----------|
| E-H1 | 25 of 28 agents have no prompt files (only empire-coordinator, holding-devops, operations-analyst do) | `contracts/prompts/` |
| E-H2 | `composite_shortlist` threshold: 72 in events, 75 in policy | `events.yaml` vs `policy.yaml` |
| E-H3 | `pipeline_capacity_max`: 50 in root policy, 3 in operating policy | `policy.yaml` vs `operating/policy.yaml` |
| E-H4 | `contest_spread_threshold`: 10 in root policy, 30 in scoring policy | `policy.yaml` vs `scoring/policy.yaml` |
| E-H5 | Runtime policy has 20+ keys absent from top-level policy | `runtime/policy.yaml` vs `policy.yaml` |
| E-H6 | 7+ events emitted with no subscriber anywhere (e.g., `cto.pattern_detected`, `opco.spend_request`, `cycle_reset`) | `events.yaml`, `agents.yaml` |
| E-H7 | `scan.requested` and `vertical.resumed` payloads differ between top-level and runtime events | `events.yaml` vs `runtime/events.yaml` |

### MEDIUM (13)

E-M1: Event count discrepancy between PRODUCT-BRIEF/CHANGELOG (179 vs 181). E-M2: schema.yaml misleading name (contains entity schema, not validation schemas). E-M3: 8 runtime-only tools not in top-level tools.yaml. E-M4: `spend.approved.amount` typed as string instead of number. E-M5: Mixed integer/number types across schemas. E-M6: Weak inner schemas on object/array payloads. E-M7: Unnamespaced operating events pollute global event space. E-M8: opco-marketing has worker permissions but holds tools requiring higher privilege. E-M9: opco-support has worker permissions but holds tools requiring higher privilege. E-M10: `emit_events` constraints not enforced at runtime. E-M11: Timer cadences undefined. E-M12: Operations-analyst prompt lacks per-event processing rules. E-M13: opco-devops conversation mode mismatch between agents.yaml and prompt.

---

## Part 3: Flow Contracts Audit

### CRITICAL (3)

**F-C1: `scan.requested` — the entry point to the entire pipeline — has no documented producer**
`discovery/events.yaml` line 1 defines it. `discovery/schema.yaml` line 5 lists it as an input pin. No node or agent in any flow emits it. The system's entry event is implicit.

**F-C2: Operating flow has no system node for build/deploy pipeline enforcement**
13 agents, 1 system node (lifecycle-orchestrator handling only top-level state transitions). No deterministic enforcement of build ordering (spec→tech spec→build→QA→deploy), deploy gate checks, or escalation chains. All other flows have system nodes providing this safety net. Operating relies entirely on agent self-coordination.

**F-C3: 8 of 13 operating agent prompts are TODO stubs**
`opco-cto.md` (49 lines), `opco-pm.md` (35 lines), `opco-tech-writer.md` (35 lines), `opco-backend.md` (35 lines), `opco-frontend.md` (35 lines), `opco-qa.md` (30 lines), `opco-devops.md` (35 lines), `opco-support.md` (40 lines) — all contain `<!-- TODO -->` markers with no operational guidance. The entire execution layer is unspecified.

### HIGH (8)

| ID | Finding | Location |
|----|---------|----------|
| F-H1 | `scoring.contested` → `scoring.contest_resolved` chain broken — contests raised but never resolved | `scoring/nodes.yaml:17`, `scoring/events.yaml:70-80` |
| F-H2 | `opco.teardown_complete` has no producer — teardown can never complete | `operating/nodes.yaml:111`, `operating/agents.yaml:29-40` |
| F-H3 | `vertical.discovered` produced only by system_node — agent-only runtime readers miss it | `discovery/nodes.yaml:114-115` |
| F-H4 | Discovery→Scoring has no cross-flow payload schema contract | `discovery/events.yaml:256-267` vs `scoring/nodes.yaml:20-58` |
| F-H5 | Validation kills don't back-propagate to scoring entity state — killed verticals stay "shortlisted" | `scoring/schema.yaml:28`, `validation/nodes.yaml:108-115` |
| F-H6 | `schedule` tool used by 8+ agents but undefined in operating/tools.yaml | `operating/agents.yaml` |
| F-H7 | opco-cto prompt wrong reporting line (says CEO, agents.yaml says head-of-product) | `operating/prompts/opco-cto.md:4`, `operating/agents.yaml:201` |
| F-H8 | operating/policy.yaml critically sparse — only 4 parameters, missing build timeouts, budget defaults, escalation thresholds, steady-state criteria | `operating/policy.yaml` |

### MEDIUM (15)

| ID | Finding | Location |
|----|---------|----------|
| F-M1 | scanner-agent schema.yaml omits typed `scan_assigned` variants | `discovery/schema.yaml:44` |
| F-M2 | Internal routing events in global events.yaml (should be scoped) | `discovery/events.yaml:337-361` |
| F-M3 | `source.scraped` payload too thin for signal accumulation | `discovery/events.yaml:231-236` |
| F-M4 | `vertical.derived` field: `parent_id` vs `parent_vertical_id` mismatch | `scoring/events.yaml:3` vs `scoring/prompts/analysis-agent.md:135` |
| F-M5 | discovery-coordinator prompt payload mismatches `dedup.resolved` schema | `discovery/prompts/discovery-coordinator.md:22` |
| F-M6 | scanner-agent prompt references fields not in `source.scraped` | `discovery/prompts/scanner-agent.md` |
| F-M7 | Discovery policy.yaml lacks retry/escalation policies | `discovery/policy.yaml` |
| F-M8 | `validation.timeout_escalation` consumed externally but undocumented | `validation/events.yaml:147-156` |
| F-M9 | factory-cto prompt hardcodes Paraguay-specific references | `validation/prompts/factory-cto.md:31` |
| F-M10 | `validation.package_ready` payload types are `string`, should be `object` | `validation/events.yaml:90-96` |
| F-M11 | `spend.approved`/`spend.rejected` consumed by opco-ceo but undefined | `operating/agents.yaml:17-18` |
| F-M12 | `opco.terminated` produced but missing from output pins | `operating/schema.yaml:23-27` |
| F-M13 | 4 tools (`dns_configure`, `domain_availability_check`, etc.) referenced but undefined | `operating/agents.yaml:314-319` |
| F-M14 | opco-tech-writer reports-to contradicts agents.yaml | `operating/prompts/opco-tech-writer.md:3` |
| F-M15 | `launched`→`operating` transition has no guard criteria | `operating/nodes.yaml:82-84` |

### Cross-Flow Handoff Map

| Source | Event | Target | Status |
|--------|-------|--------|--------|
| External | `scan.requested` | Discovery | **BROKEN** — no producer documented |
| Discovery | `vertical.discovered` | Scoring | **Working** — no schema contract |
| Scoring | `vertical.shortlisted` | Validation | **Working** |
| Scoring | `vertical.marginal` | Operating (timers) | **Unusual** — cross-flow timer ownership |
| Validation | `vertical.ready_for_review` | Mailbox (human) | **Working** — no timeout |
| Mailbox | `mailbox.item_decided` | Operating | **Working** |
| Validation | `vertical.killed` | Scoring | **MISSING** — no back-propagation |
| Operating | `opco.terminated` | Empire-coordinator | **MISSING** from output pins |

---

## Part 4: Changelog Evolution Audit

### Evolution Arc

18 versions in 10 days (2026-02-28 → 2026-03-09), tracking a dramatic architectural transformation:

| Phase | Versions | Theme |
|-------|----------|-------|
| 1 | v2.0.43–v2.0.46 | Feature development + audit hardening |
| 2 | v2.0.47–v2.0.49 | Contract discipline (templates, schema discipline) |
| 3 | v2.0.50–v2.2.0 | Architecture decomposition (monolith → 5 system nodes) |
| 4 | v2.2.1–v2.2.2 | Coherence + reorganization (platform/empire split) |
| 5 | v2.3.0–v2.4.0 | Radical simplification + Flow model (15 files → 8) |
| 6 | v2.5.0–v2.6.0 | Platform extraction (100% declarative, 0 product hooks) |

### CRITICAL (4)

**CL-C1: Handler count instability — 5 different values across 4 versions**
v2.2.0: 30→47 (amended in-place). v2.4.0: 53→51→47. v2.5.0: 47. v2.6.0: 51. Empire v3.0.0: 52→51. The foundational metric is self-contradictory within single changelogs.

**CL-C2: Event count instability — 4 conflicting totals**
v2.4.0: 175 (109+66). v2.6.0: 181. Empire v3.0.0: 179. Empire v3.0.1: 181 (58+123). Root events halved from 109 to 58 without any changelog explaining which ~50 events moved or were removed.

**CL-C3: v2.2.0 changelog retroactively contains v2.2.1 content**
Sections titled "Contract Coherence Fix" and "Scoring policy fix" describe changes documented identically in v2.2.1. Either the changelog was retroactively edited (violating immutability) or v2.2.1 is a duplicate release.

**CL-C4: Breaking changes not consistently marked**
v2.3.0 eliminates 9 contract files + renames all remaining files — no "Breaking Change" section. v2.4.0 introduces the Flow model (entirely new directory structure) — no breaking change marker. v2.6.0 eliminates the monolithic spec file — no marker.

### HIGH (6)

| ID | Finding |
|----|---------|
| CL-H1 | Vertical-to-opportunity rename deferred in v2.0.43, never delivered — 3 naming conventions now coexist |
| CL-H2 | 9 OpCo agent prompts missing since v2.0.46 — never addressed across 18 versions |
| CL-H3 | Eliminated contracts (verification-gates, upgrade-actions, DDL) may still be needed — no migration guide |
| CL-H4 | Platform version jumps v1.0→v1.1 same day with substantial additions |
| CL-H5 | Semver violations: v2.0.43–v2.0.50 introduce breaking changes as patch versions |
| CL-H6 | Runtime compatibility layer added as afterthought — spec evolved ahead of implementation |

### MEDIUM (8)

CL-M1: Signal threshold changed 3 times without clear rationale chain. CL-M2: v2.0.44 spec slimming removed 4,600 lines — no verification mechanism for pointer drift. CL-M3: Same-day version bumps (5 releases on 2026-03-09) suggest insufficient stabilization. CL-M4: v2.4.0 contains duplicate sections (copy-paste artifact). CL-M5: Campaign `time_cap` noted as unverified, never resolved. CL-M6: Compliance check count instability (44 vs 40 vs 45). CL-M7: Agent count discrepancy in v2.6.0 flow table (13 agents, 4 prompts). CL-M8: Terminology churn creates cognitive overhead (7 renames across versions).

---

## Part 5: Prioritized Action Plan

### Tier 1 — Blockers (Fix Before Any OpCo Spinup)

| # | Action | Fixes | Effort |
|---|--------|-------|--------|
| 1 | **Write 8 operating agent prompts** (CTO, PM, Tech Writer, Backend, Frontend, QA, DevOps, Support) | F-C3, CL-H2 | Large |
| 2 | **Add build-pipeline system node** to operating flow with deterministic spec→build→QA→deploy enforcement | F-C2 | Large |
| 3 | **Fix `deploy.completed`/`devops.deploy_complete` event name mismatch** | E-C2 | Small |
| 4 | **Define `query_entities` tool** or remove from operations-analyst | E-C1 | Small |
| 5 | **Define `schedule` tool** referenced by 8+ operating agents | F-H6 | Small |
| 6 | **Add producer for `opco.teardown_complete`** (assign to opco-ceo or opco-devops) | F-H2 | Small |
| 7 | **Document `scan.requested` producer** — the pipeline entry point | F-C1 | Small |

### Tier 2 — Structural Integrity (Fix Before Production)

| # | Action | Fixes | Effort |
|---|--------|-------|--------|
| 8 | **Add external event authentication** (API key/JWT/mTLS for event ingestion) | P-C1 | Medium |
| 9 | **Add event chain depth limit** (max 50 chained emissions → dead letter) | P-H3 | Small |
| 10 | **Fix vocabulary `required_files`** to match `flow_tree_walker` (`package.yaml`, `schema.yaml` only) | P-H1, P-M1 | Small |
| 11 | **Reconcile permissions lists** — merge 10+4 into one authoritative list of 13 | P-H2 | Small |
| 12 | **Add `clear_gates`, `payload_transform`, `from` to handler execution order** | P-M6, P-M7 | Small |
| 13 | **Reconcile policy value discrepancies** (`composite_shortlist`, `pipeline_capacity_max`, `contest_spread_threshold`) | E-H2–H5 | Medium |
| 14 | **Add cross-flow handoff contracts** with shared payload schemas | F-H4, F-H5 | Medium |
| 15 | **Fix scoring contest chain** — assign `scoring.contest_resolved` producer | F-H1 | Small |
| 16 | **Add validation kill → scoring back-propagation** | F-H5 | Medium |

### Tier 3 — Hardening (Fix Before Scale)

| # | Action | Fixes | Effort |
|---|--------|-------|--------|
| 17 | Add observability specification (metrics, tracing, health checks) | P-H4 | Medium |
| 18 | Add secrets management specification | P-M11 | Medium |
| 19 | Add data classification and encryption-at-rest policy | P-H4 (related) | Medium |
| 20 | Add event store retention/compaction policy | P-M13 | Small |
| 21 | Expand operating/policy.yaml (build timeouts, budgets, escalation thresholds) | F-H8 | Medium |
| 22 | Define 4 missing operating tools (`dns_configure`, `domain_availability_check`, etc.) | F-M13 | Small |
| 23 | Fix reporting line inconsistencies (opco-cto, opco-tech-writer) | F-H7, F-M14 | Small |

### Tier 4 — Hygiene (Fix When Convenient)

| # | Action | Fixes |
|---|--------|-------|
| 24 | Establish authoritative handler/event/check counts in one source-of-truth file | CL-C1, CL-C2, CL-M6 |
| 25 | Mark all breaking changes retroactively in changelogs | CL-C4 |
| 26 | Remove stale "project" terminology and handoff compliance rules | P-L4, P-M3 |
| 27 | Fix field name mismatches (`parent_id`/`parent_vertical_id`, prompt payload structures) | F-M4, F-M5, F-M6 |
| 28 | Templatize geography-specific content in factory-cto prompt | F-M9 |
| 29 | Remove `subscriptions_bootstrap` deprecated field entirely (no production consumers yet) | P-L10 (related) |

---

## Appendix: Strengths

Despite the issues above, this spec has notable strengths:

1. **Zero TBDs/TODOs in platform layer** — fully specified where it's specified
2. **Clean agent isolation** — no backdoor communication channels, all interaction through validated events
3. **YAML authoritatively encodes prose** — clear precedence rule prevents ambiguity
4. **Pin model (input/output pins per flow)** — elegant composition primitive
5. **10-step handler execution order** — deterministic, auditable execution semantics
6. **Discovery, scoring, and validation flows are well-specified** — complete state machines, good prompts, proper system nodes
7. **Changelogs are individually detailed** — each version documents rationale, not just changes
8. **Flow composition model (IC chip analogy)** — powerful abstraction for multi-flow systems
