# EmpireAI Architecture v0.4-2 End-to-End Gap Addendum

Scope: Additional gaps found from an end-to-end walk (factory -> approval -> spinup -> build -> launch -> operate -> recover/kill), excluding items already listed in `empireai-architecture-v0.4-2-feedback.md`.

Reviewed on: 2026-02-11

## Additional Findings

### 1) Recovery query does not match OpCo routing model
- Severity: Critical
- Issue: OpCo delivery is routing-table based, but crash recovery pulls by static event subscriptions (`e.type = ANY($1)`).
- Impact: Routed OpCo events can be missed after restart.
- Recommendation: Recovery must reconstruct pending deliveries from routing rules (or persist resolved recipients at publish-time).
- References: `docs/specs/empireai-architecture-v0.4-2.md:727`, `docs/specs/empireai-architecture-v0.4-2.md:733`

### 2) Failed events are treated as processed in recovery
- Severity: Critical
- Issue: Recovery checks `NOT EXISTS receipt`, but receipts include `status='error'`.
- Impact: Events that failed once may never be retried.
- Recommendation: Recover by status (`processed` only counts as done), or maintain retry policy + dead-letter state.
- References: `docs/specs/empireai-architecture-v0.4-2.md:733`, `docs/specs/empireai-architecture-v0.4-2.md:735`, `docs/specs/empireai-architecture-v0.4-2.md:2251`

### 3) New agent replay boundary is undefined
- Severity: High
- Issue: Replay query has no lower bound by agent start/subscription time.
- Impact: Newly hired agents may receive large irrelevant historical backlog.
- Recommendation: Store subscription effective timestamps and replay only from activation point.
- References: `docs/specs/empireai-architecture-v0.4-2.md:732`, `docs/specs/empireai-architecture-v0.4-2.md:733`, `docs/specs/empireai-architecture-v0.4-2.md:2273`

### 4) 50-75 scoring branch is specified but not operationalized
- Severity: High
- Issue: Scoring defines a “deeper analysis” path for 50-75, but event catalog only models shortlist/reject outcomes.
- Impact: Mid-band candidates have no deterministic workflow.
- Recommendation: Add explicit events/stages/owner for deeper-analysis loop.
- References: `docs/specs/empireai-architecture-v0.4-2.md:394`, `docs/specs/empireai-architecture-v0.4-2.md:399`, `docs/specs/empireai-architecture-v0.4-2.md:1057`, `docs/specs/empireai-architecture-v0.4-2.md:1058`

### 5) `more-data` decision path is not fully defined
- Severity: High
- Issue: Mailbox supports `more-data`, but pipeline flow only continues from approve -> spinup.
- Impact: Humans can request more data without a defined return loop and SLA.
- Recommendation: Define the request/response event cycle and stage transitions for `vertical.needs_more_data`.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1104`, `docs/specs/empireai-architecture-v0.4-2.md:1499`, `docs/specs/empireai-architecture-v0.4-2.md:1501`

### 6) Agent lifecycle events model CEO-only emitters despite delegated manager authority
- Severity: High
- Issue: VP/manager hire-fire authority is defined, but lifecycle events list only OpCo CEO as emitter.
- Impact: Event provenance and authorization audits become inaccurate.
- Recommendation: Model emitter as any authorized manager and enforce parent-chain checks in payload validation.
- References: `docs/specs/empireai-architecture-v0.4-2.md:777`, `docs/specs/empireai-architecture-v0.4-2.md:783`, `docs/specs/empireai-architecture-v0.4-2.md:1144`, `docs/specs/empireai-architecture-v0.4-2.md:1146`

### 7) Spinup routing baseline is inconsistent in different sections
- Severity: High
- Issue: One section installs bootstrap+seeded at spinup, another says bootstrap-only.
- Impact: Different implementations will produce different day-1 behavior.
- Recommendation: Publish one canonical spinup routing set and remove conflicting text.
- References: `docs/specs/empireai-architecture-v0.4-2.md:770`, `docs/specs/empireai-architecture-v0.4-2.md:1228`, `docs/specs/empireai-architecture-v0.4-2.md:1827`

### 8) Port allocation timing is inconsistent
- Severity: Medium
- Issue: Mandate includes pre-assigned port, but DevOps flow requests allocation at first deploy.
- Impact: Conflicting control points for infra provisioning.
- Recommendation: Decide whether port is allocated at approval/spinup or at first deploy; keep one source of truth.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1815`, `docs/specs/empireai-architecture-v0.4-2.md:3958`, `docs/specs/empireai-architecture-v0.4-2.md:4180`

### 9) Deployment-failure actor mismatch
- Severity: Medium
- Issue: Failure flow says CTO rolls back and redeploys; deployment role says DevOps executes deploy operations.
- Impact: Recovery playbooks and tooling permissions are unclear.
- Recommendation: CTO decides and coordinates; DevOps executes rollback/redeploy.
- References: `docs/specs/empireai-architecture-v0.4-2.md:2755`, `docs/specs/empireai-architecture-v0.4-2.md:2758`, `docs/specs/empireai-architecture-v0.4-2.md:3952`

### 10) Stage lifecycle is documented as comments, not enforceable contract
- Severity: Medium
- Issue: `verticals.stage` is free text with comment-only progression.
- Impact: Invalid jumps and inconsistent reporting/state recovery.
- Recommendation: Add enum + transition guard rules (or explicit state-machine table).
- References: `docs/specs/empireai-architecture-v0.4-2.md:2195`, `docs/specs/empireai-architecture-v0.4-2.md:2197`, `docs/specs/empireai-architecture-v0.4-2.md:2200`

### 11) Inbound webhook dedup/idempotency not defined
- Severity: Medium
- Issue: Authenticity is checked, but duplicate webhook replay handling is unspecified.
- Impact: Duplicate customer messages/events can trigger repeated actions.
- Recommendation: Persist provider event IDs with replay window and idempotent publish semantics.
- References: `docs/specs/empireai-architecture-v0.4-2.md:990`, `docs/specs/empireai-architecture-v0.4-2.md:993`

### 12) Tenant isolation controls are implicit, not enforced at tool-executor layer
- Severity: High
- Issue: One-schema-per-vertical is defined, but runtime tool execution is transparent and backend agents have raw `sql_execute`.
- Impact: Cross-vertical data access becomes possible by prompt drift/tool misuse.
- Recommendation: Enforce vertical-scoped DB/session context in tool wrappers (hard policy, not prompt-only).
- References: `docs/specs/empireai-architecture-v0.4-2.md:824`, `docs/specs/empireai-architecture-v0.4-2.md:832`, `docs/specs/empireai-architecture-v0.4-2.md:3857`, `docs/specs/empireai-architecture-v0.4-2.md:4181`

### 13) Governance precedence gap: board directives vs spend-control process
- Severity: Medium
- Issue: Board directives are highest authority, but spend still requires mailbox approval in policy and agent rules.
- Impact: Agents may interpret direct commands as bypassing spend controls.
- Recommendation: Add explicit precedence rule: directives can reprioritize work, but cannot bypass spend approval workflow.
- References: `docs/specs/empireai-architecture-v0.4-2.md:145`, `docs/specs/empireai-architecture-v0.4-2.md:2629`, `docs/specs/empireai-architecture-v0.4-2.md:3997`

### 14) Budget throttle behavior is undefined
- Severity: Medium
- Issue: Portfolio config says “reduce agent activity at 90%”, but there is no policy for what gets paused/degraded first.
- Impact: Non-deterministic budget protection and possible business-critical throttling.
- Recommendation: Define deterministic degrade order (critical ops first, growth experiments later, etc.).
- References: `docs/specs/empireai-architecture-v0.4-2.md:2524`

### 15) PII/compliance remains unresolved while handling live customer conversations
- Severity: High
- Issue: Support processes WhatsApp/email customer messages, while privacy handling is left as an open question.
- Impact: Legal/compliance risk in production deployment.
- Recommendation: Define data-handling policy before launch: allowed data classes, redaction, retention, model processing boundaries.
- References: `docs/specs/empireai-architecture-v0.4-2.md:4045`, `docs/specs/empireai-architecture-v0.4-2.md:3117`



GOVERNANCE OPCO

  Missing links/channels

  1. No explicit VP-to-VP operating channel. This is still an open question, which means coordination latency between product and growth is unresolved (docs/specs/empireai-architecture-v0.4-2.md:3111).
  2. Launch coordination is treated as “discovered” instead of guaranteed, so first-launch ops can stall while teams “learn” coordination (docs/specs/empireai-architecture-v0.4-2.md:583, docs/specs/empireai-architecture-v0.4-2.md:1895).
  3. No formal cross-OpCo “what worked” channel (CEO-to-CEO learning is open). This slows portfolio compounding (docs/specs/empireai-architecture-v0.4-2.md:3113).
  4. Churn loop is not fully closed operationally. Churn is detected/diagnosed, but there is no required loop that forces remediation back into product + growth playbooks (docs/specs/empireai-architecture-v0.4-2.md:581, docs/specs/
     empireai-architecture-v0.4-2.md:1226).
  5. Revenue collection ops channel is undefined (charging/collections is still an open question), which is a core operating gap (docs/specs/empireai-architecture-v0.4-2.md:3115).
  6. Incident channel exists per function (support_critical, channel_blocked, build_blocked) but no shared cross-functional incident bridge with single owner (docs/specs/empireai-architecture-v0.4-2.md:1393, docs/specs/empireai-
     architecture-v0.4-2.md:1395).

  Missing critical roles

  1. RevOps/Billing Ops role is missing. You track spend, but collections, dunning, invoice hygiene, and pricing ops ownership are not assigned (docs/specs/empireai-architecture-v0.4-2.md:2408, docs/specs/empireai-architecture-v0.4-
     2.md:3115).
  2. Customer Success/Onboarding is not seeded by default. Support is reactive; activation/retention operations are under-owned early (docs/specs/empireai-architecture-v0.4-2.md:1825, docs/specs/empireai-architecture-v0.4-2.md:2046).
  3. Incident Commander role is missing. CoS observes and routes, but no explicit command role for multi-team outages.
  4. Holding DevOps backup capacity role is missing. The spec itself flags Holding DevOps as potential bottleneck as verticals scale (docs/specs/empireai-architecture-v0.4-2.md:3123).

  Highest-impact fix set (ops-only)

  1. Add mandatory channels: vp_product <-> vp_growth, incident_bridge, churn_remediation_loop, portfolio_playbook_broadcast.
  2. Seed 2 lightweight roles: RevOps and Customer Success/Onboarding.
  3. Define one incident command owner per vertical (usually CoS or CTO) with explicit escalation protocol.

  If you want, I can draft a compact “Ops Efficiency Patch” section you can paste directly into v0.4.2.
