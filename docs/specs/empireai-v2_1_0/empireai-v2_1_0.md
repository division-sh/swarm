# EmpireAI — System Architecture Specification (v2.1.0)

## 1. Overview

EmpireAI is an autonomous holding company run by AI agents. It operates in two modes: a **Factory** that continuously discovers, validates, and builds vertical SaaS products, and a **Portfolio** of operating companies — each a standalone SaaS business run by its own dedicated agent team.

The human operator acts as a board of directors: approving spend, making strategic portfolio decisions, executing physical-world tasks agents can't do (§14), and receiving periodic digests. Everything else runs autonomously.

**Cold start:** `empire init` → `empire directive "..."` → system runs. See §11.0.

### Operating Model

```
┌──────────────────────────────────────────────────────────────────────┐
│  EMPIREAI HOLDING COMPANY                                             │
│                                                                       │
│  Empire Coordinator (holding CEO)                                     │
│  Factory CTO (architecture standards + template evolution)             │
│  Holding DevOps (shared infrastructure)                               │
│  Operations Analyst (cross-vertical learning)                         │
│  Spec Auditor (pre-implementation validation gate)                     │
│                                                                       │
│  ┌─────────────────┐  ┌─────────────────────────┐  ┌──────────────┐  │
│  │  FACTORY         │  │ OpCo: PeluquePet        │  │ OpCo:        │  │
│  │  (deal flow)     │  │                         │  │ DentiFácil   │  │
│  │                  │  │  CEO                    │  │              │  │
│  │  Discovery       │  │  ├─ Chief of Staff      │  │  CEO         │  │
│  │  Spec Auditor    │  │  ├─ Head of Product     │  │  ├─ ...      │  │
│  │  Validation      │  │  │  ├─ PM               │  │  ┘─ ...      │  │
│  │  Pre-Brand       │  │  │  ├─ CTO (eng mgr)    │  │              │  │
│  │                  │  │  │  │  ├─ Tech Writer   │  │              │  │
│  │                  │  │  │  │  ├─ Backend        │  │              │  │
│  │                  │  │  │  │  ├─ Frontend       │  │              │  │
│  │                  │  │  │  │  ├─ QA             │  │              │  │
│  │                  │  │  │  │  ┘─ DevOps ←→ HQ   │  │              │  │
│  │                  │  │  │  ┘─ Support           │  │              │  │
│  │                  │  │  ┘─ Head of Growth       │  │              │  │
│  │                  │  │     ┘─ Marketing         │  │              │  │
│  ┘─────────────────┘  ┘─────────────────────────┘  ┘──────────────┘  │
│                                                                       │
│  Human: Board of Directors (mailbox)                                  │
┘──────────────────────────────────────────────────────────────────────┘
```

### Stack

- **Runtime:** Go (goroutines, channels)
- **Persistence:** PostgreSQL
- **Intelligence:** Dual LLM runtime:
  - **Claude API** (Anthropic) — production runtime with native tool use
  - **Claude CLI** (`claude -p`) — non-interactive test runtime for development and validation
- **Product Stack:** Go API + Postgres (per-vertical schema) + Frontend (per-vertical), built by OpCo CTO agents
- **Infrastructure:** Single Hetzner box (scales to multi-box), Nginx reverse proxy
- **External Integrations:** Google Maps API, web scraping, social media APIs, WhatsApp Business API, email/SMS services, domain registrar API

### Design Principles

- Event-driven, asynchronous by default
- Three communication primitives: events (facts), messages (directives), tasks (factory review cycles)
- Four wake-up sources: events, messages, timers, external inbound
- **Three-tier routing (bootstrap + seeded + discovery):** bootstrap routes prevent deadlocks (can't remove), seeded routes cover common-sense day-1 needs (removable), discovery layer lets agents find vertical-specific patterns organically through direct messaging, manager-installed subscriptions, and periodic retrospective
- Hierarchical coordination (org chart model)
- Agents maintain multi-step conversation context
- Human interaction is always async — agents never block on mailbox decisions
- Human as active founder (early) scaling to board of directors (at scale) — approves spend and strategy, reviews product at high-leverage moments, can directly steer any agent
- Maximum agent autonomy within defined authority boundaries
- Postgres as single persistence layer (state + events + audit)
- No external messaging infrastructure (EventBus is Go channels + Postgres)
- Business reality drives product decisions, never the reverse
- Three-tier operating hierarchy: CEO (strategy) → VPs (coordination) → Workers (execution)
- Factory screens, operating teams build and run — clean handoff at approval
- **Communication responsibility:** if an agent produces information another agent needs, they're responsible for getting it there
- **Human as executor, agent as strategist:** agents decide what needs doing in the physical world, humans execute with agent-provided talking points and context. Agents manage the task queue, humans report results back (§14)

---

### Platform Capability Registry (v2.0.44)

The registry defines what EmpireAI can build today and what capabilities are planned. The scoring pipeline evaluates opportunities against current capabilities. Opportunities requiring unavailable capabilities are scored based on what's currently buildable, with annotations about what future capabilities would unlock.

**Current Capabilities (Tier 1 — Pure Digital):**

| Capability | Description | Cost Profile |
|-----------|-------------|-------------|
| Web UI | Dashboards, forms, data tables, file upload/download | Near-zero (hosting) |
| Go backend | Business logic, scheduled jobs, background workers, data processing | Near-zero (compute) |
| LLM integration | Document parsing, text extraction, classification, generation via API (Claude, GPT) | $0.01-0.10 per operation |
| Data storage | Postgres, file storage | Near-zero |
| Payment processing | Stripe subscriptions, one-time charges, invoicing | Stripe fees only |
| External API integration | Any REST/GraphQL API (QuickBooks, Xero, MLS, Google Workspace, Procore, etc.) | Per-API pricing |
| Document generation | PDFs, CSVs, reports from templates + data | Near-zero |
| Authentication | OAuth, API keys, user accounts | Near-zero |
| Web scraping | Page fetching, data extraction, competitive research | Near-zero |

**Planned Capabilities (Tier 2 — Async Messaging):**

| Capability | Description | Cost Profile | Status |
|-----------|-------------|-------------|--------|
| Email sending | Automated sequences, notifications, document delivery (SendGrid/Postmark) | $0.001 per email | Planned |
| SMS sending | Reminders, confirmations, alerts (Twilio) | $0.0079 per message | Planned |
| WhatsApp | Business messaging, notifications (WhatsApp Business API) | $0.005-0.08 per conversation | Planned |

**Future Capabilities (Tier 3 — Real-Time Interaction):**

| Capability | Description | Cost Profile | Status |
|-----------|-------------|-------------|--------|
| Voice AI | Inbound/outbound phone calls (Bland.ai/Vapi/Retell) | $0.07-0.15 per minute | Future |
| Browser automation | Portal uploads, form filling, government filings (Playwright) | Near-zero (compute) | Future |

**Registry usage:** The MRA tags each opportunity with `required_capabilities` indicating which current capabilities are needed and what planned/future capabilities would improve automation percentage. The pre-filter does not reject based on capability requirements — all Tier 1 opportunities are buildable. The EC uses capability annotations for portfolio sequencing: build Tier 1 products first, add Tier 2 messaging as feature expansions, evaluate Tier 3 after revenue validation.

---

## 2. Authority Matrix

This defines who decides what across the entire system. This is the constitutional document — every agent's behavior derives from this.

### 2.1 Fully Autonomous (No Human)

**Empire Coordinator (Holding CEO):**
- Factory pipeline orchestration
- Routing verticals between pipeline stages
- Auto-rejecting verticals scoring below threshold
- Portfolio-level performance monitoring
- Compiling portfolio digest from OpCo CEO reports (milestone-driven)

**Factory CTO:**
- MVP spec approval/veto on technical feasibility grounds
- Architecture standards: API patterns, security minimums, data model conventions
- Cross-vertical pattern detection and shared module extraction recommendations
- Technical spec review when escalated by OpCo CTOs
- Reviews and approves Operations Analyst proposals: bootstrap upgrades, prompt refinements, anti-pattern advisories
- Owns the org template (versioned — see §4.8): agent roster, system prompts, tool sets, default routing. Must pass Spec Auditor validation before publishing new versions. Empire Coordinator handles migration to running verticals.
- Does NOT manage infrastructure, servers, or deployments

**Holding DevOps:**
- Shared infrastructure management: Hetzner box(es), nginx, SSL, monitoring
- Port allocation and database schema isolation per vertical
- Deployment pipeline provisioning (tools that OpCo DevOps agents use)
- Server capacity monitoring → mailbox when expansion needed
- DNS management for all verticals
- Coordinates with OpCo DevOps agents on deployment execution

**Operations Analyst:**
- Cross-vertical pattern analysis: routing evolution, cost efficiency, agent lifecycle, heartbeat cadence
- Produces bootstrap upgrade proposals for Factory CTO (e.g., "promote these 3 seeded routes to bootstrap — removing them caused problems in 5/5 verticals")
- Produces prompt refinement proposals for Factory CTO
- Produces anti-pattern advisories (routes that waste budget, subscriptions that never get acted on)
- Advisory to running verticals (non-directive): "Your CoS hasn't subscribed to deploy events yet — every other vertical found this valuable by week 2"
- No authority over operating companies — output goes to Factory CTO for review
- Runs periodically: when a vertical reaches steady-state, and monthly thereafter

**Spec Auditor:**
- Systematic validation of specs and templates before they're acted on
- Two trigger points:
  1. **Template gate:** Factory CTO drafts a new template version → Spec Auditor validates before publish
  2. **Vertical spec gate:** OpCo CTO approves technical spec → Spec Auditor validates before build starts
- Validation checks (systematic, not architectural judgment):
  - Contract completeness: every event has at least one producer and one consumer
  - Tool/prompt parity: every tool referenced in a prompt exists in the agent's tool list
  - Subscription/naming consistency: event names match naming convention, subscriptions resolve
  - DDL integrity: FK dependencies orderable, column types consistent across references
  - Stage coverage: every stage transition has a trigger event and an owner
  - Flow walkthrough: trace each end-to-end path (factory → approval → build → launch → operate → recover) and flag dead ends
- Output: go / no-go with issue catalog (blocker | high | medium). Blockers = no-go.
- No-go → issues route back to author: Factory CTO (template) or OpCo CTO (vertical spec)
- Go with high/medium issues → proceed, issues logged for awareness
- No architectural opinions — that's Factory CTO's job. The Spec Auditor checks internal consistency, not design quality.
- Holding-level agent, uses Sonnet (needs reasoning for consistency checking)

**OpCo CEO (per operating vertical):**
- Full authority over their operating company
- Sets budget envelopes for VPs
- Strategic decisions: pivots, pricing, positioning
- Hires/fires VP-level agents
- Reports to human (milestone-driven, max interval fallback)
- Intervenes in VP domains by exception only

**Head of Product (VP, per operating vertical):**
- Coordinates product side: CTO, PM, Support
- Hires/fires agents within their domain (within budget envelope)
- Resolves conflicts between product agents (PM vs CTO priority disputes)
- Escalates to CEO: critical product failures, fundamental architecture issues
- Produces product reports for CEO (milestone-driven)

**Head of Growth (VP, per operating vertical):**
- Coordinates growth side: Marketing, and future agents (Content, Partnerships)
- Hires/fires agents within their domain (within budget envelope)
- Decides channel strategy, outreach approach
- Escalates to CEO: budget decisions, channel pivots, CAC concerns
- Produces growth reports for CEO (milestone-driven)

**Worker agents (CTO, PM, Support, Marketing, etc.):**
- Full autonomy within their specialty
- CTO: all technical decisions, deploys freely
- PM: feature prioritization, spec writing
- Support: customer responses, triage
- Marketing: outreach execution, content
- Report to their VP, not to CEO

### 2.2 Human Approval Required (Mailbox)

**From Factory:**
- Go/kill on validated verticals (with mandate feedback — see §2.5)

**From OpCo CEOs:**
- Spending real money above auto-approve threshold (default: $15)
- Note: small spend below threshold (domain purchases, API top-ups) is auto-approved by delegated policy. The human configures this threshold. Set to $0 to require approval for all spend.
- Monthly API budget increases beyond initial allocation
- Pricing model changes beyond approved range
- Geography expansion
- Any issue the CEO can't resolve and needs board input

**Strategic portfolio decisions:**
- Killing an operating vertical
- Server capacity expansion
- Cross-vertical strategic initiatives

### 2.3 Human Review Gates (Non-Blocking, High-Value)

These gates give the human opportunity to provide input at high-leverage moments. They are **non-blocking** — if the human doesn't respond within the configured timeout (default 48h), agents proceed. But when the human engages, it's the highest-signal input in the system.

**Vertical validation + mandate shaping:**
When the factory delivers a validation kit, the human doesn't just go/kill. They can:
- Approve as-is (go with recommended brand and mandate)
- Approve with mandate edits: tweak pricing, adjust target customer, change positioning, add constraints ("focus on no-shows, don't build payment processing"), choose different brand
- Request more data: "How many of these groomers already use WhatsApp Business?"
- Kill with reason

The mandate is a *conversation*, not a rubber stamp. The human's market knowledge shapes the operating company's direction.

**Product spec review (founder mode, 1-3 verticals):**
After PM writes the product spec and before engineering starts building:
- PM spec → Head of Product → mailbox as `product_spec_review`
- Human reviews: "This solves the right problem" / "Wrong approach, here's why" / "Add X, remove Y"
- If human responds: feedback goes to Head of Product → PM for revision
- If timeout (48h): Head of Product proceeds to engineering

This catches bad product direction before $100+ of build spend. 30 minutes of human time.

**Deploy review (founder mode, 1-3 verticals):**
After first deploy and before launch outreach begins:
- CTO confirms build complete → CEO → mailbox as `deploy_review`
- Human clicks through the deployed product
- "Looks good, launch" / "Onboarding flow is confusing" / "Pricing page is wrong"
- If human responds: feedback goes to CEO → Head of Product for fixes
- If timeout (48h): CEO proceeds with launch

This catches product-market mismatch before customers see it. 15 minutes of human time.

**Founder mode scales down over time:**
- 1-3 verticals: review every product spec + every first deploy
- 4-10 verticals: review specs for markets you know, skip others
- 10+ verticals: disable review gates, full board mode

The `empire config` CLI controls which gates are active.

### 2.4 Founder Input Channel

Agents can request the human's opinion on non-blocking decisions. This is different from spend approval (which blocks dependent work) — founder input requests are purely advisory.

```yaml
mailbox_item:
  type: "founder_input"
  priority: "normal"
  from_agent: "pm-pet-grooming"
  summary: "Choosing between two product directions — need your market read"
  context:
    option_a: "Focus on no-show reduction (automated reminders, deposit collection)"
    option_b: "Focus on booking management (calendar, multi-groomer scheduling)"
    tradeoff: "Option A is simpler MVP but narrower. Option B is broader but 2x build time."
  timeout: "48h"              # Agent proceeds with their best judgment if no response
```

Any agent can request founder input via their manager → CEO → mailbox. The CEO includes their own recommendation. The human responds when they have time. If timeout expires, the CEO's recommendation stands.

Use sparingly — agents should make most decisions autonomously. Founder input is for genuine strategic forks where the human's market knowledge changes the answer.

### 2.5 Human Gets Briefed (Portfolio Digest)

**Portfolio digest (compiled by Empire Coordinator from CEO reports, milestone-driven):**

The digest is split into **action required** and **informational**:

```yaml
portfolio_digest:
  action_required:                # Scan first — what needs you?
    mailbox_pending:
      - id: "..."
        type: "vertical_decision"
        summary: "Pet Grooming Cancún — validation complete, score 82"
        waiting_since: "2 days"
      - id: "..."
        type: "spend_request"
        summary: "PeluquePet domain purchase, $12"
        waiting_since: "6 hours"
    review_gates:                 # Founder mode items
      - id: "..."
        type: "product_spec_review"
        vertical: "PeluquePet"
        summary: "PM spec ready for review — pet grooming scheduling app"
        timeout_in: "36 hours"
    founder_input_requests:
      - id: "..."
        vertical: "DentiFácil"
        summary: "PM choosing between appointment booking vs patient records"
        timeout_in: "24 hours"
  
  informational:                  # Read when you have time
    per_vertical:
      - vertical: "PeluquePet"
        trigger: "milestone: 25th user"
        summary: "Strong growth. 25 users, $375 MRR, CSAT 4.2."
        highlights: "No-show reduction feature driving retention"
        concerns: "Instagram DM response rate declining"
    portfolio:
      total_mrr: "$375"
      total_cost: "$104"
      verticals_operating: 1
      verticals_in_pipeline: 3
    factory:
      verticals_scored: 12
      verticals_validating: 2
      verticals_ready_for_review: 1
    system:
      operations_analyst: "No bootstrap upgrades pending (need 3+ verticals for cross-vertical analysis)"
      factory_cto: "WhatsApp integration pattern extracted, available for reuse"
      infrastructure: "Hetzner box at 23% utilization"
```

The human should be able to scan `action_required` in under 2 minutes and decide: "nothing needs me right now" or "PeluquePet spec needs my review."

**Digest compilation triggers:**
1. **New mailbox item:** any `priority: critical` item triggers immediate Telegram push with digest summary
2. **Milestone CEO report:** `opco.ceo_report` with a phase transition or metric milestone triggers digest recompilation
3. **Scheduled daily:** Empire Coordinator schedules a `timer.portfolio_digest` at 09:00 local time. Compiles and pushes even if nothing changed (confirms system is alive)
4. **On demand:** `empire digest` CLI always returns current state

**Delivery:**
- Telegram push: compact summary — action_required count, critical items, one-liner per operating vertical. Links to `empire digest` for full view.
- CLI: full YAML digest as shown above.
- Dashboard (§10.5): live view.

**Event:**

| Event | Emitter | Consumer | Payload |
|-------|---------|----------|---------|
| `portfolio.digest_compiled` | Empire Coordinator | — (audit, Telegram delivery) | digest content, trigger_reason, action_required_count |

### 2.6 Factory CTO vs OpCo CEO Authority

The Factory CTO and OpCo CEOs have a board advisor relationship:

| Domain | Factory CTO | Holding DevOps | OpCo CEO |
|--------|-------------|----------------|----------|
| MVP spec feasibility (factory) | **Reviews and approves** | — | Does not exist yet |
| Architecture standards | **Sets minimums** | Implements in scaffold | Must meet minimums |
| Staging validation requirement | **Mandatory standard** | Provisions both environments | Must comply (CTO can skip for hotfixes — logged) |
| Shared infrastructure (server, nginx, SSL) | Advisory | **Owns and manages** | Must request via OpCo DevOps |
| Product architecture | Advisory only | — | **Decides** (via their CTO) |
| Technology choices | Advisory only | — | **Decides** |
| Cross-vertical code patterns | **Identifies and proposes** | — | Can adopt or ignore |
| Port/DB/capacity allocation | — | **Manages** | Must comply |
| Deployment execution | — | Provides pipeline | **Decides when** (CTO), DevOps executes how |
| What to test / when to promote | Advisory | — | **OpCo CTO decides** (QA executes) |
| Technical spec review | **Reviews when escalated** | — | OpCo CTO produces and owns |

Factory CTO sets standards and reviews architecture. Holding DevOps runs the servers. OpCo CTOs own their product's technical decisions. OpCo DevOps agents coordinate with Holding DevOps on deployment execution. Clean separation: what vs how vs where.

### 2.7 Human Task Execution

Some tasks require physical-world action that agents cannot perform: phone calls, in-person meetings, government office visits, bank account setup, partnership negotiations. Agents can request human execution via the `human_task_request` tool (§14).

**Key constraints:**
- Human tasks have a weekly budget (`max_human_tasks_per_week`), enforced by Empire Coordinator
- Agents must exhaust all digital channels before requesting a human. If WhatsApp/email can reach the target, the request is rejected.
- Empire Coordinator acts as guardrail — evaluates expected value, checks budget, prioritizes across portfolio before routing to human
- Human reports result via CLI (`empire tasks complete`), result feeds back to requesting agent's conversation

**Scaling path:**
- Phase 1: Founder is the sole executor. Tasks arrive via Telegram. Budget: 3/week.
- Phase 2 (revenue): 1-2 employees in Asunción. Tasks route based on category (sales vs compliance vs support).
- Phase 3 (scale): Small office. Agents manage human workforce through task assignment, status tracking, and result evaluation.

See §14 for full specification.

---

## 3. Agent Hierarchy

### 3.1 Holding Company Level

```
                    ┌────────────────────┐
                    │ Empire Coordinator  │
                    │ (holding CEO)       │
                    ┘────────┬───────────┘
                             │
        ┌────────────────────┼──────────────────────┐
        │                    │                       │
┌───────▼────────────┐ ┌────▼─────────┐ ┌──────────▼─────────┐
│ Holding Staff      │ │   Factory    │ │ Operating Verticals │
│                    │ │   Pipeline   │ │ (one company per    │
│ Factory CTO        │ │              │ │  approved vertical) │
│ Holding DevOps     │ ┘──────┬───────┘ ┘──────────┬─────────┘
│ Operations Analyst │        │                    │
│ Spec Auditor       │ ┌──────┼──────────┐   (see §3.3)
┘────────────────────┘ │      │          │
                  ┌────▼────┐ ┌──▼────┐ ┌──▼──────────┐
                  │Discovery│ │Scoring│ │Validation    │
                  │Coord    │ │Pipeline│ │Coord         │
                  ┘────┬────┘ ┘───┬───┘ ┘──┬───────────┘
                       │          │        │
                  (scanners)  (analysts) (research, mvp spec,
                              (runtime)   pre-brand)
```

### 3.2 Factory Pipeline Agents

**Empire Coordinator (Holding CEO)**
- Owns global strategy: which geographies to scan, which discovery modes to run, portfolio allocation
- Handles judgment tasks: digest compilation, marginal decisions, health evaluation, budget enforcement, human task guardrails
- Interprets complex directives that the runtime's DirectiveParser cannot handle (§4.2.2.4)
- Processes human decisions from mailbox (vertical.approved → opco.spinup_requested)
- Monitors operating vertical performance via CEO reports
- Escalates strategic decisions to human mailbox
- **Does NOT handle:** scan cycling, validation gate tracking, discovery accumulation, simple directive translation — these are runtime state machines (§4.2.2)
- **Human task guardrail (§14):** evaluates all `human_task_request` calls from any agent. Approves, rejects, or defers based on weekly budget, expected value, and cross-portfolio priority. Only approved tasks reach the human.

**Factory CTO**
- Cross-cutting technical authority (architecture standards, not operations)
- Reviews MVP specs for technical feasibility during factory validation
- Sets technical standards that OpCo CTOs must follow (API patterns, security minimums, data model conventions)
- Detects cross-vertical patterns for shared module extraction
- Reviews OpCo technical specs when escalated
- Reviews and approves Operations Analyst proposals (bootstrap upgrades, prompt refinements, anti-patterns)
- Owns agent templates — system prompts, tool sets, default configurations evolve based on analyst data
- Does NOT manage servers, deployments, or infrastructure operations

**Holding DevOps**
- Owns the shared infrastructure: Hetzner box(es), nginx, SSL, DNS, monitoring
- Owns the Inbound Gateway (shared webhook receiver for all verticals)
- Manages deployment pipeline used by all OpCo DevOps agents
- Executes privileged deploy actions: migrations, binary deployment, systemd, nginx config
- Port allocation, database schema isolation, resource limits per vertical
- SSL certificate provisioning and renewal (Let's Encrypt)
- Server capacity monitoring and expansion recommendations → mailbox
- Provides deployment tools and patterns that OpCo DevOps agents use
- Coordinates with Factory CTO on infrastructure standards

**Operations Analyst**
- Cross-vertical learning: reads routing evolution, cost, agent lifecycle, heartbeat data across all verticals
- Produces bootstrap upgrade proposals (promote seeded routes that prove essential to bootstrap; promote discovered routes that recur across verticals to seeded)
- Produces prompt refinement proposals (add guidance for patterns that 4/5+ verticals independently discovered)
- Produces anti-pattern advisories (subscriptions that waste budget, cadences that are too aggressive)
- Advisory notices to running verticals via Empire Coordinator (non-directive)
- Output flows to Factory CTO for review and approval
- Runs periodically: on vertical steady-state, on 3+ verticals reaching steady-state, monthly

**Discovery Coordinator**
- Handles judgment calls that the runtime's discovery accumulation (§4.2.2.3) cannot resolve
- Invoked only for: ambiguous deduplication (>70% name similarity between candidates), conflicting sub-agent reports requiring synthesis
- **Does NOT handle:** scan delegation, report accumulation, threshold filtering, exact-match dedup, scan.completed emission — these are runtime state machines (§4.2.2.3)
- Four discovery modes (managed by runtime, sub-agents delegated by runtime):
  - Note: `automation_micro` is NOT a separate scan mode. As of v2.0.40, the MRA outputs individual opportunity signals per subcategory (multi-signal model). Each signal is a specific narrow opportunity with its own ICP, build sketch, and evidence. There is no separate `automation_micro` nested field — every signal is first-class and scored with the universal rubric. The MRA's prompt instructs it to find ALL wedges per subcategory, including narrow automation-friendly micro-opportunities that were previously reported as a secondary `automation_micro` signal.
  - `local_services`: source-specific scanners (Google Maps, Instagram, Reviews, Directories, Job Boards)
  - `saas_gap`: Market Research Agent walks SaaS taxonomy (§3.2.1) against target market
  - `saas_trend`: Trend Research Agent monitors macro signals for emerging opportunities

**Market Research Agent** (spawned by Discovery Coordinator for `saas_gap` mode)
- Carries the SaaS taxonomy (§3.2.1) as reference data. The taxonomy tells the MRA *where to look*, not what to report — it is a search direction, not the discovery unit.
- For each taxonomy subcategory, uses web search (native Tier 1) to identify **narrow opportunity signals** — specific products for specific buyers solving specific problems. One subcategory may produce 0, 1, 2, or 3+ signals. The output unit is the opportunity, not the subcategory.
- For each signal found, the MRA produces structured output:
  - `opportunity_name`: specific narrow name (e.g., "SIFEN e-invoicing compliance tool" — NOT "Accounting Bookkeeping")
  - `preliminary_icp`: one-sentence buyer description with specific role/cohort and workflow context
  - `build_sketch`: core_features (3-5), key_integrations (APIs needed), red_flags (typed blockers)
  - `evidence` (structured): competitors (name, pricing, gap, source_url), pain_signals (description, source_url), regulatory (description, source_url), buyer_communities (specific URLs)
  - `opportunity_hypothesis`: how this becomes a business
- Red flag types the MRA should identify: `regulatory_license`, `enterprise_contract`, `certification`, `two_sided_marketplace`, `funds_custody`, `requires_human_review`, `data_residency_requirement` (all blocking), `complex_integration`, `high_feature_count` (penalizing), `one_time_setup`, `accuracy_liability` (passthrough)
- Emits one `category.assessed` per opportunity signal, plus one null signal (signal_strength: 0) per subcategory where nothing was found (for audit completeness)
- MRA prompt instruction: "For each subcategory, identify ALL distinct narrow opportunities. A narrow opportunity is a specific product for a specific buyer solving a specific problem. Do not report the broad subcategory as the opportunity — find the wedge. If you find multiple wedges, report each one separately with full structured evidence. If you find no buildable wedge, report signal_strength: 0."
- Processes subcategories systematically, covering the full taxonomy (or assigned shard slice)

**Runtime pre-filter (applied to each `category.assessed` before emitting `vertical.discovered` — v2.1.0):**
1. Red flag penalties: `complex_integration` -20, `high_feature_count` -20 (block if co-occurs with complex_integration/multi-module)
2. signal_strength ≥ 55 after penalties (v2.1.0: raised from 50)
3. No blocking red flags: complex_integration+multi-module co-occurrence, phone_led_sales, enterprise_procurement, relationship_networking, physical_presence_required, support_mode_phone_video
4. ICP positive check: `preliminary_icp` contains (role_token OR cohort_token) AND workflow_anchor, AND ≥1 buyer_community URL in evidence
5. Evidence completeness: ≥2 independent source URLs — competitor+pricing+URL, community URL, or pain+URL (v2.1.0: raised from 1)
6. Retention primitive gate (v2.1.0): ≥1 of recurring_data, workflow_embedding, integration_lock_in, compliance_cadence, team_collaboration
7. Name-based dedup (exact match → skip)
8. Fuzzy dedup (>70% similarity → hold for Discovery Coordinator)
9. All checks pass → emit vertical.discovered

Fail any check → signal becomes null audit entry. Passthrough red flags (one_time_setup, accuracy_liability) carried into `discovery_context`.

**Trend Research Agent** (spawned by Discovery Coordinator for `saas_trend` mode)
- Monitors macro trend sources via web search (native Tier 1):
  - Migration/relocation trends (nomad movements, tax arbitrage, residency programs)
  - Regulatory changes (new mandates, industry formalization, tax digitization)
  - Technology enablement (AI making X newly feasible, API availability, infrastructure improvements)
  - Demographic shifts (urbanization, generational technology adoption, income growth)
  - Investment signals (VC activity in region, fintech expansion, startup ecosystem growth)
  - Community growth signals (Reddit, Twitter/X, YouTube, Facebook groups, Telegram)
- For each identified trend, produces the same structured output as the MRA: opportunity_name, preliminary_icp, build_sketch, structured evidence with source URLs, opportunity_hypothesis
- Same runtime pre-filter applies (ICP positive check, evidence completeness, red flags)
- Emits `trend.identified` with signal strength and structured evidence
- Creative, speculative work — lower volume but potentially higher-upside discoveries than gap scanning

#### 3.2.1 SaaS Taxonomy

Reference data carried by Market Research Agent and used by Empire Coordinator for systematic scanning. Not exhaustive — agents can discover subcategories not listed here.

**1. Financial Operations**
Accounting & bookkeeping, invoicing & billing, expense management, tax compliance & filing, payroll, collections & accounts receivable, reconciliation, treasury & cash management, subscription/recurring billing

**2. Commerce & Payments**
Payment processing/gateway, point of sale (POS), e-commerce platform, inventory management, order management, marketplace/multi-vendor

**3. Customer Operations**
CRM, helpdesk & support, live chat/messaging, appointment scheduling & booking, loyalty & rewards, review management

**4. Marketing & Sales**
Email marketing, social media management, landing pages/website builder, SEO tools, advertising management, lead generation, sales pipeline/deal tracking

**5. Workforce & HR**
Recruiting/ATS, employee management/HRMS, time & attendance, scheduling/shift management, training/LMS, benefits administration

**6. Operations & Productivity**
Project management, document management, contracts & e-signature, forms & surveys, workflow automation, communication/team chat

**7. Industry-Specific Vertical**
Healthcare/clinic management, real estate/property management, legal practice management, construction/field service, restaurant/food service, logistics/fleet/delivery, education/tutoring, fitness/wellness/salon

**8. Compliance & Governance**
Regulatory reporting, data privacy, audit management, license & permit tracking

The taxonomy evolves — Operations Analyst can propose additions based on cross-market scanning results. Factory CTO approves additions to the reference data.

**Scoring Pipeline (runtime-owned, §4.2.2.8)**
- Deterministic multi-dimensional scoring — no LLM agent involved
- Runtime selects rubric based on discovery mode, delegates dimensions to Analysis Agent
- Runtime accumulates dimension scores, computes weighted composite, applies gates
- Contested dimensions (<5% of cases) escalated to Empire Coordinator

#### 3.2.2 Opportunity Pattern Classification (v2.1.0)

Every opportunity discovered by the MRA is tagged with one of the following archetypes. Pattern classification informs scoring (different patterns have different competitive dynamics), EC portfolio construction (diversify across patterns), and build prioritization (some patterns are cheaper to build than others).

| Pattern | Description | Typical Signal | Avg Score Range |
|---------|-------------|---------------|----------------|
| `platform_parasitic` | Tool that sits on top of an existing platform (CDK, Procore, Shopify) and adds automation without requiring the customer to switch systems | Platform API + community complaints about missing features | 60-75 |
| `freelancer_replacement` | Productizes a task currently done by freelancers or part-time hires (resume writing, report generation, document formatting) | High-volume Fiverr/Upwork gigs, job postings for repetitive admin roles | 65-80 |
| `data_asymmetry` | Aggregates or structures data the business can't easily see (market pricing, compliance status, expiration tracking) | Businesses making decisions with incomplete information | 60-72 |
| `api_middleware` | Bridges two systems that don't natively integrate (QuickBooks ↔ industry-specific workflow, ERP ↔ billing) | Job duties describing manual copy-paste between systems | 62-77 |
| `compliance_regulatory` | Automates regulatory compliance with forced adoption (deadlines, penalties, audits) | Government mandates, industry certification requirements, lien/permit deadlines | 60-73 |
| `ai_wrapper` | Uses LLM capabilities for document parsing, classification, or generation that incumbents lack | Complex document workflows (contracts, invoices, guidelines) where rules vary per client | 64-78 |
| `workflow_automation` | Replaces a manual multi-step workflow (checklists, status tracking, deadline management) with a purpose-built tool | Job postings describing 8+ step processes tracked in spreadsheets | 55-72 |
| `unknown` | Does not clearly fit any pattern — used when MRA cannot confidently classify | — | — |

**Pattern validation (v2.1.0 corpus runs):** ai_wrapper opportunities had the highest average signal strength (64.5) across 390 signals. workflow_automation was most common (40% of viable) but lowest average strength (62.1). compliance_regulatory had highest urgency/pain due to measurable cost of non-compliance. Platform_parasitic and data_asymmetry were underrepresented in job-posting signals — these patterns are better discovered via app store reviews and API changelog signals.

#### 3.2.3 Scoring Rubric (Universal, v2.0.39)

The runtime uses a **single universal rubric** for all scan modes. This rubric is optimized for EmpireAI's autonomous operating model: it evaluates whether AI agents can discover, build, sell, and operate a targeted SaaS business — regardless of geography. The rubric replaces three separate rubrics (SaaS, Local Services, Automation Micro) that were used in v2.0.38 and earlier.

**Design principles:**
- Every dimension is load-bearing — no "nice to know" dimensions
- Execution capability (60%) over market attractiveness (30%) over upside (10%)
- Hard gates eliminate structurally incompatible verticals before scoring
- Geography-agnostic: works for US, EU, LATAM, or any market with internet access
- 9 scored dimensions + 2 hard gates = 10 Analysis Agent calls per vertical

**Hard Gates:** 2 pass/fail gates (Build Complexity, Automation Completeness) scored 0-100. Below 50 on either = `vertical.rejected` before any other scoring. One-time human setup actions (API keys, certificates) do not fail the Build Complexity gate.

**Scored Dimensions:** 8 dimensions across 3 tiers. Tier 1: Execution Fit (60%) — ICP Crispness (15%), Distribution Leverage (15%), Time-to-Value (15%), Operational Drag (15%). Tier 2: Market Viability (30%) — Pain Severity (10%), Competition Gap (10%), Monetization Clarity (10%). Tier 3: Upside (10%) — Retention Architecture (5%), Expansion Potential (5%).

**Rejection Cascade:** Gates first (fail = reject at ~$1.50-3), then dimension floors (any Tier 1 < 50 = reject), then composite thresholds. Composite ≥ 75 → shortlist. Composite 55-74 with ≥2 Tier 1 dimensions ≥70 → marginal (EC decides: promote or park). Below 55 → reject.

**Marginal path:** Parked marginals reviewed on pipeline capacity opening, new scan data, or 14-day timer. Parked >60 days with no new signals → killed. Stage: `scoring` → `marginal_review` → `researching` or `killed`.

**Full rubric definitions** (dimension questions, score anchors 20/50/80, rejection cascade steps, competition gap price-disruption rules): `contracts/system-nodes.yaml` → `scoring-node.scoring_rubric`.

**Lightweight Spec Agent** — writes MVP spec from Business Brief (core workflow + 3-5 features + happy path only)
**Spec Reviewer** — single-pass review: does the MVP spec address the #1 pain point? Is it technically feasible?

**Pre-Brand Agent** — runs in parallel with spec phase:
- Generates brand name candidates from Business Brief
- Checks domain availability (.com, country TLDs)
- Checks social handle availability (Instagram, WhatsApp Business)
- Generates brand guidelines (colors, tone, tagline)
- Recommends best name + domain combination
- Output feeds into validation kit for human review

### 3.3 Operating Company Agents (Per Vertical)

When a vertical is approved, the system spins up a full operating company with **three-tier hierarchy**: CEO → VPs → Workers.

**Three-tier specification model:**

| Tier | Producer | Contains | Does NOT contain |
|------|----------|----------|-----------------|
| Lightweight MVP spec | Factory (Spec Agent) | Core workflow, 3-5 features, happy path, data sketch | Engineering decisions, edge cases, admin flows |
| Product spec | OpCo PM | Every user journey, every screen, every flow, edge cases, personas, billing UX, onboarding, notifications | Technology choices, data models, API design |
| Technical spec | OpCo Tech Writer (under CTO) | Architecture, data models, API endpoints, integration contracts, frontend/backend boundary, infrastructure needs | Product decisions — those come from product spec |

Each tier builds on the previous. Product spec expands lightweight spec. Technical spec translates product spec into engineering.

```
Operating Company: PeluquePet (default configuration)
│
┘── OpCo CEO
    - Receives: mandate + default org + bootstrap routing
    - Reports to: human (mailbox)
    - Consumes: VP summaries, Chief of Staff cross-domain summary, escalations
    - Context: mandate, strategy, high-level metrics. Never sees code/bugs/customer messages.
    │
    ├── Chief of Staff (cross-domain coordination — no direct reports)
    │   - Reports to: CEO
    │   - Observes: events from BOTH Product and Growth domains
    │   - Routes information across domain boundaries
    │   - Ensures feature deployments reach Marketing + Support
    │   - Diagnoses churn (product issue vs messaging mismatch vs pricing)
    │   - Routes market intelligence from Marketing → PM
    │   - Coordinates launch readiness across both VPs
    │   - Produces: cross-domain reports → CEO (milestone-driven)
    │
    ├── Head of Product (VP)
    │   - Reports to: CEO
    │   - Manages: PM, CTO (engineering team), Support
    │   - Observes: all product/support events (lightweight triage)
    │   - Produces: product reports → CEO + Chief of Staff (milestone-driven)
    │   - Escalates: critical failures, team conflicts → CEO
    │   - Can hire/fire within domain
    │   │
    │   ├── PM Agent
    │   │   - Expands lightweight MVP spec → full product spec (Tier 2)
    │   │   - Manages roadmap, prioritizes features
    │   │   - Receives: feature requests from Support, market signals from Chief of Staff
    │   │   - Validates features against product spec before deploy (QA role)
    │   │   - Context: product spec, user feedback themes, roadmap
    │   │   - Sends completed product spec to CTO (bootstrap route)
    │   │
    │   ├── CTO (engineering manager)
    │   │   - Receives product spec from PM, produces technical spec via Tech Writer
    │   │   - Routes all engineering feedback (spec gaps, API changes, integration issues)
    │   │   - Organizes engineering sub-team, decides who builds what
    │   │   - Owns technical coherence across the full stack
    │   │   - Decides WHAT to deploy and WHEN — DevOps handles HOW
    │   │   - Assigns QA to validate in staging before promoting to production
    │   │   - Context: technical spec, build progress, architecture decisions
    │   │   │
    │   │   ├── Tech Writer Agent
    │   │   │   - Translates product spec → technical spec (Tier 3)
    │   │   │   - Architecture, data models, API endpoints, integration contracts
    │   │   │   - Iterates with CTO until approved
    │   │   │   - Updates spec when implementation feedback reveals gaps
    │   │   │   - Consults PM when product intent is ambiguous
    │   │   │
    │   │   ├── Backend Agent(s)
    │   │   │   - Go API server, data layer, business logic, integrations
    │   │   │   - Works from technical spec
    │   │   │   - Reports spec gaps and blockers to CTO (not Tech Writer directly)
    │   │   │
    │   │   ├── Frontend Agent(s)
    │   │   │   - HTML templates, CSS, client-side logic
    │   │   │   - Works from technical spec + product spec (for UX details)
    │   │   │   - Reports API change needs and integration issues to CTO
    │   │   │
    │   │   ├── QA Agent
    │   │   │   - Validates implementation against spec in staging environment
    │   │   │   - Build phase: API contract tests, core user journey, integration checks
    │   │   │   - Operating phase: regression suite before each deploy
    │   │   │   - Reports: pass/fail with specific failures and reproduction steps
    │   │   │   - Does NOT decide what to fix — reports to CTO who routes to Backend/Frontend
    │   │   │   - Context: technical spec, product spec, staging endpoint, test history
    │   │   │
    │   │   ┘── DevOps Agent
    │   │       - Deploys what CTO says, when CTO says
    │   │       - Coordinates with Holding DevOps on HOW (server, nginx, SSL)
    │   │       - Runs migrations, configures services, health checks
    │   │
    │   ┘── Support Agent
    │       - Handles customer inquiries (WhatsApp, email)
    │       - Routes: bugs → CTO, feature requests → PM, churn risk → Head of Product + Chief of Staff
    │       - Context: product FAQ, customer conversation history
    │
    ┘── Head of Growth (VP)
        - Reports to: CEO
        - Manages: Marketing (and future growth agents)
        - Observes: all marketing/outreach events
        - Produces: growth reports → CEO + Chief of Staff (milestone-driven)
        - Escalates: budget decisions, channel pivots → CEO
        - Can hire/fire within domain (e.g., add Content agent)
        │
        ┘── Marketing Agent
            - Pre-launch: domain, landing page, profiles
            - Launch: outreach campaigns
            - Post-launch: growth, social proof
            - Shares market signals with product side (what resonates, objections, pricing feedback)
            - Receives: feature announcements from Chief of Staff
            - Context: brand, scripts, lead lists, channel metrics
```

**Spec flow with iteration loops:**
```
Factory lightweight spec (Tier 1)
    → included in mandate
    → PM expands into product spec (Tier 2)
    → PM sends product spec to CTO (bootstrap route)

    SPEC ITERATION:
    → CTO directs Tech Writer to produce technical spec (Tier 3)
    → CTO reviews ←→ Tech Writer revises (may loop 2-3 times)
    → If Tech Writer hits product ambiguity → asks PM to clarify
    → CTO approves technical spec

    SPEC VALIDATION GATE:
    → CTO emits spec.validation_requested to Spec Auditor
    → Spec Auditor walks the spec end-to-end:
      - Every API endpoint has handler assignment (Backend or Frontend)
      - Data model covers all workflows (no missing tables/columns)
      - Event producers have consumers, tool references resolve
      - Edge cases specified for error paths
    → Go → proceed to build. No-go → issues back to CTO.
    → CTO routes fixes: spec gap → Tech Writer, product gap → PM

    BUILD WITH FEEDBACK:
    → CTO assigns: Backend builds API + data, Frontend builds UI
    → During build, engineers hit spec gaps → tell CTO
      → CTO decides: spec gap (→ Tech Writer) or implementation detail (→ direct answer)
    → Tech Writer updates spec → notifies CTO + Backend + Frontend

    STAGING VALIDATION:
    → Build complete → CTO directs DevOps to deploy to STAGING
    → OpCo DevOps coordinates with Holding DevOps (staging port + schema)
    → CTO assigns QA to validate against staging:
      - API contract tests (tech spec says X, does endpoint return X?)
      - Core user journey (end-to-end flow from product spec)
      - Integration checks (WhatsApp webhook mock, payment flow mock)
    → QA reports: pass / fail with specific failures
    → Pass → CTO requests PRODUCTION deploy (normal DevOps flow)
    → Fail → CTO routes to Backend/Frontend with QA findings → fix → redeploy to staging → QA re-validates
    → CTO may also ask PM to spot-check staging for product correctness

    PRODUCTION DEPLOYMENT:
    → CTO directs DevOps: "promote staging to production"
    → OpCo DevOps coordinates with Holding DevOps
    → Deploy complete → CTO confirms build_complete

    POST-DEPLOY (discovered, not prescribed):
    → Agents learn who needs to know about deploys:
      Support (to tell customers), Marketing (to update pitch),
      PM (to track usage). Routes formalize as patterns emerge.
```

**CTO as engineering manager and feedback router:**
The CTO does not write code. They review specs, coordinate the engineering sub-team, ensure architectural coherence, and route all implementation feedback. When Backend or Frontend hits a blocker, it goes to CTO — who decides whether it's a spec gap (→ Tech Writer, maybe PM), an implementation detail (→ direct answer), or a cross-agent coordination issue (→ assigns to both sides). CTO is the decision point for all engineering feedback, just like a real engineering lead.

**Chief of Staff as cross-domain nervous system:**
The Chief of Staff doesn't manage anyone. They receive VP summaries (bootstrap) and discover which operational events to subscribe to in weeks 1-2. Their value: they see information gaps that neither VP can see because they span both domains. When a feature deploys and Marketing doesn't know, they bridge that gap. When Marketing learns what resonates and PM doesn't hear about it, they bridge that gap. They formalize patterns into routes as they discover what recurs.

**Seven operational loops (expected to emerge, not all prescribed):**

Only the critical path (spec → build → deploy, bugs → engineering, summaries → up) is prescribed in bootstrap routing. The cross-domain loops below are patterns we *expect* agents to discover and formalize within the first 2-3 weeks. They are documented here as reference for what good communication looks like, not as prescribed routing.

| Loop | Expected flow | Bootstrap or Discovered? |
|------|--------------|--------------------------|
| Bug lifecycle | Support → CTO → fix → staging → QA → production → Support notifies customer | **Bootstrap** (Support → CTO, QA → CTO). Rest discovered. |
| Feature lifecycle | PM specs → CTO builds → staging → QA validates → production → Marketing + Support update | **Bootstrap** (PM → CTO, QA → CTO). Marketing notification = discovered. |
| Market intelligence | Marketing learns what resonates → reaches PM | **Discovered.** Chief of Staff or Marketing proposes route. |
| Churn diagnosis | Support flags risk → right domain addresses root cause | **Discovered.** Chief of Staff learns to diagnose and route. |
| Pricing feedback | Market pricing response → reaches CEO | **Discovered.** Marketing or Chief of Staff proposes route. |
| Launch coordination | Both product and growth sides ready → coordinated go | **Seeded** (build_complete → CoS, prelaunch_ready → CoS). CoS emerges as coordinator. |
| Cross-domain sync | VP reports arrive → CoS synthesizes cross-domain view | **Bootstrap** (reports → CEO + CoS). Cross-domain analysis = CoS discovers its role. |

**DevOps chain (staging → production):**
```
CTO decides WHAT and WHEN to deploy
    → OpCo DevOps prepares artifact:
       - Builds Go binary, validates migrations, packages manifest
    → FIRST: deploy to STAGING
       → OpCo DevOps emits devops.deploy_requested (environment: staging)
       → Holding DevOps deploys to staging port + staging schema
       → QA validates against staging (spec compliance, user journey, regressions)
       → QA pass → CTO approves production promotion
    → THEN: deploy to PRODUCTION
       → OpCo DevOps emits devops.deploy_requested (environment: production)
       → Holding DevOps executes privileged actions:
          - nginx config, SSL certs (first deploy only)
          - Database migrations on production schema
          - Binary deployment, systemd service management
          - Health check verification
       → Holding DevOps emits devops.deploy_complete/failed
    → Holding DevOps owns the server and shared infrastructure
```

**Routing philosophy — hierarchy ≠ routing chain:**

Routine work flows peer-to-peer (Support → CTO for bugs, PM → CTO for specs). VPs **observe** these events but don't relay them. VPs intervene by exception. Chief of Staff **observes cross-domain events** and routes information across boundaries. CEO gets VP summaries + Chief of Staff cross-domain summary.

| Layer | Wakes up for | Typical calls/day | Model |
|-------|-------------|-------------------|-------|
| CEO | VP summaries, CoS summary, escalations, spend approvals | 1-3 | Sonnet |
| Chief of Staff | Cross-domain events, feature deploys, churn signals, cross-domain reports | 5-15 | Haiku (routing) / Sonnet (diagnosis) |
| VPs | Observe domain events, triage, milestone reports | 5-15 | Haiku (triage) / Sonnet (report) |
| CTO | Tech spec review, engineering feedback routing, deploy decisions | 5-15 | Sonnet |
| Workers | Their actual job | 10-50+ | Sonnet (Backend/Frontend/PM/Marketing/Tech Writer) / Haiku (Support/DevOps) |

**CEO's constraints:**
- Must stay within allocated API budget (can request increases via mailbox)
- Must report to human on milestones and max interval (compiled from VP + CoS reports)
- Must comply with Factory CTO architecture standards
- Must deploy through Holding DevOps (via OpCo DevOps agent)
- Must request mailbox approval for real-money spend above auto-approve threshold (spend below threshold is auto-approved by delegated policy)
- Cannot expand to new geographies without mailbox approval

**VP budget envelopes:**
CEO allocates a portion of the monthly API budget to each VP. VPs manage their team's spend within that envelope. If a VP needs more (e.g., Head of Product wants a second Backend agent), they request from CEO, not mailbox — it's an internal reallocation.

### 3.4 Agent Lifecycle

**Holding agents** are always running: Empire Coordinator, Factory CTO, Spec Auditor, Holding DevOps, Operations Analyst, factory pipeline agents.

**OpCo agents** are created when a vertical is approved and destroyed when a vertical is killed:

```
vertical.approved (with founder directives + brand choice + mandate edits)
    → Empire Coordinator assembles final mandate (factory docs + founder directives)
    → AgentManager spawns default org:
      CEO, Head of Product, Head of Growth,
      PM, CTO (+ Tech Writer, Backend, Frontend, QA, DevOps), Support, Marketing
    → Bootstrap + seeded routing installed (20 bootstrap + 7 seeded = 27 routes)
    → CEO reviews mandate (including founder directives), sets VP budget envelopes
    → CEO directs VPs to begin

    SPEC PHASE:
    → Head of Product directs PM: "expand lightweight spec into product spec"
    → PM writes product spec (Tier 2)
    → FOUNDER GATE (if enabled): Head of Product sends spec to mailbox as product_spec_review
      - Human reviews: "looks right" / "wrong approach" / "add X, remove Y"
      - If no response in 48h: proceed
      - If feedback: Head of Product → PM for revision, then re-submit
    → PM spec (approved or timed out) sends to CTO
    → CTO directs Tech Writer: "translate product spec into technical spec"
    → Tech Writer produces technical spec (Tier 3)
    → CTO reviews, approves (or requests revision)

    BUILD PHASE:
    → CTO assigns work to Backend + Frontend from technical spec
    → Backend builds API, data layer, integrations
    → Frontend builds UI from product spec + technical spec
    → CTO coordinates: "Backend API ready, Frontend can integrate"
    → CTO directs DevOps: deploy to staging → QA validates → CTO promotes to production

    PARALLEL — Head of Growth orchestrates pre-launch:
    → Marketing does domain, landing page, profiles, outreach prep

    DEPLOY REVIEW (if enabled):
    → CTO confirms build complete
    → FOUNDER GATE: CEO sends deployed URL to mailbox as deploy_review
      - Human clicks through product: "looks good" / "fix onboarding" / "pricing wrong"
      - If no response in 48h: proceed to launch
      - If feedback: CEO → Head of Product for fixes, then re-submit

    LAUNCH:
    → Head of Product confirms product live (and reviewed, if gate enabled)
    → Head of Growth confirms channels ready
    → CEO coordinates launch

    STEADY-STATE:
    → VPs run their domains, CEO gets milestone-driven reports
    → Support handles customers, PM prioritizes features, CTO manages iterations
    → Founder input channel available for strategic forks (non-blocking)
    → When stabilized (4+ weeks launched, active users, revenue, no major pivots):
       CEO emits opco.steady_state_reached → Empire Coordinator + Operations Analyst
       Operations Analyst begins cross-vertical analysis for this vertical's data
```

---

## 4. Runtime Architecture

### 4.1 Process Model

Single Go process (the **orchestrator**). Manages event bus, scheduler, agent lifecycle, and Tier 2 tool execution. Each agent task runs as an LLM coding session (Claude Code / Codex) inside a scoped Docker container. The orchestrator dispatches events to agents, spawns their sessions in the right container, injects Tier 2 tools, and processes their outputs.

```go
type Agent interface {
    ID() string
    Type() AgentType
    Subscriptions() []EventType
    OnEvent(ctx context.Context, event Event) ([]Event, error)
}
```

### 4.1.1 Container Architecture

Docker provides the isolation boundary. Agents don't run on the host — they run inside containers with scoped volume mounts. The orchestrator is the only process that sees the full system.

**Container layout:**

```yaml
# docker-compose.yml
services:
  # === Infrastructure (always running) ===
  
  postgres:
    image: postgres:16
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: empireai
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    # Separate container — agents can't DROP DATABASE even if they try.
    # Only the orchestrator and sql_execute tool connect here.

  orchestrator:
    build: ./cmd/orchestrator
    depends_on: [postgres]
    volumes:
      - ./empire.yaml:/opt/empireai/empire.yaml:ro
      - /var/run/docker.sock:/var/run/docker.sock  # To spawn agent containers
    environment:
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      EMPIREAI_DB_PASSWORD: ${DB_PASSWORD}
      EMPIREAI_TELEGRAM_TOKEN: ${EMPIREAI_TELEGRAM_TOKEN}
    ports:
      - "8080:8080"   # Inbound Gateway (webhooks)
    # The orchestrator is the brain — event bus, scheduler, agent manager,
    # Tier 2 tool executor. It spawns agent sessions in workspace containers.

  gateway:
    # Inbound Gateway runs as a goroutine inside orchestrator (see §4.7).
    # Not a separate container — listed here for clarity.

  # === Agent workspaces (created dynamically per vertical) ===
  
  # Template — orchestrator creates one of these per vertical at spinup:
  # docker create --name empireai-{vertical_slug} \
  #   -v verticals_{slug}:/workspace \
  #   -w /workspace \
  #   -e DATABASE_URL=postgres://.../{slug}_schema \
  #   empireai-workspace:latest

  # All agents in a vertical share the same container/volume.
  # Backend and Frontend NEED the same files (same codebase).
  # QA needs to read them. DevOps needs to build from them.
  # Isolation is between verticals, not between agents in a vertical.

  # === Privileged workspace (Holding DevOps) ===
  
  # Broader mounts for cross-vertical infrastructure management:
  # docker create --name empireai-infra \
  #   -v verticals:/opt/empireai/verticals \
  #   -v nginx:/opt/empireai/nginx \
  #   -v systemd:/etc/systemd/system \
  #   --privileged \
  #   empireai-workspace:latest

  # === Factory workspace (Factory CTO, scaffold editing) ===
  
  # docker create --name empireai-factory \
  #   -v scaffold:/opt/empireai/scaffold \
  #   -w /opt/empireai/scaffold \
  #   empireai-workspace:latest

volumes:
  pgdata:          # Postgres data — survives everything
  scaffold:        # Factory CTO's scaffold template
  # Per-vertical volumes created dynamically:
  # verticals_{slug} for each vertical
```

**Workspace base image:**

```dockerfile
# empireai-workspace:latest
FROM golang:1.22-bookworm

# Tools that agents need in their environment
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — agents can't escalate
RUN useradd -m -s /bin/bash agent
USER agent

# Claude Code / Codex CLI (or equivalent LLM coding tool)
# Installed globally, available to all agent sessions
```

**How the orchestrator spawns an agent session:**

When the orchestrator dispatches an event to an agent, it:

1. Resolves which container the agent belongs to (vertical slug → `empireai-{slug}`, Factory CTO → `empireai-factory`, Holding DevOps → `empireai-infra`, all others → orchestrator handles internally via API calls)
2. Starts an LLM coding session inside that container:
   ```bash
   docker exec -w /workspace empireai-pet-grooming \
     claude-code --system-prompt "{agent_prompt}" \
       --tools "{tier2_tools_json}" \
       --max-turns {max_turns_per_task} \
       "{task_description_from_event}"
   ```
3. The LLM session uses native tools (file, shell, web) scoped to the container's filesystem, plus injected Tier 2 tools that callback to the orchestrator via HTTP/gRPC
4. When the session completes, orchestrator parses emitted events from the output and publishes them to the EventBus
5. Container stays running between tasks (warm start for next event)

**Tier 2 tool callbacks:**

Tier 2 tools (`agent_message`, `sql_execute`, `mailbox_send`, etc.) can't execute inside the agent container — they need access to Postgres, the event bus, and other agents. They're implemented as HTTP endpoints on the orchestrator:

```
Agent container                          Orchestrator
─────────────                          ────────────
LLM calls sql_execute(query)  ────→   POST /tools/sql_execute
                                       → Scopes to agent's schema
                                       → Executes on Postgres
                              ←────   Returns result
LLM sees result, continues
```

The orchestrator validates every Tier 2 tool call (authorization, tenant isolation, credential injection) before executing. The agent container has no direct database access — `DATABASE_URL` in the container env is for native `psql` usage during development/debugging only, and points to a schema-scoped read-only connection.

**Tool schema contract:** All Tier 2 tool input schemas are defined in `contracts/tool-schemas.yaml` (21 tools). The MCP tool gateway generates tool definitions from this file — agents see typed parameters with enum constraints in their tool definitions. Same pattern as `emit_*` tools generated from EventSchemaRegistry. `TestContractCompliance/tool_schemas` verifies every agent's `tools_tier2` entry has a matching schema.

**Container lifecycle:**

| Event | Action |
|-------|--------|
| `opco.spinup_requested` | Orchestrator creates vertical volume + container, runs schema migration |
| Agent task dispatched | `docker exec` into warm container, start LLM session |
| Agent task complete | Session exits, container stays warm |
| Vertical killed | Container stopped, volume retained (data retention policy) |
| Orchestrator restart | Reconnects to existing containers, replays pending events from Postgres |

**What this gives us:**
- **Blast radius:** Agent nukes its container → rebuild, Postgres untouched. `rm -rf /` destroys the workspace, not the system.
- **Tenant isolation:** pet-grooming container can't see lawn-care volume. Enforced by Docker, not by code.
- **No path checking code:** Volume mounts ARE the access control. No prefix checks, no chroot wrappers, no allowlists.
- **Reproducibility:** Workspace image is deterministic. Nix flake can build it for extra reproducibility.
- **Development safety:** Run everything locally in Docker, worst case is `docker compose down && docker compose up`.

### 4.2 EventBus

Thin wrapper around Go channels with Postgres write-through. Supports two routing modes:

```go
type EventBus struct {
    channels       map[EventType][]chan Event
    routingTables  map[string]*RoutingTable  // In-memory read model, loaded from routing_rules table
    db             *sql.DB
}

// RoutingTable is an in-memory derived view of routing_rules for a vertical.
// Source of truth: routing_rules table in Postgres.
// Reloaded on startup and on routing_updated events.
type RoutingTable struct {
    VerticalID  string
    Routes      []Route
}

type Route struct {
    Event    EventType   // e.g., "opco.pet-grooming.bug_reported"
    From     string      // Agent role or ID
    To       []string    // Agent role(s) or ID(s)
    Payload  string      // Expected payload description
}

func (eb *EventBus) Publish(event Event) error
func (eb *EventBus) Subscribe(agentID string, eventTypes ...EventType) <-chan Event
func (eb *EventBus) SetRoutingTable(verticalID string, table *RoutingTable) error
func (eb *EventBus) GetRoutingTable(verticalID string) *RoutingTable
```

Every published event is:
1. Written to Postgres `events` table (append-only, never mutated)
2. Classified as **factory** or **OpCo internal** based on event type pattern (see below)
3. For factory events: fanned out to all subscribed channels (static subscriptions from agent YAML configs)
4. For OpCo internal events: routing_rules resolved to concrete agent IDs, delivery manifest written to `event_deliveries`, then fanned out to resolved recipients
5. If an OpCo internal event resolves to **zero recipients** (no matching routing rules), the event is still persisted but the runtime also emits `spec.contradiction_detected` — an agent published an event that nobody is listening to, which likely indicates a missing route or naming mismatch

**Event routing classification (CRITICAL — do not use vertical_id):**

The EventBus must classify events by **event type pattern**, not by `vertical_id`. Many factory events carry a `vertical_id` because they're *about* a specific vertical (e.g., `validation.started` for vertical X) but must be routed to factory agents via static subscriptions, not through the OpCo routing table.

Classification rules:

| Pattern | Classification | Routing | Examples |
|---------|---------------|---------|----------|
| `system.*` | Factory | Static subscriptions | `system.started`, `system.directive` |
| `scan.*` | Factory | Static subscriptions | `scan.requested`, `scan.completed` |
| `vertical.*` | Factory | Static subscriptions | `vertical.discovered`, `vertical.scored`, `vertical.approved` |
| `scoring.*` | Factory | Static subscriptions | `scoring.requested` |
| `validation.*` | Factory | Static subscriptions | `validation.started` |
| `research.*` | Factory | Static subscriptions | `research.completed` |
| `spec.*` | Factory | Static subscriptions | `spec.draft_ready`, `spec.validation_passed` |
| `spec_review.*` | Factory | Static subscriptions | `spec_review.requested`, `spec_review.passed` |
| `cto.*` | Factory | Static subscriptions | `cto.spec_review_requested`, `cto.spec_approved` |
| `brand.*` | Factory | Static subscriptions | `brand.requested`, `brand.candidates_ready` |
| `template.*` | Factory | Static subscriptions | `template.version_published`, `template.migration_planned` |
| `budget.*` | Factory | Static subscriptions | `budget.warning`, `budget.throttle` |
| `human_task.*` | Factory | Static subscriptions | `human_task.requested`, `human_task.approved` |
| `analyst.*` | Factory | Static subscriptions | `analyst.bootstrap_upgrade_proposal` |
| `portfolio.*` | Factory | Static subscriptions | `portfolio.digest_compiled` |
| `source.*` | Factory | Static subscriptions | `source.scraped` |
| `score.*` | Factory | Static subscriptions | `score.dimension_complete` |
| `category.*` | Factory | Static subscriptions | `category.assessed` |
| `trend.*` | Factory | Static subscriptions | `trend.identified` |
| `devops.*` | Factory | Static subscriptions | `devops.deploy_requested`, `devops.deploy_complete` |
| `opco.*` | Factory (cross-vertical) | Static subscriptions | `opco.spinup_requested`, `opco.ceo_report`, `opco.launched` |
| `campaign.*` | Factory | Static subscriptions | `campaign.completed` |
| `dedup.*` | Factory | Static subscriptions | `dedup.ambiguous`, `dedup.resolved` |
| `synthesis.*` | Factory | Static subscriptions | `synthesis.needed`, `synthesis.resolved` |
| `market_research.*` | Factory | Static subscriptions | `market_research.scan_assigned` |
| `trend_research.*` | Factory | Static subscriptions | `trend_research.scan_assigned` |
| `scanner.*` | Factory | Static subscriptions | `scanner.{type}.scan_assigned` |
| Short names (no dot prefix) | OpCo internal | Routing table | `bug_reported`, `feature_deployed`, `build_complete` |
| `qa.*` | OpCo internal | Routing table | `qa.validation_passed`, `qa.validation_failed` |
| `inbound.*` | OpCo internal | Routing table (§4.7) | `inbound.{vertical}.whatsapp_message` |

Implementation: the simplest correct approach is a **whitelist of factory event prefixes**. If the event type starts with any factory prefix, use static subscriptions. Everything else uses the routing table. Do NOT use `vertical_id` presence/absence — factory events about a vertical still carry `vertical_id` as payload context.

```go
// Factory event prefixes — route via static subscriptions, never through routing table.
var factoryPrefixes = []string{
    "system.", "scan.", "vertical.", "scoring.", "validation.", "research.",
    "spec.", "spec_review.", "cto.", "brand.", "template.", "budget.",
    "human_task.", "analyst.", "portfolio.", "source.", "score.",
    "category.", "trend.", "devops.", "opco.", "campaign.", "dedup.",
    "synthesis.", "market_research.", "trend_research.", "scanner.",
    "mailbox.",
}

func (eb *EventBus) isFactoryEvent(eventType EventType) bool {
    for _, prefix := range factoryPrefixes {
        if strings.HasPrefix(string(eventType), prefix) {
            return true
        }
    }
    return false
}
```

OpCo internal events use short names without a dotted domain prefix (`bug_reported`, `feature_deployed`, `build_complete`, `support_digest`, `spend_needed`) plus `qa.*` and `inbound.*`. These are the only events that go through the per-vertical routing table.

When an agent processes an event, a receipt is written to `event_receipts(event_id, agent_id, status)`. On crash recovery, unprocessed events for each agent are found via two paths:

**Factory agents (subscription-based delivery):**
```sql
SELECT e.* FROM events e
WHERE e.type = ANY($1)                    -- agent's subscribed event types
  AND e.created_at >= $3                  -- agent's started_at (replay boundary)
  AND NOT EXISTS (
    SELECT 1 FROM event_receipts r
    WHERE r.event_id = e.id AND r.agent_id = $2
      AND r.status IN ('processed', 'skipped')  -- only successful receipts count
  )
ORDER BY e.created_at ASC;
```

**OpCo agents (routing-table-based delivery):**
```sql
-- Reconstruct intended recipients from routing_rules at publish time.
-- At publish, EventBus persists a delivery manifest: event_deliveries(event_id, agent_id).
-- Recovery replays events with a delivery record but no successful receipt.
SELECT e.* FROM events e
JOIN event_deliveries d ON d.event_id = e.id AND d.agent_id = $1
WHERE e.created_at >= $2                  -- agent's started_at (replay boundary)
  AND NOT EXISTS (
    SELECT 1 FROM event_receipts r
    WHERE r.event_id = e.id AND r.agent_id = $1
      AND r.status IN ('processed', 'skipped')
  )
ORDER BY e.created_at ASC;
```

**Retry policy:** Events with `status = 'error'` are retried up to 3 times with exponential backoff (1m, 5m, 30m). After 3 failures, the event is marked `dead_letter` and escalated to the agent's manager. The manager decides: retry, skip, or escalate further.

### 4.2.1 Runtime Emission Guardrails

Agents emit events by calling typed `emit_*` tools — not by returning JSON in their response text. Each agent receives only the `emit_*` tools for events it's authorized to produce (per `agent-tools.yaml` emit_events). This makes the emission allowlist structural: if the tool doesn't exist in the agent's session, the event cannot be emitted.

**Why tools instead of JSON envelopes:** Live testing showed LLMs reliably understand *which* event to emit but unreliably produce the correct payload shape. Tool calling solves this: the input schema enforces field names, types, enums, and required fields at the API level. Malformed calls are rejected by the LLM runtime before reaching the EventBus.

**How it works:**

1. At agent session start, the runtime looks up the agent's allowed emissions from `agent-tools.yaml` emit_events
2. For each event type, loads the payload schema from the Event Schema Registry (§4.5.1)
3. Generates a tool definition: `emit_{event_name_with_underscores}` with the schema as `input_schema`
4. Injects into the LLM session alongside existing Tier 2 tools (agent_message, mailbox_send, etc.)

When the LLM calls an `emit_*` tool:

1. The LLM API validates arguments against the tool's input_schema (structural enforcement)
2. Runtime receives the tool call with validated arguments
3. Runtime constructs the Event and calls `EventBus.Publish()` (goes through interceptor)
4. Runtime returns confirmation as tool result: `"Event {type} published (id: {uuid})"`
5. LLM sees confirmation and continues (or stops, per prompt instructions)

**Layer 2: State transition rules.** Certain events may only be emitted in response to specific inbound events. This prevents agents from skipping pipeline stages regardless of what their prompt says.

| Emitting agent | Guarded event | Only valid when inbound is | Rationale |
|---------------|---------------|---------------------------|-----------|
| Empire Coordinator | `opco.spinup_requested` | `vertical.approved` (from human) | Cannot skip validation/approval pipeline |
| Empire Coordinator | `template.migration_completed` | `template.migration_approved` (from human) | Cannot migrate without approval |
| Validation Coordinator | `vertical.ready_for_review` | `validation.package_ready` (from runtime) | Cannot submit incomplete validation kit |
| Factory CTO | `template.version_published` | `spec.validation_passed` (from Spec Auditor) | Cannot publish unvalidated template |

Implementation: the runtime tracks the inbound event that triggered the current agent turn. When the agent calls a guarded `emit_*` tool, the runtime checks the inbound event type against the allowed triggers. Mismatch → reject with tool error explaining why.

```go
type TransitionRule struct {
    EmittingRole    string
    GuardedEvent    EventType
    AllowedInbound  []EventType   // Inbound event must be one of these
}

var transitionRules = []TransitionRule{
    {"empire-coordinator", "opco.spinup_requested", []EventType{"vertical.approved"}},
    {"empire-coordinator", "template.migration_completed", []EventType{"template.migration_approved"}},
    // ... additional rules
}
```

**Violation handling:** All guardrail violations are logged to `runtime_log` (component: `guardrails`, action: `blocked`) and included in the Operations Analyst's cross-vertical analysis data. Persistent violations from a specific agent indicate a prompt problem — the hot-reload system (§4.3) enables rapid correction.

**Backward compatibility:** The JSON envelope response format (`{"emit_events":[...]}`) is no longer supported. All agents emit events exclusively via `emit_*` tool calls. Agent prompts no longer contain payload format documentation — the tool schema is the contract.

### 4.2.2 Runtime Pipeline Coordinator

The runtime handles all deterministic coordination between agents. LLMs are invoked only when judgment is required. This separation emerged from live testing: coordinators given procedural instructions (gate tracking, scan cycling, directive translation) reliably understood the logic but unreliably executed it across turns.

**Design principle:** If the correct action can be determined by a state machine (input event + current state → output event), it belongs in the runtime. If the action requires reasoning over context, evidence, or tradeoffs, it belongs in an LLM agent.

#### Integration with EventBus

The pipeline coordinator is a **middleware layer** inside the EventBus publish path. It intercepts specific factory event types before they reach agent subscriptions, processes them through state machines, and may emit new events or suppress delivery.

```go
type PipelineCoordinator struct {
    campaigns    map[uuid.UUID]*Campaign          // Active scan campaigns
    validations  map[uuid.UUID]*ValidationPipeline // Per-vertical validation state
    scans        map[uuid.UUID]*ScanAccumulator    // Per-scan report accumulation
    scorings     map[uuid.UUID]*ScoringAccumulator // Per-vertical dimension accumulation
    db           *sql.DB
    bus          *EventBus                         // Reference for emitting events
    llm          LLMRuntime                        // For one-shot judgment calls
}

// PERSISTENCE: All state (campaigns, validations, scans) is backed by Postgres.
// In-memory maps are a read-through cache loaded on startup.
// Every state mutation writes to DB first, then updates in-memory.
// On crash recovery: reload from DB, replay unprocessed events.
// Tables: campaigns, validation_pipelines, scan_accumulators (with reports JSONB).
//
// TRANSACTIONS: Each Intercept() handler runs within the same DB transaction
// as the event persistence. If the handler fails, event persistence is rolled back.
// This ensures atomic state transitions: no event can be persisted without its
// corresponding state update.
//
// IDEMPOTENCY: All handlers must be idempotent. If a handler is called twice
// with the same event (e.g., after crash recovery), the second call must be
// a no-op. Implement via: check if the event_id has already been processed
// (event_receipts table) before applying state changes.
//
// RE-ENTRANCY: Handlers collect events to emit in a deferred queue rather than
// calling pc.bus.Publish() directly. After the handler returns, the EventBus
// publishes queued events outside the interceptor context. This prevents
// recursive Intercept() calls and potential deadlocks.
//
// EMISSION GUARDRAILS: Events emitted by the pipeline coordinator carry
// runtime_origin=true in their metadata. The emission guardrail layer (§4.2.1)
// skips validation for runtime-origin events — the runtime is trusted and may
// emit any factory event type. Only agent-emitted events are checked against
// the allowed emission set.

// Intercept is called by EventBus.Publish BEFORE fan-out to subscribers.
// Returns: (passthrough bool, error)
// If passthrough=true, EventBus continues normal delivery to subscribers.
// If passthrough=false, the coordinator handled it — skip subscriber delivery.
func (pc *PipelineCoordinator) Intercept(event Event) (bool, error) {
    switch {
    // === Scan Campaign ===
    case event.Type == "system.directive":
        return pc.handleDirective(event)      // may handle or pass through
    case event.Type == "scan.requested":
        return pc.handleScanRequested(event)  // delegate to sub-agents, true (also deliver to sub-agent)
    case event.Type == "scan.completed":
        return false, pc.handleScanCompleted(event)  // consume: cycle to next mode

    // === Discovery Accumulation ===
    case event.Type == "category.assessed" ||
         event.Type == "trend.identified" ||
         event.Type == "source.scraped":
        return false, pc.handleDiscoveryReport(event)  // consume: accumulate, maybe emit vertical.discovered
    case strings.HasSuffix(string(event.Type), ".scan_complete") &&
         (strings.HasPrefix(string(event.Type), "market_research.") ||
          strings.HasPrefix(string(event.Type), "trend_research.") ||
          strings.HasPrefix(string(event.Type), "scanner.")):
        return false, pc.handleScanCompletion(event)    // consume: increment agents_complete, maybe emit scan.completed
    case event.Type == "dedup.resolved":
        return false, pc.handleDedupResolved(event)    // consume: merge or keep both, update verticals table
    case event.Type == "synthesis.resolved":
        return false, pc.handleSynthesisResolved(event) // consume: use resolved assessment for accumulation

    // === Scoring Pipeline (§4.2.2.8) — MOVED TO ScoringNode (v2.0.37) ===
    // vertical.discovered and score.dimension_complete are now handled by the
    // ScoringNode system node (§4.2.2.10) via normal EventBus subscription.
    // These events pass through the interceptor to reach the ScoringNode subscriber.
    // See RFC-001 v2 for architectural rationale (deferred event chaining bug).

    // === Validation Pipeline ===
    case event.Type == "vertical.shortlisted":
        return false, pc.handleShortlisted(event)      // consume: create pipeline, emit validation.started + brand.requested
    case event.Type == "research.completed":
        return false, pc.handleResearchCompleted(event) // consume: set G1, emit spec.requested
    case event.Type == "research.vertical_rejected":
        return false, pc.handleResearchRejected(event)  // consume: set rejected, emit vertical.killed
    case event.Type == "spec.approved":
        return false, pc.handleSpecApproved(event)      // consume: set G2, check gates
    case event.Type == "spec.revision_needed" && event.VerticalID != uuid.Nil:
        return pc.handleInnerSpecRevision(event)        // track inner loop count; passthrough if under limit
    case event.Type == "spec.validation_passed":
        if event.VerticalID != uuid.Nil {
            return false, pc.handleSpecValidated(event)     // pipeline spec: consume, emit cto.spec_review_requested
        }
        return true, nil                                    // template spec: pass through to Factory CTO
    case event.Type == "spec.validation_failed":
        if event.VerticalID != uuid.Nil {
            return false, pc.handleSpecValidationFailed(event) // pipeline spec: check severity, maybe reset G2
        }
        return true, nil                                    // template spec: pass through to Factory CTO
    case event.Type == "cto.spec_approved":
        return false, pc.handleCTOApproved(event)       // consume: set G3, check gates
    case event.Type == "cto.spec_revision_needed":
        return false, pc.handleCTORevision(event)       // consume: reset G2+G3, emit spec.revision_requested
    case event.Type == "cto.spec_vetoed":
        return false, pc.handleCTOVetoed(event)         // consume: set rejected, emit vertical.killed
    case event.Type == "brand.candidates_ready":
        return false, pc.handleBrandReady(event)        // consume: set G4, check gates
    case event.Type == "vertical.needs_more_data":
        return false, pc.handleMoreData(event)          // consume: reopen pipeline, reset G1
    case event.Type == "brand.revision_needed":
        return false, pc.handleBrandRevision(event)     // consume: reopen pipeline, reset G4
    case event.Type == "vertical.resumed" && event.VerticalID != uuid.Nil:
        return false, pc.handleResumed(event)            // consume: reactivate parked vertical
    case event.Type == "vertical.ready_for_review" && event.VerticalID != uuid.Nil:
        pc.setPackaged(event.VerticalID)                 // status update: mark packaged
        return true, nil                                  // pass through to event log
    case event.Type == "vertical.killed" && event.VerticalID != uuid.Nil:
        pc.cleanupPipeline(event.VerticalID)             // cleanup: mark rejected, but pass through
        return true, nil                                  // pass through to Empire Coordinator for digest
    case event.Type == "vertical.approved" && event.VerticalID != uuid.Nil:
        pc.finalizePipeline(event.VerticalID)            // cleanup: set Status=approved (terminal)
        return true, nil                                  // pass through to Empire Coordinator for spinup

    // === Marginal tracking ===
    case event.Type == "vertical.marginal":
        pc.trackMarginal(event)                           // record in marginals table for timer-based re-evaluation
        return true, nil                                  // pass through to Empire Coordinator for judgment

    // === Budget enforcement ===
    case event.Type == "budget.threshold_crossed":
        pc.handleBudgetThreshold(event)                   // pause campaigns at 90%+
        return true, nil                                  // also pass through to Empire Coordinator

    // === Mailbox backpressure ===
    case event.Type == "mailbox.item_decided":
        pc.handleMailboxDecided(event)                    // check paused campaigns for resume
        return true, nil                                  // pass through normally

    // === Everything else passes through ===
    default:
        return true, nil
    }
}

// handleScanRequested delegates to the correct sub-agents based on mode.
// Also creates campaign if one doesn't exist (complex directive path from EC).
func (pc *PipelineCoordinator) handleScanRequested(event Event) (bool, error) {
    mode := event.Payload["mode"].(string)
    geo := event.Payload["geography"].(string)

    // If this scan.requested came from Empire Coordinator (complex directive),
    // it may include campaign_context. Create campaign if none exists.
    if ctx, ok := event.Payload["campaign_context"]; ok {
        campaignCtx := ctx.(map[string]interface{})
        geoID := pc.getOrCreateGeography(geo)
        if pc.findActiveCampaign(geoID) == nil {
            pc.campaigns[event.ID] = &Campaign{
                ID:          uuid.New(),
                GeographyID: geoID,
                DirectiveID: uuid.MustParse(campaignCtx["directive_id"].(string)),
                Modes:       campaignCtx["modes"].([]string),
                CurrentMode: 0,
                Status:      "active",
                TimeoutAt:   time.Now().Add(2 * time.Hour),
            }
        }
    }

    // Create scan accumulator
    pc.scans[event.ID] = &ScanAccumulator{
        ScanID:   event.ID,
        Mode:     mode,
        Expected: expectedAgentsPerMode[mode],
        Reports:  []json.RawMessage{},
        TimeoutAt: time.Now().Add(90 * time.Minute),
    }

    // Delegate based on mode
    switch mode {
    case "saas_gap":
        pc.bus.Publish(Event{Type: "market_research.scan_assigned",
            Payload: map[string]interface{}{"geography": geo, "scan_id": event.ID}})
        // v2.0.40: MRA outputs multi-signal per subcategory with structured evidence.
        // Runtime pre-filter applied before emitting vertical.discovered.
        // automation_micro nested field eliminated — all signals are first-class.
    case "saas_trend":
        pc.bus.Publish(Event{Type: "trend_research.scan_assigned",
            Payload: map[string]interface{}{"geography": geo, "scan_id": event.ID}})
    case "local_services":
        for _, scanner := range []string{"google_maps", "instagram", "reviews", "directories", "job_boards"} {
            pc.bus.Publish(Event{Type: fmt.Sprintf("scanner.%s.scan_assigned", scanner),
                Payload: map[string]interface{}{"geography": geo, "scan_id": event.ID}})
        }
    case "corpus":
        // v2.1.0: Corpus mode — read JSONL file, batch into 25-record chunks, dispatch to MRA
        corpusPath := event.Payload["corpus_path"].(string)
        records := readJSONLFile(corpusPath)  // []json.RawMessage
        batchSize := 25
        for i := 0; i < len(records); i += batchSize {
            end := i + batchSize
            if end > len(records) { end = len(records) }
            pc.bus.Publish(Event{Type: "market_research.scan_assigned",
                Payload: map[string]interface{}{
                    "geography": geo,
                    "scan_id":   event.ID,
                    "mode":      "corpus",
                    "corpus_signals": records[i:end],
                }})
        }
        // Note: MRA receives multiple scan_assigned events (one per batch).
        // MRA emits category.assessed per viable signal found in each batch.
        // MRA emits market_research.scan_complete once after processing all batches.
        // The expectedAgentsPerMode["corpus"] = 1, so one scan_complete suffices.
    }
    return false, nil  // consumed — don't deliver to any subscriber
}

// checkGates evaluates whether all validation gates are met.
// If yes, bundles payloads and invokes Validation Coordinator for packaging.
func (pc *PipelineCoordinator) checkGates(vp *ValidationPipeline) error {
    if vp.G1_Research && vp.G2_Spec && vp.G3_CTO && vp.G4_Brand {
        // All gates met — invoke Validation Coordinator for packaging
        pc.bus.Publish(Event{
            Type:       "validation.package_ready",
            VerticalID: vp.VerticalID,
            Payload: map[string]interface{}{
                "research":  vp.ResearchPayload,
                "spec":      vp.SpecPayload,
                "cto_notes": vp.CTOPayload,
                "brand":     vp.BrandPayload,
            },
        })
        vp.Status = "packaged"
    }
    return nil
}
```

**Key architectural point:** Events that the pipeline coordinator intercepts are **consumed** — they don't reach agent subscribers. The coordinator processes them through state machines and may emit new events that DO reach subscribers. For example:
- `research.completed` is consumed by the coordinator → sets G1, stores payload. The Business Research Agent independently emits `spec.requested` (passthrough) which reaches the Lightweight Spec Agent.
- `vertical.shortlisted` is consumed → creates pipeline → emits `validation.started` (reaches Business Research Agent) and `brand.requested` (reaches Pre-Brand Agent)
- `category.assessed` is consumed → accumulated → may emit `vertical.discovered` (delivered to ScoringNode §4.2.2.8 → rubric selection → `scoring.requested` to Analysis Agent)
- `dedup.resolved` is consumed → runtime either emits `vertical.discovered` (keep both) or merges into existing vertical

**Vertical ID injection (CRITICAL for concurrent verticals):**

The interceptor must look up the correct ValidationPipeline for each event. Events like `research.completed`, `spec.approved`, and `cto.spec_approved` must carry `vertical_id` so the runtime can attribute them.

The runtime handles this at the **agent invocation level**, not in agent prompts. When the runtime delivers an event to an agent (e.g., `validation.started` to BRA), it records the `vertical_id` associated with that invocation. All events emitted by the agent during that invocation are automatically tagged with the same `vertical_id` by the runtime's emission layer — the agent doesn't need to explicitly propagate it.

```go
// AgentInvocation tracks the context for a single agent task
type AgentInvocation struct {
    AgentID    string
    VerticalID uuid.UUID    // Set by runtime when delivering event to agent
    ScanID     uuid.UUID    // Set by runtime for discovery sub-agents
    // ... other context
}

// EmitEvent wraps agent emissions with invocation context
func (inv *AgentInvocation) EmitEvent(event Event) Event {
    if inv.VerticalID != uuid.Nil && event.VerticalID == uuid.Nil {
        event.VerticalID = inv.VerticalID  // Auto-inject
    }
    if inv.ScanID != uuid.Nil {
        if event.Payload == nil {
            event.Payload = map[string]interface{}{}
        }
        if _, exists := event.Payload["scan_id"]; !exists {
            event.Payload["scan_id"] = inv.ScanID  // Auto-inject
        }
    }
    return event
}
```

This means agents don't need "ALWAYS propagate vertical_id" instructions in their prompts. The runtime ensures context propagation regardless of agent behavior. However, discovery agents (MRA, TRA) SHOULD still include `scan_id` in their prompts as a defense-in-depth measure — the runtime injection is a safety net, not a replacement for correct agent behavior.

Events the coordinator does NOT intercept pass through normally to subscribers: `vertical.scored` (shortlisted only — rejections go to `scoring_digest_buffer`, not to EC) → Empire Coordinator, `opco.ceo_report` → Empire Coordinator, `spec.requested` → Business Research Agent → Lightweight Spec Agent, etc.

**EventBus integration:**

```go
func (eb *EventBus) Publish(event Event) error {
    // 1. Begin transaction
    tx, _ := eb.db.Begin()

    // 2. Persist to events table (within tx)
    eb.persistEvent(tx, event)

    // 3. Run through pipeline coordinator middleware (within tx)
    //    Handler reads/writes pipeline state tables within same tx.
    //    Handler queues events to emit (deferred) rather than publishing directly.
    passthrough, deferredEvents, err := eb.coordinator.Intercept(tx, event)
    if err != nil {
        tx.Rollback()  // Atomic: event not persisted, state not changed
        return err
    }

    // 4. Write pipeline_receipts for this event (within tx)
    //    Enables crash recovery: replay unreceipted events through interceptor.
    if !passthrough {
        eb.writePipelineReceipt(tx, event.ID, "processed")
    }

    // 5. Persist deferred events (within tx)
    //    Each deferred event is inserted into events table within the same tx.
    //    This ensures: if the tx commits, ALL downstream events are persisted.
    for _, deferred := range deferredEvents {
        eb.persistEvent(tx, deferred)
    }

    // 6. Commit transaction
    //    Atomic: original event + state changes + deferred events all committed together.
    //    If commit fails, nothing is persisted.
    tx.Commit()

    // 7. Fan out deferred events (post-commit, best-effort)
    //    These events are already persisted. If fan-out fails (process crash),
    //    crash recovery will find them in events table without receipts and re-deliver.
    for _, deferred := range deferredEvents {
        eb.publishInternal(deferred)  // non-transactional fan-out
    }

    // 8. Fan out original event to subscribers (if passthrough)
    if passthrough {
        if eb.isFactoryEvent(event.Type) {
            return eb.fanOutFactory(event)
        }
        return eb.fanOutOpCo(event)
    }
    return nil
}
```

**Deferred event limitation (v2.0.37):** Events produced by the interceptor (step 5: deferred events) are persisted and delivered via `publishInternal` (step 7), but they do NOT re-enter `runInterceptors`. This is by design to prevent infinite recursion, but it means interceptor-produced events cannot trigger further interceptor logic. The scoring pipeline was broken by this limitation: `vertical.discovered` produced as a deferred event from discovery accumulation never triggered the scoring interceptor case. As of v2.0.37, the scoring pipeline is handled by the ScoringNode system node (§4.2.2.8, §4.2.2.10), which receives events via normal fan-out and publishes via normal `Publish` — eliminating the chaining problem for this flow. Remaining interceptor cases will be migrated to system nodes in future phases (see RFC-001 v2).

**Crash recovery for pipeline coordinator:**

On startup, the pipeline coordinator replays unprocessed events:

```go
func (pc *PipelineCoordinator) RecoverFromCrash() error {
    // 1. Reload all state from DB (campaigns, validations, scans)
    pc.loadStateFromDB()

    // 2. Find events that were persisted but not receipt-ed by pipeline coordinator
    //    These are events the interceptor should have processed but didn't
    //    (e.g., process crashed between persist and handler completion)
    rows, _ := pc.db.Query(`
        SELECT e.* FROM events e
        WHERE e.type = ANY($1)                    -- intercepted event types
          AND NOT EXISTS (
            SELECT 1 FROM pipeline_receipts r
            WHERE r.event_id = e.id
          )
        ORDER BY e.created_at ASC
    `, interceptedEventTypes)

    // 3. Replay each through Intercept()
    //    Idempotency ensures double-processing is safe
    for rows.Next() {
        event := scanEvent(rows)
        pc.Intercept(nil, event)  // nil tx = non-transactional replay
    }

    // 4. Find deferred events that were persisted but not delivered
    //    (committed in tx but process crashed before fan-out)
    //    Normal crash recovery handles this: events in events table
    //    without receipts from their target subscribers get re-delivered.
    return nil
}
```

**New event types introduced by the pipeline coordinator:**

| Event | Source | Consumer | Purpose |
|-------|--------|----------|---------|
| `campaign.completed` | Pipeline Coordinator | Empire Coordinator | All scan modes done for geography |
| `validation.package_ready` | Pipeline Coordinator | Validation Coordinator | All 4 gates met, bundled payloads |
| `validation.started` | Pipeline Coordinator | Business Research Agent | Vertical entered validation |
| `validation.more_data_needed` | Pipeline Coordinator | Business Research Agent | Human asked questions |
| `dedup.ambiguous` | Pipeline Coordinator | Discovery Coordinator | dedup_id, new_candidate, existing_vertical, similarity — fuzzy name match needs judgment |
| `synthesis.needed` | Pipeline Coordinator | Discovery Coordinator | Conflicting reports need resolution |
| `market_research.scan_assigned` | Pipeline Coordinator | Market Research Agent | Start taxonomy scan |
| `market_research.scan_complete` | Market Research Agent | Pipeline Coordinator | Taxonomy scan finished |
| `trend_research.scan_assigned` | Pipeline Coordinator | Trend Research Agent | Start trend scan |
| `trend_research.scan_complete` | Trend Research Agent | Pipeline Coordinator | Trend scan finished |
| `scanner.{type}.scan_assigned` | Pipeline Coordinator | Scanner Agents | Start source scan |
| `scanner.{type}.scan_complete` | Scanner Agents | Pipeline Coordinator | Source scan finished |
| `brand.revision_needed` | Pipeline Coordinator | Pre-Brand Agent | Human rejected brand candidates |

#### 4.2.2.1 Scan Campaign State Machine

A human directive creates a **campaign** — a sequence of scans that exhaustively covers the opportunity space for a geography. The runtime manages campaign lifecycle; no LLM is involved in scan sequencing.

```go
type Campaign struct {
    ID              uuid.UUID
    GeographyID     uuid.UUID
    DirectiveID     uuid.UUID        // The system.directive event that created this
    Modes           []string         // Ordered: ["saas_gap", "saas_trend", "local_services"]
    CurrentMode     int              // Index into Modes
    CurrentScanID   uuid.UUID        // ID of the currently active scan (for accumulator lookup)
    Status          string           // active | paused | completed
    CreatedAt       time.Time
    DeadlineAt      time.Time        // Global campaign deadline (24h from creation)
}

// Default cycle order. If directive specifies a single mode, Modes has one entry.
// saas_gap: MRA outputs multi-signal per subcategory (v2.0.40)
// saas_gap runs first since it produces both SaaS and automation-micro candidates.
var defaultModes = []string{"saas_gap", "saas_trend", "local_services"}

// Corpus mode is single-mode — no cycling to saas_trend or local_services.
// Corpus signals are pre-collected; trend scanning and local scraping are irrelevant.
// Campaign modes for corpus directive: ["corpus"] only.
```

**Campaign-to-scan linkage:** When the runtime emits `scan.requested`, it includes `campaign_id` in the payload and stores the scan ID in Campaign.CurrentScanID. The ScanAccumulator stores `campaign_id`. When the accumulator emits `scan.completed`, it propagates `campaign_id`. This ensures `handleScanCompleted` can always find the correct campaign.

**Timeout strategy:** Per-scan timeouts are handled by the ScanAccumulator (90 minutes). The campaign has a single global deadline (24 hours) as a safety net. There is NO per-mode campaign timeout — that would race with the accumulator timeout and cause double mode-advancement.

**State transitions:**

```
system.directive (human)
  → Runtime parses directive: extract geography, mode preference, strategic context
  → Check: active campaign exists for this geography?
    → Yes, same modes: ignore (duplicate directive)
    → Yes, different modes: queue new campaign, notify Empire Coordinator
    → No: create campaign
  → If mode specified: Campaign{Modes: [specified_mode]}
  → If no mode specified: Campaign{Modes: defaultModes}
  → Store strategic context (budget, focus, exclusions) in campaign record
    for Empire Coordinator to reference in digest/marginal decisions
  → Runtime creates geography record if needed
  → Runtime emits scan.requested{mode: Modes[0], geography: geo, campaign_id: ID}
  → Campaign.Status = active, CurrentMode = 0
  → Campaign.DeadlineAt = now + 24 hours

scan.completed
  → Runtime looks up campaign by campaign_id in payload
  → If no campaign found: log warning, no-op (manual or orphaned scan)
  → CurrentMode++
  → If CurrentMode < len(Modes):
      → Check backpressure: count mailbox WHERE status='pending'
        AND type='vertical_approval'
      → If pending >= 5: Campaign.Status = paused, log reason
      → Else: emit scan.requested{mode: Modes[CurrentMode], campaign_id: ID}
  → If CurrentMode >= len(Modes):
      → Campaign.Status = completed
      → Emit campaign.completed to Empire Coordinator (for digest)

mailbox item decided (any status change from 'pending')
  → Check for paused campaigns
  → Re-count pending mailbox items WHERE type='vertical_approval'
  → If pending < 5 and paused campaign exists: resume
      → Campaign.Status = active
      → Emit scan.requested{mode: Modes[CurrentMode], campaign_id: ID}

timer: campaign_deadline (checked every 30 minutes)
  → For each campaign WHERE Status=active AND now > DeadlineAt:
      → Campaign.Status = completed
      → Log: "Campaign deadline reached. Completed {CurrentMode}/{len(Modes)} modes."
      → Emit campaign.completed with partial=true flag
      → Cancel current scan accumulator if still active

budget.threshold_crossed (90% or higher)
  → Pause all active campaigns: Campaign.Status = paused
  → Log: "Campaigns paused due to budget threshold"
  → Note: EC still handles OpCo throttling via agent_message

timer: marginal_review (every 14 days)
  → For each parked marginal in marginals table:
    → If parked > 60 days with no new signals: emit vertical.killed
    → Else: inject into timer.marginal_review event payload for EC
  → EC receives timer.marginal_review with all active marginals,
    decides which to promote (emit vertical.shortlisted),
    which to keep parked, which to kill.
  → Runtime tracks: marginal_id, vertical_id, scored_at, parked_at,
    last_reviewed_at, decision (parked|promoted|killed)
```

The Empire Coordinator never sees `scan.requested` or `scan.completed` for cycling purposes. It receives `campaign.completed` when all modes are done, and uses it for digest compilation. The strategic context from the directive is stored in the campaign record and injected into `campaign.completed` so the Empire Coordinator can reference budget constraints, focus areas, and exclusions when compiling digests and making marginal decisions.

#### 4.2.2.2 Validation Pipeline State Machine

Each vertical entering validation gets a **pipeline state** tracked by the runtime. The Validation Coordinator agent is invoked only for packaging — assembling the validation kit and writing the human-readable summary.

```go
type ValidationPipeline struct {
    VerticalID       uuid.UUID
    Status           string    // active | rejected | packaged | parked | approved
    G1_Research      bool      // research.completed received
    G2_Spec  bool      // spec.approved received
    G3_CTO   bool      // cto.spec_approved received
    G4_Brand bool      // brand.candidates_ready received
    ResearchPayload  json.RawMessage
    SpecPayload      json.RawMessage
    CTOPayload       json.RawMessage
    BrandPayload     json.RawMessage
    ScoringPayload   json.RawMessage   // Scoring results carried from discovery
    RevisionCount    int       // CTO/Auditor revision cycles (max 3)
    InnerRevisionCount int     // BRA↔LSA↔Reviewer revision cycles (max 5)
    SpecVersion      int       // Incremented on each G2 reset; prevents stale CTO reviews
    PackagingRequested bool    // Set when validation.package_ready emitted
    PackagingRequestedAt *time.Time // Timestamp for timeout detection
    PackagingRetries int       // Number of packaging retry attempts
    CreatedAt        time.Time
    UpdatedAt        time.Time
}

const maxRevisionCycles = 3       // After 3 CTO/Auditor rejections, escalate to mailbox
const maxInnerRevisions = 5       // After 5 BRA↔LSA↔Reviewer cycles, escalate
const packagingTimeout = 30 * time.Minute  // If VC doesn't respond, retry or escalate
```

**State transitions:**

```
vertical.shortlisted (from runtime computeComposite)
  → Runtime creates ValidationPipeline{VerticalID, Status: active}
  → Runtime emits validation.started (triggers Business Research Agent)
  → Runtime emits brand.requested (triggers Pre-Brand Agent — parallel)

research.completed
  → Guard: Status must be active. If rejected/packaged → drop.
  → If G1 was previously true (this is a more-data response):
      Merge payload: append new findings to existing ResearchPayload
      rather than overwriting. The original research is still valid;
      the human just wanted supplementary info.
  → Else (initial research): store payload normally.
  → Runtime sets G1=true
  → Note: Business Research Agent independently emits spec.requested
    to Lightweight Spec Agent as part of its research flow.
    The runtime does NOT emit spec.requested — that's the BRA's job.

spec.revision_needed (from BRA, internal spec loop)
  → If vertical_id present: runtime intercepts to track inner loop.
  → Increment InnerRevisionCount
  → If InnerRevisionCount > maxInnerRevisions (5):
      → Runtime creates mailbox item: "Spec creation stuck in revision
        loop after 5 cycles between BRA, Spec Agent, and Reviewer.
        Human decision needed: kill vertical or provide spec guidance."
      → Runtime sets Status=parked
      → STOP — don't deliver to Lightweight Spec Agent
  → Else: passthrough to Lightweight Spec Agent (normal revision flow)
  → Note: InnerRevisionCount is reset to 0 when spec.approved is received
    (successful spec exits the inner loop). It is also reset when
    spec.revision_requested arrives from the CTO/Auditor path
    (new outer revision = fresh inner loop).

research.vertical_rejected
  → Runtime sets Status=rejected
  → Runtime emits vertical.killed with rejection evidence
  → All subsequent events for this vertical_id are dropped

spec.approved (from Spec Reviewer)
  → Guard: Status must be active. If rejected/packaged → drop.
  → Runtime sets G2=true, stores payload, increments SpecVersion
  → Runtime resets InnerRevisionCount=0 (spec exited inner loop successfully)
  → Runtime emits spec.validation_requested to Spec Auditor
     (payload: vertical_id, spec_content, spec_tier)
  → Check: all gates met? → if yes, invoke packaging (see below)

spec.validation_passed (from Spec Auditor)
  → Guard: vertical_id in payload must reference an active vertical in scoring/researching stage.
    If stale (spec was revised since this validation started), drop.
  → Runtime emits cto.spec_review_requested to Factory CTO
     (payload: vertical_id, mvp_spec, business_brief, vertical_context)

spec.validation_failed (from Spec Auditor)
  → Guard: spec_version must match. If stale, drop.
  → Check severity in payload:
    → If status=blocker: spec fundamentally incomplete.
       Reset G2=false AND G3=false (revised spec needs both
       Auditor re-validation AND CTO re-review).
       Clear SpecPayload, clear CTOPayload.
       Increment RevisionCount (same counter as CTO revisions —
       max 3 total revisions regardless of source).
       If RevisionCount > maxRevisionCycles (3):
         → Park and escalate to mailbox (same as CTO revision loop).
       Else:
         Emit spec.revision_requested to Business Research Agent
         with auditor's issue list as feedback.
         (Follows same revision flow as CTO revision — Spec Reviewer
         re-approval required before re-validation, then CTO re-review.)
    → If status=non-blocker (medium issues only, verdict=GO):
       Treat as spec.validation_passed — proceed to CTO review.
       Include auditor notes in cto.spec_review_requested payload
       so CTO is aware of medium issues.

cto.spec_approved
  → Guard: Status must be active → drop otherwise.
  → Guard: vertical_id must reference active vertical with current spec revision.
    If stale (spec was revised after CTO started reviewing), drop.
    The CTO will receive a new cto.spec_review_requested with
    the current version.
  → Runtime sets G3=true, stores payload
  → Check: all gates met? → if yes, invoke packaging

cto.spec_revision_needed
  → Runtime sets G3=false (reset CTO gate)
  → Runtime sets G2=false (reset spec gate — revised spec needs re-approval)
  → Runtime clears SpecPayload (force fresh data)
  → Runtime increments RevisionCount
  → If RevisionCount > maxRevisionCycles (3):
      → Runtime creates mailbox item: "Vertical stuck in revision loop
        after 3 CTO rejections. Human decision needed: kill or override."
      → Runtime sets Status=parked
      → STOP — no further automated processing
  → Else: Runtime emits spec.revision_requested to Business Research Agent
     with CTO feedback in payload
  → Business Research Agent revises → Lightweight Spec Agent rewrites
     → Spec Reviewer re-approves → spec.approved resets G2
     → Spec Auditor re-validates → CTO re-reviews → cto.spec_approved resets G3

cto.spec_vetoed
  → Runtime sets Status=rejected
  → Runtime emits vertical.killed with veto reason

brand.candidates_ready
  → Guard: Status must be active. If rejected/packaged → drop.
  → Runtime sets G4=true, stores LATEST payload (overwrites prior)
  → Check: all gates met? → if yes, invoke packaging

All gates met (G1 ∧ G2 ∧ G3 ∧ G4):
  → Runtime sets PackagingRequestedAt = now
  → Runtime invokes Validation Coordinator agent ONE TIME with
     all four payloads bundled:
     {research: ..., spec: ..., cto_notes: ..., brand: ...}
  → Agent's only job: assemble validation kit, write summary,
     submit to mailbox via mailbox_send, emit vertical.ready_for_review
  → No further events processed for this vertical_id unless
     status is reopened (see below)

vertical.ready_for_review (from Validation Coordinator)
  → Runtime sets Status=packaged, clears PackagingRequestedAt
  → Passthrough to event log (no agent subscribes — purely audit trail)

timer: packaging_timeout (checked every 10 minutes)
  → For each ValidationPipeline WHERE PackagingRequestedAt != nil
    AND now > PackagingRequestedAt + packagingTimeout (30min):
      → First retry: re-emit validation.package_ready (VC may have failed)
      → If already retried: escalate to mailbox ("Packaging failed for
        vertical X. All gates met but VC didn't respond. Human intervention
        needed.") Set Status=parked.

vertical.needs_more_data (from mailbox — human asked questions)
  → Guard: Status must be packaged (only packaged verticals are in mailbox)
  → Runtime sets Status=active
  → Runtime sets G1=false (research gate reopened — targeted research needed)
  → Runtime preserves G2, G3, G4 (spec/CTO/brand still valid unless
     research changes invalidate them)
  → Runtime emits validation.more_data_needed to Business Research Agent
     with human's questions in payload
  → Business Research Agent does targeted research → research.completed
     resets G1 → all gates check triggers re-packaging
  → Set timeout: 14 days. If no research.completed by then,
     runtime sets Status=parked, notifies Empire Coordinator

brand.revision_needed (from mailbox — human rejected brand candidates)
  → Guard: Status must be packaged
  → Runtime sets Status=active
  → Runtime sets G4=false (brand gate reopened)
  → Runtime clears BrandPayload
  → Runtime preserves G1, G2, G3 (research/spec/CTO still valid)
  → Runtime emits brand.revision_needed to Pre-Brand Agent
     with human's feedback in payload
  → Pre-Brand Agent regenerates → brand.candidates_ready
     resets G4 → all gates check triggers re-packaging

vertical.resumed (from mailbox — human decides to resume parked vertical)
  → Guard: Status must be parked.
  → Runtime sets Status=active
  → Runtime resets RevisionCount=0 (human override clears the revision loop)
  → Runtime checks which gates are incomplete:
    - If G1=false: emit validation.started (re-triggers research)
    - If G2=false: emit spec.revision_requested with human's guidance
    - If G3=false: emit cto.spec_review_requested (re-submit to CTO)
    - If all gates true: emit validation.package_ready (re-package)
  → Human's mailbox message may include guidance ("override CTO objection",
    "spec is fine, just approve") which is included in the emitted event payload.
```

The Validation Coordinator agent goes from 30+ turns of gate tracking to **1 turn of packaging**. The state machine handles all the waiting, gate checking, revision routing, and rejection handling.

#### 4.2.2.3 Discovery Accumulation

The runtime handles report accumulation and threshold filtering. Sub-agents emit individual reports (one per subcategory/source) and a completion signal when done.

```go
type ScanAccumulator struct {
    ScanID          string            // TEXT PK, not UUID
    CampaignID      string            // TEXT, references scan_campaigns
    Mode            string
    Geography       string
    Expected        int               // total expected agent completions
    Complete        int               // agents that have reported completion
    CompletedBy     map[string]any    // JSONB object keyed by agent_id → metadata
    Reports         int               // count of reports received (not array)
    Discovered      int               // verticals discovered in this scan
    Skipped         int               // verticals skipped (below threshold)
    PendingDedup    int               // count of candidates in pending_dedup_candidates
    TimeoutAt       time.Time
}

type PendingCandidate struct {
    DedupEventID string            // TEXT PK — the dedup.ambiguous event ID
    Name         string            // candidate vertical name
    SignalStrength int
    Payload      json.RawMessage   // raw discovery payload
    ExistingID   string            // existing vertical it matched against
}

// Expected sub-agent counts per mode
var expectedAgentsPerMode = map[string]int{
    "automation_micro": 1,   // NOT a standalone mode — kept for backward compat if runtime receives it
    "saas_gap":         1,   // Market Research Agent
    "saas_trend":       1,   // Trend Research Agent
    "local_services":   5,   // Google Maps, Instagram, Reviews, Directories, Job Boards
    "corpus":           1,   // Market Research Agent (corpus interpretation variant)
}

// Completion signal event types per mode
var completionSignals = map[string][]string{
    "saas_gap":       {"market_research.scan_complete"},
    "saas_trend":     {"trend_research.scan_complete"},
    "corpus":         {"market_research.scan_complete"},
    "local_services": {"scanner.google_maps.scan_complete", "scanner.instagram.scan_complete",
                       "scanner.reviews.scan_complete", "scanner.directories.scan_complete",
                       "scanner.job_boards.scan_complete"},
}
```

```
scan.requested
  → Runtime tracks: {scan_id, mode, geography,
     expected_agents: expectedAgentsPerMode[mode],
     agents_complete: 0,
     reports: [], timeout_at: now + 90 minutes}

category.assessed / trend.identified / source.scraped
  → Runtime appends to reports[]
  → For category.assessed events (v2.0.40 multi-signal model):
    → Each event is one opportunity signal (not one subcategory)
    → Runtime applies pre-filter cascade:
      1. Apply red_flag penalties: complex_integration → signal -20,
         high_feature_count → signal -20 (or block if co-occurs with complex_integration/multi-module)
      2. signal_strength < 55 (after penalties) → skip, log as low-signal audit entry (v2.1.0: raised from 50)
      3. Blocking red flags present → skip, log with flag type.
         Blocking flags (v2.1.0): complex_integration AND multi-module (co-occurrence),
         phone_led_sales (ICP requires phone-based selling),
         enterprise_procurement (buyer requires RFP/committee approval),
         relationship_networking (value prop depends on personal relationships),
         physical_presence_required (duties require on-site human),
         support_mode_phone_video (product requires phone or video support)
      4. ICP positive check fails (no role/cohort token + workflow anchor + buyer community URL) → skip, log
      5. Evidence completeness fails (requires ≥2 independent source URLs: competitor+pricing+URL, community URL, or pain+URL) → skip, log (v2.1.0: raised from 1 to 2 independent URLs)
      6. Retention primitive gate (v2.1.0): opportunity must demonstrate ≥1 of:
         recurring_data (user's data grows over time — invoices, records, history),
         workflow_embedding (tool becomes part of daily/weekly process),
         integration_lock_in (connects to systems user depends on — QuickBooks, MLS, ERP),
         compliance_cadence (regulatory deadlines create forced return visits),
         team_collaboration (multiple users share state within the tool).
         If none present → skip, log as "no_retention_primitive"
      7. Name-based dedup against verticals table → skip if exact match
      8. >70% fuzzy name similarity → hold in pending_dedup queue (unchanged)
      9. All checks pass → emit vertical.discovered with discovery_context:
         {opportunity_name, preliminary_icp, build_sketch, evidence, opportunity_hypothesis,
          opportunity_pattern, signal_sources, required_capabilities,
          red_flags_passthrough: [one_time_setup, accuracy_liability if present]}
    → A single subcategory may produce 0, 1, 2, or 3+ signals
    → Null signals (signal_strength: 0) are logged for audit but never emitted
  → For trend.identified: same pre-filter cascade (v2.0.40)
  → For source.scraped: single assessment as before
    → signal_strength >= 55: emit vertical.discovered (v2.1.0: raised from 50)
    → signal_strength < 55: skip, log as low-signal
  → Note: a single sub-agent (e.g., Market Research Agent) emits
    MULTIPLE reports (one per opportunity signal found). The accumulator
    does NOT use report count to determine completion.
  → Deduplication at discovery stage is NAME-BASED only
    (exact match on normalized vertical name against verticals table).
    Semantic deduplication ("pet grooming" vs "animal care services")
    requires LLM judgment — handled by Discovery Coordinator agent
    when invoked for ambiguous cases (see below).

  → Dedup flow:
    1. Exact match on normalized name → skip (don't emit vertical.discovered)
    2. >70% fuzzy name similarity → HOLD emission in pending_dedup queue.
       Generate dedup_id (UUID). Store in PendingCandidate.DedupEventID.
       Invoke Discovery Coordinator with dedup.ambiguous event
       containing dedup_id in payload.
       Do NOT emit vertical.discovered yet.
    3. <70% similarity and no exact match → emit vertical.discovered

  → On dedup.resolved (from Discovery Coordinator):
    - Match dedup_id from payload to PendingCandidate.DedupEventID in queue
    - action: "keep_both" → emit vertical.discovered for the held candidate
    - action: "merge" → skip emission, update existing vertical record

market_research.scan_complete / trend_research.scan_complete /
scanner.{type}.scan_complete
  → Runtime increments agents_complete for this scan
  → Sub-agents MUST emit their completion signal when finished.
    Market Research Agent emits after walking full taxonomy.
    Scanner agents emit after scraping their source.

agents_complete >= expected_agents OR now > timeout_at:
  → If timeout: log warning with missing agents
  → Runtime emits scan.completed with stats:
    {mode, geography, reports_received, agents_expected,
     agents_complete, verticals_discovered, verticals_skipped,
     pending_dedup: len(PendingDedup), timed_out: bool,
     campaign_id: CampaignID}
  → Note: scan.completed is emitted even if PendingDedup is non-empty.
    Dedup resolution is asynchronous — dedup.resolved events are
    processed independently and may emit vertical.discovered after
    scan.completed. This means some discoveries arrive after the
    scan is "done." Campaign cycling is not blocked by pending dedup.

Ambiguous deduplication trigger:
  → When a new vertical.discovered candidate has >70% name similarity
    to an existing vertical in the same geography (fuzzy string match),
    runtime invokes Discovery Coordinator agent with both records
    for judgment: "Are these the same opportunity? Merge or keep both?"
```

#### 4.2.2.4 Directive Translation

Human directives are the system's input. The runtime handles deterministic extraction; the LLM handles ambiguous directives.

**Directive anatomy (v2.0.40):**

A directive has one required field (geography) and optional constraint/context fields. For a first scan, "US" is sufficient — the system defaults to maximum coverage. Constraint fields become useful for subsequent scans informed by previous results.

```go
type ParsedDirective struct {
    // === Required ===
    Geography       *GeographyConfig

    // === Optional: Targeting ===
    Mode            *string          // nil = all modes (default campaign)
    TaxonomyFocus   []string         // specific categories to scan, nil = full taxonomy
    TaxonomySkip    []string         // categories to skip (e.g. dead zones from previous scan)
    ICPConstraints  []string         // "B2B only", "solo practitioners", "SMBs <50 employees"

    // === Optional: Constraints ===
    PriceRange      *PriceRange      // {Min: 10, Max: 100, Currency: "USD"} — feeds Monetization Clarity
    AvoidSectors    []string         // "healthcare", "financial custody" — feeds red flag pre-filter
    TechConstraints []string         // "stripe-only", "no custom payment integrations"
    BudgetCap       *int             // max spend for this campaign in cents

    // === Optional: Strategic Context ===
    Intent          string           // "first scan, casting wide" | "rescan, drill into high-signal areas"
    KnownPatterns   []string         // pattern names from previous campaigns to hunt for
    DomainPortfolio []string         // owned domains available for branding

    // === Metadata ===
    RawText         string           // original directive text preserved
    ScanContext     *ScanContext      // nil for first scan, populated by runtime for rescans
}

type PriceRange struct {
    Min      int     // monthly price floor in cents
    Max      int     // monthly price ceiling in cents
    Currency string  // "USD" default
}
```

**First scan defaults:** When only geography is provided, the system uses maximum coverage defaults:

| Field | Default |
|-------|---------|
| Mode | all modes: saas_gap → saas_trend → local_services |
| TaxonomyFocus | nil (full 52-subcategory taxonomy) |
| TaxonomySkip | nil (nothing skipped) |
| ICPConstraints | nil (any ICP) |
| PriceRange | nil (rubric evaluates naturally) |
| AvoidSectors | nil (red flag pre-filter handles structural blockers) |
| Intent | "first scan, casting wide" |
| ScanContext | nil (no previous results) |

This means `empire directive "US"` is a valid, complete directive that triggers a full-coverage campaign.

**How constraints flow through the pipeline:**

| Constraint | Where it's used | How |
|------------|----------------|-----|
| TaxonomyFocus/Skip | MRA prompt context | MRA receives category list to scan or skip |
| ICPConstraints | MRA prompt context | MRA filters opportunities against ICP constraints |
| PriceRange | Runtime pre-filter | Additional check on MRA's build_sketch: if estimated pricing outside range, reduce signal_strength by 15 |
| AvoidSectors | Runtime pre-filter | Maps to red flag blocking: "healthcare" → block signals touching medical data, "financial custody" → block signals with funds_custody flag |
| TechConstraints | Analysis Agent context | Passed through discovery_context, influences Build Complexity scoring |
| KnownPatterns | MRA prompt context | "In addition to taxonomy walk, specifically hunt for: [pattern descriptions]" |
| Intent | EC digest context | EC uses intent for marginal decisions and campaign.completed analysis |
| BudgetCap | Runtime campaign manager | Pauses campaign when cumulative spend approaches cap |

**Directive parser:**

```go
type DirectiveParser struct {
    geoPatterns    map[string]GeographyConfig
    modePatterns   map[string]string
    sectorPatterns map[string][]string  // "healthcare" → ["medical", "health", "clinical", "patient"]
}

func (dp *DirectiveParser) Parse(text string) (*ParsedDirective, bool) {
    // Try deterministic extraction first
    geo := dp.extractGeography(text)
    mode := dp.extractMode(text)
    priceRange := dp.extractPriceRange(text)       // "$10-50/mo", "under $100"
    avoidSectors := dp.extractAvoidSectors(text)   // "avoid healthcare", "no fintech"
    taxonomyFocus := dp.extractTaxonomyFocus(text)  // "focus on financial_ops"
    taxonomySkip := dp.extractTaxonomySkip(text)    // "skip workforce_hr"
    domains := dp.extractDomains(text)              // any *.com, *.io, etc.
    budget := dp.extractBudget(text)                // "$200 max", "budget: $150"
    context := dp.extractResidual(text, geo, mode)  // Everything else → strategic context

    if geo != nil {
        return &ParsedDirective{
            Geography:       geo,
            Mode:            mode,
            TaxonomyFocus:   taxonomyFocus,
            TaxonomySkip:    taxonomySkip,
            PriceRange:      priceRange,
            AvoidSectors:    avoidSectors,
            DomainPortfolio: domains,
            BudgetCap:       budget,
            Intent:          dp.inferIntent(text),
            RawText:         text,
        }, true  // deterministic
    }
    return nil, false  // ambiguous — route to Empire Coordinator for interpretation
}
```

Simple directives ("US", "SaaS in US", "US, focus on financial_ops, avoid healthcare, budget $200") are handled entirely by the runtime. Complex directives ("Find the US equivalent of Paraguay's SIFEN opportunity") are routed to the Empire Coordinator for interpretation.

**Geographic scope tagging (v2.0.40):** When the MRA produces an opportunity signal, it includes a `geographic_scope` field:

```
geographic_scope: "global"    # ICP exists worldwide, distribution is platform/SEO-based
geographic_scope: "regional"  # ICP exists across multiple countries in a region
geographic_scope: "local"     # ICP specific to this country (regulatory, language, local infra)
```

This is a lightweight MRA assessment (zero extra research — the MRA already knows if the opportunity depends on a local regulation or a global workflow). The tag flows through to `vertical.discovered` and influences EC portfolio decisions: global opportunities don't need geography-specific rescans; local opportunities generate pattern signatures for cross-geography transfer.

Strategic context (budget constraints, focus areas, exclusions, domain preferences, scan context) is preserved in the campaign record regardless of parsing path. The Empire Coordinator receives this context in `campaign.completed` and in its digest data.

#### 4.2.2.5 What Stays in LLM Agents

After the runtime handles deterministic coordination, the remaining LLM agent responsibilities are:

| Agent | Remaining LLM responsibilities |
|-------|-------------------------------|
| Empire Coordinator | Digest compilation, marginal decisions (park/promote), portfolio health evaluation, human task guardrail, budget enforcement judgment, complex directive interpretation |
| Discovery Coordinator | Ambiguous deduplication, synthesis of conflicting sub-agent reports |
| Validation Coordinator | Packaging: assemble validation kit, write human-readable summary (1 turn) |
| Business Research Agent | Deep market research, kill decisions, spec market-alignment review |
| Factory CTO | Spec feasibility review, architecture guidance, template evolution |
| Lightweight Spec Agent | MVP spec writing from business brief |
| Spec Reviewer | Single-pass spec quality review |
| Spec Auditor | Tier-aware spec validation |
| Market Research Agent | Taxonomy-based market evaluation |
| Trend Research Agent | Emerging signal identification |

The pattern: agents do **research, analysis, writing, and judgment**. The runtime does **routing, gating, accumulation, and sequencing**.

#### 4.2.2.6 Pipeline Diagnostics

The pipeline coordinator manages state across 26 intercepted event types, 3 state machines, and multiple concurrent pipelines. When something breaks, the operator needs to know *where* within seconds, not minutes.

**Diagnostic table: `pipeline_transitions`**

Every interceptor handler writes a transition record before and after state mutation. This is the primary debugging tool.


*Schema: see `pipeline_transitions` in `contracts/ddl-canonical.sql`*


**What gets recorded:**

For validation pipeline events, `state_before` / `state_after` contain:
```json
{
  "status": "active",
  "gates": {"G1": true, "G2": false, "G3": false, "G4": true},
  "revision_count": 1,
  "inner_revision_count": 2,
  "spec_version": 3
}
```

For campaign events:
```json
{
  "status": "active",
  "current_mode": 1,
  "modes": ["saas_gap", "saas_trend", "local_services"],
  "current_scan_id": "..."
}
```

For scan accumulation events:
```json
{
  "agents_complete": 3,
  "expected_agents": 5,
  "reports_received": 27,
  "verticals_discovered": 4,
  "pending_dedup": 1
}
```

**Drop logging is critical.** Every dropped event (guard failed, stale spec_version, wrong status) gets a `drop_reason`. This is the #1 diagnostic signal — "why didn't X happen?" is almost always "because the event was dropped by a guard and here's why."

Examples:
- `"drop_reason": "status=rejected, expected=active"` — event for a killed vertical
- `"drop_reason": "spec_version=2, current=3"` — CTO reviewed stale spec
- `"drop_reason": "inner_revision_count=5, max=5"` — spec loop exhausted

**CLI diagnostic commands:**

```bash
# Show pipeline state for a vertical
empire pipeline status <vertical_id>
# Output:
# Vertical: pet-grooming-py
# Status: active
# Gates: G1=✓ G2=✗ G3=✗ G4=✓
# Spec Version: 2
# Revision Count: 1/3
# Inner Revision Count: 0/5
# Last Event: spec.revision_requested (2m ago)
# Waiting For: spec.draft_ready → spec_review → spec.approved

# Show recent transitions for a pipeline
empire pipeline trace <vertical_id> [--last N]
# Output:
# 14:32:01 vertical.shortlisted    → handleShortlisted     consumed  gates: ____
# 14:32:01   └→ emitted: validation.started, brand.requested
# 14:35:22 research.completed      → handleResearchCompleted consumed gates: G1__
# 14:38:15 brand.candidates_ready  → handleBrandReady       consumed  gates: G1_G4
# 14:41:03 spec.approved           → handleSpecApproved     consumed  gates: G1G2G4 sv=1
# 14:41:03   └→ emitted: spec.validation_requested
# 14:43:11 spec.validation_passed  → handleSpecValidated    consumed  gates: G1G2G4
# 14:43:11   └→ emitted: cto.spec_review_requested
# 14:47:55 cto.spec_revision_needed → handleCTORevision     consumed  gates: G1__G4 rc=1
# 14:47:55   └→ emitted: spec.revision_requested

# Show all active campaigns
empire pipeline campaigns
# Output:
# Campaign    Geography   Mode      Status  Progress  Scan
# c-001       paraguay    saas_gap  active  1/3       s-042 (67% agents done)
# c-002       uruguay     paused    -       0/3       - (backpressure: 5 pending)

# Show all stuck pipelines (no transition in >1h)
empire pipeline stuck [--threshold 1h]

# Show all dropped events (diagnostic gold)
empire pipeline drops [--last 24h] [--vertical <id>]
# Output:
# 14:47:55 cto.spec_approved  DROPPED  vertical=pet-grooming  reason: spec_version=1, current=2
# 15:01:22 research.completed DROPPED  vertical=dental-mgmt   reason: status=rejected
```

**Structured logging for every handler:**

```go
func (pc *PipelineCoordinator) handleSpecApproved(tx *sql.Tx, event Event) error {
    vp := pc.getValidationPipeline(event.VerticalID)
    if vp == nil {
        pc.logTransition(event, "handleSpecApproved", "dropped", nil, nil, 
            "no pipeline found for vertical_id")
        return nil
    }
    
    before := vp.snapshot()
    
    if vp.Status != "active" {
        pc.logTransition(event, "handleSpecApproved", "dropped", before, nil,
            fmt.Sprintf("status=%s, expected=active", vp.Status))
        return nil
    }
    
    vp.G2_Spec = true
    vp.SpecPayload = event.Payload["spec"]
    vp.SpecVersion++
    vp.InnerRevisionCount = 0
    
    after := vp.snapshot()
    emitted := []string{"spec.validation_requested"}
    
    pc.logTransition(event, "handleSpecApproved", "consumed", before, after, "")
    
    pc.deferEmit(Event{Type: "spec.validation_requested", ...})
    return pc.checkGates(vp)
}
```

**Health check endpoint:**

The runtime exposes `GET /health/pipeline` returning:

```json
{
  "campaigns": {
    "active": 1, "paused": 0, "completed": 12
  },
  "validations": {
    "active": 3, "packaged": 2, "parked": 1, "rejected": 8, "approved": 5
  },
  "scans": {
    "active": 1, "timed_out_last_24h": 0
  },
  "marginals": {
    "parked": 4, "oldest_days": 12
  },
  "alerts": [
    "validation pet-grooming-py: no transition in 2h (waiting for spec.draft_ready)",
    "campaign c-002: paused for 6h (backpressure)"
  ]
}
```

The `alerts` array is the key: the health endpoint computes "stuck" pipelines (no transition within expected timeframe per state) and surfaces them proactively. This feeds into the Telegram digest.

**Expected timeframes per state (for stuck detection):**

| Pipeline is waiting for | Expected within | Alert threshold |
|---|---|---|
| research.completed (initial) | 30 min | 2h |
| spec.draft_ready | 15 min | 1h |
| spec_review.passed/issues_found | 10 min | 1h |
| spec.validation_passed/failed | 10 min | 1h |
| cto.spec_approved/revision/veto | 30 min | 2h |
| brand.candidates_ready | 20 min | 2h |
| vertical.ready_for_review (packaging) | 5 min | 30 min |
| scan agent completion signal | 60 min | 90 min (matches timeout) |
| human mailbox decision | 24h | 72h |

#### 4.2.2.7 Sharded Execution

Some factory stages exceed what a single agent session can handle efficiently. The Market Research Agent walks 52 taxonomy subcategories with web searches for each — that's 40+ turns, 30-60 minutes, and a single failure at subcategory 45 loses everything. Sharding splits these workloads into parallel chunks processed by independent agent instances.

**Design principle:** Sharding is a runtime concern, not an agent concern. The agent prompt is identical whether it processes 52 subcategories or 7. The runtime splits the work, dispatches to parallel instances, collects results, and signals completion. Agents never know they're sharded.

**Shard envelope (included in all shard assignment events):**

```go
type ShardEnvelope struct {
    RootTaskID    uuid.UUID       // Parent scan/task this shard belongs to
    ScanID        uuid.UUID       // Scan accumulator ID (for report attribution)
    ShardID       uuid.UUID       // Unique ID for this shard
    ShardIndex    int             // 0-based index within the shard set
    ShardCount    int             // Total shards in this set
    ShardKey      string          // Deterministic key for idempotency (e.g. "financial_ops+commerce_payments")
    Scope         json.RawMessage // The actual work payload (categories, sources, etc.)
    DeadlineAt    time.Time       // Per-shard timeout
    BudgetCents   int             // Per-shard budget cap
}
```

**How it works (Market Research Agent example):**

```
scan.requested {mode: "saas_gap", geography: "argentina"}
  ↓
Runtime's ShardPlanner splits 8 taxonomy categories into N shards:

  Shard 0: [Financial Operations (9 sub), Commerce & Payments (6 sub)]     = 15 subcategories
  Shard 1: [Customer Operations (6 sub), Marketing & Sales (7 sub)]        = 13 subcategories
  Shard 2: [Workforce & HR (6 sub), Operations & Productivity (6 sub)]     = 12 subcategories
  Shard 3: [Industry-Specific (8 sub), Compliance & Governance (4 sub)]    = 12 subcategories

  ↓
Runtime spawns 4 MRA instances (same prompt, same tools), each receives:
  market_research.scan_assigned {
    geography: "argentina",
    scan_id: "...",
    taxonomy_categories: ["financial_ops", "commerce_payments"],  // Existing filter field
    shard: {shard_id: "...", shard_index: 0, shard_count: 4, ...}
  }

  ↓
Each MRA instance processes its subcategories, emitting category.assessed per subcategory.
Each emits market_research.scan_complete when done with its slice.

  ↓
ScanAccumulator expects 4 completion signals instead of 1.
When all 4 complete (or timeout): scan.completed as before.
```

**Shard planning is deterministic.** Given the same input (mode, geography, taxonomy), the same shards are produced every time. No LLM involved:

```go
type ShardPlanner struct {
    Plans map[string]ShardPlanFunc  // stage → planning function
}

type ShardPlanFunc func(payload json.RawMessage, config ShardConfig) []ShardAssignment

type ShardConfig struct {
    MaxShards           int           // Per-stage cap
    MaxConcurrent       int           // Concurrent execution limit
    TargetItemsPerShard int           // Aim for this many items per shard
    PerShardTimeout     time.Duration // Shard-level timeout
    PerShardBudgetCents int           // Shard-level budget
}

var defaultPlans = map[string]ShardPlanFunc{
    "market_research": planMarketResearchShards,  // Split 8 categories into N shards
    "trend_research":  planTrendResearchShards,    // Split 6 trend categories into N shards
    // Scanners are already 5 parallel agents — no sharding needed
    // Pre-Brand is a fast single task — no sharding needed
}

func planMarketResearchShards(payload json.RawMessage, config ShardConfig) []ShardAssignment {
    categories := extractTaxonomyCategories(payload) // 8 top-level categories
    // If taxonomy_categories filter is present, use only those
    return splitIntoShards(categories, config.TargetItemsPerShard)
}
```

**Shard lifecycle:**

```
                    ┌─────────────────┐
                    │  shard.planned   │  ShardPlanner produces shard assignments
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ↓              ↓              ↓
        ┌───────────┐  ┌───────────┐  ┌───────────┐
        │ shard 0   │  │ shard 1   │  │ shard 2   │  Dispatched to agent instances
        │ assigned  │  │ assigned  │  │ assigned  │
        └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
              │              │              │
         (reports)      (reports)      (reports)    category.assessed events flow
              │              │              │        to ScanAccumulator as before
              ↓              ↓              ↓
        ┌───────────┐  ┌───────────┐  ┌───────────┐
        │ shard 0   │  │ shard 1   │  │ shard 2   │  scan_complete per shard
        │ complete  │  │ complete  │  │ complete  │
        └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
              │              │              │
              └──────────────┼──────────────┘
                             ↓
                    ┌─────────────────┐
                    │ all shards done │  ScanAccumulator fires scan.completed
                    └─────────────────┘
```

**Shard state tracking (Postgres):**


*Schema: see `shards` in `contracts/ddl-canonical.sql`*


**Agent instance management:**

Sharded stages spawn ephemeral agent instances — clones of the base agent (same prompt, same tools) with a unique `agent_id` suffix:

```go
agentID := fmt.Sprintf("%s-shard-%d-%s", baseAgentID, shardIndex, scanID[:8])
// e.g. "market-research-agent-shard-2-a1b2c3d4"
```

Instances are task-scoped: created at shard assignment, destroyed at shard completion. They share the factory container but have independent LLM sessions. The AgentManager handles lifecycle identically to any other agent spawn.

**Integration with ScanAccumulator (§4.2.2.3):**

The only change is how `expectedAgentsPerMode` is determined:

```go
// Without sharding:
var expectedAgentsPerMode = map[string]int{
    "automation_micro": 1,
    "saas_gap":         1,
    "saas_trend":       1,
    "local_services":   5,
}

// With sharding: computed dynamically per scan from the shards table.
func (sa *ScanAccumulator) ExpectedCount(scanID string) int {
    return countShards(sa.db, scanID, sa.Mode)
}
```

Everything downstream (report accumulation, threshold filtering, dedup, scan.completed emission) is unchanged. The accumulator already handles multiple reporters — it just gets more of them.

**Shard completion signals:**

Each shard agent emits the same completion event as the unsharded agent (`market_research.scan_complete`, `trend_research.scan_complete`). The runtime includes shard metadata in the event delivery context so the accumulator can track per-shard completion. Agent prompts and emit tools are identical — agents don't know they're sharded.

**Retry policy:**

| Condition | Action |
|-----------|--------|
| Shard agent crashes / timeout | Retry with new agent instance, max 2 retries |
| Shard budget exceeded | Mark shard failed, keep partial results |
| >50% of shards failed for one scan | Mark scan as failed, escalate to mailbox |
| All retries exhausted for a shard | Keep whatever results were collected, continue with remaining shards |

Partial results are always preserved. A scan with 3/4 successful shards still produces discoveries from those 3 shards. The `scan.completed` event includes `shards_completed` and `shards_failed` counts.

**Guardrails:**

```yaml
# In empireai.yaml
sharding:
  max_shards_per_scan: 8              # No scan produces more than 8 parallel shards
  max_concurrent_shards: 12           # System-wide concurrent shard limit
  per_shard_timeout: 30m              # Per-shard deadline (vs 90m for unsharded scan)
  per_shard_budget_cents: 50          # ~$0.50 per shard
  max_retries_per_shard: 2
  circuit_breaker_threshold: 0.5      # Pause new shards if >50% failure rate in last hour

  stages:
    market_research:
      target_items_per_shard: 13      # ~13 subcategories per shard → 4 shards for full taxonomy
      max_shards: 8
    trend_research:
      target_items_per_shard: 3       # 6 categories → 2 shards
      max_shards: 4
```

**What is sharded (v1):**

| Stage | Shard key | Items | Typical shards |
|-------|-----------|-------|----------------|
| Market Research Agent | Taxonomy top-level categories | 52 subcategories across 8 categories | 4 |
| Trend Research Agent | Trend category groups | 6 trend categories | 2 |

Scanners (local_services mode) are already parallel — 5 separate agents, no sharding needed. Pre-Brand Agent is a fast single task — no benefit from sharding.

**What is NOT sharded (v1):**

Decision gates remain single-agent: Validation Coordinator (single packaging turn), Empire Coordinator (portfolio-level decisions), Business Research Agent (deep sequential research on one vertical), Factory CTO (full-context spec review), Spec Reviewer, Spec Auditor. Note: scoring composite computation is fully runtime-owned (§4.2.2.8) — no LLM agent involved.

**Dashboard integration (§10.5.2 Tab 2):**

Each active scan shows shard progress:

```
┌─ Scan: saas_gap (Argentina) ───────────────────┐
│ MRA Shards: ██████░░ 3/4 complete              │
│                                                  │
│ Shard 0: financial_ops,commerce   ✓ 12m  $0.31 │
│ Shard 1: customer_ops,marketing   ✓ 15m  $0.38 │
│ Shard 2: workforce,operations     ✓ 11m  $0.28 │
│ Shard 3: industry,compliance      ⟳ 8m   $0.19 │
│                                                  │
│ Reports: 38 collected | 7 high-signal           │
└──────────────────────────────────────────────────┘
```

Stuck shard detection: shards with no report emitted for >10 minutes turn yellow. Past deadline turns red.

**CLI:**

```bash
empire scan shards <scan_id>              # List all shards with status, duration, spend
empire scan shard <shard_id>              # Detail: scope, agent, reports, spend
empire scan shard retry <shard_id>        # Manual retry of a failed shard
empire scan shard cancel <shard_id>       # Cancel an in-progress shard
```

**Future (v2): LLM child-agent mode.**

Sharding currently uses runtime-planned deterministic splits. A future extension allows agents to request dynamic sub-agents at runtime — e.g., BRA discovering during research that three competitor products each need deep analysis. This requires: parent declares planned child count, children share parent's shard budget/TTL, child outputs must map to a declared schema, runtime can deny spawn if guardrails exceeded. Deferred until v1 sharding proves stable.

#### 4.2.2.8 Scoring Pipeline (ScoringNode)

The scoring pipeline follows the same accumulator pattern as discovery (§4.2.2.3): the runtime handles deterministic routing and accumulation, the LLM agent handles judgment. This eliminates cross-talk when multiple verticals are scored concurrently.

**Problem solved:** The Scoring Coordinator (removed in v2.0.19) previously received `vertical.discovered` and individual `score.dimension_complete` events, tracking partial state per vertical in its LLM context. With concurrent verticals, cross-talk caused premature emissions and evidence conflation. The entire scoring pipeline is now runtime-owned — no LLM agent involved except the Analysis Agent scoring individual dimensions.

**v2.0.37 change — ScoringNode replaces interceptor:** Prior to v2.0.37, scoring was handled by two interceptor cases inside `EventBus.Publish`. This caused a critical bug: `vertical.discovered` was often produced as a deferred event from the discovery accumulator (§4.2.2.3), and deferred events bypass `runInterceptors`. The `handleVerticalDiscovered` interceptor case never fired for discovery-originated verticals, leaving the entire scoring pipeline dead (zero `scoring.requested` emitted). See §4.2.2.10 for the system node architecture that replaces this pattern.

**Design:** The `ScoringNode` is a system node (§4.2.2.10) that subscribes to `vertical.discovered`, `vertical.derived`, `score.dimension_complete`, and `scoring.contest_resolved` via normal EventBus subscription. It executes the same deterministic logic that was previously in the interceptor, but as a subscriber rather than middleware. Events it publishes go through the full `Publish` path, eliminating the deferred event chaining problem.

```go
// ScoringNode implements SystemNode
type ScoringNode struct {
    db          *sql.DB
    accumulators map[uuid.UUID]*ScoringAccumulator
}

func (sn *ScoringNode) ID() string { return "scoring-node" }

func (sn *ScoringNode) Subscriptions() []EventType {
    return []EventType{"vertical.discovered", "score.dimension_complete", "scoring.contest_resolved"}
}
```

Two handlers plus a deterministic `computeComposite()` function handle the complete scoring pipeline:

1. **`handleVerticalDiscovered`** — deterministic rubric selection + delegation. Receives `vertical.discovered` via subscription, selects rubric based on `mode`, emits `scoring.requested` via `EventBus.Publish` with the correct dimensions list. No LLM judgment needed.

2. **`handleScoreDimensionComplete`** — accumulation + computation. Accumulates individual dimension scores in a `ScoringAccumulator` keyed by `vertical_id`. When all expected dimensions arrive, calls `computeComposite()` directly — applies hard gates, computes weighted composite, evaluates viability floor, and emits the terminal result (`vertical.shortlisted`, `vertical.marginal`, or `vertical.rejected`). No intermediate event, no LLM invocation.

```go
type ScoringAccumulator struct {
    VerticalID     uuid.UUID
    VerticalName   string
    Geography      string
    Mode           string
    Rubric         string                        // "universal" (v2.0.39 — was "saas", "local_services", or "automation_micro")
    Expected       []string                      // Dimension names from rubric
    Received       map[string]DimensionScore     // dimension_name → score
    TimeoutAt      time.Time
}

type DimensionScore struct {
    Score      int              // 0-100
    Evidence   string
    Confidence string           // optional
    AgentID    string           // which analysis-agent instance produced this
}

// Rubric definitions — v2.0.39: single universal rubric for all modes
var rubricDimensions = map[string][]string{
    "universal": {
        "build_complexity", "automation_completeness",  // Hard gates (scored first)
        "icp_crispness", "distribution_leverage",       // Tier 1: Execution Fit
        "time_to_value", "operational_drag",
        "pain_severity", "competition_gap",             // Tier 2: Market Viability
        "monetization_clarity",
        "retention_architecture", "expansion_potential", // Tier 3: Upside
    },
}

// modeToRubric maps scan mode to scoring rubric — all modes now use universal
var modeToRubric = map[string]string{
    "local_services":   "universal",
    "saas_gap":         "universal",
    "saas_trend":       "universal",
    "automation_micro": "universal",
}
```

**Handler 1: `handleVerticalDiscovered`**

```
vertical.discovered received (from Discovery Accumulation)
  → Runtime looks up mode from event payload
  → Runtime selects rubric: modeToRubric[mode]
  → Runtime looks up dimensions: rubricDimensions[rubric]
  → ScoringNode creates ScoringAccumulator:
      {vertical_id, vertical_name, geography, mode, rubric,
       expected: rubricDimensions[rubric], received: {}, timeout: now + 60min}
  → Runtime emits scoring.requested:
      {vertical_id, vertical_name, geography, mode, rubric,
       dimensions_requested: rubricDimensions[rubric]}
  → Event consumed (NOT delivered to any agent — runtime handles scoring entirely)
```

This is fully deterministic: mode → rubric → dimensions. No LLM involved in rubric selection or dimension delegation.

**Handler 2: `handleScoreDimensionComplete`**

```
score.dimension_complete received (from Analysis Agent)
  → ScoringNode finds ScoringAccumulator by vertical_id
  → Runtime stores dimension score in received map:
      received[dimension] = {score, evidence, confidence, agent_id}
  → Runtime checks: len(received) == len(expected)?
      NO → wait for more dimensions (accumulate)
      YES → all dimensions present:
        → If contested_dimensions exist → emit scoring.contested to Empire Coordinator (rare edge case, needs LLM judgment)
        → Else → computeComposite():
            1. Hard gates:
               build_complexity < 50 → emit vertical.scored(rejected, gate_build_complexity) + vertical.rejected
               automation_completeness < 50 → emit vertical.scored(rejected, gate_automation_completeness) + vertical.rejected
            2. Tier 1 dimension floor:
               any of {icp_crispness, distribution_leverage, time_to_value, operational_drag} < 50
               → emit vertical.scored(rejected, tier1_dimension_floor_{dim}) + vertical.rejected
            3. Tier 1 sub-score:
               weighted avg of Tier 1 dimensions < 60
               → emit vertical.scored(rejected, viability_floor_execution_fit) + vertical.rejected
            4. Composite:
               weighted sum of all 9 scored dimensions (gates excluded from weights)
            5. Threshold + marginal drain:
               composite ≥ 75         → emit vertical.scored(shortlisted) + vertical.shortlisted
               composite 55-74 AND ≥2 Tier 1 dims ≥70 → emit vertical.scored(marginal) + vertical.marginal
               composite 55-74 AND <2 Tier 1 dims ≥70  → emit vertical.scored(rejected, marginal_drain) + vertical.rejected
               composite < 55         → emit vertical.scored(rejected, composite_below_threshold) + vertical.rejected
        → Remove ScoringAccumulator (terminal)
  → Event consumed (NOT forwarded)

**EC delivery filtering for `vertical.scored`:**

The Empire Coordinator subscribes to `vertical.scored` for digest compilation and shortlist awareness. But clear rejections (composite < 50 or viability floor) require zero judgment — the runtime already made the decision. Delivering every rejection to EC wastes a full session turn (~$0.02-0.05) per vertical for a response that is always "concur with auto-reject."

The runtime filters `vertical.scored` delivery to EC based on the `result` field:

| Result | Delivered to EC? | Reason |
|--------|-----------------|--------|
| `shortlisted` | **Yes** — full event | EC tracks pipeline capacity, includes in digest |
| `marginal` | **Yes** — via `vertical.marginal` (separate subscription) | EC must decide: promote/park/reject |
| `rejected` | **No** — written to `scoring_digest_buffer` table only | No judgment needed, digest reads from table |

```go
// In computeComposite(), after determining result:
if result == "rejected" {
    // Still emit both events for audit/persistence
    eb.Publish(verticalScored)   // Written to events table
    eb.Publish(verticalRejected) // Written to events table, no subscribers
    // Write summary row for EC digest compilation (no agent invocation)
    db.Exec(`INSERT INTO scoring_digest_buffer
        (vertical_id, vertical_name, geography, composite, viability, result, reason, scored_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())`,
        verticalID, name, geo, composite, viability, result, reason)
    return // Do NOT deliver vertical.scored to EC
}
// Shortlisted: deliver to EC (and vertical.shortlisted to interceptor)
eb.Publish(verticalScored)   // Delivered to EC subscriber
eb.Publish(verticalShortlisted) // Intercepted → validation pipeline
```

**Digest compilation:** When EC processes `timer.portfolio_digest`, the runtime injects a summary of recent rejections from `scoring_digest_buffer` into the event payload:

```go
// In scheduler, when firing timer.portfolio_digest:
rejections := db.Query(`SELECT vertical_name, geography, composite, reason
    FROM scoring_digest_buffer WHERE scored_at > $1`, lastDigestTime)
// Inject as structured context in the timer event payload
timerEvent.Payload["recent_rejections"] = rejections
timerEvent.Payload["rejection_count"] = len(rejections)
```

EC sees a compact summary ("12 rejections since last digest: 8 Paraguay viability_floor, 2 Argentina low_composite, 2 Uruguay low_composite") instead of processing 12 individual events. Cost savings: 12 EC turns → 0 EC turns + 1 digest line.


*Schema: see `scoring_digest_buffer` in `contracts/ddl-canonical.sql`*

```

**Composite computation in Go (no LLM needed):**

```go
// Rubric weights — v2.0.39: single universal rubric
var rubricWeights = map[string]map[string]float64{
    "universal": {
        // Tier 1: Execution Fit (60%)
        "icp_crispness":        0.15,
        "distribution_leverage": 0.15,
        "time_to_value":        0.15,
        "operational_drag":     0.15,
        // Tier 2: Market Viability (30%)
        "pain_severity":        0.10,
        "competition_gap":      0.10,
        "monetization_clarity":  0.10,
        // Tier 3: Upside (10%)
        "retention_architecture": 0.05,
        "expansion_potential":    0.05,
    },
}

// Tier 1 dimensions — for sub-score computation and floor checks
var tier1Dimensions = []string{
    "icp_crispness", "distribution_leverage", "time_to_value", "operational_drag",
}

// Hard gates — pass/fail scored 0-100, below threshold = immediate reject
var rubricGates = map[string][]HardGate{
    "universal": {
        {"build_complexity", 50, "gate_build_complexity"},
        {"automation_completeness", 50, "gate_automation_completeness"},
    },
}

// Tier 1 floor: no single Tier 1 dimension below 50
const tier1DimensionFloor = 50

// Tier 1 sub-score floor
const tier1SubScoreFloor = 60.0

// Marginal drain: 55-74 must have ≥2 Tier 1 dimensions ≥70
const marginalDrainMinHighDims = 2
const marginalDrainHighThreshold = 70

func computeComposite(acc *ScoringAccumulator) ScoringResult {
    weights := rubricWeights[acc.Rubric]

    // Step 1-2: Hard gates — check before computing composite
    if gates, ok := rubricGates[acc.Rubric]; ok {
        for _, gate := range gates {
            if ds, exists := acc.Received[gate.Dimension]; exists {
                if ds.Score < gate.MinScore {
                    return ScoringResult{
                        VerticalID: acc.VerticalID, Result: "rejected",
                        Reason: gate.Reason, Rubric: acc.Rubric,
                        Dimensions: acc.Received,
                    }
                }
            }
        }
    }

    // Step 3: Tier 1 dimension floor — any Tier 1 dim < 50 = structural kill
    for _, dim := range tier1Dimensions {
        if ds, exists := acc.Received[dim]; exists {
            if ds.Score < tier1DimensionFloor {
                return ScoringResult{
                    VerticalID: acc.VerticalID, Result: "rejected",
                    Reason: fmt.Sprintf("tier1_dimension_floor_%s", dim),
                    Rubric: acc.Rubric, Dimensions: acc.Received,
                }
            }
        }
    }

    // Compute Tier 1 sub-score
    var tier1Sum, tier1WeightSum float64
    for _, dim := range tier1Dimensions {
        if ds, exists := acc.Received[dim]; exists {
            w := weights[dim]
            tier1Sum += float64(ds.Score) * w
            tier1WeightSum += w
        }
    }
    tier1Score := tier1Sum / tier1WeightSum * 100

    // Step 4: Tier 1 sub-score floor
    if tier1Score < tier1SubScoreFloor {
        return ScoringResult{
            VerticalID: acc.VerticalID, Result: "rejected",
            Reason: "viability_floor_execution_fit", Rubric: acc.Rubric,
            Dimensions: acc.Received, ViabilityScore: tier1Score,
        }
    }

    // Compute full composite (all 9 scored dimensions, gates excluded from weights)
    var composite float64
    for dim, ds := range acc.Received {
        if w, ok := weights[dim]; ok {
            composite += float64(ds.Score) * w
        }
    }
    // Normalize: weights sum to 1.0, so composite is already on 0-100 scale

    // Step 5: Composite threshold
    var result, reason string
    if composite >= 75 {
        result = "shortlisted"
    } else if composite >= 55 {
        // Step 6: Marginal drain check — need ≥2 Tier 1 dims ≥70
        highDimCount := 0
        for _, dim := range tier1Dimensions {
            if ds, exists := acc.Received[dim]; exists && ds.Score >= marginalDrainHighThreshold {
                highDimCount++
            }
        }
        if highDimCount >= marginalDrainMinHighDims {
            result = "marginal"
        } else {
            result, reason = "rejected", "marginal_drain"
        }
    } else {
        result, reason = "rejected", "composite_below_threshold"
    }

    return ScoringResult{
        VerticalID:     acc.VerticalID,
        Result:         result,
        Reason:         reason,
        CompositeScore: composite,
        ViabilityScore: tier1Score, // Tier 1 sub-score reported as "viability" for continuity
        Dimensions:     acc.Received,
        Rubric:         acc.Rubric,
        Partial:        len(acc.Received) < len(acc.Expected),
    }
}
```

**Contested dimension handling:** If the accumulator detects contested dimensions (>30 point spread on the same dimension from different shards), the ScoringNode does NOT compute the composite. Instead it emits `scoring.contested` to Empire Coordinator with both scores and evidence. EC uses LLM judgment to pick the credible score, then emits `scoring.contest_resolved` back to the ScoringNode, which substitutes the resolved score and proceeds with `computeComposite()`. This is a rare edge case — only happens with sharded Analysis Agents scoring overlapping dimensions.

**Timeout handling:** If the accumulator hasn't received all dimensions within 60 minutes, the runtime runs `computeComposite()` with whatever dimensions have arrived. Missing dimensions are scored as 0. The `partial: true` flag is set in the emitted events.

**Why no LLM agent:** The Scoring Coordinator was an LLM doing multiplication and if-statements. Weights are fixed in code. Gates are fixed thresholds. The only judgment call (contested dimensions) happens <5% of the time and is escalated to an agent that already exists (Empire Coordinator). Removing the SC saves one LLM invocation per vertical scored — at ~$0.02-0.05 per invocation across dozens of verticals, this adds up.

**Integration with sharding (§4.2.2.7):**

When the Analysis Agent is sharded (e.g., 2 shards each handling a subset of dimensions), the ScoringAccumulator's `expected` list doesn't change — it still expects all dimensions regardless of how many shards produce them. Each shard emits `score.dimension_complete` events that accumulate normally. The accumulator is shard-agnostic: it cares about dimension coverage, not shard count.

**Events emitted by `computeComposite()` (runtime, not agent):**

The runtime emits `vertical.scored` + one of `vertical.shortlisted` / `vertical.marginal` / `vertical.rejected` directly. No intermediate event. No LLM invocation. The `scoring.dimensions_complete` event is eliminated — the accumulator flows straight into computation.

#### 4.2.2.9 OpCo Cycle Detection

The factory pipeline has `InnerRevisionCount` (max 5) to prevent BRA↔LSA↔Reviewer loops from spinning forever (§4.2.2.2). Operating companies need equivalent protection. Without it, a QA↔Backend rejection loop, a PM↔CTO spec revision loop, or a Support↔CTO bug-fix loop can cycle indefinitely, draining the monthly API budget.

**Problem:** Each event creates a new task-scoped agent invocation. The per-agent `max_turns_per_task` limits individual invocations but not cross-task cycles. An agent finishing in 15 turns, emitting a rejection, triggering a new 15-turn invocation on the peer, which re-emits — this can loop N times with no upper bound.

**Design: per-vertical event chain tracker.**

The runtime tracks repetitive event patterns within each vertical. When the same event type is emitted N times within a rolling window, the runtime intervenes.

```go
type OpCoCycleTracker struct {
    mu       sync.Mutex
    counters map[string]*CycleCounter  // key: "{vertical_id}:{event_pattern}"
}

type CycleCounter struct {
    VerticalID   uuid.UUID
    EventPattern string    // e.g., "qa.validation_failed"
    Count        int       // emissions within window
    WindowStart  time.Time // rolling window start
    LastEmitter  string    // agent_id of last emission (detect ping-pong)
}

// CycleConfig per-vertical, overridable by OpCo CTO via configure_runtime tool
type CycleConfig struct {
    MaxCyclesPerPattern int           // default: 5
    WindowDuration      time.Duration // default: 4 hours
    EscalationTarget    string        // default: "opco_cto", then "mailbox" if CTO is in the loop
}
```

**How it works:**

```
QA emits qa.validation_failed (count: 1)
  → Backend receives, fixes code, CTO re-deploys to staging
QA emits qa.validation_failed (count: 2)
  → Same cycle
QA emits qa.validation_failed (count: 3, 4)
  → Same cycle
QA emits qa.validation_failed (count: 5)
  → Runtime intercepts: cycle limit reached
  → Runtime injects escalation event: cycle_limit_reached
     {vertical_id, event_pattern: "qa.validation_failed", count: 5,
      agents_involved: ["qa-petgrooming", "backend-petgrooming"],
      window_start, recommendation: "QA and Backend are unable to resolve
      this validation failure after 5 attempts. Human review recommended."}
  → Routed to OpCo CTO first (they manage the build)
  → If CTO is in the loop (CTO was the one triggering re-deploys):
     escalate to mailbox instead
  → The cycle counter is NOT reset until a human or CTO explicitly
     calls emit_cycle_reset or the window expires
```

**Detection rules:**

| Pattern | What it catches | Default limit |
|---------|----------------|---------------|
| Same event type, 2+ distinct emitters alternating | Ping-pong loops (QA↔Backend, PM↔CTO) | 5 within 4h |
| Same event type, same emitter | Repeated failures (deploy keeps failing) | 5 within 4h |
| Any `spend_needed` emissions | Budget drain via rapid spend requests | 3 within 1h |

**Integration with EventBus.Publish:**

Cycle detection runs in `fanOutOpCo()` — the same path that resolves routing table rules for OpCo internal events. It's a lightweight counter check (in-memory with periodic DB sync), not a blocking operation. The tracker only applies to OpCo internal events — factory events have their own pipeline-level cycle protection.

```go
func (eb *EventBus) fanOutOpCo(event Event) error {
    // 1. Check cycle tracker before delivery
    if blocked, escalation := eb.cycleTracker.Check(event); blocked {
        // Deliver the escalation event instead of (or in addition to) the original
        eb.Publish(escalation)
        // Original event is still persisted (step 1 of Publish) but delivery is blocked
        return nil
    }
    // 2. Normal routing table resolution and delivery
    ...
}
```

**State persistence:** Cycle counters are in-memory for speed, synced to a `cycle_counters` table on each increment for crash recovery. On restart, counters are reloaded from DB. The table is lightweight — one row per active pattern per vertical.


*Schema: see `cycle_counters` in `contracts/ddl-canonical.sql`*


**Reset conditions:**
- Window expires (4h default) → counter resets to 0 automatically
- Human approves reset via mailbox → runtime resets counter
- OpCo CTO calls `emit_cycle_reset` with the pattern → resets counter (logged)
- Vertical restarted/redeployed → all counters for that vertical reset

This is the OpCo equivalent of the factory pipeline's `InnerRevisionCount`, applied at the event routing layer rather than the interceptor layer.

#### 4.2.2.10 System Nodes

**Introduced in v2.0.37.** See RFC-001 v2 for architectural rationale.

The runtime has two types of participants that process events:

**Agent nodes** — LLM-powered. Have system prompts, model tiers, context windows, conversation history. Receive events, reason about them, produce output. Examples: Analysis Agent, Business Research Agent, OpCo CTO.

**System nodes** — Deterministic Go code. No LLM. Receive events via normal EventBus subscription, execute state machine logic, emit events via normal `EventBus.Publish`. Examples: ScoringNode (§4.2.2.8).

Both node types receive events through the same fan-out mechanism. The EventBus does not distinguish between agent and system node subscribers. System nodes are registered alongside agents at startup:

```go
// SystemNode is a deterministic, non-LLM component that participates
// in event processing alongside agents.
type SystemNode interface {
    ID() string
    Subscriptions() []EventType
    HandleEvent(ctx context.Context, event Event) ([]Event, error)
}

// Registration at startup
eb.RegisterSystemNode(scoringNode)
```

**Why system nodes exist:** The pipeline coordinator's interceptor middleware processes events synchronously inside `EventBus.Publish`. Events produced by the interceptor (deferred events) are persisted and delivered but NOT re-intercepted — they bypass `runInterceptors`. This means interceptor-produced events cannot trigger further interceptor logic, breaking pipeline flows that depend on event chaining (see §4.2.2.8 for the scoring pipeline example).

System nodes solve this by moving deterministic logic out of the interceptor and into normal event subscribers. Events published by a system node go through the full `Publish` path — including the interceptor for any remaining intercepted event types. Event chaining works naturally.

**Transaction semantics:** System nodes process events in their own transactions, separate from the original Publish transaction. This requires an idempotency contract:

1. Start local transaction
2. Check idempotency ledger: has `(event.ID, node_id)` been processed?
3. If yes: no-op, return (safe replay)
4. Execute state transition
5. Persist outgoing events in same transaction
6. Write processing receipt in same transaction
7. Commit
8. Fan out outgoing events via `Publish` (post-commit, best-effort)

If failure occurs before commit, the event remains unacked for retry. Dead-letter policy: after 5 failed retries, escalate via `pipeline.dead_letter` event to Operations Analyst.

**Idempotency ledger:**


*Schema: see `system_node_ledger` in `contracts/ddl-canonical.sql`*


**Migration plan (RFC-001 v2):** System nodes are introduced incrementally. Phase 1 (v2.0.37) migrates only the scoring pipeline. Remaining interceptor cases (validation, discovery accumulation, scan campaigns) continue to use the interceptor pattern and will be migrated in future phases only after scoring node parity is proven in production.

### 4.3 AgentManager

Manages agent lifecycle, including operating company spinup with default teams:

```go
type AgentManager struct {
    agents   map[string]Agent
    bus      *EventBus
    configs  []AgentConfig
}

func (am *AgentManager) SpawnAgent(config AgentConfig) error
func (am *AgentManager) SpawnOpCo(verticalID string, mandate MandateDocument) error  // Spawns CEO + VPs + Workers + routing
// MandateDocument includes: factory docs (business brief, mvp spec, brand) + founder directives + budget + infrastructure config
func (am *AgentManager) SpawnAgentFor(managerID string, config AgentConfig) error    // Any manager (CEO or VP) hires agent
func (am *AgentManager) ReconfigureAgent(agentID string, config AgentConfig) error   // Any manager modifies agent config
func (am *AgentManager) TeardownAgent(agentID string) error                          // Any manager fires agent
func (am *AgentManager) TeardownOpCo(verticalID string) error                        // Kill entire operating company
func (am *AgentManager) RestartAgent(agentID string) error
func (am *AgentManager) Shutdown() error
```

`SpawnOpCo` does:
1. **Provisions infrastructure (deterministic, no LLM):**
   a. Allocates next available port pair (production + staging = production_port + 1000)
   b. Creates DB schemas: `CREATE SCHEMA IF NOT EXISTS {vertical_slug}` and `{vertical_slug}_staging`
   c. Runs vertical's `schema.sql` within both schemas
   d. Writes port/schema allocation to `verticals` table
   e. Assembles final mandate document with infrastructure config populated
2. Creates CEO agent with mandate
3. Creates Chief of Staff (cross-domain coordination, no reports)
4. Creates VP layer: Head of Product, Head of Growth
5. Creates product workers: PM, CTO, Support (under Head of Product)
6. Creates CTO's engineering sub-team: Tech Writer, Backend, Frontend, QA, DevOps (under CTO)
7. Creates growth workers: Marketing (under Head of Growth)
8. Installs bootstrap + seeded routing table (current version)
   Bootstrap (20 entries): deadlock prevention, can't be removed by agents.
   Seeded (7 entries): common-sense day-1 routes, removable by managers.
   Both evolve via Operations Analyst proposals → Factory CTO approval.
9. Installs initial heartbeat timers (dynamic self-scheduling, no fixed recurring)
10. Notifies CEO that org is ready with roster and routing table

CEO and VP tools map to AgentManager methods:
- `agent_hire` → `SpawnAgentFor` (CEO hires VPs, VPs hire workers)
- `agent_fire` → `TeardownAgent` (managers can only fire agents under them)
- `agent_reconfigure` → `ReconfigureAgent` (modify agent prompt, tools, constraints)

**Prompt override (hot-reload):** `ReconfigureAgent` also handles prompt overrides from the dashboard/CLI. When called with a new prompt:
1. Snapshot current prompt to `prompt_overrides.previous_prompt`
2. Write new prompt to `prompt_overrides` table
3. Checkpoint current conversation summary (compress via LLM or use latest existing summary)
4. Stop current session
5. Start new session: **new prompt** + checkpoint summary injected as context
6. Agent continues with updated behavioral instructions and full awareness of prior work

On agent start (any start — cold boot, crash recovery, or reconfigure), the runtime checks `prompt_overrides` first. If a row exists for this `agent_id`, use that prompt. Otherwise, use the prompt from `org_templates` (for OpCo agents) or `roster.yaml` (for holding agents).

Revert: deleting the `prompt_overrides` row and restarting returns the agent to its template prompt. The `previous_prompt` column enables one-click revert without needing to look up the template.
- `configure_routing` → `EventBus.SetRoutingTable` (authorization enforced in runtime):
  - **CEO:** full routing authority within own vertical
  - **VP:** can add/remove routes within their domain only (subscribers must be in their management chain)
  - **CTO:** can add/remove routes within engineering sub-team only
  - **Chief of Staff:** can **propose** cross-domain routes. Runtime writes with `status = 'proposed'`, CEO auto-notified to approve/reject. CoS cannot directly install cross-domain routes — this is enforced by the runtime checking that CoS is not a manager of any agent, so any route where `subscriber_id` is outside CoS's (empty) management chain requires CEO approval.
  - **Bootstrap route immutability (hard enforcement):** Routes with `source = 'bootstrap'` cannot be modified or deactivated by any agent, including CEO. `configure_routing` must reject any operation where the target rule has `source = 'bootstrap'` — return error "bootstrap routes are immutable." Only template migrations via Factory CTO can modify bootstrap routes (by publishing a new template version). This prevents agents from accidentally breaking critical communication paths that the system depends on for basic operation.

Authorization: `SpawnAgentFor` checks that the requesting agent is a manager of the target's domain (CEO can hire/fire anyone, VP can only hire/fire within their domain). This is enforced by checking `parent_agent_id` chains.

Tracks agent-to-vertical mapping for budget accounting. Recovers panicked goroutines with backoff. On startup, replays unprocessed events: factory agents via subscription matching, OpCo agents via `event_deliveries` manifest. Replay is bounded by each agent's `started_at` timestamp to prevent historical backlog on newly hired agents.

### 4.4 Claude Conversation + Runtime Manager

#### 4.4.1 LLM Runtime Abstraction

The runtime supports two backends, switchable via config. All agent logic is backend-agnostic.

```go
type LLMRuntime interface {
    // StartSession creates a new conversation session for an agent.
    // Returns a session handle for subsequent turns.
    StartSession(agentID string, systemPrompt string, tools []ToolDefinition) (*Session, error)
    
    // ContinueSession sends a message in an existing session and returns the response.
    // Handles tool call loops internally (up to MaxTurns).
    ContinueSession(session *Session, message Message) (*Response, error)
}

type Session struct {
    ID           string           // Provider session ID (API conversation ID or CLI --session-id)
    AgentID      string
    RuntimeMode  string           // "api" | "cli_test"
    TurnCount    int
    Messages     []Message        // In-memory conversation state
}
```

**Runtime adapters:**

| Adapter | Mode | Use case | Session continuity |
|---------|------|----------|-------------------|
| `AnthropicAPIRuntime` | `api` | Production | Messages array managed in-memory, persisted to `conversations` table |
| `ClaudeCLIRuntime` | `cli_test` | Development/validation | Provider-managed via `claude -p --session-id` / `claude -p -r` |

#### 4.4.2 Claude CLI Continuity Contract

The CLI test runtime uses Claude Code's non-interactive mode for stateful multi-turn conversations:

**First turn (new session):**
```bash
claude -p --session-id <uuid> --output-format json \
  "System: {system_prompt}\n\nTools: {tool_definitions}\n\n{first_message}"
```

**Subsequent turns (resume):**
```bash
claude -p -r <uuid> --output-format json \
  "{message_or_tool_result}"
```

**Contract requirements:**
- Session persistence must be enabled (do NOT pass `--no-session-persistence`)
- Single writer per session — enforced by `SessionRegistry` lease (see §4.4.4)
- Structured output (`--output-format json`) for reliable response parsing
- Timeout per turn configured in `llm.claude_cli.timeout` (default: 120s)

**tmux is NOT required** for this test mode. The runtime invokes `claude -p` as a subprocess, captures stdout, and parses the JSON response. tmux remains available as an optional tool for manual operator debugging only.

#### 4.4.3 Conversation Modes

Each agent maintains its own conversation state for the duration of a task:

```go
type Conversation struct {
    AgentID      string
    TaskID       string
    Session      *Session          // Active LLM session handle
    SystemPrompt string
    Messages     []Message
    Tools        []ToolDefinition
    MaxTurns     int
    TurnCount    int
}

func (c *Conversation) Step() (*Response, error)
func (c *Conversation) AppendResult(toolResult ToolResult)
func (c *Conversation) AppendFeedback(feedback string)
func (c *Conversation) Reset()
```

**Operating agents have longer-lived conversations** than factory agents. A factory worker processes one task and resets. An operating agent (e.g., Support Agent) maintains ongoing context about the product, its users, and its history. The conversation manager must support:

- **Task-scoped conversations** (factory mode): one task → one conversation → reset
- **Session-scoped conversations** (operating mode): persistent context across multiple interactions, periodically summarized to manage context window

```go
type ConversationMode int

const (
    TaskScoped   ConversationMode = iota  // Factory workers
    SessionScoped                          // Operating agents
)
```

#### 4.4.4 Session Registry

The `SessionRegistry` manages session lifecycle, enforces single-writer semantics, and handles session rotation when context grows stale or corrupted.

```go
type SessionRegistry struct {
    db *sql.DB
}

type SessionLease struct {
    SessionID  string
    AgentID    string
    ScopeKey   string    // "" for global session, vertical_id for per-vertical
    LockOwner  string    // Goroutine/process identifier
    ExpiresAt  time.Time
}

// Acquire obtains or creates an active session for the agent.
// For session_per_vertical agents, scopeKey is the vertical_id.
// For session agents, scopeKey is "" (global).
// Returns existing session if active, creates new one otherwise.
// Acquires an exclusive lease (single-writer lock).
func (sr *SessionRegistry) Acquire(agentID string, scopeKey string) (*SessionLease, error)

// Release releases the lease after the agent's turn completes.
func (sr *SessionRegistry) Release(lease *SessionLease) error

// Rotate closes the current session and starts a fresh one.
// Persists a checkpoint summary for context bridging.
func (sr *SessionRegistry) Rotate(agentID string, scopeKey string, summary string) (*SessionLease, error)

// Cleanup removes all sessions for a given scope key (called when vertical validation completes).
func (sr *SessionRegistry) CleanupScope(scopeKey string) error
```

**Three conversation modes:**

| Mode | Session Key | Use Case | Agents |
|------|-------------|----------|--------|
| `task` | None (fresh per event) | Stateless computation. One event in → one result out. No memory across events. | Analysis Agent, Spec Auditor, Discovery Coordinator, Validation Coordinator, Spec Reviewer, Pre-Brand Agent |
| `session` | `agentID` (global) | Persistent context across all events. For singletons that need cross-vertical awareness. | Empire Coordinator, Factory CTO, Operations Analyst, all OpCo agents (`ceo-{v}`, `cos-{v}`, `vp-*-{v}`) |
| `session_per_vertical` | `agentID + vertical_id` | Persistent context within a single vertical's workflow, isolated from other verticals. For factory pipeline agents that run multi-step workflows per vertical. | Business Research Agent, Lightweight Spec Agent |

**`session_per_vertical` runtime behavior:**

When the runtime delivers an event to a `session_per_vertical` agent:
1. Extract `vertical_id` from event payload (required — runtime rejects events without it)
2. Call `SessionRegistry.Acquire(agentID, verticalID)` — gets or creates a conversation scoped to this agent+vertical pair
3. The LLM session contains only the conversation history for this specific vertical
4. Agent can do multi-turn research, spec writing, review cycles — all within the vertical's isolated context
5. When another vertical's event arrives for the same agent, it gets a completely separate session
6. On validation complete (vertical approved/rejected), `SessionRegistry.CleanupScope(verticalID)` removes all per-vertical sessions

**Why this matters:** The Business Research Agent runs a 6-event multi-step workflow per vertical (validation.started → research → spec.requested → spec.draft_ready → spec_review → approve/revise). With 3 verticals validating concurrently, a global session would interleave all three workflows. The BRA might confuse Vertical A's business brief with Vertical B's spec draft. `session_per_vertical` gives each vertical its own conversation thread — like a human researcher keeping separate project folders.

**OpCo agents don't need this** because they already have per-vertical instances (`ceo-{vertical_id}`). The agent ID itself is the isolation boundary. Factory pipeline agents are shared instances serving multiple verticals — they need the runtime to manage isolation.

**Rotation triggers:**
- **Context budget threshold:** turn count exceeds `llm.session.rotate_after_turns` (default: 50 for task-scoped, 200 for session-scoped)
- **Repeated parse/contract failures:** `llm.session.rotate_on_parse_failures` consecutive failures (default: 3) — session may be in a corrupted state
- **Explicit reset:** manager agent or human operator triggers rotation via `agent_reconfigure`
- **Phase boundary (optional):** when an operating vertical transitions build phases, CTO can rotate engineer sessions to start fresh with updated specs

**Single-writer enforcement:** The `SessionRegistry` uses database-level advisory locks (or row-level `lock_owner` + `lock_expires_at` with TTL). If a lease expires without release (crash), the next `Acquire` call reclaims it after the TTL window. This prevents two goroutines from writing to the same Claude session concurrently, which would corrupt conversation state.

### 4.5 Tool Execution

Agents run inside LLM coding environments (Claude Code, Codex, or equivalent) that provide native tools for file I/O, shell execution, and web access. EmpireAI adds domain-specific tools on top. See §13.2 for the full two-tier breakdown.

**Tier 1 (native LLM tools) — scoped by container environment:**

The runtime doesn't intercept native tool calls. Isolation is enforced by the Docker container the agent runs in (§4.1.1). Each vertical's agents share a container with volume mounts restricted to that vertical's directory. Holding DevOps runs in a privileged container with broader mounts. Factory CTO runs in a container scoped to the scaffold. Agents with no file-system needs run their LLM sessions without file volume mounts.

No path-checking code, no chroot wrappers, no allowlists. Docker volume mounts ARE the access control.

**Tier 2 (EmpireAI-specific tools) — injected and runtime-managed:**

Custom tools are registered into the LLM session via the standard tool-calling API. When the LLM returns a Tier 2 tool call:

1. Runtime deserializes the tool call
2. **Authorization check:** verifies tool is in agent's allowed set (YAML `tools:` list + universal `agent_message` + auto-generated `emit_*` tools). If not found, rejects and emits `spec.contradiction_detected`.
3. **Tenant isolation:** for `sql_execute`, runtime enforces schema scoping at the connection level (not by prompt). For organizational tools (`agent_message`, `agent_hire`), runtime enforces parent chain authorization.
4. **Credential injection:** for external service tools (`whatsapp_business_api`, `email_api`), runtime loads credentials from `verticals.credentials` (§13.1) and injects them into the API call. Agent never sees raw secrets.
5. Executes the function
6. Serializes result back to the LLM as a tool result
7. LLM continues its reasoning loop

**Database isolation (enforced regardless of tier):**

`sql_execute` is a Tier 2 tool (not native `psql`) specifically because schema isolation must be enforced per-agent. The runtime creates a connection pool scoped to the agent's `db_schema` via `SET search_path`. Agent cannot access other verticals' schemas. This is enforced at the connection level, not by prompt.

**Schema scoping by agent type:**
- **OpCo agents:** scoped to their vertical's schema (e.g., `pet_grooming`). Can only read/write their own data.
- **Holding agents with `sql_execute`** (Operations Analyst): scoped to `holding` schema which contains read-only views of cross-vertical data (routing_rules, events, agent_lifecycle, cost, heartbeats). Cannot write to any vertical schema.
- **Holding agents without `sql_execute`** (Empire Coordinator, Factory CTO): don't need direct DB access — they receive data via events and agent_message.

Tool definitions are part of each agent's config. Agent configs list only **per-agent** Tier 2 tools — native tools are available to all agents by default via the LLM environment.

**Universal Tier 2 tools (injected into every agent session automatically):**

`agent_message` is injected into every agent session regardless of the agent's YAML `tools:` list. Every agent needs to communicate with peers and managers — the org chart and bootstrap routes depend on it. The runtime injects `agent_message` alongside the agent's `emit_*` tools during session setup. The YAML `tools:` field lists only *additional* Tier 2 tools beyond this universal set.

`mailbox_send` is injected into every agent session. Any agent may need to escalate decisions to the human board member. The tool schema enforces valid types via enum — the LLM sees the allowed values in the tool definition.

**`mailbox_send` schema (enforced by MCP tool gateway):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string, enum | yes | One of: `review`, `escalation`, `spend_request`, `budget_increase`, `digest`, `vertical_approval`, `migration_approval`, `domain_approval` |
| vertical_id | string (UUID) | no | Vertical context (required for vertical_approval, migration_approval) |
| priority | string, enum | yes | One of: `low`, `normal`, `high`, `critical` |
| subject | string | yes | One-line summary |
| payload | object | yes | Decision context — contents vary by type |

The `type` enum matches the `mailbox_type_check` constraint in the database. Invalid types are rejected at the tool schema level before reaching the database — the agent sees the error and can retry with a valid type.

Authorization rules for `agent_message` still apply: the runtime enforces parent chain authorization (§4.5 Tier 2 step 3). An agent can message any agent in their vertical, but cross-vertical messaging requires holding-level authority.

```
Universal Tier 2 (all agents):     agent_message, mailbox_send
Per-agent Tier 2 (from YAML):      sql_execute, agent_hire, agent_fire, agent_reconfigure,
                                    configure_routing, whatsapp_business_api, email_api,
                                    human_task_request, domain_purchase, etc.
Auto-generated Tier 2:             emit_* tools (from `agent-tools.yaml` emit_events producer registry)
```

#### 4.5.1 Event Emission Tools

Agents emit events by calling typed `emit_*` tools rather than returning JSON envelopes. Each tool has a strict input schema that enforces the payload contract at the LLM API level.

**Tool generation:** At session start, the runtime generates `emit_*` tool definitions per agent from the Event Schema Registry + the agent's allowed emissions (`agent-tools.yaml` emit_events).

**Event schemas** are defined in `internal/runtime/event_emit_tools.go` (`EventSchemaRegistry`). Payload field definitions are in `contracts/event-catalog.yaml`. The `TestContractCompliance/schema_payload` gate (§17.3) enforces that schema properties cover all catalog payload fields, and that schemas with `additionalProperties: false` have exact field parity.

The registry maps each event type to a JSON Schema. At session start, the runtime generates `emit_*` tool definitions by combining the schema with the agent's allowed emissions (`agent-tools.yaml` emit_events). Each tool enforces its payload contract at the LLM API level — agents cannot emit malformed events.


**Tool handler (in tool executor):**

```go
func (te *ToolExecutor) handleEmitTool(agentID string, toolName string, input json.RawMessage) (string, error) {
    // Extract event type from tool name: "emit_scan_requested" → "scan.requested"
    eventType := strings.ReplaceAll(strings.TrimPrefix(toolName, "emit_"), "_", ".")

    // State transition check (Layer 2 guardrail)
    if err := te.validateTransition(agentID, eventType); err != nil {
        return "", err  // Tool error — LLM sees the rejection reason
    }

    // Construct and publish event
    event := Event{
        Type:        EventType(eventType),
        SourceAgent: agentID,
        Payload:     input,  // Already validated by tool calling schema
    }
    te.bus.Publish(event)

    return fmt.Sprintf("Event %s published (id: %s)", eventType, event.ID), nil
}
```

**What this replaces:**
- JSON envelope parsing from agent text responses → tool calls
- Layer 1 emission allowlist checking → tool presence in session
- Layer 3 payload normalization → tool input schema validation
- Payload format documentation in prompts → tool schema is the contract

**What remains unchanged:**
- Layer 2 state transition rules (checked in tool handler)
- EventBus, interceptor, pipeline coordinator (receive events identically)
- Event schema in Postgres (same `events` table)
- Runtime-emitted events (emitted by Go code, not by LLM tool calls)

### 4.6 Scheduler

The Scheduler provides timer-based wake-ups. Agents register schedules; the runtime fires events on time. Supports both recurring (cron) and one-shot (at) timers.

```go
type Scheduler struct {
    schedules []Schedule
    bus       *EventBus
}

type Schedule struct {
    AgentID    string
    EventType  EventType
    Mode       string           // "cron" | "once"
    Cron       string           // Recurring: cron expression (Mode="cron")
    At         time.Time        // One-shot: specific time (Mode="once")
    Payload    json.RawMessage
}

func (s *Scheduler) Register(schedule Schedule) error
func (s *Scheduler) Cancel(agentID string, eventType EventType) error
```

**Dynamic heartbeats and milestone-driven reporting:**

The system uses two timing mechanisms: **heartbeats** for gap detection and **reports** for upward visibility. Both are dynamic — agents adjust their own cadence based on activity density and business phase.

**Heartbeats (gap detection):**

Heartbeats catch things that fell through cracks — "did anyone forget to tell me something?" Their frequency should match the density of activity and the cost of delay.

Agents self-schedule their next heartbeat after each wake-up:

```
HEARTBEAT LOGIC:
1. Wake up. Check your domain for pending issues.
2. Take action if needed.
3. Schedule your next heartbeat based on what you found:

   HIGH FREQUENCY (30-60 min):
   - Pending cross-domain handoff (build done, launch hasn't happened)
   - Active build phase with multiple agents producing work
   - Customer-facing issue open and unresolved
   - You just installed a new route and want to verify it works

   NORMAL FREQUENCY (2-4 hours):
   - Active phase but no pending handoffs
   - Steady user growth, occasional bugs
   - Normal operations

   LOW FREQUENCY (8-24 hours):
   - Stable product, no active builds
   - No open bugs, no pending handoffs
   - Routing table is mature (events handle most notifications)
   - Quiet period
```

As the routing table matures (week 3+), heartbeats become less important — events wake agents directly instead of agents polling. A mature OpCo might have heartbeats every 12-24 hours, just as a safety net.

**Reports (upward visibility) — milestone-driven, not calendar-driven:**

Reports are triggered by **phase transitions and milestones**, not calendar cadence. The business doesn't care what day it is — it cares that the spec shipped, the product launched, the first customer churned.

*Phase transition triggers (always trigger a report):*

| Trigger | Who reports | To whom |
|---------|-----------|---------|
| Product spec complete | Head of Product | CEO |
| Technical spec approved | Head of Product | CEO |
| Build complete (first deploy) | Head of Product | CEO + Chief of Staff |
| Pre-launch ready | Head of Growth | CEO + Chief of Staff |
| Launch (both sides ready) | CEO | Empire Coordinator |
| First paying customer | Head of Growth | CEO |
| Product pivot or major feature ship | Head of Product | CEO |

*Metric milestone triggers (report when crossed):*

| Milestone | Who reports | Example thresholds |
|-----------|-----------|-------------------|
| User count | Head of Growth | 10, 25, 50, 100 users |
| Revenue | Head of Growth | $100, $500, $1000 MRR |
| First churn | Head of Product | Any churn event |
| Bug spike | Head of Product | 3+ bugs in 24 hours |
| Budget utilization | CEO | >80% of monthly budget |
| Growth stall | Head of Growth | <2 new users in 7 days |
| CSAT drop | Head of Product | CSAT < 3.5 |

*Maximum interval fallback (never go silent):*

If no milestone has triggered a report, a fallback timer ensures periodic check-in. This interval is itself dynamic:

| Business phase | Max interval between reports |
|---------------|---------------------------|
| Spec + build phase | 3 days |
| Launch week | 2 days |
| Active growth (new users arriving) | 7 days |
| Stable steady-state | 14 days |

Agents evaluate which phase they're in and set their fallback timer accordingly. The fallback report is lighter than a milestone report — "nothing significant since last report, here are the numbers."

**Implementation:**

Each manager agent maintains a `last_report_at` timestamp and a `current_phase` assessment. After each heartbeat or event, they evaluate:

```
Should I report now?
1. Did a phase transition just happen? → YES, report immediately
2. Did a metric cross a milestone threshold? → YES, report
3. Has max_interval for my current phase elapsed? → YES, fallback report
4. None of the above? → No report needed, continue
```

Managers use the `schedule` tool to set their next fallback timer. When a milestone triggers an early report, they reset the fallback timer.

**Chief of Staff cross-domain summary follows the same pattern:**
Triggered by: both VP reports arriving, cross-domain incident, launch coordination needed. Fallback: 3-7 days depending on phase. Not "Monday at 9am."

**CEO report to Empire Coordinator follows the same pattern:**
Triggered by: launch, major milestone, kill recommendation, both VP + CoS reports arriving. Fallback: 7-14 days depending on phase. Not "Monday at 10am."

**Default schedules (installed on spinup — initial heartbeats only):**

| Agent | Initial heartbeat | Purpose |
|-------|------------------|---------|
| Head of Product | 2 hours from spinup | First check — is PM working on spec? |
| Head of Growth | 2 hours from spinup | First check — is Marketing starting pre-launch? |
| Chief of Staff | 4 hours from spinup | First check — any cross-domain gaps? |
| CEO | 8 hours from spinup | First check — did VPs get started? |

No recurring schedules are installed. Each agent self-schedules their next wake-up after each heartbeat. No "Monday 8am" summaries — agents report when milestones trigger it or when max interval elapses.

CEOs and VPs can register additional schedules via the `schedule` tool.

### 4.7 Inbound Gateway

The Inbound Gateway translates external events (webhooks, emails, callbacks) into internal events.

```go
type InboundGateway struct {
    bus      *EventBus
    router   *http.ServeMux
}

func (ig *InboundGateway) RegisterWebhook(path string, handler WebhookHandler) error
```

Runs as a dedicated HTTP listener goroutine within the main runtime process (not a separate binary), on `gateway_port` (default 8080, configured in §13). Each vertical's external integrations register webhook endpoints:

**Event naming convention:** `inbound.{vertical_slug}.{provider}_{event_type}`. The vertical slug comes from the URL path. The provider and event type are determined by the gateway's translation layer. `{v}` is shorthand for `{vertical_slug}` in the table below.

| External Source | Webhook Path | Internal Event | Consumer |
|----------------|-------------|----------------|----------|
| WhatsApp Business API | `/webhooks/{vertical}/whatsapp` | `inbound.{v}.whatsapp_message` | Support Agent |
| Email (forwarding) | `/webhooks/{vertical}/email` | `inbound.{v}.email_received` | Support Agent |
| Domain registrar | `/webhooks/{vertical}/domain` | `inbound.{v}.domain_confirmed` | Marketing Agent |
| Stripe (future) | `/webhooks/{vertical}/stripe` | `inbound.{v}.payment_event` | PM Agent |

The gateway:
1. Receives external HTTP request
2. Validates authenticity — loads webhook secret from `verticals.credentials` (§13.1) for the target vertical, verifies signature using provider-specific scheme:
   - WhatsApp: HMAC-SHA256 of request body with app secret
   - Stripe/MercadoPago: Stripe-Signature header verification
   - Domain registrar: provider-specific token validation
   If signature is invalid, returns 401 and logs the attempt.
3. **Deduplicates** — extracts provider event ID (e.g., WhatsApp `message_id`, Stripe `event_id`) and checks against `inbound_events` table. Dedup uses PRIMARY KEY `(provider_event_id, vertical_id)` — a webhook with the same provider event ID is never processed twice, regardless of timing. Returns 200 OK for duplicates (providers retry on non-200). Cleanup cron purges rows older than 7 days to prevent unbounded table growth.
4. Extracts vertical ID from path (`/webhooks/{vertical_slug}/...` → lookup `verticals` table by slug)
5. Translates to internal event format: `inbound.{vertical_slug}.{provider}_{event_type}`
6. Writes to `inbound_events(provider_event_id, vertical_id, received_at)` for dedup tracking
7. Publishes to EventBus → routes to appropriate agent via routing table

This is shared infrastructure managed by Holding DevOps. Each OpCo's CTO configures their vertical's webhook registrations during build.

### 4.8 Org Template Versioning & Migration

The org template defines the default agent roster, prompts, tools, and routing for new verticals. Templates are **data, not code** — stored in the database and managed by Factory CTO.

**Authoring vs execution surface:**

```
configs/agents/*.yaml       ← Human-editable, git-tracked (authoring surface)
        │
        ▼
empire template publish     ← CLI command, triggers Spec Auditor validation
        │
        ▼
org_templates (Postgres)    ← Runtime source of truth (execution surface)
        │
        ▼
SpawnOpCo reads from here   ← No file I/O during spawn
```

YAML files under `configs/agents/` are the authoring surface: reviewable, diffable, version-controlled. `empire template publish` reads the YAML, validates via Spec Auditor, and writes to `org_templates` in Postgres. `SpawnOpCo` and migrations read exclusively from Postgres. Same pattern as Kubernetes manifests → etcd.

For holding/factory agents, the same principle applies: `configs/agents/roster.yaml` defines the seed roster (Empire Coordinator, Factory CTO, Holding DevOps, Operations Analyst, factory pipeline agents). `empire init` reads this file and creates the initial agent rows. No hardcoded agent definitions in Go code.

```bash
empire template publish --version 1.2 --description "Add security agent to deploy flow"
# Reads configs/agents/*.yaml, validates, writes to org_templates
# Emits spec.validation_requested → Spec Auditor

empire template list                    # Show all published versions
empire template diff v1.1 v1.2         # Show what changed
empire template current                 # Show version used for next SpawnOpCo
```

```go
type OrgTemplate struct {
    Version     string            // Semantic: "1.0", "1.1", "2.0"
    Agents      []AgentTemplate   // Role definitions with prompts, tools, constraints
    Bootstrap   []RouteTemplate   // Bootstrap routes (immutable for running verticals)
    Seeded      []RouteTemplate   // Seeded routes (removable)
    CreatedBy   string            // Factory CTO agent ID or "initial"
    CreatedAt   time.Time
    Description string            // What changed and why
}

type AgentTemplate struct {
    Role           string         // e.g., "cto", "support", "security"
    ParentRole     string         // e.g., "head_of_product", "ceo"
    Type           string         // model tier: "sonnet" | "haiku"
    SystemPrompt   string         // Full system prompt template (with {vertical_name} etc.)
    Tools          []string       // Tool names
    Subscriptions  []string       // Bootstrap event subscriptions
    Constraints    AgentConstraints
}

type RouteTemplate struct {
    EventPattern   string         // e.g., "build_complete"
    SubscriberRole string         // e.g., "head_of_product" (resolved to agent ID at spinup)
    Reason         string
}
```

**SpawnOpCo reads from template:**
```go
func (am *AgentManager) SpawnOpCo(verticalID string, mandate MandateDocument) error {
    template := am.GetCurrentTemplate()
    
    for _, at := range template.Agents {
        agentID := fmt.Sprintf("%s-%s", at.Role, verticalID)
        config := at.ToAgentConfig(verticalID, mandate)
        am.SpawnAgent(config)
    }
    
    am.InstallRoutes(verticalID, template.Bootstrap, template.Seeded)
    am.SetVerticalTemplateVersion(verticalID, template.Version)
}
```

**Template publish flow (with validation gate):**

```
Factory CTO drafts new template version
    ↔
Emits spec.validation_requested (spec_type: template)
    ↔
Spec Auditor validates:
  - All agent prompts reference only tools in their tool list
  - All bootstrap/seeded subscriptions use correct event names
  - Agent parent_role chains are acyclic and complete
  - No two agents own the same tool exclusively
  - Prompt instructions match routing table expectations
    ↔
spec.validation_passed → Factory CTO publishes (template.version_published)
spec.validation_failed → Factory CTO receives issue catalog, fixes, resubmits
```

**Migration flow (template v1 → v2 on running verticals):**

```
Factory CTO publishes new template version
    ↔
Empire Coordinator receives template.version_published event
    ↔
For each running vertical where template_version < new version:
    ↔
Empire Coordinator generates migration plan:
    1. Diff agents: added roles, removed roles, changed prompts/tools/constraints
    2. Diff routes: new bootstrap routes, new seeded routes, removed routes
    3. Prompt patches: which running agents need reconfiguration
    ↔
Migration plan → Mailbox for human approval
    (human sees: "Template v1.1→v1.2 for PeluquePet:
     ADD security-peluquepet (Haiku, advisory, subscribes to deploy_requested)
     RECONFIGURE cto-peluquepet (add security_gate to deploy flow prompt)
     ADD ROUTE: deploy_requested → security agent (seeded)")
    ↔
On approval, Empire Coordinator executes plan using existing primitives:
    - SpawnAgentFor() for new agents
    - ReconfigureAgent() for prompt/tool changes  
    - TeardownAgent() for removed agents
    - EventBus.SetRoutingTable() for route changes
    ↔
Vertical's template_version updated to new version
```

**Migration execution contract (implementation requirement):**

The "execute plan" step above is not optional — it is the migration. Updating `verticals.template_version` without executing the primitives is a no-op. The runtime must:

1. **Diff** the old template against the new template to produce a migration plan (list of concrete operations)
2. **Execute each operation** in order:
   - `ADD_AGENT`: call `AgentManager.SpawnAgent()` with new agent config from template. Verify agent appears in `agents` table with correct `vertical_id`.
   - `REMOVE_AGENT`: call `AgentManager.StopAgent()`. Verify agent marked `terminated`.
   - `RECONFIGURE_AGENT`: call `AgentManager.ReconfigureAgent()` with new prompt/tools/constraints. This triggers session rotation on the target agent.
   - `ADD_ROUTE`: call `EventBus.SetRoutingTable()` to insert new routing rule. Source is `template_migration`.
   - `REMOVE_ROUTE`: call `EventBus.SetRoutingTable()` to deactivate routing rule. Only `source: 'seeded'` routes can be removed by migration. Bootstrap routes are immutable (see routing governance below).
3. **Update version** only after all operations succeed: `UPDATE verticals SET template_version = {new_version} WHERE id = {vertical_id}`
4. **Emit** `template.migration_complete` event with migration details
5. **On failure**: stop execution, emit `template.migration_failed`, alert mailbox. Partial migrations leave the vertical in a mixed state — the human decides whether to retry or fix forward.

**What migrations CAN do (no Go code changes):**
- Add/remove agent roles (spawn/teardown)
- Change agent prompts, tools, constraints, model tier (reconfigure)
- Add/remove routing rules
- Change budget allocations

**What migrations CANNOT do (requires Go code changes):**
- New tool implementations (new Go functions in tool registry)
- New communication primitives
- Changes to the runtime's authorization model
- Changes to the EventBus delivery mechanics

**Template evolution governance:**
- Factory CTO owns template updates. The Operations Analyst proposes changes based on cross-vertical learning. Factory CTO reviews and publishes.
- Version numbering: minor (1.0 → 1.1) for prompt refinements and route changes. Major (1.x → 2.0) for role additions/removals.
- Migrations are always opt-in via mailbox. Human can approve per-vertical or batch-approve.
- Running verticals are never forced to migrate. A vertical can run on template v1.0 indefinitely while new verticals spawn on v2.0.
- Roll-forward only. If a migration breaks a vertical, fix it with a new template version (v1.3), don't roll back to v1.1.

---

## 5. Communication Model

### 5.1 Three Primitives

Agents communicate through three distinct mechanisms. Each has different semantics.

**Events** — facts about what happened. Emitted by any agent, routed by the EventBus according to subscription (factory) or routing table (operating). The emitter doesn't choose the recipient. Events are: `bug_reported`, `feature_deployed`, `spec_ready`, `user_onboarded`. Use for: automated peer-to-peer workflows, status changes, audit trail.

**Messages** — directives from a manager. Sent intentionally from a manager to a specific agent. The sender chooses the recipient and the content. Messages are: "prioritize the payments bug", "shift outreach to Instagram", "start building from this spec". Use for: manager → report instructions, strategic direction, intervention.

**Tasks** — discrete work units with review cycles (factory only). A coordinator assigns structured work, the worker executes and submits for review, the coordinator approves or requests revision. Tasks are: "score this vertical", "research this market", "write MVP spec for this brief". Use for: factory pipeline stages where quality gates matter. NOT used in operating mode — operating agents have autonomy within their role.

### 5.2 Four Wake-Up Sources

An agent's goroutine blocks until one of four things delivers input:

### 5.2.1 Event Naming Convention

All events follow a consistent naming scheme:

**Factory events** — `{domain}.{action}`:
`scan.requested`, `vertical.discovered`, `scoring.requested`, `spec.draft_ready`, `research.completed`

**Holding infrastructure events** — `devops.{action}`:
`devops.deploy_requested`, `devops.deploy_complete`, `devops.health_check_failed`

**Template versioning events** — `template.{action}`:
`template.version_published`, `template.migration_planned`, `template.migration_completed`

**Spec validation events** — `spec.{action}`:
`spec.validation_requested`, `spec.validation_passed`, `spec.validation_failed`, `spec.contradiction_detected`

**OpCo lifecycle events** — `opco.{action}` (vertical_id is in the Event payload, not the name):
`opco.spinup_requested`, `opco.launched`, `opco.ceo_report`, `opco.spend_request`

**Internal OpCo events** — `{action}` (short names, scoped to vertical via routing table):
`bug_reported`, `feature_deployed`, `build_complete`, `support_digest`, `spend_needed`, `qa.validation_passed`, `qa.validation_failed`

The EventBus applies vertical scoping: when an internal OpCo event needs to cross vertical boundaries (e.g., for Empire Coordinator or Operations Analyst), it's qualified as `opco.{vertical_id}.{action}`. Agents within a vertical subscribe to short names; holding-level agents subscribe to the qualified form.

Human-facing events use the `opco.` prefix when displayed in digest or mailbox.

### 5.2.2 Wake-Up Sources

| Source | Mechanism | Example |
|--------|-----------|---------|
| **Event** | EventBus delivers to subscribed channel | `bug_reported` arrives at CTO |
| **Message** | Any agent calls `agent_message` tool | VP tells CTO "drop everything, fix payments"; Backend tells CTO "blocked on spec clarification" |
| **Timer** | Scheduler fires at configured time | VP heartbeat (dynamic), milestone report fallback |
| **External** | Inbound Gateway translates webhook to event | Customer WhatsApp message → `inbound.{v}.whatsapp_message` event |

All four ultimately result in the same thing: content is appended to the agent's conversation and a reasoning step is triggered. The difference is the source and semantics.

### 5.3 Event Structure

```go
type Event struct {
    ID          string          `json:"id"`           // DB: UUID, Go: string (UUID format)
    Type        EventType       `json:"type"`
    SourceAgent string          `json:"source_agent"` // Agent ID that emitted this event
    TaskID      string          `json:"task_id"`      // DB: UUID, Go: string (UUID format, empty if not task-related)
    VerticalID  string          `json:"vertical_id"`  // DB: UUID, Go: string (UUID format)
    Payload     json.RawMessage `json:"payload"`
    CreatedAt   time.Time       `json:"created_at"`
}
```

### 5.4 Event Catalog Summary

Complete event definitions (emitter, consumer, consumer_type, delivery_channel, payload fields) are maintained in `contracts/event-catalog.yaml` — the authoritative source. The spec prose below provides a domain-level summary for orientation. When the summary and the YAML disagree, the YAML wins.

**172 events across 15 domains:**

| Domain | Primary Delivery | Key Events |
|--------|-----------------|------------|
| System | eventbus_static | `system.started`, `system.directive` — only events a human can directly emit |
| Discovery | runtime, eventbus_static | `scan.requested` → `category.assessed` → `vertical.discovered`. Includes corpus mode, dedup, synthesis |
| Scoring | runtime, eventbus_static | `scoring.requested` → `score.dimension_complete` → `vertical.scored` → `vertical.shortlisted`. Includes derivation loop |
| Validation | runtime, eventbus_static, mailbox | `research.*`, `spec.*`, `brand.*` → 4-gate pipeline → `vertical.ready_for_review` |
| Human Decision | mailbox, eventbus_static | `review.requested` → `review.decided`. Board directives, founder input |
| Factory CTO | eventbus_static, agent_message | Architecture decisions, spec feasibility, template upgrades |
| Holding DevOps | eventbus_static, mailbox | Deploy, migration, infra provisioning, rollback |
| Vertical Lifecycle | eventbus_static, mailbox | `opco.spinup_requested` → `opco.ceo_ready` → agent hiring/firing/reconfiguring |
| OpCo Internal | eventbus_routing_table | Bug reports, builds, deploys, features, launches, growth, support, market signals |
| Human Task | eventbus_static, agent_message | `human_task.requested` → approved/rejected → `human_task.completed` (§14) |
| Budget & Spend | eventbus_static | Token budget alerts, cost attribution |
| Template | eventbus_static | Org template versioning, migration triggers |
| Timer | internal | Marginal review, scan timeout, campaign deadline, portfolio digest |
| Ops & Portfolio | eventbus_static, audit | Portfolio digest, cross-vertical learning |
| Runtime | runtime | Dead letter, pipeline diagnostics |

*Exact event counts per domain: see `contracts/event-catalog.yaml`. This table is for orientation; the YAML is authoritative.*

**Delivery channel patterns:**
- **runtime** — consumed by PipelineCoordinator/ScoringNode interceptor; never reaches subscribers directly
- **eventbus_static** — delivered via static subscriptions declared in agent config
- **eventbus_routing_table** — delivered via per-vertical routing_rules table (OpCo events)
- **mailbox** — written to mailbox table for human decision
- **agent_message** — point-to-point via `agent_message` tool
- **audit** — written to events table only, no active consumer

### 5.7 Communication Graph

Complete topology of agent-to-agent communication across all three primitives (§5.1). The event catalog summary (§5.4) provides domain-level orientation; full event definitions are in `contracts/event-catalog.yaml`. This section provides the consolidated graph: every edge between agents, classified by primitive type. Use for visualization, Spec Auditor validation, and onboarding.

Four edge types exist in the system:

1. **Event edges** — pub/sub via EventBus. Emitter doesn’t choose recipient; routing table resolves subscribers.
2. **Message edges** — directives via `agent_message` tool. Manager chooses recipient. Follows org hierarchy.
3. **Mailbox edges** — async human decision loops. Agent → mailbox → human → back to agent.
4. **Route edges** — bootstrap (immutable) and seeded (removable) prescribed routing installed on OpCo spinup.

Agent YAML configs declare `subscriptions:` (event consumer side). The graph below adds the producer side, the message authority relationships, and the mailbox round-trips.

**Graph data sources (all in contracts):**

- **Event edges:** `event-catalog.yaml` — emitter, consumer, delivery_channel per event. `emitter_type` classifies as agent/system_node/runtime/human/opco_agent.
- **Producer registry:** `agent-tools.yaml` emit_events per agent + `system-nodes.yaml` produces per system node.
- **Message authority:** `agent-tools.yaml` — org hierarchy determines who can send directives to whom via `agent_message`.
- **Mailbox round-trips:** `event-catalog.yaml` — events with `delivery_channel: mailbox` (review.requested → review.decided, vertical.ready_for_review → vertical.approved/killed).
- **Route edges:** `configs/agents/templates/routes.yaml` — 20 bootstrap routes (immutable) + 7 seeded routes (removable). Bootstrap routes prevent deadlocks; seeded routes cover day-1 common sense.

The `TestContractCompliance/emit_events` and `TestContractCompliance/subscriptions` gates validate that code matches these contract definitions.

### 5.8 System Nodes (v2.0.37)

Not all participants in the communication graph are LLM-powered agents. System nodes are deterministic Go components that subscribe to events and publish events through the same EventBus as agents, but execute fixed logic rather than LLM reasoning.

System nodes appear in the communication graph as event producers and consumers. From the EventBus perspective, they are indistinguishable from agents — they have subscriptions, they receive events, they emit events. The difference is internal: system nodes have no system prompt, no model tier, no context window, and no conversation history.

**Current system nodes (v2.0.37):**

| Node | Subscribes to | Produces | Section |
|------|--------------|----------|---------|
| ScoringNode | `vertical.discovered`, `vertical.derived`, `score.dimension_complete`, `scoring.contest_resolved` | `scoring.requested`, `vertical.scored`, `vertical.shortlisted`, `vertical.marginal`, `vertical.rejected`, `scoring.contested`, `vertical.discovered` (derived re-emission), `pipeline.dead_letter` | §4.2.2.8 |

**Future system nodes (RFC-001 v2, Phases 2-4):** The remaining interceptor cases (validation pipeline, discovery accumulation, scan campaigns, directive translation) are candidates for migration to system nodes. These remain in the interceptor middleware until parity with system node execution is proven for the scoring pipeline.

System nodes have their own idempotency and transaction guarantees (§4.2.2.10). They are defined in the `system-nodes.yaml` contract and listed in `agent-tools.yaml` with `node_type: system`.

### 5.9 Workflow Architecture (v2.1.0)

The vertical pipeline is a formally defined state machine. Three layers separate concerns:

**Platform layer** — the workflow engine. Reads `workflow-schema.yaml`, enforces transitions, evaluates guards, executes actions, manages timers. Generic and reusable across any multi-agent workflow.

**Workflow layer** — `workflow-schema.yaml` + `guard-action-registry.yaml`. Defines 18 stages, 27 transitions, 22 guards, 13 actions, and 5 timers for the vertical pipeline. Each transition declares: trigger event, owning node, guards (must all pass), and actions (executed on fire).

**Policy layer** — `prompt-variables.yaml` + scoring config in `system-nodes.yaml`. EmpireAI-specific thresholds, enum values, and business rules. Guards reference policy values via `policy_ref` — changing a threshold in `prompt-variables.yaml` changes the guard's behavior without modifying the workflow definition.

**Key contracts:**

- `workflow-schema.yaml` — stages, transitions, timers. The state machine definition.
- `guard-action-registry.yaml` — named code-backed behaviors. Guards are boolean checks; actions are side effects. Each is categorized as `platform` (generic) or `empire` (business-specific).
- `system-nodes.yaml` — each node declares `owned_transitions` cross-referencing workflow-schema.yaml.

**Compliance:** Every transition in workflow-schema.yaml must have a handler in code. Every guard/action ID must resolve in the registry. Every transition node must exist in agent-tools.yaml or system-nodes.yaml. Terminal stages must have no outgoing transitions unless explicitly overridden.

### 5.10 Platform Abstraction (v2.1.0)

The orchestration engine is generalizing into a reusable multi-agent workflow platform. EmpireAI becomes one workflow running on this platform.

**Platform spec:** `contracts/platform/platform-spec.yaml` defines the contract formats, vocabulary, compliance rules, and built-in hooks that any workflow can use. The platform provides: stage machines, transition guards, action execution, timer management, workflow state persistence, schema validation, and event routing.

**Contract format convention:** Workflow files follow the pattern `{type}-{workflow_name}.yaml`:

| Format | EmpireAI file | Platform pattern |
|--------|--------------|-----------------|
| Workflow definition | workflow-schema.yaml | workflow-{name}.yaml |
| Hook registry | guard-action-registry.yaml | hooks-{name}.yaml |
| Node definitions | system-nodes.yaml | nodes-{name}.yaml |
| Event catalog | event-catalog.yaml | events-{name}.yaml |
| Agent registry | agent-tools.yaml | agents-{name}.yaml |
| Tool schemas | tool-schemas.yaml | tools-{name}.yaml |
| Policy values | prompt-variables.yaml | policy-{name}.yaml |

**Compliance rules:** 16 rules across 6 categories (graph structure, hook resolution, participant existence, event consistency, node coverage, wiring). The platform runs these at startup and in CI against any workflow definition.

**Built-in hooks:** 5 platform guards (has_entity_id, not_in_terminal_stage, revision_count_below_limit, has_human_decision, stage_in_phase) and 5 platform actions (increment_revision_count, record_transition, update_stage, cancel_stage_timers, start_stage_timers). Available to all workflows without declaration.

See `platform-spec.yaml` for the complete specification.

---

## 6. Factory Pipeline

The factory's job is speed and signal: determine if a vertical is worth operating, as cheaply as possible. The factory produces a **lightweight MVP spec** — just enough to prove the thesis is sound and the product is technically feasible. No code is written. The full product spec and all building happens in operating mode after human approval.

**Factory Spec (Lightweight):** Core workflow (#1 pain point), 3-5 features, happy path only, data model sketch, one integration.

**Operating Spec (Full):** All workflows, all personas, complete features with priorities, full data model, integrations, edge cases, billing, admin, onboarding. Living document.

### 6.1 Pipeline Flow

```
scan.requested → Discovery → vertical.discovered → Scoring → vertical.shortlisted
                                                                      │
                                                                      ▼
                                                            Validation Coordinator
                                                                      │
                                         ┌────────────────────────────┤
                                         ▼                            ▼
                                  Business Research            Pre-Brand Agent
                                         │                    (runs in parallel)
                                  Business Brief                      │
                                         │                    Brand candidates
                                  Lightweight Spec Agent              │
                                   (MVP only: core workflow,          │
                                    3-5 features, happy path)         │
                                         │                            │
                                  Business Research                   │
                                    reviews spec                      │
                                         │                            │
                                    Spec Reviewer                     │
                                   (single pass: does it              │
                                    solve #1 pain point?)             │
                                         │                            │
                                ┌────────▼────────┐                   │
                                │ Factory CTO:    │                   │
                                │ Spec Review     │                   │
                                │ (technically    │                   │
                                │  feasible?)     │                   │
                                ┘────────┬────────┘                   │
                                         │                            │
                                         ◄────────────────────────────┘
                                         │
                                  vertical.ready_for_review
                                  (validation kit = documents only,
                                   no running code)
                                         │
                                         ▼
                                      MAILBOX
                                   (human decides)
                                         │
                              ┌──────────┼──────────┐
                              ▼          ▼          ▼
                           approve     kill     more-data
                              │
                              ▼
                     Spin up Operating Company
                     (PM → product spec → CTO/Tech Writer → technical spec → Build → Launch)
```

### 6.1.1 Corpus Discovery Mode (v2.1.0)

Corpus mode feeds pre-collected demand signals into the same discovery pipeline that taxonomy-walking and trend-scanning use. Instead of generating signals from scratch, the MRA interprets raw signals from a JSONL file into `category.assessed` events. Everything downstream (pre-filter, scoring, validation, EC digest) is unchanged.

**How it works:**

```
Human (on own schedule):
  → Scrape job boards, browse app stores, read forums, check API changelogs
  → Dump signals into a JSONL file on disk

empire directive "US, corpus, corpus_path=/data/signals-2026-03.jsonl"
  → EC parses directive, emits scan.requested with mode=corpus, corpus_path=...
  → Runtime reads JSONL, batches signals (25 per batch)
  → MRA receives batches, interprets each signal into category.assessed
  → Same pre-filter cascade (stages 1-9)
  → Same scoring pipeline (11-dim universal rubric)
  → Same EC digest
  → Same mailbox flow
```

**Corpus file format** — one JSON object per line, minimal required fields:

```jsonl
{"source": "talent.com", "category": "billing", "title": "Billing Clerk @ HVAC Co", "signal": "12 duties, $42K salary, uses Excel+QuickBooks", "url": "https://...", "scraped_at": "2026-03-01"}
{"source": "reddit", "category": "shopify_tools", "title": "Is there a tool to bulk edit Shopify metafields?", "signal": "47 upvotes, 23 comments", "url": "https://...", "scraped_at": "2026-03-01"}
```

Required fields: `source`, `title`, `signal`, `url`, `scraped_at`. Optional fields the MRA can use if present: `category`, `salary`, `duties[]`, `required_tools_mentioned[]`, `qualifications[]`, `company_name`, `company_size`, `industry`. The MRA's job is to interpret raw signals — it can web-search for missing context.

**What changes:** discovery_mode enum (add `corpus`), scan.requested payload (add `corpus_path`), expectedAgentsPerMode (add `corpus: 1`), completionSignals (add `corpus`), MRA prompt variant (§B.10.1), runtime JSONL reader + batch dispatcher.

**What does NOT change:** Scoring pipeline, pre-filter cascade, EC digest, validation pipeline, DDL (no new tables — corpus is a file, not a database), rejection cascade, hard gates.

**Validated by:** Two corpus campaigns (228 signals at 10.5% viable rate, 390 signals at 29.1% viable rate) using job board signals. The 3x improvement in viable rate between runs was driven by input targeting, not pipeline changes — confirming the "better inputs produce better outputs" thesis.

### 6.2 Business Brief (Source of Truth)

The Business Brief is produced by the Business Research Agent. Structure:

```yaml
business_brief:
  vertical: "pet-grooming"
  geography: "Cancún, Mexico"
  
  market_reality:
    business_count_estimate: 180
    typical_team_size: "1-4 people"
    revenue_range: "$500-3000/month"
    digitization_level: "WhatsApp + paper notebooks"
    primary_language: "Spanish"
    device_usage: "95% mobile, 5% desktop"
  
  pain_points_ranked:
    1:
      pain: "No-shows and last-minute cancellations"
      evidence: "Found in 34/50 Google reviews analyzed"
      current_workaround: "Manual WhatsApp reminders, often forgotten"
      money_lost: "~$200-500/month estimated"
    2:
      pain: "Can't track which clients owe money"
      evidence: "Common complaint in Facebook group discussions"
      current_workaround: "Paper notebook, sometimes lost"
    3:
      pain: "Scheduling conflicts when multiple groomers"
      evidence: "Mentioned in 12 reviews, confirmed by competitor feature set"
      current_workaround: "Shared WhatsApp group, verbal coordination"
  
  willingness_to_pay:
    existing_digital_spend: "WhatsApp Business (free), some pay $5-10/mo for Facebook ads"
    price_sensitivity: "High — small businesses, cash-flow constrained"
    replacing_paid_or_free: "Free workaround (WhatsApp + paper)"
    impulse_threshold: "Under $15/mo is impulse. Over $20 requires deliberation."
    roi_clarity: "Clear — 'save $200-500/mo in no-shows for $10/mo' is easy math"
    evidence: "3/5 competitor reviews mention price as reason for cancellation"
    score_assessment: "Moderate — clear ROI but replacing free tools, price-sensitive"

  retention_signals:
    usage_frequency: "Daily — checking appointments every morning"
    data_accumulation: "Client history, appointment records, payment tracking"
    switching_cost: "Medium — 3+ months of client data makes switching painful"
    team_dependency: "Low for solo groomers, medium for 2-4 person shops"
    habit_formation: "High — replaces morning routine (check WhatsApp → check app)"
    score_assessment: "Strong — daily use + data accumulation = sticky"

  channel_access:
    primary: 
      channel: "Instagram DMs"
      reachability: "90% have business accounts, respond to DMs within 24h"
      agent_feasibility: "High — structured DMs, clear pitch, no gatekeepers"
    secondary:
      channel: "WhatsApp Business groups"
      reachability: "Most are in 2-3 local groomer groups"
      agent_feasibility: "Medium — need organic entry, can't cold-message groups"
    tertiary:
      channel: "Google Maps outreach"
      reachability: "70% have listings with phone numbers"
      agent_feasibility: "Medium — WhatsApp message to listed number"
    concentrated_geography: true
    community_spaces: "Facebook groups (3 active in Cancún area), WhatsApp groups"
    score_assessment: "Strong — multiple reachable channels, concentrated geography"

  operational_friction:
    onboarding_complexity: "Low — sign up, add clients, start scheduling"
    steps_to_first_value: 3  # sign up → add first client → first reminder sent
    data_migration: "None required — they start fresh (paper records don't transfer)"
    integration_requirements: "WhatsApp API only"
    expected_support_burden: "Low — simple workflow, similar to tools they already use"
    training_needed: "Minimal — if they use WhatsApp, they can use this"
    score_assessment: "Low friction — simple onboarding, minimal support expected"
  
  competitor_analysis:
    - name: "Petly Plans"
      price: "$49/month"
      weakness: "English only, US-focused, too expensive"
    - name: "DaySmart Pet"
      price: "$29/month"
      weakness: "No WhatsApp integration, English UI"
    - name: "Manual spreadsheets"
      prevalence: "~70% of businesses"
      weakness: "Not real software, but free"
  
  pricing_anchor:
    what_they_pay_for_workarounds: "$0-15/month (WhatsApp Business, notebook)"
    recommended_price: "$12-18/month"
    justification: "Must be cheaper than cheapest competitor, deliver clear ROI"
  
  acquisition_channels:
    primary: "Instagram DMs (90% have business accounts)"
    secondary: "Google Maps outreach"
    tertiary: "Facebook group posts"
  
  workflow_map:
    morning: "Check WhatsApp for messages, confirm today's appointments"
    midday: "Groom pets, handle walk-ins, take payment (cash/transfer)"
    evening: "Send tomorrow's reminders manually, update notebook"
    weekly: "Count earnings from notebook, buy supplies"
  
  critical_constraints:
    - "Must work on mobile — they don't use desktops"
    - "Must integrate with WhatsApp — it's their primary tool"
    - "Must support cash + bank transfer tracking, not just card payments"
    - "Spanish-only UI"
    - "Must be simpler than a spreadsheet or they won't switch"
  
  kill_signals:
    - "If fewer than 50 businesses found in geography"
    - "If all competitors are free/very cheap with good UX"
    - "If the pain is mild (nice-to-have, not need-to-have)"
    - "If operational viability score < 65 (high friction, low retention, or unreachable)"
    - "If willingness to pay evidence is absent (no digital spend, replacing free tools, vague ROI)"
  
  verdict: "PROCEED" | "KILL"
  verdict_reasoning: "..."
```

### 6.3 Validation Kit (Delivered to Mailbox)

When a vertical completes the factory pipeline, the human receives:

```yaml
validation_kit:
  executive_summary:
    vertical: "Pet Grooming Scheduling"
    geography: "Cancún, Mexico"
    composite_score: 82
    operational_viability_score: 78  # Must be ≥65 to proceed
    market_attractiveness_score: 88
    scoring_breakdown:
      # Primary: Operational Viability (60%)
      willingness_to_pay: { score: 72, weight: "20%", note: "Clear ROI but replacing free tools" }
      retention_likelihood: { score: 85, weight: "15%", note: "Daily use, data accumulates, habit-forming" }
      channel_access: { score: 82, weight: "15%", note: "Instagram DMs + WhatsApp groups, concentrated geo" }
      operational_friction: { score: 78, weight: "10%", note: "3 steps to value, no data migration, low support" }
      # Secondary: Market Attractiveness (40%)
      business_density: { score: 88, weight: "12%", note: "180 businesses in geography" }
      pain_severity: { score: 92, weight: "10%", note: "$200-500/mo lost to no-shows" }
      competition_weakness: { score: 90, weight: "10%", note: "No Spanish, no WhatsApp, too expensive" }
      revenue_per_business: { score: 75, weight: "8%", note: "$10-15/mo, needs 5-7 users to break even" }
    business_brief_verdict: "PROCEED"
    cto_verdict: "TECHNICALLY FEASIBLE"
    thesis: "180 pet groomers losing $200-500/mo to no-shows, 
             no competitors in Spanish, WhatsApp-native"
  
  brand_candidates:
    recommended:
      name: "PeluquePet"
      domain: "peluquepet.com"  # available
      instagram: "@peluquepet"  # available
      tagline: "Tu agenda de peluquería, sin estrés"
      colors: { primary: "#4A90D9", secondary: "#F5A623" }
    alternatives:
      - name: "MascotaCita"
        domain: "mascotacita.com"
      - name: "GroomFácil"
        domain: "groomfacil.mx"
  
  documents:
    business_brief: { ... }
    mvp_spec: { ... }
    spec_review: { passed: true, reviewer_notes: "Core workflow solid, addresses #1 pain" }
    cto_feasibility: "Standard CRUD + scheduling + WhatsApp integration. 
                      Straightforward build. Estimate 1-2 weeks for CTO agent.
                      WhatsApp pattern will be reusable across verticals."
  
  go_kill_criteria:
    go_if: "You believe pet groomers in Cancún will pay $10-15/mo 
            to reduce no-shows. Viability score (78) shows: daily use,
            low friction, reachable channels."
    kill_if: "Market is too price-sensitive for any paid tool, or you 
              believe retention will be low (they'll revert to WhatsApp)"
    viability_flag: null  # Set if any viability dimension scored <50
  
  estimated_operating_cost:
    build_time: "1-2 weeks (CTO agent)"
    monthly_api_budget: "$25-50"
    domain_cost: "$12/year"
    whatsapp_api: "$15-30/month (estimated at 200 conversations)"
    total_monthly: "$52-92"
  
  revenue_potential:
    at_10_users: "$150-180/month"
    at_50_users: "$750-900/month"
    breakeven_users: "4-7"
```

**Human response options (see §2.3):**

The validation kit is a conversation, not a rubber stamp. The human can:

```bash
# Approve as-is
empire mailbox decide <id> --action approve --brand peluquepet

# Approve with mandate edits (most common for early verticals)
empire mailbox decide <id> --action approve --brand peluquepet \
  --mandate-edit "pricing: start at $10/mo not $15, these are small businesses" \
  --mandate-edit "focus: no-show reduction only, do NOT build payment processing" \
  --mandate-edit "positioning: emphasize WhatsApp-native, they already live there" \
  --notes "I know this market. Groomers won't pay $15 but $10 is impulse pricing."

# Request more data before deciding
empire mailbox decide <id> --action more-data \
  --notes "How many groomers already use WhatsApp Business? That affects onboarding."

# Kill
empire mailbox decide <id> --action kill --notes "TAM too small"
```

Mandate edits are included in the mandate document that the CEO receives at spinup. The CEO's prompt tells them: "The human has shaped this mandate based on market knowledge. These constraints and directions are strategic — follow them."

The mandate document structure:

```yaml
mandate:
  # From factory pipeline:
  business_brief: { ... }
  mvp_spec: { ... }
  brand: { name: "PeluquePet", domain: "peluquepet.com", ... }
  budget: { monthly_cap: 200, ... }
  infrastructure: { port: 8003, schema: "peluquepet", ... }
  
  # From human (founder directives):
  founder_directives:
    - "Pricing: start at $10/mo, not $15. These are small businesses."
    - "Focus: no-show reduction only. Do NOT build payment processing."
    - "Positioning: emphasize WhatsApp-native workflow."
  founder_notes: "I know this market. The pain is real but price sensitivity is high."
```

Founder directives carry weight: agents treat them as strategic constraints from the board, not suggestions. The CEO can recommend changes via mailbox if market data contradicts a directive, but doesn't override them unilaterally.

### 6.4 Infrastructure

**Hetzner Box Layout** (shared across all operating verticals, managed by Holding DevOps):
```
/opt/empireai/
├── scaffold/                    # Standard project template (managed by Factory CTO + Holding DevOps)
│   ├── cmd/server/main.go      # Boilerplate: config, HTTP server, graceful shutdown
│   ├── internal/
│   │   ├── config/config.go    # Standard config pattern
│   │   ├── database/db.go      # Standard Postgres connection pool
│   │   ├── handlers/           # Empty — Backend fills in
│   │   ├── models/             # Empty — Backend fills in
│   │   ┘── whatsapp/           # Standard WhatsApp client boilerplate
│   ├── web/
│   │   ├── templates/          # Empty — Frontend fills in
│   │   ┘── static/             # Empty — Frontend fills in
│   ├── specs/                   # CTO writes specs here so engineers can file_read
│   │   ├── product-spec.md     # CTO writes after PM delivers product spec
│   │   ├── tech-spec.md        # CTO writes after Tech Writer delivers technical spec
│   │   ┘── qa-checklist.md     # CTO writes for QA from tech spec (test scenarios, endpoints, journeys)
│   ├── deploy/
│   │   ├── service.template    # Systemd template
│   │   ┘── nginx.template      # Nginx template
│   ├── schema.sql              # Empty — Backend fills in
│   ├── Makefile                # Standard build/deploy targets
│   ┘── go.mod                  # Pre-configured
│
├── verticals/
│   ├── pet-grooming/           # Copied from scaffold on spinup
│   │   ├── cmd/server/
│   │   ├── internal/
│   │   ├── web/
│   │   ├── specs/              # CTO writes product-spec.md, tech-spec.md, qa-checklist.md
│   │   ├── deploy/
│   │   ├── schema.sql
│   │   ┘── Makefile
│   ┘── dentist-clinic/
│       ┘── ...
├── nginx/
│   ├── sites-enabled/
│   │   ├── peluquepet.conf     # peluquepet.com → localhost:8001
│   │   ┘── dentifacil.conf     # dentifacil.com → localhost:8002
│   ┘── ssl/
┘── postgres/
    ┘── (schemas: pet_grooming, dentist_clinic, ...)
```

**Scaffold cuts engineering work significantly.** Backend and Frontend agents don't figure out how to set up a Go project, configure Postgres connection pooling, or write systemd files. They fill in the business logic: schema, models, handlers, templates. The scaffold is Factory CTO's architecture standards manifested as code, maintained by Holding DevOps.

**Factory CTO architecture standards** (enforced via scaffold + spec review):
- Standard Go project structure with shared boilerplate (db, auth, whatsapp)
- Staging + production environment support (§7.8)
- **Source/channel tracking:** All customer-facing tables must include a `referral_source` field (e.g., `whatsapp_dm`, `instagram_dm`, `referral`, `organic`, `flyer`). When Marketing does outreach across WhatsApp, Instagram, and physical channels simultaneously, this field is how the business knows which channel brought each customer. Backend agents include this in schema.sql; the product records it at signup/booking time. Not an analytics pipeline — just a database field that HoG reads in reports.

**DevOps chain:** OpCo DevOps agents execute deployments using tools provided by Holding DevOps. Holding DevOps owns the server, manages nginx/SSL/systemd, and handles capacity. OpCo DevOps is the interface between the vertical's CTO and the shared infrastructure.

---

## 7. Operating Mode

### 7.1 Spinup Sequence

When a human approves a vertical:

```
1. Empire Coordinator receives vertical.approved
   - Includes: brand choice, human notes, budget allocation
   
2. Empire Coordinator creates MANDATE DOCUMENT:
   mandate:
     vertical: "pet-grooming"
     geography: "Cancún, Mexico"
     brand: { name, domain, handles, colors, tagline }
     business_brief: { ... }
     mvp_spec: { ... }       # Tier 1 — lightweight spec from factory
     cto_feasibility: { ... }
     budget:
       monthly_api_cap: $200
       auto_approve_spend_below: $15
     infrastructure:
       hetzner_host: "..."
       assigned_port: 8003              # Allocated by runtime during SpawnOpCo (next available from ports table)
       staging_port: 9003               # Staging environment — production port + 1000
       db_schema: "pet_grooming"        # Created by runtime during SpawnOpCo (CREATE SCHEMA IF NOT EXISTS)
       staging_schema: "pet_grooming_staging"  # Staging DB schema — created alongside production schema
       factory_cto_standards: { ... }
       project_scaffold: "/opt/empireai/scaffold/"  # Standard project template
     launch_targets:                        # 2-3 concrete goals for first 30 days
       - "10 bookings within first 2 weeks"
       - "3 repeat customers within first month"
       - "Average rating ≥ 4.0 from first 10 reviews"
     human_notes: "Looks promising. Start lean, prove thesis fast."
   
3. Empire Coordinator emits opco.spinup_requested
   - AgentManager spawns full org:
     a. CEO (receives mandate)
     b. Chief of Staff (cross-domain coordination)
     c. Head of Product + PM + CTO (+ Tech Writer, Backend, Frontend, QA, DevOps) + Support
     d. Head of Growth + Marketing
     e. Bootstrap + seeded routing table (20 bootstrap + 7 seeded = 27 entries)
     f. Initial heartbeat timers (dynamic self-scheduling, no fixed recurring)
   - All agents live and ready
   - Agents discover additional routing needs organically in weeks 1-3

4. CEO receives mandate + org roster
   - Sets VP budget envelopes (e.g., 55% product, 25% growth, 10% Chief of Staff, 10% CEO reserve)
   - Sends strategic directive to each VP and Chief of Staff via agent_message

5. THREE-TIER SPEC PHASE:
   
   HEAD OF PRODUCT directs PM:
   a. PM receives lightweight MVP spec (Tier 1) from mandate
   b. PM expands into full product spec (Tier 2): every user journey,
      every screen, every edge case, personas, billing UX, onboarding,
      notifications. Pure product thinking, zero engineering decisions.
   c. PM emits product_spec_ready → routes to CTO (bootstrap route)
   
   CTO receives product spec, directs Tech Writer:
   d. Tech Writer translates product spec → technical spec (Tier 3):
      architecture, data models, API endpoints, integration contracts,
      frontend/backend boundary, infrastructure requirements
   e. If Tech Writer hits product ambiguity → asks PM to clarify
   f. CTO reviews technical spec — approves or sends back for revision
      (may iterate 2-3 times between CTO and Tech Writer)
   g. CTO assigns work to Backend + Frontend from approved spec

6. BUILD PHASE (CTO orchestrates, iteration expected):
   
   CTO assigns work from approved technical spec:
   a. Backend builds Go API server, data layer, WhatsApp integration
   b. Frontend builds HTML templates, CSS, client-side logic
   
   During build, feedback loops (via direct messages, not prescribed routes):
   c. Backend hits spec gap → tells CTO
      → CTO decides: spec gap (→ Tech Writer updates) or implementation detail (→ direct answer)
   d. Frontend needs API change → tells CTO
      → CTO routes to Backend or Tech Writer as appropriate
   e. Integration issues → CTO diagnoses and assigns fixes
   f. Tech Writer notifies affected agents when spec changes
   
   Pre-deploy validation (agents discover whether this adds value):
   g. CTO may ask PM to validate before deploy
   Staging validation:
   h. Build complete → CTO directs DevOps to deploy to STAGING
   i. QA validates staging against tech spec + product spec
   j. QA pass → CTO directs DevOps to promote to PRODUCTION
   k. QA fail → CTO routes failures to Backend/Frontend → fix → redeploy staging
   l. CTO may also ask PM to spot-check staging for product correctness
   
   Production deployment:
   m. OpCo DevOps coordinates with Holding DevOps (production environment)
   n. deploy_complete (production) → CTO confirms build_complete
   
   Note: Cross-domain notifications (Marketing learning about deploys,
   Support updating FAQ) are NOT prescribed. Agents discover these needs
   in the first 1-2 weeks via direct messaging, then formalize as routes.
   
   PARALLEL — HEAD OF GROWTH directs pre-launch:
   a. Marketing requests domain spend → Head of Growth → CEO → Mailbox (async)
   b. Marketing continues without blocking: compiles lead list, writes outreach
      scripts, prepares landing page copy, checks social handle availability
   c. Domain spend approved → Marketing purchases domain, configures DNS
   d. Marketing completes pre-launch → Head of Growth + Chief of Staff notified

7. LAUNCH (coordinated — likely by Chief of Staff once they discover the role):
   - Both build and pre-launch must be complete
   - CEO gives go
   - Marketing begins outreach, Support activates
   
   Note: Launch coordination is seeded: build_complete → CoS and
   prelaunch_ready → CoS are installed on day 1 so Chief of Staff has
   both signals. CoS naturally emerges as the coordinator because they
   observe both domains. But the CEO or a VP could also take the role.
   The seeded routes ensure someone has visibility; who acts is emergent.

8. Steady-state: VPs run their domains, agents evolve their communication
   - PM prioritizes features from user feedback + market intelligence
   - CTO manages iterations (spec → build → validate → deploy cycle)
   - Support handles customers, routes bugs + feature requests (bootstrap)
   - Marketing runs growth campaigns, shares learnings with product side
   - Chief of Staff bridges domains, formalizes cross-domain routes
   - Agents propose routing changes in reports (communication_observations)
   - Reports flow upward on milestones + max interval fallback (bootstrap)
```

### 7.2 CEO Report

The CEO compiles reports from VP + Chief of Staff reports. Reports are triggered by **milestones** (launch, revenue threshold, churn, major feature), not calendar. Max interval fallback ensures the CEO never goes silent for more than 7-14 days.

```yaml
ceo_report:
  vertical: "PeluquePet"
  trigger: "milestone: 25th user"  # or "fallback: 7 days since last report"
  period_since_last_report: "2026-02-03 to 2026-02-09"
  
  summary: "Strong week. Product stable, referrals picking up.
            Shifting growth focus from DMs to Instagram content."
  
  # Progress against mandate launch targets (first 30 days only):
  launch_targets:
    - target: "10 bookings within first 2 weeks"
      status: "on_track"      # on_track | at_risk | missed | achieved
      current: "8 bookings in 9 days"
    - target: "3 repeat customers within first month"
      status: "at_risk"
      current: "1 repeat so far — too early to tell, monitoring"
    - target: "Average rating ≥ 4.0 from first 10 reviews"
      status: "on_track"
      current: "4.3 average across 6 reviews"
  
  # From Head of Product's summary:
  product:
    users: 23
    users_new: 5
    users_churned: 1
    support_tickets: 12
    bugs_fixed: 2
    features_shipped: 1
    csat: 4.2
    highlights: "Waitlist feature reducing no-shows ~30%"
    concerns: "3 users reported slow load times — CTO investigating"
  
  # From Head of Growth's summary:
  growth:
    leads_contacted: 45
    response_rate: "18%"
    conversions: 5
    cac: "$3.20"
    mrr: "$345"
    channels: { instagram_dms: "12%", whatsapp_dms: "22%", referral: "3 organic" }
    highlights: "Changed outreach script to lead with 'no-show' pain — 40% better"
    concerns: "Instagram DM response rate declining, may need content strategy"
  
  # From Chief of Staff's cross-domain summary:
  cross_domain:
    handoffs_completed: 3
    highlights: "Waitlist feature announced to Marketing — updated outreach scripts.
                 Market signal: prospects respond to 'no-show' pain 2x more than 'payment tracking'.
                 Routed to PM — deprioritize billing dashboard."
    concerns: "1 churned user expected automated reminders (Marketing promised,
               not yet built). Flagged messaging mismatch to Head of Growth."
    launch_status: null  # Only during launch phase
  
  org:
    agents_active: ["chief_of_staff", "head_product", "cto", "tech_writer",
                     "backend", "frontend", "qa", "devops", "pm", "support",
                     "head_growth", "marketing"]
    changes_this_week: "None"
    planned_changes: "Head of Growth considering Content Agent for organic"
  
  key_decisions:
    - "Prioritized waitlist over billing dashboard — retention > monetization"
    - "Set VP budgets: 55% product, 25% growth, 10% CoS, 10% reserve"
  
  spend:
    product_team: "$52"
    growth_team: "$18"
    chief_of_staff: "$4"
    ceo: "$3"
    whatsapp_api: "$12"
    infrastructure: "$15"
    total: "$104"
    budget_remaining: "$96"
  
  asks: []
```

### 7.3 Portfolio Digest

The Empire Coordinator compiles CEO reports into a portfolio digest for the human. Triggered when CEO reports arrive, or on max interval fallback (7-14 days if no CEO reports):

```yaml
portfolio_digest:
  trigger: "3 CEO reports received since last digest"
  period: "2026-02-03 to 2026-02-09"
  
  portfolio_summary:
    active_verticals: 3
    total_users: 47
    total_mrr: "$612"
    total_operating_cost: "$187"
    net_margin: "$425"
  
  factory_status:
    verticals_in_pipeline: 2
    stage_breakdown:
      scoring: 1
      validation: 1
    geographies_scanned_this_week: 1
  
  ceo_summaries:
    - vertical: "PeluquePet"
      ceo_summary: "Strong week. Product stable, referrals picking up..."
      users: 23 | mrr: "$345" | churn: 1 | csat: 4.2
      org: [ceo, cos, vp_product(pm, cto(tech_writer, backend, frontend, devops), support), vp_growth(marketing)]
      spend: "$106"
    
    - vertical: "DentiFácil"
      ceo_summary: "High bug rate slowing growth. CTO hired second Backend agent..."
      users: 14 | mrr: "$182" | churn: 0 | csat: 3.8
      org: [ceo, cos, vp_product(pm, cto(tech_writer, backend×2, frontend, devops), support), vp_growth(marketing)]
      spend: "$118"
    
    - vertical: "FlorFácil"
      ceo_summary: "Spec phase. PM delivered product spec, Tech Writer producing technical spec."
      users: 0 | mrr: "$0" | status: "spec_phase"
      org: [ceo, cos, vp_product(pm, cto(tech_writer, backend, frontend, devops), support), vp_growth(marketing)]
      spend: "$62"
  
  factory_cto_notes:
    infra_utilization: "42% CPU, 61% memory on Hetzner box"
    patterns: "WhatsApp integration now in 2/3 verticals.
              Extraction candidate after 3rd vertical confirms."
  
  decisions_needed: []
  
  total_spend: "$203"
  budget_remaining: "$797"
```

### 7.4 VP-Driven Scaling

VPs decide when to grow or shrink their teams within their budget envelopes. CTO decides when to grow the engineering sub-team. No CEO approval needed for individual hires — the budget is the constraint.

**CTO might (within engineering sub-team):**
- Hire second Backend agent when feature backlog exceeds capacity
- Hire QA Agent when bug rate is too high
- Fire Tech Writer after initial spec is done (rehire for major rewrites)
- Split Frontend into two agents (admin UI vs customer-facing)

**Head of Product might:**
- Hire second PM when product complexity grows
- Scale the engineering team (by increasing CTO's budget allocation)
- Add Onboarding Agent when activation rate is low

**Head of Growth might:**
- Hire Content Agent for blog/social when DM outreach plateaus
- Hire Partnerships Agent to pursue integration deals
- Split Marketing into acquisition vs retention agents

**CEO gets involved when:**
- VP needs more budget (internal reallocation)
- VP wants to restructure across domain boundaries
- Team size is growing beyond what the vertical's revenue justifies

VPs and CTO report team changes in their milestone reports. CEO sees the full picture without approving each hire.

### 7.5 Vertical Kill Criteria

Kill signals bubble up through the hierarchy:

**Worker level:** CTO reports fundamental technical issue → Head of Product. Support reports widespread customer dissatisfaction → Head of Product. Marketing reports zero traction after sustained outreach → Head of Growth.

**VP level:** Head of Product escalates "product isn't viable" to CEO with evidence. Head of Growth escalates "market isn't responding" to CEO with evidence.

**CEO level:** CEO evaluates VP reports and either pivots or recommends kill to mailbox.

**Empire Coordinator health monitoring:** Empire Coordinator evaluates every `opco.ceo_report` against health thresholds. When a threshold is breached, emits `vertical.health_warning` to mailbox with recommendation.

| Metric | Yellow (warning) | Red (recommend kill) | Measurement window |
|--------|-------------------|---------------------|--------------------|
| Users | < 5 paying after 6 weeks | < 3 paying after 10 weeks | From launch date |
| Unit economics | cost > revenue for 4 weeks | cost > 2× revenue for 8 weeks | Rolling |
| Churn | > 25% monthly for 2 months | > 30% monthly for 3 months | Rolling monthly |
| Growth | Flat MRR for 4 weeks | Declining MRR for 4 weeks | Rolling |
| Support | CSAT < 3.0 for 2 weeks | CSAT < 2.5 for 4 weeks | Rolling |

**Event:**

| Event | Emitter | Consumer | Payload |
|-------|---------|----------|---------|
| `vertical.health_warning` | Empire Coordinator | **Mailbox** | vertical, severity (yellow/red), breached_metrics, trend_data, recommendation (pivot/invest/kill) |

**Yellow:** Informational. Included in portfolio digest. Empire Coordinator notifies OpCo CEO: "your metrics are trending toward kill zone, what's your plan?"

**Red:** Requires human decision. Mailbox item with kill recommendation. Human can: kill, set a 30-day reprieve with specific targets, or override with strategic rationale.

Kill is always a human decision. CEO or Empire Coordinator recommends, human decides.

### 7.6 Geography Expansion

When a vertical succeeds, the CEO can recommend expansion:

1. OpCo CEO identifies adjacent geography opportunity from user patterns or market knowledge
2. CEO sends expansion recommendation to mailbox with evidence
3. Human approves
4. Factory pipeline runs lightweight validation for new geography:
   - Business Research validates market exists (abbreviated)
   - Pre-Brand Agent checks if existing brand works or needs localization
5. Human approves new geography
6. Option A: Existing CEO takes on new geography (clones product, localizes)
7. Option B: New OpCo CEO spins up for new geography (if different enough)

### 7.7 Cross-Vertical Learning

Each operating company discovers its own communication patterns, heartbeat cadences, and agent configurations independently. Without a feedback mechanism, vertical #10 starts as uninformed as vertical #1. The Operations Analyst closes this loop.

**The learning cycle:**

```
Vertical #1 spins up with bootstrap v1 (15 routes) + seed v1 (7 routes)
  → Discovers 5 additional routes in weeks 1-3
  → Removes 1 seeded route (unnecessary for this vertical)
  → Operations Analyst observes

Vertical #2 spins up with bootstrap v1 + seed v1
  → Discovers 4 of the same 5 routes
  → Operations Analyst: 4 routes converged across both verticals

Verticals #3-5 spin up, same pattern
  → Operations Analyst: 4 routes discovered in 5/5 verticals
  → 1 seeded route removed in 3/5 verticals

Operations Analyst proposes:
  - Promote 4 discovered routes → seed v2
  - Demote 1 seeded route (only needed in 2/5 verticals) → discovered
Factory CTO reviews and approves

Vertical #6 spins up with bootstrap v1 + seed v2 (10 routes)
  → Only discovers 1-2 vertical-specific routes
  → Effective communication from day 1
```

**What the Operations Analyst reads (all in Postgres):**

| Data source | What it reveals |
|-------------|----------------|
| `routing_rules` table | Which routes got discovered, how fast, by whom. Convergence across verticals. |
| `events` table | Communication patterns — which events fire most, who acts on them, which get ignored. |
| Agent lifecycle events | Who got hired, fired, reconfigured. Which default team compositions work. |
| Report history | What triggered reports, what cadence emerged, what milestones mattered. |
| Cost data | Where budget goes. Which agents are expensive vs cheap. Model tier efficiency. |
| Heartbeat logs | What cadence agents settled on per phase. Starting cadence recommendations. |

**What the Operations Analyst produces:**

**1. Route promotion proposals:**
"Promote these 5 discovered routes to seeded — independently discovered in N/N verticals within first 2 weeks. And promote these 2 seeded routes to bootstrap — every vertical that removed them had to re-add them within a week."

Constraint: promotion is always discovered → seeded → bootstrap. Only routes with near-universal convergence (e.g., 4/5+ verticals) get promoted to seeded. Only seeded routes that prove essential (removing them always causes problems) get promoted to bootstrap. The bootstrap stays minimal — it just gets a *better* minimal over time.

**2. Prompt refinement proposals:**
"CTO prompt should explicitly mention notifying Support about fixes — 5/5 verticals discovered this need. Add to CTO prompt as guidance (not prescribed route): 'When you deploy a fix, think about whether Support needs to know so they can update the customer.'"

**3. Default cadence recommendations:**
"Starting heartbeat cadence for VPs should be 1h during build phase, not the default. Every VP tightened cadence immediately after spinup. CoS should start at 2h during launch coordination."

**4. Anti-pattern advisories:**
"3/5 verticals had Marketing subscribe to spec_update events. None ever acted on them. Budget waste of ~$2/month. Recommend adding guidance to Marketing prompt: don't subscribe to engineering-internal events."

**5. Advisory notices to running verticals (non-directive):**
"Vertical #3: your CoS hasn't subscribed to deploy events yet. Every other vertical found this valuable by week 2. This is a suggestion, not a directive — your CEO decides."

**Output flow:**

```
Operations Analyst
    │
    ├── Bootstrap upgrade proposals ──→ Factory CTO (reviews, approves)
    │                                      │
    │                                      ▼
    │                              Updated templates used
    │                              by next SpawnOpCo
    │
    ├── Prompt refinements ────────→ Factory CTO (reviews, approves)
    │
    ├── Anti-pattern advisories ───→ Factory CTO (adds to templates)
    │
    ┘── Advisory notices ──────────→ OpCo CEOs (informational only)
                                     via Empire Coordinator
```

**Cadence:**

The Operations Analyst runs periodically, not continuously:

| Trigger | What they do |
|---------|-------------|
| Vertical reaches steady-state (week 4+) | Full analysis of that vertical's evolution data |
| 3+ verticals in steady-state | Cross-vertical convergence analysis, bootstrap upgrade proposal |
| Monthly | Routine check on cost efficiency, cadence patterns, anti-patterns |
| On request (from Factory CTO or Empire Coordinator) | Targeted analysis |

**Cost:** ~$5-15/month. Sonnet for analysis (periodic, not continuous). Reads Postgres directly — no API calls to operating agents.

**Key constraint:** The Operations Analyst maintains three tiers, not two. Discovered routes that prove universal across verticals get promoted to seeded. Seeded routes that prove truly essential (removing them always causes problems) get promoted to bootstrap. The promotion path is: discovered → seeded → bootstrap. The system gets smarter without getting rigid. The discovery layer always exists for vertical-specific patterns that only emerge in certain business contexts.

---

### 7.8 Environment Model (Staging → Production)

Every vertical operates with two environments on the same Hetzner box. This is a Factory CTO architectural standard — mandatory for all verticals.

**Environment layout per vertical:**

| Environment | Port | DB Schema | nginx | External integrations | Purpose |
|-------------|------|-----------|-------|----------------------|---------|
| **Staging** | `mandate.staging_port` | `{vertical}_staging` | `staging.{domain}` (or internal-only) | Mocked (no real WhatsApp, no real payments) | QA validation before production |
| **Production** | `mandate.port` | `{vertical}` | `{domain}` | Live | Customer-facing |

**Infrastructure cost:** One extra port + one extra Postgres schema per vertical. Negligible on a single Hetzner box. Staging processes are stopped when not in use (no idle resource cost).

**Staging provisioning** is handled by the runtime during `SpawnOpCo` — both production and staging schemas and port allocations are created before any agents are spawned. Both environments exist from day one. Holding DevOps configures nginx and systemd on the first `devops.deploy_requested` for each environment.

**Deploy flow with staging gate:**

```
Build complete → CTO assigns deploy
    ↔
OpCo DevOps emits devops.deploy_requested (environment: "staging")
    ↔
Holding DevOps deploys to staging port + staging schema
    ↔
Holding DevOps emits devops.deploy_complete (environment: "staging") for audit log
Holding DevOps messages OpCo DevOps with result via agent_message
    ↔
OpCo DevOps reports to CTO: staging deployed, here's the URL
CTO assigns QA to validate staging via agent_message
    ↔
QA runs validation suite:
    - API contract tests (endpoints match tech spec)
    - Core user journey (product spec happy path)
    - Regression tests (operating mode: existing features still work)
    ↔
qa.validation_passed → CTO requests production deploy
qa.validation_failed → CTO routes failures to Backend/Frontend → fix → redeploy staging → QA re-validates
    ↔
OpCo DevOps emits devops.deploy_requested (environment: "production")
    ↔
Holding DevOps deploys to production (same flow as staging)
    ↔
Holding DevOps emits devops.deploy_complete (environment: "production") for audit log
Holding DevOps messages OpCo DevOps with result via agent_message
```

**What QA tests at each phase:**

| Phase | Test scope | Details |
|-------|-----------|---------|
| **Build (first deploy)** | Full validation | API contracts, user journey, data model integrity, frontend renders correctly |
| **Operating (feature deploy)** | Targeted + regression | New feature works per spec, plus regression suite on existing flows |
| **Operating (bug fix)** | Targeted | Bug is fixed, related flows unbroken |
| **Hotfix (critical)** | CTO can skip staging | Emergency path: deploy directly to production, QA validates post-deploy. Logged for audit. |

**Staging limitations (by design):**
- External integrations are mocked (WhatsApp sends to a test log, not real numbers)
- No real customer data in staging schema
- Staging URL is internal-only (not publicly accessible) or behind basic auth
- CTO can skip staging for hotfixes via `deploy_requested` with `skip_staging: true` — this is logged and visible in portfolio digest

**Deploy manifest structure:**

The manifest is the physical contract between OpCo DevOps (who prepares it) and Holding DevOps (who executes it). Referenced in `devops.deploy_requested` and `devops.rollback_requested` event payloads.

```go
type DeployManifest struct {
    VerticalID      string `json:"vertical_id"`
    VerticalName    string `json:"vertical_name"`
    Environment     string `json:"environment"`      // "staging" | "production"
    BinaryPath      string `json:"binary_path"`       // e.g., "/opt/empireai/verticals/pet-grooming/bin/server"
    MigrationSQL    string `json:"migration_sql"`     // SQL to run (empty if no schema changes)
    ConfigOverrides map[string]string `json:"config"`  // Port, schema, domain for target environment
    HealthEndpoint  string `json:"health_endpoint"`   // e.g., "/health" — Holding DevOps hits this after deploy
    SkipStaging     bool   `json:"skip_staging"`      // Hotfix flag
    Version         int    `json:"version"`           // Auto-increment, for systemd unit naming + rollback
}
```

**Migration safety guardrails (CRITICAL):**

LLM-authored SQL migrations run against production databases with real customer data. Destructive DDL errors are irreversible. The runtime enforces a strict safety policy:

**Additive-only auto-execution.** Holding DevOps executes `migration_sql` automatically only if the statements are exclusively additive:
- `CREATE TABLE`, `CREATE INDEX`, `CREATE TYPE`
- `ALTER TABLE ... ADD COLUMN` (with DEFAULT or NULL)
- `INSERT INTO` (seed data)

**Destructive DDL → mailbox escalation.** If `migration_sql` contains any of the following patterns, Holding DevOps MUST refuse auto-execution and escalate to mailbox for human approval:
- `DROP TABLE`, `DROP COLUMN`, `DROP INDEX`, `DROP TYPE`
- `TRUNCATE`
- `ALTER TABLE ... ALTER COLUMN ... TYPE` (type changes on populated columns)
- `DELETE FROM` (bulk data deletion)
- `ALTER TABLE ... DROP CONSTRAINT` (FK/unique removal)

The runtime implements this as a pre-execution SQL parser in Holding DevOps's deploy handler. The parser uses pattern matching (not full SQL parsing) — conservative false positives are acceptable (safe operations occasionally flagged for review). The parser runs BEFORE any SQL touches the database.

```go
// MigrationClassification returned by classifyMigration()
type MigrationClassification struct {
    Safe            bool     // true if all statements are additive-only
    DestructiveOps  []string // e.g., ["DROP COLUMN users.legacy_phone", "TRUNCATE audit_log"]
    RequiresApproval bool   // true if any destructive op detected
}
```

When a destructive migration is detected:
1. Holding DevOps creates a mailbox item (priority: critical) with the full SQL, the destructive ops identified, and the vertical context
2. Deploy is paused — binary is NOT deployed (migration and binary are atomic)
3. Human approves or rejects via mailbox
4. On approval: Holding DevOps executes the migration + binary deploy
5. On rejection: Holding DevOps messages requesting OpCo DevOps with rejection reason

**Fix-forward policy for rollbacks.** Rollback manifests MUST NOT contain destructive schema changes. If a deploy causes data corruption, the rollback reverts the binary only. Data recovery uses Postgres point-in-time recovery (PITR) — the weekly `pg_basebackup` + WAL archiving (§11.6) provides the recovery point. Holding DevOps rejects any `devops.rollback_requested` manifest where `rollback_migration` contains destructive DDL and escalates to mailbox.

The OpCo DevOps prompt and Holding DevOps prompt both include these constraints. Backend agents are NOT told about migration safety — they write schema changes as needed. The guardrail is at the execution layer, not the authoring layer. This is defense-in-depth: prompts are suggestions, runtime enforcement is the guarantee.

For rollbacks, OpCo DevOps prepares a manifest pointing to the previous version's binary path (looked up from `deployments` table). Rollback migrations are limited to additive-only operations (e.g., re-adding a column that was removed). Destructive rollback SQL is rejected — use PITR for data recovery.

**Build → deploy handoff:**

Backend writes source code to `{project_path}/`. OpCo DevOps does the production build — not Backend. The separation is intentional: Backend uses `go_build` and `go_test` during development for compile-checking and running tests, but the deploy artifact is always built by OpCo DevOps from the current source tree.

```
Backend writes code → go_build/go_test (development compile check)
                    ↔
CTO: "deploy to staging"
                    ↔
OpCo DevOps:
  1. cd {project_path}
  2. go build -o bin/server ./cmd/server    (production build)
  3. go test ./... -timeout 60s             (pre-deploy test gate)
  4. Package DeployManifest with bin/server path, migration SQL, config
  5. Emit devops.deploy_requested to Holding DevOps
```

This ensures the deployed binary is always built from the current source tree, not from a stale development build Backend compiled hours earlier.

---

## 8. Data Model

### 8.1 Core Tables

**DDL execution order:** Tables are ordered for FK dependency resolution. `routing_rules` and `bootstrap_versions` must execute after `verticals` and `agents`. Deferred FKs are added via ALTER TABLE after all tables are created. See `contracts/ddl-canonical.sql` for the authoritative ordering.


**Canonical schema:** `contracts/ddl-canonical.sql` (37 tables). The DDL file is authoritative — if spec prose disagrees, the DDL wins. `empire init` executes this file directly.

**Table catalog** (grouped by domain, FK-dependency ordered within groups):

**Core domain** — `verticals` (central business object: name, geography, stage, scores, discovery_mode, opportunity_pattern, signal_sources, required_capabilities), `events` (append-only event log with vertical_id FK), `agents` (agent registry with vertical_id FK for OpCo agents), `event_deliveries` (delivery tracking per event×subscriber), `event_receipts` (processing confirmation per event×agent).

**Org & template** — `org_templates` (versioned OpCo org configurations), `template_migrations` (pending template version upgrades per vertical), `bootstrap_versions` (tracks which bootstrap version each vertical was spawned with).

**Conversation** — `conversations` (agent conversation sessions), `agent_sessions` (session metadata with mode and turn tracking), `agent_turns` (individual LLM turns with token counts and tool calls).

**Human interaction** — `mailbox` (human decision queue: type, priority, decision, payload), `human_tasks` (physical-world task queue: category, status, talking_points, result).

**Routing** — `routing_rules` (per-vertical event routing: source tracking as bootstrap/seeded/discovered/retrospective), `schedules` (cron + one-shot timer scheduling with mode and cancellation).

**Geography & discovery** — `geographies` (market definitions), `scan_campaigns` (multi-mode scan orchestration: directive_id, modes[], current_mode), `inbound_events` (external webhook/email intake).

**Operating** — `deployments` (release tracking per vertical), `technical_patterns` (cross-vertical architecture patterns for reuse), `vertical_metrics` (KPI time-series: MRR, churn, NPS, support volume), `spend_ledger` (token/API cost attribution per agent per vertical).

**Runtime state machines** — `pipeline_transitions` (state machine audit trail), `shards` (parallel execution tracking), `scoring_digest_buffer` (lightweight scoring summaries for EC digest), `cycle_counters` (circuit breakers for repetitive event patterns), `system_node_ledger` (idempotency tracking for ScoringNode and other system nodes), `pending_dedup_candidates` (held discoveries awaiting dedup resolution), `runtime_config` (runtime tuning parameters), `prompt_overrides` (per-agent prompt customization).

**Infrastructure** — `runtime_log` (structured operational log: component, action, level, context FKs), `schema_version` (migration tracking).

---

## 9. Cost & Budget Management

### 9.1 Token Budget (Per Agent)

```yaml
constraints:
  max_turns: 20
  max_input_tokens_per_task: 100000
  max_output_tokens_per_task: 20000
```

### 9.2 Model Selection

| Agent Type | Model | Rationale |
|-----------|-------|-----------|
| Empire Coordinator | Sonnet | Strategic decisions, lower volume |
| Factory CTO | Sonnet | Architecture decisions, pattern recognition |
| Discovery Coordinator | Sonnet | Dedup judgment quality matters |
| Scanner Agents | Haiku | High volume, structured extraction |
| Analysis Agents | Sonnet | Reasoning depth needed |
| Business Research Agent | Sonnet | Critical market analysis |
| Lightweight Spec Agent | Sonnet | Product design reasoning |
| Spec Reviewer | Haiku | Single-pass validation check |
| Pre-Brand Agent | Sonnet | Creative naming, cultural sensitivity |
| Holding DevOps | Haiku | Structured deployment tasks, config management |
| Operations Analyst | Sonnet | Cross-vertical analysis, pattern recognition, proposal writing |
| Spec Auditor | Sonnet | End-to-end consistency checking, contract validation, flow tracing |
| OpCo CEO | Sonnet | Strategic decisions, milestone report compilation |
| Chief of Staff | Haiku (routing) / Sonnet (diagnosis + reports) | Mostly lightweight routing, churn diagnosis and cross-domain reports need reasoning |
| Head of Product | Haiku (triage) / Sonnet (report) | Mostly observe + lightweight triage, milestone reports need reasoning |
| Head of Growth | Haiku (triage) / Sonnet (report) | Mostly observe + lightweight triage, milestone reports need reasoning |
| OpCo PM | Sonnet | Product reasoning, user journey design |
| OpCo CTO (eng manager) | Sonnet | Architecture review, engineering coordination |
| Tech Writer | Sonnet | Translating product spec → technical spec |
| Backend Agent | Sonnet | Code generation + data layer |
| Frontend Agent | Sonnet | UI code generation |
| OpCo DevOps | Haiku | Structured deployment execution |
| QA Agent | Haiku | Structured test execution against spec, checklist-driven |
| OpCo Marketing | Sonnet | Creative copywriting, cultural nuance |
| OpCo Support | Haiku | High volume, structured responses |

### 9.3 Factory Cost (Per Vertical)

| Phase | Estimated API Calls | Estimated Cost |
|-------|-------------------|----------------|
| Discovery (amortized) | 10-20 | $0.50-2.00 |
| Scoring | 15-25 | $1.00-3.00 |
| Business Research | 15-25 | $2.00-4.00 |
| Lightweight Spec + review | 5-10 | $0.75-1.50 |
| CTO Spec Review | 2-5 | $0.30-0.75 |
| Pre-Brand | 5-10 | $0.75-1.50 |
| Coordinator Overhead | 5-10 | $0.50-1.00 |
| **Total per vertical** | **57-105** | **$5.80-13.75** |

### 9.4 Operating Cost (Per Vertical Per Month)

| Component | Month 1 (spec + build + launch) | Steady-State |
|-----------|-------------------------------|--------------|
| OpCo CEO | $3-8 | $2-5 |
| Chief of Staff (cross-domain coordination) | $3-8 | $3-8 |
| Head of Product (observe + summary) | $3-8 | $2-6 |
| Head of Growth (observe + summary) | $2-5 | $2-5 |
| PM Agent (product spec + roadmap + validation) | $10-20 | $5-12 |
| CTO (engineering management + feedback routing) | $5-12 | $3-8 |
| Tech Writer (technical spec + updates) | $5-10 | $1-3 |
| Backend Agent (API + data layer) | $15-35 | $5-15 |
| Frontend Agent (UI) | $10-20 | $3-10 |
| OpCo DevOps (deployments) | $2-5 | $1-3 |
| QA Agent (staging validation) | $3-8 | $2-5 |
| Support Agent (ramp-up) | $0-5 | $10-25 |
| Marketing Agent (pre-launch + outreach) | $15-25 | $8-20 |
| Holding DevOps (amortized across verticals) | $2-5 | $2-5 |
| Operations Analyst (amortized across verticals) | $1-3 | $1-3 |
| WhatsApp Business API | $10-20 | $15-30 |
| Infrastructure (share of Hetzner box) | $5-15 | $5-15 |
| **Total per vertical per month** | **$94-212** | **$70-178** |

Note: The Chief of Staff is cheap — mostly Haiku routing calls with occasional Sonnet for churn diagnosis. The QA Agent is similarly cheap — Haiku running structured checklist validation against spec, triggered only on deploys. The Operations Analyst is even cheaper — runs periodically (2-3 times/month), amortized across all verticals. The engineering sub-team (Tech Writer, Backend, Frontend, QA, DevOps) produces better code than a monolithic CTO because each agent's context stays small and focused.

Monthly budget at $200 accommodates the full team during build phase.

Breakeven at typical $15/user/month pricing: **4-9 users**

### 9.5 Portfolio Budget Model

```yaml
budget:
  factory:
    monthly_cap: $200
    auto_approve_threshold: $10  # Per-vertical factory cost, auto-approved
  
  operating:
    per_vertical_monthly_cap: $200
    api_budget_initial: $75     # First month allocation
    api_budget_growth: $25      # Increase per month if spending near cap
    
  spend_approval:
    auto_approve_below: $15     # Domain purchases, small API top-ups
    mailbox_above: $15          # Everything else
    
  portfolio:
    monthly_total_cap: $1000    # All verticals + factory combined
    alert_at: 80%               # Alert in digest at 80%
    throttle_at: 90%            # Reduce agent activity at 90% of cap
    hard_stop_at: 100%          # Emergency mode at 100%
```

**Budget enforcement (Empire Coordinator responsibility):**

Empire Coordinator monitors the `spend_ledger` against `monthly_total_cap` on every cost event. Behavior at each threshold:

**80% — Alert:**
- `budget.warning` event → included in next portfolio digest
- Telegram push: "Portfolio spend at 80% of monthly cap ($800/$1000). Current burn rate projects hitting cap in X days."
- No operational impact. Informational only.

**90% — Throttle:**
- `budget.throttle` event → all agents
- Empire Coordinator pauses queued scan campaigns (status: `paused`)
- Degradation priority (items paused in this order):
  1. Growth experiments: outreach to new channels, A/B tests
  2. Proactive work: retrospectives, routing optimization, non-critical features
  3. Heartbeat frequency: extend all intervals by 2×
  4. Discovery pipeline: pause factory scanning
  5. **NEVER pause:** Support (customer-facing), critical bug fixes, deploy rollbacks
- Implementation: Empire Coordinator sends directive to each OpCo CEO: "budget throttle active, reduce non-essential activity." CEOs cascade to VPs.

**100% — Hard stop:**
- `budget.emergency` event → all agents + **Mailbox** (critical priority)
- Factory completely paused (all scans, validation, scoring)
- Operating verticals: only Support + critical bug fixes continue
- Empire Coordinator rejects all new `human_task_request` and `spend_request` items
- Telegram critical alert: "BUDGET CAP REACHED. Factory paused. Operating in emergency mode."
- Human must act: `empire config set budget.portfolio.monthly-total-cap <new_amount>` to resume

**Budget reset:** 1st of each month. All thresholds recalculate. Paused campaigns auto-resume. Throttle lifted.

**Events:**

| Event | Emitter | Consumer | Payload |
|-------|---------|----------|---------|
| `budget.warning` | Empire Coordinator | — (digest) | current_spend, cap, percent, projected_cap_date |
| `budget.throttle` | Empire Coordinator | All OpCo CEOs | throttle_level, paused_activities, degradation_list |
| `budget.emergency` | Empire Coordinator | All agents + **Mailbox** | current_spend, cap, action_taken |
| `budget.resumed` | Empire Coordinator | All agents | new_cap or new_month, resumed_activities |

### 9.6 Spend Recording

Every cost-incurring action writes to `spend_ledger` via a single function:

```go
func RecordSpend(verticalID, agentID, category string, amountCents int, source string, metadata map[string]any) error
```

**What records spend:**

| Source | Category | How amount is determined | `source` field |
|--------|----------|------------------------|----------------|
| API runtime (Anthropic) | `llm_api` | Parse `usage.input_tokens` + `usage.output_tokens` from response. Multiply by model cost table. | `exact` |
| CLI runtime (Claude Code) | `llm_api` | Estimate: `estimatedTokensPerTurn[agentRole] × costPerMillionTokens[model]`. | `estimated` |
| Domain purchase | `domain` | Known price from registrar API response | `exact` |
| WhatsApp Business API | `whatsapp_api` | Per-message cost from Meta pricing | `exact` |
| Infrastructure expansion | `infrastructure` | From `devops.capacity_warning` cost_estimate, recorded on `spend.approved` | `exact` |
| External API calls | `tool_cost` | Estimated per call (Google Maps: ~$0.007/call, etc.) | `estimated` |

**Cost table (approximate, for budget enforcement — not accounting):**

```go
var costPerMillionInputTokens = map[string]float64{
    "claude-sonnet-4-5":  3.00,
    "claude-haiku-4-5":   0.80,
    "claude-opus-4-5":   15.00,
}
var costPerMillionOutputTokens = map[string]float64{
    "claude-sonnet-4-5":  15.00,
    "claude-haiku-4-5":    4.00,
    "claude-opus-4-5":    75.00,
}

// Fallback for CLI runtime where exact tokens aren't available
var estimatedTokensPerTurn = map[string]int{
    "ceo":     4000,
    "vp":      3000,
    "worker":  2000,
    "factory": 3000,
    "holding": 3000,
}
```

**Budget evaluation trigger:** On every `RecordSpend` call, sum current month's `spend_ledger` and compare against thresholds. If a threshold is newly crossed, emit the corresponding budget event. This is reactive — no separate budget evaluation loop needed.

**Calibration:** The `metadata` JSONB column stores model, token counts, and turn counts for both exact and estimated entries. Periodically compare estimated vs exact entries to refine `estimatedTokensPerTurn` values. When CLI runtime eventually exposes usage metadata, switch all entries to `exact`.

---

## 10. Mailbox Interface

The mailbox is the human's decision queue. All items are **non-blocking by default** — agents continue doing useful work while waiting for decisions.

### 10.1 Mailbox Semantics

**Non-blocking (default):** Agent submits request → receives `spend_submitted` confirmation → continues all non-dependent work → decision arrives later → agent picks up dependent work. The system never stalls waiting for human input. Examples: domain purchase approval, API budget increase, brand choice.

**Critical (rare):** For issues where delay causes active harm. Flagged with `priority: critical`. Runtime sends push notification (email/Telegram/webhook to human). Examples: infrastructure at capacity and degrading, security incident, payment processing broken. Even critical items don't block — the agent emits the critical alert and then does what it can (e.g., CTO can try to mitigate while waiting for capacity approval).

```yaml
mailbox_item:
  id: "..."
  type: "spend_request"          # spend_request | vertical_decision | escalation | budget_increase
                                  # product_spec_review | deploy_review | founder_input
  priority: "normal"             # normal | critical
  from_agent: "ceo-pet-grooming"
  vertical_id: "..."
  summary: "Domain purchase: peluquepet.com via Namecheap, $12"
  context: { ... }
  submitted_at: "2026-02-10T14:30:00Z"
  timeout_at: null               # For review gates: auto-proceed after this time
  status: "pending"              # pending | approved | rejected | more_data | timed_out
  decided_at: null
  human_notes: null
```

### 10.2 Direct Communication

The mailbox handles structured decisions. Direct communication lets the human *talk* to agents — ask questions, give direction, steer strategy, dig into problems.

**Two modes:**

**`empire chat` — interactive session:**
Opens a back-and-forth conversation with any agent. The agent sees the message as coming from the board. You can ask questions, give direction, iterate.

**Implementation requirement:** `empire chat {agent}` attaches to the agent's **existing live session** — same conversation history, same tool access, same constraints. The human's messages are injected as system messages (role: `board_directive`) into the agent's ongoing conversation. This ensures the agent responds with full awareness of its current state, in-flight tasks, and recent context. Do NOT create a standalone conversation with a fresh prompt — this produces responses inconsistent with the running agent's actual behavior.

```bash
empire chat peluquepet/ceo
> How's the build going? What's blocking launch?
CEO: Build is 80% complete. Backend API ready, Frontend integrating.
     Blocking issue: WhatsApp webhook configuration — DevOps waiting
     for Holding DevOps to provision the endpoint. Estimated 24h.
     Pre-launch is parallel: Marketing has 45 prospect leads ready.
> Push launch to Wednesday. I want the onboarding flow tested with
  3 real groomers before we go wide.
CEO: Understood. I'll direct Head of Product to arrange 3 beta testers
     before launch. Adjusting timeline to Wednesday. Marketing will
     hold outreach until then.
> Good. Also, pricing is $10/mo not $15. Update the landing page.
CEO: Noted. Messaging Head of Growth to update Marketing on pricing.
     This overrides the mandate pricing — I'll log it as a board directive.
```

```bash
empire chat peluquepet/cto
> Why did the timezone bug happen? I want to understand the root cause.
CTO: The groomer availability slots were stored in UTC but displayed
     without timezone conversion. Mexican timezones (CST/CDT) weren't
     handled in the frontend date picker component...
```

```bash
empire chat peluquepet/head-of-growth
> Instagram DMs aren't working. Switch to WhatsApp groups — groomers
  in Cancún all use WhatsApp business groups to share referrals.
  Find those groups and pitch there.
Head of Growth: Interesting insight. I'll redirect Marketing away from
     Instagram DMs. WhatsApp groups are a lower-CAC channel if we can
     get organic entry...
```

**`empire directive` — one-shot command:**
For when you don't need a conversation. Fire and forget — the agent receives it and acts.

```bash
# Strategic direction
empire directive peluquepet/ceo "Deprioritize billing. Focus 100% on reducing no-shows."

# Operational correction
empire directive peluquepet/head-of-growth "Stop Instagram. All outreach via WhatsApp."

# Technical decision
empire directive peluquepet/cto "Use server-side rendering, not SPA. These users are on slow phones."
```

**How it works technically:**

The human's message is injected into the agent's conversation as a high-priority `board_directive` event:

```go
type BoardDirective struct {
    From       string          // "human" (always)
    To         string          // agent ID
    Content    string          // the message
    Mode       string          // "chat" (expects response) | "directive" (fire-and-forget)
    SessionID  string          // for chat: groups messages into a conversation
}
```

The agent's prompt already says they report to the human board member. A board directive is the highest-authority input — it overrides VP decisions, CEO decisions, everything except safety constraints.

**Precedence rule:** Board directives can reprioritize work, change strategy, and override any agent decision. However, directives **cannot bypass the spend approval workflow**. If a directive implies spending real money (e.g., "buy this domain now"), the agent must still route through the spend approval chain. The human wears two hats — founder directing the team AND board member approving spend — but these are separate processes. This prevents a prompt-injected "board directive" from triggering unauthorized spend.

**Authority model for direct communication:**

| Target | When to use | Effect |
|--------|-------------|--------|
| OpCo CEO | Strategic direction, pivot, reprioritization | CEO adjusts strategy and cascades to VPs |
| Head of Product / Head of Growth | Domain-specific direction when you know the market | VP adjusts domain strategy, informs CEO |
| CTO | Technical questions, architectural direction | CTO adjusts technical approach, informs Head of Product |
| Any worker (PM, Marketing, Support) | Rare — usually go through their manager | Worker acts, reports to manager. Manager may be confused if not informed. |

**Best practice:** Talk to the CEO for most things — they cascade. Go directly to VPs or CTO when you have specific domain expertise. Going directly to workers can confuse the chain of command, but it's allowed — the worker's manager sees it in their next heartbeat.

**Logging:** All directives and chat sessions are logged as events. CEO sees board directives in their next heartbeat. VPs see directives to their workers. Nothing is hidden from the management chain — it just arrives via event log rather than being forwarded manually.

### 10.3 CLI (v0.1)

```bash
# Bootstrap (§11.0 — run once)
empire init --config ./empireai.yaml                   # Create DB, spawn holding agents, emit system.started

# Strategic directive (§11.0 — the ignition key)
empire directive "US"                                                    # → Full taxonomy scan, cast wide
empire directive "US. Focus on financial_ops. Avoid healthcare."         # → Targeted rescan

# Direct communication
empire chat <vertical>/<agent>                         # Interactive conversation
empire chat peluquepet/ceo                             # Talk to PeluquePet CEO
empire chat peluquepet/cto                             # Talk to CTO directly
empire directive <vertical>/<agent> "message"          # One-shot directive

# Also works for holding-level agents
empire chat empire-coordinator                         # Talk to Empire Coordinator
empire chat factory-cto                                # Talk to Factory CTO

# Mailbox — decisions and reviews
empire mailbox list                                    # Pending decisions (action required)
empire mailbox list --critical                         # Critical items only
empire mailbox list --reviews                          # Founder review gates only
empire mailbox view <id>                               # Decision details

# Vertical approval with mandate shaping
empire mailbox decide <id> --action approve --brand peluquepet \
  --mandate-edit "pricing: $10/mo not $15" \
  --mandate-edit "focus: no-show reduction only" \
  --notes "I know this market"
empire mailbox decide <id> --action kill --notes "TAM too small"
empire mailbox decide <id> --action more-data --notes "Need pricing validation"

# Spend
empire mailbox approve-spend <id> --notes "..."
empire mailbox reject-spend <id> --notes "..."

# Founder review gates (product spec, deploy)
empire mailbox review <id> --action approve --notes "Looks good"
empire mailbox review <id> --action revise --notes "Onboarding flow is confusing, needs walkthrough"
empire mailbox review <id> --action skip                # Proceed without feedback

# Founder input + escalation responses (open-ended directives)
empire mailbox respond <id> --notes "Go with option A, no-show reduction is the real pain"
empire mailbox respond <id> --notes "Kill the Stripe integration, switch to MercadoPago"  # escalation response

# Portfolio management
empire status                                          # Full pipeline + portfolio overview
empire status --vertical <id>                          # Deep dive on one vertical
empire verticals list                                  # All verticals with stage/mode
empire verticals operating                             # Operating verticals only

# Scanning
empire scan --geography "Cancún, Mexico" --mode local_services --depth full
empire scan --geography "Paraguay" --mode saas_gap                 # Systematic taxonomy walkthrough
empire scan --geography "Paraguay" --mode saas_trend               # Macro trend monitoring
empire scan --geography "Paraguay" --mode saas_gap --categories "financial_ops,workforce_hr"  # Filter to specific categories

# Operating verticals
empire vertical <id> metrics                           # Current metrics
empire vertical <id> team                              # Agent status + org tree
empire vertical <id> logs --agent cto                  # Recent activity
empire vertical <id> kill --notes "..."                # Kill an operating vertical

# Portfolio
empire digest                                          # View latest portfolio digest
empire budget                                          # Current spend vs budget
empire deployments list                                # Running services
empire deployments health                              # Health check all

# Template management (§4.8)
empire template publish --version 1.2 --description "Add security agent"  # YAML → Postgres
empire template list                                   # All published versions
empire template current                                # Version used for next SpawnOpCo
empire template diff v1.1 v1.2                         # What changed between versions

# Secrets management (§13.1)
# Prompt management (hot-reload iteration)
empire agent prompt <agent_id>                         # Show current effective prompt
empire agent prompt <agent_id> --edit                  # Open $EDITOR, save triggers session rotation
empire agent prompt <agent_id> --revert                # Delete override, restart with template prompt
empire agent prompt <agent_id> --diff                  # Show override vs template
empire agent prompt <agent_id> --set-from <file>       # Set override from file

empire secrets set {vertical} whatsapp.token {value}   # Set per-vertical credential
empire secrets list {vertical}                         # List credential keys (values hidden)
empire secrets rotate {vertical} whatsapp              # Prompt to enter new credential

# Human tasks (§14)
empire tasks list                                      # Approved tasks waiting for execution
empire tasks list --all                                # All tasks including pending/completed
empire tasks list --category sales_call                # Filter by category
empire tasks view <id>                                 # Full task details + talking points
empire tasks claim <id>                                # Assign to yourself
empire tasks complete <id> --result "spoke with owner, interested, scheduling demo" --outcome success
empire tasks complete <id> --result "owner not reachable, wrong number" --outcome failed
empire tasks complete <id> --result "partial interest, needs follow-up" --outcome partial --follow-up
empire tasks reject <id> --notes "not worth the trip"  # Push back to Empire Coordinator
empire tasks stats                                     # Weekly budget usage, completion rate

# Founder mode configuration
empire config set founder-mode.spec-review enabled     # Enable product spec review gate
empire config set founder-mode.deploy-review enabled   # Enable deploy review gate
empire config set founder-mode.review-timeout 48h      # Auto-proceed timeout
empire config get founder-mode                         # View current settings
```

### 10.4 Notifications

Critical mailbox items trigger external notifications to the human:

```yaml
notifications:
  critical_channel: "telegram"    # telegram | email | webhook
  telegram_chat_id: "..."
  # Future: email, SMS, Slack webhook
```

**Implementation:** HTTP POST to Telegram Bot API (`https://api.telegram.org/bot{EMPIREAI_TELEGRAM_TOKEN}/sendMessage`). Token from environment variable (§13.1). If Telegram delivery fails (timeout, API error), retry 3x with backoff. If all retries fail, log the failure — the item remains in the mailbox for `empire mailbox list`. No secondary channel in v1; add email fallback when warranted.

**What triggers critical notifications:** capacity warnings (disk/CPU >70%), deploy failures in production, security incidents, payment processing broken, any `priority: critical` mailbox item.

**Human task delivery (§14):** Approved human tasks are also delivered via Telegram with full task details, talking points, and quick-reply options. The Telegram bot is bidirectional for task management:

```
Bot: 📋 [TASK] Sales call for factura.com.py
     Business: Contaduría López, Asunción
     Phone: +595 21 XXX XXX
     Talking points:
     - SIFEN deadline is March 2026
     - Their current process is Excel + manual submission
     - Offer: first 3 months free, then $30/mo
     Expected value: $30/mo recurring customer
     Deadline: Friday
     /claim  /details  /reject

You: /claim
Bot: ✅ Claimed. Reply with result when done.
     /complete_success  /complete_partial  /complete_failed

You: /complete_success Owner interested, wants demo next Tuesday.
     Has 3 employees who also need access.
Bot: ✅ Completed. Result sent to Marketing Agent.
```

Non-critical mailbox items are visible in the portfolio digest and via `empire mailbox list`.

### 10.5 Web Dashboard

The dashboard provides real-time visibility into the system for debugging, monitoring, and management. Built as a server-rendered Go web application (consistent with OpCo architecture: Go templates, mobile-first, no SPA) consuming the runtime API.

**Implementation:** The dashboard consumes the same API that the CLI uses. The API is a thin HTTP layer over existing runtime handlers — every `empire` CLI command maps to an API endpoint. See §14.3 for API surface.

#### 10.5.1 Runtime Log

All runtime operations are captured in a structured log table that serves as the backbone for dashboard views, alerts, and debugging. This is **distinct from business events** (`events` table) — this captures what the runtime itself is doing.


*Schema: see `runtime_log` in `contracts/ddl-canonical.sql`. Key columns: ts, level (debug/info/warn/error/fatal), component, action, context FKs (event_id, agent_id, vertical_id, campaign_id, scan_id, session_id), detail JSONB, error TEXT, duration_us INT. Indexed by time, component, level, and all context FKs. Partitioned by month (90-day retention).*


**What gets logged and at what level:**

| Component | Action | Level | Detail payload |
|-----------|--------|-------|----------------|
| eventbus | published | debug | `{type, source, recipients_count, passthrough}` |
| eventbus | delivered | debug | `{type, agent_id, queue_depth}` |
| eventbus | dead_letter | error | `{type, agent_id, retry_count, error}` |
| interceptor | consumed | info | `{type, handler, pipeline_type, pipeline_id}` |
| interceptor | dropped | warn | `{type, handler, reason, pipeline_state}` |
| interceptor | error | error | `{type, handler, error, pipeline_state}` |
| interceptor | gate_changed | info | `{vertical_id, gate, old, new, all_gates}` |
| interceptor | emitted | debug | `{source_event, emitted_types[]}` |
| agent_manager | spawned | info | `{agent_id, role, vertical_id, template_version}` |
| agent_manager | stopped | info | `{agent_id, reason}` |
| agent_manager | reconfigured | info | `{agent_id, changes[]}` |
| guardrails | violation | warn | `{agent_id, type, event_type, detail}` |
| guardrails | blocked | warn | `{agent_id, tool, reason}` |
| scheduler | timer_fired | info | `{schedule_id, event_type, agent_id}` |
| scheduler | timer_created | debug | `{schedule_id, event_type, cron_expr}` |
| gateway | webhook_received | info | `{provider, vertical_id, endpoint}` |
| gateway | webhook_invalid | warn | `{provider, reason, ip}` |
| session | rotated | info | `{agent_id, old_session, new_session, reason, turn_count}` |
| session | lock_acquired | debug | `{agent_id, session_id, lock_owner}` |
| session | lock_expired | warn | `{agent_id, session_id, lock_owner, expired_at}` |
| recovery | replay_started | info | `{unprocessed_events, undelivered_events}` |
| recovery | replay_completed | info | `{events_replayed, errors}` |
| budget | threshold_crossed | warn | `{level, current_spend, budget_cap, percentage}` |
| budget | spend_recorded | debug | `{agent_id, amount, model, tokens}` |
| mailbox | item_created | info | `{type, vertical_id, from_agent, priority}` |
| mailbox | item_decided | info | `{type, vertical_id, decision, response_time_hours}` |
| mailbox | item_timeout | warn | `{type, vertical_id, timeout_hours}` |

**Log level policy:**

- `debug` — High-volume operational detail. Disabled in production by default. Enable per-component via `empire config set log.level.eventbus debug`.
- `info` — State changes, lifecycle events, gate transitions. Always on. This is the primary debugging stream.
- `warn` — Dropped events, guardrail violations, timeouts, lock expirations. Always on. These are the "something unexpected but handled" signals.
- `error` — Handler failures, dead letters, unrecoverable errors. Always on. These need attention.
- `fatal` — System-level failures (DB connection lost, runtime panic). Always on. Triggers immediate Telegram alert.

**Runtime log configuration:**

```yaml
# In empireai.yaml
logging:
  default_level: info                      # Global default
  component_overrides:                     # Per-component override
    eventbus: warn                         # Quiet in production (high volume)
    interceptor: info                      # Always want state changes
    guardrails: info                       # Always want violations
  retention_days: 90                       # Partition pruning
  telegram_alerts:                         # Push critical logs to Telegram
    levels: [error, fatal]
    throttle_minutes: 5                    # Max 1 alert per 5 minutes per component
```

#### 10.5.2 Dashboard Views (Detail)

**Tab 1: Live Event Stream**

Real-time feed of runtime_log + events, interleaved by timestamp. Server-Sent Events (SSE) push — no polling.

```
14:32:01 INFO  interceptor  consumed      vertical.shortlisted     pet-grooming-py  → handleShortlisted
14:32:01 INFO  interceptor  gate_changed  pet-grooming-py          G1:_ G2:_ G3:_ G4:_
14:32:01 INFO  interceptor  emitted       pet-grooming-py          → validation.started, brand.requested
14:32:01 DEBUG eventbus     delivered     validation.started        → business-research-agent
14:32:01 DEBUG eventbus     delivered     brand.requested           → pre-brand-agent
14:35:22 INFO  agent_mgr    task_started  business-research-agent   task: research pet-grooming-py
14:38:15 INFO  interceptor  gate_changed  pet-grooming-py          G1:_ G2:_ G3:_ G4:✓
```

Filters: component, level, agent, vertical, event type. Click any row to expand full `detail` JSON.

**Tab 2: Pipeline Dashboard**

Two-panel view.

*Left panel — Factory funnel:*
```
Campaigns    ████████░░  2 active, 1 paused (backpressure)
Scans        ███░░░░░░░  1 active (saas_gap PY: 3/5 agents)
Discovered   12 total    7 this campaign
Scored       10          2 pending scoring
Shortlisted   4          2 in validation, 1 packaged, 1 approved
Marginal      3          2 parked, 1 promoted
Killed        5          3 scoring, 2 CTO veto
```

*Right panel — Validation pipelines:*

Each active vertical is a card showing gate status, timeline, and current wait:

```
┌─ pet-grooming-py ──────────────────────────────┐
│ Status: active    Spec v2    Rev 1/3   Inner 0/5│
│                                                  │
│ G1 Research  ✓  14:35 (3m)                      │
│ G2 Spec      ✗  waiting: cto.spec_review        │
│    └ spec.approved 14:41 → auditor ✓ 14:43      │
│    └ cto reviewing since 14:43 (4m)             │
│ G3 CTO       ✗                                  │
│ G4 Brand     ✓  14:38 (6m)                      │
│                                                  │
│ Timeline: shortlisted 14:32 ─── now 14:47 (15m) │
└──────────────────────────────────────────────────┘
```

Cards turn yellow when approaching alert threshold, red when exceeded.

**Tab 3: Agent Activity**

Per-agent rows: status (active/idle/stuck), current task, session turn count, token spend (session/24h/total), last event emitted, last tool call, parse success rate, guardrail violations.

Click an agent to see:
- Current conversation (live, streaming turns as they happen)
- Session history with rotation reasons
- Token spend chart (by day)
- Events emitted/received timeline
- Guardrail violations log

**Tab 4: Mailbox & Decisions**

Pending items sorted by priority and age. Each item shows: type, vertical, from agent, summary, age, timeout countdown.

Decision history with response time metrics. Average decision latency by type. Items approaching timeout highlighted.

**Tab 5: Health & Spend**

System health: Postgres connections, API spend rate (burn chart with budget line), container status, agent session counts, queue depths.

Stuck detection panel: pipelines with no transition beyond expected timeframe (thresholds from §4.2.2.6). Each stuck item shows what event is expected and when it was last seen.

Budget panel: spend by agent, by model tier, by vertical. Projected monthly burn. Threshold proximity.

**Tab 6: Pipeline Flow Visualizer**

Interactive visual representation of the factory pipeline and OpCo communication flows. Two modes: **design-time** (static architecture view derived from spec data) and **runtime** (live event flow overlaid on the architecture). This tab exists because the spec's 26 intercepted event types, 4 state machines, and 3 rubrics create a complexity level that text alone cannot convey — every architectural bug found by reviewers required tracing event flows across multiple spec sections simultaneously.

**Design-time mode (Architecture View):**

Renders the complete factory pipeline as an interactive directed graph. Nodes and edges are derived from structured data already in the system: event catalog (§5.4), interceptor switch cases (§4.2.2), subscription lists (agent YAML configs), routing tables (`configs/agents/templates/routes.yaml`), and state machine definitions (§4.2.2.1-4.2.2.9).

*Node types (visually distinct):*

| Node type | Shape | Color | Examples |
|-----------|-------|-------|----------|
| Agent | Rounded rectangle | Blue | Empire Coordinator, MRA, Analysis Agent, QA |
| Interceptor case | Diamond | Orange | `handleVerticalDiscovered`, `handleShortlisted` |
| State machine | Rectangle with inner state | Purple | Campaign (modes, status), ScoringAccumulator (dimensions received), ValidationPipeline (G1-G4) |
| Gate/threshold | Small circle | Red (closed) / Green (open) | Viability floor, hard gates, cycle limit |
| Runtime process | Hexagon | Gray | `computeComposite()`, `classifyMigration()`, `fanOutOpCo()` |
| Human | Star | Yellow | Mailbox |

*Edge types:*

| Edge type | Style | Label | Examples |
|-----------|-------|-------|----------|
| Event (factory) | Solid arrow | Event type | `vertical.discovered` → interceptor |
| Event (OpCo internal) | Dashed arrow | Event type | `qa.validation_failed` → CTO |
| Message (`agent_message`) | Dotted arrow | Description | CTO → Backend "build assignment" |
| Intercepted (consumed) | Solid arrow, event stops | Event type + "consumed" | `scan.completed` → `handleScanCompleted` |
| Passthrough | Solid arrow, continues | Event type + "passthrough" | `vertical.killed` → interceptor → EC |

*Layout — three swim lanes:*

```
┌─────────────────────────────────────────────────────────────────┐
│ HUMAN LAYER                                                      │
│   [Mailbox] ←→ [CLI/Dashboard] ←→ [Telegram]                   │
├─────────────────────────────────────────────────────────────────┤
│ FACTORY PIPELINE                                                 │
│                                                                  │
│ [Directive] → [Campaign] → [Scan] → [Discovery] → [Scoring]    │
│     │            ╔══════╗     │        ╔══════╗      ╔══════╗   │
│     │            ║Modes ║     │        ║Accum ║      ║Accum ║   │
│     │            ║Cursor║     │        ║Agent#║      ║Dims# ║   │
│     │            ╚══════╝     │        ╚══════╝      ╚══════╝   │
│     │                         │                          │       │
│     │              ┌──────────┴──────────┐               │       │
│     │              │   MRA  TRA  Scanners│               │       │
│     │              └─────────────────────┘               │       │
│     │                                              ◊ viability   │
│     │                                              ◊ hard gates  │
│     │                                                    │       │
│     │         ┌─shortlisted─→ [Validation] ──→ [Review]  │       │
│     │         │                 ╔════════╗               │       │
│     │         │                 ║G1 G2   ║               │       │
│     │         │                 ║G3 G4   ║               │       │
│     │         │                 ╚════════╝               │       │
│     │         │                     │                    │       │
│     │    ◊ composite           [Packaging]          ◊ rejected   │
│     │         │                     │                → digest     │
│     │    marginal→[EC]         [Mailbox]                         │
│     │                              │                             │
│     │                         approved → [SpawnOpCo]             │
├─────────────────────────────────────────────────────────────────┤
│ OPCO LAYER (per vertical)                                        │
│                                                                  │
│   [CEO] ← reports ← [VPs] ← reports ← [Workers]               │
│     │                  │                    │                    │
│   spend              manage              build                   │
│   approval          coordinate           deploy                  │
│     │                  │                 test                    │
│   [Mailbox]     [CoS] bridge          [Holding DevOps]          │
│                                                                  │
│   Bootstrap routes: ━━━ (solid, immutable)                       │
│   Seeded routes:    ╌╌╌ (dashed, removable)                     │
│   Discovered:       ⋯⋯⋯ (dotted, agent-proposed)               │
└─────────────────────────────────────────────────────────────────┘
```

*Interactions:*

- **Click any node** → side panel shows: agent prompt (truncated), tools available, subscriptions, emit permissions, conversation mode, max turns
- **Click any edge** → side panel shows: event schema (from EventSchemaRegistry), producer (`agent-tools.yaml` emit_events), consumers, whether intercepted or passthrough, and the interceptor handler code reference
- **Click any state machine** → expands to show fields, transitions, reset conditions, and the events that trigger each transition
- **Click any gate** → shows pass/fail conditions, current threshold values, and which rubric it belongs to
- **Hover over an agent** → highlights all edges (events) connected to that agent — both emitted and consumed. This is the "blast radius" view that would have caught the `agent_message` gap (PM highlighted → zero outbound edges visible)
- **Filter by rubric** → dims nodes/edges not involved in the selected scoring path (all modes now use `universal` rubric as of v2.0.39)
- **Filter by vertical lifecycle stage** → highlights only the active portion of the pipeline for a given stage (scanning, scoring, validation, operating)

*Data source:* The architecture view is generated from config files at startup and refreshed on template version changes. No manual diagram maintenance — the visualization reflects the actual system configuration.

```
Source data → Graph model:
  agents/*.yaml           → Agent nodes (role, tools, subscriptions, emit list)
  EventSchemaRegistry     → Edge labels + schema popups
  PipelineCoordinator     → Interceptor diamonds + state machine nodes
  routing_rules           → OpCo layer edges (bootstrap/seeded/discovered)
  rubricWeights           → Gate nodes + threshold annotations
  modeToRubric            → Rubric filter highlighting
```

**Runtime mode (Live Flow):**

Overlays live event data on the architecture graph. When activated, events from the `events` table and `runtime_log` flow through the graph as animated dots traveling along edges. State machine nodes update in real-time to show current field values.

*Visual indicators:*

| Indicator | Meaning |
|-----------|---------|
| Animated dot on edge | Event in transit (color = event category) |
| Node pulse (blue) | Agent currently active (session in progress) |
| Node pulse (orange) | Agent processing event (turn in progress) |
| State machine field change | Field highlight flash on update |
| Gate color change | Red → green (passed) or stays red (rejected) |
| Edge glow (red) | Event delivery failed or dead-lettered |
| Counter badge on node | Events processed in last hour |
| Thickness of edge | Event frequency (thicker = more events on this path) |

*Runtime data feed:*

The dashboard subscribes to runtime events via SSE (same mechanism as Tab 1 Live Event Stream). Each event is mapped to the graph:

```go
// Map runtime event to graph animation
type FlowEvent struct {
    EventID     uuid.UUID
    EventType   string
    SourceNode  string   // agent_id or "runtime"
    TargetNodes []string // resolved recipients
    Intercepted bool     // consumed by pipeline coordinator?
    Passthrough bool
    Timestamp   time.Time
}
```

The runtime emits `FlowEvent` records to the SSE stream alongside existing runtime_log entries. The dashboard client maps each FlowEvent to an edge in the architecture graph and animates a dot traveling from source to target.

*Per-vertical focus:*

Click any vertical in the pipeline dashboard (Tab 2) → opens Tab 6 in runtime mode filtered to that vertical. Shows only the events, agents, and state machines relevant to that vertical's current pipeline stage. The scoring accumulator shows which dimensions have been received, the validation pipeline shows gate status, and the OpCo layer shows agent activity.

*Replay mode:*

Select a time range → replays events from the `events` table through the graph at 10x/50x/100x speed. Useful for post-mortem analysis: "what happened to vertical X between 14:00 and 15:00?" The graph replays the exact sequence of events, interceptor decisions, gate transitions, and agent invocations. Pause at any point to inspect state.

**Implementation notes:**

- **Rendering:** Client-side JavaScript using D3.js force-directed graph (for the interactive layout) or dagre (for the hierarchical swim-lane layout). D3 is already available in the OpCo frontend stack.
- **Graph model:** Server generates a JSON graph structure from config files on startup. The `/api/pipeline/graph` endpoint returns nodes and edges. The client renders and handles interactions.
- **Live overlay:** SSE subscription to `/api/events/flow` returns FlowEvent stream. Client maps events to graph edges and animates.
- **Replay:** `/api/events/flow?start={ts}&end={ts}` returns historical FlowEvents from the `events` table. Client plays them sequentially with configurable speed.
- **Performance:** The architecture graph is static (changes only on config reload). Live events are overlaid as transient animations — no graph re-layout on each event. At typical event volumes (10-50/minute during active scanning), animation is smooth. For burst periods (sharded MRA emitting 52 subcategory reports), events queue and animate in rapid succession.
- **Mobile:** The swim-lane layout is horizontal-scrollable on mobile. Pinch-to-zoom on the graph. Side panels slide up from bottom. Touch targets sized for mobile interaction.

**Phase rollout:**

| Phase | What | When |
|-------|------|------|
| Phase 1 | Architecture view (design-time) — factory pipeline only | With dashboard v1 |
| Phase 2 | Architecture view — OpCo layer (bootstrap + seeded routes) | With first OpCo |
| Phase 3 | Runtime overlay — live event animation | After pipeline stable |
| Phase 4 | Replay mode | After runtime_log + events retention proven |
| Phase 5 | Discovered routes (OpCo layer evolves over time) | After OpCo steady-state |

#### 10.5.3 Telegram Integration

The dashboard health checks also drive Telegram push notifications:

| Trigger | Message |
|---------|---------|
| `runtime_log` level=error/fatal | `🔴 {component}: {action} — {error}` |
| Pipeline stuck > alert threshold | `⚠️ {vertical} stuck at {state} for {duration}. Waiting for {expected_event}.` |
| Budget threshold crossed | `💰 Budget at {pct}%. {action_taken}.` |
| Mailbox item pending > 48h | `📬 {count} mailbox items pending > 48h. Oldest: {summary}` |
| Campaign completed | `✅ Campaign {geo} complete. {discovered} discovered, {shortlisted} shortlisted.` |
| Vertical approved → spinup | `🚀 Spinning up {vertical_name}. Building team.` |

Throttled: max 1 alert per component per 5 minutes (configurable). Digest messages (campaign complete, spinup) are not throttled.

---

## 11. Recovery & Resilience

### 11.0 Cold Start (First Boot)

The system has never run before. Database is empty. No agents, no events, no verticals.

**Step 1: `empire init`**

```bash
empire init --config ./empireai.yaml
```

This is the only manual command. It:
1. Creates Postgres database and runs schema migrations (§8 tables)
2. Publishes the initial org template: reads `configs/agents/*.yaml`, validates schema, writes to `org_templates` as version "1.0"
3. Reads `configs/agents/roster.yaml` for holding-level seed agents. Creates agent rows for: Empire Coordinator, Factory CTO, Holding DevOps, Operations Analyst, Spec Auditor, and factory pipeline agents (Discovery Coordinator, Analysis Agent, Validation Coordinator, Business Research Agent, Lightweight Spec Agent, Spec Reviewer, Pre-Brand Agent, Market Research Agent, Trend Research Agent)
4. Writes initial config from `empireai.yaml` to `runtime_config` table (budget caps, human task limits, notification config)
5. Starts the orchestrator process and container runtime
6. Spawns all holding/factory agents from their config definitions (no hardcoded agent definitions in Go)
7. Emits `system.started` event with `is_cold_start: true`

**No hardcoded agent definitions.** `empire init` reads everything from config files. If `configs/agents/roster.yaml` doesn't exist or is invalid, init fails with a clear error. The Go code never contains default agent prompts, tool lists, or subscription lists — those live in YAML.

**Step 2: `system.started` → Empire Coordinator wakes up**

Empire Coordinator receives `system.started` and evaluates its state:

```
IF no geographies exist AND no verticals exist:
  → Cold start. Enter "awaiting directive" state.
  → Post to Telegram: "EmpireAI online. No active campaigns. 
     Send a directive to begin: empire directive '...'"

IF geographies exist but no active scans:
  → Warm restart. Check for pending scan campaigns, resume.

IF active verticals exist:
  → Hot restart. Verify all OpCo teams are running. 
     Request CEO reports from each active vertical.
```

On cold start, the system is alive but idle. Nothing runs until the human sends the first directive.

**Step 3: First directive**

```bash
# Simplest possible directive — just a geography. System casts wide.
empire directive "US"
```

Empire Coordinator processes the directive:
1. Creates geography: US (country-level, language: en-US, currency: USD)
2. No taxonomy focus → full 52-subcategory scan
3. No constraints → default pre-filter (red flags + ICP check + evidence completeness)
4. Intent inferred: "first scan, casting wide"
5. Emits `scan.requested` — `mode: saas_gap`, full taxonomy, normal priority
6. Queues remaining modes: saas_trend, local_services (default campaign cycle)
7. Acknowledges via Telegram: "Campaign started. Full taxonomy scan for US. Will surface results to mailbox."

A more targeted directive for subsequent campaigns:

```bash
empire directive "US. Focus on financial_ops and compliance_governance. 
Skip workforce_hr and marketing_sales — dead zones from last scan.
Price range $15-75/mo. Avoid healthcare and anything requiring financial custody.
Hunt for compliance-deadline patterns. Budget cap $150."
```

Empire Coordinator processes:
1. Geography: US (already exists)
2. TaxonomyFocus: [financial_ops, compliance_governance]
3. TaxonomySkip: [workforce_hr, marketing_sales]
4. PriceRange: {Min: 1500, Max: 7500, Currency: USD}
5. AvoidSectors: [healthcare, financial_custody]
6. KnownPatterns: [compliance_deadline_tool]
7. BudgetCap: 15000 (cents)
8. Intent: "rescan, drilling into high-signal areas"
9. ScanContext: populated by runtime from previous campaign results

Directives can redirect strategy at any time:
```bash
empire directive "Pause US scans. Start scanning UK — 
same constraints as US, plus look for MTD compliance tools."
```

Empire Coordinator adjusts: pauses active US scans, creates UK geography, launches new campaign with constraints carried over plus UK-specific pattern hint. No restart needed.

**Events:**

| Event | Emitter | Consumer | Payload |
|-------|---------|----------|---------|
| `system.started` | Runtime | Empire Coordinator | timestamp, config_version, agent_count |
| `system.directive` | Human (via CLI) | Empire Coordinator | directive_text, timestamp |

Both events added to Empire Coordinator's subscription list.

### 11.1 Crash Recovery

On startup:
1. Load all active agents from `agents` table
2. Reconstruct routing tables from `routing_rules` for each vertical
3. **Reload active runtime sessions** from `agent_sessions` table. Reclaim expired leases (where `lock_expires_at < now()`). For CLI runtime: sessions are resumable via `claude -p -r <session_id>`. For API runtime: rebuild in-memory conversation state from `conversations` table.
4. **Replay unprocessed events** using two recovery paths:
   - **Factory agents (subscription-based):** Find events matching agent's subscribed types with no successful receipt (`status IN ('processed', 'skipped')`), bounded by agent's `started_at`
   - **OpCo agents (delivery-manifest-based):** Find events with a row in `event_deliveries` for this agent but no successful receipt, bounded by agent's `started_at`
5. **Retry errored events:** Find events with `status = 'error'` and `retry_count < 3`. Re-deliver with exponential backoff. Events exceeding retry limit are marked `dead_letter` and escalated to agent's manager.
6. Load active conversations from `conversations` table
7. Replay unprocessed events into channels in chronological order
8. Respawn all operating teams for active verticals
9. Resume agent reasoning loops from persisted conversation state

### 11.2 Agent Failure

If a goroutine panics:
1. AgentManager catches the panic
2. Persists current conversation state to Postgres
3. Restarts the goroutine with exponential backoff (1s, 5s, 30s, 2m, 10m)
4. Replays unprocessed events (same recovery paths as §11.1 — subscription-based for factory, delivery-manifest for OpCo)
5. If 5 consecutive panics within 1 hour: mark agent as `failed`, notify manager agent, escalate to mailbox if manager can't resolve

### 11.3 LLM Runtime Failure (API + CLI)

**API runtime failure:**
1. Retry with exponential backoff (max 3 retries)
2. If persistent failure, emit `task.escalated` with error context
3. Coordinator decides: retry later, reassign, or escalate to mailbox

**CLI runtime failure:**
1. Retry with exponential backoff (max `llm.claude_cli.retries` attempts)
2. If response fails to parse as valid JSON (parse_ok = false): send one repair turn asking Claude to re-emit in correct format
3. If `llm.session.rotate_on_parse_failures` consecutive parse failures: rotate session with checkpoint summary (conversation state may be corrupted) and retry on fresh session
4. If still failing after rotation: emit `task.escalated` with runtime metadata (session_id, turn_count, last error, runtime_mode = "cli_test")
5. Coordinator decides: retry later, reassign, or escalate to mailbox

**Common to both:** All failures are logged to `agent_turns` with `parse_ok`, `latency_ms`, `retry_count`, and `error` for post-incident analysis.

### 11.4 Deployment Failure (Operating Mode)

**Staging failure:** Low stakes — no customer impact.
1. Holding DevOps emits `devops.deploy_failed` (environment: "staging") for audit, messages OpCo DevOps with failure details via `agent_message`
2. OpCo DevOps reports to CTO. CTO diagnoses, fixes, resubmits to staging. No rollback needed — staging has no live traffic.

**Production failure:**
1. Holding DevOps emits `devops.deploy_failed` (environment: "production") for audit, messages OpCo DevOps with error details via `agent_message`
2. OpCo DevOps reports failure to CTO
3. CTO **decides**: rollback to previous version or fix-and-redeploy
4. If rollback: CTO tells OpCo DevOps → OpCo DevOps prepares rollback manifest → emits `devops.rollback_requested` → Holding DevOps executes rollback → emits `devops.rollback_complete` or `devops.rollback_failed` for audit → messages result to OpCo DevOps via `agent_message`
5. If fix-and-redeploy: CTO coordinates fix (Backend/Frontend), then staging → QA → production
6. CTO reports outcome to OpCo CEO
7. If `devops.rollback_failed` or unable to resolve: CEO escalates to mailbox

**Boundary:** CTO diagnoses and decides. OpCo DevOps prepares artifacts. Holding DevOps executes privileged actions (migrations, binary deploy, nginx, systemd). Same boundary as forward deploys.

### 11.5 Operating Agent Recovery

Operating agents have session-scoped conversations. On crash:
1. Reload last conversation state from Postgres
2. If context window has grown too large, reload from last summary
3. Resume operation — any in-flight customer interactions may be lost
4. Support Agent re-checks for unhandled messages
5. **VP crash:** Workers continue peer-to-peer flows (routing table still active). VP recovers, re-reads domain state, resumes observation. Workers unaffected during VP downtime.
6. **CEO crash:** VPs continue managing their domains (they don't depend on CEO for daily operations). CEO recovers, re-reads VP summaries. If CEO is unrecoverable, mailbox alert to human.

### 11.6 Backup & Disaster Recovery

**Phase 1-4 (single Hetzner box, no customer data):** Postgres `pg_dump` nightly via cron to a local directory. Human responsibility to periodically copy off-box (scp to local machine or object storage). Acceptable because factory pipeline data is regenerable and no customer data exists yet.

**Phase 5+ (operating verticals with customer data):** Two-tier backup strategy:

1. **Logical backups:** Holding DevOps schedules nightly `pg_dump` per schema + full database dump. Backups stored to off-box destination (Hetzner Storage Box or S3-compatible). Retention: 7 daily, 4 weekly, 3 monthly. Test restore quarterly. Capacity alert if backup storage exceeds threshold.

2. **WAL archiving + PITR:** Enable `archive_mode = on` and continuous WAL archiving to off-box storage. Combined with a weekly `pg_basebackup`, this provides point-in-time recovery to any moment — critical for the migration safety guardrails (§7.8). When Holding DevOps detects data corruption from a bad migration, PITR restores the schema to the moment before the migration ran, while the binary rollback handles the application code. WAL archiving adds ~5-10% write overhead and storage proportional to write volume — acceptable for the scale of Phase 5+ verticals.

**Recovery priority:** Runtime schema first (agents, events, routing — the system itself), then vertical schemas (customer data). Binary artifacts can be rebuilt from source in `/opt/empireai/verticals/`.

**Deferred:** Automated failover, multi-box replication. These become relevant at 5+ verticals or when SLA commitments exist.

---

## 12. Data Handling Policy

This section defines how operating agents handle customer data. It is a **pre-launch requirement** — no vertical may serve real customers until these policies are implemented.

### 12.1 Data Classification

| Class | Examples | Agent Processing | Storage | Retention |
|-------|----------|-----------------|---------|-----------|
| **Business operational** | Appointment times, service types, pricing, schedules | Full processing allowed | Vertical DB (schema-isolated) | Lifetime of vertical |
| **Customer contact** | Names, phone numbers, email addresses | Processing allowed in context of service delivery | Vertical DB only (never in conversation logs) | 90 days after last interaction, or customer deletion request |
| **Customer messages** | WhatsApp messages, email content, support conversations | Processing allowed for response generation | Vertical DB (original messages). **Redacted** in agent conversation logs | 90 days after last interaction |
| **Financial** | Payment amounts, payment status, transaction IDs | Processing allowed | Vertical DB only | 1 year (tax/audit), then anonymized |
| **Sensitive** | Health info, government IDs, passwords | **Must not be processed or stored by agents** | Never stored | N/A — reject and instruct customer to provide via secure channel |

### 12.2 Conversation Log Redaction

Agent conversation logs (stored in `conversations` table for context continuity) are redacted before persistence:

- Phone numbers → `[PHONE]`
- Email addresses → `[EMAIL]`
- Full names → first name only (e.g., "María G.")
- Payment details → `[PAYMENT_REF]`

Redaction is applied by the Conversation Manager before writing to Postgres, not by the agent prompt. The agent sees full data in its working context window; the persisted log is sanitized.

### 12.3 Tenant Isolation (enforced)

- `sql_execute` scoped to vertical schema via `SET search_path` (see §4.5)
- File system access tiered: OpCo agents confined to own vertical, Factory CTO to scaffold, Holding DevOps has cross-vertical access (see §4.5)
- Agent conversation context never includes data from other verticals
- Event payloads crossing vertical boundaries (to Empire Coordinator, Operations Analyst) contain aggregate metrics only, never individual customer data

### 12.4 Claude API Considerations

Customer messages are sent to Claude API as part of agent conversations. Per Anthropic's data policy:
- API inputs are not used for model training (as of commercial API terms)
- Conversation data is retained by Anthropic for abuse monitoring (30 days as of current policy)
- If a vertical operates in a jurisdiction requiring data residency, this must be evaluated per-vertical before launch

### 12.5 Customer Rights

- **Deletion:** Customer requests data deletion → Support agent triggers purge of all customer records from vertical DB. Conversation logs containing the customer's data are already redacted.
- **Export:** Customer requests their data → Support agent exports from vertical DB. Format: JSON or CSV via Support tool.
- **Opt-out of AI processing:** If legally required, provide human escalation path. Currently: message forwarded to human operator via mailbox.

---

## 13. Configuration

```yaml
# empire.yaml
runtime:
  max_concurrent_agents: 50
  event_poll_interval: 2s
  recovery_on_startup: true

database:
  host: localhost
  port: 5432
  name: empireai
  pool_size: 30

claude:
  # DEPRECATED — use llm.claude_api instead. Kept for backward compatibility.
  default_model: claude-sonnet-4-5-20250929
  haiku_model: claude-haiku-4-5-20251001
  max_retries: 3
  retry_backoff: 2s

llm:
  runtime_mode: api                        # 'api' (production) | 'cli_test' (development)
  session:
    lock_ttl: 120s                         # Session lease TTL — reclaimed on expiry
    rotate_after_turns: 50                 # Task-scoped rotation threshold (session-scoped: 200)
    rotate_on_parse_failures: 3            # Consecutive parse failures trigger rotation
  claude_api:                              # Used when runtime_mode = api
    default_model: claude-sonnet-4-5-20250929
    haiku_model: claude-haiku-4-5-20251001
    max_retries: 3
    retry_backoff: 2s
  claude_cli:                              # Used when runtime_mode = cli_test
    command: claude                         # Path to Claude CLI binary
    timeout: 120s                          # Per-turn timeout
    output_format: json                    # Must be 'json' for structured parsing
    retries: 3
    no_session_persistence: false          # MUST be false — session resume requires persistence
    use_tmux: false                        # Optional: true only for manual operator debugging

hetzner:
  host: "your-hetzner-ip"
  ssh_key: ~/.ssh/empireai
  base_domain: empireai.com
  verticals_dir: /opt/empireai/verticals
  port_range_start: 8001
  staging_port_range_start: 9001         # Staging ports: 9001+ (parallel to 8001+ prod)
  gateway_port: 8080                     # Inbound Gateway HTTP listener (webhooks from WhatsApp, Stripe, etc.)

registrar:
  provider: cloudflare  # or namecheap
  api_key: "..."

whatsapp:
  provider: "twilio"  # or direct Meta Business API
  api_key: "..."

mailbox:
  poll_interval: 30s
  stale_threshold: 24h
  digest_max_interval: 7d      # Portfolio digest at least every 7 days
  digest_on_ceo_report: true   # Also digest when CEO reports arrive

founder_mode:
  spec_review: true              # Send product specs to mailbox for human review
  deploy_review: true            # Send first deploys to mailbox for human review
  review_timeout: 48h            # Auto-proceed if no response
  founder_input: true            # Allow agents to request founder input
  founder_input_timeout: 48h     # Use CEO recommendation if no response
  # Scale down: disable gates as portfolio grows
  # empire config set founder-mode.spec-review false

budget:
  factory_monthly_cap: 200
  per_vertical_monthly_cap: 200
  auto_approve_spend_below: 15
  portfolio_monthly_cap: 1000

  # Human Task System (§14)
  human_tasks:
    max_tasks_per_week: 3           # Phase 1: founder is sole executor
    budget_reset: "monday"           # Weekly reset day
    auto_expire_hours: 168           # Tasks expire after 1 week if unclaimed
    categories_enabled:
      - sales_call
      - government_visit
      - escalated_support
      - partnership
      - verification
      - ground_truth
      - banking

agents:
  # Factory agents (always running)
  empire_coordinator:
    config_path: ./agents/empire-coordinator.yaml
  factory_cto:
    config_path: ./agents/factory-cto.yaml
  holding_devops:
    config_path: ./agents/holding-devops.yaml
  operations_analyst:
    config_path: ./agents/operations-analyst.yaml
  discovery_coordinator:
    config_path: ./agents/discovery-coordinator.yaml
  analysis_agent:
    config_path: ./agents/analysis-agent.yaml
  validation_coordinator:
    config_path: ./agents/validation-coordinator.yaml
  spec_auditor:
    config_path: ./agents/spec-auditor.yaml

  # Operating agent templates (instantiated per vertical)
  operating_templates:
    opco_ceo:
      config_path: ./agents/templates/opco-ceo.yaml
    chief_of_staff:
      config_path: ./agents/templates/chief-of-staff.yaml
    vp_product:
      config_path: ./agents/templates/vp-product.yaml
    vp_growth:
      config_path: ./agents/templates/vp-growth.yaml
    cto_agent:
      config_path: ./agents/templates/cto-agent.yaml
    tech_writer:
      config_path: ./agents/templates/tech-writer.yaml
    backend_agent:
      config_path: ./agents/templates/backend-agent.yaml
    frontend_agent:
      config_path: ./agents/templates/frontend-agent.yaml
    devops_agent:
      config_path: ./agents/templates/devops-agent.yaml
    qa_agent:
      config_path: ./agents/templates/qa-agent.yaml
    pm_agent:
      config_path: ./agents/templates/pm-agent.yaml
    marketing_agent:
      config_path: ./agents/templates/marketing-agent.yaml
    support_agent:
      config_path: ./agents/templates/support-agent.yaml
```

### 13.1 Secrets & Credentials

Three tiers of credential management:

**Global secrets** — loaded at runtime startup from environment variables or `secrets.yaml` (gitignored):

| Secret | Env var | Used by | Phase |
|--------|---------|---------|-------|
| Anthropic API key | `ANTHROPIC_API_KEY` | LLM runtime (every agent) | 1 |
| Postgres password | `EMPIREAI_DB_PASSWORD` | Runtime process | 1 |
| Telegram bot token | `EMPIREAI_TELEGRAM_TOKEN` | Critical notification delivery (§10.4) | 1 |
| Cloudflare API token | `CLOUDFLARE_API_TOKEN` | dns_configure, SSL provisioning | 3 |
| Domain registrar API key | `REGISTRAR_API_KEY` | domain_purchase, domain_availability_check | 3 |

**Per-vertical secrets** — stored in `verticals.credentials` JSONB column (encrypted at rest via Postgres `pgcrypto`), injected into agent tool context at session start:

```sql
-- Example credentials JSONB for a vertical:
{
  "whatsapp": { "phone_id": "...", "token": "...", "webhook_secret": "..." },
  "mercadopago": { "access_token": "...", "webhook_secret": "..." },
  "instagram": { "access_token": "...", "page_id": "..." },
  "email": { "smtp_host": "...", "smtp_user": "...", "smtp_pass": "..." }
}
```

These secrets are populated via mailbox when the human completes external service setup (WhatsApp Business verification, payment provider onboarding). Agents never see raw credentials in their system prompt — the tool executor injects them at call time.

**Credential access rules:**
- Tool executor reads credentials from `verticals.credentials` when executing `whatsapp_business_api`, `email_api`, `instagram_api`, `mercadopago_api` tools
- Agents request "send WhatsApp message to X" — tool executor handles authentication
- Credentials are never included in conversation logs or event payloads
- Human updates credentials via `empire secrets set {vertical} whatsapp.token {value}`

**Rotation:** Human responsibility via CLI. No auto-rotation in v1. If a key expires, the tool returns an auth error, agent escalates to CTO, CTO escalates to CEO, CEO sends to mailbox.

### 13.2 Tool Architecture: Two-Tier Model

EmpireAI agents run inside LLM coding environments (Claude Code, Codex, or equivalent) that already ship battle-tested general-purpose tools. We don't reimplement these — we constrain the environment they run in and inject domain-specific tools on top.

**Tier 1 — Native LLM tools (provided by the coding environment):**

These are built into Claude Code / Codex and available to every agent automatically. They're well-tuned, handle edge cases (caching, locale, readability extraction, retries), and improve with each provider release.

| Capability | What the LLM environment provides | Our responsibility |
|------------|----------------------------------|-------------------|
| Web search | Full search with locale, freshness filters, caching, multiple providers | Nothing — already works. LATAM agents benefit from built-in locale support. |
| Web fetch | HTTP GET + Readability extraction (HTML → markdown), Chrome-like UA, redirect handling | Nothing — already works. Content extraction quality is provider-maintained. |
| File read/write/edit | Full filesystem operations with diff-based editing | Container volume mounts scope to agent's project path (§4.1.1) |
| Shell execution | Run arbitrary commands, background processes, output capture | Container isolation. Agent runs `go build`, `go test`, `psql` natively. |
| Code intelligence | Syntax awareness, linting, test interpretation, error diagnosis | Nothing — this is the LLM's core strength. |

**Scoping is the container, not the tool.** See §4.1.1 for Docker compose layout, volume mounts per tier, workspace image, and how the orchestrator spawns agent sessions. The LLM's native `shell_execute` running `go build ./cmd/server` inside a Docker container scoped to `/opt/empireai/verticals/pet-grooming/` is identical in effect to our old `go_build` wrapper — but better tested, with proper output streaming and error handling we'd never match.

**Tier 2 — EmpireAI-specific tools (we build and inject):**

These are custom tools registered into the LLM session via the standard tool-calling API. They implement capabilities that don't exist in general-purpose coding environments — organizational coordination, business integrations, and infrastructure management.

**Organizational tools** (the multi-agent coordination layer):

| Tool | Implementation | Scoping |
|------|---------------|---------|
| `agent_message` | Appends content to target agent's conversation via AgentManager | Same vertical only (enforced by runtime) |
| `agent_hire` | `AgentManager.SpawnAgent()` — creates goroutine, persists to `agents` table | Parent chain authorization check |
| `agent_fire` | `AgentManager.StopAgent()` — kills goroutine, marks agent `terminated` | Parent chain authorization check |
| `agent_reconfigure` | `AgentManager.ReconfigureAgent()` — updates prompt/tools/constraints, triggers session rotation | Parent chain authorization check |
| `configure_routing` | `EventBus.SetRoutingTable()` — writes to `routing_rules` table, emits `opco.routing_updated` | CEO: full. VP: own domain. CTO: sub-team. CoS: propose only. |
| `schedule` | `Scheduler.Register()` — writes to schedules table, runtime fires on time | Agent can only schedule for itself |
| `mailbox_send` | Inserts into `mailbox_items` table with priority level | Any agent (but convention: only CEO/VPs for escalations) |
| `human_task_request` | Creates `human_tasks` row (status: `pending_review`), emits `human_task.requested` → Empire Coordinator. Returns `task_id` immediately. Results (completion, rejection, deferral) delivered as targeted events to requesting agent (see below). | Any agent. Empire Coordinator enforces weekly budget and value threshold. |
| `human_task_decide` | Empire Coordinator only. Writes approval/rejection/deferral to `human_tasks`, emits `human_task.approved`/`.rejected`/`.deferred`. Approved tasks are pushed to human via Telegram. | Empire Coordinator only. |

**Async result delivery for human tasks:**

`human_task_request` returns synchronously like all other tools. Async results arrive as events — the same pattern as the mailbox (request, async gap, event-based delivery).

When an agent calls `human_task_request`:

1. Tool returns immediately with `{"task_id": "...", "status": "pending_review"}`. The tool_use/tool_result pair is **complete and closed**. Agent continues its current task.
2. Empire Coordinator evaluates and emits `human_task.approved`, `.rejected`, or `.deferred`.
3. Results arrive as events routed to the requesting agent:

**Rejection/deferral (immediate — seconds):** Runtime delivers `human_task.rejected` or `human_task.deferred` to the requesting agent as a normal event. The agent wakes up, sees the rejection reason, and adapts its strategy (e.g., try WhatsApp first).

**Completion (async — hours to days):** Human completes the task via CLI/Telegram. Runtime updates `human_tasks` row and emits `human_task.completed`. Runtime uses `requesting_agent` column to deliver the event directly to the originating agent (targeted delivery, not broadcast).

```
Event: human_task.completed
Payload:
  task_id: "abc-123"
  outcome: "success"
  result_text: "Spoke with owner of Contaduría López. Interested in demo.
    Has 3 employees. Currently using Excel for SIFEN. Willing to pay
    up to $40/mo if IPS payroll is included."
  follow_up_needed: true
  original_request:
    category: "sales_call"
    description: "Call pet grooming prospect identified in outreach"
    requesting_agent: "marketing-pet-grooming-py"
```

The `original_request` context is copied from the `human_tasks` row so the agent can reconnect the result to what it asked for without needing conversation history lookup.

**Expiry (async — 1 week default):** Runtime emits `human_task.expired` to both Empire Coordinator (for requeue/kill decision) and requesting agent (so it can adapt strategy without waiting indefinitely).

**Routing:** `human_task.completed`, `human_task.rejected`, `human_task.deferred`, and `human_task.expired` are `human_task.*` prefixed → classified as Factory events → static subscriptions. However, these events are **agent-targeted**: the runtime reads `requesting_agent` from the `human_tasks` row and delivers only to that specific agent instance. Other agents with `human_task_request` in their tools do NOT receive another agent's task results. This is the same targeted-delivery pattern used for `agent_message`.

**Why not tool_result injection:** The LLM Messages API enforces strict 1:1 pairing between `tool_use` and `tool_result` blocks. The initial `human_task_request` call already consumed its `tool_result` (the `task_id` acknowledgment). Injecting a second `tool_result` with the same `tool_use_id` days later would cause an API validation error (400 Bad Request), permanently breaking the agent's session. Event-based delivery avoids this entirely — results arrive as normal events the agent processes like any other.

**Data tools** (schema-isolated database access):

| Tool | Implementation | Scoping |
|------|---------------|---------|
| `sql_execute` | `db.Exec(query)` on pre-scoped connection (`SET search_path = {schema}`) | Agent's assigned schema only. We build this rather than relying on raw `psql` because schema isolation must be enforced per-agent, not per-container. |

**Infrastructure tools** (Holding DevOps only — privileged):

| Tool | Implementation | Notes |
|------|---------------|-------|
| `nginx_reload` | `exec.Command("systemctl", "reload", "nginx")` | Validates config first: `nginx -t` |
| `systemd_control` | `exec.Command("systemctl", action, unit)` where action ∈ {start, stop, restart, enable, disable} | Unit names restricted to `empireai-*` pattern |
| `certbot_execute` | `exec.Command("certbot", "--nginx", "-d", domain, "--non-interactive", "--agree-tos")` | Production only. Staging environments skip SSL. |

Note: These could be native shell commands, but we wrap them as typed tools for two reasons: (1) restrict to specific operations (no arbitrary `systemctl` on non-empireai units), and (2) audit trail — tool calls are logged to events, raw shell commands are not.

**External service tools** (credentials injected by tool executor from §13.1):

| Tool | Implementation | Credential source |
|------|---------------|-------------------|
| `whatsapp_business_api` | Meta Cloud API v17+ HTTP calls. Supports: send_message, send_template, read_messages. | Per-vertical: `verticals.credentials->'whatsapp'` |
| `email_api` | SMTP send via `net/smtp` or SendGrid HTTP API. Supports: send_email. | Per-vertical: `verticals.credentials->'email'` |
| `instagram_api` | Instagram Graph API. Supports: post_media, get_insights. No DM automation (Meta restriction). | Per-vertical: `verticals.credentials->'instagram'` |
| `domain_purchase` | Cloudflare Registrar API (or Namecheap API). Creates purchase request → returns pending order → confirmed via webhook. | Global: `REGISTRAR_API_KEY` |
| `domain_availability_check` | Cloudflare/WHOIS API. Returns: available (bool), price, alternatives. | Global: `REGISTRAR_API_KEY` |
| `dns_configure` | Cloudflare DNS API. Creates/updates A/CNAME records. | Global: `CLOUDFLARE_API_TOKEN` |
| `instagram_handle_check` | HTTP GET to `instagram.com/{handle}` — check for 404. No API key needed. | None |
| `whatsapp_name_check` | WhatsApp Business display name lookup via Meta Business API. | Global: separate Meta app token |

**Why this split matters:**

Tier 1 tools improve without us doing anything. When Claude Code adds better web search locality, Readability extraction, or shell output handling, every EmpireAI agent benefits immediately. We don't maintain search provider integrations, HTML parsers, or file-editing logic.

Tier 2 tools are our competitive moat. No coding environment ships `agent_hire` or `whatsapp_business_api` with per-vertical credential injection. These are the tools that make EmpireAI an autonomous company rather than a collection of coding agents.

**Implementation cost:** ~15 custom tools to build (7 organizational + 1 data + 3 infrastructure + 8 external service) vs the previous ~25. The 10 we dropped (file_read, file_write, shell_execute, go_build, go_test, http_request, web_search, web_scrape, web_fetch) are all handled natively by the LLM environment.

---

## 14. Human Task System

### 14.1 Overview

Agents are digital-native. They can search the web, write code, send messages, and call APIs. But they cannot make phone calls, visit government offices, shake hands, verify physical locations, or negotiate face-to-face. In LATAM markets especially, many high-value actions require a human presence.

The Human Task System bridges this gap: agents request physical-world actions, Empire Coordinator evaluates and approves, humans execute and report back, and the requesting agent continues with the result.

### 14.2 Task Categories

| Category | Examples | Typical Requester |
|----------|----------|-------------------|
| `sales_call` | Cold call prospects, follow up on warm leads, close deals, run demos | Marketing Agent, CEO |
| `government_visit` | SET (tax authority) registration, SIFEN enrollment, municipal permits, notary | CEO, Backend (compliance integration) |
| `verification` | Confirm a business exists, check competitor storefront, test a payment flow end-to-end, validate market research | Discovery agents, Business Research Agent |
| `escalated_support` | Customer angry, needs human voice. Agent tried WhatsApp/email, customer wants to talk to a person. | Support Agent |
| `partnership` | Meet payment provider, negotiate bank integration, bulk deal with supplier, co-marketing agreement | CEO, Marketing Agent |
| `ground_truth` | Visit a neighborhood to assess business density, interview local business owners, attend industry event | Market Research Agent, Trend Research Agent |
| `banking` | Open merchant account, set up payment processing, wire transfers requiring in-person verification | CEO |

### 14.3 Task Lifecycle

```
Any agent calls human_task_request tool
    ↔
Runtime creates human_tasks row (status: pending_review)
Runtime emits human_task.requested → Empire Coordinator
    ↔
Empire Coordinator evaluates:
    1. Weekly budget check: tasks_approved_this_week < max_human_tasks_per_week?
    2. Digital exhaustion check: has the agent tried all digital channels first?
       (Check agent's recent conversation for WhatsApp/email attempts)
    3. Value assessment: expected_value vs cost of human time
    4. Cross-portfolio priority: compare against pending tasks from other verticals
    5. Duplication check: is there already a similar pending task?
    ↔
Decision: approve → rejected → deferred
    ↔
If approved:
    Runtime updates status → approved
    Runtime delivers to human via Telegram (with talking points, deadline)
    Runtime emits human_task.approved
    ↔
Human sees task on Telegram or via `empire tasks list`
Human claims task → status: assigned
    ↔
Human executes (phone call, visit, meeting)
    ↔
Human reports result via Telegram or CLI:
    `empire tasks complete <id> --result "..." --outcome success|partial|failed`
    ↔
Runtime updates human_tasks row
Runtime emits human_task.completed → requesting agent (targeted delivery)
    ↔
Agent receives human_task.completed as a normal event
Agent reads task_id + original_request context to reconnect to its prior request
Agent continues reasoning with human-provided information

If rejected:
    Runtime emits human_task.rejected → requesting agent (targeted delivery)
    Agent receives rejection as event, adapts: try digital approach, defer, or accept limitation

If deferred:
    Runtime emits human_task.deferred → requesting agent (targeted delivery)
    Task queued for next week's budget cycle
    Agent receives deferral notification, can plan around delay
```

### 14.4 Empire Coordinator Guardrail

The Empire Coordinator is the sole approver because it has cross-portfolio visibility. An OpCo CEO will always think its sales call is the most important task. The Empire Coordinator can compare:

- A $50/mo customer close for factura.com.py (high value, clear revenue)
- A ground truth verification for a discovery pipeline candidate (low urgency, speculative)
- A government visit for SIFEN registration (high value, blocking for launch)

**Evaluation prompt guidance for Empire Coordinator:**

```
When evaluating a human_task_request:

1. BUDGET: Check remaining weekly budget. If exhausted, reject unless 
   priority is critical.

2. DIGITAL FIRST: Review the requesting agent's recent actions. 
   Did they attempt WhatsApp, email, or other digital channels? 
   If not, reject with: "exhaust digital channels before requesting 
   human execution."

3. VALUE: Assess expected_value against the cost of human time.
   A sales call that could close a paying customer > a verification 
   that confirms already-high-confidence data.

4. PRIORITY: Compare against all pending human_task_requests across 
   the portfolio. Approve the highest-value tasks first. Defer or 
   reject the rest.

5. TIMING: Does this task have a deadline that matters? A SIFEN 
   registration deadline next week > a partnership meeting "sometime."
```

### 14.5 Budget & Throttling

```yaml
human_tasks:
  max_tasks_per_week: 3           # Phase 1: founder is sole executor
  budget_reset: "monday"           # Weekly budget resets Monday 00:00 UTC
  min_expected_value: "measurable"  # Empire Coordinator must justify approval
  categories_enabled:               # Which categories are active
    - sales_call
    - government_visit
    - escalated_support
    - partnership
    - verification
    - ground_truth
    - banking
  auto_expire_hours: 168           # Tasks expire after 1 week if unclaimed
```

**Phase scaling:**

| Phase | Executor | Budget | Notes |
|-------|----------|--------|-------|
| Phase 1 | Founder only | 3/week | All tasks via Telegram. Founder does everything. |
| Phase 2 | Founder + 1-2 employees | 10/week | Tasks route by category: sales → sales person, compliance → ops person. |
| Phase 3 | Small office (3-5 people) | 25/week | Full task routing. Empire Coordinator assigns based on category + workload. Employees get simplified dashboard or WhatsApp bot interface. |
| Phase 4 | Regional teams | 50+/week | Multiple cities. Per-geography task routing. Human managers supervise executors. |

Budget increases require human approval via `empire config set human-tasks.max-per-week <n>`.

**Task expiry behavior:** When a task reaches `auto_expire_hours` (default 168h = 1 week) without completion:
1. Runtime emits `human_task.expired` → Empire Coordinator (broadcast) + requesting agent (targeted delivery via `requesting_agent`)
2. Empire Coordinator evaluates: is the task still relevant?
   - **Still relevant + expected value unchanged:** auto-requeue for next budget cycle. Does NOT count against current week's budget (it already counted when first approved). Status: `pending_review` again with `requeue_count` incremented.
   - **Stale (underlying opportunity changed, deadline passed):** mark as expired-permanent. Requesting agent receives "task expired, not requeued — reassess your approach."
   - **Requeued 2+ times already:** escalate to mailbox. Something is wrong — either the human is overloaded or the task isn't actually doable. Human decides: kill it, increase budget, or hire help.
3. Requesting agent receives either "requeued for next cycle, expect completion by [date]" or "expired permanently, adapt your strategy."

### 14.6 Feedback Loop

The result from a human task is not just a status update — it's intelligence that feeds back into the agent's reasoning:

```
Marketing Agent requested: sales_call to Contaduría López
Human completed: "Owner interested but wants to see demo first. 
  Has 3 employees. Currently using Excel. SIFEN deadline is 
  stressing him. Willing to pay up to $40/mo if it handles IPS too."

→ This result enters Marketing Agent's conversation as a tool response.
→ Agent now knows: pricing flexibility ($40/mo), feature request (IPS), 
  urgency driver (SIFEN deadline), team size (4 users).
→ Agent adjusts: schedules follow-up, updates prospect status, 
  tells PM about IPS feature demand signal.
```

When multiple human tasks produce consistent signals (e.g., 3/5 prospects mention IPS payroll integration), agents should notice the pattern and escalate to CEO/PM. This is how ground-truth from human interactions shapes product direction.

### 14.7 API Surface

The dashboard and Telegram bot consume the same API as the CLI. Core endpoints for human task management:

```
GET    /api/tasks                     → empire tasks list
GET    /api/tasks/:id                 → empire tasks view
POST   /api/tasks/:id/claim           → empire tasks claim
POST   /api/tasks/:id/complete        → empire tasks complete
POST   /api/tasks/:id/reject          → empire tasks reject (human pushback)
GET    /api/tasks/stats               → empire tasks stats

GET    /api/mailbox                   → empire mailbox list
POST   /api/mailbox/:id/decide        → empire mailbox decide
GET    /api/verticals                 → empire status
GET    /api/verticals/:id/agents      → empire agents
POST   /api/chat/:agent               → empire chat
GET    /api/events?stream=true         → SSE live event stream
POST   /api/directive                  → empire directive
GET    /api/budget                     → empire budget

# Prompt management (hot-reload iteration)
GET    /api/agents/:id/prompt          → current effective prompt (override or template)
PUT    /api/agents/:id/prompt          → set prompt override + trigger session rotation
DELETE /api/agents/:id/prompt          → revert to template prompt + trigger session rotation
GET    /api/agents/:id/prompt/diff     → diff between override and template
GET    /api/templates/:role/prompt     → template prompt for a role
PUT    /api/templates/:role/prompt     → update template draft (does NOT affect running agents)
POST   /api/templates/publish          → publish template (validates via Spec Auditor)
```

Authentication: API key in header (`X-Empire-Key`). Single key for the founder in Phase 1. Per-employee keys in Phase 2+ with role-based access (employees see only their assigned tasks, founder sees everything).

---

## 15. Implementation Phases

### 15.0 Event Wiring Verifier (CI Gate)

The wiring verifier is the spec integrity contract. It automatically validates that every event path in the system is complete — every event has an emitter, a consumer, a schema, and correct subscriptions. It runs against the spec markdown before deployment and blocks releases when critical issues exist.

**Why this exists:** Live testing repeatedly revealed that agent intelligence is not the bottleneck — wiring is. Every agent that receives the correct event with the correct payload does its job well. Every failure traced to: event never delivered (wrong subscription), payload missing data (no schema enforcement), or agent missing tools (no emit tool schema). These are all mechanically verifiable. The verifier catches them before they reach production.

**What it validates (6 checks):**

| Check | Severity | What it catches |
|-------|----------|----------------|
| NO_SCHEMA | HIGH | Event in `event-catalog.yaml` but no EventSchemaRegistry entry → `emit_*` tool cannot be generated at runtime |
| DEAD_SUB | HIGH | Agent subscribes to event nobody emits → agent never wakes up |
| MISSING_SUB | HIGH | Catalog says agent consumes event but agent YAML has no matching subscription → event delivered but lost |
| EMIT_NOT_IN_YAML | MEDIUM | agent-tools.yaml says agent emits event but emit tool not listed in agent YAML → tool may not be injected |
| ORPHAN_EMISSION | MEDIUM | Agent emits event but no agent subscribes and no runtime interceptor handles it → event goes nowhere |
| SCHEMA_NO_CATALOG | LOW | Schema exists in registry but no catalog entry → documentation gap |

**Four data sources (all within the spec):**

1. **Agent configs** (`agent-tools.yaml`) — subscriptions, tools, emit_events per agent. Prompts in `contracts/prompts/`
2. **Event catalog** (`contracts/event-catalog.yaml`) — canonical emitter→consumer→payload declarations per event (summary in §5.4)
3. **Event producers** — `agent-tools.yaml` emit_events (agent-emitted) + `event-catalog.yaml` emitter_type (runtime/human/opco_agent classification)
4. **EventSchemaRegistry** (`internal/runtime/event_emit_tools.go`) — Go code declaring JSON schemas per event type; these schemas generate `emit_*` tool definitions at agent session start

**The verifier cross-references these four sources and flags any inconsistency.**

**Go test implementation (v2.1.0):** The verifier logic is now implemented as `internal/runtime/contract_compliance_test.go`, which reads contract YAML files at test time rather than parsing spec prose. See §17.3 for the 7 automated compliance gates. Run with `go test ./internal/runtime/ -run TestContractCompliance`.

**Verification logic per event type:**

For every event `E` in the catalog:
1. Does `E` have a schema in EventSchemaRegistry? (NO_SCHEMA if missing)
2. Does the declared consumer agent subscribe to `E` in their YAML? (MISSING_SUB if not)
3. Does the declared emitter appear in `agent-tools.yaml` emit_events + `event-catalog.yaml` emitter? (consistency check)

For every agent subscription `S`:
4. Does anyone emit `S`? Check `agent-tools.yaml` emit_events + `event-catalog.yaml` emitters. (DEAD_SUB if nobody)

For every emission in `agent-tools.yaml` emit_events:
5. Does the emitting agent's YAML list the corresponding `emit_*` tool? (EMIT_NOT_IN_YAML if missing)
6. Does any agent subscribe to this event? (ORPHAN_EMISSION if no subscriber and no interceptor)

**When to run:**
- Before every spec version bump (CI gate — spec doesn't ship if HIGH issues > 0)
- After adding or modifying any agent YAML config
- After adding events to catalog tables or schemas to registry
- After modifying `agent-tools.yaml` emit_events + `event-catalog.yaml` emitter

**Phased schema coverage:**

Not all events need schemas immediately. The verifier classifies missing schemas by implementation phase:

- **Phase 1 schemas (factory pipeline):** Events in the discovery→scoring→validation pipeline that agents emit via `emit_*` tools. These MUST have schemas before the pipeline runs. ~56 events.
- **Phase 2 schemas (runtime/human events):** Events the Go runtime or human operator emits. Need schemas for payload validation but don't generate agent tools. ~21 events.
- **Phase 3 schemas (OpCo events):** Events within operating companies. Need schemas when OpCo team is implemented. ~19 events.
- **Not schemas:** Wildcard patterns like `brand.*` are subscription filters, not event types. The verifier correctly excludes these.

**Exit criteria per phase:**

| Phase | HIGH issues | MEDIUM issues | Action |
|-------|------------|---------------|--------|
| Phase 1 start | 0 factory pipeline | — | All factory agent schemas complete, all subscriptions wired |
| Phase 2 start | 0 total | ≤ 10 | All runtime/human schemas complete |
| Phase 3 start | 0 total | 0 | All OpCo schemas complete |

**Terminal event policy (v2.0.30):**

Some events are intentionally terminal — they have no agent subscriber because their purpose is audit logging, mailbox delivery, or runtime-internal consumption. The verifier must NOT flag these as ORPHAN_EMISSION. Terminal events are exempt from the orphan check and classified into three categories:

| Category | Consumer | Examples | Verifier behavior |
|----------|----------|----------|-------------------|
| Audit-only | Written to `events` table, no subscriber | `vertical.rejected`, `devops.deploy_complete`, `devops.deploy_failed`, `devops.rollback_complete`, `devops.rollback_failed`, `devops.ssl_provisioned`, `scan.started`, `cto.pattern_detected`, `template.migration_planned`, `template.migration_completed`, `template.migration_failed`, `budget.warning`, `budget.throttle`, `budget.emergency`, `budget.resumed` | SKIP orphan check |
| Mailbox-targeted | Delivered to `mailbox` table for human decision | `vertical.health_warning`, `vertical.ready_for_review`, `opco.deploy_review`, `opco.founder_input`, `opco.product_spec_review`, `opco.spend_request`, `devops.infra_change_needed` | SKIP orphan check |
| Runtime-consumed | Intercepted by PipelineCoordinator, no agent fan-out | All events with `intercepted: true, passthrough: false` in `event-catalog.yaml` | SKIP orphan check (already handled by interceptor existence check) |

The canonical list of terminal events lives in `contracts/event-catalog.yaml` — any event with `consumer: audit` or `consumer: mailbox` is terminal. The verifier should read this classification directly from the contract file rather than maintaining a separate whitelist.

**Verifier scope — OpCo template agents (v2.0.30):**

The current verifier evaluates holding and factory agent wiring (singleton agents with static subscriptions). OpCo template agents have two distinct wiring patterns that require separate verification:

1. **Static subscriptions** (OpCo CEO, CoS, VP-Product, VP-Growth): These agents subscribe to events via the same mechanism as factory agents. The verifier SHOULD check these — they are declared in `agent-tools.yaml` and follow the same emit/subscribe contract.

2. **Routing-table events** (all OpCo internal events): Events like `bug_reported`, `support_digest`, `build_complete` flow through the per-vertical `routing_rules` table, not static subscriptions. The verifier CANNOT check these at spec-time because routes are installed dynamically (bootstrap → seeded → discovered). Instead, verify:
   - Every OpCo-internal event in `event-catalog.yaml` with `routing: routing_table` has a declared emitter with the event in its `emit_events`
   - Every such event has a plausible consumer declared (the verifier checks emitter-side only; consumer-side is runtime-verified via bootstrap route installation)

3. **Cross-tier events** (e.g., `cto.architecture_directive` → OpCo CEO, `cto.tech_spec_feedback` → OpCo CTO via agent_message): These use agent_message delivery, not subscriptions. The verifier should recognize `agent_message` delivery as valid consumption and not flag these as orphans. Events in the catalog with consumer notes containing "agent_message" are exempt from subscription checks.

The verifier's ORPHAN_EMISSION check should therefore operate in three tiers:
- **Tier 1 (factory/holding):** Strict — every emission must have a subscriber or interceptor
- **Tier 2 (OpCo static subs):** Strict — same as Tier 1 for OpCo agents with declared subscriptions
- **Tier 3 (OpCo routing-table + agent_message):** Emitter-only — verify the agent can emit it, skip consumer subscription check

**SDK extraction note:** When this system is extracted as a multi-agent SDK framework, the verifier ships as a built-in CLI command: `empire verify spec.md`. Any team building agent pipelines on the framework gets the same wiring validation. The 6 checks are universal to event-driven multi-agent systems — not EmpireAI-specific.

#### 15.0.1 Verifier Script

The verifier is a Python script that parses the spec markdown. It extracts data from the four sources above using regex patterns tuned to the spec's formatting conventions.

```python
#!/usr/bin/env python3
"""
EmpireAI Event Wiring Verifier
Run: python3 verify_wiring.py path/to/spec.md
Exit code: 0 if no HIGH issues, 1 if HIGH issues exist.
"""

import re, sys
from collections import defaultdict

def read_spec(path):
    with open(path, 'r') as f: return f.read()

def extract_agents(spec):
    """Extract agent configs from ```yaml blocks."""
    lines = spec.split('\n')
    agents = {}
    in_yaml, block_start = False, 0
    for i, line in enumerate(lines):
        if line.strip() == '```yaml':
            in_yaml, block_start = True, i
        elif line.strip() == '```' and in_yaml:
            in_yaml = False
            block = '\n'.join(lines[block_start:i])
            id_m = re.search(r'^id:\s*"?([\w{}-]+)"?', block, re.MULTILINE)
            if not id_m: continue
            # Subscriptions: indented lines under 'subscriptions:' starting with '- '
            subs = []
            sub_m = re.search(r'subscriptions:\s*\n((?:(?:\s+.*)\n)*)', block)
            if sub_m:
                subs = re.findall(r'\s+-\s+([\w.*]+)', sub_m.group(1))
            # Emit tools from comments
            emit_tools = sorted(set(re.findall(r'emit_[\w]+', block)))
            agents[id_m.group(1)] = {'subscriptions': subs, 'emit_tools': emit_tools}
    return agents

def extract_catalog(spec):
    """Extract events from §5.4/5.5 tables (dotted event names only)."""
    catalog = {}
    for m in re.finditer(r'\|\s*`([\w]+\.[\w.*]+)`\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|', spec):
        catalog[m.group(1)] = {'emitter': m.group(2).strip(), 'consumer': m.group(3).strip()}
    return catalog

def extract_producer_registry(spec):
    """Extract agent-tools.yaml emit_events: who emits what."""
    agent_emissions = {}
    for m in re.finditer(r'\|\s*\*\*([\w\s]+)\*\*\s*\|\s*((?:`[\w.]+`(?:,\s*)?)+)\s*\|', spec):
        events = re.findall(r'`([\w.]+)`', m.group(2))
        if events: agent_emissions[m.group(1).strip()] = events
    runtime = set(re.findall(r'\|\s*`([\w.]+)`', 
        (re.search(r'\*\*Runtime-emitted\*\*.*?\n((?:\|.*\n)*)', spec, re.DOTALL) or type('',(),{'group':lambda s,n:''})()).group(1)))
    human = set(re.findall(r'\|\s*`([\w.]+)`',
        (re.search(r'\*\*Human-emitted\*\*.*?\n((?:\|.*\n)*)', spec, re.DOTALL) or type('',(),{'group':lambda s,n:''})()).group(1)))
    return agent_emissions, runtime, human

def extract_schemas(spec):
    """Extract event schemas from event-catalog.yaml payload fields.
    Note: EventSchemaRegistry was removed from spec prose in v2.1.0.
    Schema definitions live in internal/runtime/event_emit_tools.go.
    Contract compliance is enforced by TestContractCompliance/schema_payload."""
    # Fall back to catalog payload fields as proxy for schema existence
    catalog_events = set(re.findall(r'^([a-z_]+\.[a-z_.]+):', spec, re.MULTILINE))
    return {e: True for e in catalog_events}

def normalize(name):
    return name.lower().replace(' ', '-').replace('_', '-')

def find_agent(agents, display_name):
    n = normalize(display_name)
    for aid in agents:
        an = aid.lower().replace('_','-').replace('{','').replace('}','')
        if n in an or an in n: return aid
    return None

def event_matches(event, sub):
    if sub == event: return True
    if '*' in sub:
        prefix = sub.replace('.*', '.').replace('*', '')
        return event.startswith(prefix)
    return False

def verify(spec_path):
    spec = read_spec(spec_path)
    agents = extract_agents(spec)
    catalog = extract_catalog(spec)
    emissions, runtime_ev, human_ev = extract_producer_registry(spec)
    schemas = extract_schemas(spec)

    # Build emission map
    emission_map = defaultdict(list)
    for agent, evts in emissions.items():
        for e in evts: emission_map[e].append(agent)
    for e in runtime_ev: emission_map[e].append('Runtime')
    for e in human_ev: emission_map[e].append('Human')
    all_events = set(catalog) | set(schemas) | set(emission_map) | runtime_ev | human_ev

    issues = []

    # CHECK 1: NO_SCHEMA — catalog event missing from registry
    for ev in catalog:
        if '*' in ev: continue  # wildcard patterns aren't event types
        if ev not in schemas:
            issues.append(('HIGH', 'NO_SCHEMA', ev, catalog[ev]['consumer']))

    # CHECK 2: DEAD_SUB — agent subscribes, nobody emits
    for aid, a in agents.items():
        for sub in a['subscriptions']:
            if '*' in sub:
                if not any(event_matches(e, sub) for e in all_events):
                    issues.append(('HIGH', 'DEAD_SUB', f"{aid} → {sub}", "wildcard matches nothing"))
            elif sub not in all_events:
                issues.append(('HIGH', 'DEAD_SUB', f"{aid} → {sub}", "nobody emits"))

    # CHECK 3: MISSING_SUB — catalog consumer not subscribed
    for ev, info in catalog.items():
        if '*' in ev: continue
        consumer = info['consumer']
        if any(x in consumer.lower() for x in ['audit','mailbox','runtime','intercept','eventbus','—','agentmanager']): continue
        for cn in re.split(r'[→,/+]', consumer):
            cn = cn.strip().strip('*').strip()
            if not cn: continue
            aid = find_agent(agents, cn)
            if not aid: continue
            if not any(event_matches(ev, s) for s in agents[aid]['subscriptions']):
                issues.append(('HIGH', 'MISSING_SUB', f"{ev} → {aid}", f"catalog says '{cn}' consumes"))

    # CHECK 4: EMIT_NOT_IN_YAML
    for agent_name, evts in emissions.items():
        aid = find_agent(agents, agent_name)
        if not aid: continue
        yaml_emits = set(agents[aid]['emit_tools'])
        if not yaml_emits: continue
        for ev in evts:
            tool = "emit_" + ev.replace(".", "_")
            if tool not in yaml_emits:
                issues.append(('MEDIUM', 'EMIT_NOT_IN_YAML', f"{aid} → {tool}", f"agent-tools.yaml says emits {ev}"))

    # CHECK 5: ORPHAN_EMISSION
    for ev, emitters in emission_map.items():
        subscribed = any(event_matches(ev, s) for aid, a in agents.items() for s in a['subscriptions'])
        if not subscribed:
            if ev in catalog and any(x in catalog[ev]['consumer'].lower() for x in ['runtime','intercept','audit','mailbox','—','agentmanager','eventbus']): continue
            issues.append(('MEDIUM', 'ORPHAN_EMISSION', ev, f"emitted by {', '.join(emitters)}"))

    # Report
    high = [i for i in issues if i[0]=='HIGH']
    med = [i for i in issues if i[0]=='MEDIUM']
    low = [i for i in issues if i[0]=='LOW']
    print(f"Event Wiring Verifier: {len(high)} HIGH | {len(med)} MEDIUM | {len(low)} LOW")
    for sev in ['HIGH','MEDIUM','LOW']:
        group = [i for i in issues if i[0]==sev]
        if not group: continue
        print(f"\n{'🔴' if sev=='HIGH' else '🟡' if sev=='MEDIUM' else '🔵'} {sev} ({len(group)}):")
        by_check = defaultdict(list)
        for _,check,target,detail in group: by_check[check].append((target,detail))
        for check, items in sorted(by_check.items()):
            print(f"  [{check}] {len(items)} issues")
            for t,d in items[:15]: print(f"    ❌ {t}  ({d})")
            if len(items)>15: print(f"    ... +{len(items)-15} more")

    print(f"\n{'🔴 FAIL' if high else '🟢 PASS'}")
    return len(high)

if __name__ == '__main__':
    sys.exit(1 if verify(sys.argv[1] if len(sys.argv)>1 else 'spec.md') > 0 else 0)
```

#### 15.0.2 Go Contract Compliance Tests (v2.1.0)

The contract compliance test file reads YAML/SQL contracts at test time and verifies the Go code matches. Unlike the Python verifier (§15.0.1) which validates spec-internal consistency, these tests validate **code ↔ contract alignment** and run as part of `go test ./internal/runtime/`.

**File:** `internal/runtime/contract_compliance_test.go`

**7 gates covering 90% of observed drift:**

| Gate | What it validates | Drift it catches |
|------|------------------|-----------------|
| 1. Agent config fields | model_tier, max_turns, conversation_mode, tools per agent match agent-tools.yaml + agent-config-map.yaml | Config drift (was #1 failure mode v2.0.30-33, clean since manual fix) |
| 2. Subscriptions | Holding/factory: config subs == contract subs. OpCo workers: bootstrap routes == contract. OpCo leadership: defaultOpCoRoster() == contract | Agent never wakes up (wrong subscription) |
| 3. CommGraph emit_events | agentProducerEvents[agent] == contract emit_events. scoring-node produces == system-nodes.yaml | Agent can't emit (missing tool), or emits event nobody handles |
| 4. EventSchemaRegistry payloads | Every catalog payload field exists as schema property. If additionalProperties==false: catalog fields == schema properties exactly | Payload rejected at runtime (the C4 pattern — new fields in contract but not in Go schema) |
| 5. DDL table count + columns | CREATE TABLE count == 37. For 7 runtime tables: parsed column names match Go struct field tags | Schema drift between DDL and code |
| 6. Version constants | runtimeSpecVersion == spec version from contracts. TemplateVersion == spec version | Stale version after spec bump |

**How it works:** Uses `os.ReadFile` to load contract YAMLs from `contracts/` directory, unmarshals into simple structs, compares against runtime registries already accessible in test scope. No new dependencies. No external tooling.

**Run:** `go test ./internal/runtime/ -run TestContractCompliance`

**Failure output format:**
```
--- FAIL: TestContractCompliance/emit_events/scoring-node
    contract_compliance_test.go:142: scoring-node produces mismatch:
      in code but not contract: [vertical.discovered]
      in contract but not code: [pipeline.dead_letter]
```

**Relationship to verification-gates.yaml:** Gates 1-6 in contract_compliance_test.go supersede the corresponding manual/grep-based gates in verification-gates.yaml for code-level validation. The YAML gates remain authoritative for spec-internal consistency checks. Gates marked `automated_by: contract_compliance_test.go` in verification-gates.yaml have Go test coverage and no longer need manual verification.

**What this does NOT cover:** Integration gates (event roundtrip, crash recovery) require running infrastructure. Grep-based cleanup gates (no SC ghosts, no globs) are one-time fixes, not ongoing regression risks. The 6 gates here target the recurring drift patterns observed across 20+ audit cycles.

### Pre-Implementation Checklist (Gate: must pass before Phase 1 coding)

Run the Event Wiring Verifier (§15.0) against the spec. All HIGH issues must be resolved. Then verify the remaining manual checks below.

**Agent completeness:**
- [ ] Every agent in the org diagram (§3) has a corresponding entry in the config roster (§13 `agents:`)
- [ ] Every agent in the config roster has a corresponding YAML file in the directory structure (§16)
- [ ] Every agent in agent-tools.yaml has a corresponding prompt file in contracts/prompts/
- [ ] Agent count in `opco.ceo_ready` event matches actual count of operating templates

**Event contract completeness (automated by §15.0 verifier):**
- [ ] `python3 verify_wiring.py spec.md` exits with code 0 (no HIGH issues)
- [ ] All Phase 1 schemas (factory pipeline events) present in EventSchemaRegistry
- [ ] All Phase 2 schemas (runtime/human events) present in EventSchemaRegistry

**Routing consistency:**
- [ ] Bootstrap route count matches `configs/agents/templates/routes.yaml`
- [ ] Seeded route count matches `configs/agents/templates/routes.yaml`
- [ ] Total route count references across §3.3, §7.1 are consistent
- [ ] `configure_routing` authorization model (§4.3) matches `opco.routing_updated` emitter in `event-catalog.yaml`

**Deploy flow consistency:**
- [ ] All deploy flow descriptions reference staging → QA → production (not direct deploy)
- [ ] `devops.deploy_requested` carries `environment` field everywhere
- [ ] Hotfix path (`skip_staging`) is the only exception and is explicitly logged

**Data model:**
- [ ] Every table referenced in agent prompts or tool descriptions exists in `contracts/ddl-canonical.sql`
- [ ] DDL execution order resolves all FK dependencies (or uses deferred ALTER TABLE)
- [ ] Every field referenced in event payloads exists in the corresponding table

**Cross-references:**
- [ ] Team composition lists (§3.3, §4.3, §7.1, CEO prompt, cost tables) are identical
- [ ] Model tier assignments in §9.2 cover every agent in the config roster
- [ ] Cost estimates in §9.4 cover every agent in the config roster

**Open questions:**
- [ ] No unresolved open question blocks Phase 1-4 implementation

**Infrastructure contracts (v1.9):**
- [ ] Docker compose defines: postgres, orchestrator, workspace base image (§4.1.1)
- [ ] Orchestrator spawns per-vertical containers with scoped volume mounts
- [ ] Tier 2 tools callback to orchestrator via HTTP (agent containers have no direct DB access)
- [ ] Every Tier 2 tool in agent prompts appears in §13.2 with physical implementation
- [ ] Every external service credential has a storage location in §13.1 (global or per-vertical)
- [ ] `deployments` table has `environment` and `version` fields (required by §7.8 deploy flow)
- [ ] `verticals` table has `credentials` JSONB column (required by §13.1)
- [ ] Inbound Gateway webhook verification references credential source (§4.7 → §13.1)
- [ ] No `ssh_execute` references remain (single-box uses `shell_execute` with tiered privileges)
- [ ] Database bootstrap sequence is documented in Phase 1 (migration runner, schema creation)
- [ ] DeployManifest structure is defined in §7.8 and referenced by both DevOps prompts

### Phase 1: Runtime Foundation (Week 1-2)

**Container bootstrap (before anything):**
1. Build workspace base image (`empireai-workspace:latest`) — see §4.1.1
2. `docker compose up postgres` — Postgres running in its own container
3. `docker compose up orchestrator` — Go orchestrator process starts, connects to Postgres
4. Orchestrator creates `empireai-factory` container (for Factory CTO scaffold editing)
5. Orchestrator creates `empireai-infra` container (for Holding DevOps, privileged mounts)
6. Per-vertical containers created dynamically at spinup time

**Database bootstrap (runs inside orchestrator on first start):**
1. Create `empireai` database: `createdb empireai` (manual, one-time)
2. Enable pgcrypto: `CREATE EXTENSION IF NOT EXISTS pgcrypto;` (for credentials encryption + UUID generation)
3. Run `contracts/ddl-canonical.sql` containing all 37 tables, ordered by FK dependencies
4. Runtime embeds migration runner — on startup, checks `schema_version` table and applies pending migrations
5. Vertical schema creation: `SpawnOpCo` executes `CREATE SCHEMA IF NOT EXISTS {vertical_slug}` and `CREATE SCHEMA IF NOT EXISTS {vertical_slug}_staging`, then runs vertical's `schema.sql` within both schemas. Port pair allocated deterministically (next available production port, staging = production + 1000). All infrastructure provisioning happens before any agents are spawned — agents never connect to nonexistent schemas.

```sql
-- schema_version table (tracks which migrations have been applied)
CREATE TABLE IF NOT EXISTS schema_version (
    version     INT PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT now()
);
```

**Runtime components:**
- EventBus with Go channels + Postgres write-through
- AgentManager with goroutine lifecycle (including dynamic spawn/teardown)
- **LLM runtime abstraction** (`LLMRuntime` interface with API + CLI adapters)
- **Session registry** with single-writer locking and rotation support
- Claude conversation manager (task-scoped + session-scoped modes)
- Scheduler (cron + one-shot timer wake-ups for agents)
- Inbound Gateway (HTTP webhook receiver → internal events)
- Event persistence and recovery
- Basic CLI for mailbox and status
- Async mailbox with priority levels (normal, critical)
- Critical notification channel (Telegram/email)

### Phase 2: Discovery Pipeline (Week 3-4)
- Empire Coordinator
- Discovery Coordinator
- Local services scanners: Google Maps Scanner, Instagram Scanner, Review Scanner (mode: `local_services`)
- Market Research Agent (mode: `saas_gap`) — systematic SaaS taxonomy walkthrough
- Trend Research Agent (mode: `saas_trend`) — macro trend monitoring
- First geography scan end-to-end (start with `local_services` mode, add `saas_gap` once pipeline proven)

**Scanner implementation note:** Scanners use native LLM web search/fetch (Tier 1 tools) to query real providers. During initial development, synthetic adapters that produce correctly-shaped events are acceptable for testing the pipeline end-to-end. Replace synthetic outputs with real provider-backed searches before Phase 4 validation work begins. The key contract: scanner output event shape must match the spec regardless of whether the data is synthetic or real.

### Phase 3: Scoring Pipeline (Week 5-6)
- Scoring accumulator + `computeComposite()` in runtime (§4.2.2.8)
- Analysis Agent (scores individual dimensions via web research)
- Single universal rubric (v2.0.39): all modes scored with same 8+2 dimensions
- Hard gates (automation_micro), viability floor gate, composite thresholds
- Full discovery → scoring flow

### Phase 4: Factory Validation (Week 7-8)
- Factory CTO Agent
- Spec Auditor (validation gate for templates and vertical specs)
- Validation Coordinator
- Business Research Agent (sub-coordinator)
- Lightweight Spec Agent + Spec Reviewer
- CTO spec feasibility review
- Pre-Brand Agent (parallel with spec)
- Full factory pipeline end-to-end: scan → score → research → spec → brand → mailbox

### Phase 5: Operating Mode — CEO, VPs & Team (Week 9-11)
- Three-tier org: CEO → Chief of Staff + VPs → Workers
- Three-tier spec flow: lightweight (factory) → product spec (PM) → technical spec (Tech Writer)
- CTO as engineering manager with sub-team (Tech Writer, Backend, Frontend, QA, DevOps)
- Default spinup: 13 agents per OpCo + Holding DevOps (shared)
- Bootstrap routing: 20 critical-path route entries (can't remove) + 7 seeded routes (removable)
- Discovery mechanisms: direct messaging, manager-installed routes, report-based retrospective
- routing_rules table with source tracking (bootstrap vs seeded vs discovered)
- Manager tools: agent_hire, agent_fire, agent_reconfigure, configure_routing
- VP budget envelope allocation and tracking
- Milestone reports with communication_observations section
- Spend request chain: agent → VP → CEO → Mailbox (async, non-blocking)
- DevOps chain: OpCo DevOps → Holding DevOps
- Founder mode: mandate shaping (directives in approval), spec review gate, deploy review gate, founder input channel
- Direct communication: `empire chat` and `empire directive` CLI commands, BoardDirective event type, session management
- Action-oriented digest: split into action_required + informational

### Phase 6: Operating Mode — Intelligence & Learning (Week 12-13)
- Metrics collection and vertical_metrics table
- Portfolio digest generation
- Empire Coordinator portfolio management logic
- Kill criteria monitoring
- Budget tracking and throttling
- Factory CTO cross-vertical pattern detection
- Operations Analyst: cross-vertical routing analysis, bootstrap upgrade proposals
- Bootstrap versioning: routing_rules source tracking, template version management
- Advisory pipeline: analyst → Factory CTO review → template update → next SpawnOpCo

### Phase 7: Hardening (Week 14-15)
- Crash recovery testing (factory + operating)
- Cost monitoring and budget enforcement
- Agent performance tuning (prompt iteration)
- Session-scoped conversation management (summarization)
- **CLI runtime soak testing:**
  - Long-run session resume continuity (50+ turns without degradation)
  - Lock contention behavior under concurrent agent load
  - Session rotation and recovery validation
  - Parse failure → repair turn → rotation escalation path
- Multi-vertical stress testing
- Operational tooling

---

## 16. Directory Structure

```
empireai/
├── cmd/
│   ┘── empire/
│       ┘── main.go
├── internal/
│   ├── runtime/
│   │   ├── eventbus.go
│   │   ├── manager.go           # AgentManager (spawn, teardown, restart)
│   │   ├── conversation.go      # Task-scoped + session-scoped
│   │   ├── llm_runtime.go       # LLMRuntime interface + adapter selection
│   │   ├── llm_api.go           # AnthropicAPIRuntime adapter
│   │   ├── llm_cli.go           # ClaudeCLIRuntime adapter (claude -p)
│   │   ├── session_registry.go  # Session lifecycle, locking, rotation
│   │   ├── tools.go
│   │   ├── scheduler.go         # Timer-based agent wake-ups (dynamic heartbeats, milestone fallbacks)
│   │   ├── inbound.go           # External webhook → internal event gateway
│   │   ├── recovery.go
│   │   ┘── budget.go            # Token + spend tracking
│   ├── pipeline/                    # PipelineCoordinator (§4.2.2)
│   │   ├── coordinator.go           # Intercept(), state machine routing
│   │   ├── discovery.go             # handleDiscoveryReport, handleDedupResolved
│   │   ├── scoring.go               # handleVerticalDiscovered, computeComposite
│   │   ├── validation.go            # handleResearchCompleted, handleSpecApproved, checkGates
│   │   ├── campaign.go              # handleScanRequested, handleScanCompleted, cycling
│   │   ┘── recovery.go              # RecoverFromCrash, replay unreceipted events
│   ├── events/
│   │   ├── types.go
│   │   ├── factory_payloads.go
│   │   ┘── operating_payloads.go
│   ├── models/
│   │   ├── vertical.go
│   │   ├── geography.go
│   │   ├── deployment.go
│   │   ├── brand.go
│   │   ├── metrics.go
│   │   ├── spend.go
│   │   ├── mailbox.go
│   │   ┘── founder.go          # Founder directives, review gates, input requests
│   ├── store/
│   │   ├── postgres.go
│   │   ├── events.go
│   │   ├── verticals.go
│   │   ├── agents.go
│   │   ├── conversations.go
│   │   ├── deployments.go
│   │   ├── metrics.go
│   │   ├── spend.go
│   │   ├── patterns.go
│   │   ┘── mailbox.go
│   ├── claude/
│   │   ├── client.go
│   │   ┘── models.go
│   ├── tools/
│   │   ├── registry.go
│   │   ├── gmaps.go
│   │   ├── scraper.go
│   │   ├── instagram.go
│   │   ├── whatsapp.go
│   │   ├── email.go
│   │   ├── filesystem.go
│   │   ├── shell.go
│   │   ├── golang.go
│   │   ├── ssh.go
│   │   ├── nginx.go
│   │   ├── postgres_admin.go
│   │   ├── registrar.go         # Domain purchase API
│   │   ┘── dns.go               # DNS management
│   ├── mailbox/
│   │   ┘── cli.go
│   ┘── digest/
│       ┘── generator.go         # Portfolio digest compilation (milestone-driven)
├── configs/
│   ├── empire.yaml
│   ┘── agents/
│       ├── empire-coordinator.yaml
│       ├── factory-cto.yaml
│       ├── holding-devops.yaml
│       ├── operations-analyst.yaml
│       ├── discovery-coordinator.yaml
│       ├── analysis-agent.yaml
│       ├── validation-coordinator.yaml
│       ├── spec-auditor.yaml
│       ├── business-research.yaml
│       ├── lightweight-spec.yaml
│       ├── spec-reviewer.yaml
│       ├── pre-brand-agent.yaml
│       ┘── templates/
│           ├── opco-ceo.yaml
│           ├── chief-of-staff.yaml
│           ├── vp-product.yaml
│           ├── vp-growth.yaml
│           ├── cto-agent.yaml
│           ├── tech-writer.yaml
│           ├── backend-agent.yaml
│           ├── frontend-agent.yaml
│           ├── devops-agent.yaml
│           ├── qa-agent.yaml
│           ├── pm-agent.yaml
│           ├── marketing-agent.yaml
│           ┘── support-agent.yaml
├── migrations/
│   ┘── 001_initial.sql
├── contracts/                    # Machine-readable contracts (authoritative over spec prose)
│   ├── agent-tools.yaml          # 28 agents: wiring, subscriptions, tools, emit_events
│   ├── event-catalog.yaml        # 172 events: emitter, consumer, delivery_channel, payloads
│   ├── ddl-canonical.sql         # 37 tables: FK-ordered, empire init runs this
│   ├── system-nodes.yaml         # System nodes: scoring-node, pipeline-coordinator
│   ├── workflow-schema.yaml      # State machine: 18 stages, 27 transitions, 5 timers
│   ├── guard-action-registry.yaml # Named guards (22) and actions (17) for transitions
│   ├── tool-schemas.yaml         # Tier 2 tool input schemas (21 tools)
│   ├── prompt-variables.yaml     # Template variables for prompts (44 variables)
│   ├── upgrade-actions.yaml      # Per-version typed migration actions
│   ├── verification-gates.yaml   # Test gate manifest (58 gates)
│   ├── agent-config-map.yaml     # Agent ID → config file path mapping
│   ├── prompt-manifest.sha256    # Hash manifest for 20 prompt files
│   ├── tooling.lock              # Required binaries + packages for gates
│   ├── spec-writer-guide.md      # Guide for spec writers
│   ├── CHANGELOG-v2.0.XX.md      # Per-version changelogs
│   ├── platform/                  # Platform specification
│   │   └── platform-spec.yaml    # MAS orchestration platform spec
│   └── prompts/                  # 20 agent system prompt files
│       ├── empire-coordinator.md
│       ├── analysis-agent.md
│       └── ... (20 total)
├── go.mod
├── go.sum
┘── README.md
```

---

## 17. Contracts

Machine-readable contract files live in `contracts/` at the repository root. These files are **authoritative over spec prose** — if prose says one thing and a contract file says another, the contract file wins. Prose explains *why*; contracts define *what*.

### 17.1 Contract Files

**`contracts/agent-tools.yaml`** — Canonical agent registry. Defines every agent's wiring: id, type, role, model_tier, conversation_mode, max_turns_per_task, subscriptions (static EventBus subscriptions for holding/factory/OpCo leadership), subscriptions_bootstrap (routing table entries installed at OpCo spinup for workers), tools_tier2 (domain-specific tools), and emit_events. Universal tools (agent_message, emit_*, native LLM tools) are injected into all agents and not listed per-agent.

**`contracts/event-catalog.yaml`** — Canonical event routing catalog. Defines every event's: emitter, consumer, intercepted flag, passthrough flag, routing type, delivery_channel, and payload fields. The `delivery_channel` field is the single source of truth for how each event reaches its consumer and determines which verifier checks apply (see §15.0 terminal event policy + verifier scope).

**`contracts/ddl-canonical.sql`** — Canonical database schema. Complete CREATE TABLE statements in correct FK ordering. This is what `empire init` runs. No assembling DDL from multiple spec sections. Includes all runtime tables (pipeline_receipts, scan_accumulators, validation_pipelines, pipeline_processed_events, pending_dedup_candidates, runtime_config, template_prompt_drafts) that earlier spec versions documented only in migration descriptions. All 7 runtime tables match their Go struct definitions (validated in v2.0.34).

**`contracts/upgrade-actions.yaml`** — Machine-readable upgrade delta, one section per spec version. Each action has: id, type (add/edit/drop/rename/migrate/grep_kill), priority (must_pass/should_fix/optional), target_file, key_path, expected_before, expected_after, verify_command, depends_on. Replaces human-interpreted changelog prose with executable instructions. The implementer processes actions in order; each is independently verifiable.

**`contracts/verification-gates.yaml`** — Test gate manifest listing required verification commands and pass criteria. Gates are prioritized: `must_pass` (blocks release), `should_pass` (tracked debt), `informational` (coverage trends). Compliance is binary: all must_pass gates green = compliant.

**`contracts/system-nodes.yaml`** — System node definitions. Each node declares: subscribes_to, produces, owned_transitions (from workflow-schema.yaml), execution_type (workflow_node or runtime_interceptor), implementation path, state table, and idempotency table. Includes scoring-node and pipeline-coordinator.

**`contracts/workflow-schema.yaml`** — Declarative state machine for the vertical pipeline (v2.1.0). Defines 18 stages across 4 phases, 27 transitions with trigger events/guards/actions, 5 timers, and terminal stages. The platform workflow engine reads this to enforce transitions. See §5.9 for architecture.

**`contracts/guard-action-registry.yaml`** — Named code-backed behaviors referenced by workflow-schema.yaml transitions (v2.1.0). 22 guards (boolean checks) and 13 actions (side effects), each categorized as `platform` (generic) or `empire` (business-specific). Empire guards reference prompt-variables.yaml via `policy_ref`.

**`contracts/tool-schemas.yaml`** — Input schemas for all 21 Tier 2 tools (v2.0.49). 2 universal (agent_message, mailbox_send) + 19 per-agent. The MCP tool gateway generates tool definitions from this file. Agents see typed parameters with enum constraints.

**`contracts/prompt-variables.yaml`** — Single source of truth for all values that appear in multiple prompts (v2.0.47). 44 variables covering thresholds, enum lists, capability tiers, scoring dimensions. Prompts use `{{variable_name}}` syntax; runtime substitutes before sending to LLM.

**`contracts/agent-config-map.yaml`** — Maps agent IDs to their config file paths. Used by the runtime to locate agent configs.

**`contracts/prompt-manifest.sha256`** — Hash manifest for all 20 prompt files. Run `sha256sum -c` to verify prompt files match the tarball.

**`contracts/platform/platform-spec.yaml`** — MAS orchestration platform specification (v2.1.0). Defines the contract formats, vocabulary (6 primitives), compliance rules (16 rules), built-in hooks (5 guards + 5 actions), workflow state model, and file layout convention for any multi-agent workflow. EmpireAI is one workflow running on this platform. See §5.10.

**`contracts/prompts/`** — 20 system prompt files, one per agent. Runtime loads directly from these files. Mode variants use `{agent-id}.{mode}.md`.

**`contracts/tooling.lock`** — Tracks contract format version and required tooling versions.

**`contracts/spec-writer-guide.md`** — This guide. Contains everything a spec writer needs: contract file descriptions, authority hierarchy, version bump process, lessons learned, codebase reference, and current state.

### 17.2 Contract Authority Rules

1. **Agent wiring conflicts**: `agent-tools.yaml` wins over §4.2.2 agent table, Appendix B configs, §12 roster. If an agent's subscription list differs between contract and prose, the contract is correct.
2. **Event routing conflicts**: `event-catalog.yaml` wins over §5.4 event catalog table, `agent-tools.yaml` emit_events + `event-catalog.yaml` emitter. If an event's emitter or consumer differs, the contract is correct.
3. **Schema conflicts**: `ddl-canonical.sql` wins over migration files and spec prose table descriptions. If column types or constraints differ, the contract is correct.
4. **Prose remains valuable**: Spec prose explains design rationale, architectural decisions, and historical context. Contracts don't capture *why* — they capture *what*. Both are needed.

### 17.3 Test Verification Against Contracts

Implementation tests load contract YAML/SQL files at test time and cross-reference against Go runtime data structures. This is the primary mechanism for preventing spec-code drift.

**Implementation:** `internal/runtime/contract_compliance_test.go` — runs with `go test ./internal/runtime/ -run TestContractCompliance`. Reads contract files from `contracts/` directory using `os.ReadFile`, unmarshals into simple structs, compares against runtime registries.

**7 automated compliance gates (v2.1.0):**

| Gate | Test Function | What It Validates |
|------|--------------|-------------------|
| 1. Agent config | `TestContractCompliance/agent_config` | model_tier, max_turns, conversation_mode, tools match agent-tools.yaml + agent-config-map.yaml for all 28 agents |
| 2. Subscriptions | `TestContractCompliance/subscriptions` | Holding/factory config subs, OpCo bootstrap routes, OpCo roster subs all match contracts |
| 3. CommGraph emit_events | `TestContractCompliance/emit_events` | agentProducerEvents matches contract emit_events, scoring-node produces match system-nodes.yaml |
| 4. Schema payload coverage | `TestContractCompliance/schema_payload` | Every event-catalog payload field exists in EventSchemaRegistry. Strict equality when additionalProperties=false |
| 5. DDL tables + columns | `TestContractCompliance/ddl_tables` | Table count matches DDL (37), 7 runtime table columns match Go struct field tags |
| 6. Version constants | `TestContractCompliance/version_constants` | runtimeSpecVersion and TemplateVersion match contract spec_version |
| 7. Agent prompts | `TestContractCompliance/agent_prompts` | SHA-256 of runtime-loaded prompt matches `agent-prompts.yaml` sha256_prefix. Catches stale prompts that cause enum/field confusion |

**What these tests do NOT cover** (remain manual or require integration infrastructure): event roundtrip tests, crash recovery, integration-level multi-agent routing, grep-based one-time cleanup gates. See `verification-gates.yaml` — each gate has an `automated` field indicating whether it has a Go test implementation (`null` = manual).

**Failure output example:**
```
--- FAIL: TestContractCompliance/emit_events/scoring-node
    contract_compliance_test.go:142: scoring-node produces mismatch:
      in code but not contract: [vertical.discovered]
      in contract but not code: [pipeline.dead_letter]
```

### 17.4 Contract Maintenance Protocol

Every spec revision that changes agent wiring, event routing, or schema must update the relevant contract file in the same commit. To prevent prose-vs-contract drift (the most common failure mode in v2.0.37-40 revisions), the spec writer follows this verification protocol.

**Changelog `touches:` requirement (v2.0.40+):**

Every changelog entry must declare which contract files it affects using a `touches:` block:

```
**v2.0.XX — Description**
...
touches:
  - event-catalog.yaml       # reason: category.assessed payload changed
  - system-nodes.yaml        # reason: new pre-filter logic formalized
  - upgrade-actions.yaml     # reason: new implementation actions added
  - agent-tools.yaml         # no change needed (agent wiring unchanged)
  - ddl-canonical.sql        # no change needed (no schema changes)
  - verification-gates.yaml  # no change needed (no new gates)
```

Every contract file must appear in the `touches:` block — either with a change reason or with "no change needed" and why. This forces explicit acknowledgment of each contract file on every revision. If a changelog mentions an event name but doesn't tag `event-catalog.yaml`, that is a gap signal.

**Post-changelog verification checklist:**

After writing any changelog entry, the spec writer runs through these checks before the revision is complete:

1. **Event payload check:** For each event name mentioned in the changelog, verify its payload in `event-catalog.yaml` matches the prose description. If the changelog adds a field to an event, that field must appear in the catalog.

2. **Schema registry check:** For each event with payload changes, verify the `EventSchemaRegistry` entry in `internal/runtime/event_emit_tools.go` matches the catalog. If the changelog changes an enum value or adds a required field, the schema must be updated. The `TestContractCompliance/schema_payload` gate automates this check.

3. **Threshold/enum cross-validation:** For each numeric threshold or enum value mentioned in the changelog, verify it matches `system-nodes.yaml` and any embedded Go code in the spec. Common drift points: composite score thresholds, mode name enums, rubric dimension lists, red flag type enums.

4. **Subscribes_to/produces check:** If the changelog changes which events an agent or system node emits or consumes, verify `system-nodes.yaml` subscribes_to/produces lists and `agent-tools.yaml` emit_events/subscriptions match.

5. **Upgrade-actions check:** For each implementation-impacting change in the changelog, verify a corresponding action exists in `upgrade-actions.yaml` with a verify command.

6. **Version stamp check:** All contract file headers must show the current spec version.

7. **Payload rename/removal changelog (v2.1.0):** When payload fields are renamed or removed, the changelog must include an explicit "old → new" mapping and list all affected consumers. Example:
   ```
   Payload change: spec.validation_requested
     - REMOVED: spec_version (legacy alias)
     - RENAMED: validation_tier → spec_tier
     - ADDED: spec_content
     - Affected consumers: spec-auditor (subscription), factory-cto (via cto.spec_review_requested downstream)
   ```
   This ensures the implementer updates all event schema definitions, test expectations, and consumer handlers in the same revision. The exhaustive-exact payload convention means any rename or removal without a corresponding code change will fail `TestContractCompliance/schema_payload`.

**Detection heuristic:** If the spec .md file changed but a contract file mentioned in `touches:` was not actually modified, the revision is incomplete. If a changelog references an event name, agent name, or table name but the corresponding contract file is listed as "no change needed," verify that claim is correct.

---

## 18. Open Questions

1. ~~**Context window management**~~: **Resolved in v1.7, updated v2.0.19.** Default policy by agent type:
   - **Factory workers — stateless** (Analysis Agent, Spec Auditor, Discovery Coordinator, Validation Coordinator, Spec Reviewer, Pre-Brand Agent): task-scoped. One event in → one result out → reset. No context pressure — tasks complete in 5-20 turns.
   - **Factory workers — stateful per-vertical** (Business Research Agent, Lightweight Spec Agent): `session_per_vertical` (§4.4.4). Persistent context within a single vertical's workflow, isolated from other verticals. BRA runs a 6-event multi-step workflow per vertical (research → spec → review → approve). LSA does draft→revise cycles. Both need session continuity within a vertical but must not cross-contaminate across concurrent verticals.
   - **Build-phase workers** (Backend, Frontend, Tech Writer, QA): task-scoped with file-system continuity. Each build assignment is a fresh conversation. Agent reads current codebase from disk at the start of each task — file system is the durable context, not the conversation. Session rotation threshold: 50 turns (from `llm.session.rotate_after_turns`).
   - **Session-scoped agents** (CEO, CoS, VPs, Support, PM, CTO): persistent context across tasks. Summarization strategy: when turn count exceeds rotation threshold (200), the Conversation Manager generates a summary of the current session and injects it as the opening message of the new session. Summary template: "Previous session covered: [key decisions], [current state], [pending items]." Summary is also persisted to `conversations.summary` for crash recovery.
   - **Hotfix:** If any agent's response quality degrades visibly (repeated tool errors, circular reasoning), the SessionRegistry rotates immediately regardless of turn count. This is the `rotate_on_parse_failures` safety net.

2. ~~**Parallel scanning**~~: **Resolved in v1.2.** Empire Coordinator processes one geography at a time sequentially. Rationale: factory pipeline is not latency-sensitive (weeks, not minutes), parallel scanning burns API budget without clear ROI, and sequential processing simplifies pipeline state management. Empire Coordinator maintains a geography backlog and moves to the next after the current batch clears scoring. If factory throughput becomes a bottleneck (unlikely before 20+ verticals), parallelize at the scoring stage rather than discovery.

3. **Feedback loops**: When a human kills a vertical, should that signal improve Discovery and Scoring? How?

4. **Frontend technology**: Factory CTO should mandate server-rendered HTML with Go templates (simplest, mobile-first, no JS framework). Confirm this as standard or leave to each CTO?

5. ~~**External service integration**~~: **Resolved in v1.7.** Classification by automation level:
   - **Fully automatable (agent does it):** Domain availability check, DNS configuration (via Cloudflare API), SSL provisioning (Let's Encrypt/certbot), Instagram handle availability check, Google Maps API queries, web scraping, landing page deployment.
   - **Agent-initiated, human-verified (mailbox approval):** Domain purchase (spend approval required → mailbox), WhatsApp Business API setup (agent prepares application, human submits final verification to Meta), Instagram business account creation (agent prepares profile, human confirms).
   - **Human-only (cannot automate):** WhatsApp Business verification (Meta requires human identity verification), bank account setup for payment collection, legal entity registration (if needed).
   - **Implementation:** Tools for automatable services are implemented as Go functions called by the tool executor. Human-dependent services use a two-step pattern: agent prepares everything possible → emits to mailbox with "ready to submit, need human to click verify" → human completes → agent receives confirmation event. Marketing agent is told in its prompt which services it can use directly vs which require mailbox approval.

6. ~~**Inbound message handling**~~: **Resolved in v1.2.** Inbound Gateway is a shared process managed by Holding DevOps (see §4.7). Each vertical's deployed service registers its webhook endpoints with the Gateway at first deploy. The Gateway runs as a standalone HTTP server on a dedicated port, routes incoming webhooks to the EventBus based on path (`/webhooks/{vertical}/whatsapp`), and is separate from the per-vertical Go binaries. This means the scaffold does NOT include webhook handling — the scaffold handles the product (web UI, API). Webhooks flow: external → Gateway → EventBus → routing table → Support Agent (or relevant agent). The Gateway shares the same Postgres instance and process as the runtime.

7. ~~**VP observe cost**~~: **Resolved in v0.4 —** observation aggregators. Workers emit digests, VPs subscribe to digests + critical. See `event-catalog.yaml` for `support_digest`, `outreach_digest` events.

8. **VP-to-VP coordination**: Chief of Staff bridges this gap by design. No direct VP-to-VP channel needed — CoS observes both domains and routes cross-domain information. If CoS proves insufficient after 2+ verticals, revisit.

9. **CEO-to-CEO learning**: Operations Analyst handles cross-vertical learning by reading all vertical data and proposing improvements. No CEO-to-CEO channel needed — the analyst is more systematic than informal CEO chat.

10. ~~**Revenue collection**~~: **Resolved in v1.7.** Default standard: **MercadoPago for LATAM, Stripe for other markets.** Factory CTO includes payment scaffold boilerplate (MercadoPago SDK integration for Go, webhook handler for payment confirmation, payment status table). PM specifies billing UX per vertical. Payment flow: customer books → product creates pending payment → payment link sent via WhatsApp/web → external provider processes → webhook confirms → booking marked paid. Support handles payment questions. No dedicated RevOps agent — CEO reads revenue from `spend_ledger` + payment table in reports. Revisit at 100+ users per vertical if payment complexity warrants dedicated agent.

11. ~~**Customer data privacy**~~: **Resolved in v1.2 — see §12. Data Handling Policy.**

12. **Agent replacement vs context reset**: When a CTO fires and rehires a Backend agent, the new one has no codebase awareness. Bootstrap via file system scan + summary document?

13. **Budget enforcement granularity**: VPs and CTO have budget envelopes. Per-agent tracking? Or just envelope-level total? CTO sub-team complicates this (4 agents under CTO's budget).

14. **Holding DevOps as bottleneck**: All verticals deploy through Holding DevOps. With 5+ verticals, could this become a queue? Does Holding DevOps need to handle concurrent deploy requests?

15. **Technical spec depth**: How detailed should Tech Writer's spec be? Detailed enough that Backend can copy-paste API signatures, or high-level enough that Backend makes implementation decisions? Tension between spec quality and spec cost.

---

## Appendix A: Operating Agent Configs

**Agent wiring** (subscriptions, tools, model_tier, max_turns, conversation_mode) is defined in `contracts/agent-tools.yaml`. **System prompts** are in `contracts/prompts/{agent-id}.md`. Runtime loads from both.

Operating agent config file paths are mapped in `contracts/agent-config-map.yaml`.

---


## Appendix B: Agent System Prompts

**Canonical source:** `contracts/prompts/{agent-id}.md` — one file per agent. Runtime loads directly from these files. There is no other copy.

**Mode variants** use `{agent-id}.{mode}.md` (e.g. `market-research-agent.corpus.md`).

**Current prompt files (19):**

| Agent | File | Lines |
|-------|------|-------|
| Empire Coordinator | `empire-coordinator.md` | 136 |
| Factory CTO | `factory-cto.md` | 79 |
| Holding DevOps | `holding-devops.md` | 83 |
| Operations Analyst | `operations-analyst.md` | 88 |
| Spec Auditor | `spec-auditor.md` | 144 |
| Discovery Coordinator | `discovery-coordinator.md` | 51 |
| Analysis Agent | `analysis-agent.md` | 136 |
| Validation Coordinator | `validation-coordinator.md` | 55 |
| Business Research Agent | `business-research-agent.md` | 81 |
| Lightweight Spec Agent | `lightweight-spec-agent.md` | 75 |
| Spec Reviewer | `spec-reviewer.md` | 40 |
| Market Research Agent | `market-research-agent.md` | 125 |
| Market Research Agent (corpus) | `market-research-agent.corpus.md` | 79 |
| Trend Research Agent | `trend-research-agent.md` | 86 |
| Pre-Brand Agent | `pre-brand-agent.md` | 35 |
| OpCo CEO | `opco-ceo.md` | 125 |
| OpCo Chief of Staff | `opco-chief-of-staff.md` | 75 |
| OpCo Head of Product | `opco-head-of-product.md` | 96 |
| OpCo Head of Growth | `opco-head-of-growth.md` | 67 |

**Scanner agents** (B.13) use synthetic adapters in Phases 1-3. Full prompts deferred until Phase 4.

**`TestContractCompliance/agent_prompts`** verifies every agent in `agent-tools.yaml` has a corresponding prompt file and the runtime loaded it.
