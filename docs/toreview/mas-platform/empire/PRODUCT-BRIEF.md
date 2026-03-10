# EmpireAI — Product Brief

## Vision

EmpireAI is an autonomous AI holding company. It discovers micro-SaaS opportunities, validates them, and operates them as independent businesses — with minimal human intervention. The system functions as an AI-powered venture studio: a factory pipeline continuously finds and scores market opportunities, and operating companies (OpCos) run live SaaS products using multi-agent teams.

## Strategic Context

**Market focus:** LATAM/Paraguay initially. WhatsApp integration and regional compliance (SIFEN e-invoicing) are key differentiators.

**Why AI agents:** Software businesses have high automation potential. The repetitive parts — market research, spec writing, code generation, deployment, customer support — can be delegated to specialized agents. The creative parts — opportunity selection, strategic pivots, brand positioning — stay with the human (empire-coordinator + mailbox decisions).

**Why a pipeline:** One vertical is a bet. Fifty verticals running simultaneously is a portfolio. The factory pipeline is designed to flood the funnel and filter aggressively — most verticals get killed during scoring or validation. The survivors are high-confidence bets.

## Architecture

Empire runs on the MAS Platform as a composition of 4 flows:

**Discovery** → Scan markets, accumulate signals, produce vertical candidates. Multiple scan modes (SaaS gap analysis, trend surfing, local services, corpus mining). Agents fan out across subcategories, scanner sources, and trend topics. Signals accumulate and get deduplicated before becoming verticals.

**Scoring** → Multi-dimension scoring with weighted composite. 11 dimensions across 3 tiers (build feasibility 60%, market opportunity 30%, growth potential 10%). Hard gates on build_complexity and automation_completeness. Contest resolution when multiple analysts disagree. Derivation loop spawns alternative hypotheses from weak-scoring parents.

**Validation** → 4-gate pipeline: business research (deep-dive brief), MVP spec writing (with revision loop), CTO feasibility review (veto power), brand design (domain + social handle availability). Each gate is tracked independently. Failures route back to earlier gates or kill the vertical.

**Operating** → Dynamic flow instances — one per approved vertical. Full OpCo team: CEO, CTO, product, growth, engineering (backend, frontend, QA, devops), marketing, support, tech writer. 13 agents coordinating to build, launch, and operate a live SaaS product.

3 root-level agents span all flows: empire-coordinator (strategic oversight), operations-analyst (metrics), holding-devops (infrastructure).

## Key Learnings

**"The agents are smart — the wiring is broken."** Every production failure traced to event delivery issues, missing payload data, or tool schema problems — not agent intelligence. This insight shaped the entire architecture: deterministic orchestration via system nodes, typed event payloads, strict handler contracts.

**Scoring methodology matters critically.** Simple averages vs weighted tiers produce dangerously different results. An early GPT report used flat averaging and recommended unbuildable opportunities. The 3-tier weighted model with hard gates prevents this.

**Validate before infrastructure.** The original design called for 38 data sources, Postgres tables, S3, embedding models, pgvector, and scraper orchestration. We stripped all of it in favor of JSONL file drops for corpus mode. Validate the opportunity pipeline first, scale infrastructure second.

**Discovery quality is the binding constraint.** The first US market scan produced verticals scoring 56-71 with zero shortlisted. The pipeline is only as good as what enters it. This drove the "flood and filter" approach with multiple scan modes and corpus mining.

**Deterministic orchestration over LLM-managed routing.** CrewAI and similar frameworks let LLMs decide event routing. This fails at scale — nondeterministic, unreproducible, impossible to debug. Our system nodes are Go code (now declarative YAML) that route deterministically. The LLM reasons within its agent session; the platform handles everything else.

## Evolution

The spec evolved through 20+ versions:

- **v2.0.x** — Contract foundation. Iterative refinement of agents, events, tools, policy.
- **v2.1.0** — Platform-spec introduced. Vocabulary and abstractions.
- **v2.2.0** — Pipeline-coordinator decomposed into 5 system nodes.
- **v2.3.0** — Permissions, tool ownership, 9 files eliminated.
- **v2.4.0** — Flow composition model. Pin-based schemas. 4 flows.
- **v2.5.0** — Execution primitives. Compliance guidelines. Contract-driven policy.
- **v2.6.0** — Engine spec. Unified flow model. URI addressing.
- **v3.0.0** — Platform/product split. Independent versioning. Self-contained flow packages.
- **v3.0.1** — Zero product hooks. 100% declarative. All behavior in YAML.

From 15 contract files to 4 self-contained flows. From 8 product hooks to zero. From a monolithic prose spec to contracts-as-spec.

## What's Next

**For Empire:**
- OpCo prompt maturation — 9 worker agents need production-ready behavioral guidance
- Corpus campaign execution — seed data pipeline for discovery
- Voice agent integration (Retell) for LATAM WhatsApp
- First end-to-end vertical run

**For the Platform:**
- Implementer adopts v3.0 contracts (currently on v2.2.1 runtime)
- Phase 11: declarative node execution (engine interprets YAML handlers)
- Platform marketplace (flow packages)
- Visual workflow canvas (Tauri + React Flow)

## Numbers

| Metric | Value |
|--------|-------|
| Flows | 4 + root |
| Handlers | 51 (100% declarative) |
| Agents | 28 |
| Events | 176 |
| Tools | 20 (9 platform, 11 workflow) |
| Prompts | 29 |
| Product hooks | 0 |
| Platform version | >=1.1.0 |
