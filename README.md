<p align="center"><img src="logo.png" alt="Division Swarm logo" width="280"></p>

# Division Swarm

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue.svg" alt="License: Apache 2.0"></a>
  <a href="platform-spec.yaml"><img src="https://img.shields.io/badge/platform--spec-v0.7.0-15803D.svg" alt="Platform spec v0.7.0"></a>
  <a href="https://goreportcard.com/report/github.com/division-sh/swarm"><img src="https://goreportcard.com/badge/github.com/division-sh/swarm" alt="Go Report Card"></a>
  <a href="https://github.com/division-sh/swarm/discussions"><img src="https://img.shields.io/github/discussions/division-sh/swarm" alt="GitHub Discussions"></a>
  <a href="https://docs.division.sh"><img src="https://img.shields.io/badge/docs-docs.division.sh-15803D.svg" alt="Docs"></a>
</p>

**The operating system for autonomous multi-agent systems.**

Swarm runs fleets of LLM agents as a durable, stateful system. You declare it in YAML and a deterministic engine runs it, owning state, routing, isolation, cost, and recovery. The LLM never runs the system: agents reason in scoped sessions and emit events, and deterministic code, never the model, decides what each result changes.

More than a simple orchestrator that decides which agent runs next, Swarm runs the system around it. Work is modeled as entities (an order, a ticket, a candidate business) moving through a state machine you declare: the runtime schedules hundreds at once, keeps them isolated, meters their spend, persists their state, and resumes them after a crash, days later if a timer or a human kept them waiting. Any run can be replayed or forked from the log. Deterministic routing is one piece; the rest is the operating system.

Single Go binary, local/dev SQLite by default with Postgres as a production opt-in. Multiple LLM backends ship today: the Anthropic API, the Claude CLI, OpenAI-compatible Chat Completions endpoints, native OpenAI Responses, and a credentialless deterministic Python mock runtime.

**Long-term direction:** entire divisions (engineering, support, operations) running as autonomous Swarm flows. Humans in the loop where judgment is required; agents and deterministic system nodes everywhere else.

---

## Design positions

Swarm makes a small number of opinionated choices and sticks to them.

- **Work is a durable state machine.** Every unit of work is an entity that moves through declared states by guarded transitions. The runtime persists state, so state survives a crash and hundreds of entities advance independently without interfering.
- **The control loop is deterministic.** No LLM decides what fires next. Routing is derived from declared subscriptions, and every event runs through a fixed handler pipeline. Conditions use [CEL](https://github.com/google/cel-spec): strongly typed, non-Turing-complete, no hallucinating router.
- **Every transition is one transaction.** Guard, accumulate, compute, commit, emit: all-or-nothing. A crash mid-handler leaves no partial state.
- **Flows are composable units.** A flow package declares its identity, state machine, system nodes, events, agents, tools, and policy. Typed input and output pins make composition mechanical rather than a refactor. Create your specialized flows and import them in other Swarm projects.
- **Agents are isolated.** Each agent runs in a scoped session and observes only the events it subscribes to. A coordinator addresses managers; managers address workers; workers do not share a context window. Per-entity Docker workspaces extend the isolation to the filesystem and process level: agents working on entity X have no access to entity Y's working tree.
- **Execution is replayable.** Every event and every state mutation is persisted. Any run can be reconstructed turn by turn, audited end to end, or resumed from its last consistent checkpoint after a crash. Auditability is native.
- **Runs are forkable.** Re-execute any run from any point in its history with a counterfactual like a different policy value, revised agent intent, or an entirely new contract bundle, and compare outcomes against the original. Last week's failed run can be replayed against this week's fixed contracts. The cost of iterating on a flow drops to the cost of forking a run, which turns continuous improvement into a regular engineering loop instead of a deploy-and-watch cycle.
- **Humans participate as a first-class actor.** Approvals, rejections, and deferrals flow through a durable mailbox using the same event model as the rest of the runtime. A pending decision is just another event waiting for its handler: a human's, in this case. Autonomy is a dial, not a switch.
- **Spec-driven and well-suited for agentic authoring.** The platform specification is a single machine-readable YAML file, the static analyzer's errors cite check ids and exact field paths an LLM can act on directly, and 200+ verifying conformance bundles provide working patterns to mirror. See [For agent authors](https://docs.division.sh/build/agentic-authoring).

The tradeoff: a Swarm flow cannot be re-wired by an LLM at runtime. That rigidity is the point.

---

## Try it

Follow the [v0.7.1 Quickstart](#quickstart) below to install the released
binary, create a shipped starter and get a deterministic mock result. No
hand-written flow files, Go toolchain or provider credentials are needed
for that mock test. Live Telegram execution is a separate step.

---

## A flow is a contract

A handler is declared in YAML. The engine reads it and executes it. No product-specific code hooks.

```yaml
# nodes.yaml
ticket-orchestrator:
  ticket.validated:
    guard: state.priority in ['high', 'urgent']
    data_accumulation:
      writes: [order_summary, validation_context]
    sets_gate: g1_validation
    advances_to: processing
    emit: work.assigned
```

Execution order is fixed by the dependency graph:

```
guard ──▶ accumulate ──▶ compute ──▶ {advances_to, sets_gate, data_accumulation} ──▶ emit ──▶ action
                                              │
                                       atomic commit
```

Conditions use [CEL](https://github.com/google/cel-spec): strongly typed, non-Turing-complete.

---

## Quickstart

### Install v0.7.1

The [release](https://github.com/division-sh/swarm/releases/tag/v0.7.1)
publishes these exact assets:

| Asset | Platform |
| --- | --- |
| `swarm_v0.7.1_linux_amd64.tar.gz` | Linux amd64, compatible glibc and `libgcc_s.so.1`; not static or Alpine/musl |
| `swarm_v0.7.1_darwin_arm64.tar.gz` | macOS arm64; built and smoke-tested by the release workflow on macOS 15 |
| `SHA256SUMS` | Checksums for both archives |

This Linux transcript was run on Ubuntu 22.04.5 with curl, CA certificates,
tar and GNU coreutils available. Use an empty directory for the release
files. No source checkout or Go installation is required.

```bash
mkdir -p "$HOME/.local/bin" "$HOME/swarm-v0.7.1"
cd "$HOME/swarm-v0.7.1"
curl --fail --location --silent --show-error --output swarm_v0.7.1_linux_amd64.tar.gz https://github.com/division-sh/swarm/releases/download/v0.7.1/swarm_v0.7.1_linux_amd64.tar.gz
curl --fail --location --silent --show-error --output SHA256SUMS https://github.com/division-sh/swarm/releases/download/v0.7.1/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf swarm_v0.7.1_linux_amd64.tar.gz
install -m 755 swarm "$HOME/.local/bin/swarm"
export PATH="$HOME/.local/bin:$PATH"
swarm version
```

The checksum command prints:

```text
swarm_v0.7.1_linux_amd64.tar.gz: OK
```

The download, extraction, install and PATH commands are silent on success.
Version output begins:

```text
Swarm v0.7.1
Commit: e32076a89371a04c5196008965ed93530b937e90
Built: 2026-11-09T07:35:22Z
Go: go1.26.8 linux/amd64
```

### Create and test the starter

```bash
swarm new zero-agent-automation
cd zero-agent-automation
swarm verify --portable
swarm test . tests/smoke.yaml
```

The scaffold creates admitted source and private deployment files. Its
printed instructions are:

```text
Created zero-agent-automation in zero-agent-automation

Next:
  cd zero-agent-automation
  swarm verify --portable
  swarm test . tests/smoke.yaml
  swarm secrets set -- 'telegram.telegram_bot_token'
  swarm serve --dev
```

Verification begins with these lines, followed by the deployment checks
it did not run and the embedded pack inventory:

```text
verify ok: source=zero-agent-automation@1.0.0
portable structural checks passed; deployment admission not evaluated
```

The test prints:

```text
scenario ok: tests/smoke.yaml
swarm test ok: scenarios=1
```

That is a deterministic mock result, not a Telegram send. The test owns a
fresh private runtime; it needs no running server, Docker, LLM key or bot
token. For the v0.7.1 artifact above, keep the dot in the test command: its
first argument is the source directory. Serve and verify use the current
directory when it is omitted.

### The printed follow-on steps

```bash
swarm secrets set -- 'telegram.telegram_bot_token'
swarm serve --dev
```

The credential command asks `Secret value:` and `Confirm secret value:`
without echoing input. It reports credential presence, not token validity:

```text
secret set: key=telegram.telegram_bot_token source=file writable=yes shadowed=no present=yes
Credential presence here is inventory only; static provider/channel requirements use the file tier.
```

The [installed-artifact proof](https://github.com/division-sh/swarm/issues/1578#issuecomment-6735094993)
executed these exact commands using a synthetic credential with provider
networking blocked. Serve selected the project-local SQLite scratch store,
reported that no workspace was required and reached `/readyz`. Its final
readiness and Ctrl-C shutdown output was:

```text
ready — waiting for events
^Cshutdown · complete
```

The process exited zero. This proves startup,
not a real bot token or Telegram delivery. Do not use a dummy credential
for live work; the separate consented real-token check has not been run.

`serve --dev` starts live execution, not mock mode, and replaces its
project-local scratch history on each invocation. No explicit SQLite path
is written by this starter. Plain `serve` uses retained state instead.
`run start` publishes into an existing serve; it does not boot a one-shot
runtime in v0.7.1.

### Current source after #3001 (unreleased)

Builds containing [#3001](https://github.com/division-sh/swarm/pull/3001)
accept a scenario file as the first argument and use the current directory:

```bash
swarm test tests/smoke.yaml
```

Their scaffold prints that dotless test command. This is separate from the
v0.7.1 archive instructions and recorded output above; it does not assert
that a newer release archive is available.

### Deployment configuration

SQLite requires no separate database service. No LLM key is needed for
this agent-free flow, but live connector sends require their provider
credentials. For Postgres, workspace and LLM setup, see
[Configuration](https://docs.division.sh/reference/configuration). Use
[`swarm.example.yaml`](swarm.example.yaml) as the non-secret deployment
reference. Swarm reads one file: `.swarm/swarm.yaml` in the current directory,
or built-in defaults if that file is absent. `--config <file>` replaces that
selection completely. `SWARM_CONFIG` selects the same file when `--config` is
absent; `--config` wins when both are set. Swarm does not discover root
`swarm.yaml` or user-global config files, and does not load repo `.env` files.
For contract credentials, use `swarm secrets set` and validate with
`swarm secrets check`.

The credential file defaults to `os.UserConfigDir()/swarm/credentials.json`,
independently of the project and `--swarm-dir` runtime-state directory. Set
`SWARM_CREDENTIALS_FILE` to a different file for each instance that needs a
separate file tier, for both `swarm secrets` and `swarm serve`. This override
does not isolate ambient environment credentials or managed/OAuth stores:
tool credentials retain their environment-over-file precedence, while
provider/channel consumers retain their file-only policy. At boot, `serve`
names each required credential's effective source without printing its value.
Inventory presence is not provider/channel admission: a tool's environment
source and a provider's file-only requirement remain separate observations.
Provider session material lives under `--swarm-dir` by connection ID; use a
different `--swarm-dir` for each serve instance that needs session isolation.

For local webhook development, `swarm serve --dev --expose` owns a temporary
public callback URL and reconciles registration declared by a configured channel
binding. Managed exposure requires `cloudflared` 2021.7.0 or newer (`brew install
cloudflared`). Quick-tunnel URLs are ephemeral and have no availability SLA. To
use an externally managed HTTPS origin instead, provide both
`--public-webhook-base-url` and an explicit loopback
`--public-webhook-listen`. This explicit external-origin mode works with both
development serve and plain retained serve; only the managed quick tunnel is
development-only. The public listener exposes `/webhooks/*` only and never
exposes API, MCP, health, or other control-plane routes.

For credentialless scenarios, use `swarm test` and give each agent one
contracts-root-relative Python performance. The command owns a fresh private
runtime; no running server or deployment configuration is required. `swarm serve`
and `swarm serve --dev` always select live execution, even with doubles present:

```yaml
# agents.yaml
reviewer:
  id: reviewer
  role: reviewer
  intent: intent/reviewer.md
  model: regular
  memory: false
  subscriptions: [review.requested]
  mock:
    kind: python
    module: mocks/reviewer.py
```

The module exports `handle(input)` and returns one JSON-compatible object with
`text`, `calls`, or both. Mock turns use normal sessions, tools, routing,
persistence, and readbacks, but the runtime rejects every external effect before
launch. The compiled generation captures the module bytes and digest; it never
rereads the source file while running.

---

## Core concepts

| Concept | One line |
|---|---|
| **Flow** | A self-contained package with input/output pins. The marketplace unit. |
| **State** | A named state in a flow's state machine. An entity is in exactly one at a time. |
| **System node** | Deterministic code. Subscribes to events, advances state, emits events. No LLM. |
| **Agent** | LLM-powered. Subscribes to events, reasons, calls tools. Can own transitions that require judgment. |
| **Handler** | The unit of execution attached to an event. Composed of guard, accumulate, advances_to, sets_gate, emit, action. |
| **Event** | A typed message with a payload schema declared in `events.yaml`. |
| **Gate** | A named boolean set by a handler. Other handlers can guard on it. |
| **Timer** | A durable time-based trigger attached to a stage. Persisted across restarts. |
| **Pin** | A typed input or output of a flow. Used at composition time to wire flows together. |

Authoritative reference: [`platform-spec.yaml`](platform-spec.yaml). The generated public API artifact is [`openrpc.json`](openrpc.json).

---

## CLI

The `swarm` CLI is one client over the v1 RPC; full command reference at [docs.division.sh/reference/cli](https://docs.division.sh/reference/cli).

---

## Architecture

A contract bundle is verified by the static analyzer, then loaded into the engine, which starts the event loop. From there, everything is an event. An event, whether it arrived from ingress or was emitted by a node or an agent, is validated against its schema, persisted, and routed to its subscribers by their declared subscriptions, never by an LLM. Exactly one system node owns each event: it runs a fixed handler pipeline and commits the state change, gate writes, data writes, and any emitted events in a single transaction. Agents subscribe too, but they only reason inside a scoped session and emit their results back as events; a system node decides what those results actually change. Because every event and every state mutation is persisted, any run can be replayed turn by turn or forked from the log.

```mermaid
flowchart TB
    ING["ingress: RPC / API / MCP"] -->|events| BUS(["event bus"])
    BUS -->|"routed by subscription"| SN["system nodes<br/>deterministic, own state"]
    BUS -->|"routed by subscription"| AG["agents<br/>LLM, scoped sessions"]
    SN -->|emit| BUS
    AG -->|"emit results"| BUS
    SN ==>|"advance state, one transaction"| STORE[("Selected store:<br/>SQLite local/dev default<br/>or Postgres opt-in")]
    STORE -.->|"replay / fork"| BUS
```

Only the system node writes state, and it does so in one transaction; agents reach the store only indirectly, by emitting an event a node handles. Underneath the loop are the primitives that make the design positions enforceable:

- **A static analyzer for contracts.** Before any event fires, every contract is run through dozens of structural checks: state reachability, payload completeness, agent routing, timer lifecycle, CEL parse, and agent-intent validation. A bundle that boots has passed this analysis.
- **Two-layer event-sourced persistence.** Every event lands in the selected runtime store; every entity state mutation lands in a separate mutation log with before/after diffs. Local/dev runs use SQLite by default, while Postgres remains the explicit external/production option. Replay says "show me what happened"; the mutation log says "show me exactly what changed." Accumulator projections compute read-models off the streams, so handlers and external readers can query aggregated state without scanning the raw logs.
- **Role-based routing authority.** Agent isolation is enforced by an authority layer that decides, per entity status, which agent role may address which other role. "Any agent can talk to any agent" is not a default the runtime offers.
- **Reliability primitives.** Undeliverable events retry with exponential backoff and land in a dead-letter store after exhaustion. Dead-letters are indexed and replayable.
- **Live budget tracking.** Token usage is tracked per entity and per actor in real time. Declared thresholds escalate into throttle and emergency states; humans get a mailbox item when the runtime hits an emergency.
- **A comprehensive JSON-RPC API.** The generated method catalog spans runs, events, agents, conversations, entities, mailbox, and runtime control, plus WebSocket subscriptions for live traces, event streams, log tails, and health. The surface is published as a machine-readable OpenRPC document, so clients can be generated rather than hand-coded. The `swarm` CLI is one such client; anyone can build their own.
- **MCP gateway.** Swarm both exposes and consumes the Model Context Protocol. External tools can drive a runtime over MCP; agents pull tool definitions from upstream MCP servers without code changes.

---

## When to use Swarm

Swarm is opinionated. Work is a durable state machine, routing is deterministic, and persistence, isolation, and cost control are the substrate rather than add-ons. That depth buys reproducibility, crash recovery, and audit, and it costs YAML and a static-analyzer pass before the runtime starts. The tradeoff is the right one for some workloads and the wrong one for others.

The static analyzer's refusal to boot on a half-finished contract is a feature in production and a friction in exploration. Plan accordingly.

### Strong fit

- **Long-running workflows where reproducibility matters.** Anything where someone will eventually ask "why did the system do that on October 14th at 03:47?"
- **Multi-agent coordination at three or more roles.** Coordinator, specialists, reviewer. Hierarchical addressing avoids the context-window explosion of flat setups.
- **Crash recovery is a requirement, not a nice-to-have.** Atomic transitions and event replay are baseline, not bolt-on.
- **Regulated or audited environments.** Two-layer event-plus-mutation persistence makes "what changed, when, and because of which event" a single SQL query.
- **High-volume parallel entities.** Orders, tickets, leads, claims. Per-entity workspaces let hundreds flow through the same contract without interfering.
- **Cost-controlled deployments.** Live budget tracking with throttle and emergency states is built in, not a future add-on.

---

## Project status

**Pre-1.0. Breaking changes expected.**

- Platform specification: **v0.7.0**, complete. See [`platform-spec.yaml`](platform-spec.yaml).
- Engine: Go. Handler-first execution for the proven-safe subset; full handler-first execution in progress.
- Conformance suite: **12 tiers, 200+ distinct test contract bundles** spanning primitives, accumulation, atomic event-loop semantics, composition, boot verification, runtime fork, and policy patterns. The suite runs against an internal scripted harness, and the active Python mock backend provides credentialless full-runtime agent proof without live LLM spend.
- Used internally to power autonomous multi-agent workflows. External use at your own risk.

The trajectory points at running whole company divisions as autonomous flows. 

---

## Documentation

Public documentation is at [docs.division.sh](http://docs.division.sh).
Available in this repo too:

| Document | Read this when… |
|---|---|
| [`platform-spec.yaml`](platform-spec.yaml) | You need the authoritative specification. |
| [`openrpc.json`](openrpc.json) | You need the generated public API artifact. |
| [For agent authors](https://docs.division.sh/build/agentic-authoring) | You are an LLM agent helping a human author a Swarm flow. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | You want to contribute issues, docs, code, or tests. |
| [`SECURITY.md`](SECURITY.md) | You need to report a suspected vulnerability privately. |
| [GitHub Discussions](https://github.com/division-sh/swarm/discussions) | You want to ask a question, share a flow, or propose a feature. |

---

## Development

Requirements: Go 1.26.0 or newer, with Go 1.26.8 as the preferred toolchain
in [`go.mod`](go.mod). With `GOTOOLCHAIN=auto`, Go can select or download a
newer toolchain when needed. For offline builds or `GOTOOLCHAIN=local`, install
a compatible Go toolchain yourself; Go 1.26.8 is recommended, and Go 1.25
cannot build this module without a toolchain switch.

Docker is the default workspace-isolation backend; an
explicit host backend is available for local-dev or trusted remote work (see
[`swarm.example.yaml`](swarm.example.yaml)). Host backend command execution is
trusted/unsafe: native `bash` commands run as the host user when both host
backend selection and `native_tools.bash` authorization are present. Host bash
is full host-user shell execution from the workspace backing directory; use
relative paths for workspace files, and absolute paths follow the host
deployment namespace and OS permissions. It is not command-limited or
Docker-equivalent isolation, and Claude/provider host execution remains
unsupported.
Plain local `swarm run start <source-root> ...` uses SQLite at `.swarm/stores/dev.db` unless
you explicitly opt into Postgres with `store.backend: postgres` in runtime
config. Build or pull the configured workspace image (`swarm-workspace:latest`
by default), or configure a compatible image before commands that start the
runtime.

Repo `.env` files are not loaded by Swarm commands. Keep durable settings in
config files and put contract-required credentials in the local secrets store:

```bash
swarm secrets set sendgrid_api_key --stdin
swarm secrets check
```

Handler actions are retired. Create keyed receivers through connection-owned
resolution and same-name supplied state from the creating message. Pins are
names-only; `initialize` and `instance_variables` are not admitted. Built-in local Git commits are not
currently available; there is no replacement action or compatibility alias.

```bash
go build ./cmd/swarm
golangci-lint run
go run ./cmd/swarm-unused
go run ./cmd/swarm-test
go run ./cmd/swarm-test --tier lifecycle # retained restart journeys
go run ./cmd/swarm-test --full # exhaustive proof, including both 900s soaks
```

`swarm-unused` defaults to native-only analysis on the current host. Master
nightly and explicit full dispatch CI merge native Linux and Darwin reachability
with tests enabled across default, race, and `issue2413` configurations before
enforcing U1000. PR and push CI skip these three native analysis jobs at every
tier; the analyzer's contract tests remain in their existing proof tiers. The
noncompiling `issue2438` configuration remains parked under E's
[#2496](https://github.com/division-sh/swarm/issues/2496) until compile repair.

For faster scoped validation while developing, use the changed-package local
loop documented in [`CONTRIBUTING.md`](CONTRIBUTING.md).

To run a flow locally, see the [docs Quickstart](https://docs.division.sh/quickstart).

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR. High-risk
semantic/runtime work must follow its pre-audit, source-owner, and proof-audit
requirements.

---

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
