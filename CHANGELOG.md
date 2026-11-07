# Changelog

All notable changes to Division Swarm are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Division Swarm is pre-1.0; the public surface may change between minor versions
until v1.0. Each entry below lists the platform-spec version it ships against.

## [Unreleased]

Release target: v0.7.0 on November 7, 2026, without a second release candidate.
These notes describe merged source; the open readiness items below are not
release approval or a promise of old-store migration.

### Added

- Telegram deployment connect names the authenticated bot and supplies a
  prefilled send-code link, confirms the authenticated claimant without another
  yes/no prompt, and prints the operator-ready next commands. A newly committed
  operator binding sends `Swarm operator successfully connected` through the
  existing registered acknowledgement journal before terminal success; later
  connect, serve, restart and reset do not resend it. Account approval and
  provider credential/intake checks are unchanged. Human claim deadlines for
  both providers show local date/time to seconds with the timezone. (#2988)
  Interactive provider credentials keep hidden terminal entry before cancellable
  account confirmation.

- A newly bound registered WhatsApp operator receives one journaled
  `Swarm operator successfully connected` acknowledgement before connect reports
  success. Reconnect and restart do not resend it. (#2577)

- `swarm describe pack` lists selected providers and describes their admitted
  events, connector tools, provenance and target-scoped requirements. Tool detail
  and consumption fragments share the same typed readback; credentials and live
  readiness are not inferred from installed declarations. (#1963)

- Directory-owned workflow/provider source and scoped captured declarations retain
  exact source roots and artifacts through restart and fork recovery. This is the
  local pack-source model, not a hosted Hive registry. (#2791)
- Public service-mocked journeys cover every provider family in #2853 on both
  stores where persistence is involved, including signed webhooks, connectors,
  search, external MCP over HTTP and stdio, and OAuth connect/refresh. Slack's
  signed challenge/ingress and replay coverage completes the connector journey.
  Healthy coverage additions are not claimed as product-defect reproductions.
  (#2860, #2879, #2897, #2905, #2913)
- Advisory synthetic DeepSeek evidence is separate from deterministic CI. The
  stdio MCP check uses two real inference requests and verifies that the model
  consumes the linked tool result and copies its previously hidden receipt.
  This is not live acceptance of every provider, transport or device.
  (#2877, #2913)

### Changed

- The registered acknowledgement uses a new closed nullable-bundle journal
  schema. Incompatible upgrade and downgrade stores refuse boot; there is no
  migration. Preserve the existing database, SDK state, proofs and credentials;
  select separate fresh state and pair again rather than transplanting it. (#2577)
- An rc.1 operator-channel proof store must be removed and the channel re-paired
  before starting v0.7. Strict loading is unchanged; its refusal now includes
  this operator action instead of only the retired-field decode error. (#2942)
- Standby serve, `swarm health` and `/readyz` distinguish a held store from the
  last admitted serve instance and generation. `swarm store status` reports
  possession separately from readiness and reads interrupted-work counts without
  taking possession or changing recovery. (#2717)
- Operator setup docs now list native OpenAI Responses alongside the existing
  Anthropic API, Claude CLI, and OpenAI-compatible Chat Completions backends,
  including the required `OPENAI_API_KEY` credential example. Backend selection
  remains `--backend`, then `llm.backend`, then the default `anthropic`.
- Fork selects the live flow run; compatible live bundle upgrades are admitted
  at startup. Fixed-cut work retains its admitted source, and unsafe transitions
  refuse rather than discard work. (#2810, #2812)
- Flat `state.yaml` owns state declarations. Own-flow `writes` is a field list
  or scalar `all`; fieldless flows need no invented state row or state tool.
  State readers select the flow contract instead of a global entity type.
  This is the first grammar slice, not all of #2293. (#2807)
- Node handler keys declare inputs, emit sites determine outputs, guards are
  scalar CEL, and activities have explicit IDs. Timer and join identities are
  derived by their admitted owners. The separate gates/readiness/schema/criteria
  grammar slice is not included. (#2899)
- Reusable agent memory survives an admitted in-place upgrade; the next turn uses
  the new prompt/model, including resumed Claude CLI calls. Memory-off, authored
  rotation and explicit teardown retain their distinct behavior. (#2869)
- Stored API conversations retain ordered assistant tool calls and tool results.
  Required reasoning is retained within the current round and dropped when the
  turn ends. This does not change that retention decision. (#2867)
- Provider user turns contain the payload or admitted authored content. An
  authored projection can include `event.at` and `event.producer`; no participant
  name or request-level user field is added. Tool results are plain values, with
  relay retrieval instructions preserved under bounded rendering. (#2890)
- Generated whole-state reads return declared business fields without runtime
  metadata or the owning flow's instance-key field; a key-only state reads as
  `{}`. The operator-notice description has no appended usage paragraph.
  (#2890, #2912; completes #2910)
- API tool names use deterministic encoding plus exact admitted reverse lookup.
  Unknown/unoffered names, collisions and over-long names refuse rather than
  widening the canonical tool surface. (#2897)
- Graceful serve stop seals fresh work and drains accepted work and continuations
  before retiring their dependencies, within the existing grace period/deadline.
  Genuine expiry and incomplete cleanup remain visible. (#2895)

### Fixed

- Ordinary WhatsApp traffic during an unapproved linked-account preview no
  longer fences enrollment. Cancelling account or operator confirmation aborts
  without approving, rejecting or logging out the paired account; EOF retains
  the existing default-no behavior. Physical sitting qualification remains open.
  (#2577)
- WhatsApp first login works in both serve orders. Inbound replies retain the
  holder's exact source/root, restored replies drain before readiness, and resume
  waits for confirmed login. Boot output reports actual account readiness; early
  provider login-handoff ordering preserves pairing. Deterministic peer proof
  does not replace the open physical sitting. (#2847, #2854, #2857, #2863, #2873)
- Telegram connect/list keeps private admission evidence out of public JSON.
  Newly connected or newly held accounts can be picked up while serve runs.
  (#2858, #2862)
- OpenAI-compatible replay sends `content: ""` for an empty assistant reply.
  Request-refusing 4xx responses retain provider details; 408/429 remain retryable
  and no automatic redispatch is introduced. (#2861)
- Anthropic Messages sends native `tool_use` and linked `tool_result` blocks with
  their original IDs. ID-less human-task observations remain observation text
  per entry; linked results still refuse orphan references. (#2901)
- Interrupted read-only HTTP provider calls can resume during startup recovery
  through the existing retry budget and fresh authority. An immutable
  credential-free journal and replacement validation preserve provenance;
  uncertain writes and contradictory evidence remain conservative. (#2884)
- `swarm test` and serve read the same complete deployment LLM configuration,
  including OpenAI-compatible base URLs and Responses settings. (#2875)
- Signed GitHub deliveries with an authored normalized consumer retain admitted
  raw-output metadata instead of returning HTTP 503. Authentication, schema and
  duplicate checks remain strict; wholly unconsumed policy is unchanged. (#2879)
- Startup-recovery refusals name the boot step/native cause and retain
  reproducible configuration-bound diagnostics. Dev startup discloses its scratch
  store and the retained store it bypasses. Unsupported status hints are omitted;
  project-default inspection completion remains pending #2911. (#2906)
- Channel/serve store selection, credential-source readback, webhook route/schema
  diagnostics, model/cost provenance and terminal decision cards retain their
  shipped operator surfaces. (#2819, #2729, #2723, #2766, #2767, #2270, #2782)

### Removed

- Retired imports/ingress/manifest-name and manifest transport spellings have no
  aliases; directory identity and BODY transport declarations are canonical.
  The unsigned-webhook acknowledgement control is removed. (#2791)
- Use `operator_account` and `external_account`, not the old account-reference
  spellings. Permanent source-freeze surfaces are removed. (#2815, #2835)
- Old `entities.yaml`, entity-type selectors, node `execution_type`,
  `subscribes_to`, `produces`, `event_handlers`, guard registries and `on_fail`
  forms are retired. Use flat state declarations and direct handlers instead.
  (#2807, #2899)
- Per-field generated reads and runtime result envelopes are retired. External
  tool consumers must use the current whole-read business-field map and plain
  result values. (#2890, #2912)
- The `--abandon-active-runs` boot flag is removed. Use the separate explicit
  `swarm control stop --all` operation when that is intended. (#2895)

### Maintenance

- Routing cleanup removes copied identities, obsolete facades/ledgers and
  redundant projections while retaining admitted/native ownership and public
  behavior; it is not a new routing grant or a guaranteed speedup. (#2413 series)
- Long-lived test helpers and actual payload processes are bound to the outer
  test lifetime with checked stop/join. Renderer/grammar fixtures are current;
  production runner/lease policy and test bounds are unchanged.
  (#2894, #2908, #2909)

### Release Readiness

- S2 native commit-tail ordering (#2794) and served-live-run publication (#2881)
  are open v0.7 work, not shipped claims. #2911 is also pending. Refresh these
  notes after their actual landing and qualification.
- WhatsApp's #2577 physical sitting, independent operator/customer checks and
  next-day continuity remain open. Mocked peers and named earlier automated
  receipts are not physical or fresh-master acceptance. Deployment-level HITL
  Inbox/cards over registered bots remain deferred to v0.8 (#2836).
- Back up deployment state and credentials and review source/config changes.
  Retained memory does not promise an old-store compatibility reader or a
  qualified binary-only migration.
- Planned assets are native-CGO Linux amd64 and macOS arm64 archives plus
  `SHA256SUMS`. Linux requires compatible glibc/libm/loader/libgcc_s.so.1, not
  musl/Alpine/static; macOS is built/smoked on macOS 15, not certified on older
  versions. Windows, signing/notarization and Homebrew are outside this workflow.
- The final-master artifact-only rehearsal, exact asset sizes/checksums, tag and
  publication remain pending under #2852. The October 31 rehearsal proves only
  its own source, not the final release. No rc.2 or early release is authorized.

## [1.6.0] - 2026-06-01

First public open-source release. Engine is Phase 11 (handler-first execution for
the proven-safe subset; full handler-first execution in progress). Conformance
suite covers 200+ contract bundles across 12 tiers (primitives, accumulation,
atomic event-loop semantics, composition, boot verification, runtime fork,
policy patterns).

### Added

- `swarm fork <source-run-id> --at-event <event-id>` CLI: re-execute a run from
  any point in its history.
- `swarm forkchat` CLI: fork one agent conversation into a sandbox, optionally at
  a turn or event with an injected message.
- `swarm agent diagnose <id>` CLI: inspect why an agent is stuck (its
  pending-delivery queue in detail).
- `swarm bundle` CLI family: list, view, register, and delete persisted contract
  bundles by canonical hash.
- GitHub Discussions enabled for questions, flow sharing, and feature requests.

### Changed

- Default runtime store for local/dev runs is now file-backed SQLite at
  `.swarm/dev.db`; Postgres is opt-in via `--store postgres` or `store.backend`.
- LLM runtime names: `anthropic` (was `api`), `claude_cli` (was `cli_test`).
  Legacy aliases retained.
- `swarm event publish` (was `swarm publish`); `swarm mailbox` (was
  `swarm control mailbox`). Old forms removed.
- README trimmed to front-door scope (identity, design positions, project
  status). Tutorial content now lives at [docs.division.sh](https://docs.division.sh).

### Fixed

- SQLite store now accepts relative-path DSNs (e.g. `.swarm/dev.db`) without
  failing to open; the file URL serializer was emitting an unparseable form
  with leading-dot paths. (#1222)
- `swarm run` now accepts the `--data` flag (previously only `swarm serve`
  did); the workspace `/data` mount auto-creates at `.swarm/data/` when
  unset, so the runtime no longer requires `SWARM_WORKSPACE_DATA_SOURCE`
  for agent flows that do not read shared reference data. (#1223)

### Retired

- `SWARM_LLM_BACKEND` environment variable: rejected at boot. Backend is now
  selected via `--backend` on `swarm serve` / `swarm run`, or `llm.backend`
  in `config.yaml`.
- `model_tier` agent field: rejected at boot. Authored agents declare `model`
  with an alias (`cheap`, `regular`, or `frontier`); `llm.models` maps each
  alias to a concrete model per backend.
- Docker Compose: the bundled `docker-compose.yml` is no longer supported; the
  zero-service SQLite default replaces the compose-based local-dev path.

[Unreleased]: https://github.com/division-sh/swarm/compare/v0.6.0...HEAD
[1.6.0]: https://github.com/division-sh/swarm/releases/tag/v1.6.0
