# Changelog

All notable changes to Division Swarm are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Division Swarm is pre-1.0; the public surface may change between minor versions
until v1.0. Each entry below lists the platform-spec version it ships against.

## [Unreleased]

### Changed

- Operator setup docs now list native OpenAI Responses alongside the existing
  Anthropic API, Claude CLI, and OpenAI-compatible Chat Completions backends,
  including the required `OPENAI_API_KEY` credential example. Backend selection
  remains `--backend`, then `llm.backend`, then the default `anthropic`.

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

[Unreleased]: https://github.com/division-sh/swarm/compare/v1.6.0...HEAD
[1.6.0]: https://github.com/division-sh/swarm/releases/tag/v1.6.0
