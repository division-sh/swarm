# Public Docs Disposition

This file records the public/private disposition for root docs, committed docs,
GitHub issue templates, generated/archive docs artifacts, and future local docs
artifact policy.

The root `platform-spec.yaml` remains the authoritative platform
specification. The root `openrpc.json` remains the generated public API
artifact. Rendered or historical docs under `docs/` are not competing semantic
owners unless a future tracked spec change promotes them.

## Root Public Surfaces

| Path | Disposition |
| --- | --- |
| `README.md` | Public landing page, quickstart, docs table, and project status. Runtime-onboarding semantics remain split to their own trackers. |
| `LICENSE` | Public Apache-2.0 license text. |
| `CONTRIBUTING.md` | Public contributor workflow and semantic-change process pointer. |
| `SECURITY.md` | Public vulnerability reporting policy. |
| `.env.example` | Public local environment template. Secrets and backend semantics remain owned by runtime/config docs. |
| `platform-spec.yaml` | Authoritative platform specification. |
| `openrpc.json` | Generated public API artifact. |

## GitHub Public Intake Surfaces

| Path | Disposition |
| --- | --- |
| `.github/ISSUE_TEMPLATE/config.yml` | Public issue-template contact links. Points to public contributor/security/root docs, not private maintainer process docs. |
| `.github/ISSUE_TEMPLATE/feature-request.yml` | Public feature request intake template. Retained with generic backlog wording. |
| `.github/ISSUE_TEMPLATE/runtime-bug.yml` | Public runtime bug intake template. Links public contributor, bug-intake, and security docs. |
| `.github/ISSUE_TEMPLATE/runtime-improvement.yml` | Public runtime improvement intake template. Links public contributor docs and root spec authority. |

## Retained Public Or Maintainer Docs

| Path | Disposition |
| --- | --- |
| `docs/AUDITOR_PROTOCOL.md` | Public maintainer process doc for audit-only work. Absolute local path removed. |
| `docs/BUG_INTAKE_AND_REPRO.md` | Public maintainer process doc for bug classification. |
| `docs/EVENT_IDENTITY_AND_ROUTING_IMPLEMENTATION_PLAN.md` | Historical/maintainer implementation-plan record. Retained with repo-relative links. |
| `docs/FAIL_FAST_AUDIT.md` | Public maintainer audit guidance. |
| `docs/FLOW_INSTANCE_IDENTITY_IMPLEMENTATION_PLAN.md` | Historical/maintainer implementation-plan record. |
| `docs/IMPLEMENTER_GUIDELINES.md` | Public maintainer process doc for semantic/runtime work. Repo-relative links only. |
| `docs/IMPLEMENTER_REVIEW_CHECKLIST.md` | Public maintainer review checklist. Repo-relative links only. |
| `docs/PROCESS_CHECKLIST_TEMPLATES.md` | Public maintainer checklist templates. Repo-relative links only. |
| `docs/PROMPT_TEMPLATES.md` | Public maintainer prompt/checklist templates. Repo-relative links only. |
| `docs/PUBLIC_DOCS_DISPOSITION.md` | Public docs disposition manifest. |
| `docs/RUNTIME_IMPROVEMENTS_AND_WATCHLIST.md` | Public maintainer watchlist narrative. Repo-relative links only. |
| `docs/RUNTIME_LOGGING_BOUNDARY.md` | Public maintainer runtime logging boundary notes. |
| `docs/SEMANTIC_DRIFT.md` | Public maintainer semantic ownership guidance. |
| `docs/SWARM_INVESTIGATE_COMMAND_DRAFT.md` | Historical superseded CLI draft retained because `platform-spec.yaml#cli_specification.source_of_truth.supersedes` and spec tests still reference it. It is not authoritative. |
| `docs/TOOL_INVOCATION_UNIFICATION_PLAN.md` | Historical/maintainer implementation-plan record. |
| `docs/TRACE_ID_REMOVAL_PLAN.md` | Historical/maintainer implementation-plan record. |
| `docs/TURN_TRANSCRIPT_PLAN.md` | Historical/maintainer implementation-plan record. |
| `docs/specs/swarm-platform/SWARM-DEVELOPER-GUIDE.md` | Public flow-authoring guide. |
| `docs/specs/swarm-platform/SWARM-PLATFORM-BRIEF.md` | Public platform overview. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.43.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.44.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.45.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.46.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.47.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.48.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.49.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.0.50.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.1.0.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.2.0.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.2.1.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.2.2.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.3.0.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.4.0.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.5.0.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/CHANGELOG-v2.6.0.md` | Historical public spec changelog. |
| `docs/specs/swarm-platform/docs/bundle-canonicalization-v1.md` | Public/historical bundle canonicalization note. |
| `docs/specs/swarm-platform/platform/BUILDER-API.md` | Public Builder API guide. |
| `docs/specs/swarm-platform/platform/CHANGELOG.md` | Public platform changelog. |
| `docs/specs/swarm-platform/platform/FLIGHT-RECORDER.md` | Public trace/debugging guide. |
| `docs/specs/swarm-platform/platform/platform-guide.md` | Public rendered platform guide; non-authoritative next to root `platform-spec.yaml`. |
| `docs/specs/swarm-platform/platform/platform-spec.md` | Public rendered platform spec; non-authoritative next to root `platform-spec.yaml`. |
| `docs/specs/swarm-platform/platform/platform-spec.print.css` | Support file for rendered platform spec output. |
| `docs/specs/swarm-platform/platform/review/platform-spec.run-trace-surface.yaml` | Tracked review-spec artifact explicitly called out by current spec authority context; not a merge-bearing spec owner. |
| `docs/specs/swarm-platform/tests/TEST-CATALOG.md` | Public conformance fixture catalog. |
| `docs/specs/swarm-platform/tests/fixtures/happy-path.yaml` | Public conformance fixture. |
| `docs/specs/swarm-platform/tests/test-accumulate-all/*` | Public conformance fixture bundle. |
| `docs/specs/swarm-platform/tests/test-guard-discard/*` | Public conformance fixture bundle. |
| `docs/specs/swarm-platform/tests/test-guard-pass/*` | Public conformance fixture bundle. |
| `docs/specs/swarm-platform/verify.py` | Public reference verifier helper for the spec docs. |
| `docs/watchlists/README.md` | Public maintainer watchlist index. Repo-relative links only. |
| `docs/watchlists/maintenance-and-cleanup.yaml` | Public maintainer maintenance watchlist. Refined for #1162. |
| `docs/watchlists/operator-surfaces.yaml` | Public maintainer operator-surface watchlist. |
| `docs/watchlists/runtime-operations.yaml` | Public maintainer runtime-operations watchlist. |
| `docs/watchlists/semantic-correctness.yaml` | Public maintainer semantic-correctness watchlist. |

## Removed Private, Local, Or Generated Artifacts

| Path | Disposition |
| --- | --- |
| `docs/AUTONOMOUS_WORK_PROTOCOL.md` | Removed private lead/agent control-bus process doc. |
| `docs/COLLABORATION_WORKFLOW.md` | Removed private multi-worktree collaboration workflow doc. |
| `docs/EMPIRE_PRODUCT_COMPATIBILITY_OWNER.md` | Removed private product-specific compatibility owner doc. |
| `docs/EPO_INVESTIGATION_SURFACES.md` | Removed private product-specific investigation doc with local paths. |
| `docs/ISSUE_BOARD_DEPENDENCY_MATRIX_2026-04-15.md` | Removed stale issue-board matrix artifact. |
| `docs/LEAD_HANDOFF_2026-04-06.md` | Removed private handoff note. |
| `docs/POTENTIAL_ISSUES.md` | Removed stale internal backlog scratchpad. |
| `docs/drafts/excluded/ARCHITECTURE_PROPOSAL_URGENCY_DRAFT.md` | Removed excluded draft artifact. |
| `docs/specs/mas-platform-v1.2.0-2.tar` | Removed generated/archive tarball. |
| `docs/specs/swarm-platform/IMPLEMENTER-HANDOFF.md` | Removed spec-side implementer handoff guide. |
| `docs/specs/swarm-platform/platform/PROMPT-test-writer.md` | Removed private prompt artifact. |
| `docs/specs/swarm-platform/platform/platform-guide.pdf` | Removed generated PDF artifact. |
| `docs/specs/swarm-platform/platform/platform-spec.pdf` | Removed generated PDF artifact. |
| `docs/templates/CONTROL.example.yaml` | Removed private local control-bus template. |
| `docs/templates/RESPONSE.example.yaml` | Removed private local control-bus template. |

## Explicit Splits

| Surface | Tracker |
| --- | --- |
| Docker Compose/Postgres onboarding and `SWARM_CONTRACTS_HOST_DIR` semantics | #1137 |
| Host workspace backend semantics | #1138 |
| Explicit `/data` source semantics | #1139 |
| LLM backend/config/secrets/model-alias docs cleanup beyond current public template wiring | #1130 |
| Retired private Makefile/scripts surfaces | #1161, closed by PR #1166 |

## Future Local Docs Artifact Policy

The repository must not ignore `docs/` wholesale. Public specs, guides,
maintainer docs, watchlists, and conformance fixtures remain committed.

Future local-only docs artifacts should use targeted paths or extensions:

| Pattern | Policy |
| --- | --- |
| `docs/local/` | Ignored local-only scratch docs. |
| `docs/**/*.local.md` | Ignored local-only markdown notes. |
| `docs/**/*.draft.md` | Ignored local-only draft markdown. |
| `docs/**/*.pdf` | Ignored generated rendered docs. |
| `docs/**/*.tar` | Ignored archive artifacts. |
