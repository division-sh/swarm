# Catalog E2E Proof Tiers

This package owns the catalog E2E proof boundary for runtime fixture behavior.

Required PR proof is split across the `catalog-required-inventory`,
`catalog-required-verify`, and `catalog-required-smoke` units in
`.github/test-proof-plan.yaml`. Full PostgreSQL catalog conformance is proof
unit `catalog-full`. CI profiles select these units; this document does not own
a second executable command.

Manual diagnostic probe:

```sh
SWARM_CATALOG_E2E_DEBUG=1 go test ./internal/runtime/cataloge2e -run '^TestTier11Probe$' -count=1 -v
```

| Family / tier | Proof owner | Semantic proof | Tier |
|---|---|---|---|
| Required catalog inventory | `internal/testcatalog.TestCatalogRequiredInventory` | strict 155-row discovery, canonical claim coverage, disposition taxonomy, structural ownership guards | required PR proof |
| Required supported verify | `internal/cliapp.TestCatalogRequiredVerifyAll` | supported verification of every fixture plus exact teaching diagnostics and protocol-only companion accounting | required PR proof |
| Required catalog smoke | `TestCatalogRequiredSmoke` | startup policy, boot warning truth, assertion harness behavior, and one real PostgreSQL runtime fixture | required PR smoke |
| SQLite local smoke | `.github/workflows/ci.yml` `sqlite-local-dev` | no-selector CLI run on default SQLite using `examples/routing/root-ingress` | required PR smoke |
| Catalog assertion harness | `assertions_test.go`, `assertions_harness_test.go` | causal entity lookup, handler outcome recognition, emitted-event assertion rules | required PR smoke through `TestCatalogRequiredSmoke`; full conformance also runs the focused tests |
| Startup policy | `startup_policy_test.go` | strict/runtime catalog startup policy and warning-fixture authoritative startup truth | required PR smoke through `TestCatalogRequiredSmoke`; full conformance also runs the focused tests |
| Replay-clean ordinary runtime | `TestCatalogReplayClean_SelectedStores`, 94 fixtures across tiers 1, 3-7, and 9-11 | one immutable transcript, exact operation outcomes and fixture assertions, causal event/delivery/dead-letter bytes, real PostgreSQL/SQLite stores, and reopen stability | full conformance/manual/nightly; sole ordinary runtime executor |
| Replay-clean census | `TestCatalogReplayCleanCensus` | exact 98 = 94 + 2 selected-contract fork + 1 direct tool + 1 boot-only ownership, with structural exclusion from the replay primitive | required structural proof |
| Tier routing ownership | `TestTier*CanonicalRoutingOwnership` | direct canonical-routing registry checks only; no fixture execution or conformance credit | structural ownership proof |
| Tier 8 boot verification | `tier8_boot_e2e_test.go`, `tests/tier8-boot-verification` | bootverify/runtime startup agreement for success, warning, and error fixtures | full conformance/manual/nightly; targeted warning truth is included in required smoke |
| Tier 11 probe | `tier11_probe_test.go` | diagnostic output around Tier 11 fixtures | no conformance credit; manual debug only |
| Tier 12 runtime fork | `tier12_runtime_fork_e2e_test.go`, `tests/tier12-runtime-fork` | selected-contract fork execution, source isolation, fork-local runtime materialization, and non-agent replay fail-closed behavior | touched-surface proof for run-fork changes; included in full conformance |
| Tier 12 runtime tools | `tier12_runtime_tools_e2e_test.go`, `tests/tier12-runtime-tools` | flow data access tool exposure, allowlist enforcement, traversal rejection, and undeclared actor fail-closed behavior | touched-surface proof for runtime tool/flow-data-access changes; included in full conformance |

Every fixture is owned exactly once by fixture-local `conformance` metadata and
the strict inventory. Retired fixtures name a current reason and replacement,
receive no active claim credit, and have no per-tier compatibility selector.
