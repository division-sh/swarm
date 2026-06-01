# Catalog E2E Proof Tiers

This package owns the catalog E2E proof boundary for runtime fixture behavior.

Required PR smoke:

```sh
go test ./internal/runtime/cataloge2e -run '^TestCatalogRequiredSmoke$' -count=1
```

Full catalog conformance:

```sh
go test ./internal/runtime/cataloge2e -count=1
```

Manual diagnostic probe:

```sh
SWARM_CATALOG_E2E_DEBUG=1 go test ./internal/runtime/cataloge2e -run '^TestTier11Probe$' -count=1 -v
```

| Family / tier | Proof owner | Semantic proof | Tier |
|---|---|---|---|
| Required catalog smoke | `TestCatalogRequiredSmoke` | startup policy, boot warning truth, assertion harness behavior, and one Tier 1 runtime fixture | required PR smoke |
| SQLite local smoke | `.github/workflows/ci.yml` `sqlite-local-dev` | no-selector CLI run on default SQLite using `tests/tier1-primitives/test-emits-single` | required PR smoke |
| Catalog assertion harness | `assertions_test.go`, `assertions_harness_test.go` | causal entity lookup, handler outcome recognition, emitted-event assertion rules | required PR smoke through `TestCatalogRequiredSmoke`; full conformance also runs the focused tests |
| Startup policy | `startup_policy_test.go` | strict/runtime catalog startup policy and warning-fixture authoritative startup truth | required PR smoke through `TestCatalogRequiredSmoke`; full conformance also runs the focused tests |
| Tier 1 primitives | `tier1_primitives_e2e_test.go`, `tests/tier1-primitives` | primitive emits, advances, guards, gates, rules, evidence, payload transforms, data accumulation, and on-complete behavior | full conformance/manual/nightly; `test-emits-single` is the smoke representative |
| Tier 2 accumulation | `tier2_accumulation_e2e_test.go`, `tests/tier2-accumulation` | accumulator all/partial/threshold/timeout/idempotency/crash-recovery/on-complete rollback behavior | full conformance/manual/nightly |
| Tier 3 list processing | `tier3_list_processing_e2e_test.go`, `tests/tier3-list-processing` | fan-out, filter, group, and reduce behavior | full conformance/manual/nightly |
| Tier 4 cross entity | `tier4_cross_entity_e2e_test.go`, `tests/tier4-cross-entity` | query/filter/group and cross-entity state clearing | full conformance/manual/nightly |
| Tier 5 lifecycle | `tier5_lifecycle_e2e_test.go`, `tests/tier5-flow-lifecycle` | auto emit, terminal state behavior, timers, templates, and wildcard subscription behavior | full conformance/manual/nightly; touched-surface proof for lifecycle/timer changes |
| Tier 6 event loop | `tier6_event_loop_e2e_test.go`, `tests/tier6-event-loop` | event atomicity, rollback, chain depth, concurrency, dead letters, validation, and persisted-before-delivery behavior | full conformance/manual/nightly; touched-surface proof for event-loop/pipeline changes |
| Tier 7 composition | `tier7_composition_e2e_test.go`, `tests/tier7-composition` | cross-flow subscription, dual delivery, lifecycle, chain, and wildcard composition behavior | full conformance/manual/nightly |
| Tier 8 boot verification | `tier8_boot_e2e_test.go`, `tests/tier8-boot-verification` | bootverify/runtime startup agreement for success, warning, and error fixtures | full conformance/manual/nightly; targeted warning truth is included in required smoke |
| Tier 9 composition patterns | `tier9_composition_patterns_e2e_test.go`, `tests/tier9-composition-patterns` | accumulate/compute/branch, gate chains, guard/query/capacity, rules fanout/data, counter escalation, lifecycle patterns | full conformance/manual/nightly |
| Tier 10 policy patterns | `tier10_policy_patterns_e2e_test.go`, `tests/tier10-policy-patterns` | policy capacity, counters, hard gates, multi-guard partials, threshold, and timeout behavior | full conformance/manual/nightly; touched-surface proof for policy changes |
| Tier 11 flow composition | `tier11_flow_composition_e2e_test.go`, `tests/tier11-flow-composition` | child flow loading/events, nesting, pin wiring, policy/tool inheritance, child gates, required agents, sibling isolation, and subject ID flow behavior | full conformance/manual/nightly |
| Tier 11 probe | `tier11_probe_test.go` | diagnostic output around Tier 11 fixtures | obsolete/duplicate as required proof; replacement is `TestTier11FlowCompositionCatalogFixtures_RealRuntime` plus `TestTier11FlowCompositionCatalogFixtures_AreExplicitlyClassified`; manual debug only |
| Tier 12 runtime fork | `tier12_runtime_fork_e2e_test.go`, `tests/tier12-runtime-fork` | selected-contract fork execution, source isolation, fork-local runtime materialization, and non-agent replay fail-closed behavior | touched-surface proof for run-fork changes; included in full conformance |
| Tier 12 runtime tools | `tier12_runtime_tools_e2e_test.go`, `tests/tier12-runtime-tools` | flow data access tool exposure, allowlist enforcement, traversal rejection, and undeclared actor fail-closed behavior | touched-surface proof for runtime tool/flow-data-access changes; included in full conformance |

Excluded legacy fixtures remain owned by their per-tier exclusion maps. Do not delete or migrate them as part of catalog tiering unless the replacement issue is explicitly in scope.
