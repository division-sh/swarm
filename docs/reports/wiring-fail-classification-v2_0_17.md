# Wiring Verifier FAIL Classification (Spec v2.0.17)

Generated from:

- `go test ./internal/runtime -run TestSpecRuntimeWiringVerification -v`
- FAIL lines extracted from `/tmp/wiring_fail_report.log`

## Summary

- Total FAIL items: **46**
- **Spec gaps:** **0**
- **Implementation gaps:** **46**
  - Runtime/config/prompt implementation gaps: **22**
  - Verifier implementation gaps (false positives from scanner/heuristics/scope): **24**

## Detailed Classification

| # | FAIL item | Gap type | Classification note |
|---|---|---|---|
| 1 | empire-coordinator prompt references emit_portfolio_digest_compiled but no EventSchemaRegistry entry exists | Implementation gap (runtime schema) | Missing explicit schema in `internal/runtime/event_emit_tools.go`. |
| 2 | empire-coordinator prompt references emit_vertical_health_warning but no EventSchemaRegistry entry exists | Implementation gap (runtime schema) | Missing explicit schema in `internal/runtime/event_emit_tools.go`. |
| 3 | discovery-coordinator subscribes to synthesis.needed but no producer (agent/runtime) emits it | Implementation gap (runtime flow) | Spec defines runtime emission; runtime currently emits `dedup.ambiguous` but not `synthesis.needed`. |
| 4 | empire-coordinator subscribes to system.started but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced in CLI init flow (`cmd/empire/init.go`), outside current verifier runtime-only producer scan. |
| 5 | empire-coordinator subscribes to system.directive but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Human/CLI and directive forwarding paths emit this, but verifier producer scan misses those sources. |
| 6 | empire-coordinator subscribes to vertical.approved but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced by mailbox decision side effects in `cmd/empire/main.go`. |
| 7 | empire-coordinator subscribes to template.version_published but prompt has no handling instructions | Implementation gap (prompt clarity) | Prompt has migration section but not explicit per-event handling block for this subscription. |
| 8 | empire-coordinator subscribes to template.migration_approved but no producer (agent/runtime) emits it | Implementation gap (mailbox side-effect wiring) | No mailbox side-effect emission for template migration approval in `cmd/empire/main.go`. |
| 9 | empire-coordinator subscribes to template.migration_completed but only self-emitted events were found | Implementation gap (subscription design) | Subscription appears redundant/misaligned; event should primarily notify other consumers. |
| 10 | empire-coordinator subscribes to template.migration_completed but prompt has no handling instructions | Implementation gap (prompt/config mismatch) | Subscribed event is not explicitly handled in prompt. |
| 11 | empire-coordinator subscribes to template.migration_failed but only self-emitted events were found | Implementation gap (subscription design) | Same self-loop issue as migration_completed. |
| 12 | empire-coordinator subscribes to template.migration_failed but prompt has no handling instructions | Implementation gap (prompt/config mismatch) | Subscribed event is not explicitly handled in prompt. |
| 13 | empire-coordinator subscribes to opco.launched but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced by OpCo roles (template roles), not loaded from roster in current verifier producer index. |
| 14 | empire-coordinator subscribes to opco.ceo_report but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Same template-role producer visibility issue. |
| 15 | empire-coordinator subscribes to opco.steady_state_reached but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Same template-role producer visibility issue. |
| 16 | empire-coordinator subscribes to opco.teardown_complete but no producer (agent/runtime) emits it | Implementation gap (runtime flow) | No concrete runtime emitter found for `opco.teardown_complete`. |
| 17 | empire-coordinator subscribes to human_task.expired but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Emitted in human-task expiry flow under `cmd/empire/human_tasks.go`. |
| 18 | empire-coordinator subscribes to budget.threshold_crossed but no producer (agent/runtime) emits it | Implementation gap (verifier parser) | Event is emitted in `internal/runtime/budget.go`; current AST extractor misses variable-based publish payloads. |
| 19 | empire-coordinator subscribes to timer.portfolio_digest but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Timer emission is orchestrated from CLI monitor wiring, not currently indexed by verifier runtime scan. |
| 20 | empire-coordinator subscribes to timer.marginal_review but no producer (agent/runtime) emits it | Implementation gap (runtime scheduling) | No concrete emitter found for this timer event. |
| 21 | factory-cto subscribes to template.publish_requested but no producer (agent/runtime) emits it | Implementation gap (runtime/CLI wiring) | Subscription exists, but no publish path currently emits this event. |
| 22 | factory-cto subscribes to analyst.anti_pattern_advisory but prompt has no handling instructions | Implementation gap (prompt coverage) | Prompt handles other analyst events but not anti-pattern advisory explicitly. |
| 23 | factory-cto subscribes to opco.*.steady_state_reached but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | OpCo template producers not included in current producer index. |
| 24 | factory-cto subscribes to opco.*.cto_escalation but no producer (agent/runtime) emits it | Implementation gap (event wiring) | No matching producer event currently wired for this pattern. |
| 25 | holding-devops subscribes to devops.deploy_requested but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced by template role `devops-agent`; verifier currently roster-only for agent producers. |
| 26 | holding-devops subscribes to devops.health_check_failed but only self-emitted events were found | Implementation gap (subscription design) | Current producer map only finds holding-devops self emission for this event. |
| 27 | holding-devops subscribes to devops.health_check_failed but prompt has no handling instructions | Implementation gap (prompt coverage) | Prompt does not define explicit handler behavior for inbound health_check_failed events. |
| 28 | holding-devops subscribes to devops.rollback_requested but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced by template role `devops-agent`; not currently indexed by verifier from roster. |
| 29 | holding-devops subscribes to devops.rollback_failed but only self-emitted events were found | Implementation gap (subscription design) | Self-loop pattern; no clear upstream non-self producer in current wiring map. |
| 30 | holding-devops subscribes to devops.rollback_failed but prompt has no handling instructions | Implementation gap (prompt coverage) | Prompt describes rollback execution, not explicit response to rollback_failed subscription. |
| 31 | holding-devops subscribes to spend.approved but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced by mailbox decision side effects in CLI path. |
| 32 | holding-devops subscribes to spend.rejected but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Produced by mailbox decision side effects in CLI path. |
| 33 | holding-devops subscribes to timer.infra_health_check but no producer (agent/runtime) emits it | Implementation gap (runtime scheduling) | No concrete emitter found for this timer event. |
| 34 | market-research-agent subscribes to market_research.scan_assigned but prompt has no handling instructions | Implementation gap (prompt explicitness) | Prompt is behaviorally aligned but lacks explicit event-labeled handler section. |
| 35 | operations-analyst subscribes to opco.*.ceo_report but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | OpCo producer roles not included in current producer index. |
| 36 | operations-analyst subscribes to opco.*.steady_state_reached but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | OpCo producer roles not included in current producer index. |
| 37 | operations-analyst subscribes to mailbox.* but no producer (agent/runtime) emits it | Implementation gap (verifier scope) | Mailbox events are emitted in CLI decision flow, outside current producer scan scope. |
| 38 | operations-analyst subscribes to budget.* but prompt has no handling instructions | Implementation gap (prompt coverage) | Prompt output sections do not include explicit `budget.*` event handling guidance. |
| 39 | scanner-agent subscribes to scanner.*.scan_assigned but prompt has no handling instructions | Implementation gap (verifier heuristic) | Prompt references `scanner.{type}.scan_assigned`; matcher currently misses this equivalent form. |
| 40 | spec-auditor subscribes to spec.validation_requested but prompt has no handling instructions | Implementation gap (prompt explicitness) | Prompt is checklist-heavy but lacks explicit event-name handler label. |
| 41 | spec-reviewer subscribes to spec_review.requested but prompt has no handling instructions | Implementation gap (prompt explicitness) | Prompt says “YOU RECEIVE” but does not name subscription event explicitly. |
| 42 | trend-research-agent subscribes to trend_research.scan_assigned but prompt has no handling instructions | Implementation gap (prompt explicitness) | Prompt implies assignment handling but does not explicitly label event handler section. |
| 43 | analysis-agent expects field name from scoring.requested but schema has no such property | Implementation gap (verifier heuristic) | Heuristic over-matches generic `name`; schema correctly uses `vertical_name`. |
| 44 | analysis-agent expects field scoring from scoring.requested but schema has no such property | Implementation gap (verifier heuristic) | Heuristic over-matches “scoring” vocabulary in narrative text. |
| 45 | factory-cto expects field spec from spec.validation_passed but schema has no such property | Implementation gap (verifier heuristic) | Heuristic infers `spec` from wording; schema intentionally centers verdict/issues. |
| 46 | factory-cto expects field spec from spec.validation_failed but schema has no such property | Implementation gap (verifier heuristic) | Same heuristic overreach as previous item. |

## Interpretation

- **No direct spec contradictions detected** in this FAIL set.
- The FAILs are predominantly:
  - Missing runtime/CLI wiring for specific events (real implementation gaps), or
  - Verifier limitations (producer discovery scope + prompt-field heuristic precision).

## Priority Fix Order

1. Real runtime/CLI wiring gaps: #1, #2, #3, #8, #16, #20, #21, #24, #33.
2. Prompt/config mismatches: #7, #10, #12, #22, #27, #30, #34, #38, #40, #41, #42.
3. Verifier hardening (reduce false positives): #4, #5, #6, #13, #14, #15, #17, #18, #19, #23, #25, #28, #31, #32, #35, #36, #37, #39, #43, #44, #45, #46.
