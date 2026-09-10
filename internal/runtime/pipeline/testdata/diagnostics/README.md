# Authored Rule Receiver Retry Diagnostic

This preserves a live failure separately from the original receiver-preparation
fixture restoration. The ordinary receiver tests keep their assertions and
direct unconditional advancement. Their old raw rule produced no authored
selection fact; converting it to YAML changed that coverage. That regression
repair does not establish that authored-rule retries are correct.

`authored_rule_receiver_retry_test.go` is an explicit, expected-red diagnostic.
It lives under testdata so it does not silently change the normal package suite.
It asserts the desired successful retry, not that the conflict is correct.
Use a Go overlay to add it to package pipeline without changing production or
baseline files. Both current and pre-retirement runs use this exact same file.

## Source And Controls

The fixture is loaded through the real YAML contract loader. It declares root
stages queued/done, event source.evt, and node-a with an unconditional authored
rule complete (`condition: 'true'`, `advances_to: done`). The diagnostic asserts
that the rule is authored and has its canonical declaration identity:

`./handler_rule/nodes["node-a"].handlers["source.evt"].rules[0]`

On both SQLite and PostgreSQL, the no-fault control delivers successfully,
persists the exact selected rule fact, and changes the workflow stage to done.
The two fault variants differ only in whether the first attempt starts through
fresh delivery admission or an already-acquired recovered claim.

## Observed Failure

1. An initial delivery has no selection row. A dependency-unavailable receiver
   read failure occurs inside preparation, before handler/rule execution.
2. The runtime accepts retry: handled=true, no returned error, status=failed,
   retry_count=1, a nonzero next-eligible time, a retained continuation, and a
   durable retry_scheduled outcome. The workflow remains queued.
3. Failure settlement has nevertheless stored context=none and
   disposition=not_applicable as the immutable per-delivery selection fact.
4. The test clears only the injected read failure and advances retry eligibility
   using the same helper as the original test. It reuses the exact event, route,
   recipient, and delivery. Runtime admission acquires claim version 2.
5. The retry attempts the exact authored selected fact. This is observed on the
   real failure-settlement call after the engine mutation cannot settle. The
   selected fact equals the source rule identity, context, and display label.
6. Settlement fails with `delivery handler rule selection contradicts the
   canonical fact`. The stored fact remains not_applicable, the workflow remains
   queued, and delivery remains in_progress with retry_count=1. The only durable
   outcome remains retry_scheduled.

Retry is expressly permitted by the original
TestReceiverPreparationFailureClaimMatrixBothStores: transient failures must
retain continuation and later reach delivered against the same receiver.

## Owner Boundary For Lead Classification

In pipeline/coordinator.go, prepareDeliveryTargetApplication failure returns an
empty contractHandlerExecutionResult. The retry failure-settlement path passes
admittedHandlerRuleSelection(result.RuleSelection), converting absent selection
to NotApplicable before the handler has run.

In store/internal/backend/delivery/adapter.go, settle persists the rule selection
before choosing retry_scheduled. persistHandlerRuleSelection keys the fact only
by delivery_id, inserts with ON CONFLICT DO NOTHING, then rejects unequal facts.
Thus attempt-local absence is frozen as whole-delivery negative evidence even
though the same delivery is explicitly allowed to execute later.

Suggested classification for lead review: premature finalization of per-delivery
selection evidence on retryable pre-execution failure. This is not a claim of
broader closure or permission to patch either owner. No receiver/store production
changes are included.

## Reproduction

Current overlay: /tmp/2415-authored-rule-retry-current-overlay.json
Pre-retirement overlay: /tmp/2415-authored-rule-retry-baseline-overlay.json
Clean-master 032 overlay: /tmp/2415-authored-rule-retry-master032-overlay.json

Run from the corresponding worktree:

```sh
go test -overlay /tmp/2415-authored-rule-retry-current-overlay.json ./internal/runtime/pipeline -run '^TestAuthoredRuleReceiverPreparationRetryDiagnosticBothStores$' -count=3 -v
```

Use the baseline overlay from /Users/youmew/dev/swarm/worktrees/agent-a-2419,
identified by the parent as #2419 head 43af, not clean master 032. Its unrelated
SQL change is not overlaid onto #2415. The exact clean-master 032 run uses
/tmp/swarm-2415-current-master-probes and the master032 overlay. Head labels are
parent-provided; no git commands were used. Both comparators still have
WorkflowDefinition. No baseline/master files were edited.
The delivery backend adapter is byte-identical across all three worktrees; its
SHA-256 is e53a43a1651e0e0eeb6b5dc443670b724c32b1b5a84cf4c9be0bf82ebd38a69e.
The coordinator delivery-execution/settlement path is unchanged; its only file
difference is the unrelated previewState field. The diagnostic source used by
all three overlays has SHA-256
b1dacdee9159773d3c9786c6a30dc809826326bf697ee710922e4d8c440d2eab.

Receipts:

- /tmp/2415-authored-rule-retry-current.log: RED 1.079s; both controls pass and all four retry cases fail at the settlement conflict.
- /tmp/2415-authored-rule-retry-baseline.log: RED 1.032s; identical control/failure results with the same authored fixture.
- /tmp/2415-authored-rule-retry-current-count3.log: current #2415 RED count3, 5.605s.
- /tmp/2415-authored-rule-retry-baseline-count3.log: #2419 head 43af RED count3, 3.930s.
- /tmp/2415-authored-rule-retry-master032-count3.log: clean master 032 RED count3, 2.406s.

Each count3 receipt has six passing no-fault subtest executions and twelve
failing retry subtest executions. All final receipts compiled and reached the
actual settlement conflict. There is no compilation failure among these proof
receipts; a compilation failure would not demonstrate this runtime defect.

## Production References

Paths below are relative to the repository. The clean-master lines are stable
for lead classification; current #2415 equivalents are also listed.

| Role | Clean Master 032 | Current #2415 |
| --- | --- | --- |
| Preparation error returns empty execution result before handler invocation | internal/runtime/pipeline/coordinator.go:714 | internal/runtime/pipeline/coordinator.go:715 |
| Retry settlement consumes absent result selection | internal/runtime/pipeline/coordinator.go:802 | internal/runtime/pipeline/coordinator.go:803 |
| Empty selection becomes NotApplicable | internal/runtime/pipeline/engine_bridge.go:581 | internal/runtime/pipeline/engine_bridge.go:583 |
| Actual authored selected-fact producer | internal/runtime/engine/executor.go:3045 | internal/runtime/engine/executor.go:3051 |
| Selected fact assigned to execution result | internal/runtime/engine/executor.go:3049 | internal/runtime/engine/executor.go:3055 |
| Settlement persists selection before choosing retry outcome | internal/store/internal/backend/delivery/adapter.go:1102 | same |
| Immutable delivery_id-only insert | internal/store/internal/backend/delivery/adapter.go:1196 | same |
| Unequal-fact rejection | internal/store/internal/backend/delivery/adapter.go:1226 | same |

The current engine adapter passes mutation.HandlerRuleSelection into its
atomic delivery-success plan at internal/runtime/pipeline/engine_adapter.go:371;
beginWorkflowEngineDeliverySuccess preserves it at engine_adapter.go:107. The
diagnostic also observes the retry's exact selected fact at the subsequent
failure-settlement boundary after that engine mutation fails to settle.

Work stops at this diagnostic receipt. The parent owns the lead scope
amendment/split request; no production remedy or new scope is self-authorized.

The normal pipeline suite, without this explicitly overlaid diagnostic, passes
in 84.438s (/tmp/2415-pipeline-fixture-resume-4.log). That result does not resolve
or remove this authored-rule failure.
