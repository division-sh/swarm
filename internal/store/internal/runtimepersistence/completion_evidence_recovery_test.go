package runtimepersistence

import (
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestCompletion2270ForkChatUncertainRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"authorized", "launched", "response-observed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				fixture := newForkChatCompletionAuthorityFixture(t, backend == "sqlite")
				prepared := prepareForkChatCompletionGroup(t, fixture, "2270-recovery", "preserve the admitted request")
				ctx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "recovery")
				request := handle.Attempt().Authority.CompletionRequest
				if cut != "authorized" {
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if cut == "response-observed" {
					if err := handle.MarkResponseObserved(ctx, map[string]any{"model": "not-decoded-usage", "input_tokens": 42, "total_cost_usd": 3}); err != nil {
						t.Fatal(err)
					}
				}
				expireForkChatGroupLease(t, fixture, prepared.ForkTurnID)
				now := fixture.now.Add(10 * time.Minute)
				want := runtimeeffects.RecoverySummary{OutcomeUncertain: 1}
				children := 1
				if cut == "authorized" {
					want, children = runtimeeffects.RecoverySummary{PrelaunchTerminal: 1}, 0
				}
				if summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now)); err != nil || summary != want {
					t.Fatalf("recover %s: %+v, %v", cut, summary, err)
				}
				requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, children, 0)
				before := requireCompletion2270ForkRecoveryEvidence(t, fixture, request, children)
				if summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(now.Add(time.Second))); err != nil || summary != (runtimeeffects.RecoverySummary{}) {
					t.Fatalf("repeat recovery created another completion: %+v, %v", summary, err)
				}
				after := requireCompletion2270ForkRecoveryEvidence(t, fixture, request, children)
				if before != after {
					t.Fatal("repeat recovery changed frozen evidence")
				}
			})
		}
	}
}

func requireCompletion2270ForkRecoveryEvidence(t *testing.T, fixture forkChatCompletionAuthorityFixture, request runtimeeffects.CompletionRequest, children int) string {
	t.Helper()
	snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := DecodeSelectedForkSnapshotRowsForTest(snapshot["conversation_fork_turn_completions"])
	if err != nil || len(rows) != children {
		t.Fatalf("recovered child cardinality: %d, %v", len(rows), err)
	}
	if children == 0 {
		return ""
	}
	raw, err := rows[0]["completion_evidence"].Text()
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := runtimeeffects.DecodeCompletionEvidence([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := evidence.ValidateRequest(request, ""); err != nil {
		t.Fatal(err)
	}
	if len(evidence.Models) != 0 || evidence.ProviderReportedCostUSD != nil || evidence.CostProvenance != runtimeeffects.CompletionCostUnavailable {
		t.Fatalf("recovery invented decoded provider evidence: %+v", evidence)
	}
	exactness, err := rows[0]["usage_exactness"].Text()
	if err != nil || exactness != string(runtimeeffects.CompletionUsageUnavailable) {
		t.Fatalf("recovery fabricated exact usage: %q, %v", exactness, err)
	}
	for _, column := range []string{"input_tokens", "output_tokens", "provider_reported_cost_usd"} {
		if string(rows[0][column].Value) != "null" {
			t.Fatalf("recovery fabricated %s: %s", column, rows[0][column].Value)
		}
	}
	return raw
}
