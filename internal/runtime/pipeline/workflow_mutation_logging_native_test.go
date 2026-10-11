package pipeline

import "testing"

func VerifyQ10NativeFieldlessNoStagePrivateHistoryForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml": "name: q10-fieldless-no-stage\n",
		"events.yaml": "observe:\n",
		"nodes.yaml":  "observer:\n  execution_type: system_node\n  event_handlers:\n    observe: {}\n",
	})
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, ".")
			instance.Gates = map[string]bool{"ready": true}
			instance.Bookkeeping = map[string]any{"authority": "header"}
			instance.StateBuckets = map[string]any{"count": int64(1)}
			if err := fixture.Construct(ctx, instance); err != nil {
				t.Fatal(err)
			}
			physical, err := fixture.PhysicalCounts(ctx)
			if err != nil || physical.EntityStates != 0 || physical.ConstructedHeaders != 1 {
				t.Fatalf("fieldless construction must retain one real header without a data row: %+v err=%v", physical, err)
			}
			current, records, err := fixture.TrackedMutationProjection(ctx, runtimeRunID(ctx), instance.EntityID)
			if err != nil {
				t.Fatal(err)
			}
			if current.CurrentState == nil || *current.CurrentState != "pending" || len(current.Fields) != 0 ||
				current.Gates["ready"] != true || current.Bookkeeping["authority"] != "header" || mustCanonicalJSON(t, current.Accumulator["count"]) != "1" {
				t.Fatalf("fieldless no-stage native construction lost internal/private facts: %+v", current)
			}
			if len(records) == 0 {
				t.Fatal("constructed no-stage member has no native mutation history")
			}
			assertNativeTrackedMutationProjectionForTest(t, fixture, ctx, instance.EntityID,
				"lifecycle_state:", "gate:ready", "bookkeeping:authority", "accumulator:count")
		})
	}
}
