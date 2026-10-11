package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/google/uuid"
)

func nativeMutationLoggingFixture(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) (WorkflowActivityNativeFixtureForTest, context.Context) {
	t.Helper()
	fixture := open(t)
	ctx := runtimeeffects.WithExecutionMode(correlation.WithRunID(fixture.Context, testPipelineRunID), runtimeeffects.ExecutionModeLive)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	return fixture, ctx
}

func VerifyQ10NativeFieldlessNoStagePrivateHistoryForTest(t *testing.T, open func(*testing.T) WorkflowActivityNativeFixtureForTest) {
	f, ctx := nativeMutationLoggingFixture(t, open)
	entity := uuid.NewString()
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: "fieldless", EntityID: entity,
		WorkflowName: "fieldless", WorkflowVersion: "1", CurrentState: "pending", StageDefined: false,
		Gates: map[string]bool{"ready": true}, Bookkeeping: map[string]any{"authority": "header"},
		StateBuckets: map[string]any{"count": int64(1)}})
	if err := f.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	evidence := requireNativeMutationProjection(t, f, ctx, entity)
	current := evidence.Current
	if current.CurrentState == nil || *current.CurrentState != "pending" || len(current.Fields) != 0 ||
		current.Gates["ready"] != true || current.Bookkeeping["authority"] != "header" || mustCanonicalJSON(t, current.Accumulator["count"]) != "1" {
		t.Fatalf("fieldless no-stage native construction lost internal/private facts: %+v", current)
	}
	requireMutationPaths(t, evidence, "lifecycle_state:", "gate:ready", "bookkeeping:authority", "accumulator:count")
	assertNativeMutationReconstruction(t, evidence)
}

func requireNativeMutationProjection(t *testing.T, fixture WorkflowActivityNativeFixtureForTest, ctx context.Context, entity string) WorkflowMutationProjectionForTest {
	t.Helper()
	evidence, err := fixture.ReadMutationProjection(ctx, correlation.RunIDFromContext(ctx), entity)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func requireMutationPaths(t *testing.T, evidence WorkflowMutationProjectionForTest, paths ...string) {
	t.Helper()
	seen := map[string]bool{}
	for _, row := range evidence.Mutations {
		seen[string(row.Domain)+":"+row.Path] = true
	}
	for _, path := range paths {
		if !seen[path] {
			t.Fatalf("missing mutation %s in %v", path, seen)
		}
	}
}

func assertNativeMutationReconstruction(t *testing.T, evidence WorkflowMutationProjectionForTest) {
	t.Helper()
	if len(evidence.Mutations) == 0 {
		t.Fatal("entity_mutations is empty; canonical mutation surface is missing")
	}
	var records []mutationlog.ProjectionMutation
	for _, row := range evidence.Mutations {
		var value any
		if len(row.NewValue) > 0 {
			if err := canonicaljson.DecodePreservingNumberLexemes(row.NewValue, &value); err != nil {
				t.Fatal(err)
			}
		}
		records = append(records, mutationlog.ProjectionMutation{Domain: row.Domain, Path: row.Path, NewValue: value})
	}
	actual, err := mutationlog.ReconstructEntityStateProjection(records)
	if err != nil {
		t.Fatal(err)
	}
	if mustCanonicalJSON(t, actual) != mustCanonicalJSON(t, evidence.Current) {
		t.Fatalf("mutation reconstruction mismatch:\n got=%s\nwant=%s", mustCanonicalJSON(t, actual), mustCanonicalJSON(t, evidence.Current))
	}
}
