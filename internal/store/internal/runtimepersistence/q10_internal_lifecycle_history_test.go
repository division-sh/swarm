package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
)

func assertQ10NativeInternalHistory(t *testing.T, ctx context.Context, selected any, run, entity string, want *string) {
	t.Helper()
	evidence, err := ReadTrackedEntityMutationProjectionStorageForTest(ctx, selected, run, entity)
	if err != nil {
		t.Fatal(err)
	}
	if (want == nil) != (evidence.CurrentState == nil) || (want != nil && *evidence.CurrentState != *want) {
		t.Fatalf("real writer/canonical observer changed internal lifecycle: got=%v want=%v", evidence.CurrentState, want)
	}
	current := mutationlog.EntityStateProjection{CurrentState: evidence.CurrentState, Fields: map[string]any{}}
	for _, bucket := range []struct {
		raw []byte
		dst *map[string]any
	}{{evidence.Fields, &current.Fields}, {evidence.Bookkeeping, &current.Bookkeeping}, {evidence.Gates, &current.Gates}, {evidence.Accumulator, &current.Accumulator}} {
		if bucket.raw != nil {
			if err := canonicaljson.DecodePreservingNumberLexemes(bucket.raw, bucket.dst); err != nil {
				t.Fatal(err)
			}
		}
	}
	var records []mutationlog.ProjectionMutation
	for _, row := range evidence.Mutations {
		var value any
		if err := canonicaljson.DecodePreservingNumberLexemes(row.NewValue, &value); err != nil {
			t.Fatal(err)
		}
		records = append(records, mutationlog.ProjectionMutation{Domain: mutationlog.Domain(row.Domain), Path: row.Path, NewValue: value})
	}
	if len(records) == 0 {
		t.Fatal("native construction/import lost its history")
	}
	rebuilt, err := mutationlog.ReconstructEntityStateProjection(records)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(expected) {
		t.Fatalf("real writer and canonical history disagree: got=%s want=%s", actual, expected)
	}
}
