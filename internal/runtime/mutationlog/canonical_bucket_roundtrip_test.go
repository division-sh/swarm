package mutationlog

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestCanonicalNodeBucketMutationRoundTrip(t *testing.T) {
	for _, flow := range []string{".", "review", "outer/review"} {
		for _, bucket := range []string{"handler_accumulators", "handler_joins"} {
			t.Run(flow+"/"+bucket, func(t *testing.T) {
				node, err := identity.AdmitExecutableNodeDeclaration(flow, "collector")
				if err != nil {
					t.Fatal(err)
				}
				before := EntityStateProjection{Accumulator: map[string]any{}}
				after := EntityStateProjection{Accumulator: map[string]any{node.Key(): map[string]any{bucket: map[string]any{"retained": "evidence"}}}}
				records, err := BuildEntityStateDiffRecords("entity", before, after, Writer{Type: "system_node", ID: node.Key(), HandlerStep: "accumulate"})
				if err != nil || len(records) != 1 {
					t.Fatalf("canonical diff: %#v %v", records, err)
				}
				record := records[0]
				if record.Path != node.Key() || record.Domain != DomainAccumulator {
					t.Fatalf("writer lost canonical identity: %#v", record)
				}
				got, err := ReconstructEntityStateProjection([]ProjectionMutation{{Domain: record.Domain, Path: record.Path, NewValue: record.NewValue}})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(after.Accumulator, got.Accumulator) {
					t.Fatalf("canonical node key is an atomic bucket, not an authored dotted path: want=%#v got=%#v", after.Accumulator, got.Accumulator)
				}
				removed, err := BuildEntityStateDiffRecords("entity", after, before, Writer{Type: "system_node", ID: node.Key(), HandlerStep: "clear"})
				if err != nil || len(removed) != 1 {
					t.Fatalf("clear diff: %#v %v", removed, err)
				}
				if err := ApplyEntityStateProjectionMutation(&got, removed[0].Domain, removed[0].Path, removed[0].NewValue); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before.Accumulator, got.Accumulator) {
					t.Fatalf("clear resurrected bucket evidence: %#v", got.Accumulator)
				}
			})
		}
	}
}
