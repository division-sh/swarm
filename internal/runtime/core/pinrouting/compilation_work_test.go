package pinrouting

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type censusCountingSource struct {
	semanticview.Source
	builds   int
	withdraw bool
}

func (s *censusCountingSource) AuthoredEventEntries() map[string]runtimecontracts.EventCatalogEntry {
	s.builds++
	return s.Source.AuthoredEventEntries()
}

func (s *censusCountingSource) FlowScopeByID(id string) (semanticview.FlowScope, bool) {
	if s.withdraw {
		return semanticview.FlowScope{}, false
	}
	return s.Source.FlowScopeByID(id)
}

func TestCompileConnectGraphSharesOnlyOperationLocalCensus(t *testing.T) {
	for _, tc := range targetFreeSyntheticProjectionCases() {
		t.Run(tc.name, func(t *testing.T) {
			base, endpoint := targetFreeSyntheticProjectionFixture(t, tc.mint, false)
			source := &censusCountingSource{Source: base}
			if _, issue := LowerPublicInputRoutePlan(source, semanticview.AuthoredEventEndpoint{}); issue.Failure.Empty() || source.builds != 0 {
				t.Fatal("invalid endpoint was not rejected before census construction")
			}
			graph := CompileConnectGraph(source)
			if source.builds != 1 {
				t.Fatalf("census builds = %d, want exactly one per compilation", source.builds)
			}
			if len(graph.receiverPlans) != 1 || len(graph.issues) != 0 {
				t.Fatalf("receiver plans/issues = %d/%v", len(graph.receiverPlans), graph.issues)
			}
			plan, issue := LowerPublicInputRoutePlan(source, endpoint)
			if !issue.Failure.Empty() || !reflect.DeepEqual(plan, graph.receiverPlans[0]) {
				t.Fatalf("operation-local lowering differs from fresh public admission: %v", issue)
			}
			if source.builds != 2 {
				t.Fatalf("public admission reused old census: %d", source.builds)
			}
			source.withdraw = true
			if got := CompileConnectGraph(source); len(got.receiverPlans) != 0 || source.builds != 3 {
				t.Fatalf("later compile reused withdrawn source: plans=%d builds=%d", len(got.receiverPlans), source.builds)
			}
			if _, issue := LowerPublicInputRoutePlan(source, endpoint); issue.Failure.Empty() {
				t.Fatal("public admission accepted withdrawn source")
			}
		})
	}
}
