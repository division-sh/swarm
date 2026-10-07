package runstart

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReviewer2566NestedFeedReceiverMustEnterFiniteClosure(t *testing.T) {
	source := finiteTestSource(t, canonicalrouting.CopyFiniteNestedServiceFeed(t))
	feed, err := events.NewDeploymentFeedRoutingSource("producer")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := pinrouting.AdmitDeploymentFeedDeclaration(source, events.EventType("producer/work.requested"), feed)
	if err != nil {
		t.Fatal(err)
	}
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Plans()) != 1 || len(graph.Issues()) != 0 {
		t.Fatalf("expected admitted feed route: %v", graph.Issues())
	}
	if err := ValidateFinite(source, []pinrouting.SourceEvent{selected}); err == nil {
		t.Fatal("finite admission accepted an admissible nested feed whose connected worker has no final stage")
	}
}

func TestFiniteStartSelectedFeedsUseExactRoutesAndRecursiveConstructors(t *testing.T) {
	for _, tc := range []struct {
		offender string
		variant  canonicalrouting.FiniteClosureOffender
	}{
		{"worker", canonicalrouting.FiniteClosureWorkerService},
		{"worker/support/leaf", canonicalrouting.FiniteClosureChildService},
		{"none", canonicalrouting.FiniteClosureEnded},
	} {
		t.Run(tc.offender, func(t *testing.T) {
			// A feed does not instantiate its keyed source, even a service.
			source := finiteTestSource(t, canonicalrouting.CopyFiniteSelectedFeedClosure(t, tc.variant))
			routing, err := events.NewDeploymentFeedRoutingSource("producer")
			if err != nil {
				t.Fatal(err)
			}
			var feeds []pinrouting.SourceEvent
			for _, name := range []string{"producer/work.safe", "producer/work.requested"} {
				feed, err := pinrouting.AdmitDeploymentFeedDeclaration(source, events.EventType(name), routing)
				if err != nil {
					t.Fatal(err)
				}
				feeds = append(feeds, feed)
			}
			if err := ValidateFinite(source, feeds[:1]); err != nil {
				t.Fatalf("unselected source or route entered the closure: %v", err)
			}
			err = ValidateFinite(source, feeds)
			if tc.offender == "none" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var refusal *FiniteStartError
			if !errors.As(err, &refusal) || refusal.FlowID != tc.offender {
				t.Fatalf("selected feed closure missed %s: %v", tc.offender, err)
			}
		})
	}
}
