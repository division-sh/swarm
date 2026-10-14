package runforkpersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestCapturedConstructionLineageUsesExactOriginBoundary(t *testing.T) {
	runID, eventID := eventtest.UUID("captured-source"), eventtest.UUID("captured-event")
	origin, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointEvent, 7, eventID)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"exact", "run", "revision", "event", "event_run", "future_event", "missing_event"} {
		t.Run(cut, func(t *testing.T) {
			snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: 7, Events: []runForkRevisionEvent{{RunID: runID, EventID: eventID, runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3}}}}
			switch cut {
			case "run":
				snapshot.RunID = eventtest.UUID("foreign-source")
			case "revision":
				snapshot.Revision++
			case "event":
				snapshot.Events[0].EventID = eventtest.UUID("foreign-event")
			case "event_run":
				snapshot.Events[0].RunID = eventtest.UUID("foreign-source")
			case "future_event":
				snapshot.Events[0].FirstRevision = 8
			case "missing_event":
				snapshot.Events = nil
			}
			if err := validateCapturedOriginBoundary(origin, snapshot); (err == nil) != (cut == "exact") {
				t.Fatalf("%s: %v", cut, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resolver := &capturedConstructionResolver{active: map[string]bool{runID: true}, snapshots: make(map[constructionCut]*runForkRevisionSnapshot)}
	if _, err := resolver.resolve(ctx, &runForkRevisionSnapshot{RunID: runID, Revision: 7}); !errors.Is(err, context.Canceled) {
		t.Fatalf("lineage ignored cancellation: %v", err)
	}
	if _, err := resolver.sourceCut(context.Background(), origin); err == nil {
		t.Fatal("cyclic durable lineage reached SQL")
	}
}
