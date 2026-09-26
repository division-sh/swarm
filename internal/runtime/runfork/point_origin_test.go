package runfork

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestForkPointMaterializationOriginPreservesExactPoint(t *testing.T) {
	eventID := uuid.NewString()
	for _, tc := range []struct {
		name  string
		point RunForkPoint
	}{
		{name: "event", point: RunForkPoint{Kind: RunForkPointEvent, Revision: 7, EventID: eventID}},
		{name: "deployment_revision", point: RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin, err := tc.point.MaterializationRunOrigin("source-run")
			if err != nil {
				t.Fatal(err)
			}
			if origin.Kind() != runlifecycle.OriginForkMaterialization || origin.SourceRunID() != "source-run" ||
				origin.ForkPointKind() != tc.point.Kind || origin.ForkRevision() != tc.point.Revision || origin.SourceEventID() != tc.point.EventID {
				t.Fatalf("fork origin lost exact point: point=%+v origin=%+v", tc.point, origin)
			}
		})
	}
	for _, point := range []RunForkPoint{
		{Kind: RunForkPointDeploymentRevision, Revision: 8, EventID: eventID},
		{Kind: RunForkPointDeploymentRevision, Revision: 8, EventName: "root.ready"},
		{Kind: RunForkPointEvent, Revision: 8},
		{Kind: RunForkPointEvent, Revision: 0, EventID: eventID},
	} {
		if _, err := point.MaterializationRunOrigin("source-run"); err == nil {
			t.Fatalf("invalid fork point formed lifecycle origin: %+v", point)
		}
	}
}
