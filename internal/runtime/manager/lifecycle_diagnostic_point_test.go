package manager

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestLifecycleDiagnosticOriginRequiresExactTypedSelectedPoint(t *testing.T) {
	for _, point := range []runfork.RunForkPoint{
		{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 2},
		{Kind: runfork.RunForkPointDeploymentRevision, Revision: 2},
	} {
		origin := LifecycleDiagnosticOrigin{
			Owner: LifecycleDiagnosticSelectedFork, Causality: LifecycleDiagnosticObservation,
			SourceRunID: uuid.NewString(), ForkPoint: point, ForkEventID: point.EventID,
			SelectedFork: effects.SelectedContractForkAuthority{
				ExecutionID: uuid.NewString(), ForkRunID: uuid.NewString(), Generation: 1,
				AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container",
				ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
			},
		}
		if err := origin.Validate(); err != nil {
			t.Fatalf("valid %s diagnostic rejected: %v", point.Kind, err)
		}
		bad := origin
		bad.ForkEventID = uuid.NewString()
		if err := bad.Validate(); err == nil {
			t.Fatalf("%s diagnostic accepted a fabricated event", point.Kind)
		}
		bad = origin
		bad.ForkPoint.Revision = 0
		if err := bad.Validate(); err == nil {
			t.Fatalf("%s diagnostic accepted an unbound revision", point.Kind)
		}
	}
}
