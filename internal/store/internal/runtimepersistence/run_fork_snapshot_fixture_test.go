package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func runForkSnapshotFixtureEvent(t *testing.T, ctx context.Context, selected any) events.Event {
	t.Helper()
	runID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "snapshot.fixture", "fixture", "", []byte(`{"value":1}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	event, err := bindSemanticEventFixturePayload(event)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := CommitSemanticEventFixtureForTest(ctx, selected, admitted, testRouteSettlement(event, nil), nil, pipelineobligation.ScopeDirect, nil); err != nil || !inserted {
		t.Fatalf("seed unrevisioned historical event: inserted=%t err=%v", inserted, err)
	}
	return event
}
