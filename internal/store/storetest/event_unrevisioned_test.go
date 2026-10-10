package storetest

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestUnrevisionedSemanticEventFixtureMatchesCanonicalMutationProjection(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			rawStore := backend.open(t)
			canonicalStore := backend.open(t)
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			for _, selected := range []RunFixtureStore{rawStore, canonicalStore} {
				RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})
			}
			node, err := runtimeidentity.ParseExecutableNode("flow_a", "fixture-node")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
			}
			name, err := agentidentity.DeclaredName("fixture-agent", "fixture-owner")
			if err != nil {
				t.Fatal(err)
			}
			identity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			routes := []events.DeliveryRoute{route, {
				Recipient:     events.MustAgentDeliveryRecipient("fixture-agent"),
				AgentIdentity: identity,
			}}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID, "fixture.ready", "fixture", "", []byte(`{"value":1}`), 0, runID,
				events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at,
			)
			beforeRevision := ReadSemanticEventFixtureEvidence(t, ctx, rawStore, runID, eventID).RevisionCount
			if got := CommitSemanticEventWithRoutes(t, ctx, rawStore, event, routes, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendInserted {
				t.Fatalf("raw fixture outcome = %v, want inserted", got)
			}
			rawEvidence := ReadSemanticEventFixtureEvidence(t, ctx, rawStore, runID, eventID)
			if got := rawEvidence.RevisionCount; got != beforeRevision {
				t.Fatalf("raw fixture revisions = %d, want unchanged %d", got, beforeRevision)
			}

			bound, err := eventfixture.BindPayload(event)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPublish(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			canonicalRevision := ReadSemanticEventFixtureEvidence(t, ctx, canonicalStore, runID, eventID).RevisionCount
			inserted, err := private.CommitRevisionedSemanticEventFixtureForTest(
				ctx, canonicalStore, admitted, canonicalFixtureSettlement(t, admitted.Event(), routes),
				routes, runtimepipelineobligation.ScopeSubscribed, nil,
			)
			if err == nil && !inserted {
				t.Fatal("canonical mutation did not insert the fixture")
			}
			if err != nil {
				t.Fatalf("canonical mutation fixture: %v", err)
			}
			canonicalEvidence := ReadSemanticEventFixtureEvidence(t, ctx, canonicalStore, runID, eventID)
			if got := canonicalEvidence.RevisionCount; got <= canonicalRevision {
				t.Fatalf("canonical mutation revisions = %d, want after %d", got, canonicalRevision)
			}

			if !rawEvidence.RecordFound || !canonicalEvidence.RecordFound {
				t.Fatal("fixture comparison requires both complete persisted records")
			}
			rawRecord := rawEvidence.Record
			canonicalRecord := canonicalEvidence.Record
			if !rawRecord.Equal(canonicalRecord) {
				t.Fatalf("unrevisioned event record differs from canonical mutation projection: raw=%#v canonical=%#v", rawRecord, canonicalRecord)
			}
			for _, selectedRoute := range routes {
				deliveryID, err := runtimedelivery.DeliveryID(eventID, selectedRoute)
				if err != nil {
					t.Fatal(err)
				}
				raw, rawFound := rawEvidence.DeliveryProjections[deliveryID]
				canonical, canonicalFound := canonicalEvidence.DeliveryProjections[deliveryID]
				if !rawFound || !canonicalFound {
					t.Fatalf("exact delivery projection missing: raw=%v canonical=%v", rawFound, canonicalFound)
				}
				if !reflect.DeepEqual(raw, canonical) {
					t.Fatalf("unrevisioned delivery projection differs from canonical mutation: raw=%#v canonical=%#v", raw, canonical)
				}
			}
			if got := CommitSemanticEventWithRoutes(t, ctx, rawStore, event, routes, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendExactDuplicate {
				t.Fatalf("exact duplicate outcome = %v, want exact duplicate", got)
			}
			duplicateEvidence := ReadSemanticEventFixtureEvidence(t, ctx, rawStore, runID, eventID)
			if got := duplicateEvidence.RevisionCount; got != beforeRevision {
				t.Fatalf("duplicate fixture revisions = %d, want unchanged %d", got, beforeRevision)
			}
			if got := len(duplicateEvidence.DeliveryProjections); got != len(routes) {
				t.Fatalf("duplicate fixture delivery count = %d, want %d", got, len(routes))
			}
		})
	}
}

func TestUnrevisionedSemanticDeliveryFixtureImmediateTerminalization(t *testing.T) {
	type terminalizingStore interface {
		RunFixtureStore
		TerminalizeRun(context.Context, string, string) ([]runtimedelivery.Terminalization, error)
		Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
	}
	for _, backend := range []struct {
		name string
		open func(*testing.T) terminalizingStore
	}{
		{"sqlite", func(t *testing.T) terminalizingStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) terminalizingStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			selected := backend.open(t)
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC()
			RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})
			node, err := runtimeidentity.ParseExecutableNode("flow_a", "fixture-node")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID, "fixture.ready", "fixture", "", []byte(`{"value":1}`), 0, runID,
				events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at,
			)
			if got := CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendInserted {
				t.Fatalf("fixture outcome = %v, want inserted", got)
			}
			deliveryID, err := runtimedelivery.DeliveryID(eventID, route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := selected.Snapshot(ctx, deliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if backend.name == "sqlite" && before.CreatedAt.Nanosecond()%int(time.Millisecond) != 0 {
				t.Fatalf("SQLite fixture created_at = %s, want database-clock millisecond precision", before.CreatedAt)
			}
			transitions, err := selected.TerminalizeRun(ctx, runID, "run_terminal")
			if err != nil {
				t.Fatalf("immediate terminalization: %v", err)
			}
			if len(transitions) != 1 || transitions[0].Current.DeliveryID != deliveryID || transitions[0].Current.Status != runtimedelivery.StatusDeadLetter {
				t.Fatalf("terminalization transitions = %#v, want one dead-lettered delivery", transitions)
			}
			if transitions[0].Current.UpdatedAt.Before(before.CreatedAt) {
				t.Fatalf("terminalized updated_at %s precedes created_at %s", transitions[0].Current.UpdatedAt, before.CreatedAt)
			}
		})
	}
}
