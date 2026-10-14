package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type rollbackRoutingFixture interface {
	UpsertFlowInstanceRoute(context.Context, bus.FlowInstanceRouteRecord) error
	RollbackFlowInstanceRoute(context.Context, flowidentity.RunScopedFlowInstance) error
	InspectSnapshot(context.Context, func(context.Context) error) error
}

func TestPublicInputRollbackObservationsPreserveGlobalRoutesAndAllEventScopesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			direct, foreign := runForkSnapshotFixtureEvent(t, ctx, selected), runForkSnapshotFixtureEvent(t, ctx, selected)
			seedPublicRollbackRoutes(t, ctx, selected, direct.RunID(), foreign.RunID())
			subscribed := seedPublicRollbackSubscribedEvent(t, ctx, selected, foreign.RunID())
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			assertPublicRollbackPredicates(t, ctx, selected, direct, subscribed, foreign)
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 7 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("rollback observations merged independent cuts or wrote: %+v", counts)
			}
			assertPublicRollbackSnapshotRefusals(t, ctx, selected, direct.ID())
		})
	}
}

func assertPublicRollbackPredicates(t *testing.T, ctx context.Context, selected any, direct, subscribed, foreign events.Event) {
	t.Helper()
	if count, err := CountTemplatePrefixRoutingRulesForTest(ctx, selected, "telegram-chat"); err != nil || count != 3 {
		t.Fatalf("global template-prefix count lost foreign-run/inactive/node routes: %d %v", count, err)
	}
	for _, event := range []events.Event{direct, subscribed, foreign} {
		if count, err := CountTemplateEventReplayScopesForTest(ctx, selected, event.ID()); err != nil || count != 1 {
			t.Fatalf("exact event count lost direct/subscribed scope: %d %v", count, err)
		}
	}
	if count, err := CountTemplateEventReplayScopesForTest(ctx, selected, uuid.NewString()); err != nil || count != 0 {
		t.Fatalf("event count included foreign events: %d %v", count, err)
	}
	if count, err := CountTemplateSubscribedReplayScopeForTest(ctx, selected, direct.ID()); err != nil || count != 0 {
		t.Fatalf("different subscribed-only owner was broadened: %d %v", count, err)
	}
	if count, err := CountTemplateSubscribedReplayScopeForTest(ctx, selected, subscribed.ID()); err != nil || count != 1 {
		t.Fatalf("subscribed positive control missing: %d %v", count, err)
	}
}

func seedPublicRollbackRoutes(t *testing.T, ctx context.Context, selected any, run, foreignRun string) {
	t.Helper()
	owner := selected.(rollbackRoutingFixture)
	for _, row := range []struct {
		run, path, subscriberType string
		inactive                  bool
	}{
		{run, "telegram-chat/one", "agent", false},
		{foreignRun, "telegram-chat/two", "node", true},
		{foreignRun, "telegram-chat/three", "node", false},
		{foreignRun, "telegram-chat-other/four", "agent", false},
	} {
		identity := flowidentity.RunScopedFlowInstance{RunID: row.run, Route: flowidentity.RouteForInstancePath(row.path)}
		at := time.Now().UTC()
		commitPreparedWorkflowAggregateFixture(t, ctx, selected.(agentFixtureFlowStore), row.run, pipeline.WorkflowInstance{
			InstanceID: identity.Route.InstanceID, StorageRef: identity.Route.InstancePath, EntityID: uuid.NewString(), WorkflowName: identity.Route.ScopeKey, WorkflowVersion: "fixture",
			Mode: "template", StageDefined: true, CurrentState: "ready", Fields: map[string]any{}, CreatedAt: at, EnteredStageAt: at,
		}, at)
		route := bus.FlowInstanceRouteRecord{Identity: identity, EventPattern: "snapshot.fixture", SubscriberType: row.subscriberType, SubscriberID: "rollback-route", SourceFlow: identity.Route.ScopeKey}
		if err := owner.UpsertFlowInstanceRoute(ctx, route); err != nil {
			t.Fatal(err)
		}
		if row.inactive {
			if err := owner.RollbackFlowInstanceRoute(ctx, identity); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func seedPublicRollbackSubscribedEvent(t *testing.T, ctx context.Context, selected any, run string) events.Event {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "snapshot.fixture", "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithAgents(ctx, selected, event, []string{"rollback-agent"}); err != nil {
		t.Fatal(err)
	}
	row, err := ReadHistoricalReplayScopeStorageForTest(ctx, selected, event.ID())
	if err != nil || row.Scope != string(pipelineobligation.ScopeSubscribed) {
		t.Fatalf("native setup did not persist subscribed scope: %+v %v", row, err)
	}
	return event
}

func assertPublicRollbackSnapshotRefusals(t *testing.T, ctx context.Context, selected any, event string) {
	t.Helper()
	owner := selected.(rollbackRoutingFixture)
	var retained context.Context
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		retained = scoped
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, read := range []func(context.Context, any) (int, error){
		func(ctx context.Context, selected any) (int, error) {
			return CountTemplatePrefixRoutingRulesForTest(ctx, selected, "telegram-chat")
		},
		func(ctx context.Context, selected any) (int, error) {
			return CountTemplateEventReplayScopesForTest(ctx, selected, event)
		},
	} {
		if count, err := read(retained, selected); err == nil || count != 0 {
			t.Fatalf("expired snapshot returned provisional rollback evidence: %d %v", count, err)
		}
		if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
			if count, err := read(scoped, &sql.DB{}); err == nil || count != 0 {
				t.Fatalf("foreign raw owner returned provisional rollback evidence: %d %v", count, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if count, err := read(canceled, selected); !errors.Is(err, context.Canceled) || count != 0 {
			t.Fatalf("canceled rollback observation returned evidence: %d %v", count, err)
		}
	}
}
