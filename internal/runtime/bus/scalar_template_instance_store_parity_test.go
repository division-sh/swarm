package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type postgresScalarTemplateInstanceStore struct {
	*store.PostgresStore
	descriptors     []runtimebus.ActiveFlowInstanceDescriptor
	descriptorCalls int
	descriptorErr   error
}

func (s *postgresScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return slices.Clone(s.descriptors), nil
}

type sqliteScalarTemplateInstanceStore struct {
	*store.SQLiteRuntimeStore
	descriptors     []runtimebus.ActiveFlowInstanceDescriptor
	descriptorCalls int
	descriptorErr   error
}

func (s *sqliteScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return slices.Clone(s.descriptors), nil
}

type scalarTemplateInstanceParityStore interface {
	runtimebus.EventStore
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.PreparedPublishEventReader
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	setScalarTemplateInstanceDescriptors([]runtimebus.ActiveFlowInstanceDescriptor)
	setScalarTemplateInstanceDescriptorError(error)
	resetScalarTemplateInstanceDescriptorCalls()
	scalarTemplateInstanceDescriptorCalls() int
}

func (s *postgresScalarTemplateInstanceStore) setScalarTemplateInstanceDescriptors(descriptors []runtimebus.ActiveFlowInstanceDescriptor) {
	s.descriptors = slices.Clone(descriptors)
}

func (s *postgresScalarTemplateInstanceStore) setScalarTemplateInstanceDescriptorError(err error) {
	s.descriptorErr = err
}

func (s *postgresScalarTemplateInstanceStore) resetScalarTemplateInstanceDescriptorCalls() {
	s.descriptorCalls = 0
}

func (s *postgresScalarTemplateInstanceStore) scalarTemplateInstanceDescriptorCalls() int {
	return s.descriptorCalls
}

func (s *sqliteScalarTemplateInstanceStore) setScalarTemplateInstanceDescriptors(descriptors []runtimebus.ActiveFlowInstanceDescriptor) {
	s.descriptors = slices.Clone(descriptors)
}

func (s *sqliteScalarTemplateInstanceStore) setScalarTemplateInstanceDescriptorError(err error) {
	s.descriptorErr = err
}

func (s *sqliteScalarTemplateInstanceStore) resetScalarTemplateInstanceDescriptorCalls() {
	s.descriptorCalls = 0
}

func (s *sqliteScalarTemplateInstanceStore) scalarTemplateInstanceDescriptorCalls() int {
	return s.descriptorCalls
}

func TestScalarTemplateInstanceResolutionPersistsAndReplaysOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			selected, db := newScalarTemplateInstanceParityStore(t, backend, ctx)
			seedCompleteEventDispatchRun(t, ctx, db, backend, runID, time.Now().UTC().Add(-time.Minute))

			repo := canonicalrouting.RepoRoot(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("load scalar resolution fixture: %v", err)
			}
			source := semanticview.Wrap(bundle)
			entityID := uuid.NewString()
			selected.setScalarTemplateInstanceDescriptors([]runtimebus.ActiveFlowInstanceDescriptor{{
				InstanceID:      "one",
				EntityID:        entityID,
				FlowInstance:    "account/one",
				BundleHash:      authorActivityTestBundleHash,
				BundleSource:    authorActivityTestBundleSource,
				WorkflowVersion: source.WorkflowVersion(),
				AddressFields:   map[string]string{"entity.account_id": "acct-1"},
			}})
			eventBus, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			if err := eventBus.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{
				Identity: runtimeflowidentity.DeriveRoute("account", "one"),
			}); err != nil {
				t.Fatalf("AddFlowInstanceRouteContext: %v", err)
			}

			eventID := uuid.NewString()
			eventTime := time.Now().UTC()
			producerEntityID := uuid.NewString()
			evt := eventtest.ExistingRunRootIngress(
				eventID,
				events.EventType("producer/account.ready"),
				"producer",
				"",
				json.RawMessage(`{"account_id":"acct-1"}`),
				0,
				runID,
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}),
				eventTime,
			)
			plan, err := eventBus.CheckPublishRecipientPlan(ctx, evt)
			if err != nil {
				t.Fatalf("CheckPublishRecipientPlan: %v", err)
			}
			wantTarget := events.RouteIdentity{FlowID: "account", FlowInstance: "account/one", EntityID: entityID}
			if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.Route().Normalized() != wantTarget.Normalized() {
				t.Fatalf("preflight failure/routes = %q/%#v, want scalar target %#v", plan.TargetFailure, plan.DeliveryRoutes, wantTarget)
			}
			if err := eventBus.Publish(ctx, evt); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			persistedRoutes, err := selected.ListEventDeliveryRoutes(ctx, eventID)
			if err != nil {
				t.Fatalf("ListEventDeliveryRoutes: %v", err)
			}
			if len(persistedRoutes) != 1 || persistedRoutes[0].Recipient.ID() != "account-node" || persistedRoutes[0].Target.Route().Normalized() != wantTarget.Normalized() {
				t.Fatalf("persisted routes = %#v, want account-node at %#v", persistedRoutes, wantTarget)
			}

			selected.setScalarTemplateInstanceDescriptorError(errors.New("descriptor lookup must not run for a durable duplicate"))
			selected.resetScalarTemplateInstanceDescriptorCalls()
			if err := eventBus.Publish(ctx, evt); err != nil {
				t.Fatalf("Publish exact duplicate after descriptor failure: %v", err)
			}
			if calls := selected.scalarTemplateInstanceDescriptorCalls(); calls != 0 {
				t.Fatalf("duplicate descriptor calls = %d, want durable identity short-circuit", calls)
			}
			conflicting := eventtest.ExistingRunRootIngress(
				eventID,
				events.EventType("producer/account.ready"),
				"producer",
				"",
				json.RawMessage(`{"account_id":"acct-2"}`),
				0,
				runID,
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}),
				eventTime,
			)
			if err := eventBus.Publish(ctx, conflicting); !errors.Is(err, events.ErrEventIdentityConflict) {
				t.Fatalf("Publish conflicting duplicate error = %v, want event identity conflict", err)
			}
			if calls := selected.scalarTemplateInstanceDescriptorCalls(); calls != 0 {
				t.Fatalf("conflicting duplicate descriptor calls = %d, want durable identity short-circuit", calls)
			}
			selected.setScalarTemplateInstanceDescriptorError(nil)

			replayed := subscribeInternalDeliveriesForTest(t, eventBus, "account-node")
			selected.setScalarTemplateInstanceDescriptors([]runtimebus.ActiveFlowInstanceDescriptor{{
				InstanceID:      "drift",
				EntityID:        uuid.NewString(),
				FlowInstance:    "account/drift",
				BundleHash:      authorActivityTestBundleHash,
				BundleSource:    authorActivityTestBundleSource,
				WorkflowVersion: source.WorkflowVersion(),
				AddressFields:   map[string]string{"entity.account_id": "acct-1"},
			}})
			selected.resetScalarTemplateInstanceDescriptorCalls()
			if _, err := eventBus.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{
				Event: evt,
				Scope: runtimepipelineobligation.ScopeSubscribed,
			}, nil); err != nil {
				t.Fatalf("RecoverPersistedPipeline: %v", err)
			}
			select {
			case delivery := <-replayed:
				replayEvent := delivery.Event()
				_ = delivery.Complete()
				if replayEvent.FlowInstance() != "account/one" || replayEvent.EntityID() != entityID {
					t.Fatalf("replayed target = %q/%q, want account/one/%s", replayEvent.FlowInstance(), replayEvent.EntityID(), entityID)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for committed scalar-resolution replay")
			}
			if calls := selected.scalarTemplateInstanceDescriptorCalls(); calls != 0 {
				t.Fatalf("replay descriptor calls = %d, want persisted route authority", calls)
			}
		})
	}
}

func newScalarTemplateInstanceParityStore(t *testing.T, backend string, ctx context.Context) (scalarTemplateInstanceParityStore, *sql.DB) {
	t.Helper()
	switch backend {
	case "sqlite":
		selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		return &sqliteScalarTemplateInstanceStore{SQLiteRuntimeStore: selected}, storetest.DatabaseForTest(selected)
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		return &postgresScalarTemplateInstanceStore{PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db)}, db
	default:
		t.Fatalf("unsupported backend %q", backend)
		return nil, nil
	}
}
