package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type postgresScalarTemplateInstanceStore struct {
	*store.PostgresStore
	descriptors     []runtimebus.ActiveFlowInstanceDescriptor
	descriptorCalls int
	descriptorErr   error
}

func (s *postgresScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptors(context.Context, string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return slices.Clone(s.descriptors), nil
}

func (s *postgresScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptorsForScope(_ context.Context, _ string, templateIDs, instancePaths []string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return scalarTemplateScopedDescriptors(s.descriptors, templateIDs, instancePaths), nil
}

func (s *postgresScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptorsForKey(_ context.Context, _ string, templateID, keyField, keyValue string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return scalarTemplateKeyedDescriptors(s.descriptors, templateID, keyField, keyValue), nil
}

type sqliteScalarTemplateInstanceStore struct {
	*store.SQLiteRuntimeStore
	descriptors     []runtimebus.ActiveFlowInstanceDescriptor
	descriptorCalls int
	descriptorErr   error
}

func (s *sqliteScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptors(context.Context, string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return slices.Clone(s.descriptors), nil
}

func (s *sqliteScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptorsForScope(_ context.Context, _ string, templateIDs, instancePaths []string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return scalarTemplateScopedDescriptors(s.descriptors, templateIDs, instancePaths), nil
}

func (s *sqliteScalarTemplateInstanceStore) ListActiveFlowInstanceDescriptorsForKey(_ context.Context, _ string, templateID, keyField, keyValue string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	s.descriptorCalls++
	if s.descriptorErr != nil {
		return nil, s.descriptorErr
	}
	return scalarTemplateKeyedDescriptors(s.descriptors, templateID, keyField, keyValue), nil
}

func scalarTemplateScopedDescriptors(descriptors []runtimebus.ActiveFlowInstanceDescriptor, templateIDs, instancePaths []string) []runtimebus.ActiveFlowInstanceDescriptor {
	var selected []runtimebus.ActiveFlowInstanceDescriptor
	for _, descriptor := range descriptors {
		if slices.Contains(templateIDs, descriptor.FlowTemplate) || slices.Contains(instancePaths, descriptor.FlowInstance) {
			selected = append(selected, descriptor)
		}
	}
	return selected
}

func scalarTemplateKeyedDescriptors(descriptors []runtimebus.ActiveFlowInstanceDescriptor, templateID, keyField, keyValue string) []runtimebus.ActiveFlowInstanceDescriptor {
	field, err := runtimecontracts.ParseTemplateInstanceField(strings.TrimPrefix(keyField, "entity."))
	if err != nil || keyField != "entity."+field.Path() {
		return nil
	}
	key := []runtimecontracts.TemplateInstanceKeyValue{{Field: field, Value: keyValue}}
	var selected []runtimebus.ActiveFlowInstanceDescriptor
	for _, descriptor := range descriptors {
		if descriptor.FlowTemplate == templateID && runtimepinrouting.ConnectInstanceKeyDescriptorMatches(key, runtimepinrouting.Descriptor{AddressFields: descriptor.AddressFields}) {
			selected = append(selected, descriptor)
		}
	}
	return selected
}

type scalarTemplateInstanceParityStore interface {
	componentFlowConstructionStore
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
			repo := canonicalrouting.RepoRoot(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("load scalar resolution fixture: %v", err)
			}
			source := semanticview.Wrap(bundle)
			sourceFact := testSourceArtifactFact(source)
			sourceBundleHash := sourceFact.BundleHash()
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runID)
			selected, db := newScalarTemplateInstanceParityStore(t, backend, ctx)
			run := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: sourceFact, Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC().Add(-time.Minute)}
			if backend == "postgres" {
				runlifecyclefixture.RequirePostgres(t, ctx, db, run)
			} else {
				runlifecyclefixture.RequireSQLite(t, ctx, db, run)
			}
			entityID := runtimeflowidentity.EntityID("account/one")
			constructor, err := runtimepipeline.CompileFlowConstructor(source, "account", "account.ready")
			if err != nil {
				t.Fatal(err)
			}
			fields, err := constructor.InitialFields(map[string]any{"account_id": "acct-1"}, "acct-1")
			if err != nil {
				t.Fatal(err)
			}
			contract, declared := entityruntime.ResolveForFlow(source, "account")
			if !declared {
				t.Fatal("account fixture lost its state contract")
			}
			at := time.Now().UTC()
			for _, flowID := range []string{".", "producer"} {
				constructed := runtimebus.ConstructedFlowInstanceIdentityFixture(source, flowID, "", runID)
				seedComponentFlowConstruction(t, ctx, selected, source, runtimepipeline.WorkflowInstance{
					InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
					ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
					WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), EnteredStageAt: at, CreatedAt: at,
				})
			}
			seedComponentFlowConstruction(t, ctx, selected, source, runtimepipeline.WorkflowInstance{
				InstanceID: "one", StorageRef: "account/one", EntityID: entityID, WorkflowName: "account", InstanceKey: "acct-1",
				WorkflowVersion: source.WorkflowVersion(), InstanceKind: "template", EntityType: contract.EntityType,
				ParentFlowID: ".", ParentFlowInstance: runID, ParentEntityID: runtimeflowidentity.EntityID(runID),
				Fields: fields, EnteredStageAt: at, CreatedAt: at,
			})
			selected.setScalarTemplateInstanceDescriptors([]runtimebus.ActiveFlowInstanceDescriptor{{
				Identity:        runtimebus.ConstructedFlowInstanceIdentityFixture(source, "account", "one", runID),
				RunID:           runID,
				InstanceID:      "one",
				EntityID:        entityID,
				FlowInstance:    "account/one",
				FlowTemplate:    "account",
				BundleHash:      sourceBundleHash,
				WorkflowVersion: source.WorkflowVersion(),
				AddressFields:   map[string]string{"entity.account_id": "acct-1"},
			}})
			eventBus, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}

			eventID := uuid.NewString()
			eventTime := time.Now().UTC()
			producerEntityID := runtimeflowidentity.EntityID("producer")
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID,
				events.EventType("producer/account.ready"),
				"producer",
				"",
				json.RawMessage(`{"account_id":"acct-1"}`),
				0,
				runID,
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}),
				eventtest.StaticFlowRoutingSource("producer", "producer", producerEntityID), eventTime,
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
			if len(persistedRoutes) != 1 || persistedRoutes[0].Recipient.LocalID() != "account-node" || persistedRoutes[0].Target.Route().Normalized() != wantTarget.Normalized() {
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
			conflicting := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID,
				events.EventType("producer/account.ready"),
				"producer",
				"",
				json.RawMessage(`{"account_id":"acct-2"}`),
				0,
				runID,
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producerEntityID}),
				evt.RoutingSource(), eventTime,
			)
			if err := eventBus.Publish(ctx, conflicting); !errors.Is(err, events.ErrEventIdentityConflict) {
				t.Fatalf("Publish conflicting duplicate error = %v, want event identity conflict", err)
			}
			if calls := selected.scalarTemplateInstanceDescriptorCalls(); calls != 0 {
				t.Fatalf("conflicting duplicate descriptor calls = %d, want durable identity short-circuit", calls)
			}
			selected.setScalarTemplateInstanceDescriptorError(nil)

			replayed := subscribeInternalDeliveriesForTest(t, eventBus, persistedRoutes[0].Recipient.ID())
			selected.setScalarTemplateInstanceDescriptors([]runtimebus.ActiveFlowInstanceDescriptor{{
				RunID:           runID,
				InstanceID:      "drift",
				EntityID:        uuid.NewString(),
				FlowInstance:    "account/drift",
				BundleHash:      sourceBundleHash,
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

func TestParentLocalReturnPersistsExactNativeReceiverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyNestedFlowConnect(t), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runID)
			selected, db := newScalarTemplateInstanceParityStore(t, backend, ctx)
			at := time.Now().UTC()
			run := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: testSourceArtifactFact(source), Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)}
			if backend == "postgres" {
				runlifecyclefixture.RequirePostgres(t, ctx, db, run)
			} else {
				runlifecyclefixture.RequireSQLite(t, ctx, db, run)
			}
			for _, flowID := range []string{".", "child", "child/grandchild"} {
				identity := runtimebus.ConstructedFlowInstanceIdentityFixture(source, flowID, "", runID)
				instance := runtimepipeline.WorkflowInstance{
					InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
					ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
					WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), EnteredStageAt: at, CreatedAt: at,
				}
				if entity, declared := entityruntime.ResolveForFlow(source, flowID); declared {
					instance.EntityType = entity.EntityType
					constructor, err := runtimepipeline.CompileFlowConstructor(source, flowID, "")
					if err != nil {
						t.Fatal(err)
					}
					instance.Fields, err = constructor.InitialFields(nil, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				seedComponentFlowConstruction(t, ctx, selected, source, instance)
			}
			eventBus, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			sender := eventtest.StaticFlowRoutingSource("child/grandchild", "child/grandchild", runtimeflowidentity.EntityID("child/grandchild"))
			event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "child/grandchild/micro.done", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, sender, at)
			plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
			want := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: runtimeflowidentity.EntityID("child")})
			if err != nil || plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target != want || plan.DeliveryRoutes[0].ConnectClaim.Empty() {
				t.Fatalf("parent-local plan=%+v err=%v, want exact admitted receiver %+v", plan, err, want)
			}
			node, localEvent, claimed := plan.DeliveryRoutes[0].ConnectClaim.NodeHandlerOwner()
			if !claimed || node.FlowPath() != "child" || node.NodeID() != "child-aggregator" || localEvent != "micro.done" {
				t.Fatalf("parent-local claim=%+v event=%q admitted=%t", node, localEvent, claimed)
			}
			if err := eventBus.Publish(ctx, event); err != nil {
				t.Fatal(err)
			}
			routes, err := selected.ListEventDeliveryRoutes(ctx, event.ID())
			if err != nil || len(routes) != 1 || !reflect.DeepEqual(routes[0], plan.DeliveryRoutes[0]) {
				t.Fatalf("persisted parent-local routes=%+v err=%v", routes, err)
			}
			carrier := subscribeInternalDeliveriesForTest(t, eventBus, routes[0].Recipient.ID())
			selected.setScalarTemplateInstanceDescriptorError(errors.New("committed parent-local replay must not query descriptors"))
			selected.resetScalarTemplateInstanceDescriptorCalls()
			if _, err := eventBus.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{Event: event, Scope: runtimepipelineobligation.ScopeSubscribed}, nil); err != nil {
				t.Fatal(err)
			}
			select {
			case delivery := <-carrier:
				if got := delivery.Event(); got.ID() != event.ID() || got.FlowInstance() != want.Route().FlowInstance || got.EntityID() != want.Route().EntityID || got.Type() != event.Type() {
					t.Fatalf("parent-local replay crossed receiver or event: %+v", got)
				}
				if err := delivery.Complete(); err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("parent-local replay did not reach its committed receiver")
			}
			if calls := selected.scalarTemplateInstanceDescriptorCalls(); calls != 0 {
				t.Fatalf("committed parent-local replay consulted mutable descriptors %d times", calls)
			}
			selected.setScalarTemplateInstanceDescriptorError(nil)
			foreign := eventtest.StaticFlowRoutingSource("child/grandchild", "child/grandchild", uuid.NewString())
			rejected := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "child/grandchild/micro.done", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, foreign, at)
			if err := eventBus.Publish(ctx, rejected); err == nil || !strings.Contains(err.Error(), "contradicts") {
				t.Fatalf("foreign parent-local publication=%v, want exact source-entity refusal", err)
			}
			if persisted, found, err := selected.LoadPreparedPublishEvent(ctx, rejected.ID()); err != nil || found || len(persisted.DeliveryRoutes) != 0 {
				t.Fatalf("rejected parent-local publication mutated store: %+v found=%t err=%v", persisted, found, err)
			}
		})
	}
}
