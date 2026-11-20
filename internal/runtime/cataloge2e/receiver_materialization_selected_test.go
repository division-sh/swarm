package cataloge2e

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestReceiverMaterializationSelectedForkExecutionBothStores(t *testing.T) {
	t.Run("nested authored selector", proveNestedAuthoredSelectorFork)
	runReceiverMaterializationSelectedForkExecution(t, false)
}

func proveNestedAuthoredSelectorFork(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyNestedEventSchema(t)
			h := newLiveForkCatalogHarness(t, root, backend)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			var selected interface {
				forkexecution.SelectedContractForkLifecycle
				forkexecution.SourceArtifactSelectedContractSourceStore
				storetest.RootedDataCatalogStore
				bus.PreparedPublishEventReader
				deliverylifecycle.Store
			} = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			for _, key := range []string{"source", "target"} {
				h.publishAndWait(catalogTriggerStep{Event: "opened", Payload: map[string]any{"account_id": key, "ticket_id": "ticket"}}, catalogRuntimePublishTimeout)
			}
			instances, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			accounts := make(map[string]events.RouteIdentity)
			for _, instance := range instances {
				if instance.WorkflowName == "account" && (instance.InstanceKey == "source" || instance.InstanceKey == "target") {
					if _, duplicate := accounts[instance.InstanceKey]; duplicate || instance.Fields["account_id"] != instance.InstanceKey {
						t.Fatalf("account construction disagrees with authored key: %+v", instance)
					}
					accounts[instance.InstanceKey] = events.RouteIdentity{FlowID: instance.WorkflowName, FlowInstance: instance.StorageRef, EntityID: instance.EntityID}
				}
			}
			var sourceTicket events.RouteIdentity
			for _, instance := range instances {
				if instance.WorkflowName == "account/ticket" && instance.ParentFlowInstance == accounts["source"].FlowInstance && instance.InstanceKey == "ticket" {
					if sourceTicket.FlowID != "" || instance.ParentEntityID != accounts["source"].EntityID || instance.Fields["ticket_id"] != "ticket" {
						t.Fatalf("source ticket construction disagrees with its parent/key: %+v", instance)
					}
					sourceTicket = events.RouteIdentity{FlowID: instance.WorkflowName, FlowInstance: instance.StorageRef, EntityID: instance.EntityID}
				}
			}
			if len(accounts) != 2 || sourceTicket.FlowID == "" {
				t.Fatalf("independent source/target constructions missing: %+v", instances)
			}
			h.publishAndWait(catalogTriggerStep{Event: "cross.requested", Payload: map[string]any{"source_account_id": "source", "ticket_id": "ticket", "target_account_id": "target"}}, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			cut := ""
			for _, event := range public {
				if strings.HasSuffix(event.EventName, "/ticket.cross") {
					if cut != "" {
						t.Fatal("expected one native nested publication")
					}
					cut = event.EventID
				}
			}
			if cut == "" {
				t.Fatal("nested node did not publish its declared event")
			}
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, selected)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: cut})
			if err != nil {
				t.Fatal(err)
			}
			selector, found, err := plan.HistoricalRouteSelector(cut)
			admission, admitted := selector.PayloadAdmission()
			schema, schemaFound, schemaErr := loaded.Source.ResolveEffectiveCompiledFlowEventSchema("account/ticket", "ticket.cross")
			if err != nil || !found || !admitted || schemaErr != nil || !schemaFound || admission.Binding().SchemaClass() != events.PayloadSchemaAuthored || admission.Binding().FlowID() != "account/ticket" || admission.Binding().SchemaDigest() != schema.AcceptanceSchemaDigest() {
				t.Fatalf("stored native selector lost declared schema evidence: found=%t admitted=%t schema=%t errors=%v/%v binding=%+v", found, admitted, schemaFound, err, schemaErr, admission.Binding())
			}
			if selector.RoutingSource().Route() != sourceTicket {
				t.Fatalf("native selector source=%+v, want independently constructed ticket=%+v", selector.RoutingSource().Route(), sourceTicket)
			}
			publication, found, err := selected.LoadPreparedPublishEvent(ctx, cut)
			if err != nil || !found || len(publication.DeliveryRoutes) != 1 {
				t.Fatalf("native C53 publication: %+v found=%t err=%v", publication, found, err)
			}
			original := publication.DeliveryRoutes[0]
			route := original.Target.Route()
			if route != accounts["target"] || publication.Event.Event().RoutingSource().Route() != sourceTicket {
				t.Fatalf("native C53 source/target disagrees with independent constructions: source=%+v target=%+v want=%+v", publication.Event.Event().RoutingSource().Route(), route, accounts["target"])
			}
			captured, entity, found, err := runforkadmission.FixedConstructionForRoute(loaded.Source, plan, flowidentity.StoredRoute(route.FlowID, flowidentity.LogicalInstanceID(route.FlowInstance), route.FlowInstance))
			if err != nil || !found || entity.Fields["cross_count"] != int64(0) {
				t.Fatalf("C53 fixed-cut target: %+v found=%t err=%v", entity, found, err)
			}
			type auditAtCut struct {
				identity flowidentity.Instance
				state    runfork.RunForkEntityState
			}
			audits := make([]auditAtCut, 0, len(accounts))
			for _, key := range []string{"source", "target"} {
				parent := accounts[key]
				path := parent.FlowInstance + "/audit"
				identity, state, found, err := runforkadmission.FixedConstructionForRoute(loaded.Source, plan, flowidentity.StoredRoute("account/audit", flowidentity.LogicalInstanceID(path), path))
				if err != nil || !found || state.CurrentState == nil || identity.ParentRoute.EntityID != parent.EntityID {
					t.Fatalf("%s audit construction at cut: %+v found=%t err=%v", key, identity, found, err)
				}
				completed := 0
				for _, work := range plan.PendingWork {
					if work.DeliveryRoute.Target.Route().EntityID == identity.EntityID && strings.HasSuffix(work.EventName, "/ticket.done") &&
						work.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
						completed++
					}
				}
				if completed != 1 {
					t.Fatalf("%s audit must have its original completed ticket delivery at cut, got %d", key, completed)
				}
				audits = append(audits, auditAtCut{identity: identity, state: state})
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executeCatalogLiveFork(t, h, ctx, forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, SourceLoader: loader, ContractSelection: selection,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig()), ExpectedBundleHash: selection.BundleHash,
			})
			if err != nil || !result.Activation.Activated || result.ExecutedEventCount < 1 {
				t.Fatalf("real stored-selector fork: %+v err=%v", result, err)
			}
			if result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 || result.ForkEvents[0].SourceEventID != cut {
				t.Fatalf("only the ticket.cross frontier may execute, got %+v", result.ForkEvents)
			}
			child := result.Materialization.ForkRunID
			target, err := runfork.ProjectCapturedConstructionIdentity(catalogRuntimeRunID, child, captured)
			if err != nil {
				t.Fatal(err)
			}
			wait, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
			defer cancel()
			var instancesAfter []pipeline.WorkflowInstance
			for {
				instances, err := h.workflow.ListWorkflowInstances(wait, child)
				if err != nil {
					t.Fatal(err)
				}
				completed := false
				for _, instance := range instances {
					completed = completed || instance.StorageRef == target.InstancePath && instance.Fields["cross_count"] == int64(1)
				}
				if completed {
					instancesAfter = instances
					break
				}
				select {
				case <-wait.Done():
					t.Fatalf("fork did not execute exact captured target: %+v", instances)
				case <-time.After(10 * time.Millisecond):
				}
			}
			for _, audit := range audits {
				projected, err := runfork.ProjectCapturedConstructionIdentity(catalogRuntimeRunID, child, audit.identity)
				if err != nil {
					t.Fatal(err)
				}
				copies := 0
				for _, instance := range instancesAfter {
					if instance.StorageRef != projected.InstancePath {
						continue
					}
					copies++
					if instance.EntityID != projected.EntityID || instance.WorkflowName != projected.TemplateID ||
						instance.ParentFlowInstance != projected.ParentRoute.FlowInstance || instance.ParentEntityID != projected.ParentEntityID ||
						instance.InstanceKey != "" || instance.CurrentState != *audit.state.CurrentState || !reflect.DeepEqual(instance.Fields, audit.state.Fields) {
						t.Fatalf("audit clone differs from fixed-cut identity/state: %+v", instance)
					}
				}
				if copies != 1 {
					t.Fatalf("audit %s has %d copies, want one", projected.InstancePath, copies)
				}
			}
			after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("selected fork mutated source: %v", err)
			}
		})
	}
}

func TestReceiverMaterializationSelectedForkPublicationFrontierBothStores(t *testing.T) {
	runReceiverMaterializationSelectedForkExecution(t, true)
}

func runReceiverMaterializationSelectedForkExecution(t *testing.T, publicationFrontier bool) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyReceiverMaterializationWithAgent(t, "renamed-observer")
			h := newLiveForkCatalogHarness(t, root, backend)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			selected := runScopedCatalogStore(t, h)
			if !publicationFrontier {
				if _, err := selected.PauseRunControlOutcome(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "receiver dependency fork", ControlledBy: "cataloge2e"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "selected"}}, 20*time.Second, true); err != nil {
				t.Fatal(err)
			}
			inputName := "start.seeded"
			if publicationFrontier {
				inputName = "receiver.seeded"
				if err := h.rt.Manager.WaitForQuiescence(ctx); err != nil {
					t.Fatal(err)
				}
			}
			inputID, err := h.readRunNamedEventIdentity(ctx, catalogRuntimeRunID, inputName)
			if err != nil {
				t.Fatal(err)
			}
			var sourceExecutionsBefore int
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, catalogRuntimeRunID).Scan(&sourceExecutionsBefore); err != nil {
				t.Fatal(err)
			}
			var sourceStore interface {
				storetest.RootedDataCatalogStore
				forkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			var store interface {
				bus.PreparedPublishEventReader
				deliverylifecycle.Store
			} = h.pg
			var lifecycle forkexecution.SelectedContractForkLifecycle = h.pg
			if h.sqlite != nil {
				sourceStore, store, lifecycle = h.sqlite, h.sqlite, h.sqlite
			}
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			cfg := testRuntimeConfig()
			cfg.LLM.Backend = "anthropic"
			executionOwner := selectedContractExecutionOwnerForCatalogHarness(t, h, lifecycle)
			result, err := executeCatalogLiveFork(t, h, ctx, forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: inputID,
				Owner: executionOwner, SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg), ExpectedBundleHash: selection.BundleHash,
			})
			if err != nil {
				logSelectedForkRecoveryFailure(t, ctx, h, result.Materialization.ForkRunID, err)
				t.Fatalf("real selected dependency execution: %+v %v", result, err)
			}
			child := result.Materialization.ForkRunID
			if child == "" || child == catalogRuntimeRunID {
				t.Fatal("missing distinct selected child")
			}
			var eventID string
			deadline := time.Now().Add(20 * time.Second)
			for {
				eventID, err = h.readRunNamedEventIdentity(ctx, child, "receiver.seeded")
				if err == nil || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("selected downstream publication: %v", err)
			}
			publication, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
			if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
				t.Fatalf("selected receiver obligations: %+v %v", publication, err)
			}
			var node, agent deliverylifecycle.Snapshot
			for _, route := range publication.DeliveryRoutes {
				id, err := deliverylifecycle.DeliveryID(eventID, route)
				if err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(20 * time.Second)
				var snapshot deliverylifecycle.Snapshot
				for {
					snapshot, err = store.Snapshot(ctx, id)
					if err != nil || snapshot.Status == deliverylifecycle.StatusDelivered || snapshot.Status == deliverylifecycle.StatusDeadLetter || time.Now().After(deadline) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
					t.Fatalf("selected final dependency: %+v %v", snapshot, err)
				}
				if route.Recipient.IsNode() {
					node = snapshot
				} else {
					agent = snapshot
				}
			}
			requireDeclaredAgentReceiverOwnership(t, child, node, agent)
			historical, fixed := agent.Route.Initialization.HistoricalConstruction()
			if !fixed || historical.SourceRunID != catalogRuntimeRunID || historical.SourceRevision != result.Materialization.ForkPoint.Revision ||
				historical.SourceReceiver != agent.Route.AgentIdentity.Route || agent.Route.Initialization.ConstructionTarget().Route() != node.Route.Target.Route() {
				t.Fatal("selected receiver lost its exact fixed-cut construction dependency")
			}
			if err := agent.Route.Initialization.ValidateEvent(publication.Event.Event()); err != nil {
				t.Fatal(err)
			}
			if err := agent.Route.Initialization.ValidateRoute(agent.Route); err != nil {
				t.Fatal(err)
			}
			if !agent.Authority.Equal(node.Authority) {
				t.Fatalf("selected ownership relation changed: node=%+v agent=%+v", node, agent)
			}
			requireReceiverConstructedBeforeDelivery(t, h, child, node)
			requireReceiverConstructedBeforeDelivery(t, h, child, agent)
			var providerCalls int
			h.llm.mu.Lock()
			calls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
			h.llm.mu.Unlock()
			for _, call := range calls {
				if call.RunID == child && call.EventID == eventID && call.AgentID == agent.SubscriberID && call.TargetEntityID == "" {
					providerCalls++
				}
			}
			if providerCalls != 1 || node.ClaimVersion != 1 || agent.ClaimVersion != 1 {
				t.Fatalf("selected exact provider execution count=%d node=%+v agent=%+v calls=%+v", providerCalls, node, agent, calls)
			}
			if !publicationFrontier {
				if err := executionOwner.RetireSelectedContexts(ctx); err != nil {
					t.Fatal(err)
				}
				requireHistoricalPublicationFreshBusReplay(t, ctx, h, publication, node, agent, calls)
			}
			var sourceExecutions int
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, catalogRuntimeRunID).Scan(&sourceExecutions); err != nil || sourceExecutions != sourceExecutionsBefore {
				t.Fatalf("fork executed source: before=%d after=%d err=%v", sourceExecutionsBefore, sourceExecutions, err)
			}
		})
	}
}
