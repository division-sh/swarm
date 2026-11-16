package cataloge2e

import (
	"context"
	"encoding/json"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run("one_key_"+string(backend), func(t *testing.T) {
			for _, name := range []string{"same_flow", "keyless_receiver", "bound_parent_and_intermediate"} {
				t.Run(name, func(t *testing.T) {
					proveSelectedCrossInstanceFork(t, backend, name)
				})
			}
		})
	}
}

type crossInstanceSelectedForkStore interface {
	forkexecution.SelectedContractForkLifecycle
	forkexecution.SourceArtifactSelectedContractSourceStore
	storetest.RootedDataCatalogStore
	bus.PreparedPublishEventReader
	deliverylifecycle.Store
}

func proveSelectedCrossInstanceFork(t *testing.T, backend catalogRuntimeBackend, name string) {
	t.Helper()
	shape := canonicalrouting.CrossInstanceNestedKeyless
	if name == "same_flow" {
		shape = canonicalrouting.CrossInstanceSameKeyedFlow
	}
	root := canonicalrouting.CopyCrossInstanceConnection(t, shape)
	deep := name == "bound_parent_and_intermediate"
	prefix, opened, request := "", "opened", "cross.requested"
	if deep {
		root = canonicalrouting.CopyBoundCrossInstanceConnection(t)
		prefix, opened, request = "envelope/", "group.opened", "group.cross.requested"
	}
	h := newLiveForkCatalogHarness(t, root, backend)
	ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
	var selected crossInstanceSelectedForkStore = h.pg
	if h.sqlite != nil {
		selected = h.sqlite
	}
	seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
	groups := []string{"left"}
	if deep {
		groups = append(groups, "right")
	}
	for _, group := range groups {
		for _, account := range []string{"7", "8"} {
			payload := map[string]any{"account_id": account, "ticket_id": "42"}
			if deep {
				payload["envelope_id"] = group
			}
			h.publishAndWait(catalogTriggerStep{Event: opened, Payload: payload}, catalogRuntimePublishTimeout)
		}
	}
	h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	instances, err := h.workflow.ListWorkflowInstances(ctx, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	parentID := catalogRuntimeRunID
	if deep {
		parentID = ""
		for _, instance := range instances {
			if instance.WorkflowName == "envelope" && instance.Fields["envelope_id"] == "left" {
				parentID = instance.EntityID
			}
		}
		if parentID == "" {
			t.Fatal("missing authored outer parent")
		}
	}
	var senderPath, keyedPath, receiverPath, intermediatePath string
	for _, instance := range instances {
		if instance.WorkflowName == prefix+"account" && instance.ParentEntityID == parentID {
			switch instance.Fields["account_id"] {
			case "7":
				senderPath = instance.StorageRef
			case "8":
				keyedPath = instance.StorageRef
			}
		}
	}
	if senderPath == "" || keyedPath == "" || senderPath == keyedPath {
		t.Fatal("native fixture lacks distinct sending and selected keyed owners")
	}
	receiverPath = keyedPath
	eventName := "/account.cross"
	if shape == canonicalrouting.CrossInstanceNestedKeyless {
		eventName = "/ticket.cross"
		for _, instance := range instances {
			if instance.WorkflowName == prefix+"account/ticket" && instance.ParentEntityID == flowidentity.EntityID(senderPath) {
				senderPath = instance.StorageRef
				break
			}
		}
		owner := keyedPath
		if deep {
			for _, instance := range instances {
				if instance.WorkflowName == prefix+"account/bridge" && instance.ParentEntityID == flowidentity.EntityID(keyedPath) {
					intermediatePath, owner = instance.StorageRef, instance.StorageRef
				}
			}
		}
		receiverPath = ""
		for _, instance := range instances {
			if strings.HasSuffix(instance.WorkflowName, "/audit") && instance.ParentEntityID == flowidentity.EntityID(owner) {
				receiverPath = instance.StorageRef
			}
		}
		if receiverPath == "" || deep && intermediatePath == "" {
			t.Fatal("missing native keyless target chain")
		}
	}
	publish := func() {
		payload := map[string]any{"source_account_id": "7", "ticket_id": "42", "target_account_id": "8"}
		if deep {
			payload["envelope_id"] = "left"
		}
		h.publishAndWait(catalogTriggerStep{Event: request, Payload: payload}, catalogRuntimePublishTimeout)
		h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
	}
	publish()
	public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	var eventID string
	for _, event := range public {
		if !strings.HasSuffix(event.EventName, eventName) {
			continue
		}
		if eventID != "" || len(event.Deliveries) != 1 || event.Deliveries[0].Route.Target.Route().FlowInstance != receiverPath || event.Deliveries[0].Status != "delivered" || len(event.DeadLetters) != 0 || event.NoDelivery != nil {
			t.Fatalf("source did not execute its exact cross-instance route: %+v", event)
		}
		eventID = event.EventID
	}
	native, found, err := storetest.ReadCanonicalEventRecord(ctx, selected, eventID)
	if err != nil || !found || native.SourceRoute().FlowInstance != senderPath {
		t.Fatalf("source publication lost its sender: found=%t event=%+v error=%v", found, native, err)
	}
	// The later send changes this same receiver. A live-state fallback would
	// start from two, not the captured zero, before executing the fork's input.
	publish()
	loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, selected)
	installCatalogSelectedSourceTopology(t, ctx, h, loaded)
	owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
	options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
	sourceRun, cut := catalogRuntimeRunID, eventID
	hops := 1
	if deep {
		hops = 2
	}
	for hop := 0; hop < hops; hop++ {
		plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: cut})
		if err != nil {
			t.Fatal(err)
		}
		before, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source, ContractSelection: selection})
		if err != nil {
			t.Fatalf("native cut not admitted: %+v error=%v", frontier, err)
		}
		recipients := 0
		for _, event := range frontier.FrontierEvents {
			if event.SourceEventID == cut {
				for _, recipient := range event.DerivedRecipients {
					_, _, connect := recipient.Connect()
					if recipient.Path != receiverPath || !connect || !recipient.Recipient.IsNode() {
						t.Fatalf("captured selector chose another receiver: %+v", recipient)
					}
					recipients++
				}
			}
		}
		if recipients != 1 {
			selector, present, selectorErr := plan.HistoricalRouteSelector(cut)
			admission, admitted := selector.PayloadAdmission()
			t.Logf("captured selector: present=%t error=%v admitted=%t class=%s schema=%s", present, selectorErr, admitted, selector.AdmissionClass(), admission.Binding().SchemaClass())
			if bundle, found := semanticview.Bundle(loaded.Source); found {
				for _, connect := range bundle.Semantics.CompositionConnects {
					if connect.Event == strings.TrimPrefix(eventName, "/") {
						input, found, inputErr := bundle.ConnectionInput(connect)
						t.Logf("captured connect %s: input=%t error=%v selected=%s source=%+v", connect.AuthoredLocation(), found, inputErr, input.SelectionFlowID(), input.SourceEvidence().Source)
					}
				}
			}
			t.Fatalf("nonempty selected execution frontier recipients=%d, want one: %+v", recipients, frontier)
		}
		publication, found, err := selected.LoadPreparedPublishEvent(ctx, cut)
		if err != nil || !found || len(publication.DeliveryRoutes) != 1 {
			t.Fatalf("captured source publication: found=%t value=%+v error=%v", found, publication, err)
		}
		originalRoute := publication.DeliveryRoutes[0]
		captured, entity, found, err := runforkadmission.FixedConstructionForRoute(loaded.Source, plan, flowidentity.StoredRoute(originalRoute.Target.Route().FlowID, flowidentity.LogicalInstanceID(receiverPath), receiverPath))
		if err != nil || !found || entity.Fields["cross_count"] != int64(0) {
			t.Fatalf("fixed receiver was not captured before delivery: found=%t entity=%+v error=%v", found, entity, err)
		}
		current, err := h.workflow.ListWorkflowInstances(ctx, sourceRun)
		if err != nil {
			t.Fatal(err)
		}
		liveCount := int64(2)
		if hop != 0 {
			liveCount = 1
		}
		liveFound := false
		for _, instance := range current {
			if instance.StorageRef == receiverPath {
				liveFound = instance.Fields["cross_count"] == liveCount
			}
		}
		if !liveFound {
			t.Fatalf("live receiver does not distinguish captured state: %+v", current)
		}
		if deep && hop == 0 {
			proveCrossInstanceCutRefusals(t, h, selected, loaded.Source, selection, plan, keyedPath, intermediatePath, receiverPath)
		}
		sourceBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
		if err != nil {
			t.Fatal(err)
		}
		result, err := executeCatalogLiveFork(t, h, ctx, forkexecution.SelectedContractExecutionRequest{
			SourceRunID: sourceRun, At: cut, Owner: owner, SourceLoader: loader, ContractSelection: selection,
			AgentRuntime: options, ExpectedBundleHash: selection.BundleHash,
		})
		if err != nil || !result.Activation.Activated || result.ExecutedEventCount < 1 {
			t.Fatalf("real selected cross-instance readiness/execution: %+v error=%v", result, err)
		}
		child := result.Materialization.ForkRunID
		projected, err := runfork.ProjectCapturedConstructionIdentity(sourceRun, child, captured)
		if err != nil {
			t.Fatal(err)
		}
		childEvent := activityidentity.ForkLineageEventID(child, cut)
		wait, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
		defer cancel()
		for {
			childPublication, found, err := selected.LoadPreparedPublishEvent(wait, childEvent)
			if err != nil {
				t.Fatal(err)
			}
			settled := false
			if found {
				if len(childPublication.DeliveryRoutes) != 1 {
					t.Fatalf("fork replaced its exact recipient partition: %+v", childPublication)
				}
				route := childPublication.DeliveryRoutes[0]
				localEvent, hasEvent := route.ConnectClaim.ReceiverEvent()
				if route.Target.Route() != (events.RouteIdentity{FlowID: projected.TemplateID, FlowInstance: projected.InstancePath, EntityID: projected.EntityID}) || route.Recipient != originalRoute.Recipient || !route.ConnectClaim.Equal(originalRoute.ConnectClaim) || !hasEvent || localEvent == "" {
					t.Fatalf("fork changed selected receiver/handler/claim: %+v", route)
				}
				id, err := deliverylifecycle.DeliveryID(childEvent, route)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := selected.Snapshot(wait, id)
				if err != nil || snapshot.Terminal() && snapshot.Status != deliverylifecycle.StatusDelivered {
					t.Fatalf("selected recipient failed: %+v error=%v", snapshot, err)
				}
				settled = snapshot.Status == deliverylifecycle.StatusDelivered
				if settled && snapshot.ClaimVersion != 1 {
					t.Fatalf("selected recipient executed more than once: %+v", snapshot)
				}
			}
			childInstances, err := h.workflow.ListWorkflowInstances(wait, child)
			if err != nil {
				t.Fatal(err)
			}
			counted := false
			for _, instance := range childInstances {
				if instance.StorageRef == projected.InstancePath {
					counted = instance.Fields["cross_count"] == int64(1)
				}
			}
			if settled && counted {
				for _, entity := range plan.Entities {
					meta := entity.MaterializationMetadata
					fixed, _, found, err := runforkadmission.FixedConstructionForRoute(loaded.Source, plan, flowidentity.StoredRoute(meta.FlowTemplate, flowidentity.LogicalInstanceID(meta.FlowInstance), meta.FlowInstance))
					if err != nil || !found {
						t.Fatalf("captured sibling construction: found=%t error=%v", found, err)
					}
					identity, err := runfork.ProjectCapturedConstructionIdentity(sourceRun, child, fixed)
					if err != nil {
						t.Fatal(err)
					}
					want := make(map[string]any, len(entity.Fields))
					for field, value := range entity.Fields {
						want[field] = value
					}
					if fixed.InstancePath == receiverPath {
						want["cross_count"] = int64(1)
					}
					matched := false
					for _, instance := range childInstances {
						if instance.StorageRef == identity.InstancePath {
							matched = instance.EntityID == identity.EntityID && instance.ParentEntityID == identity.ParentEntityID && maps.EqualFunc(instance.Fields, want, reflect.DeepEqual)
						}
					}
					if !matched {
						t.Fatalf("selected child changed captured sibling identity/fields: identity=%+v want=%+v instances=%+v", identity, want, childInstances)
					}
				}
				break
			}
			select {
			case <-wait.Done():
				t.Fatalf("fork did not execute from captured zero exactly once: instances=%+v error=%v", childInstances, wait.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		cancel()
		after, err := json.Marshal(plan)
		if err != nil || string(before) != string(after) {
			t.Fatalf("admission/execution mutated supplied cut: %v", err)
		}
		sourceAfter, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
		if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
			t.Fatalf("fork mutated its immediate source native state: %v", err)
		}
		if deep {
			childPlan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: child, At: childEvent})
			if err != nil {
				t.Fatal(err)
			}
			_, inherited, found, err := runforkadmission.FixedConstructionForRoute(loaded.Source, childPlan, projected.Route())
			if err != nil || !found || inherited.MaterializationMetadata.HistoricalConstruction == nil || inherited.MaterializationMetadata.HistoricalConstruction.SourceRunID != sourceRun {
				t.Fatalf("fork lost original captured constructor provenance: %+v found=%t error=%v", inherited, found, err)
			}
		}
		sourceRun, cut, receiverPath = child, childEvent, projected.InstancePath
	}
}

func proveCrossInstanceCutRefusals(t *testing.T, h *runtimeHarness, selected crossInstanceSelectedForkStore, source semanticview.Source, selection runfork.RunForkContractSelection, original runfork.RunForkPlan, keyedPath, intermediatePath, receiverPath string) {
	t.Helper()
	ctx := h.ctx
	domain, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, original.SourceRunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing_keyed", "duplicated_keyed", "missing_intermediate", "crossed_intermediate", "missing_receiver", "crossed_receiver"} {
		t.Run(name, func(t *testing.T) {
			baseline, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: original, Source: source, ContractSelection: selection})
			if err != nil || len(baseline.FrontierEvents) == 0 {
				t.Fatalf("uncorrupted native cut not admitted before corruption: %+v error=%v", baseline, err)
			}
			plan := original
			body, err := json.Marshal(original.Entities)
			if err != nil {
				t.Fatal(err)
			}
			plan.Entities = nil
			if err := json.Unmarshal(body, &plan.Entities); err != nil {
				t.Fatal(err)
			}
			path, boundary := keyedPath, "selected captured receiver requires one exact key/parent match"
			if strings.Contains(name, "intermediate") {
				path, boundary = intermediatePath, "fixed receiver lacks exact keyless ancestor"
			} else if strings.Contains(name, "receiver") {
				path, boundary = receiverPath, "fixed receiver lacks exact keyless ancestor"
			}
			index := -1
			for i, entity := range plan.Entities {
				if entity.MaterializationMetadata.FlowInstance == path {
					index = i
				}
			}
			if index < 0 {
				t.Fatalf("native cut lacks corruption target %s", path)
			}
			switch {
			case strings.HasPrefix(name, "missing_"):
				plan.Entities = append(plan.Entities[:index], plan.Entities[index+1:]...)
			case name == "duplicated_keyed":
				plan.Entities = append(plan.Entities, plan.Entities[index])
				boundary = "duplicate entity ownership"
			default:
				plan.Entities[index].MaterializationMetadata.ParentInstance = original.SourceRunID
				boundary = "fixed header contradicts captured construction identity"
			}
			before, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			_, err = runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: selection})
			if err == nil || !strings.Contains(err.Error(), boundary) {
				t.Fatalf("%s did not reach its named refusal boundary %q: %v", name, boundary, err)
			}
			t.Logf("captured-cut refusal boundary: %s", err)
			after, marshalErr := json.Marshal(plan)
			if marshalErr != nil || string(before) != string(after) {
				t.Fatalf("refusal mutated supplied captured evidence: %v", marshalErr)
			}
			afterDomain, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, original.SourceRunID)
			if err != nil || !reflect.DeepEqual(domain, afterDomain) {
				t.Fatalf("captured-cut refusal mutated native source: %v", err)
			}
		})
	}
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
