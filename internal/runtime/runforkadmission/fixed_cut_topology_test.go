package runforkadmission_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestT17HistoricalTopologyRetainsOriginalConstructionAndExactSelectors(t *testing.T) {
	for _, hops := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("hops_%d", hops), func(t *testing.T) {
			source, plan, selectors := t17FixedCutTopologyFixture(t)
			for i := 0; i < hops; i++ {
				plan, selectors = t17ProjectHistoricalCut(t, source, plan, selectors)
			}
			for _, entity := range plan.Entities {
				if len(entity.MaterializationMetadata.InitialMaterialization) != 0 || entity.MaterializationMetadata.HistoricalConstruction == nil {
					t.Fatal("historical header was made native")
				}
			}
			before, _ := json.Marshal(plan)
			frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
			if err != nil {
				t.Fatal(err)
			}
			routes, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
			if err != nil {
				t.Fatal(err)
			}
			topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: routes})
			if err != nil || !topology.DynamicTopologySupported || len(topology.DynamicTopologyProofs) != 4 {
				t.Fatalf("historical topology lost captured construction: %+v %v", topology, err)
			}
			if _, err := runforkexecution.BuildSelectedContractExecutionModel(runforkexecution.SelectedContractExecutionModelRequest{Admission: frontier, RouteAdmission: routes, RouteTopology: topology}); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(plan)
			if string(before) != string(after) {
				t.Fatal("historical topology rewrote its source evidence")
			}
		})
	}
}

func TestT17HistoricalTopologyRefusesCrossedConstructionAndSelectorLineage(t *testing.T) {
	for _, name := range []string{"missing_selector", "foreign_selector_source", "ancestor_event_as_child", "foreign_key", "foreign_parent", "missing_revision", "native_conflict"} {
		t.Run(name, func(t *testing.T) {
			source, original, selectors := t17FixedCutTopologyFixture(t)
			ancestorEventID := selectors[0].ID()
			plan, selectors := t17ProjectHistoricalCut(t, source, original, selectors)
			meta := plan.Entities[2].MaterializationMetadata
			switch name {
			case "missing_selector":
				selectors = selectors[1:]
			case "foreign_selector_source", "ancestor_event_as_child":
				selector := selectors[0]
				sourceRun, id := original.SourceRunID, selector.ID()
				if name == "foreign_selector_source" {
					sourceRun = uuid.NewString()
				} else {
					id = ancestorEventID
				}
				lineage, err := events.NewSelectedForkLineage(plan.SourceRunID, sourceRun, ancestorEventID, "selection:test", "", selector.ExecutionMode())
				if err != nil {
					t.Fatal(err)
				}
				replacement, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Lineage: lineage, Facts: events.EventFacts{ID: id, Type: selector.Type(), Producer: events.ProducerClaim{Type: selector.Producer().Type(), ID: selector.Producer().ID()}, RoutingSource: selector.RoutingSource(), Payload: selector.Payload(), CreatedAt: selector.CreatedAt()}})
				if err != nil {
					t.Fatal(err)
				}
				admission, _ := selector.PayloadAdmission()
				selectors[0], err = events.ApplyPayloadAdmission(replacement, admission)
				if err != nil {
					t.Fatal(err)
				}
			case "foreign_key":
				meta.InstanceKey = "foreign-key"
			case "foreign_parent":
				meta.ParentInstance = "foreign-parent"
			case "missing_revision":
				meta.HistoricalConstruction.SourceRevision = 0
			case "native_conflict":
				meta.InitialMaterialization = original.Entities[2].MaterializationMetadata.InitialMaterialization
			}
			var ids []string
			for _, event := range selectors {
				ids = append(ids, event.ID())
			}
			plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, ids)
			var err error
			plan, err = plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, selectors)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(plan)
			frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
			if err == nil {
				var routes runfork.RunForkSelectedContractRouteAdmission
				routes, err = runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
				if err == nil {
					topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: routes})
					if err == nil && topology.DynamicTopologySupported {
						t.Fatal("crossed historical evidence became complete topology authority")
					}
				}
			}
			after, _ := json.Marshal(plan)
			if string(before) != string(after) {
				t.Fatal("historical refusal mutated its captured evidence")
			}
		})
	}
}

func t17ProjectHistoricalCut(t *testing.T, source semanticview.Source, plan runfork.RunForkPlan, selectors []events.Event) (runfork.RunForkPlan, []events.Event) {
	t.Helper()
	previous := plan
	nextRun := uuid.NewString()
	plan.SourceRunID, plan.Entities = nextRun, nil
	for _, entity := range previous.Entities {
		meta := entity.MaterializationMetadata
		route := flowidentity.StoredRoute(meta.FlowTemplate, flowidentity.LogicalInstanceID(meta.FlowInstance), meta.FlowInstance)
		fixed, _, found, err := runforkadmission.FixedConstructionEvidenceForRoute(previous, route)
		if err != nil || !found {
			t.Fatalf("source construction: %t %v", found, err)
		}
		projected, err := runfork.ProjectCapturedConstructionIdentity(previous.SourceRunID, nextRun, fixed)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(projected, source.WorkflowVersion())
		if err != nil {
			t.Fatal(err)
		}
		config, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		metadata := *meta
		metadata.FlowConfig, metadata.FlowInstance, metadata.ParentInstance = config, projected.InstancePath, projected.ParentRoute.FlowInstance
		metadata.InitialMaterialization = nil
		metadata.HistoricalConstruction = &runfork.RunForkCapturedHistoricalConstruction{SourceRunID: previous.SourceRunID, SourceRevision: previous.ForkPoint.Revision, SourceEntity: entity}
		projectedEntity := entity
		projectedEntity.EntityID, projectedEntity.MaterializationMetadata = projected.EntityID, &metadata
		// Mutable field values are not captured instance keys.
		if meta.EntityType != "" {
			projectedEntity.Fields = make(map[string]any, len(entity.Fields))
			for field, value := range entity.Fields {
				projectedEntity.Fields[field] = value
			}
			flow, _ := source.FlowSchemaByID(meta.FlowTemplate)
			if !flow.Instance.Empty() {
				projectedEntity.Fields[flow.Instance.Path()] = "current-value-not-construction-key"
			}
		}
		plan.Entities = append(plan.Entities, projectedEntity)
	}
	var projectedSelectors []events.Event
	var ids []string
	eventIDs := make(map[string]string)
	for _, selector := range selectors {
		projected, err := runfork.ProjectSelectedContractSourceEvent(previous.SourceRunID, nextRun, runfork.RunForkSelectedContractSourceEvent{SourceEventID: selector.ID(), EventName: string(selector.Type()), RoutingSource: selector.RoutingSource(), ExecutionMode: selector.ExecutionMode(), Payload: selector.Payload()})
		if err != nil {
			t.Fatal(err)
		}
		lineage, err := events.NewSelectedForkLineage(nextRun, previous.SourceRunID, selector.ID(), "selection:test", "", selector.ExecutionMode())
		if err != nil {
			t.Fatal(err)
		}
		id := runfork.ProjectReplayEventID(nextRun, selector.ID())
		event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Lineage: lineage, Facts: events.EventFacts{ID: id, Type: events.EventType(projected.EventName), Producer: events.ProducerClaim{Type: selector.Producer().Type(), ID: selector.Producer().ID()}, RoutingSource: projected.RoutingSource, Payload: projected.Payload, CreatedAt: selector.CreatedAt()}})
		if err != nil {
			t.Fatal(err)
		}
		admission, _ := selector.PayloadAdmission()
		event, err = events.ApplyPayloadAdmission(event, admission)
		if err != nil {
			t.Fatal(err)
		}
		projectedSelectors = append(projectedSelectors, event)
		ids, eventIDs[selector.ID()] = append(ids, id), id
	}
	plan.PendingWork = append([]runfork.RunForkPendingWork(nil), previous.PendingWork...)
	for i := range plan.PendingWork {
		item := &plan.PendingWork[i]
		item.EventID = eventIDs[item.EventID]
		for _, selector := range projectedSelectors {
			if selector.ID() == item.EventID {
				item.RoutingSource = selector.RoutingSource()
			}
		}
		if item.FlowInstance == previous.SourceRunID {
			item.FlowInstance = nextRun
		}
	}
	last := projectedSelectors[len(projectedSelectors)-1]
	plan.ForkPoint.Input, plan.ForkPoint.EventID, plan.ForkPoint.RoutingSource = last.ID(), last.ID(), last.RoutingSource()
	plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, ids)
	var err error
	plan, err = plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, projectedSelectors)
	if err != nil {
		t.Fatal(err)
	}
	return plan, projectedSelectors
}

func TestT17FixedCutTopologyUsesConstructorsAndSelectorsNotDynamicFrontier(t *testing.T) {
	source, plan, _ := t17FixedCutTopologyFixture(t)
	before, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if len(frontier.FrontierEvents) != 1 || !reflect.DeepEqual(frontier.FrontierEvents[0].SourceFlowInstances, []string{plan.SourceRunID}) {
		t.Fatalf("historical selectors invented dynamic work: %+v", frontier.FrontierEvents)
	}
	routes, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
	if err != nil {
		t.Fatal(err)
	}
	topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: routes})
	if err != nil {
		t.Fatal(err)
	}
	if !topology.StaticTopologySupported || !topology.DynamicTopologySupported || len(topology.DynamicFlowInstances) != 4 || len(topology.DynamicTopologyProofs) != 4 {
		t.Fatalf("exact nested constructor topology missing: %+v", topology)
	}
	for _, proof := range topology.DynamicTopologyProofs {
		if proof.FixedCutEvidenceFingerprint == "" || len(proof.SourceEventIDs) != 1 || len(proof.DerivedRecipients) != 1 {
			t.Fatalf("topology lacks fixed-cut receipt/selector binding: %+v", proof)
		}
	}
	if _, err := runforkexecution.BuildSelectedContractExecutionModel(runforkexecution.SelectedContractExecutionModelRequest{Admission: frontier, RouteAdmission: routes, RouteTopology: topology}); err != nil {
		t.Fatalf("canonical preparation rejected fixed-cut topology: %v", err)
	}
	forged := topology
	forged.DynamicTopologyProofs = append([]runfork.RunForkSelectedContractDynamicTopologyProof(nil), topology.DynamicTopologyProofs...)
	forged.DynamicTopologyProofs[0].FixedCutEvidenceFingerprint = strings.Repeat("0", 64)
	if _, err := runforkexecution.BuildSelectedContractExecutionModel(runforkexecution.SelectedContractExecutionModelRequest{Admission: frontier, RouteAdmission: routes, RouteTopology: forged}); err == nil {
		t.Fatal("preparation accepted changed fixed-cut fingerprint")
	}
	after, err := json.Marshal(plan)
	if err != nil || string(before) != string(after) {
		t.Fatalf("admission mutated source plan: %v", err)
	}
	changed := routes
	changed.SelectedRouteEvents = append([]runfork.RunForkSelectedContractRouteEvent(nil), routes.SelectedRouteEvents...)
	changed.SelectedRouteEvents[0].SourceEventID = uuid.NewString()
	if _, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: changed}); err == nil {
		t.Fatal("changed historical selector retained constructed topology proof")
	}
}

func TestT17FixedCutTopologyRefusesMissingForeignAndChangedSelectors(t *testing.T) {
	for _, name := range []string{"missing_selector", "foreign_run", "foreign_cut", "changed_key", "changed_schema", "changed_bundle", "changed_routing", "missing_constructor", "foreign_parent"} {
		t.Run(name, func(t *testing.T) {
			source, plan, selectors := t17FixedCutTopologyFixture(t)
			switch name {
			case "missing_selector":
				var err error
				plan, err = plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, selectors[1:])
				if err != nil {
					t.Fatal(err)
				}
			case "foreign_run":
				plan.SourceRunID = uuid.NewString()
			case "foreign_cut":
				plan.ForkPoint.Revision++
			case "changed_routing":
				plan.PendingWork[0].RoutingSource = eventtest.RootRoutingSource(uuid.NewString())
			case "changed_key", "changed_schema", "changed_bundle":
				original := selectors[0]
				payload := original.Payload()
				if name == "changed_key" {
					payload = []byte(`{"case_id":"foreign","seed":true}`)
				}
				replacement := t17SelectorEvent(t, source, plan.SourceRunID, original.ID(), string(original.Type()), ".", "left.loop.start", original.RoutingSource(), payload)
				if name == "changed_schema" || name == "changed_bundle" {
					originalAdmission, _ := original.PayloadAdmission()
					originalBinding := originalAdmission.Binding()
					schemaDigest := originalBinding.SchemaDigest()
					if name == "changed_schema" {
						schemaDigest = "sha256:" + strings.Repeat("2", 64)
					}
					binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), FlowID: originalBinding.FlowID(), EventKey: originalBinding.EventKey(), SchemaDigest: schemaDigest, SchemaClass: originalBinding.SchemaClass()})
					if err != nil {
						t.Fatal(err)
					}
					admission, err := events.NewPayloadAdmission(payload, binding)
					if err != nil {
						t.Fatal(err)
					}
					replacement = eventtest.ExistingRunRootIngressWithRoutingSource(original.ID(), original.Type(), "operator", "", payload, 0, plan.SourceRunID, events.EventEnvelope{}, original.RoutingSource(), original.CreatedAt())
					replacement, err = events.ApplyPayloadAdmission(replacement, admission)
					if err != nil {
						t.Fatal(err)
					}
				}
				selectors[0] = replacement
				var err error
				plan, err = plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, selectors)
				if err != nil {
					t.Fatal(err)
				}
			case "missing_constructor":
				plan.Entities = append(plan.Entities[:2:2], plan.Entities[3:]...)
			case "foreign_parent":
				meta := *plan.Entities[2].MaterializationMetadata
				var config map[string]any
				if err := json.Unmarshal(meta.FlowConfig, &config); err != nil {
					t.Fatal(err)
				}
				config["parent_entity_id"] = uuid.NewString()
				var err error
				meta.FlowConfig, err = json.Marshal(config)
				if err != nil {
					t.Fatal(err)
				}
				plan.Entities[2].MaterializationMetadata = &meta
			}
			frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
			if err != nil {
				t.Logf("refused at frontier: %v", err)
				return
			}
			routes, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
			if err != nil {
				t.Logf("refused at route admission: %v", err)
				return
			}
			topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: routes})
			if err == nil && topology.DynamicTopologySupported {
				t.Fatal("unsupported selector/constructor gained dynamic topology proof")
			}
			t.Logf("refused at topology: dynamic=%t error=%v", topology.DynamicTopologySupported, err)
		})
	}
}

func TestT17FixedCutStaticChildConnectUsesExactCapturedParent(t *testing.T) {
	source, plan, selectors := t17FixedCutTopologyFixture(t)
	sinkMeta := plan.Entities[3].MaterializationMetadata
	finalMeta := plan.Entities[4].MaterializationMetadata
	completed := t17SelectorEvent(t, source, plan.SourceRunID, uuid.NewString(), sinkMeta.FlowInstance+"/work.completed", sinkMeta.FlowTemplate, "work.completed",
		eventtest.ConcreteTemplateRoutingSource(sinkMeta.FlowTemplate, sinkMeta.FlowInstance, plan.Entities[3].EntityID), []byte(`{"result":"left"}`))
	selectors = append(selectors, completed)
	var ids []string
	for _, selector := range selectors {
		ids = append(ids, selector.ID())
	}
	plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, ids)
	var err error
	plan, err = plan.WithHistoricalRouteSelectors(plan.ForkPoint.Revision, selectors)
	if err != nil {
		t.Fatal(err)
	}
	plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: completed.ID(), EventName: string(completed.Type()), RoutingSource: completed.RoutingSource(), FlowInstance: finalMeta.FlowInstance, Classification: runfork.RunForkPendingClassificationDeliveredCompleted})
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range history.SelectedRouteEvents {
		if event.SourceEventID != completed.ID() {
			continue
		}
		if len(event.DerivedRecipients) != 1 || event.DerivedRecipients[0].Path != finalMeta.FlowInstance || event.DerivedRecipients[0].HandlerNode() != identitytest.FlowNode(t, finalMeta.FlowTemplate, "collector") {
			t.Fatalf("static child escaped selected constructor parent: %+v", event)
		}
		return
	}
	t.Fatal("static completed source lost its selected route evidence")
}

func t17FixedCutTopologyFixture(t *testing.T) (semanticview.Source, runfork.RunForkPlan, []events.Event) {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleNestedTemplates(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	run := uuid.NewString()
	root := flowidentity.Stored(source, ".", run, run, flowidentity.EntityID(run), "")
	outer, err := flowidentity.KeylessChild(source, root, "outer")
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: run}
	t17AddConstructor(t, &plan, source, root, pipeline.FlowConstructionInput{}, nil)
	t17AddConstructor(t, &plan, source, outer, pipeline.FlowConstructionInput{}, nil)
	var selectors []events.Event
	for _, side := range []string{"left", "right"} {
		start := t17SelectorEvent(t, source, run, uuid.NewString(), side+".loop.start", ".", side+".loop.start", eventtest.RootRoutingSource(run), []byte(`{"case_id":"shared","seed":true}`))
		loop := t17ReceiverInstance(t, source, start, outer)
		t17AddConstructor(t, &plan, source, loop, pipeline.FlowConstructionInput{EventID: start.ID(), Input: "loop.start"}, map[string]any{"case_id": "shared"})
		selectors = append(selectors, start)
		plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: start.ID(), EventName: string(start.Type()), RoutingSource: start.RoutingSource(), FlowInstance: loop.InstancePath, Classification: runfork.RunForkPendingClassificationDeliveredCompleted})
		escaped := t17SelectorEvent(t, source, run, uuid.NewString(), loop.InstancePath+"/loop.escaped", loop.TemplateID, "loop.escaped", eventtest.ConcreteTemplateRoutingSource(loop.TemplateID, loop.InstancePath, loop.EntityID), []byte(`{"revision_id":"revision-one"}`))
		sink := t17ReceiverInstance(t, source, escaped, loop)
		t17AddConstructor(t, &plan, source, sink, pipeline.FlowConstructionInput{EventID: escaped.ID(), Input: "loop.escaped"}, map[string]any{"revision_id": "revision-one"})
		final, err := flowidentity.KeylessChild(source, sink, sink.TemplateID+"/final")
		if err != nil {
			t.Fatal(err)
		}
		t17AddConstructor(t, &plan, source, final, pipeline.FlowConstructionInput{EventID: escaped.ID()}, nil)
		selectors = append(selectors, escaped)
		plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: escaped.ID(), EventName: string(escaped.Type()), RoutingSource: escaped.RoutingSource(), FlowInstance: sink.InstancePath, Classification: runfork.RunForkPendingClassificationDeliveredCompleted})
	}
	frontier := t17SelectorEvent(t, source, run, uuid.NewString(), "work.observed", ".", "work.observed", eventtest.RootRoutingSource(run), []byte(`{"seed":true}`))
	selectors = append(selectors, frontier)
	plan.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Input: frontier.ID(), EventID: frontier.ID(), EventName: string(frontier.Type()), RoutingSource: frontier.RoutingSource(), Revision: 97, Timestamp: frontier.CreatedAt()}
	plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: frontier.ID(), EventName: string(frontier.Type()), RoutingSource: frontier.RoutingSource(), Classification: runfork.RunForkPendingClassificationPending, SubscriberType: "node", SubscriberID: identitytest.FlowNode(t, ".", "controller").Key(), FlowInstance: run})
	var ids []string
	for _, selector := range selectors {
		ids = append(ids, selector.ID())
	}
	plan = plan.WithHistoricalEvents(97, ids)
	plan, err = plan.WithHistoricalRouteSelectors(97, selectors)
	if err != nil {
		t.Fatal(err)
	}
	return source, plan, selectors
}

func t17SelectorEvent(t *testing.T, source semanticview.Source, run, id, name, flow, local string, routing events.RoutingSource, payload []byte) events.Event {
	t.Helper()
	schema, found, err := source.ResolveEffectiveCompiledFlowEventSchema(flow, local)
	if err != nil || !found {
		t.Fatalf("selector schema %s/%s: %v", flow, local, err)
	}
	bundle, _ := semanticview.Bundle(source)
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{BundleHash: hash, FlowID: schema.FlowPath(), EventKey: schema.EventName(), SchemaDigest: schema.AcceptanceSchemaDigest(), SchemaClass: events.PayloadSchemaAuthored})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := events.NewPayloadAdmission(payload, binding)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(id, events.EventType(name), "operator", "", payload, 0, run, events.EventEnvelope{}, routing, time.Unix(1700000000, 0).UTC())
	event, err = events.ApplyPayloadAdmission(event, admission)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func t17ReceiverInstance(t *testing.T, source semanticview.Source, event events.Event, parent flowidentity.Instance) flowidentity.Instance {
	t.Helper()
	sourceEvent, err := pinrouting.AdmitSourceEvent(event.Type(), event.RoutingSource())
	if err != nil {
		t.Fatal(err)
	}
	matched := pinrouting.CompileConnectGraph(source).MatchingSourceEvent(sourceEvent)
	if len(matched) != 1 {
		t.Fatalf("selector has %d receivers", len(matched))
	}
	var payload map[string]any
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for field, value := range payload {
		if text, ok := value.(string); ok {
			values["payload."+field] = text
		}
	}
	material, failure := pinrouting.InstanceKeyMaterialForConnectRoutePlan(matched[0], pinrouting.AdmitConnectRouteMatchValues(values))
	if !failure.Empty() {
		t.Fatal(failure.Code())
	}
	digest := matched[0].ReceiverKeyDigest(material.Keys)
	instance, err := flowidentity.KeyedChild(source, parent, matched[0].ReceiverEndpoint().Readback().FlowID, "ti-"+digest[:24])
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func t17AddConstructor(t *testing.T, plan *runfork.RunForkPlan, source semanticview.Source, instance flowidentity.Instance, input pipeline.FlowConstructionInput, fields map[string]any) {
	t.Helper()
	bundle, _ := semanticview.Bundle(source)
	hash, err := contracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := entityruntime.ResolveForFlow(source, instance.TemplateID)
	scope, _ := source.FlowScopeByID(instance.TemplateID)
	var key string
	flow, _ := source.FlowSchemaByID(instance.TemplateID)
	if !flow.Instance.Empty() {
		key, err = pipeline.AdmitFlowInstanceKey(source, instance.TemplateID, fields[flow.Instance.Path()])
		if err != nil {
			t.Fatal(err)
		}
	}
	graph, found := semanticview.WorkflowStageTopology(source, instance.TemplateID)
	if !found {
		t.Fatal("constructor has no lifecycle")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1700000000, 0).UTC()
	activation := pipeline.FlowInstanceActivationPlan{Identity: instance, CreatingInput: input, OccurredAt: at,
		Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{Identity: instance, RunID: plan.SourceRunID, BundleHash: hash, WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		Instance: pipeline.WorkflowInstance{InstanceID: instance.InstanceID, StorageRef: instance.InstancePath, EntityID: instance.EntityID, EntityType: contract.EntityType, Fields: fields,
			InstanceKind: scope.Mode, Mode: scope.Mode, InstanceKey: key, Status: "active", WorkflowName: instance.TemplateID, WorkflowVersion: source.WorkflowVersion(), CurrentState: initial.ID(), StageDefined: graph.StageCount() != 0, EnteredStageAt: at, CreatedAt: at,
			ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance, ParentEntityID: instance.ParentEntityID}}
	record, err := activation.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	plan.Entities = append(plan.Entities, runfork.RunForkEntityState{EntityID: instance.EntityID, CurrentState: initial.ID(), EnteredStateAt: &at, Fields: fields,
		MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance, FlowTemplate: instance.TemplateID, FlowInstance: instance.InstancePath, ParentInstance: instance.ParentRoute.FlowInstance, InstanceKey: key, Mode: scope.Mode, EntityType: contract.EntityType, FlowConfig: record.Config, InitialMaterialization: record.InitialMaterialization}})
}
