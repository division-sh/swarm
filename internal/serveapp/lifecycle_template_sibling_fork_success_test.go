package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Keep the original T17 success oracle active. Construction is read through
// the canonical header reader rather than reproducing the old fixture's join.
func TestServedCompiledTransitionTemplateSiblingForkOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			executions := make(chan lifecycleTemplateForkObservation, 1)
			failureReadback := make(chan func(), 1)
			defer func() {
				select {
				case readback := <-failureReadback:
					readback()
				default:
				}
			}()
			previous := buildSelectedAPICapabilities
			buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
				selected = owner
				caps, err := previous(owner, req)
				if executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor); err == nil && ok {
					family, available := owner.RunFork()
					if req.SelectedFork != nil {
						family, available = *req.SelectedFork, true
					}
					execute := executor.ExecuteSelectedContractRunFork
					executor.ExecuteSelectedContractRunFork = func(ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
						started := time.Now()
						result, err := execute(ctx, request)
						if err == nil && request.ForkOperation != nil {
							select {
							case executions <- lifecycleTemplateForkObservation{Request: *request.ForkOperation, Result: result}:
							default:
							}
						}
						if err != nil {
							// Keep the real owner error before JSON-RPC's fallback;
							// leave the request, result and error unchanged.
							t.Logf("T17_NATIVE_FORK_FAILURE call=runforkexecution.ExecuteSelectedContractRunFork source=%s cut=%s materialized=%s activated=%t elapsed=%s", request.SourceRunID,
								request.At, result.Materialization.ForkRunID, result.Activation.Activated, time.Since(started))
							logLifecycleTemplateForkCause(t, err, 0)
						}
						if available && (err != nil || result.ExecutedEventCount != 1) {
							select {
							case failureReadback <- func() {
								logLifecycleTemplateForkAccounting(t, owner.RuntimeDeps().DeliveryStore, request, result)
								logLifecycleTemplateForkTopology(t, family, request)
							}:
							default:
							}
						}
						return result, err
					}
					caps.RunFork = executor
				}
				return caps, err
			}
			t.Cleanup(func() { buildSelectedAPICapabilities = previous })
			publication := newExactOriginPublicationProbe(t)
			rt := startLifecycleTemplateRuntime(t, backend, canonicalrouting.CopyLifecycleNestedTemplates(t))
			runID, sourceEvent, siblings := prepareLifecycleTemplateSiblings(t, rt)
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, runID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "template-pause"})
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.observed", "run_id": runID, "source_event_id": sourceEvent, "payload": map[string]any{"seed": true}, "idempotency_key": "template-frontier"})
			waitServedEventPublishedLog(t, rt.Endpoint, runID, frontier.EventID)
			origins := []string{frontier.EventID}
			for _, sibling := range siblings {
				if len(sibling.publications) != 5 {
					t.Fatalf("sibling %s lost an acknowledged source publication: %v", sibling.side, sibling.publications)
				}
				origins = append(origins, sibling.publications...)
			}
			if err := waitExactOriginPublications(t.Context(), publication, origins...); err != nil {
				t.Fatal(err)
			}
			before := lifecycleStoredSnapshot(t, rt, runID)
			cut := requireLifecycleTemplatePendingCut(t, rt, selected, runID, frontier.EventID)
			params := map[string]any{"source_run_id": runID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "template-fork"}
			var fork, duplicate apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
			if fork.ForkRunID == "" || fork.ForkRunID == runID || !reflect.DeepEqual(fork, duplicate) || fork.ExecutedEventCount != 0 ||
				fork.ForkRunStatus != runfork.RunForkActivatedStatus || fork.ForkEventID != frontier.EventID || fork.ForkRevision != cut.Revision ||
				fork.ForkPointKind != string(cut.Kind) || fork.BundleHash != rt.BundleHash || fork.SourceRunID != runID {
				t.Fatalf("template fork=%#v duplicate=%#v", fork, duplicate)
			}
			var observed lifecycleTemplateForkObservation
			select {
			case observed = <-executions:
			default:
				t.Fatal("T17 acknowledgment lacks its actual retained executor invocation")
			}
			if observed.Result.ExecutedEventCount != 0 || len(observed.Result.ForkEvents) != 0 ||
				!observed.Result.Activation.Activated || observed.Result.Materialization.SelectedContractBinding == nil {
				t.Fatalf("T17 activation incorrectly reports completed continuation: %+v", observed.Result)
			}
			family, _ := selected.RunFork()
			ack, found, err := family.Operations().LoadForkOperationByID(t.Context(), observed.Request.OperationID)
			if err != nil || !found || ack.Validate() != nil || ack.Status != runfork.ForkOperationActivated || ack.Result == nil ||
				ack.ForkRunID != fork.ForkRunID || ack.BindingID != observed.Result.Materialization.SelectedContractBinding.BindingID ||
				ack.Request.SourceRunID != runID || ack.Request.ForkEventID != frontier.EventID || ack.Request.IdempotencyKey != "template-fork" ||
				ack.Request.ResolvedPoint == nil || *ack.Request.ResolvedPoint != cut || ack.Result.ExecutedEventCount != 0 {
				t.Fatalf("T17 did not acknowledge its exact operation/cut/binding: %+v found=%t error=%v", ack, found, err)
			}
			completed := requireLifecycleTemplateFrontierDelivery(t, selected, runID, fork.ForkRunID, frontier.EventID, observed.Result.Owner)
			instances, err := rt.ReceiverStateReader.ListWorkflowInstances(t.Context(), fork.ForkRunID)
			if err != nil {
				t.Fatal(err)
			}
			var children []lifecycleTemplateSibling
			for _, parent := range siblings {
				child, matches := parent, 0
				for _, instance := range instances {
					if instance.WorkflowName == parent.flow+"/sink" && instance.CurrentState == "review" {
						child.gateEntity, child.gateInstance = instance.EntityID, instance.StorageRef
						matches++
					}
				}
				if matches != 1 {
					t.Fatalf("child lacks its unique logical gate owner %s: matches=%d instances=%+v", parent.flow+"/sink", matches, instances)
				}
				child.gate = readLifecycleTemplateGate(t, rt, fork.ForkRunID, child.gateEntity, child.flow+"/sink")
				projected, err := runfork.ProjectEntityOwnership(runID, fork.ForkRunID, parent.gateEntity, parent.gateInstance)
				if err != nil || child.gateEntity != projected.Fork.EntityID || child.gateInstance != projected.Fork.FlowInstance ||
					child.gate.CardID == parent.gate.CardID || child.gate.ActivationID == parent.gate.ActivationID ||
					child.gate.RoutesJSON != parent.gate.RoutesJSON || child.gate.BundleHash != parent.gate.BundleHash {
					t.Fatalf("fork did not remint ownership while preserving exact frozen source: parent=%#v child=%#v", parent, child)
				}
				child.decision = lifecycleDecisionParamsForCard(t, rt, child.gate.CardID, "approve")
				child.decision["idempotency_key"] = child.side + "-fork-decide"
				t.Logf("TEMPLATE_FORK source_run=%s fork_run=%s side=%s parent_card=%s child_card=%s parent_activation=%s child_activation=%s parent_instance=%s child_instance=%s routes=%s", runID, fork.ForkRunID, child.side, parent.gate.CardID, child.gate.CardID, parent.gate.ActivationID, child.gate.ActivationID, parent.gateInstance, child.gateInstance, child.gate.RoutesJSON)
				children = append(children, child)
			}
			if children[0].gate.CardID == children[1].gate.CardID || children[0].gate.ActivationID == children[1].gate.ActivationID || children[0].gateInstance == children[1].gateInstance {
				t.Fatal("fork sibling gates aliased")
			}
			completeLifecycleTemplateSiblings(t, rt, fork.ForkRunID, children)
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
			if lifecycleStoredSnapshot(t, rt, runID) != before {
				t.Fatal("fork sibling verdict changed parent state/history")
			}
			for _, parent := range siblings {
				requireLifecycleEventCount(t, rt, runID, parent.flow+"/sink/work.completed", 0)
				pending := readLifecycleTemplateGate(t, rt, runID, parent.gateEntity, parent.flow+"/sink")
				if !reflect.DeepEqual(pending, parent.gate) {
					t.Fatalf("fork mutated parent frozen gate: before=%#v after=%#v", parent.gate, pending)
				}
				params := lifecycleDecisionParamsForCard(t, rt, parent.gate.CardID, "approve")
				if params["observed_content_hash"] != parent.decision["observed_content_hash"] {
					t.Fatal("fork changed parent card contents")
				}
			}
			var lateReplay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &lateReplay)
			if !reflect.DeepEqual(fork, lateReplay) {
				t.Fatalf("T17 continuation changed the permanent acknowledgment: before=%+v after=%+v", fork, lateReplay)
			}
			ackAfter, found, err := family.Operations().LoadForkOperationByID(t.Context(), ack.Request.OperationID)
			if err != nil || !found || !reflect.DeepEqual(ack, ackAfter) {
				t.Fatalf("T17 completed continuation rewrote its operation: %+v found=%t error=%v", ackAfter, found, err)
			}
			current, err := selected.RuntimeDeps().DeliveryStore.Snapshot(t.Context(), completed.DeliveryID)
			if err != nil || !reflect.DeepEqual(completed, current) {
				t.Fatalf("T17 replay duplicated or changed its exact completed delivery: %+v error=%v", current, err)
			}
			requireLifecycleEventCount(t, rt, fork.ForkRunID, "work.observed", 1)
		})
	}
}

type lifecycleTemplateForkObservation struct {
	Request runfork.ForkOperationRequest
	Result  runforkexecution.SelectedContractExecutionResult
}

func requireLifecycleTemplatePendingCut(t *testing.T, rt servedControlProofRuntime, selected *selectedStoreOwner, runID, eventID string) runfork.RunForkPoint {
	t.Helper()
	if selected == nil {
		t.Fatal("T17 requires its original selected store")
	}
	ctx := t.Context()
	family, found := selected.RunFork()
	if !found {
		t.Fatal("T17 requires its selected fork family")
	}
	plan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatal(err)
	}
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{
		RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore()}
	loaded, err := loader.InspectRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{
		SourceRunID: runID, BundleHash: rt.BundleHash, Selection: selection})
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: selection})
	if err != nil || frontier.FrontierEventCount != 1 || len(frontier.FrontierEvents) != 1 || frontier.FrontierEvents[0].SourceEventID != eventID ||
		frontier.FrontierEvents[0].EventName != "work.observed" || len(frontier.FrontierEvents[0].DerivedRecipients) != 1 {
		t.Fatalf("T17 dropped the one owed frontier event: %+v error=%v", frontier, err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
	if err != nil {
		t.Fatal(err)
	}
	native := selected.RuntimeDeps().DeliveryStore
	deliveries, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, native, eventID)
	receipt, receiptErr := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, native, eventID)
	if err != nil || receiptErr != nil || len(deliveries) != 1 || deliveries[0].SubscriberType != "node" ||
		deliveries[0].SubscriberID != node.Key() || deliveries[0].Status != "pending" || deliveries[0].FlowInstance != runID ||
		deliveries[0].EntityID != runID || receipt.Found || frontier.FrontierEvents[0].DerivedRecipients[0].Recipient != events.MustNodeDeliveryRecipient(node) {
		t.Fatalf("T17 cut lacks its exact pending controller/no-receipt precondition: deliveries=%+v receipt=%+v errors=%v/%v", deliveries, receipt, err, receiptErr)
	}
	return plan.ForkPoint
}

func requireLifecycleTemplateFrontierDelivery(t *testing.T, selected *selectedStoreOwner, source, child, sourceEventID, authority string) deliverylifecycle.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), servedProofPollDeadline)
	defer cancel()
	native := selected.RuntimeDeps().DeliveryStore
	eventID := activityidentity.ForkLineageEventID(child, sourceEventID)
	node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
	if err != nil {
		t.Fatal(err)
	}
	for {
		event, found, err := storetest.ReadCanonicalEventRecord(ctx, native, eventID)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			lineage, present := event.SelectedForkLineage()
			if event.RunID() != child || event.Type() != "work.observed" || !present || lineage.DestinationRunID() != child ||
				lineage.SourceRunID() != source || lineage.SourceEventID() != sourceEventID || lineage.AuthorityStamp() != authority {
				t.Fatalf("T17 continuation lost its exact own publication/lineage: event=%s lineage=%+v", event.ID(), lineage)
			}
			rows, err := storetest.ReadReceiverDeliveryStorage(ctx, native, child)
			if err != nil {
				t.Fatal(err)
			}
			var exact []storetest.ReceiverDeliveryStorageEvidence
			for _, row := range rows {
				if row.EventID == eventID {
					exact = append(exact, row)
				}
			}
			if len(exact) > 1 {
				t.Fatalf("T17 continued the frontier to multiple deliveries: %+v", exact)
			}
			if len(exact) == 1 {
				delivery, err := native.Snapshot(ctx, exact[0].DeliveryID)
				receipt, receiptErr := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, native, eventID)
				if err != nil || receiptErr != nil || (delivery.Terminal() && delivery.Status != deliverylifecycle.StatusDelivered) {
					t.Fatalf("T17 exact child consumer failed: %+v receipt=%+v errors=%v/%v", delivery, receipt, err, receiptErr)
				}
				if delivery.Status == deliverylifecycle.StatusDelivered && receipt.Found {
					if delivery.RunID != child || delivery.EventID != eventID || delivery.SubscriberID != node.Key() || delivery.ClaimVersion != 1 ||
						delivery.RetryCount != 0 || delivery.Failure != nil || delivery.SettledAt.IsZero() ||
						receipt.Outcome != "success" || len(receipt.Failure) != 0 {
						t.Fatalf("T17 lacks its once-only delivered claim/success receipt: %+v receipt=%+v", delivery, receipt)
					}
					page, err := selected.Observability().ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
						Filter: operatorread.OperatorEventListFilter{RunID: child, EventName: "work.observed"}, Limit: 2})
					if err != nil || len(page.Events) != 1 || page.NextCursor != "" || page.Events[0].EventID != eventID {
						t.Fatalf("T17 duplicate physical continuation publication: %+v error=%v", page, err)
					}
					return delivery
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("T17 exact publication/claim/receipt did not complete: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func logLifecycleTemplateForkCause(t *testing.T, err error, depth int) {
	t.Helper()
	if err == nil {
		return
	}
	t.Logf("T17_NATIVE_FORK_CAUSE depth=%d type=%T error=%v", depth, err, err)
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			logLifecycleTemplateForkCause(t, cause, depth+1)
		}
		return
	}
	logLifecycleTemplateForkCause(t, errors.Unwrap(err), depth+1)
}

func logLifecycleTemplateForkAccounting(t *testing.T, selected any, request runforkexecution.SelectedContractExecutionRequest, result runforkexecution.SelectedContractExecutionResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delivery, deliveryErr := storetest.ReadEventDeliveryDiagnosticRows(ctx, selected, request.At)
	pipelineReceipt, receiptErr := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, selected, request.At)
	t.Logf("T17_ACCOUNTING executed=%d fork_events=%+v source_deliveries=%+v source_pipeline_receipt=%+v read_errors=%v/%v",
		result.ExecutedEventCount, result.ForkEvents, delivery, pipelineReceipt, deliveryErr, receiptErr)
}

// This runs only after the public HTTP failure has returned, before serve
// cleanup. It reads the recorded cut without another Prepare or execution grant.
func logLifecycleTemplateForkTopology(t *testing.T, family storeselected.RunFork, request runforkexecution.SelectedContractExecutionRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	planRequest := runfork.RunForkPlanRequest{SourceRunID: request.SourceRunID, At: request.At, AtStart: request.AtStart}
	if request.ForkOperation == nil || request.At == "" || request.AtStart {
		t.Log("T17_TOPOLOGY_READBACK missing exact public event-cut operation")
		return
	}
	if request.ForkOperation.ResolvedPoint != nil {
		point := *request.ForkOperation.ResolvedPoint
		planRequest.ResolvedPoint = &point
	}
	plan, err := family.Plan(ctx, planRequest)
	if err != nil || plan.SourceRunID != request.SourceRunID || plan.ForkPoint.EventID != request.At ||
		(planRequest.ResolvedPoint != nil && plan.ForkPoint != *planRequest.ResolvedPoint) {
		t.Logf("T17_TOPOLOGY_READBACK call=selected.RunFork.Plan source=%s cut=%s actual=%+v error=%v", request.SourceRunID, request.At, plan.ForkPoint, err)
		return
	}
	point := plan.ForkPoint
	inspector, ok := request.SourceLoader.(interface {
		InspectRunForkSelectedContractSourceForRequest(context.Context, runforkexecution.SelectedContractSourceLoadRequest) (runforkexecution.LoadedSelectedContractSource, error)
	})
	if !ok {
		t.Log("T17_TOPOLOGY_READBACK deployed source loader lacks read-only inspection")
		return
	}
	loaded, err := inspector.InspectRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{
		SourceRunID: request.SourceRunID, BundleHash: request.ExpectedBundleHash, Selection: request.ContractSelection})
	if err != nil {
		t.Logf("T17_TOPOLOGY_READBACK call=InspectRunForkSelectedContractSourceForRequest error=%v", err)
		return
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: loaded.Selection})
	if err != nil {
		t.Logf("T17_TOPOLOGY_READBACK call=AdmitContractFrontier error=%v", err)
		return
	}
	routes, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: loaded.Selection, FrontierAdmission: frontier})
	if err != nil {
		t.Logf("T17_TOPOLOGY_READBACK call=AdmitSelectedContractRouteHistory error=%v", err)
		return
	}
	topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{
		Admission: frontier, RouteAdmission: routes})
	if err != nil {
		t.Logf("T17_TOPOLOGY_READBACK call=BuildSelectedContractRouteTopology error=%v", err)
		return
	}
	t.Logf("T17_TOPOLOGY_READBACK point=%+v route_history=%+v static=%t dynamic=%t instances=%v proofs=%+v plan_unsupported_cases=%+v route_unsupported_cases=%+v topology_unsupported_cases=%+v",
		point, plan.RouteHistory, topology.StaticTopologySupported, topology.DynamicTopologySupported, topology.DynamicFlowInstances,
		topology.DynamicTopologyProofs, plan.UnsupportedBlockers, routes.UnsupportedBlockers, topology.UnsupportedBlockers)
	t.Logf("T17_TOPOLOGY_FRONTIER events=%+v", frontier.FrontierEvents)
	t.Logf("T17_TOPOLOGY_PENDING count=%d work=%+v inputs=%+v", plan.PendingWorkCount, plan.PendingWork, plan.HistoricalInputCoordinates())
	instances, err := runforkadmission.ConstructedInstances(loaded.Source, plan)
	if err != nil {
		t.Logf("T17_TOPOLOGY_READBACK call=ConstructedInstances error=%v", err)
		return
	}
	table, err := bus.DeriveRouteTable(loaded.Source)
	if err != nil {
		t.Logf("T17_TOPOLOGY_READBACK call=DeriveRouteTable error=%v", err)
		return
	}
	for _, instance := range instances {
		t.Logf("T17_SOURCE_CONSTRUCTOR instance=%+v", instance)
		identity, err := flowidentity.NewRunScopedFlowInstance(plan.SourceRunID, instance.Route())
		if err != nil {
			t.Logf("T17_TOPOLOGY_READBACK call=NewRunScopedFlowInstance error=%v", err)
			return
		}
		if err := table.AddFlowInstanceRoute(bus.FlowInstanceRouteMaterializationRequest{Identity: identity, Instance: instance}); err != nil {
			t.Logf("T17_TOPOLOGY_READBACK call=RouteTable.AddFlowInstanceRoute error=%v", err)
			return
		}
	}
	for _, entity := range plan.Entities {
		t.Logf("T17_SOURCE_CONSTRUCTOR_ENTITY entity=%s fields=%+v materialization=%+v", entity.EntityID, entity.Fields, entity.MaterializationMetadata)
	}
	sources := map[string]events.RoutingSource{plan.ForkPoint.EventID: plan.ForkPoint.RoutingSource}
	for _, item := range plan.PendingWork {
		if !item.RoutingSource.Empty() {
			sources[item.EventID] = item.RoutingSource
		}
	}
	graph := pinrouting.CompileConnectGraph(loaded.Source)
	for _, event := range routes.SelectedRouteEvents {
		if event.Disposition != runfork.RunForkSelectedContractDispositionFailClosed {
			continue
		}
		t.Logf("T17_TOPOLOGY_UNSUPPORTED_ROUTE event=%+v source=%+v", event, sources[event.SourceEventID])
		sourceEvent, err := pinrouting.AdmitSourceEvent(events.EventType(event.EventName), sources[event.SourceEventID])
		if err != nil {
			t.Logf("T17_TOPOLOGY_READBACK call=AdmitSourceEvent event=%s error=%v", event.SourceEventID, err)
			continue
		}
		for _, receiver := range graph.MatchingSourceEvent(sourceEvent) {
			if !receiver.RequiresRuntimeResolution() {
				continue
			}
			// Fail-closed runtime resolution has no admitted target. Observe
			// that evaluator state, not a guessed child from the census.
			evaluation := table.EvaluateConnectPlan(plan.SourceRunID, receiver, nil)
			ledger, err := evaluation.Ledger()
			t.Logf("T17_RECEIVER_CALL call=RouteTable.EvaluateConnectPlan event=%s receiver=%+v input_targets=nil matched=%t requires_runtime_resolution=%t recipients=%d error=%v",
				event.SourceEventID, receiver.Readback(), evaluation.Matched(), evaluation.RequiresRuntimeResolution(), len(evaluation.Recipients()), err)
			for _, outcome := range ledger.Plans() {
				t.Logf("T17_RECEIVER_CASE plan=%s resolution=%s targets=%+v candidates=%+v", outcome.PlanIdentity(), outcome.Resolution().Code(), outcome.Targets(), outcome.Candidates())
			}
		}
	}
}
