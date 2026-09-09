package pipeline_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestTargetedDeclaredKeyAgreementAndConflictExecuteThroughDurableEventBusOnBothStores(t *testing.T) {
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		for _, acquisition := range []string{"select", "select_or_create"} {
			for _, keyRelation := range []string{"agreement", "business_difference", "conflict", "later_match"} {
				t.Run(storeCase.name+"/"+acquisition+"/"+keyRelation, func(t *testing.T) {
					selected := storeCase.open(t)
					runID := uuid.NewString()
					insertGateRecoveryRun(t, selected, runID)
					ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))

					source, node := targetedDeclaredKeyExecutionSource(t, acquisition)
					module := proposedEffectProofModule{
						source: source,
						workflow: runtimepipeline.NewWorkflowDefinition("review", []runtimepipeline.WorkflowStage{
							{Name: "active"},
							{Name: "done", Terminal: true},
						}, nil),
						nodes: []runtimepipeline.WorkflowNode{{
							Node: node, Subscriptions: []events.EventType{"work.keyed"},
							ExecutionType: runtimecontracts.SystemNodeExecutionType,
							Policies:      map[string]runtimepipeline.WorkflowEventPolicy{"work.keyed": {Consume: true}},
						}},
					}
					eventBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
					if err != nil {
						t.Fatalf("new declared-key EventBus: %v", err)
					}
					coordinator := newGateRecoveryCoordinator(eventBus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: module})

					exactPath := "review/" + uuid.NewString()
					exactRoute := runtimeflowidentity.RouteForInstancePath(exactPath)
					exactEntityID := runtimeflowidentity.EntityID(exactPath)
					competingPath := "review/" + uuid.NewString()
					competingRoute := runtimeflowidentity.RouteForInstancePath(competingPath)
					competingEntityID := runtimeflowidentity.EntityID(competingPath)
					payloadKey := "payload-key"
					exactKey := payloadKey
					if keyRelation == "business_difference" {
						exactKey = "different-exact-key"
					}
					createdAt := time.Now().UTC()
					sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
					if !ok {
						t.Fatal("declared-key execution context is missing bundle source fact")
					}
					bundleHash := sourceFact.BundleHash()
					instances := []runtimepipeline.WorkflowInstance{
						{
							InstanceID: exactRoute.InstanceID, StorageRef: exactPath, EntityID: exactEntityID,
							WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), Mode: "template", CurrentState: "active",
							EnteredStageAt: createdAt, CreatedAt: createdAt, Fields: map[string]any{"receiver_id": exactRoute.InstanceID, "account_id": exactKey, "owner": "exact"},
							EntityType: "review_entity",
						},
						{
							InstanceID: competingRoute.InstanceID, StorageRef: competingPath, EntityID: competingEntityID,
							WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), Mode: "template", CurrentState: "active",
							EnteredStageAt: createdAt, CreatedAt: createdAt, Fields: map[string]any{"receiver_id": competingRoute.InstanceID, "account_id": payloadKey, "owner": "competing"},
							EntityType: "review_entity",
						},
					}
					materialize := func(instance runtimepipeline.WorkflowInstance) {
						readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
							Identity: runtimeflowidentity.Instance{
								TemplateID: "review", ScopeKey: "review", InstanceID: instance.InstanceID,
								InstancePath: instance.StorageRef, EntityID: instance.EntityID, HasStoredPath: true,
							},
							RunID: runID, BundleHash: bundleHash,
							WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
						}
						instance.RuntimeReadiness = &readiness
						if _, err := coordinator.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceForRun(runID, instance.StorageRef), instance, createdAt); err != nil {
							t.Fatalf("materialize %s: %v", instance.Fields["owner"], err)
						}
						if err := coordinator.MarkDynamicFlowRuntimeTopologyReady(ctx, readiness, createdAt); err != nil {
							t.Fatalf("mark %s topology ready: %v", instance.Fields["owner"], err)
						}
						if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: testRunScopedWorkflowInstanceForRun(runID, instance.StorageRef)}); err != nil {
							t.Fatalf("publish %s route: %v", instance.Fields["owner"], err)
						}
					}
					materialize(instances[0])
					if keyRelation != "later_match" {
						materialize(instances[1])
					}

					payload, err := json.Marshal(map[string]any{"receiver_id": exactRoute.InstanceID, "account_id": payloadKey, "item": "accepted"})
					if err != nil {
						t.Fatal(err)
					}
					target := events.RouteIdentity{FlowID: "review", FlowInstance: exactPath, EntityID: exactEntityID}
					event := targetedDeclaredKeyPublication(t, ctx, eventBus, source, runID, payload, target, createdAt.Add(time.Minute))
					plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
					if err != nil {
						t.Fatalf("plan targeted declared-key delivery: %v", err)
					}
					if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != node.Key() ||
						!plan.DeliveryRoutes[0].Target.ExistingEntity() || plan.DeliveryRoutes[0].Target.Route() != target {
						t.Fatalf("targeted declared-key plan = %#v, want exact owner %#v", plan, target)
					}
					if err := eventBus.Publish(ctx, event); err != nil {
						t.Fatalf("persist targeted declared-key delivery: %v", err)
					}
					prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, event.ID())
					if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
						t.Fatalf("load targeted declared-key publication: found=%t routes=%#v err=%v", found, prepared.DeliveryRoutes, err)
					}
					if keyRelation == "later_match" {
						// The second matching receiver is created only after the actual
						// EventBus commit. Execution must validate, not rerun acquisition.
						materialize(instances[1])
					}
					if keyRelation == "conflict" {
						if _, err := selected.db.ExecContext(ctx, `UPDATE entity_state SET entity_type=$1 WHERE run_id=$2 AND entity_id=$3`, "wrong_entity_type", runID, exactEntityID); err != nil {
							t.Fatal(err)
						}
					}
					delivery, err := events.NewDeliveryEvent(prepared.Event.Event(), prepared.DeliveryRoutes[0])
					if err != nil {
						t.Fatalf("construct targeted declared-key delivery: %v", err)
					}
					forward, _, outcome, executionErr := coordinator.InterceptDeliveryRoute(ctx, delivery, prepared.DeliveryRoutes[0])
					if forward {
						t.Fatal("targeted declared-key delivery was forwarded instead of consumed")
					}

					exact, exactFound, exactErr := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, exactRoute.InstancePath))
					competing, competingFound, competingErr := coordinator.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, competingRoute.InstancePath))
					if exactErr != nil || !exactFound || competingErr != nil || !competingFound {
						t.Fatalf("load declared-key owners: exact=%t/%v competing=%t/%v", exactFound, exactErr, competingFound, competingErr)
					}
					if keyRelation != "conflict" {
						if executionErr != nil {
							t.Fatalf("execute exact key agreement: %v", executionErr)
						}
						if disposition, disposed := outcome.Disposition(); disposed {
							t.Fatalf("exact key agreement disposition = %s failure=%+v", disposition.Kind(), disposition.Failure())
						}
						if exact.Revision != 2 || competing.Revision != 1 || exact.Fields["owner"] != "exact" || competing.Fields["owner"] != "competing" {
							t.Fatalf("agreement mutations: exact=%#v competing=%#v", exact, competing)
						}
						reloaded, found, err := selected.events.LoadPreparedPublishEvent(ctx, event.ID())
						if err != nil || !found || len(reloaded.DeliveryRoutes) != 1 || reloaded.DeliveryRoutes[0].Target != prepared.DeliveryRoutes[0].Target {
							t.Fatalf("execution changed persisted target: found=%t err=%v routes=%#v", found, err, reloaded.DeliveryRoutes)
						}
						return
					}
					failureText := ""
					if executionErr != nil {
						failureText = executionErr.Error()
					} else if disposition, disposed := outcome.Disposition(); !disposed {
						t.Fatalf("key conflict executed without error or terminal disposition: %#v", outcome)
					} else if disposition.Kind() != runtimepipelineobligation.DispositionDeadLetter || disposition.Failure() == nil {
						t.Fatalf("key conflict disposition = %s failure=%#v, want dead letter", disposition.Kind(), disposition.Failure())
					} else {
						failure := disposition.Failure()
						if failure.Detail.Code != "unclassified_runtime_error" || failure.Component != "workflow-runtime" || failure.Operation != "execute_handler" {
							t.Fatalf("key conflict failure envelope = %#v", failure)
						}
						failureText = failure.Detail.Code
					}
					if executionErr != nil {
						if want := "entity_type"; !strings.Contains(failureText, want) {
							t.Fatalf("key conflict failure = %q, want %q", failureText, want)
						}
					}
					if exact.Revision != 1 || competing.Revision != 1 || exact.Fields["owner"] != "exact" || competing.Fields["owner"] != "competing" {
						t.Fatalf("key conflict mutated state: exact=%#v competing=%#v", exact, competing)
					}
				})
			}
		}
	}
}

func targetedDeclaredKeyExecutionSource(t *testing.T, acquisition string) (semanticview.Source, runtimeidentity.ExecutableNode) {
	t.Helper()
	root := canonicalrouting.CopyTargetedDeclaredKey(t, acquisition)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	node := externalPipelineSourceNode(t, source, "review", "key-consumer")
	return source, node
}

func targetedDeclaredKeyPublication(t *testing.T, ctx context.Context, bus *runtimebus.EventBus, source semanticview.Source, runID string, payload []byte, target events.RouteIdentity, at time.Time) events.Event {
	t.Helper()
	seed := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.requested", "operator", "", payload, 0, runID, events.EventEnvelope{}, at)
	if err := bus.Publish(ctx, seed); err != nil {
		t.Fatalf("publish declared root input: %v", err)
	}
	producer := externalPipelineSourceNode(t, source, "", "controller")
	return eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "work.keyed", eventtest.Producer(events.EventProducerNode, producer.Key()), "", payload, 0,
		events.LineageFromEvent(seed), events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), eventtest.RootRoutingSource(runID), at.Add(time.Second))
}
