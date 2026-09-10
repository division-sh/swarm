package pipeline_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
			for _, keyRelation := range []string{"agreement", "conflict", "later_match"} {
				t.Run(storeCase.name+"/"+acquisition+"/"+keyRelation, func(t *testing.T) {
					selected := storeCase.open(t)
					runID := uuid.NewString()
					insertGateRecoveryRun(t, selected, runID)
					ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))

					source, node := targetedDeclaredKeyExecutionSource(t, acquisition)
					module := proposedEffectProofModule{
						source: source,
						nodes: []runtimepipeline.WorkflowNode{{
							Node: node, Subscriptions: []events.EventType{"review/work.keyed"},
							ExecutionType: runtimecontracts.SystemNodeExecutionType,
							Policies:      map[string]runtimepipeline.WorkflowEventPolicy{"review/work.keyed": {Consume: true}},
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
					if keyRelation == "conflict" {
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
							EnteredStageAt: createdAt, CreatedAt: createdAt, Fields: map[string]any{"account_id": exactKey, "owner": "exact"},
							EntityType: "review_entity",
						},
						{
							InstanceID: competingRoute.InstanceID, StorageRef: competingPath, EntityID: competingEntityID,
							WorkflowName: "review", WorkflowVersion: source.WorkflowVersion(), Mode: "template", CurrentState: "active",
							EnteredStageAt: createdAt, CreatedAt: createdAt, Fields: map[string]any{"account_id": payloadKey, "owner": "competing"},
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
						if _, err := coordinator.MaterializeInitialEntry(ctx, instance, createdAt); err != nil {
							t.Fatalf("materialize %s: %v", instance.Fields["owner"], err)
						}
						if err := coordinator.MarkDynamicFlowRuntimeTopologyReady(ctx, readiness, createdAt); err != nil {
							t.Fatalf("mark %s topology ready: %v", instance.Fields["owner"], err)
						}
						if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.RouteForInstancePath(instance.StorageRef)}); err != nil {
							t.Fatalf("publish %s route: %v", instance.Fields["owner"], err)
						}
					}
					materialize(instances[0])
					if keyRelation != "later_match" {
						materialize(instances[1])
					}

					payload, err := json.Marshal(map[string]any{"account_id": payloadKey, "item": "accepted"})
					if err != nil {
						t.Fatal(err)
					}
					target := events.RouteIdentity{FlowID: "review", FlowInstance: exactPath, EntityID: exactEntityID}
					event := eventtest.ExistingRunRootIngress(
						uuid.NewString(), "review/work.keyed", "operator", "", payload, 0, runID,
						events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), createdAt.Add(time.Minute),
					)
					plan, err := eventBus.CheckPublishRecipientPlan(ctx, event)
					if err != nil {
						t.Fatalf("plan targeted declared-key delivery: %v", err)
					}
					if len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Recipient.ID() != node.Key() ||
						!plan.DeliveryRoutes[0].Target.ExistingEntity() || plan.DeliveryRoutes[0].Target.Route() != target {
						t.Fatalf("targeted declared-key plan = %#v, want exact owner %#v", plan.DeliveryRoutes, target)
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
					delivery, err := events.NewDeliveryEvent(prepared.Event.Event(), prepared.DeliveryRoutes[0])
					if err != nil {
						t.Fatalf("construct targeted declared-key delivery: %v", err)
					}
					forward, _, outcome, executionErr := coordinator.InterceptDeliveryRoute(ctx, delivery, prepared.DeliveryRoutes[0])
					if forward {
						t.Fatal("targeted declared-key delivery was forwarded instead of consumed")
					}

					exact, exactFound, exactErr := coordinator.Load(ctx, exactRoute)
					competing, competingFound, competingErr := coordinator.Load(ctx, competingRoute)
					if exactErr != nil || !exactFound || competingErr != nil || !competingFound {
						t.Fatalf("load declared-key owners: exact=%t/%v competing=%t/%v", exactFound, exactErr, competingFound, competingErr)
					}
					if keyRelation != "conflict" {
						if executionErr != nil {
							t.Fatalf("execute exact key agreement: %v", executionErr)
						}
						if _, disposed := outcome.Disposition(); disposed {
							t.Fatalf("exact key agreement disposition = %#v", outcome)
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
						if want := acquisition + "_entity_conflict"; !strings.Contains(failureText, want) {
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
	bundle := admitTargetedDeclaredKeyContract(t, acquisition)
	source := semanticview.Wrap(bundle)
	node := externalPipelineSourceNode(t, source, "review", "key-consumer")
	return source, node
}

func admitTargetedDeclaredKeyContract(t *testing.T, acquisition string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{

		"schema.yaml":          "name: declared-key-execution\n",
		"review/schema.yaml":   "name: review\nmode: template\nstages:\n  active: {initial: true}\n  done: {terminal: true}\n",
		"review/entities.yaml": "review_entity:\n  account_id: text\n",
		"review/events.yaml":   "work.keyed:\n  account_id: text\n  item: text\n",
		"review/nodes.yaml":    "key-consumer:\n  id: key-consumer\n  execution_type: system_node\n  subscribes_to: [work.keyed]\n  event_handlers:\n    work.keyed:\n      " + acquisition + "_entity:\n        by: {account_id: payload.account_id}\n      accumulate: {into: items, from: payload}\n",
	}
	for relative, body := range files {
		path := filepath.Join(root, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create declared-key contract directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write declared-key contract: %v", err)
		}
	}
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	admitted, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load declared-key contract: %v", err)
	}
	return admitted
}
