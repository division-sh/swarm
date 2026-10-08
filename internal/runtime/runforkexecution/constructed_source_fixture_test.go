package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

// Real constructor preparation and tree commit establish the source history.
// Empty route projections do not prove physical runtime attachment or public launch.
func seedSelectedConstructedRootHistory(t *testing.T, ctx context.Context, db *sql.DB, selected *store.PostgresStore, loaded LoadedSelectedContractSource, runID, eventID, eventName string, at time.Time, mode executionmode.Mode, routes []events.DeliveryRoute, inputs ...selectedExecutionInputFixture) events.Event {
	t.Helper()
	bundle, found := semanticview.Bundle(loaded.Source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("source constructor requires its admitted artifact")
	}
	ctx = effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact), mode)
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{
		Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Minute),
		Source: loaded.SourceArtifactFact, Artifact: bundle.SourceArtifact,
	})
	payload, err := json.Marshal(map[string]any{"entity_id": runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) > 1 {
		t.Fatal("source history accepts one exact payload")
	}
	if len(inputs) == 1 && len(inputs[0].payload) != 0 {
		payload = inputs[0].payload
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, events.EventType(eventName), "source-runtime", "", payload, 0, runID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID), events.NoRoutingSource(), at, mode)
	if len(inputs) == 1 && inputs[0].flow != "" {
		event, err = eventtest.AdmitPayload(event, inputs[0].flow, eventName)
		if err != nil {
			t.Fatal(err)
		}
	}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, routes, pipelineobligation.ScopeSubscribed)
	posture := executionposture.Live
	if mode == executionmode.Mock {
		posture = executionposture.MockOnly
	}
	options, _, err := selectedContractAgentModelOptions(SelectedContractAgentRuntimeOptions{
		ExecutionPosture: posture, Config: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
	})
	if err != nil {
		t.Fatal(err)
	}
	work, found := worklifetime.OccurrenceFromContext(ctx)
	if !found {
		t.Fatal("source constructor requires its owned runtime occurrence")
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	sourceBus, err := bus.NewEventBusWithOptions(selected, bus.EventBusOptions{
		ExecutionPosture: posture, WorkOwner: work, PipelineObligations: selected.PipelineObligations(),
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact,
		RuntimeInstanceID: runForkTestRuntimeInstanceID, DeliveryAuthority: authority, ReceiverExecution: eventreceiver.NormalExecution(),
		Durable: bus.DurableDependencies{
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
			ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, PreparedEvents: selected,
			TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected, ConstructionPublications: selected,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(sourceBus, pipeline.PipelineCoordinatorOptions{
		Module: selectedContractWorkflowModule{source: loaded.Source}, Persistence: pipeline.NewWorkflowPersistence(selected),
		SourceArtifactFact: loaded.SourceArtifactFact, ExecutionPosture: posture, ReceiverExecution: eventreceiver.NormalExecution(),
		RunLifecycle: selected, PipelineObligations: selected.PipelineObligations(), DeliveryStore: selected, DeadLetters: selected,
		DecisionCards: selected, ProposedEffects: selected, HumanTasks: selected, DecisionCardDraftExpiry: selected, HumanTaskExpiry: selected,
		DeliveryRuntime: sourceBus, WorkOwner: work,
	})
	if coordinator == nil {
		t.Fatal("source constructor coordinator was not admitted")
	}
	options.BaseContext, options.SemanticSource, options.SourceArtifactFact = ctx, loaded.Source, loaded.SourceArtifactFact
	options.WorkflowInstances = coordinator
	options.ReceiverExecution = eventreceiver.NormalExecution()
	planner := manager.NewAgentManagerWithOptions(nil, nil, options)
	identity := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source, Instance: identity, OccurredAt: at})
	if err != nil {
		t.Fatalf("prepare source constructor tree: %v", err)
	}
	command := bus.FlowInstanceActivationCommand{Plan: plan}
	for _, construction := range plan.ConstructionPlans() {
		command.RouteTopology = append(command.RouteTopology, bus.FlowInstanceRouteRecordSet{
			Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: construction.Identity.Route()},
		})
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Created || !committed.Acknowledged {
		t.Fatalf("commit source constructor tree: created=%v acknowledged=%v err=%v", committed.Created, committed.Acknowledged, err)
	}
	return event
}

func selectedConstructedRootNodeRoute(source semanticview.Source, runID, nodeID string) events.DeliveryRoute {
	return events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustRunForkRootNode(nodeID)),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: semanticview.RootExecutionFlowID(source), FlowInstance: runID, EntityID: runID,
		}),
	}
}

func seedSelectedConstructedAgentRootHistory(t *testing.T, ctx context.Context, db *sql.DB, selected *store.PostgresStore, loaded LoadedSelectedContractSource, runID, eventID string, at time.Time) {
	t.Helper()
	route := selectedExecutionTestAgentRoute(t, runID, "test-agent", runID)
	route.Target = events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: semanticview.RootExecutionFlowID(loaded.Source), FlowInstance: runID, EntityID: runID,
	})
	seedSelectedConstructedRootHistory(t, ctx, db, selected, loaded, runID, eventID, "task.assigned", at, executionmode.Live, []events.DeliveryRoute{route})
}
