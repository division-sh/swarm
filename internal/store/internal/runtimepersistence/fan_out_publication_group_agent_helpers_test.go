package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agents"
	"github.com/division-sh/swarm/internal/runtime/authority"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type publicationGroupAgentGate struct {
	hold    bool
	started chan struct{}
	once    sync.Once
	entries atomic.Int32
}

type publicationGroupGatedAgent struct {
	manager.Agent
	gate *publicationGroupAgentGate
}

func (a *publicationGroupGatedAgent) OnEvent(ctx context.Context, event events.Event) ([]events.Event, error) {
	a.gate.entries.Add(1)
	a.gate.once.Do(func() { close(a.gate.started) })
	if a.gate.hold {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a.Agent.OnEvent(ctx, event)
}

type publicationGroupSpendProjection struct{}

func (publicationGroupSpendProjection) ProjectCommittedCompletionSpend(context.Context, effects.CompletionSpendProjection) {
}

func publicationGroupRealAgentFactory(t *testing.T, selected any, eventBus *bus.EventBus, source semanticview.Source, gate *publicationGroupAgentGate) manager.AgentFactory {
	t.Helper()
	cfg := &config.Config{}
	cfg.LLM.Backend, cfg.LLM.Session.LockTTL = llmselection.BackendAnthropic, time.Minute
	profile, err := llmselection.ResolveLiveBackend(cfg.LLM.Backend)
	if err != nil {
		t.Fatal(err)
	}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{
		Cfg: cfg, Sessions: sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL), Conversations: selected.(llm.ConversationPersistence), Events: eventBus,
		LockOwner:            "publication-group-agent-crash",
		CompletionController: effects.NewCompletionController(selected.(effects.Store), selected.(effects.CompletionStore), selected.(effects.CompletionHeartbeatStore), publicationGroupSpendProjection{}).WithExecutionPosture(executionposture.MockOnly),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	provider := authority.NewSourceProvider(source)
	executor := runtimetools.NewExecutorWithOptions(eventBus, runtimetools.ExecutorOptions{Config: cfg, WorkflowSource: source, ModelRuntimes: runtimes, AuthorityProvider: provider, EmitRegistry: runtimetools.NewEmitRegistry(source, provider)})
	factory := agents.NewLLMAgentFactory(runtimes, executor, agents.LLMAgentOptions{})
	return func(cfg actors.AgentConfig) (manager.Agent, error) {
		agent, err := factory(cfg)
		if err != nil {
			return nil, err
		}
		return &publicationGroupGatedAgent{Agent: agent, gate: gate}, nil
	}
}

// Unlike the ordinary live seed helper, this creates mock causal lineage at
// admission. No persisted event or capsule is rewritten to change its mode.
func seedPublicationGroupAgentIntent(t *testing.T, backend string, fixture authorActivityReceiptFixture, source semanticview.Source) (context.Context, fanOutOwnerFixture) {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		t.Fatal("agent crash source requires its actual admitted bundle")
	}
	runID, at := uuid.NewString(), time.Now().UTC()
	ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
	node := mustPersistenceRootNode("fan-out-source")
	plans := source.FanOutPlansForHandler(node, "items.ready")
	if len(plans) != 1 {
		t.Fatal("agent crash requires exactly one declared fan-out")
	}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "items.ready", "fan-out-test", "", json.RawMessage(`{"items":["item-000","item-001","item-002"]}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at, executionmode.Mock)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
	selected := fixture.store.(storeTestDurableEventBusStore)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, trigger, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claim, err := claimDeliveryFixture(ctx, selected, trigger, route)
	if err != nil {
		t.Fatal(err)
	}
	seedWorkflowTargetStateForTransition(t, backend, fixture.db, runID, runID, runID, "pending", 1, at)
	if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET entity_type='root' WHERE run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	request := fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: claim.Claim.DeliveryID(), ElementRef: plans[0].Ref.ElementRef}, PlanRef: plans[0].Ref,
		Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: trigger.ID(), Field: "items"}, Cardinality: 3,
		Capsule: fanoutobligation.Capsule{NodeKey: node.Key(), ExecutionFlowID: ".", Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID,
			HandlerEventKey: "items.ready", CurrentState: "review", ProducerSource: trigger.RoutingSource(), Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: route.Target}, Lineage: events.LineageFromEvent(trigger)},
	}
	record := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "pending", 1, at)
	record.CurrentState, record.EntityType, record.Mode = "review", "root", "static"
	record.EnteredStageAt, record.UpdatedAt = at, at
	if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
		State: record, FanOutIntent: &request,
		DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}); err != nil {
		t.Fatal(err)
	}
	return ctx, fanOutOwnerFixture{runID: runID, eventID: trigger.ID(), deliveryID: claim.Claim.DeliveryID(), flowPath: plans[0].Ref.ElementRef.FlowPath, semanticPath: plans[0].Ref.ElementRef.SemanticPath, createdAt: at, bundleHash: bundle.SourceArtifact.BundleHash(), artifact: bundle.SourceArtifact}
}

type publicationGroupSignalCrash struct {
	bus.DeliveryContinuationOwner
	armed   atomic.Bool
	barrier func()
}

func (s *publicationGroupSignalCrash) Signal() {
	if s.armed.CompareAndSwap(true, false) {
		s.barrier()
	}
	s.DeliveryContinuationOwner.Signal()
}

type publicationGroupAgentCrashGroup struct {
	pipelineobligation.PublicationGroup
	control *pipelineCrashConnector
	gate    *publicationGroupAgentGate
	signal  *publicationGroupSignalCrash
	cut     string
	before  func()
}

var _ pipelineobligation.PublicationGroup = publicationGroupAgentCrashGroup{}

func (g publicationGroupAgentCrashGroup) ValidateCommittedMembership(claims []pipelineobligation.Claim) error {
	return g.PublicationGroup.ValidateCommittedMembership(claims)
}

func (g publicationGroupAgentCrashGroup) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	if len(members) != 3 {
		return pipelineobligation.PublicationGroupOutcome{}, fmt.Errorf("agent crash requires three actual grouped members, got %d", len(members))
	}
	select {
	case <-g.gate.started:
	case <-ctx.Done():
		return pipelineobligation.PublicationGroupOutcome{}, ctx.Err()
	}
	g.before()
	if g.cut == "after_commit_before_ack" {
		g.control.armed.Store(true)
		ctx = context.WithValue(ctx, fanOutCrashCommitKey{}, g.control)
	}
	result, err := g.PublicationGroup.Settle(ctx, members)
	if err == nil && g.cut == "before_continuation_signal" {
		if len(result.Results) != 3 {
			return result, fmt.Errorf("agent signal cut requires three acknowledged members")
		}
		for _, member := range result.Results {
			if !member.Outcome.Committed() || !member.Outcome.DeliveryHandoffCommitted() {
				return result, fmt.Errorf("agent signal cut requires actual committed delivery handoff")
			}
		}
		g.signal.armed.Store(true)
	}
	return result, err
}

type publicationGroupAgentCrashOwner struct {
	pipeline.FanOutObligationOwner
	fault publicationGroupAgentCrashGroup
}

func (o publicationGroupAgentCrashOwner) BeginFanOutPublicationGroup(ctx context.Context, claim fanoutobligation.Claim) (pipelineobligation.PublicationGroup, error) {
	group, err := o.FanOutObligationOwner.BeginFanOutPublicationGroup(ctx, claim)
	if err != nil {
		return nil, err
	}
	o.fault.PublicationGroup = group
	return o.fault, nil
}

type publicationGroupAgentCrashExecutor struct {
	*publicationGroupCrashExecutor
	fault publicationGroupAgentCrashGroup
}

func (e *publicationGroupAgentCrashExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	if e.fault.cut != "" {
		owner = publicationGroupAgentCrashOwner{FanOutObligationOwner: owner, fault: e.fault}
	}
	return e.PipelineCoordinator.ServeFanOutCandidate(ctx, owner, key)
}
