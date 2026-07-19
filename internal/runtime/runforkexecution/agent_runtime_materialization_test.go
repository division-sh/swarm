package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

func TestSelectedContractAgentRuntimeWaitsForCurrentRouteSettlementAfterPredecessorRetirement(t *testing.T) {
	eventBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "fork-agent", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "fork-agent", Generation: 2}
	eventBus.ReplaceAgentRoute(oldToken, selectedContractAgentRouteAdmission(t, oldToken.AgentID, "item.received"))
	oldEvent := eventtest.RuntimeControl(eventtest.UUID("old-work"), events.EventType("item.received"), "test", "", []byte(`{}`), 0, eventtest.UUID("run-1"), "", events.EventEnvelope{}, time.Now())
	if err := eventBus.Publish(context.Background(), oldEvent); err != nil {
		t.Fatalf("publish predecessor event: %v", err)
	}
	newRoute := eventBus.ReplaceAgentRoute(newToken, selectedContractAgentRouteAdmission(t, newToken.AgentID, "item.received"))
	newEvent := eventtest.RuntimeControl(eventtest.UUID("new-work"), events.EventType("item.received"), "test", "", []byte(`{}`), 0, eventtest.UUID("run-1"), "", events.EventEnvelope{}, time.Now())
	if err := eventBus.Publish(context.Background(), newEvent); err != nil {
		t.Fatalf("publish successor event: %v", err)
	}
	select {
	case <-newRoute:
	case <-time.After(time.Second):
		t.Fatal("successor event was not dequeued")
	}

	runtime := &selectedContractAgentRuntime{manager: runtimemanager.NewAgentManager(nil, nil)}
	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := runtime.WaitForQuiescence(waitCtx, eventBus); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence with current route work = %v, want deadline exceeded", err)
	}
	eventBus.CompleteAgentRouteDelivery(oldToken)
	if got := eventBus.PendingAgentRouteDeliveries(); got != 1 {
		t.Fatalf("late predecessor completion changed current pending count to %d", got)
	}
	eventBus.CompleteAgentRouteDelivery(newToken)
	waitCtx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.WaitForQuiescence(waitCtx, eventBus); err != nil {
		t.Fatalf("WaitForQuiescence after current route settlement: %v", err)
	}
}

func selectedContractAgentRouteAdmission(t *testing.T, agentID string, subscriptions ...string) semanticview.FlowOwnedAgentSubscriptionAdmission {
	t.Helper()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       agentID,
		Subscriptions: subscriptions,
	})
	if err != nil {
		t.Fatalf("admit selected-contract agent route: %v", err)
	}
	return admission
}

type selectedContractSelfReleaseScopeProbe struct {
	want runtimeauthoractivity.Scope
	seen chan runtimeauthoractivity.Scope
}

func (p *selectedContractSelfReleaseScopeProbe) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	if req.OperationKind == "self_release" {
		if err := ctx.Err(); err != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("self-release context is canceled: %w", err)
		}
		scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
		if !ok {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("self-release author activity scope is required")
		}
		if scope != p.want {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("self-release author activity scope = %#v, want %#v", scope, p.want)
		}
		p.seen <- scope
	}
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID:        req.OperationID,
		TransitionID:       req.OperationID + "-transition",
		AgentID:            req.AgentID,
		PreviousEpoch:      req.ExpectedEpoch,
		RuntimeEpoch:       req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration,
		Generation:         req.TargetGeneration,
		PreviousPhase:      req.ExpectedPhase,
		Phase:              req.TargetPhase,
		ConfigRevision:     req.ConfigRevision,
		RunMode:            req.RunMode,
	}, nil
}

type selectedContractSelfReleaseAgent struct {
	id string
}

func (a selectedContractSelfReleaseAgent) ID() string { return a.id }
func (selectedContractSelfReleaseAgent) Type() string { return "worker" }
func (selectedContractSelfReleaseAgent) Subscriptions() []events.EventType {
	return []events.EventType{"item.received"}
}
func (selectedContractSelfReleaseAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestSelectedContractAgentRuntimeBuildsCanonicalMockAdapter(t *testing.T) {
	eventBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	builder, err := buildSelectedContractAgentRuntimeFactory(publishSelectedContractForkEventsRequest{
		Store:        &store.PostgresStore{},
		LoadedSource: LoadedSelectedContractSource{},
		AgentRuntime: selectedContractAgentRuntimePlan{
			Proof: SelectedContractAgentRuntimeMaterialization{AgentRecipients: []string{"mock-agent"}},
			Options: SelectedContractAgentRuntimeOptions{
				Config: &config.Config{LLM: config.LLMConfig{Backend: "mock"}},
			},
		},
	}, eventBus)
	if err != nil {
		t.Fatalf("build selected-contract mock runtime: %v", err)
	}
	if builder.factory == nil {
		t.Fatal("selected-contract mock runtime returned no agent factory")
	}
	if builder.cleanup != nil {
		builder.cleanup()
	}
}

func TestStartSelectedContractAgentRuntimeDetachesCancellationAndPreservesForkScopeForSelfRelease(t *testing.T) {
	eventBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork, ID: "00000000-0000-0000-0000-000000000311",
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: "00000000-0000-0000-0000-000000000311", ForkRunID: "00000000-0000-0000-0000-000000000312", Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
		},
		ExecutionOwner: "self-release-scope-test", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
	}
	wantScope := runtimeauthoractivity.BundleScope(
		"00000000-0000-0000-0000-000000000313",
		"bundle-v1:sha256:3131313131313131313131313131313131313131313131313131313131313131",
	)
	initiatingCtx, cancel := context.WithCancel(context.Background())
	ctx := selectedForkExecutionTestContext(t, initiatingCtx, authority)
	ctx = runtimeauthoractivity.WithScope(ctx, wantScope)
	probe := &selectedContractSelfReleaseScopeProbe{want: wantScope, seen: make(chan runtimeauthoractivity.Scope, 1)}

	runtime, _, err := startSelectedContractAgentRuntime(ctx, publishSelectedContractForkEventsRequest{
		Store: &store.PostgresStore{},
		AgentRuntime: selectedContractAgentRuntimePlan{
			Records: []runtimemanager.PersistedAgent{{Config: runtimeactors.AgentConfig{
				ID: "fork-agent", Role: "worker", ExecutionMode: "live", Subscriptions: []string{"item.received"},
			}}},
			Options: SelectedContractAgentRuntimeOptions{
				AgentFactory: func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
					return selectedContractSelfReleaseAgent{id: cfg.ID}, nil
				},
				AgentManagerOptions: runtimemanager.AgentManagerOptions{LifecycleStore: probe},
			},
		},
	}, eventBus)
	if err != nil {
		t.Fatalf("startSelectedContractAgentRuntime: %v", err)
	}
	cancel()
	if err := runtime.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case got := <-probe.seen:
		if got != wantScope {
			t.Fatalf("self-release scope = %#v, want %#v", got, wantScope)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for selected-contract self-release transition")
	}
}

func TestSelectedContractStaticAgentRecordsIncludeInferredFlowRequiredAgents(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "analysis",
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analysis",
			Mode: runtimecontracts.FlowModeStatic,
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Type:          "generic",
				Role:          "analyzer",
				Subscriptions: []string{"analysis.requested"},
				EmitEvents:    []string{"analysis.done"},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analysis": flow.Schema,
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{flow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analysis": &flow,
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}

	records, err := selectedContractStaticAgentRecords(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("selectedContractStaticAgentRecords: %v", err)
	}
	count := 0
	for _, record := range records {
		if strings.TrimSpace(record.Config.ID) == "analyzer" {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("records = %#v, want analyzer from static-agent and inferred flow-required-agent materialization paths", records)
	}
}
