package runforkexecution

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

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
