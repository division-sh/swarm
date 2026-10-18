package manager

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type subscriptionAdmissionTestAgent struct{ id string }

func (a subscriptionAdmissionTestAgent) ID() string                      { return a.id }
func (subscriptionAdmissionTestAgent) Type() string                      { return "test" }
func (subscriptionAdmissionTestAgent) Subscriptions() []events.EventType { return nil }
func (subscriptionAdmissionTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func testManagerSubscriptionAdmission(t *testing.T, cfg runtimeactors.AgentConfig) semanticview.FlowOwnedAgentSubscriptionAdmission {
	t.Helper()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: cfg.ID, FlowID: cfg.FlowID, FlowPath: cfg.CanonicalFlowPath(), Subscriptions: cfg.Subscriptions,
	})
	if err != nil {
		t.Fatalf("admit test manager subscriptions: %v", err)
	}
	return admission
}

func capturedSubscriptionManagerOptions(t *testing.T) AgentManagerOptions {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.WriteManagerSubscriptionAdmission(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return AgentManagerOptions{SemanticSource: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact)}
}

func TestSpawnAgentRejectsForeignExactAndPatternBeforeRegistration(t *testing.T) {
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			eb, err := runtimebus.NewEphemeralEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
				WorkOwner:         newTestManagerWorkOwner(t),
				ReceiverExecution: eventreceiver.NormalExecution(),
			})
			if err != nil {
				t.Fatal(err)
			}
			am := newTestAgentManagerWithOptions(t, eb, func(cfg runtimeactors.AgentConfig) (Agent, error) {
				return subscriptionAdmissionTestAgent{id: cfg.ID}, nil
			}, capturedSubscriptionManagerOptions(t))
			err = spawnManagerTestAgent(am, managerTestAgentConfig(runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer",
				Identity:      runtimeagentidentitytest.Runtime(t, "reviewer", "subscription-admission-test", "review", "inst-1", "review/inst-1"),
				FlowPath:      "review/inst-1",
				Subscriptions: []string{subscription},
			}))
			if err == nil || !strings.Contains(err.Error(), "nearest common ancestor schema.yaml") {
				t.Fatalf("SpawnAgent error = %v, want admission rejection", err)
			}
			if am.Count() != 0 {
				t.Fatalf("registered agent count = %d, want zero", am.Count())
			}
		})
	}
}

func TestReconfigureAgentRejectsForeignSubscriptionWithoutReplacingCurrentAdmission(t *testing.T) {
	eb, err := runtimebus.NewEphemeralEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		WorkOwner:         newTestManagerWorkOwner(t),
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatal(err)
	}
	am := newTestAgentManagerWithOptions(t, eb, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return subscriptionAdmissionTestAgent{id: cfg.ID}, nil
	}, capturedSubscriptionManagerOptions(t))
	initial := managerTestAgentConfig(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "reviewer",
		Identity:      runtimeagentidentitytest.Runtime(t, "reviewer", "subscription-admission-test", "review", "inst-1", "review/inst-1"),
		FlowID:        "review",
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready"},
	})
	if err := spawnManagerTestAgent(am, initial); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	before, ok := testExecutionSnapshot(t, am, initial.ID, initial.FlowPath)
	if !ok {
		t.Fatal("initial execution missing")
	}

	err = reconfigureAgentThroughLifecycleForTest(t, am, initial.ID, initial.FlowPath, runtimeactors.AgentConfig{
		ExecutionMode: "live",
		Subscriptions: []string{"foreign/**/task.ready"},
	})
	if err == nil || !strings.Contains(err.Error(), "nearest common ancestor schema.yaml") {
		t.Fatalf("ReconfigureAgent error = %v, want admission rejection", err)
	}
	after, ok := testExecutionSnapshot(t, am, initial.ID, initial.FlowPath)
	if !ok {
		t.Fatal("execution disappeared after rejected reconfigure")
	}
	if after.Token != before.Token || !reflect.DeepEqual(after.Config, before.Config) || !reflect.DeepEqual(after.Admission.RoutePatterns(), before.Admission.RoutePatterns()) {
		t.Fatalf("rejected reconfigure changed execution: before=%#v after=%#v", before, after)
	}
}

func s1AgentSelectorSource(t *testing.T) semanticview.Source {
	t.Helper()
	rooted, _ := sourceartifactfixture.Rooted(t, "selected", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(filepath.Clean("../../.."), rooted, "", runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestS1AgentSubscriptionWrapperPreservesRawFlowSelector(t *testing.T) {
	source := s1AgentSelectorSource(t)
	// The minimal source has no authored event catalog; use its supported intrinsic.
	for _, flow := range []string{".", " . ", "\t.\n", "", " ", "foreign"} {
		for _, subscriptions := range [][]string{nil, {"platform.join_complete"}} {
			cfg := runtimeactors.AgentConfig{ID: "listener", FlowID: flow, Subscriptions: subscriptions}
			admission, err := admitAgentConfigSubscriptions(source, &cfg, nil)
			want := flow == "."
			if (err == nil) != want || admission.ValidForAgent("listener") != want {
				t.Errorf("raw flow %q subscriptions %v accepted=%t want=%t: %v", flow, subscriptions, admission.ValidForAgent("listener"), want, err)
			}
		}
	}
}

func TestS1ModelPreparationCannotEraseRawScopeBeforeAdmission(t *testing.T) {
	source := s1AgentSelectorSource(t)
	for _, flow := range []string{".", " . ", "\t.\n", "", " ", "./", "foreign"} {
		for _, subscriptions := range [][]string{nil, {"platform.join_complete"}} {
			cfg := runtimeactors.AgentConfig{ID: "listener", FlowID: flow, Model: "regular", Subscriptions: subscriptions}
			if err := resolveAgentModel(&cfg, executionposture.Live, llmselection.BackendAnthropic, llmselection.ModelAliases{}, true); err != nil {
				t.Fatal(err)
			}
			if cfg.FlowID != flow {
				t.Errorf("model preparation rewrote %q into %q", flow, cfg.FlowID)
			}
			admission, err := admitAgentConfigSubscriptions(source, &cfg, nil)
			want := flow == "."
			if (err == nil) != want || admission.ValidForAgent("listener") != want {
				t.Errorf("model -> admission flow %q subscriptions %v accepted=%t want=%t: %v", flow, subscriptions, admission.ValidForAgent("listener"), want, err)
			}
		}
	}
}

func TestS1RefusedSubscriptionCannotRepairNextAttempt(t *testing.T) {
	source := s1AgentSelectorSource(t)
	for _, flow := range []string{" . ", "\t.\n", "", " ", "./", "foreign"} {
		for _, subscriptions := range [][]string{nil, {"platform.join_complete"}} {
			cfg := runtimeactors.AgentConfig{ID: "listener", FlowID: flow, Subscriptions: subscriptions}
			for attempt := 0; attempt < 3; attempt++ {
				admission, err := admitAgentConfigSubscriptions(source, &cfg, nil)
				if err == nil || admission.ValidForAgent("listener") || len(admission.RoutePatterns()) != 0 {
					t.Errorf("attempt %d acquired admission for original %q, now %q: %v", attempt, flow, cfg.FlowID, err)
				}
				if cfg.FlowID != flow {
					t.Errorf("attempt %d changed refused flow %q into %q", attempt, flow, cfg.FlowID)
				}
			}
			cfg.FlowID = "."
			if admission, err := admitAgentConfigSubscriptions(source, &cfg, nil); err != nil || !admission.ValidForAgent("listener") {
				t.Fatalf("explicit caller correction lost canonical admission: %v", err)
			}
		}
	}
}
