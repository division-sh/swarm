package manager

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestConstructedRootAgentPreparationUsesExactReadinessOwner(t *testing.T) {
	for _, variant := range []string{"exact", "foreign_run", "foreign_route", "wrong_agent_route", "undeclared", "changed_attempt", "running_cell", "running_mode", "foreign_source"} {
		t.Run(variant, func(t *testing.T) {
			am, instances, bus, req := prepareConstructedRootAgentForTest(t)
			identity := am.lifecycle.executionIdentities()[0]
			key := flowActivationReadinessKey(req.TriggerEvent.RunID(), req.Instance.InstancePath)
			wantError := ""
			am.lifecycle.mu.Lock()
			cell := am.lifecycle.cells[identity]
			switch variant {
			case "foreign_run":
				copy := *cell.topology.Authority.Readiness
				copy.RunID = "88888888-8888-4888-8888-888888888888"
				cell.topology.Authority.Readiness = &copy
				wantError = "not an exact stopped preparation"
			case "running_cell":
				cell.phase = AgentLifecycleRunning
				wantError = "not an exact stopped preparation"
			case "running_mode":
				cell.runMode = AgentRunModeStandard
				wantError = "not an exact stopped preparation"
			case "wrong_agent_route":
				route, err := runtimeagentidentity.PresentRoute("foreign", "other", "foreign/other")
				if err != nil {
					t.Fatal(err)
				}
				delete(am.lifecycle.cells, identity)
				identity.Route = route
				cell.identity = identity
				am.lifecycle.cells[identity] = cell
				wantError = "does not match its preparation owner"
			}
			am.lifecycle.mu.Unlock()
			instances.readinessMu.Lock()
			row := instances.readiness[key]
			switch variant {
			case "foreign_route":
				row.Plan.Identity.ScopeKey = "foreign"
				wantError = "disagrees with flow owner"
			case "undeclared":
				row.Plan.Agents = nil
				wantError = "absent from its current preparation plan"
			case "changed_attempt":
				row.AttemptOrdinal++
				wantError = "preparation is no longer current"
			case "foreign_source":
				row.Plan.BundleHash = "bundle-v2:sha256:" + strings.Repeat("9", 64)
				wantError = "callback source is stale"
			}
			instances.readiness[key] = row
			before, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			instances.readinessMu.Unlock()
			err = am.PrepareAdmittedDynamicFlowAgentsForStart(testAuthorActivityContext(context.Background()))
			if wantError == "" && err != nil {
				t.Fatalf("admit exact root preparation: %v", err)
			}
			if wantError != "" && (err == nil || !strings.Contains(err.Error(), wantError)) {
				t.Fatalf("admission error=%v, want %q", err, wantError)
			}
			if wantError != "" {
				instances.readinessMu.Lock()
				after, encodeErr := json.Marshal(instances.readiness[key])
				instances.readinessMu.Unlock()
				if encodeErr != nil || string(before) != string(after) {
					t.Fatalf("refused preparation mutated durable readiness: %s -> %s, err=%v", before, after, encodeErr)
				}
			}
			if bus.HasFlowInstanceRoute(testActivationFlowIdentity(req)) || len(instances.armedEntries) != 0 {
				t.Fatal("stopped root preparation published executable route or timer work")
			}
		})
	}
}

func TestStandingPreparationDefersRunningPredecessor(t *testing.T) {
	first, instances, _, req := prepareConstructedRootAgentForTest(t)
	ctx := testAuthorActivityContext(context.Background())
	if err := first.PrepareAdmittedDynamicFlowAgentsForStart(ctx); err != nil {
		t.Fatal(err)
	}
	identity := first.lifecycle.executionIdentities()[0]
	state, found := first.lifecycle.stateByIdentity(identity)
	if !found || state.Topology.Authority.Readiness.Preparation {
		t.Fatalf("predecessor has no admitted topology: %+v", state)
	}
	cfg, found := first.getAgentConfigIdentity(identity)
	if !found {
		t.Fatal("predecessor configuration is missing")
	}
	predecessor := PersistedAgent{
		Config: cfg, Topology: state.Topology, ProcessBinding: state.ProcessBinding,
		LifecycleEpoch: state.RuntimeEpoch, LifecycleGeneration: state.Generation,
		LifecyclePhase: AgentLifecycleRunning, LifecycleRunMode: AgentRunModeStandard,
	}
	agents := &flowActivationTestStore{upserts: []PersistedAgent{predecessor}}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	next := newFlowActivationManager(t, bus, instances, agents)
	setFlowActivationManagerSemanticSource(next, req.ContractBundle)
	before, err := json.Marshal(agents.upserts)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(identity.RunID, req.Instance.Route())
	if err != nil {
		t.Fatal(err)
	}
	if err := next.PreparePersistedDynamicFlowRuntimeProcessTopology(ctx, owner); err != nil {
		t.Fatalf("prepare retained running predecessor: %v", err)
	}
	after, err := json.Marshal(agents.upserts)
	if err != nil || string(before) != string(after) || len(agents.terminated) != 0 {
		t.Fatalf("pre-admission preparation mutated predecessor: %s -> %s, err=%v", before, after, err)
	}
	if len(next.lifecycle.executionIdentities()) != 0 || bus.HasFlowInstanceRoute(owner) || len(instances.armedEntries) != 0 {
		t.Fatal("pre-admission preparation projected a running predecessor")
	}
}

func prepareConstructedRootAgentForTest(t *testing.T) (*AgentManager, *flowActivationTestInstanceStore, *flowActivationTestBus, runtimepipeline.FlowInstanceActivationRequest) {
	t.Helper()
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	root := t.TempDir()
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root-agent-preparation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), "task.assigned:\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "test-agent:\n  intent: {inline: Handle root work.}\n  model: regular\n  subscriptions: [task.assigned]\n")
	repo := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	setFlowActivationManagerSemanticSource(am, source)
	owner := runtimeflowidentity.Stored(source, ".", flowActivationTestRunID, flowActivationTestRunID, flowActivationTestRunID, "")
	req := runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source,
		Instance:       owner,
		Config:         map[string]any{},
		TriggerEvent:   testFlowActivationTriggerEvent("77777777-7777-4777-8777-777777777777"),
	}
	ctx := testAuthorActivityContext(context.Background())
	created, finish, err := am.PrepareStandingFlowInstance(ctx, req)
	if err != nil || !created || finish == nil {
		t.Fatalf("prepare constructed root: created=%t finish=%t err=%v", created, finish != nil, err)
	}
	identities := am.lifecycle.executionIdentities()
	if len(identities) != 1 || identities[0].Route != runtimeagentidentity.RootRoute() {
		t.Fatalf("root declaration identity = %+v", identities)
	}
	state, found := am.lifecycle.stateByIdentity(identities[0])
	if !found || state.Topology.Authority.Readiness == nil || state.Topology.Authority.Readiness.InstancePath != owner.InstancePath {
		t.Fatalf("root preparation does not own its constructed readiness: %+v found=%t", state, found)
	}
	return am, instances, bus, req
}
