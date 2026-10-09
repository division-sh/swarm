package manager

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestIssue2269StaticAndTemplateAgentsRetainExactTurnTimeout(t *testing.T) {
	root := t.TempDir()
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: turn-timeout-config\n")
	for _, flow := range []string{"static", "template"} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "schema.yaml"), "name: "+flow+"\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "events.yaml"), "work.requested:\nwork.aborted:\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "agents.yaml"), "worker:\n  intent: {inline: handle requests}\n  model: regular\n  subscriptions: [work.requested]\n  turn_timeout: {after: 10m, emit: work.aborted}\n")
	}
	repo := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	for _, flow := range []string{"static", "template"} {
		t.Run(flow, func(t *testing.T) {
			scope, ok := source.FlowScopeByID(flow)
			if !ok {
				t.Fatal("missing flow")
			}
			entry := scope.Agents["worker"]
			name := managerTestFlowAgentNamePlan(t, source, flow, "worker")
			var cfg models.AgentConfig
			var err error
			if flow == "static" {
				cfg, err = buildStaticFlowAgentConfig(managerIdentityTestRunID, source, name, flow, flow, "worker", entry, staticFlowLocalEventSetForTest(scope.Agents))
			} else {
				cfg, err = buildFlowAgentConfig(managerIdentityTestRunID, source, name, flow, "one", "entity-one", flow+"/one", "worker", entry, map[string]string{"instance_id": "one"}, staticFlowLocalEventSetForTest(scope.Agents))
			}
			if err != nil {
				t.Fatal(err)
			}
			want := timeridentity.TurnTimeout{After: 10 * time.Minute, Emit: "work.aborted"}
			if cfg.TurnTimeout == nil || *cfg.TurnTimeout != want || cfg.TurnTimeout == entry.TurnTimeout || len(cfg.EmitEvents) != 0 {
				t.Fatalf("lost bound, aliased declaration or granted emit permission: %+v", cfg)
			}
			cfg.TurnTimeout.After = time.Second
			if *entry.TurnTimeout != want {
				t.Fatal("runtime config mutated immutable declaration")
			}
		})
	}
}

func TestIssue2269TurnTimeoutParticipatesInPlanAndRecovery(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(t, ""))
	parent := runtimeflowidentity.Stored(source, semanticview.RootExecutionFlowID(source), managerIdentityTestRunID, managerIdentityTestRunID, managerIdentityTestRunID, "")
	instance, err := runtimeflowidentity.KeyedChild(source, parent, "review", "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := ConstructedFlowMaterialization(source, managerIdentityTestRunID, instance)
	if err != nil {
		t.Fatal(err)
	}
	if len(materialization.Agents) != 1 {
		t.Fatalf("constructed materialization has %d agents, want one", len(materialization.Agents))
	}
	blueprint := materialization.Agents[0]
	seen := map[string]bool{}
	for _, timeout := range []*timeridentity.TurnTimeout{nil,
		{After: time.Minute, Emit: "work.aborted"},
		{After: 2 * time.Minute, Emit: "work.aborted"},
		{After: time.Minute, Emit: "work.timed_out"},
	} {
		cfg := blueprint.Config
		cfg.TurnTimeout = timeridentity.CloneTurnTimeout(timeout)
		revision, err := AgentConfigPlanRevision(cfg, blueprint.Identity)
		if err != nil || seen[revision] {
			t.Fatalf("distinct bound aliased or failed: %+v revision=%s err=%v", timeout, revision, err)
		}
		seen[revision] = true
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var restored models.AgentConfig
		if err := json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		got, err := AgentConfigPlanRevision(restored, blueprint.Identity)
		if err != nil || got != revision {
			t.Fatalf("recovery changed exact bound identity: got=%s want=%s err=%v", got, revision, err)
		}
	}
	for _, timeout := range []timeridentity.TurnTimeout{{After: 0, Emit: "work.aborted"}, {After: time.Minute, Emit: ""}, {After: -time.Minute, Emit: "work.aborted"}} {
		cfg := blueprint.Config
		cfg.TurnTimeout = &timeout
		if _, err := AgentConfigPlanRevision(cfg, blueprint.Identity); err == nil {
			t.Fatalf("invalid retained bound admitted plan identity: %+v", timeout)
		}
		if err := ValidateAgentBuildConfiguration(cfg); err == nil {
			t.Fatalf("invalid retained bound admitted runtime construction: %+v", timeout)
		}
	}
}
