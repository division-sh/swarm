package runtime

import (
	"os"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"testing"
)

func TestRunScopedLiveIdentityStructuralRatchet(t *testing.T) {
	root := runScopedIdentityRepoRoot(t)

	agentIdentity := runScopedIdentityRead(t, root, "internal/runtime/core/agentidentity/identity.go")
	for _, required := range []string{
		"RunID string `json:\"run_id\"`",
		"func New(runID string, name Name, route Route)",
		"left.RunID",
		"json.Marshal(i)",
		"RunID:            i.RunID",
	} {
		if !strings.Contains(agentIdentity, required) {
			t.Errorf("concrete agent identity stopped owning required run axis %q", required)
		}
	}

	flowIdentity := runScopedIdentityRead(t, root, "internal/runtime/core/flowidentity/flowidentity.go")
	for _, required := range []string{
		"type RunScopedFlowInstance struct",
		"RunID string",
		"Route Route",
		"return i.RunID + \"\\x00\" + i.Route.InstancePath",
	} {
		if !strings.Contains(flowIdentity, required) {
			t.Errorf("live flow identity stopped owning required run axis %q", required)
		}
	}

	for _, relative := range []string{
		"internal/runtime/core/pinrouting",
		"internal/runtime/contracts",
	} {
		runScopedIdentityWalkProductionGo(t, filepath.Join(root, relative), func(path, source string) {
			for _, forbidden := range []string{"agentidentity.Identity", "runtimeagentidentity.Identity"} {
				if strings.Contains(source, forbidden) {
					t.Errorf("pre-run blueprint %s depends on concrete live identity %q", path, forbidden)
				}
			}
		})
	}

	lifecyclePlan := runScopedIdentityRead(t, root, "internal/runtime/pipeline/workflow_lifecycle_plan.go")
	for _, forbidden := range []string{"RunIDFromContext", "workflowTimerRunID"} {
		if strings.Contains(lifecyclePlan, forbidden) {
			t.Errorf("workflow lifecycle planning reintroduced ambient run authority %q", forbidden)
		}
	}
	activityEngine := runScopedIdentityRead(t, root, "internal/runtime/pipeline/activity_engine.go")
	if strings.Contains(activityEngine, "NewRunScopedFlowInstance(runtimecorrelation.RunIDFromContext") {
		t.Error("activity generation admission reintroduced ambient flow run authority")
	}
	timerOwner := runScopedIdentityRead(t, root, "internal/runtime/pipeline/workflow_timer_owner.go")
	if count := strings.Count(timerOwner, "runID := identity.RunID"); count < 2 {
		t.Errorf("initial timer lifecycle has %d explicit live-owner run projections, want at least 2", count)
	}

	for _, relative := range []string{
		"internal/runtime/pipeline",
		"internal/store/internal/backend/pipelinepersistence",
		"internal/store/internal/workflowroute",
	} {
		runScopedIdentityWalkProductionGo(t, filepath.Join(root, relative), func(path, source string) {
			if strings.Contains(source, "RequireRunID(") {
				t.Errorf("live flow lifecycle owner %s derives run authority from context", path)
			}
		})
	}

	routing := runScopedIdentityRead(t, root, "internal/runtime/bus/routing_derivation.go")
	for _, required := range []string{
		"instanceOwners    map[runtimeflowidentity.RunScopedFlowInstance]runtimeflowidentity.RunScopedFlowInstance",
		"instanceEventPath map[runtimeflowidentity.RunScopedFlowInstance][]string",
	} {
		if !strings.Contains(routing, required) {
			t.Errorf("RouteTable stopped keying process topology by exact live flow owner %q", required)
		}
	}

	forbiddenEverywhere := []string{
		"bindRuntimeCreatedIdentity(",
		"DeactivateRoutingRulesByEntity",
		"workflowInitialMaterializationRouteRebindAllowed",
		"allowStandingGenerationRebind",
		"rebindExistingRoute",
		"WHERE instance_path =",
		"WHERE instance_id =",
		"JOIN flow_instances fi ON fi.instance_path",
		"JOIN flow_instances AS instance ON instance.instance_path",
		"REFERENCES flow_instances(instance_path",
		"REFERENCES flow_instances (instance_path",
		"REFERENCES agents(agent_id",
		"REFERENCES agents (agent_id",
		"map[string]runtimeflowidentity.RunScopedFlowInstance",
		"map[string]agentidentity.Identity",
		"map[string]runtimeagentidentity.Identity",
	}
	runScopedIdentityWalkProductionGo(t, filepath.Join(root, "internal"), func(path, source string) {
		for _, forbidden := range forbiddenEverywhere {
			if strings.Contains(source, forbidden) {
				t.Errorf("production owner %s reintroduced partial live identity %q", path, forbidden)
			}
		}
	})

	spec := runScopedIdentityRead(t, root, "platform-spec.yaml")
	completeAgentReference := "REFERENCES agents (run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence, flow_scope_key, flow_instance_id, flow_instance)"
	completeAgentReferences := 0
	for _, line := range strings.Split(spec, "\n") {
		if !strings.Contains(line, "REFERENCES agents") {
			continue
		}
		if !strings.Contains(line, completeAgentReference) {
			t.Errorf("platform schema reintroduced partial concrete-agent reference: %s", strings.TrimSpace(line))
			continue
		}
		completeAgentReferences++
	}
	if completeAgentReferences < 2 {
		t.Errorf("platform schema has %d complete live concrete-agent references, want directive and session owners", completeAgentReferences)
	}
	for _, required := range []string{
		"PRIMARY KEY (run_id, instance_path)",
		"FOREIGN KEY (run_id, parent_instance) REFERENCES flow_instances (run_id, instance_path)",
		"CHECK ((is_materialized AND run_id IS NOT NULL AND flow_instance IS NOT NULL) OR (NOT is_materialized AND run_id IS NULL))",
		"`run + flow + instance_key` in both text and JSON output",
	} {
		if !strings.Contains(spec, required) {
			t.Errorf("platform schema stopped enforcing run-scoped live identity %q", required)
		}
	}

	describeOwner := runScopedIdentityRead(t, root, "internal/runtime/authoringview/view.go")
	if !strings.Contains(describeOwner, "run + flow + instance_key") {
		t.Error("authoring view stopped owning the concrete template identity formula")
	}
	describeText := runScopedIdentityRead(t, root, "internal/cliapp/describe.go")
	if !strings.Contains(describeText, "flow.TemplateInstance.Identity") {
		t.Error("supported describe text stopped rendering the canonical template identity projection")
	}

	testSQL := runScopedIdentityRead(t, root, "internal/store/testsql/event.go")
	for _, required := range []string{
		"WHERE run_id = NEW.run_id AND flow_template",
		"fi.run_id = NEW.run_id",
		"fi.run_id = rr.run_id",
		"fi.run_id = (SELECT run_id FROM events",
	} {
		if !strings.Contains(testSQL, required) {
			t.Errorf("test SQL failure injector stopped classifying exact run owner %q", required)
		}
	}
}

func runScopedIdentityRepoRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("locate run-scoped identity guard")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
}

func runScopedIdentityRead(t *testing.T, root, relative string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatalf("read %s: %v", relative, err)
	}
	return string(raw)
}

func runScopedIdentityWalkProductionGo(t *testing.T, root string, check func(path, source string)) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		check(path, string(raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
