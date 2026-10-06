package pipeline

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestGuardTerminationVerifiedExecutionAndRestartBothStores(t *testing.T) {
	for _, tc := range []struct {
		name, guard string
		prefix      []string
	}{
		{"single", "      guard: {id: score_check, check: false, on_fail: kill}\n", []string{"score_check"}},
		{"first failure", "      guard:\n        checks:\n          - {id: first, check: false}\n          - {id: second, check: false}\n        on_fail: kill\n", []string{"first"}},
		{"second failure", "      guard:\n        checks:\n          - {id: first, check: true}\n          - {id: second, check: false}\n        on_fail: kill\n", []string{"first", "second"}},
		{"unnamed single", "      guard: {check:   false , on_fail: kill}\n", []string{"false"}},
		{"unnamed chain", "      guard: {checks: [{check:   true }, {check:   false }], on_fail: kill}\n", []string{"true", "false"}},
		{"unnamed first failure", "      guard: {checks: [{check:   false }, {check: true}], on_fail: kill}\n", []string{"false"}},
		{"mixed chain", "      guard: {checks: [{}, {id: ' first ', check: true}, {}, {check:   false }, {}], on_fail: kill}\n", []string{"first", "false"}},
		{"padded named", "      guard: {id: ' score_check ', check: false, on_fail: kill}\n", []string{"score_check"}},
		{"on fail only no op", "      guard: {on_fail: kill}\n", nil},
		{"empty row no op", "      guard: {checks: [{}], on_fail: kill}\n", nil},
		{"policy annotation no op", "      guard: {policy_ref: threshold, on_fail: kill}\n", nil},
	} {
		target, advance := "killed", ""
		if tc.prefix == nil {
			target, advance = "done", "      advances_to: done\n"
		}
		files := map[string]string{
			"schema.yaml":   "stages:\n  ready: {}\n  " + target + ": {final: true}\npins:\n  inputs: [kill]\n",
			"events.yaml":   "kill:\n",
			"entities.yaml": "test_entity:\n  marker: text\n",
			"nodes.yaml":    "router:\n  execution_type: system_node\n  subscribes_to: [kill]\n  event_handlers:\n    kill:\n" + tc.guard + advance + "      data_accumulation:\n        writes:\n          - {target_field: marker, value: \"skipped\"}\n",
		}
		root := t.TempDir()
		for label, body := range files {
			if err := os.WriteFile(filepath.Join(root, label), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
		// The supported verifier must admit the same bytes before execution. This
		// subprocess also avoids a pipeline -> bootverify -> pipeline test cycle.
		cmd := exec.Command("go", "run", "./cmd/swarm", "verify", root, "--json")
		cmd.Dir = WorkflowRepoRoot()
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "SWARM_TEST_") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("supported verify refused source before execution: %v\n%s", err, output)
		}
		bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(WorkflowRepoRoot(), root, contracts.DefaultPlatformSpecFile(WorkflowRepoRoot()))
		if err != nil {
			t.Fatal(err)
		}
		fact, err := sourceartifact.PersistedFromArtifact(bundle.SourceArtifact, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		retained, err := fact.Decode()
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := contracts.LoadWorkflowContractBundleFromArtifact(WorkflowRepoRoot(), retained, bundle.Paths.PlatformSpecFile, contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		originalGraph, _ := bundle.WorkflowStageTopology(".")
		retainedGraph, _ := rebuilt.WorkflowStageTopology(".")
		if !reflect.DeepEqual(originalGraph.PossibleGuardTerminations(), retainedGraph.PossibleGuardTerminations()) || bundle.SourceArtifact.BundleHash() != retained.BundleHash() {
			t.Fatal("retained source changed guard model or admitted hash")
		}
		logical, err := sourceartifact.DecodeLogical(retained.LogicalBlob())
		if err != nil {
			t.Fatal(err)
		}
		logicalBundle, err := contracts.LoadWorkflowContractBundleFromArtifact(WorkflowRepoRoot(), logical, bundle.Paths.PlatformSpecFile, contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		logicalGraph, _ := logicalBundle.WorkflowStageTopology(".")
		if !reflect.DeepEqual(originalGraph.PossibleGuardTerminations(), logicalGraph.PossibleGuardTerminations()) || logical.BundleHash() != bundle.SourceArtifact.BundleHash() {
			t.Fatal("logical retained source changed effective guard identity")
		}
		if tc.prefix == nil && len(retainedGraph.PossibleGuardTerminations()) != 0 {
			t.Fatal("no-op guard acquired termination authority")
		}
		for _, label := range tc.prefix {
			found := false
			for _, possibility := range retainedGraph.PossibleGuardTerminations() {
				found = found || possibility.GuardID == label
			}
			if !found {
				t.Fatalf("compiled/readback identity lost runtime label %q", label)
			}
		}
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, rebuilt, ".", "ready", true)
				evt := f.event("kill")
				result, err := f.execute("kill", evt)
				status, marker := HandlerOutcomeKilled, "unchanged"
				if tc.prefix == nil {
					status, marker = HandlerOutcomeCompleted, "skipped"
				}
				if err != nil || result.Outcome == nil || result.Outcome.Status != status {
					t.Fatalf("guard execution: %#v %v", result, err)
				}
				after, found := f.load()
				if !found || after.CurrentState != target || after.Fields["marker"] != marker || len(after.TransitionHistory) != 1 {
					t.Fatalf("guard failure executed successful effects or lost transition: %#v", after)
				}
				record := after.TransitionHistory[0]
				if !reflect.DeepEqual(record.GuardsEvaluated, tc.prefix) || !reflect.DeepEqual(record.Evidence.GuardsEvaluated(), tc.prefix) || record.TriggerEventID != evt.ID() {
					t.Fatalf("exact guard prefix/occurrence missing: %#v", record)
				}
				if _, ordinary := record.Evidence.Compiled(); ordinary != (tc.prefix == nil) {
					t.Fatal("guard execution changed ordinary versus failed-check authority")
				}
				restarted := newPostgresWorkflowInstanceStoreForTest(f.db)
				if backend == "sqlite" {
					restarted = newSQLiteWorkflowInstanceStoreForTest(t, f.db)
				}
				route := testWorkflowInstanceRoute(f.path)
				reloaded, found, err := restarted.Load(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, route.InstancePath))
				if err != nil || !found || !reflect.DeepEqual(after, reloaded) {
					t.Fatalf("restart changed authoritative guard state/cause: found=%v err=%v %#v", found, err, reloaded)
				}
			})
		}
	}
}
