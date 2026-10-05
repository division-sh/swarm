package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"gopkg.in/yaml.v3"
)

func TestIssue2566PreanalysisSourceOrder(t *testing.T) {
	for name, source := range map[string]string{
		"direct": "stages: {registered: {initial: true}, cooling: {}, done: {terminal: true}}\n",
		"alias":  "stages: {registered: {initial: true}, cooling: &metadata {}, done: *metadata}\n",
		"merge":  "stages: {<<: &catalog {registered: {initial: true}, cooling: {}}, done: {terminal: true}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			doc, err := admitSchemaFragment(source)
			if err != nil {
				t.Fatal(err)
			}
			if got := doc.LoweredStates(); !reflect.DeepEqual(got, []string{"registered", "cooling", "done"}) {
				t.Fatalf("declaration order = %v", got)
			}
		})
	}
}

func TestIssue2566PreanalysisSortedDumpCanRemainReachable(t *testing.T) {
	source := "stages: {registered: {initial: true}, cooling: {}, done: {terminal: true}}\n"
	before, err := admitSchemaFragment(source)
	if err != nil {
		t.Fatal(err)
	}
	var unordered map[string]any
	if err := yaml.Unmarshal([]byte(source), &unordered); err != nil {
		t.Fatal(err)
	}
	dumped, err := yaml.Marshal(unordered)
	if err != nil {
		t.Fatal(err)
	}
	after, err := admitSchemaFragment(string(dumped))
	if err != nil {
		t.Fatal(err)
	}
	if before.StageDeclarations.Entries[0].ID != "registered" || after.StageDeclarations.Entries[0].ID != "cooling" {
		t.Fatalf("unexpected orders: %v -> %v", before.LoweredStates(), after.LoweredStates())
	}
	node := identitytest.RootNode(t, "router")
	transitions := []HandlerTransitionSemantic{
		{Node: node, EventType: "work.registered", AdvancesTo: "registered"},
		{Node: node, EventType: "work.cooled", AdvancesTo: "cooling"},
		{Node: node, EventType: "work.done", AdvancesTo: "done"},
	}
	for _, schema := range []FlowSchemaDocument{before, after} {
		first := schema.StageDeclarations.Entries[0].ID
		graph := BuildWorkflowStageTopology(".", first, schema.LoweredStates(), []string{"done"}, transitions, nil, nil)
		if reachable := graph.LifecycleReachableStages(first); len(reachable) != 3 {
			t.Fatalf("entry %s stranded a stage: %v", first, reachable)
		}
	}
	t.Log("Go map dump changed first declaration registered -> cooling; both entries reach all three stages")
}

func TestIssue2566PreanalysisSinksDependOnMarkers(t *testing.T) {
	node := identitytest.RootNode(t, "scorer")
	transitions := []HandlerTransitionSemantic{{
		Node: node, EventType: "score.submitted",
		OnComplete: []HandlerRuleEntry{{Condition: "payload.value >= 50", AdvancesTo: "high"}, {Condition: "else", AdvancesTo: "low"}},
	}}
	stages := []string{"collecting", "high", "low"}
	old := BuildWorkflowStageTopology(".", "collecting", stages, []string{"high", "low"}, transitions, nil, nil)
	without := BuildWorkflowStageTopology(".", "collecting", stages, nil, transitions, nil, nil)
	for _, edge := range old.Edges {
		if edge.From != "collecting" {
			t.Fatalf("marked graph unexpectedly exits a terminal stage: %#v", edge)
		}
	}
	for _, stage := range []string{"high", "low"} {
		found := false
		for _, edge := range without.Edges {
			found = found || (edge.From == stage && edge.To != stage)
		}
		if !found {
			t.Fatalf("unmarked graph unexpectedly preserved sink %s", stage)
		}
	}
	t.Log("removing markers lends the same ordinary completion handler to high and low, adding high -> low and low -> high")
}

func TestIssue2566PreanalysisRetainedSourcePreservesOrder(t *testing.T) {
	bundle, err := loadSchemaFragment(t, "stages: {registered: {initial: true}, cooling: {}, done: {terminal: true}}\n")
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := sourceartifact.PersistedFromArtifact(bundle.SourceArtifact, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := persisted.Decode()
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	repo := repoRootForContractsTest(t)
	for _, artifact := range []*sourceartifact.AdmittedSourceArtifact{catalog, retained} {
		loaded, err := LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(loaded.RootSchema.LoweredStates(), []string{"registered", "cooling", "done"}) {
			t.Fatalf("retained order = %v", loaded.RootSchema.LoweredStates())
		}
		if loaded.SourceArtifact.BundleHash() != bundle.SourceArtifact.BundleHash() {
			t.Fatal("retained source changed bundle identity")
		}
	}
}

func TestIssue2566PreanalysisLoadedBranchFixture(t *testing.T) {
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "tests/tier9-composition-patterns/test-compose-accumulate-compute-branch"), DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	before, ok := bundle.WorkflowStageTopology(".")
	if !ok {
		t.Fatal("loaded fixture has no topology")
	}
	after := BuildWorkflowStageTopology(".", before.InitialStage, bundle.RootSchema.LoweredStates(), nil,
		bundle.Semantics.HandlerTransitions, bundle.Semantics.Timers, bundle.Semantics.Loops, bundle.Semantics.Gates)
	for _, sink := range []string{"high", "mid", "low"} {
		for _, edge := range before.Edges {
			if edge.From == sink && edge.To != sink {
				t.Fatalf("original graph exits %s", sink)
			}
		}
		found := false
		for _, edge := range after.Edges {
			found = found || (edge.From == sink && edge.To != sink)
		}
		if !found {
			t.Fatalf("unmarked graph unexpectedly preserved sink %s", sink)
		}
	}
	t.Log("real loaded score fixture: the three terminal results gain cross-result edges when marker-based source exclusion is removed")
}

func TestIssue2566PreanalysisDiskSchemaCensus(t *testing.T) {
	repo := repoRootForContractsTest(t)
	files, staged, initial, terminal, mismatches := 0, 0, 0, 0, 0
	for _, root := range []string{"examples", "tests", "internal"} {
		err := filepath.WalkDir(filepath.Join(repo, root), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || entry.Name() != "schema.yaml" {
				return nil
			}
			files++
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			doc, err := admitSchemaFragment(string(body))
			if err != nil || len(doc.StageDeclarations.Entries) == 0 {
				return nil
			}
			staged++
			initial += doc.StageDeclarations.InitialCount()
			terminal += doc.StageDeclarations.TerminalCount()
			if doc.StageDeclarations.InitialCount() == 1 && doc.StageDeclarations.Entries[0].ID != doc.LoweredInitialState() {
				mismatches++
				t.Logf("old initial not first: %s (%s != %s)", path, doc.LoweredInitialState(), doc.StageDeclarations.Entries[0].ID)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("disk schema files=%d; individually admitted staged schemas=%d; initial markers=%d; terminal markers=%d; old initial not first=%d; embedded/generated/negative corpus not covered", files, staged, initial, terminal, mismatches)
}
