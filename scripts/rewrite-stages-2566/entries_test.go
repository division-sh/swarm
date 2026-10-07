package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func assertEntryGolden(expected entryGolden, schema contracts.FlowSchemaDocument) error {
	order := make([]string, 0, len(schema.StageDeclarations.Entries))
	for _, stage := range schema.StageDeclarations.Entries {
		order = append(order, stage.ID)
	}
	if schema.StageDeclarations.InitialStage() != expected.Entry || !reflect.DeepEqual(order, expected.Order) || !reflect.DeepEqual(schema.StageDeclarations.FinalStages(), expected.Finals) {
		return fmt.Errorf("%s / %s: reviewed entry/order/finals changed: entry=%s order=%v finals=%v; expected=%+v", expected.Source, expected.Flow, schema.StageDeclarations.InitialStage(), order, schema.StageDeclarations.FinalStages(), expected)
	}
	return nil
}

func TestRewrite2566EntryGoldenInventoryCoversEveryReviewedSite(t *testing.T) {
	read := func(name string, target any) {
		t.Helper()
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, target); err != nil {
			t.Fatal(err)
		}
	}
	var p plan
	var entries []entryGolden
	read("intent.json", &p)
	read("entries.json", &entries)
	wanted, actual := map[string]int{}, map[string]int{}
	for _, change := range p.Changes {
		wanted[change.File] += len(change.Equivalence)
	}
	selectors := map[string]bool{}
	for _, entry := range entries {
		key := entry.File + "/" + entry.Flow
		if selectors[key] {
			t.Fatalf("duplicate reviewed source selector %s", key)
		}
		selectors[key] = true
		actual[entry.File]++
	}
	for file, count := range wanted {
		if count == 0 {
			delete(wanted, file)
		}
	}
	if !reflect.DeepEqual(wanted, actual) {
		t.Fatalf("permanent entry oracle omitted a reviewed source family: wanted=%v actual=%v", wanted, actual)
	}
}

func TestRewrite2566GeneratedSourcesMatchReviewedEntryGoldens(t *testing.T) {
	type generatedFixture struct {
		name     string
		build    func(testing.TB) string
		expected []entryGolden
	}
	fixtures := []generatedFixture{
		{"stopped-readiness-source-replacement", func(t testing.TB) string {
			root := canonicalrouting.CopyRootIngressLegacyTemplateTargetRoute(t)
			canonicalrouting.RenameStoppedRunReadinessSource(t, root)
			return root
		}, []entryGolden{
			{Flow: ".", Entry: "new", Order: []string{"new", "waiting", "done"}, Finals: []string{"done"}},
			{Flow: "operating", Entry: "initializing", Order: []string{"initializing", "waiting", "ready"}, Finals: []string{"ready"}},
		}},
		{"receiver-mailbox-gate", canonicalrouting.CopyForkReceiverMailboxGate, []entryGolden{
			{Flow: ".", Entry: "waiting", Order: []string{"waiting", "active", "done"}, Finals: []string{"done"}},
			{Flow: "consumer", Entry: "waiting", Order: []string{"waiting", "active", "done"}, Finals: []string{"done"}},
			{Flow: "producer", Entry: "waiting", Order: []string{"waiting", "active"}, Finals: []string{"active"}},
		}},
		{"stage-completion", canonicalrouting.CopyStageCompletionJourney, []entryGolden{
			{Flow: ".", Entry: "active", Order: []string{"active", "done"}, Finals: []string{"done"}},
			{Flow: "discovery", Entry: "ready", Order: []string{"ready", "Ready"}, Finals: []string{"Ready"}},
		}},
		{"loop-join", canonicalrouting.CopyForkLoopRetainedJoin, []entryGolden{
			{Flow: ".", Entry: "queued", Order: []string{"queued", "working", "reviewing", "approved", "exhausted"}, Finals: []string{"approved", "exhausted"}},
		}},
		{"loop-join-variant", canonicalrouting.CopyForkLoopRetainedJoinSeparateCheckpoint, []entryGolden{
			{Flow: ".", Entry: "queued", Order: []string{"queued", "working", "reviewing", "approved", "exhausted"}, Finals: []string{"approved", "exhausted"}},
		}},
		{"constructed-gate", func(t testing.TB) string { return canonicalrouting.CopyConstructedGateForkControl(t, true) }, []entryGolden{
			{Flow: ".", Entry: "pending", Order: []string{"pending", "done"}, Finals: []string{"done"}},
		}},
		{"describe-variant", canonicalrouting.CopyDescribeStageGraph, []entryGolden{
			{Flow: "support", Entry: "waiting", Order: []string{"waiting", "active", "review", "timed_out"}, Finals: []string{"review", "timed_out"}},
		}},
	}
	for declarations := 0; declarations < 3; declarations++ {
		for _, frontier := range []string{"node", "activity", "activity_failure", "activity_rejected", "activity_write", "activity_write_failure", "activity_loop", "activity_loop_rule", "activity_loop_failure", "agent", "mixed", "mixed_progress"} {
			loop := strings.HasPrefix(frontier, "activity_loop")
			if loop && declarations != 0 || declarations == 0 && frontier != "node" && !strings.HasPrefix(frontier, "activity") {
				continue
			}
			order := []string{"idle", "complete"}
			if loop {
				order = []string{"idle", "working", "executing", "complete"}
			}
			fixtures = append(fixtures, generatedFixture{
				name: fmt.Sprintf("local-readiness/declared_%d/%s", declarations, frontier),
				build: func(t testing.TB) string {
					return canonicalrouting.CopySelectedForkLocalReadiness(t, declarations, frontier)
				},
				expected: []entryGolden{{Flow: "worker-flow", Entry: "idle", Order: order, Finals: []string{"complete"}}},
			})
		}
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			root := fixture.build(t)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
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
			for _, artifact := range []*sourceartifact.AdmittedSourceArtifact{bundle.SourceArtifact, catalog, retained} {
				if artifact.BundleHash() != bundle.SourceArtifact.BundleHash() || !bytes.Equal(artifact.LogicalBlob(), bundle.SourceArtifact.LogicalBlob()) {
					t.Fatal("catalog/retained reconstruction changed exact source bytes")
				}
				loaded, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, expected := range fixture.expected {
					graph, ok := loaded.WorkflowStageTopology(expected.Flow)
					if !ok || graph.InitialStage != expected.Entry || !reflect.DeepEqual(graph.StageIDs(), expected.Order) || !reflect.DeepEqual(graph.FinalStageIDs(), expected.Finals) {
						t.Fatalf("actual generated/retained source differs from reviewed %s: graph=%+v expected=%+v", expected.Flow, graph, expected)
					}
				}
			}
		})
	}
}

func TestRewrite2566EntryGoldenMatchesTypedCorpus(t *testing.T) {
	body, err := os.ReadFile("entries.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entryGolden
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("missing independently reviewed entry golden")
	}
	for _, entry := range entries {
		t.Run(entry.File+"/"+entry.Flow, func(t *testing.T) {
			if entry.Function != "" {
				assertEmbeddedEntryGolden(t, entry)
				return
			}
			if len(entry.EmbeddedPath) != 0 {
				assertSpecEntryGolden(t, entry)
				return
			}
			if filepath.ToSlash(filepath.Join(entry.Source, entry.Flow, filepath.Base(entry.File))) != entry.File {
				t.Fatalf("invalid exact source/flow tuple: %+v", entry)
			}
			body, err := os.ReadFile(filepath.Join("../..", entry.File))
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := yamlsource.Load(body)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document(entry.File).Root())
			if err != nil {
				t.Fatal(err)
			}
			if err := assertEntryGolden(entry, schema); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertSpecEntryGolden(t *testing.T, expected entryGolden) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("../..", expected.File))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		t.Fatal(err)
	}
	value := snapshot.Document(expected.File).Root()
	for _, key := range expected.EmbeddedPath {
		field, err := value.Lookup(key)
		if err != nil || field.Presence == yamlsource.PresenceMissing {
			t.Fatalf("missing embedded source field %s: %v", key, err)
		}
		value = field.Value
	}
	scalar, err := value.Scalar()
	if err != nil {
		t.Fatal(err)
	}
	embedded, err := yamlsource.Load([]byte(scalar.Value))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := contracts.AdmitFlowSchemaValue(embedded.Document(expected.Flow).Root())
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEntryGolden(expected, schema); err != nil {
		t.Fatal(err)
	}
}

func assertEmbeddedEntryGolden(t *testing.T, expected entryGolden) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("../..", expected.File))
	if err != nil {
		t.Fatal(err)
	}
	sites, err := goLiteralSites(expected.File, body)
	if err != nil {
		t.Fatal(err)
	}
	for _, site := range sites {
		if site.Function != expected.Function || site.Ordinal != expected.Literal {
			continue
		}
		snapshot, err := yamlsource.Load([]byte(site.Body))
		if err != nil {
			t.Fatal(err)
		}
		schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document(expected.File).Root())
		if err != nil {
			t.Fatal(err)
		}
		if err := assertEntryGolden(expected, schema); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("reviewed embedded source disappeared: %+v", expected)
}

func TestRewrite2566EntryGoldenRejectsSortedDumpWithoutStranding(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("stages:\n  cooling: {}\n  done: {final: true}\n  registered: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document("schema.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "registry")
	if err != nil {
		t.Fatal(err)
	}
	transitions := []contracts.HandlerTransitionSemantic{}
	for _, target := range []string{"cooling", "registered", "done"} {
		transitions = append(transitions, contracts.HandlerTransitionSemantic{Node: node, EventType: target, AdvancesTo: target})
	}
	graph := contracts.BuildWorkflowStageTopology(".", schema.StageDeclarations.InitialStage(), []string{"cooling", "done", "registered"}, []string{"done"}, transitions, nil, nil)
	if reached := graph.LifecycleReachableStages(graph.InitialStage); len(reached) != 3 {
		t.Fatalf("hostile sorting stranded stages instead of exercising the independent entry guard: %v", reached)
	}
	if err := assertEntryGolden(entryGolden{File: "schema.yaml", Source: "registry", Flow: ".", Entry: "registered", Order: []string{"registered", "cooling", "done"}, Finals: []string{"done"}}, schema); err == nil {
		t.Fatal("sorted dump silently changed the reviewed entry despite full reachability")
	}
}
