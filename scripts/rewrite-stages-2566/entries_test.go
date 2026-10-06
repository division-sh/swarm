package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
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
		t.Run(entry.File, func(t *testing.T) {
			if filepath.ToSlash(filepath.Join(entry.Source, entry.Flow, "schema.yaml")) != entry.File {
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
