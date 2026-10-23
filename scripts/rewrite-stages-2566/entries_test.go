package main

import (
	"bytes"

	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"

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
	if err := validateCurrentEntrySelector(expected); err != nil {
		t.Fatal(err)
	}
	file, function, ordinal := expected.File, expected.Function, expected.Literal
	if current := expected.Current; current != nil {
		file, function, ordinal = current.File, current.Function, current.Literal
	}
	site, err := readEntryLiteral("../..", file, function, ordinal)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(site.Body)
	if expected.Current != nil && expected.Current.Materialize {
		var root string
		switch function {
		case "CopyPipelineDeliveryAuthority":
			root = canonicalrouting.CopyPipelineDeliveryAuthority(t)
		case "CopyPipelineDeliveryRetry":
			root = canonicalrouting.CopyPipelineDeliveryRetry(t)
		default:
			t.Fatalf("unsupported closed entry source %s", function)
		}
		body, err = os.ReadFile(filepath.Join(root, "schema.yaml"))
		if err != nil || !bytes.Equal(body, []byte(site.Body)) {
			t.Fatalf("actual closed source differs from its selected schema: %v", err)
		}
	}
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := contracts.AdmitFlowSchemaValue(snapshot.Document(file).Root())
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEntryGolden(expected, schema); err != nil {
		t.Fatal(err)
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
