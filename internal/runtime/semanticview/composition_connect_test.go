package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompositionConnectFactsExposeTypedPinsAndParentConnect(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeCompositionConnectSemanticFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	inputPins := source.FlowInputEventPins("consumer")
	if len(inputPins) != 1 {
		t.Fatalf("FlowInputEventPins = %#v, want one", inputPins)
	}
	if got, want := inputPins[0].PinName(), "work_ready"; got != want {
		t.Fatalf("input pin name = %q, want %q", got, want)
	}
	if got, want := inputPins[0].EventType(), "work.ready"; got != want {
		t.Fatalf("input pin event = %q, want %q", got, want)
	}
	if inputPins[0].Address == nil || inputPins[0].Address.By != "work_id" {
		t.Fatalf("input pin address = %#v, want work_id", inputPins[0].Address)
	}

	outputPins := source.FlowOutputEventPins("producer")
	if len(outputPins) != 1 {
		t.Fatalf("FlowOutputEventPins = %#v, want one", outputPins)
	}
	if got, want := outputPins[0].PinName(), "work_ready"; got != want {
		t.Fatalf("output pin name = %q, want %q", got, want)
	}
	if got, want := outputPins[0].Key, "work_id"; got != want {
		t.Fatalf("output pin key = %q, want %q", got, want)
	}
	if got, want := len(outputPins[0].Carries), 1; got != want || outputPins[0].Carries[0] != "work_id" {
		t.Fatalf("output pin carries = %#v, want [work_id]", outputPins[0].Carries)
	}

	connects := source.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("CompositionConnects = %#v, want one", connects)
	}
	if got, want := connects[0].From, "producer.work_ready"; got != want {
		t.Fatalf("connect from = %q, want %q", got, want)
	}
	if got, want := connects[0].To, "consumer.work_ready"; got != want {
		t.Fatalf("connect to = %q, want %q", got, want)
	}
	if got, want := source.CompositionConnectsFrom("producer", "work_ready"), connects; len(got) != len(want) || got[0].From != want[0].From {
		t.Fatalf("CompositionConnectsFrom = %#v, want %#v", got, want)
	}
	if got, want := source.CompositionConnectsTo("consumer", "work_ready"), connects; len(got) != len(want) || got[0].To != want[0].To {
		t.Fatalf("CompositionConnectsTo = %#v, want %#v", got, want)
	}
}

func TestCompositionConnectFactsExposeRootProducerEndpoint(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := writeRootCompositionConnectSemanticFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	outputPins := source.FlowOutputEventPins("")
	if len(outputPins) != 1 {
		t.Fatalf("root FlowOutputEventPins = %#v, want one", outputPins)
	}
	if got, want := outputPins[0].PinName(), "root_ready"; got != want {
		t.Fatalf("root output pin name = %q, want %q", got, want)
	}

	connects := source.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("CompositionConnects = %#v, want one", connects)
	}
	from, err := connects[0].FromRef()
	if err != nil {
		t.Fatalf("FromRef: %v", err)
	}
	if !from.Root || from.FlowID != "" || from.Pin != "root_ready" {
		t.Fatalf("FromRef = %#v, want root root_ready", from)
	}
	if got, want := source.CompositionConnectsFrom("", "root_ready"), connects; len(got) != len(want) || got[0].From != want[0].From {
		t.Fatalf("CompositionConnectsFrom root = %#v, want %#v", got, want)
	}
}

func writeCompositionConnectSemanticFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyParentConnectAddressVariant(t, canonicalrouting.ParentConnectAddressSemanticView)
}

func writeRootCompositionConnectSemanticFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=none owner=semanticview.root_connect_projection proof=internal/runtime/semanticview/composition_connect_test.go:TestCompositionConnectFactsExposeRootProducerEndpoint
	t.Helper()
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: root-composition-connect-semantic
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: .root_ready
    to: consumer.ready
    delivery: one
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: root-composition-connect-semantic
pins:
  outputs:
    events:
      - name: root_ready
        event: root.ready
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "root.ready: {}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeCompositionConnectFlow(t, root, "consumer", `
pins:
  inputs:
    events:
      - name: ready
        event: root.ready
`, "root.ready: {}\n", "{}\n")
	return root
}

func writeCompositionConnectFlow(t *testing.T, root, flowID, schemaTail, events, entities string) {
	t.Helper()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
`+schemaTail)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), events)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), entities)
}
