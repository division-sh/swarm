package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
	if got, want := inputPins[0].PinName(), "deploy_completed"; got != want {
		t.Fatalf("input pin name = %q, want %q", got, want)
	}
	if got, want := inputPins[0].EventType(), "deploy.completed"; got != want {
		t.Fatalf("input pin event = %q, want %q", got, want)
	}
	if inputPins[0].Address == nil || inputPins[0].Address.By != "vertical_id" {
		t.Fatalf("input pin address = %#v, want vertical_id", inputPins[0].Address)
	}

	outputPins := source.FlowOutputEventPins("producer")
	if len(outputPins) != 1 {
		t.Fatalf("FlowOutputEventPins = %#v, want one", outputPins)
	}
	if got, want := outputPins[0].PinName(), "deploy_done"; got != want {
		t.Fatalf("output pin name = %q, want %q", got, want)
	}

	connects := source.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("CompositionConnects = %#v, want one", connects)
	}
	if got, want := connects[0].From, "producer.deploy_done"; got != want {
		t.Fatalf("connect from = %q, want %q", got, want)
	}
	if got, want := connects[0].To, "consumer.deploy_completed"; got != want {
		t.Fatalf("connect to = %q, want %q", got, want)
	}
	if got, want := source.CompositionConnectsFrom("producer", "deploy_done"), connects; len(got) != len(want) || got[0].From != want[0].From {
		t.Fatalf("CompositionConnectsFrom = %#v, want %#v", got, want)
	}
	if got, want := source.CompositionConnectsTo("consumer", "deploy_completed"), connects; len(got) != len(want) || got[0].To != want[0].To {
		t.Fatalf("CompositionConnectsTo = %#v, want %#v", got, want)
	}
}

func writeCompositionConnectSemanticFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-semantic
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: one
    map:
      vertical_id:
        source: payload.vertical_id
        target: entity.vertical_id
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-semantic\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeCompositionConnectFlow(t, root, "producer", `
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
`, "deploy.done: {}\n", "{}\n")
	writeCompositionConnectFlow(t, root, "consumer", `
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.completed
        address:
          by: vertical_id
          source: payload.vertical_id
          target: entity.vertical_id
          cardinality: one
`, "deploy.completed: {}\n", `
deployment:
  vertical_id:
    type: string
`)
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
