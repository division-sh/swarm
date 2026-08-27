package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompositionConnectFactsExposeCanonicalReceiverResolution(t *testing.T) {
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
	inputPins = source.FlowInputEventPins("account")
	if len(inputPins) != 2 {
		t.Fatalf("FlowInputEventPins = %#v, want two", inputPins)
	}
	inputPin := inputPins[1]
	if got, want := inputPin.EventType(), "account.ready"; got != want {
		t.Fatalf("input pin event = %q, want %q", got, want)
	}
	resolution := inputPin.Resolution()
	if resolution.Mode != runtimecontracts.FlowInputResolutionModeSelect {
		t.Fatalf("input pin resolution = %#v, want select", resolution)
	}
	if resolution.From != "" {
		t.Fatalf("input pin resolution.from = %q, want canonical same-name derivation", resolution.From)
	}

	outputPins := source.FlowOutputEventPins("producer")
	if len(outputPins) != 2 {
		t.Fatalf("FlowOutputEventPins = %#v, want two", outputPins)
	}
	outputPin := outputPins[1]
	if got, want := outputPin.EventType(), "account.ready"; got != want {
		t.Fatalf("output pin event = %q, want %q", got, want)
	}
	if outputPin.Digest() == "" {
		t.Fatal("output pin has no immutable digest")
	}

	connects := bundle.CompositionConnects()
	if len(connects) != 2 {
		t.Fatalf("CompositionConnects = %#v, want two", connects)
	}
	connect := connects[1]
	if got, want := connect.Event, "account.ready"; got != want {
		t.Fatalf("connect event = %q, want %q", got, want)
	}
	if got, want := connect.From, "producer"; got != want {
		t.Fatalf("connect from = %q, want %q", got, want)
	}
	if got, want := connect.To, "account"; got != want {
		t.Fatalf("connect to = %q, want %q", got, want)
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
	if got, want := outputPins[0].EventType(), "root.ready"; got != want {
		t.Fatalf("root output pin name = %q, want %q", got, want)
	}

	connects := bundle.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("CompositionConnects = %#v, want one", connects)
	}
	if got, want := connects[0].Event, "root.ready"; got != want {
		t.Fatalf("connect event = %q, want %q", got, want)
	}
	if got, want := connects[0].From, "."; got != want {
		t.Fatalf("connect from = %q, want root sentinel", got)
	}
}

func writeCompositionConnectSemanticFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
}

func writeRootCompositionConnectSemanticFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootOutputConnect(t, canonicalrouting.RootConnectNoEmitter)
}
