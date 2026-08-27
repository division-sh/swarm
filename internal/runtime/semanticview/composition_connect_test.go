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
	if got, want := inputPin.PinName(), "account_ready"; got != want {
		t.Fatalf("input pin name = %q, want %q", got, want)
	}
	if got, want := inputPin.EventType(), "account.ready"; got != want {
		t.Fatalf("input pin event = %q, want %q", got, want)
	}
	if inputPin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeSelect {
		t.Fatalf("input pin resolution = %#v, want select", inputPin.Resolution)
	}
	if carry := inputPin.Carries["account_id"]; carry.From != "payload.account_id" {
		t.Fatalf("input pin carry = %#v, want payload.account_id", carry)
	}

	outputPins := source.FlowOutputEventPins("producer")
	if len(outputPins) != 2 {
		t.Fatalf("FlowOutputEventPins = %#v, want two", outputPins)
	}
	outputPin := outputPins[1]
	if got, want := outputPin.PinName(), "account_ready"; got != want {
		t.Fatalf("output pin name = %q, want %q", got, want)
	}
	if outputPin.Key != "" || len(outputPin.Carries) != 0 {
		t.Fatalf("output pin key/carries = %q/%#v, want no receiver authority", outputPin.Key, outputPin.Carries)
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
	if got, want := outputPins[0].PinName(), "root_ready"; got != want {
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
