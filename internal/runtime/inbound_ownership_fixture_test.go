package runtime

import (
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func newInboundTestEventBus(t testing.TB, store runtimebus.EventStore, targets ...InboundTarget) (*runtimebus.EventBus, error) {
	return newInboundTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{}, targets...)
}

// Transport tests supply their selected execution owners explicitly. These
// isolated declarations do not replace the source-loaded HTTP lifecycle proofs.
func newInboundTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions, targets ...InboundTarget) (*runtimebus.EventBus, error) {
	t.Helper()
	if len(targets) == 0 {
		targets = []InboundTarget{testInboundTarget("", "")}
	}
	rawEvents := []string{"inbound.telegram", "inbound.github.push", "inbound.github.issue_comment", "inbound.slack.message_channels", "inbound.slack.message", "inbound.stripe", "inbound.twilio", "inbound.shopify", "inbound.typeform", "inbound.intercom", "inbound.partner", "inbound.json"}
	root := &runtimecontracts.FlowContractView{Path: ".", Schema: runtimecontracts.FlowSchemaDocument{Name: "inbound-fixture"}, Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
	byID := map[string]*runtimecontracts.FlowContractView{".": root}
	schemas := make(map[string]runtimecontracts.FlowSchemaDocument)
	ingress := make(map[string][]string)
	for _, target := range targets {
		flow := runtimecontracts.FlowContractView{Path: target.FlowPath, Paths: runtimecontracts.FlowContractPaths{FlowPath: target.FlowPath}}
		for _, event := range rawEvents {
			flow.Schema.Pins.Inputs.EventPins = append(flow.Schema.Pins.Inputs.EventPins, runtimecontracts.FlowInputEventPin{Event: event})
		}
		if len(opts.Interceptors) > 0 {
			flow.Events = make(map[string]runtimecontracts.EventCatalogEntry, len(rawEvents))
			handlers := make(map[string]runtimecontracts.SystemNodeEventHandler, len(rawEvents))
			for _, event := range rawEvents {
				handlers[event] = runtimecontracts.SystemNodeEventHandler{}
				flow.Events[event] = providertriggers.RawEventCatalogEntry()
			}
			flow.Nodes = map[string]runtimecontracts.SystemNodeContract{"observer": {SubscribesTo: rawEvents, EventHandlers: handlers}}
		}
		root.Children = append(root.Children, flow)
		ingress[target.FlowPath] = append([]string(nil), rawEvents...)
	}
	for index := range root.Children {
		flow := &root.Children[index]
		flow.Parent = root
		byID[flow.Path] = flow
		schemas[flow.Path] = flow.Schema
	}
	bundle := &runtimecontracts.WorkflowContractBundle{RootSchema: &root.Schema, FlowSchemas: schemas, FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: byID}}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	opts.ContractBundle = semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), ingress)
	if opts.SourceArtifactFact.Validate() != nil {
		opts.SourceArtifactFact = testSourceArtifactFact(t, targets[0].BundleHash)
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = runtimeTestOccurrence(t, opts.SourceArtifactFact.BundleHash())
	}
	return newRuntimeTestEventBusWithOptions(t, store, opts)
}
