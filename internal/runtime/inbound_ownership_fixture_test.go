package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

type inboundFixtureOwners []InboundTarget

func (owners inboundFixtureOwners) ListSelectedRunTargetOwners(_ context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	var out []runtimebus.ActiveTargetDescriptor
	for _, owner := range owners {
		if owner.RunID == runID {
			out = append(out, runtimebus.ActiveTargetDescriptor{FlowInstance: owner.FlowInstance, EntityID: owner.EntityID})
		}
	}
	return out, nil
}

func (owners inboundFixtureOwners) ListSelectedRunTargetOwnersForScope(ctx context.Context, runID string, instancePaths []string, sourceEntityID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if len(instancePaths) == 0 && sourceEntityID == "" {
		return nil, errors.New("target owner lookup requires a selected scope")
	}
	paths := make(map[string]struct{}, len(instancePaths))
	for _, path := range instancePaths {
		paths[path] = struct{}{}
	}
	all, err := owners.ListSelectedRunTargetOwners(ctx, runID)
	if err != nil {
		return nil, err
	}
	var selected []runtimebus.ActiveTargetDescriptor
	for _, owner := range all {
		_, byPath := paths[owner.FlowInstance]
		if byPath || sourceEntityID != "" && owner.EntityID == sourceEntityID {
			selected = append(selected, owner)
		}
	}
	return selected, nil
}

func TestInboundFixtureOwnersRespectGraphScope(t *testing.T) {
	owners := inboundFixtureOwners{
		{RunID: "run-1", FlowInstance: "root/first", EntityID: "entity-1"},
		{RunID: "run-1", FlowInstance: "root/second", EntityID: "entity-2"},
		{RunID: "run-2", FlowInstance: "root/first", EntityID: "entity-1"},
	}
	selected, err := owners.ListSelectedRunTargetOwnersForScope(context.Background(), "run-1", []string{"root/first"}, "")
	if err != nil || len(selected) != 1 || selected[0].EntityID != "entity-1" {
		t.Fatalf("path scope selected=%#v err=%v", selected, err)
	}
	selected, err = owners.ListSelectedRunTargetOwnersForScope(context.Background(), "run-1", nil, "entity-2")
	if err != nil || len(selected) != 1 || selected[0].FlowInstance != "root/second" {
		t.Fatalf("source-entity scope selected=%#v err=%v", selected, err)
	}
	if _, err := owners.ListSelectedRunTargetOwnersForScope(context.Background(), "run-1", nil, ""); err == nil {
		t.Fatal("empty graph scope was accepted")
	}
}

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
	root := &runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
	byID := map[string]*runtimecontracts.FlowContractView{".": root}
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
		byID[flow.Path] = flow
	}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: byID}}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	opts.ContractBundle = semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), ingress)
	opts.Durable.TargetOwners = inboundFixtureOwners(targets)
	return newRuntimeTestEventBusWithOptions(t, store, opts)
}
