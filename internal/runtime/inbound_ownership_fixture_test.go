package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"gopkg.in/yaml.v3"
)

// The external test package supplies the native store composition without an
// import cycle through storetest's runtime-facing helpers.
var InboundTransportNativeIndexFixture func(testing.TB, semanticview.Source, []InboundTarget) runtimepipeline.FlowInstanceIndexReader

func newInboundTestEventBus(t testing.TB, store runtimebus.EventStore, targets ...InboundTarget) (*runtimebus.EventBus, *providertriggers.CatalogSnapshot, error) {
	return newInboundTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{}, targets...)
}

func inboundTransportSource(t testing.TB, targets []InboundTarget, observe bool) semanticview.Source {
	t.Helper()
	flows := make([]string, 0, len(targets))
	for _, target := range targets {
		flows = append(flows, target.FlowPath)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo,
		copyInboundTransport(t, flows, observe), runtimecontracts.DefaultPlatformSpecFile(repo),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source, err := SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), nil)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

// Keep provider-aware fixture assembly beside its runtime-only caller, not in
// the low-level corpus package consumed by contracts and semanticview tests.
func copyInboundTransport(t testing.TB, flows []string, observe bool) string {
	t.Helper()
	root := t.TempDir()
	write := func(relativePath, source string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(relativePath))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
	}
	manifest := "version: 1.0.0\nplatform_version: '*'\n"
	write("manifest.yaml", manifest)
	write("schema.yaml", "stages: []\n")
	events := []string{"inbound.telegram.raw", "inbound.telegram.text_message", "inbound.github.raw.push", "inbound.github.raw.issue_comment", "inbound.slack.message_channels", "inbound.slack.message", "inbound.stripe.raw", "inbound.twilio.raw", "inbound.shopify.raw", "inbound.typeform.raw", "inbound.intercom.raw", "inbound.custom.raw"}
	inventory := packfixture.EmbeddedInventory(t)
	flows = append([]string(nil), flows...)
	sort.Strings(flows)
	for i, flow := range flows {
		if i != 0 && flow == flows[i-1] {
			continue
		}
		switch flow {
		case "test-inbound-flow", "chat-flow", "ingress", "partner-flow", "json-flow", "chat", "coordinator":
		default:
			t.Fatalf("unclassified inbound transport flow %q", flow)
		}
		write(flow+"/manifest.yaml", manifest)
		write(flow+"/schema.yaml", "stages: []\npins:\n  inputs: ["+strings.Join(events, ", ")+"]\n")
		for _, entry := range inventory.Entries() {
			if _, present := entry.Provider().Body(packmodel.BodyTrigger); !present {
				continue
			}
			write(flow+"/"+entry.ID()+"/manifest.yaml", string(entry.ManifestBody()))
			for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
				if body, present := entry.Provider().Body(role); present {
					write(flow+"/"+entry.ID()+"/"+role.FileName(), string(body.Bytes()))
				}
			}
		}
		write(flow+"/custom/manifest.yaml", manifest+"capabilities: {can: {emit_events: [raw]}, cannot: [bypass journal]}\n")
		write(flow+"/custom/trigger.yaml", `payload_object_required: true
delivery_id: {json_path: $.id, required: true}
event_type: {json_path: $.type, required: true}
event_name: {literal: raw}
ack: {mode: durable_before_dispatch}
`)
		if observe {
			handlers := make(map[string]any, len(events))
			for _, event := range events {
				handlers[event] = map[string]any{}
			}
			for name, document := range map[string]any{
				"nodes.yaml": map[string]any{"observer": map[string]any{"execution_type": "system_node", "subscribes_to": events, "event_handlers": handlers}},
			} {
				raw, err := yaml.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				write(flow+"/"+name, string(raw))
			}
		}
	}
	return root
}

// The transport publication owner stays isolated; its instance observations
// come from genuine source-bound construction in the native selected store.
func newInboundTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions, targets ...InboundTarget) (*runtimebus.EventBus, *providertriggers.CatalogSnapshot, error) {
	t.Helper()
	if len(targets) == 0 {
		targets = []InboundTarget{testInboundTarget("", "")}
	}
	source := opts.ContractBundle
	if source == nil {
		source = inboundTransportSource(t, targets, len(opts.Interceptors) > 0)
	}
	bundle, present := semanticview.Bundle(source)
	if !present || bundle.SourceArtifact == nil {
		return nil, nil, fmt.Errorf("transport fixture requires its admitted source artifact")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return nil, nil, err
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return nil, nil, err
	}
	scope, err := providers.Scope(targets[0].FlowPath)
	if err != nil {
		return nil, nil, err
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		return nil, nil, err
	}
	fact := testSourceArtifactFact(t, bundle.SourceArtifact.BundleHash())
	if opts.SourceArtifactFact.Validate() == nil && opts.SourceArtifactFact != fact {
		return nil, nil, fmt.Errorf("transport fixture source fact contradicts its source")
	}
	opts.ContractBundle, opts.SourceArtifactFact = source, fact
	opts.ProviderOutputVerifier = catalog
	if opts.WorkOwner == nil {
		opts.WorkOwner = runtimeTestOccurrence(t, fact.BundleHash())
	}
	for _, target := range targets {
		if target.BundleHash != "" && target.BundleHash != fact.BundleHash() {
			return nil, nil, fmt.Errorf("transport fixture target source contradicts its source")
		}
	}
	if InboundTransportNativeIndexFixture == nil {
		return nil, nil, fmt.Errorf("native transport fixture composition is not installed")
	}
	opts.Durable.Instances = InboundTransportNativeIndexFixture(t, source, targets)
	bus, err := newRuntimeTestEventBusWithOptions(t, store, opts)
	return bus, catalog, err
}

func inboundTestBusBundleHash(t testing.TB, bus *runtimebus.EventBus) string {
	t.Helper()
	ctx, err := bus.AdmitSourceArtifactFact(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fact, present := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !present {
		t.Fatal("transport bus omitted its admitted source fact")
	}
	return fact.BundleHash()
}
