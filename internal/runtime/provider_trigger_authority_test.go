package runtime

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"gopkg.in/yaml.v3"
)

func TestProviderIngressDeclaringFlowAuthority(t *testing.T) {
	for _, tc := range []struct {
		name        string
		copyChat    bool
		copyIngress bool
		fanout      bool
		rootIngress bool
	}{
		{name: "declared child connection"},
		{name: "unwired schema importer", copyChat: true},
		{name: "explicit fanout", copyChat: true, fanout: true},
		{name: "distinct ingress aliases", copyChat: true, copyIngress: true},
		{name: "selected root ingress", rootIngress: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
			if tc.copyChat {
				if err := os.CopyFS(filepath.Join(root, "copied-chat"), os.DirFS(filepath.Join(root, "telegram-chat"))); err != nil {
					t.Fatal(err)
				}
			}
			if tc.copyIngress {
				if err := os.CopyFS(filepath.Join(root, "other-ingress"), os.DirFS(filepath.Join(root, "telegram-ingress"))); err != nil {
					t.Fatal(err)
				}
				mutateProviderAuthorityYAML(t, filepath.Join(root, "other-ingress", "schema.yaml"), func(schema map[string]any) {
					schema["ingress"].(map[string]any)["alias"] = "other-chat"
				})
			}
			sender := "telegram-ingress"
			if tc.rootIngress {
				raw, err := os.ReadFile(filepath.Join(root, "telegram-ingress", "schema.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "schema.yaml"), raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.RemoveAll(filepath.Join(root, "telegram-ingress")); err != nil {
					t.Fatal(err)
				}
				sender = "."
			}
			connections := []map[string]any{{"event": "inbound.telegram.text_message", "from": sender, "to": "telegram-chat", "resolution": "select-or-create"}}
			want := map[string][]string{sender: {"telegram-chat"}}
			if tc.fanout {
				connections = append(connections, map[string]any{"event": "inbound.telegram.text_message", "from": sender, "to": "copied-chat", "resolution": "select-or-create"})
				want[sender] = []string{"copied-chat", "telegram-chat"}
			}
			if tc.copyIngress {
				connections = append(connections, map[string]any{"event": "inbound.telegram.text_message", "from": "other-ingress", "to": "copied-chat", "resolution": "select-or-create"})
				want["other-ingress"] = []string{"copied-chat"}
			}
			mutateProviderAuthorityYAML(t, filepath.Join(root, "schema.yaml"), func(schema map[string]any) { schema["connect"] = connections })
			source := loadProviderAuthoritySource(t, root)
			graph := pinrouting.CompileConnectGraph(source)
			missingCopiedInput := false
			findings := bootverify.Run(context.Background(), source, bootverify.Options{}).HardInvalidities()
			for _, finding := range findings {
				if finding.CheckID == "input_pin_wiring" && finding.Location == "copied-chat" {
					missingCopiedInput = true
				}
			}
			if wantMissing := tc.copyChat && !tc.fanout && !tc.copyIngress; missingCopiedInput != wantMissing {
				t.Fatalf("copied schema importer missing-connection blocker=%v want=%v; findings=%#v", missingCopiedInput, wantMissing, findings)
			}
			if issues := graph.Issues(); len(issues) != 0 {
				t.Fatalf("compiled connections: %#v", issues)
			}
			if len(graph.Plans()) != len(connections) {
				t.Fatalf("got %d plans for %d authored connections", len(graph.Plans()), len(connections))
			}
			for flow, receivers := range want {
				pin, ok := source.FlowOutputEventPin(flow, "inbound.telegram.text_message")
				if !ok {
					t.Fatalf("missing output pin in %s", flow)
				}
				if schema, ok := pin.EventSchema(); !ok || schema.Classification() != runtimecontracts.CompiledEventSchemaImported {
					t.Fatalf("output lacks immutable imported schema: %#v", schema)
				}
				got := providerAuthorityReceivers(t, graph, flow, "inbound.telegram.text_message")
				if !reflect.DeepEqual(got, receivers) {
					t.Fatalf("entry %s receivers = %v, want %v", flow, got, receivers)
				}
				for _, receiver := range receivers {
					proof := pinrouting.ResolveFlowInputProducer(source, receiver, "inbound.telegram.text_message")
					if !proof.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
						t.Fatalf("connected receiver %s has no compiled producer evidence: %#v", receiver, proof.Evidence)
					}
				}
				if got := providerAuthorityReceivers(t, graph, flow, "inbound.telegram"); len(got) != 0 {
					t.Fatalf("raw entry acquired normalized connection: %v", got)
				}
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			decoded, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			repo := canonicalrouting.RepoRoot(t)
			reconstructed, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, decoded, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			reloadedGraph := pinrouting.CompileConnectGraph(providerAuthoritySourceFromBundle(t, reconstructed))
			if len(reloadedGraph.Issues()) != 0 || len(reloadedGraph.Plans()) != len(graph.Plans()) {
				t.Fatalf("source-deleted reconstruction lost edges: %#v", reloadedGraph.Issues())
			}
			for index, plan := range graph.Plans() {
				restored := reloadedGraph.Plans()[index]
				if !reflect.DeepEqual(plan.Readback(), restored.Readback()) || !reflect.DeepEqual(plan.ProviderOutputAuthorization(), restored.ProviderOutputAuthorization()) {
					t.Fatal("source-deleted reconstruction changed provider edge authority or pin digests")
				}
			}
			for _, flow := range []string{"unbound-ingress", "telegram-chat", "copied-chat"} {
				if got := providerAuthorityReceivers(t, graph, flow, "inbound.telegram.text_message"); len(got) != 0 {
					t.Fatalf("unbound/importing flow %s acquired connections: %v", flow, got)
				}
			}
		})
	}
}

func TestSchemaImportedRootInputRoutesOnlyExplicitConnection(t *testing.T) {
	root := canonicalrouting.CopyTelegramChatWithoutIngress(t)
	source := loadProviderAuthoritySource(t, root)
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Issues()) != 0 || len(graph.Plans()) != 1 {
		t.Fatalf("root connection: plans=%#v issues=%#v", graph.Plans(), graph.Issues())
	}
	routing, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	event, err := pinrouting.AdmitSourceEvent("inbound.telegram.text_message", routing)
	if err != nil {
		t.Fatal(err)
	}
	plans := graph.MatchingSourceEvent(event)
	if len(plans) != 1 || plans[0].ProviderOutputAuthorization() != nil || plans[0].ReceiverEndpoint().Readback().FlowID != "telegram-chat" {
		t.Fatalf("schema import changed root connection authority: %#v", plans)
	}
}

func TestRawProviderOutputUsesOnlyExplicitDeclaringFlowConnection(t *testing.T) {
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	if err := os.RemoveAll(filepath.Join(root, "telegram-chat")); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(filepath.Join(root, "other-ingress"), os.DirFS(filepath.Join(root, "telegram-ingress"))); err != nil {
		t.Fatal(err)
	}
	mutateProviderAuthorityYAML(t, filepath.Join(root, "other-ingress", "schema.yaml"), func(schema map[string]any) {
		schema["ingress"].(map[string]any)["alias"] = "other-chat"
	})
	mutateProviderAuthorityYAML(t, filepath.Join(root, "telegram-ingress", "schema.yaml"), func(schema map[string]any) {
		schema["pins"].(map[string]any)["outputs"] = []any{"inbound.telegram"}
	})
	mutateProviderAuthorityYAML(t, filepath.Join(root, "schema.yaml"), func(schema map[string]any) {
		schema["connect"] = []any{map[string]any{"event": "inbound.telegram", "from": "telegram-ingress", "to": "other-ingress"}}
	})
	graph := pinrouting.CompileConnectGraph(loadProviderAuthoritySource(t, root))
	if len(graph.Issues()) != 0 || len(graph.Plans()) != 1 {
		t.Fatalf("raw connection admission: plans=%#v issues=%#v", graph.Plans(), graph.Issues())
	}
	if got := providerAuthorityReceivers(t, graph, "telegram-ingress", "inbound.telegram"); !reflect.DeepEqual(got, []string{"other-ingress"}) {
		t.Fatalf("declared raw connection receivers=%v", got)
	}
	for _, source := range []string{"other-ingress", ".", "unbound"} {
		if got := providerAuthorityReceivers(t, graph, source, "inbound.telegram"); len(got) != 0 {
			t.Fatalf("unconnected source %s acquired raw receivers=%v", source, got)
		}
	}
}

func TestSelectedRootProviderInputIsNotAmbiguousPublicEligibility(t *testing.T) {
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	path := filepath.Join(root, "telegram-ingress", "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "telegram-ingress")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "telegram-chat")); err != nil {
		t.Fatal(err)
	}
	source := loadProviderAuthoritySource(t, root)
	for _, finding := range bootverify.Run(context.Background(), source, bootverify.Options{}).HardInvalidities() {
		if finding.CheckID == "input_pin_wiring" {
			t.Errorf("public eligibility conflicts with genuine local provider: %#v", finding)
		}
	}
}

func TestProviderRootReceiverBindsImportedInputAtCanonicalCoordinate(t *testing.T) {
	const eventName = "inbound.telegram.text_message"
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	if err := os.RemoveAll(filepath.Join(root, "telegram-chat")); err != nil {
		t.Fatal(err)
	}
	mutateProviderAuthorityYAML(t, filepath.Join(root, "schema.yaml"), func(schema map[string]any) {
		schema["imports"] = map[string]any{"provider_trigger_events": []any{
			map[string]any{"provider": "telegram", "event": eventName},
		}}
		schema["pins"] = map[string]any{"inputs": []any{eventName}}
		schema["connect"] = []any{map[string]any{"event": eventName, "from": "telegram-ingress", "to": "."}}
	})
	canonicalrouting.AddProviderRootConsumer(t, root)
	source := loadProviderAuthoritySource(t, root)
	pin, ok := source.FlowInputEventPin(".", eventName)
	if !ok {
		t.Fatal("canonical root input is missing")
	}
	schema, ok := pin.ProducerEventSchema()
	if !ok || schema.Classification() != runtimecontracts.CompiledEventSchemaImported {
		t.Fatal("root input did not bind the imported provider schema")
	}
	pins := source.FlowInputEventPins(".")
	pins[0] = runtimecontracts.CompiledFlowInputPin{}
	again, ok := source.FlowInputEventPin(".", eventName)
	if !ok || again.Digest() != pin.Digest() {
		t.Fatal("root input projection mutated the compiled owner")
	}
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Issues()) != 0 || len(graph.Plans()) != 1 {
		t.Fatalf("provider-to-root graph: issues=%#v plans=%#v", graph.Issues(), graph.Plans())
	}
	if plan := graph.Plans()[0]; plan.ProviderOutputAuthorization() == nil || plan.ReceiverEndpoint().Readback().FlowID != "." {
		t.Fatalf("provider-to-root authority changed: %#v", plan.Readback())
	}
	if err := os.Remove(filepath.Join(root, "nodes.yaml")); err != nil {
		t.Fatal(err)
	}
	graph = pinrouting.CompileConnectGraph(loadProviderAuthoritySource(t, root))
	if len(graph.Plans()) != 0 || len(graph.Issues()) != 1 || graph.Issues()[0].Failure != pinrouting.ConnectFailureDeliveryTopologyInvalid {
		t.Fatalf("imported root schema became a consumer: plans=%#v issues=%#v", graph.Plans(), graph.Issues())
	}
}

func TestProviderIngressBindingRemovalDoesNotTransferConnections(t *testing.T) {
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	if err := os.CopyFS(filepath.Join(root, "other-ingress"), os.DirFS(filepath.Join(root, "telegram-ingress"))); err != nil {
		t.Fatal(err)
	}
	mutateProviderAuthorityYAML(t, filepath.Join(root, "other-ingress", "schema.yaml"), func(schema map[string]any) {
		schema["ingress"].(map[string]any)["alias"] = "other-chat"
	})
	mutateProviderAuthorityYAML(t, filepath.Join(root, "schema.yaml"), func(schema map[string]any) {
		connections := schema["connect"].([]any)
		schema["connect"] = append(connections, map[string]any{"event": "inbound.telegram.text_message", "from": "other-ingress", "to": "telegram-chat", "resolution": "select-or-create"})
	})
	for _, step := range []struct {
		remove string
		plans  int
		issues int
	}{
		{plans: 3},
		{remove: "telegram-ingress", plans: 2, issues: 1},
		{remove: "other-ingress", plans: 1, issues: 2},
	} {
		if step.remove != "" {
			mutateProviderAuthorityYAML(t, filepath.Join(root, step.remove, "schema.yaml"), func(schema map[string]any) { delete(schema, "ingress") })
		}
		source := loadProviderAuthoritySource(t, root)
		graph := pinrouting.CompileConnectGraph(source)
		if len(graph.Plans()) != step.plans || len(graph.Issues()) != step.issues {
			t.Fatalf("remove %q: plans=%d issues=%#v", step.remove, len(graph.Plans()), graph.Issues())
		}
		if step.remove != "" {
			if _, bound := source.SemanticCapabilities().ProviderTriggerOutputAuthorization(step.remove, "inbound.telegram.text_message"); bound {
				t.Fatalf("removed ingress %s retains output authority", step.remove)
			}
			if got := providerAuthorityReceivers(t, graph, step.remove, "inbound.telegram.text_message"); len(got) != 0 {
				t.Fatalf("removed ingress %s matched receivers: %v", step.remove, got)
			}
		}
		if step.remove != "other-ingress" {
			if got := providerAuthorityReceivers(t, graph, "other-ingress", "inbound.telegram.text_message"); !reflect.DeepEqual(got, []string{"telegram-chat"}) {
				t.Fatalf("surviving entry receivers = %v", got)
			}
		}
	}
}

func loadProviderAuthoritySource(t testing.TB, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	return providerAuthoritySourceFromBundle(t, bundle)
}

func providerAuthoritySourceFromBundle(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) semanticview.Source {
	t.Helper()
	packs, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	source, err := SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), packs.ProviderTriggers)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mutateProviderAuthorityYAML(t testing.TB, path string, mutate func(map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	raw, err = yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func providerAuthorityReceivers(t testing.TB, graph pinrouting.CompiledConnectGraph, flow, event string) []string {
	t.Helper()
	routing, err := events.NewExternalIngressRoutingSource(flow, "admitted-entry-entity", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	source, err := pinrouting.AdmitSourceEvent(events.EventType(event), routing)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, plan := range graph.MatchingSourceEvent(source) {
		if (plan.ProviderOutputAuthorization() != nil) != (event != "inbound.telegram") {
			t.Fatal("provider connection confused raw admission with normalized output authentication")
		}
		out = append(out, plan.ReceiverEndpoint().Readback().FlowID)
	}
	sort.Strings(out)
	return out
}
