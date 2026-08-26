package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type effectiveSourceCapabilityOverride struct {
	semanticview.Source
	capabilities semanticview.Capabilities
}

func (s effectiveSourceCapabilityOverride) SemanticCapabilities() semanticview.Capabilities {
	return s.capabilities
}

func TestAdmittedEffectiveSourceProjectionBindsBaseProvenanceAndIsStable(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	root := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-swarm-bundle")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	persisted, _ := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	ephemeral, _ := runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)

	first, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), BundleSourceFact: persisted})
	if err != nil {
		t.Fatal(err)
	}
	second, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), BundleSourceFact: persisted})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Identity().Equal(second.Identity()) {
		t.Fatalf("same effective source produced different identities: %s != %s", first.Identity().Digest(), second.Identity().Digest())
	}
	other, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), BundleSourceFact: ephemeral})
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity().Digest() == other.Identity().Digest() {
		t.Fatal("persisted and ephemeral source provenance produced the same effective digest")
	}
	if !strings.HasPrefix(first.Identity().Digest(), "sha256:") {
		t.Fatalf("digest = %q", first.Identity().Digest())
	}
}

func TestEffectiveSourceIdentityChangesWithExternalSemanticGenerations(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	root := filepath.Join(repoRoot, "internal", "cliapp", "archetypes", "zero-agent-automation")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), BundleSourceFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	source := projection.Source()

	t.Run("provider trigger", func(t *testing.T) {
		first := triggergeneration.FromCanonicalBytes([]byte(`{"catalog":"first"}`))
		second := triggergeneration.FromCanonicalBytes([]byte(`{"catalog":"second"}`))
		if digestForTriggerGeneration(t, source, fact, first) == digestForTriggerGeneration(t, source, fact, second) {
			t.Fatal("provider-trigger generation change retained effective source digest")
		}
	})

	t.Run("connector", func(t *testing.T) {
		if digestForConnectorGeneration(t, source, fact, "a") == digestForConnectorGeneration(t, source, fact, "b") {
			t.Fatal("connector generation change retained effective source digest")
		}
	})

	t.Run("channel", func(t *testing.T) {
		first, second := effectiveSourceTestChannelPlans(t, repoRoot)
		if digestForEffectiveSourceValue(t, source, fact, []packs.SatisfactionPlan{first}) == digestForEffectiveSourceValue(t, source, fact, []packs.SatisfactionPlan{second}) {
			t.Fatal("channel plan generation change retained effective source digest")
		}
	})

	t.Run("channel deployment destination is a separate concept", func(t *testing.T) {
		plan, _ := effectiveSourceTestChannelPlans(t, repoRoot)
		first, err := packs.NewOutboundBindingPlan("ops", plan, "42", nil)
		if err != nil {
			t.Fatal(err)
		}
		second, err := packs.NewOutboundBindingPlan("ops", plan, "43", nil)
		if err != nil {
			t.Fatal(err)
		}
		if first.Destination().Interface() == second.Destination().Interface() {
			t.Fatal("test bindings do not exercise different deployment destinations")
		}
		before := digestForEffectiveSourceValue(t, source, fact, []packs.SatisfactionPlan{plan})
		after := digestForEffectiveSourceValue(t, source, fact, []packs.SatisfactionPlan{plan})
		if before != after {
			t.Fatal("mutable channel deployment contaminated immutable effective source identity")
		}
	})
}

func TestProjectedConnectorPackSourceAdmitsOneMockResponseAcrossEveryScope(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	root := filepath.Join(repoRoot, "internal", "cliapp", "archetypes", "zero-agent-automation")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	bundleHash, _ := runtimecontracts.BundleHash(bundle)
	fact, _ := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), BundleSourceFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := providerconnectors.CompileMockResponsePlan(projection.Source())
	if err != nil {
		t.Fatal(err)
	}
	const toolID = "telegram.send_message"
	entries := []runtimecontracts.ToolSchemaEntry{projection.Source().ToolEntries()[toolID]}
	for _, scope := range projection.Source().ProjectScopes() {
		if entry, ok := scope.Tools[toolID]; ok {
			entries = append(entries, entry)
		}
	}
	for _, scope := range projection.Source().FlowScopes() {
		if entry, ok := scope.Tools[toolID]; ok {
			entries = append(entries, entry)
		}
	}
	for index, entry := range entries {
		if _, err := plan.Admit(toolID, entry); err != nil {
			t.Fatalf("scope entry %d rejected generated response: %v", index, err)
		}
	}
	profile, err := llmselection.ResolveActiveBackend("claude_cli")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtimebootverify.PrepareSourceBootEffectContext(projection.Source(), profile, executionposture.MockOnly); err != nil {
		t.Fatalf("projected source failed boot effect admission: %v", err)
	}
}

func TestEffectiveSourceIdentityChangesWithAuthoredProviderConnectorSchema(t *testing.T) {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("7", 64))
	if err != nil {
		t.Fatal(err)
	}
	projection := func(kind runtimecontracts.ToolSchemaKind) AdmittedEffectiveSourceProjection {
		property, err := runtimecontracts.NewToolInputSchema(kind)
		if err != nil {
			t.Fatal(err)
		}
		output, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaObject,
			runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"value": property}),
			runtimecontracts.ToolSchemaRequired("value"),
		)
		if err != nil {
			t.Fatal(err)
		}
		empty := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
		tool, err := runtimecontracts.NewToolSchemaEntry(
			runtimecontracts.WithToolCategory("provider_connector"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"}),
			runtimecontracts.WithToolSchemas(empty, output),
		)
		if err != nil {
			t.Fatal(err)
		}
		admitted, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
			Source:           semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"local.send": tool}}),
			BundleSourceFact: fact,
		})
		if err != nil {
			t.Fatal(err)
		}
		return admitted
	}
	if projection(runtimecontracts.ToolSchemaString).Identity().Equal(projection(runtimecontracts.ToolSchemaBoolean).Identity()) {
		t.Fatal("authored provider-connector schema change retained effective source identity")
	}
}

func TestEffectiveSourceProjectionHasNoProductionCompositionBypass(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	allowed := map[string]map[string]bool{
		"SourceWithConnectorPackImports": {
			"internal/runtime/effective_source.go":        true,
			"internal/cliapp/provider_connector_tools.go": true, // inventory readback, not execution admission
		},
		"SourceWithProviderTriggerEvents": {"internal/runtime/effective_source.go": true},
		"WithRuntimeTools":                {"internal/runtime/effective_source.go": true},
		"WithChannelRuntimeToolProjection": {
			"internal/runtime/tools/executor.go": true,
		},
	}
	err := filepath.WalkDir(filepath.Join(repoRoot, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			paths, watched := allowed[selector.Sel.Name]
			if watched && !paths[rel] {
				t.Errorf("production effective-source composition bypass %s survives in %s", selector.Sel.Name, rel)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func digestForTriggerGeneration(t *testing.T, source semanticview.Source, fact runtimecorrelation.BundleSourceFact, generation triggergeneration.Generation) string {
	t.Helper()
	provenance := semanticview.ProviderTriggerEventProvenance{
		Provider: "telegram", Event: "inbound.telegram.text_message", Kind: "normalized",
		PackID: "provider.telegram", PackVersion: "0.1.0",
		ManifestHash: "sha256:" + strings.Repeat("1", 64), SourceProvenance: "platform", Generation: generation,
	}
	capabilities := source.SemanticCapabilities().WithProviderTriggerEvents(source, generation, nil).WithProviderTriggerEventProvenance([]semanticview.ProviderTriggerEventProvenance{provenance})
	return digestForEffectiveSourceValue(t, effectiveSourceCapabilityOverride{Source: source, capabilities: capabilities}, fact, nil)
}

func digestForConnectorGeneration(t *testing.T, source semanticview.Source, fact runtimecorrelation.BundleSourceFact, marker string) string {
	t.Helper()
	const toolID = "telegram.send_message"
	importSource, ok := source.SemanticCapabilities().ConnectorImportSource(toolID)
	if !ok {
		t.Fatalf("fixture is missing connector import source for %s", toolID)
	}
	provenance, ok := source.SemanticCapabilities().ConnectorPackProvenance(toolID)
	if !ok {
		t.Fatalf("fixture is missing connector pack provenance for %s", toolID)
	}
	generation := semanticview.MustConnectorGenerationSurface(
		"openapi/v1", "telegram.json", "sha256:"+strings.Repeat(marker, 64),
		"telegram.profile.yaml", "sha256:"+strings.Repeat("2", 64), "sha256:"+strings.Repeat("3", 64),
		"sendMessage", []semanticview.ConnectorGenerationPermission{semanticview.MustConnectorGenerationPermission("messages:write", "Send messages")},
		"telegram.send-message", "passed", "approved",
	)
	capabilities := source.SemanticCapabilities().WithConnectorPackImports(
		map[string]semanticview.ConnectorGenerationSurface{toolID: generation},
		map[string]semanticview.ConnectorImportSource{toolID: importSource},
	).WithConnectorPackProvenance(map[string]semanticview.ConnectorPackProvenance{toolID: provenance})
	return digestForEffectiveSourceValue(t, effectiveSourceCapabilityOverride{Source: source, capabilities: capabilities}, fact, nil)
}

func digestForEffectiveSourceValue(t *testing.T, source semanticview.Source, fact runtimecorrelation.BundleSourceFact, plans []packs.SatisfactionPlan) string {
	t.Helper()
	value, err := effectiveSourceIdentityValue(source, fact, plans)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := canonicaljson.Hash(value)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func effectiveSourceTestChannelPlans(t *testing.T, repoRoot string) (packs.SatisfactionPlan, packs.SatisfactionPlan) {
	t.Helper()
	var spec runtimecontracts.PlatformSpecDocument
	snapshot, err := yamlsource.LoadFile(filepath.Join(repoRoot, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Decode(&spec); err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	triggerCatalog := packfixture.TriggerCatalog(t)
	channels := packfixture.ChannelPacks(t)
	var trigger packs.TriggerPackDescriptor
	for _, candidate := range triggerCatalog.PackDescriptors() {
		if candidate.Provider == "telegram" {
			trigger = candidate
			break
		}
	}
	var connector packs.ConnectorPackDescriptor
	connectorID := channels[0].Envelope.Requires.Packs[packs.TypeConnector]
	for _, candidate := range packfixture.ConnectorRegistry(t).PackDescriptors() {
		if candidate.Identity.ID() == connectorID {
			connector = candidate
			break
		}
	}
	compile := func(channel packs.LoadedChannelPack, trigger packs.TriggerPackDescriptor) packs.SatisfactionPlan {
		plan, compileErr := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
		if compileErr != nil {
			t.Fatal(compileErr)
		}
		return plan
	}
	first := compile(channels[0], trigger)
	changedSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaMinLength(1), runtimecontracts.ToolSchemaMaxLength(18), runtimecontracts.ToolSchemaPattern(`^-?[0-9]+$`))
	changedChannel := channels[0]
	changedChannel.Manifest.OpaqueTypes["conversation_reference"] = changedSchema
	for _, eventName := range []string{"inbound.telegram.text_message", "inbound.telegram.callback_action"} {
		event := trigger.Events[eventName]
		field := event.Fields["conversation_reference"]
		field.Schema = changedSchema
		event.Fields["conversation_reference"] = field
		trigger.Events[eventName] = event
	}
	return first, compile(changedChannel, trigger)
}
