package runtime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

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
	persisted, _ := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	ephemeral, _ := runtimecorrelation.NewSourceArtifactFact(bundleHash)

	first, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: persisted})
	if err != nil {
		t.Fatal(err)
	}
	second, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: persisted})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Identity().Equal(second.Identity()) {
		t.Fatalf("same effective source produced different identities: %s != %s", first.Identity().Digest(), second.Identity().Digest())
	}
	other, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: ephemeral})
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity().Digest() != other.Identity().Digest() {
		t.Fatal("the same admitted source fact produced different effective digests")
	}
	if !strings.HasPrefix(first.Identity().Digest(), "sha256:") {
		t.Fatalf("digest = %q", first.Identity().Digest())
	}
}

func TestProjectedConnectorPackSourceAdmitsOneMockResponseAcrossEveryScope(t *testing.T) {
	bundle, fact := effectiveSourceConnectorFixture(t)
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := providerconnectors.CompileMockResponsePlan(projection.Source())
	if err != nil {
		t.Fatal(err)
	}
	const toolID = "telegram.send_message"
	entries := []runtimecontracts.ToolSchemaEntry{projection.Source().ToolEntries()[toolID]}
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

func effectiveSourceConnectorFixture(t *testing.T) (*runtimecontracts.WorkflowContractBundle, runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	provider := packfixture.NeutralProviderMembers()
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
		"schema.yaml":             []byte("stages: []\n"),
		"child/schema.yaml":       []byte("stages: []\n"),
		"telegram/manifest.yaml":  s1EffectiveProviderScopeMembers()["team-bot/manifest.yaml"],
		"telegram/connector.yaml": provider["connector.yaml"],
	})
	return bundle, sourceartifactfixture.FactFor(rooted.Artifact())
}

func TestEffectiveSourceIdentityChangesWithAuthoredProviderConnectorSchema(t *testing.T) {
	projection := func(kind string) (AdmittedEffectiveSourceProjection, string) {
		bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
			"schema.yaml":         []byte("stages: []\n"),
			"local/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send]}, cannot: [bypass journal]}\n"),
			"local/connector.yaml": []byte(fmt.Sprintf(`tools:
  send:
    category: provider_connector
    handler_type: http
    effect_class: non_idempotent_write
    credentials: [token]
    output_schema:
      type: object
      required: [value]
      properties: {value: {type: %s}}
    http:
      method: POST
      url: 'https://example.invalid/send'
      headers: {Authorization: 'Bearer {{credentials.token}}'}
    response_success: {kind: http_status_2xx}
`, kind)),
		})
		fact := sourceartifactfixture.FactFor(rooted.Artifact())
		admitted, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
			Source:             semanticview.Wrap(bundle),
			SourceArtifactFact: fact,
		})
		if err != nil {
			t.Fatal(err)
		}
		value, err := effectiveSourceIdentityValue(admitted.Source(), fact, nil)
		if err != nil {
			t.Fatal(err)
		}
		connectors, err := canonicaljson.Hash(value["provider_connectors"])
		if err != nil {
			t.Fatal(err)
		}
		return admitted, connectors
	}
	text, textConnectors := projection("string")
	boolean, booleanConnectors := projection("boolean")
	if text.Identity().Equal(boolean.Identity()) {
		t.Fatal("authored provider-connector schema change retained effective source identity")
	}
	if textConnectors == booleanConnectors {
		t.Fatal("authored schema changed only base identity, not scoped connector evidence")
	}
}

func TestEffectiveSourceProjectionHasNoProductionCompositionBypass(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	allowed := map[string]map[string]bool{
		"SourceWithConnectorPackImports": {
			"internal/cliapp/provider_connector_tools.go": true, // inventory readback, not execution admission
		},
		"SourceWithProviderTriggerEvents": {},
		"WithRuntimeTools":                {"internal/runtime/effective_source.go": true},
		"WithChannelRuntimeToolProjection": {
			"internal/runtime/tools/executor.go": true,
		},
	}
	err := checkoutsource.WalkDir(repoRoot, filepath.Join(repoRoot, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
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

// Change generation without widening learned values beyond the destination.
