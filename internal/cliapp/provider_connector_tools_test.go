package cliapp

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/packs"
)

func TestProviderConnectorSurfaceMessageIncludesGeneratedReviewEvidence(t *testing.T) {
	guarantee, err := packs.NewGuarantee(packs.GuaranteeActivityJournal)
	if err != nil {
		t.Fatal(err)
	}
	message := packs.RenderSubject(packs.Subject{
		ID: "github.create_issue", Kind: packs.SubjectProviderConnector, Provider: "github", Action: "create_issue",
		Source: "connector_pack_import", Applicability: "effective", Status: packs.StatusReady,
		Capabilities: []packs.Capability{{Code: packs.CapabilityCallProviderAction, Target: "create GitHub issues"}},
		Guarantees:   []packs.Guarantee{guarantee},
		Evidence: []packs.Evidence{{Kind: "generation", Fields: map[string]string{
			"generator": "swarm-openapi-gen/v1", "source": "catalog/sources/github.json.gz", "source_hash": "sha256:source",
			"profile": "catalog/generator-profiles/github.yaml", "profile_hash": "sha256:profile", "manifest_hash": "sha256:manifest",
			"operation": "issues/create", "permissions": "issues:write:GitHub App Issues permission at write level",
			"fixture": "github/issues-create:passing", "review": "approved",
		}}},
	}, true)
	for _, want := range []string{
		"operation=issues/create",
		"permissions=issues:write:GitHub App Issues permission at write level",
		"source_hash=sha256:source",
		"profile_hash=sha256:profile",
		"manifest_hash=sha256:manifest",
		"fixture=github/issues-create:passing",
		"review=approved",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("message = %q, want %q", message, want)
		}
	}
}

func TestProviderCapabilitySurfaceRetiresDuplicateOwnersAndTargetBindingGuess(t *testing.T) {
	files := map[string][]string{
		"internal/packs/envelope.go": {
			"func (e Envelope) Surface", "type CapabilitySurface", "type RequirementStatus",
		},
		"internal/packs/capability_surface.go": {
			"var capabilityPhrases", "func humanSubjectKind", "func humanSubjectStatus", "phrase string\n\tenforcedBy",
		},
		"internal/providerconnectors/providerconnectors.go": {
			"type Surface struct", "SurfacesWithOptions", "type RequirementStatus", "ResolveInboundTarget", "SQLiteRuntimeStore", "PostgresRuntimeStore",
		},
		"internal/providertriggers/providertriggers.go": {
			"CapabilitySurface(", "ResolveInboundTarget", "SQLiteRuntimeStore", "PostgresRuntimeStore",
		},
		"internal/cliapp/provider_connector_tools.go": {
			"providerConnectorSurfaceMessage", "formatProviderConnectorSurfaceVerbs", "formatProviderConnectorRequirements",
			"SourceWithConnectorPackImports",
		},
		"internal/cliapp/provider_trigger_packs.go": {
			"providerTriggerPackSurfaceMessage", "formatProviderTriggerPackRequirements", "ResolveInboundTarget",
		},
		"internal/cliapp/doctor.go":     {"appendProviderTriggerPackSurfaceFindings"},
		"internal/cliapp/cli_output.go": {"map[cliHumanCodeFamily]map[string]string{"},
	}
	for name, forbidden := range files {
		body, err := os.ReadFile(filepath.Join(RepoRoot(), name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(body), token) {
				t.Fatalf("%s retains forbidden provider capability owner or target-binding seam %q", name, token)
			}
		}
	}
	capabilityBody, err := os.ReadFile(filepath.Join(RepoRoot(), "internal/packs/capability_surface.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(capabilityBody), "userfacing.ProjectHumanCode") < 5 {
		t.Fatal("provider capability rendering does not route every kind/status/capability/guarantee/requirement phrase family through the shared human-code owner")
	}
}

func TestProviderGuaranteeRegistryMatchesSpecAndNamesLiveProofs(t *testing.T) {
	var spec struct {
		ToolModel struct {
			ProviderCapabilitySurface struct {
				Guarantees struct {
					Claims map[string]struct {
						EnforcedBy     string `yaml:"enforced_by"`
						ExecutionProof string `yaml:"execution_proof"`
					} `yaml:"claims"`
				} `yaml:"guarantee_enforcement_registry"`
			} `yaml:"provider_capability_surface"`
		} `yaml:"tool_model"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), "platform-spec.yaml"), &spec)
	owners := map[string]string{}
	for code, row := range spec.ToolModel.ProviderCapabilitySurface.Guarantees.Claims {
		owners[code] = strings.TrimSpace(row.EnforcedBy)
		proof := strings.Fields(strings.TrimSpace(row.ExecutionProof))
		if len(proof) != 2 {
			t.Fatalf("guarantee %s execution_proof = %q, want '<package> <test>'", code, row.ExecutionProof)
		}
		if strings.Contains(proof[1], "/") {
			t.Fatalf("guarantee %s execution_proof = %q, want a dedicated top-level Go test", code, row.ExecutionProof)
		}
		assertGoTestFunctionExists(t, proof[0], proof[1])
	}
	if got := packs.GuaranteeEnforcementOwners(); !reflect.DeepEqual(got, owners) {
		t.Fatalf("guarantee registry differs from authoritative platform spec:\ngot:  %#v\nwant: %#v", got, owners)
	}
	ownerFiles := map[string]string{
		"internal/providertriggers.InboundAdmissionPlan.Accept":                                   "internal/providertriggers/admission.go",
		"internal/runtime.InboundGateway.HandleResolvedWebhook":                                   "internal/runtime/inbound.go",
		"internal/runtime/pipeline.pipelineActivityDispatcher.executeNonIdempotentActivityIntent": "internal/runtime/pipeline/activity_engine.go",
		"internal/runtime/pipeline.executePreparedActivityHTTPTool":                               "internal/runtime/pipeline/activity_engine.go",
	}
	for _, owner := range owners {
		path, ok := ownerFiles[owner]
		if !ok {
			t.Fatalf("guarantee enforcement owner %q has no executable-owner liveness classification", owner)
		}
		body, err := os.ReadFile(filepath.Join(RepoRoot(), path))
		if err != nil {
			t.Fatal(err)
		}
		symbol := owner[strings.LastIndex(owner, ".")+1:]
		if !strings.Contains(string(body), symbol) {
			t.Fatalf("guarantee enforcement owner %q is not live in %s", owner, path)
		}
	}
}

func assertGoTestFunctionExists(t *testing.T, packagePath, testName string) {
	t.Helper()
	found, err := goTestFunctionExists(RepoRoot(), packagePath, testName)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("execution proof %s %s does not name a live Go test", packagePath, testName)
	}
}

func goTestFunctionExists(repoRoot, packagePath, testName string) (bool, error) {
	root := filepath.Join(repoRoot, filepath.FromSlash(packagePath))
	found := false
	err := checkoutsource.WalkDir(repoRoot, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			return err
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(body), "func "+testName+"(") {
			found = true
		}
		return nil
	})
	return found, err
}

func TestProviderGuaranteeProofCensusRejectsForeignOnlyDeclaration(t *testing.T) {
	root := t.TempDir()
	proofPackage := filepath.Join(root, "internal", "proof")
	foreign := filepath.Join(proofPackage, "review-nested")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(foreign, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	proof := []byte("package proof\nimport \"testing\"\nfunc TestProviderBoundaryProof(t *testing.T) {}\n")
	if err := os.WriteFile(filepath.Join(foreign, "foreign_test.go"), proof, 0o644); err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		found, err := goTestFunctionExists(root, "internal/proof", "TestProviderBoundaryProof")
		if err != nil || found != want {
			t.Fatalf("goTestFunctionExists = %v, %v; want %v", found, err, want)
		}
	}
	check(false)
	current := filepath.Join(proofPackage, "current_test.go")
	if err := os.WriteFile(current, proof, 0o644); err != nil {
		t.Fatal(err)
	}
	check(true)
	if err := os.Remove(current); err != nil {
		t.Fatal(err)
	}
	check(false)
}
