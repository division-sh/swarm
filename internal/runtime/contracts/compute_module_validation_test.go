package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestValidateWorkflowComputeModuleContractsAcceptsPinnedFixture(t *testing.T) {
	bundle, _, _ := computeModuleValidationBundle(t)
	errs := validateWorkflowComputeModuleContracts(bundle)
	if len(errs) > 0 {
		t.Fatalf("validateWorkflowComputeModuleContracts errors = %v", errs)
	}
}

func TestValidateWorkflowComputeModuleContractsRejectsDigestMismatchAndFloats(t *testing.T) {
	bundle, module, _ := computeModuleValidationBundle(t)
	module.Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	module.OutputSchema = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"score": map[string]any{"type": "number"},
		},
	}
	flow := bundle.FlowTree.Root
	flow.Policy.Modules["structured_renderer"] = module
	errs := validateWorkflowComputeModuleContracts(bundle)
	if !errorsContain(errs, "does not match module bytes") {
		t.Fatalf("errors = %v, want digest mismatch", errs)
	}
	if !errorsContain(errs, "float/number") {
		t.Fatalf("errors = %v, want float schema rejection", errs)
	}
}

func TestPolicyModuleBytesRemainBoundToAdmittedArtifact(t *testing.T) {
	bundle, module, root := computeModuleValidationBundle(t)
	want, _, err := PolicyModuleBytes(bundle, module)
	if err != nil {
		t.Fatal(err)
	}
	modulePath := filepath.Join(root, filepath.FromSlash(module.Path))
	if err := os.WriteFile(modulePath, []byte("host mutation"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _, err := PolicyModuleBytes(bundle, module)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("admitted module changed after host mutation: got %q want %q", got, want)
	}
	module.Path = "modules/missing.wasm"
	if _, err := ResolvePolicyModulePath(bundle, module); err == nil || !strings.Contains(err.Error(), "not an admitted source member") {
		t.Fatalf("unadmitted module error = %v", err)
	}
}

func TestValidatePolicySheetComputeModuleRowRequiresDeclaredInputs(t *testing.T) {
	_, module, _ := computeModuleValidationBundle(t)
	policy := PolicyDocument{Modules: map[string]PolicyModule{"structured_renderer": module}}
	spec := &ComputeModuleSpec{
		RowID:  "render_bundle",
		Module: "structured_renderer",
		Into:   "computed.rendered_bundle",
		Input: map[string]string{
			"component": "payload.component",
		},
	}
	rule := HandlerRuleEntry{
		ID:        "render_bundle",
		PolicyRow: PolicySheetRowMetadata{Kind: PolicySheetRowKindModule, Module: spec},
		Compute: &ComputeSpec{
			Operation: ComputeOpModule,
			StoreAs:   "computed.rendered_bundle",
			Module:    spec,
		},
	}
	errs := validatePolicySheetComputeModuleRow("test row", rule, policy)
	if !errorsContain(errs, `missing required module input "files"`) {
		t.Fatalf("errors = %v, want missing required input", errs)
	}
}

func computeModuleValidationBundle(t *testing.T) (*WorkflowContractBundle, PolicyModule, string) {
	t.Helper()
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("..", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	modulePath := filepath.Join(root, "modules", "structured_renderer.wasm")
	if err := os.MkdirAll(filepath.Dir(modulePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(modulePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: render\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	module := PolicyModule{
		Path:   "modules/structured_renderer.wasm",
		ABI:    "core-json-v1",
		Entry:  "compute",
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		InputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"component", "owner", "language", "files"},
			"properties": map[string]any{
				"component": map[string]any{"type": "string"},
				"owner":     map[string]any{"type": "string"},
				"language":  map[string]any{"type": "string"},
				"files": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
			},
		},
		OutputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"content", "format", "line_count"},
			"properties": map[string]any{
				"content":    map[string]any{"type": "string"},
				"format":     map[string]any{"type": "string"},
				"line_count": map[string]any{"type": "integer"},
			},
		},
		Limits: PolicyModuleLimits{
			Gas:         5_000_000,
			MemoryPages: 17,
			OutputBytes: 1024,
		},
	}
	flow := FlowContractView{
		Paths: FlowContractPaths{FlowPath: "."},
		Policy: PolicyDocument{
			Modules: map[string]PolicyModule{"structured_renderer": module},
		},
	}
	bundle := &WorkflowContractBundle{
		SourceArtifact: artifact,
		FlowSchemas:    map[string]FlowSchemaDocument{".": {}},
		FlowTree: FlowTree{
			Root: &flow,
			ByID: map[string]*FlowContractView{
				".": &flow,
			},
			ByPath: map[string]*FlowContractView{".": &flow},
		},
	}
	return bundle, module, root
}

func errorsContain(errs []error, want string) bool {
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), want) {
			return true
		}
	}
	return false
}
