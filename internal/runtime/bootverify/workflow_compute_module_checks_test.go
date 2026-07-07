package bootverify

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCheckComputeModuleValueRowsValidatesABIAndConsumption(t *testing.T) {
	source := computeModuleCheckSource(t, true)
	findings := checkComputeModuleValueRows(&checkerContext{source: source})
	if len(findings) > 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestCheckComputeModuleValueRowsRejectsDeadBinding(t *testing.T) {
	source := computeModuleCheckSource(t, false)
	findings := checkComputeModuleValueRows(&checkerContext{source: source})
	if !computeModuleFindingContains(findings, "is not consumed") {
		t.Fatalf("findings = %#v, want dead binding", findings)
	}
}

func TestCheckComputeModuleValueRowsDispatchesPythonValidation(t *testing.T) {
	source := pythonComputeModuleCheckSource(t)
	findings := checkComputeModuleValueRows(&checkerContext{source: source})
	if len(findings) > 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func computeModuleCheckSource(t *testing.T, consumed bool) semanticview.Source {
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
	sum := sha256.Sum256(raw)
	module := runtimecontracts.PolicyModule{
		Path:   "modules/structured_renderer.wasm",
		ABI:    "core-json-v1",
		Entry:  "compute",
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		Limits: runtimecontracts.PolicyModuleLimits{
			Gas:         5_000_000,
			MemoryPages: 17,
			OutputBytes: 1024,
		},
	}
	spec := &runtimecontracts.ComputeModuleSpec{
		RowID:  "render_bundle",
		Module: "structured_renderer",
		Into:   "computed.rendered_bundle",
		Input: map[string]string{
			"component": "payload.component",
		},
		InputPaths: map[string]runtimepaths.Path{
			"component": runtimepaths.Parse("payload.component"),
		},
	}
	rules := []runtimecontracts.HandlerRuleEntry{{
		ID:        "render_bundle",
		PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindModule, Module: spec},
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpModule,
			StoreAs:   "computed.rendered_bundle",
			Module:    spec,
		},
	}}
	if consumed {
		rules = append(rules, runtimecontracts.HandlerRuleEntry{
			ID:        "rendered_yaml",
			Condition: `computed.rendered_bundle.format == "yaml"`,
			Emit:      runtimecontracts.EmitSpec{Event: "bundle.rendered"},
		})
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Paths: runtimecontracts.ContractPaths{ContractsRoot: root},
		Policy: runtimecontracts.PolicyDocument{Modules: map[string]runtimecontracts.PolicyModule{
			"structured_renderer": module,
		}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"render-node": {
				ID: "render-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"render.requested": {Rules: rules},
				},
			},
		},
	})
}

func pythonComputeModuleCheckSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	raw := []byte("def handle(input):\n    return {\"content\": input[\"component\"], \"format\": \"yaml\"}\n")
	modulePath := filepath.Join(root, "modules", "structured_renderer.py")
	if err := os.MkdirAll(filepath.Dir(modulePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(modulePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	module := runtimecontracts.PolicyModule{
		Path:   "modules/structured_renderer.py",
		Kind:   pythonmodule.Kind,
		ABI:    pythonmodule.ABI,
		Entry:  pythonmodule.DefaultEntry,
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		Limits: runtimecontracts.PolicyModuleLimits{
			Gas:         2_000_000_000,
			MemoryPages: 8192,
			OutputBytes: 1024,
		},
	}
	spec := &runtimecontracts.ComputeModuleSpec{
		RowID:  "render_bundle",
		Module: "structured_renderer",
		Into:   "computed.rendered_bundle",
		Input: map[string]string{
			"component": "payload.component",
		},
		InputPaths: map[string]runtimepaths.Path{
			"component": runtimepaths.Parse("payload.component"),
		},
	}
	rules := []runtimecontracts.HandlerRuleEntry{
		{
			ID:        "render_bundle",
			PolicyRow: runtimecontracts.PolicySheetRowMetadata{Kind: runtimecontracts.PolicySheetRowKindModule, Module: spec},
			Compute: &runtimecontracts.ComputeSpec{
				Operation: runtimecontracts.ComputeOpModule,
				StoreAs:   "computed.rendered_bundle",
				Module:    spec,
			},
		},
		{
			ID:        "rendered_yaml",
			Condition: `computed.rendered_bundle.format == "yaml"`,
			Emit:      runtimecontracts.EmitSpec{Event: "bundle.rendered"},
		},
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Paths: runtimecontracts.ContractPaths{ContractsRoot: root},
		Policy: runtimecontracts.PolicyDocument{Modules: map[string]runtimecontracts.PolicyModule{
			"structured_renderer": module,
		}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"render-node": {
				ID: "render-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"render.requested": {Rules: rules},
				},
			},
		},
	})
}

func computeModuleFindingContains(findings []Finding, want string) bool {
	for _, finding := range findings {
		if finding.CheckID == computeModuleCheckID && strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}
