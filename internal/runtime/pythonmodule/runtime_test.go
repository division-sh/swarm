package pythonmodule

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
)

func TestExecuteStructuredTransform(t *testing.T) {
	source := []byte(`def handle(input):
    lines = [
        "component: " + input["component"],
        "owner: " + input["owner"],
    ]
    for name in input["files"]:
        if name.endswith(".yaml"):
            lines.append("- deploy/" + name)
        elif name.endswith(".go"):
            lines.append("- src/" + name)
        else:
            lines.append("- " + name)
    return {"content": "\n".join(lines), "format": "yaml", "line_count": len(lines)}
`)
	input := map[string]any{
		"component": "api",
		"owner":     "platform",
		"files":     []any{"main.go", "README.md", "service.yaml"},
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(Request{
		ModuleID:    "python_renderer",
		RowID:       "render_bundle",
		Digest:      digestSource(source),
		Entry:       DefaultEntry,
		Source:      source,
		Input:       raw,
		Fuel:        2_500_000_000,
		MemoryPages: 8192,
		OutputBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Interpreter != Interpreter || result.InterpreterSHA != InterpreterDigest || result.SnapshotHash == "" || result.HarnessABI != HarnessABI || result.SourceHash != digestSource(source) {
		t.Fatalf("runtime identity = %#v", result)
	}
	if result.FuelConsumed == 0 || result.OutputHash == "" || !strings.Contains(result.Engine, "wasmtime-go") {
		t.Fatalf("trace evidence missing: %#v", result)
	}
	var output map[string]any
	if err := json.Unmarshal(result.Output, &output); err != nil {
		t.Fatalf("output JSON: %v", err)
	}
	content, _ := output["content"].(string)
	for _, want := range []string{"component: api", "owner: platform", "- src/main.go", "- deploy/service.yaml"} {
		if !strings.Contains(content, want) {
			t.Fatalf("content missing %q: %s", want, content)
		}
	}
}

func TestValidateSourceRejectsDeniedCapability(t *testing.T) {
	source := []byte("import os\n\ndef handle(input):\n    return {\"cwd\": os.getcwd()}\n")
	err := ValidateSource(Request{
		ModuleID:    "bad",
		RowID:       "policy.modules.bad",
		Digest:      digestSource(source),
		Entry:       DefaultEntry,
		Source:      source,
		Fuel:        2_000_000_000,
		MemoryPages: 8192,
		OutputBytes: 1024,
	})
	assertComputeModuleCode(t, err, computemodule.CodeDeniedCapability)
}

func TestValidateSourceRejectsBuiltinEscape(t *testing.T) {
	source := []byte(`def handle(input):
    os = __builtins__["__import__"].__globals__["builtins"].__import__("os")
    return {"cwd": os.getcwd()}
`)
	err := ValidateSource(Request{
		ModuleID:    "bad",
		RowID:       "policy.modules.bad",
		Digest:      digestSource(source),
		Entry:       DefaultEntry,
		Source:      source,
		Fuel:        2_000_000_000,
		MemoryPages: 8192,
		OutputBytes: 1024,
	})
	assertComputeModuleCode(t, err, computemodule.CodeDeniedCapability)
}

func TestExtractArtifactIgnoresPredictableStaleCache(t *testing.T) {
	digestHex := strings.TrimPrefix(InterpreterDigest, "sha256:")
	predictable := filepath.Join(os.TempDir(), "swarm-"+Interpreter+"-"+digestHex[:16])
	if err := os.MkdirAll(predictable, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(predictable) })
	if err := os.WriteFile(filepath.Join(predictable, pythonWasmPath), []byte("poisoned"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir, err := extractArtifact()
	if err != nil {
		t.Fatalf("extractArtifact: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if dir == predictable {
		t.Fatalf("extractArtifact reused predictable stale cache %s", dir)
	}
	raw, err := os.ReadFile(filepath.Join(dir, pythonWasmPath))
	if err != nil {
		t.Fatalf("read extracted python.wasm: %v", err)
	}
	if string(raw) == "poisoned" {
		t.Fatalf("extractArtifact returned stale poisoned python.wasm")
	}
}

func TestExecuteClassifiesExceptionOutputCapAndFuel(t *testing.T) {
	tests := []struct {
		name   string
		source []byte
		fuel   uint64
		output int
		code   computemodule.Code
	}{
		{
			name:   "exception",
			source: []byte("def handle(input):\n    raise ValueError(\"bad payload\")\n"),
			fuel:   2_000_000_000,
			output: 1024,
			code:   computemodule.CodePythonException,
		},
		{
			name:   "output_cap",
			source: []byte("def handle(input):\n    return {\"content\": \"x\" * 200}\n"),
			fuel:   2_000_000_000,
			output: 16,
			code:   computemodule.CodeOutputSize,
		},
		{
			name:   "fuel",
			source: []byte("def handle(input):\n    while True:\n        pass\n"),
			fuel:   10,
			output: 1024,
			code:   computemodule.CodeFuel,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Execute(Request{
				ModuleID:    "bad",
				RowID:       tc.name,
				Digest:      digestSource(tc.source),
				Entry:       DefaultEntry,
				Source:      tc.source,
				Input:       []byte(`{}`),
				Fuel:        tc.fuel,
				MemoryPages: 8192,
				OutputBytes: tc.output,
			})
			assertComputeModuleCode(t, err, tc.code)
		})
	}
}

func assertComputeModuleCode(t *testing.T, err error, code computemodule.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", code)
	}
	if !computemodule.IsDeterministicFailure(err) {
		t.Fatalf("IsDeterministicFailure(%v) = false", err)
	}
	var typed *computemodule.Error
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("error = %#v, want code %s", err, code)
	}
}

func digestSource(source []byte) string {
	sum := sha256.Sum256(source)
	return "sha256:" + hex.EncodeToString(sum[:])
}
