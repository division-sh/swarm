package computemodule

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/bytecodealliance/wasmtime-go/v46"
)

func TestExecuteStructuredRenderer(t *testing.T) {
	wasm := readFixture(t)
	if err := ValidateCoreJSONModule(wasm, DefaultEntry, 17); err != nil {
		t.Fatalf("ValidateCoreJSONModule: %v", err)
	}
	input := map[string]any{
		"component": "api",
		"owner":     "platform",
		"language":  "go",
		"files":     []any{"main.go", "README.md", "service.yaml"},
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(Request{
		ModuleID:    "structured_renderer",
		RowID:       "render_bundle",
		Digest:      digestFixture(wasm),
		Wasm:        wasm,
		Input:       raw,
		Fuel:        5_000_000,
		MemoryPages: 17,
		OutputBytes: 1024,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.FuelConsumed == 0 {
		t.Fatalf("FuelConsumed = 0, want deterministic fuel trace")
	}
	if !strings.Contains(result.Engine, versionModule+":") {
		t.Fatalf("Engine = %q, want wasmtime version", result.Engine)
	}
	var output map[string]any
	if err := json.Unmarshal(result.Output, &output); err != nil {
		t.Fatalf("output JSON: %v", err)
	}
	content, _ := output["content"].(string)
	for _, want := range []string{"component: api", "owner: platform", "language: go", "- src/main.go", "- README.md", "- deploy/service.yaml"} {
		if !strings.Contains(content, want) {
			t.Fatalf("content missing %q: %s", want, content)
		}
	}
}

func TestExecuteClassifiesFuelAndOutputCapsAsDeterministic(t *testing.T) {
	wasm := readFixture(t)
	raw := []byte(`{"component":"api","owner":"platform","language":"go","files":["main.go"]}`)
	tests := []struct {
		name string
		req  Request
		code Code
	}{
		{
			name: "fuel",
			req: Request{
				ModuleID:    "structured_renderer",
				RowID:       "render_bundle",
				Digest:      digestFixture(wasm),
				Wasm:        wasm,
				Input:       raw,
				Fuel:        1,
				MemoryPages: 17,
				OutputBytes: 1024,
			},
			code: CodeFuel,
		},
		{
			name: "output_cap",
			req: Request{
				ModuleID:    "structured_renderer",
				RowID:       "render_bundle",
				Digest:      digestFixture(wasm),
				Wasm:        wasm,
				Input:       raw,
				Fuel:        5_000_000,
				MemoryPages: 17,
				OutputBytes: 8,
			},
			code: CodeOutputSize,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Execute(tc.req)
			if err == nil {
				t.Fatalf("Execute error = nil, want %s", tc.code)
			}
			var typed *Error
			if !errors.As(err, &typed) || typed.Code != tc.code {
				t.Fatalf("error = %#v, want code %s", err, tc.code)
			}
		})
	}
}

func TestExecuteClassifiesAllocatorTrapAsTrap(t *testing.T) {
	wasm, err := wasmtime.Wat2Wasm(`(module
  (memory (export "memory") 1 1)
  (func (export "alloc") (param i32) (result i32)
    unreachable)
  (func (export "compute") (param i32 i32) (result i64)
    i64.const 0)
)`)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}
	_, err = Execute(Request{
		ModuleID:    "trap_alloc",
		RowID:       "render_bundle",
		Digest:      digestFixture(wasm),
		Wasm:        wasm,
		Input:       []byte(`{}`),
		Fuel:        10_000,
		MemoryPages: 1,
		OutputBytes: 64,
	})
	if err == nil {
		t.Fatal("Execute error = nil, want allocator trap")
	}
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != CodeTrap {
		t.Fatalf("error = %#v, want code %s", err, CodeTrap)
	}
}

func TestExecuteRejectsDigestMismatchBeforeExecution(t *testing.T) {
	wasm := readFixture(t)
	raw := []byte(`{"component":"api","owner":"platform","language":"go","files":["main.go"]}`)
	_, err := Execute(Request{
		ModuleID:    "structured_renderer",
		RowID:       "render_bundle",
		Digest:      "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		Wasm:        wasm,
		Input:       raw,
		Fuel:        5_000_000,
		MemoryPages: 17,
		OutputBytes: 1024,
	})
	if err == nil {
		t.Fatal("Execute error = nil, want digest mismatch")
	}
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != CodeDigest {
		t.Fatalf("error = %#v, want code %s", err, CodeDigest)
	}
}

func TestValidateCoreJSONModuleRequiresEngineEnforcedMemoryMaximum(t *testing.T) {
	tests := []struct {
		name        string
		memory      string
		limit       uint32
		wantMessage string
	}{
		{
			name:        "no_maximum",
			memory:      `(memory (export "memory") 1)`,
			limit:       1,
			wantMessage: "memory must declare a maximum",
		},
		{
			name:        "maximum_exceeds_contract_limit",
			memory:      `(memory (export "memory") 1 2)`,
			limit:       1,
			wantMessage: "maximum 2 pages exceeds declared limit 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wasm, err := wasmtime.Wat2Wasm(`(module
  ` + tc.memory + `
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "compute") (param i32 i32) (result i64) i64.const 0)
)`)
			if err != nil {
				t.Fatalf("Wat2Wasm: %v", err)
			}
			err = ValidateCoreJSONModule(wasm, DefaultEntry, tc.limit)
			if err == nil {
				t.Fatalf("ValidateCoreJSONModule error = nil, want %q", tc.wantMessage)
			}
			var typed *Error
			if !errors.As(err, &typed) || typed.Code != CodeMemory {
				t.Fatalf("error = %#v, want code %s", err, CodeMemory)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("error = %v, want %q", err, tc.wantMessage)
			}
		})
	}
}

func readFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/structured_renderer.wasm")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func digestFixture(wasm []byte) string {
	sum := sha256.Sum256(wasm)
	return "sha256:" + hex.EncodeToString(sum[:])
}
