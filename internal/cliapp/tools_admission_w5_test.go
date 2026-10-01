package cliapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestW5ToolsVerifyAndDescribeSupportedSurface(t *testing.T) {
	root := templateflowpilot.Write(t, templateflowpilot.Options{})
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "internal/runtime/computemodule/testdata/structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "modules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "modules/renderer.wasm"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("renderer:\n  handler_type: wasm\n  path: modules/renderer.wasm\n  abi: core-json-v1\n  entry: compute\n  digest: sha256:%x\n  input_schema: {type: object, properties: {value: {type: integer}}}\n  output_schema: {type: object, properties: {value: {type: integer}}}\n  limits: {gas: 5000000, memory_pages: 17, output_bytes: 1024}\n", sha256.Sum256(raw))
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), module+"explicit_any: {handler_type: http, http: {method: POST, url: 'https://example.invalid'}, input_schema: {}}\nomitted: {handler_type: http, http: {method: POST, url: 'https://example.invalid'}}\n")
	var stdout, stderr bytes.Buffer
	config := writeTestVerifyRuntimeConfig(t)
	for _, command := range [][]string{{"verify", root, "--config", config, "--json"}, {"describe", root, "--json"}} {
		stdout.Reset()
		stderr.Reset()
		if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), command, &stdout, &stderr, defaultRootCommandOptions()); code != 0 {
			t.Fatalf("%s=%d stdout=%s stderr=%s", command[0], code, &stdout, &stderr)
		}
		if command[0] == "describe" && (!strings.Contains(stdout.String(), "tools[") || !strings.Contains(stdout.String(), "input_schema")) {
			t.Fatalf("supported readback lost tools provenance: %s", &stdout)
		}
	}
	for _, fields := range []string{"input_schema: {type: string, minLength: 1.5}", "<<: {parameters: null}", "handler_type: wasm", "input_schema: {type: object}"} {
		t.Run(fields, func(t *testing.T) {
			writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "invalid: {"+fields+"}\n")
			stdout.Reset()
			stderr.Reset()
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", config, "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if code == 0 || !strings.Contains(stdout.String()+stderr.String(), "tools.yaml") {
				t.Fatalf("invalid source accepted/lost: code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
		})
	}
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), module)
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: business intent}\n  tools: [renderer]\n")
	stdout.Reset()
	stderr.Reset()
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", config, "--json"}, &stdout, &stderr, defaultRootCommandOptions()); code == 0 || !strings.Contains(stdout.String()+stderr.String(), "module") {
		t.Fatalf("module grant accepted: code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
	}
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: business intent}\n")
	writeDescribeTestFile(t, filepath.Join(root, "policy.yaml"), "modules: {}\n")
	stdout.Reset()
	stderr.Reset()
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", config, "--json"}, &stdout, &stderr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("literal modules key rejected: code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
	}
	stdout.Reset()
	stderr.Reset()
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root, "--json"}, &stdout, &stderr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("literal modules readback=%d: %s", code, &stderr)
	}
	var view struct {
		Root struct {
			Policy map[string]any `json:"policy"`
		} `json:"root"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if literal, ok := view.Root.Policy["modules"].(map[string]any); !ok || len(literal) != 0 {
		t.Fatalf("module-looking policy did not remain an empty literal object: %#v", view.Root.Policy)
	}
}
