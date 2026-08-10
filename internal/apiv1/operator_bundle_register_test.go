package apiv1

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestBundleRegistrationAcceptsDeclaredMockModuleDataEntry(t *testing.T) {
	repo := repoRoot(t)
	contentYAML := `
api_version: swarm.bundle.register.v1
files:
  - path: package.yaml
    text: |
      name: mocked-agent-registration
      version: "1.0.0"
      platform_version: ">=0.7.0 <0.8.0"
      flows: []
  - path: agents.yaml
    text: |
      assistant:
        id: assistant
        role: helper
        intent: {inline: "Help with the requested task."}
        model: regular
        mock:
          kind: python
          module: mocks/assistant.py
`
	mockSource := []byte("def handle(input):\n    return {\"text\": \"hello from mock\"}\n")
	projection, err := buildBundleRegistrationProjection(bundleRegistrationParams{
		ContentYAML: contentYAML,
		DataBlob: []bundleRegistrationDataEntry{
			{Path: "mocks/assistant.py", Data: mockSource},
		},
	}, bundleRegistrationRuntimeContext{
		RepoRoot:         repo,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	})
	if err != nil {
		t.Fatalf("buildBundleRegistrationProjection: %v", err)
	}
	files, ok := projection.ParsedJSON["files"].([]map[string]any)
	if !ok {
		t.Fatalf("parsed_json files = %T, want []map[string]any", projection.ParsedJSON["files"])
	}
	consumed := false
	for _, file := range files {
		if file["label"] == "bundle/mocks/assistant.py" {
			consumed = true
		}
	}
	if !consumed {
		t.Fatalf("projection files = %#v, want bundle/mocks/assistant.py consumed input", projection.ParsedJSON["files"])
	}
	if len(projection.DataBlob) == 0 {
		t.Fatal("projection.DataBlob is empty, want declared mock module data")
	}
}

func TestBundleRegistrationRejectsUndeclaredMockModuleDataEntry(t *testing.T) {
	repo := repoRoot(t)
	contentYAML := `
api_version: swarm.bundle.register.v1
files:
  - path: package.yaml
    text: |
      name: mocked-agent-registration
      version: "1.0.0"
      platform_version: ">=0.7.0 <0.8.0"
      flows: []
`
	_, err := buildBundleRegistrationProjection(bundleRegistrationParams{
		ContentYAML: contentYAML,
		DataBlob: []bundleRegistrationDataEntry{
			{Path: "mocks/undeclared.py", Data: []byte("def handle(input):\n    return {}\n")},
		},
	}, bundleRegistrationRuntimeContext{
		RepoRoot:         repo,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	})
	if err == nil {
		t.Fatal("buildBundleRegistrationProjection succeeded, want undeclared mock module rejected")
	}
	var invalid *InvalidParamsError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %T %v, want InvalidParamsError", err, err)
	}
	details, ok := invalid.Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %T, want map", invalid.Details)
	}
	reason, _ := details["reason"].(string)
	if !strings.Contains(reason, "not consumed by canonical bundle_hash owner") {
		t.Fatalf("reason = %q, want not-consumed rejection", reason)
	}
}

func TestBundleRegistrationAcceptsPolicyModuleDataEntries(t *testing.T) {
	repo := repoRoot(t)
	wasmRaw, err := os.ReadFile(filepath.Join("..", "runtime", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	pythonSource := []byte("def handle(input):\n    return {\"content\": \"\", \"format\": \"yaml\", \"line_count\": 0}\n")
	contentYAML := policyModuleRegistrationEnvelopeYAML(t, wasmRaw, pythonSource)
	projection, err := buildBundleRegistrationProjection(bundleRegistrationParams{
		ContentYAML: contentYAML,
		DataBlob: []bundleRegistrationDataEntry{
			{Path: "modules/structured_renderer.wasm", Data: wasmRaw},
			{Path: "modules/python_renderer.py", Data: pythonSource},
		},
	}, bundleRegistrationRuntimeContext{
		RepoRoot:         repo,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	})
	if err != nil {
		t.Fatalf("buildBundleRegistrationProjection: %v", err)
	}
	files, ok := projection.ParsedJSON["files"].([]map[string]any)
	if !ok {
		t.Fatalf("parsed_json files = %T, want []map[string]any", projection.ParsedJSON["files"])
	}
	consumed := map[string]bool{}
	for _, file := range files {
		consumed[file["label"].(string)] = true
	}
	for _, label := range []string{"bundle/modules/structured_renderer.wasm", "bundle/modules/python_renderer.py"} {
		if !consumed[label] {
			t.Fatalf("projection files = %#v, want consumed input %s", projection.ParsedJSON["files"], label)
		}
	}
}

func TestBundleRegistrationRejectsUndeclaredPolicyModuleDataEntry(t *testing.T) {
	repo := repoRoot(t)
	wasmRaw, err := os.ReadFile(filepath.Join("..", "runtime", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	contentYAML := policyModuleRegistrationEnvelopeYAML(t, wasmRaw, nil)
	_, err = buildBundleRegistrationProjection(bundleRegistrationParams{
		ContentYAML: contentYAML,
		DataBlob: []bundleRegistrationDataEntry{
			{Path: "modules/structured_renderer.wasm", Data: wasmRaw},
			{Path: "modules/undeclared.wasm", Data: wasmRaw},
		},
	}, bundleRegistrationRuntimeContext{
		RepoRoot:         repo,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	})
	if err == nil {
		t.Fatal("buildBundleRegistrationProjection succeeded, want undeclared module rejected")
	}
	var invalid *InvalidParamsError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %T %v, want InvalidParamsError", err, err)
	}
	details, ok := invalid.Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %T, want map", invalid.Details)
	}
	reason, _ := details["reason"].(string)
	if !strings.Contains(reason, "not consumed by canonical bundle_hash owner") {
		t.Fatalf("reason = %q, want not-consumed rejection", reason)
	}
}

func policyModuleRegistrationEnvelopeYAML(t *testing.T, wasmRaw, pythonSource []byte) string {
	t.Helper()
	wasmDigest := "sha256:" + hex.EncodeToString(sha256Sum(wasmRaw))
	policy := "modules:\n" +
		"  structured_renderer:\n" +
		"    path: modules/structured_renderer.wasm\n" +
		"    abi: core-json-v1\n" +
		"    entry: compute\n" +
		"    digest: " + wasmDigest + "\n" +
		"    limits:\n" +
		"      gas: 5000000\n" +
		"      memory_pages: 17\n" +
		"      output_bytes: 1024\n" +
		"    input_schema:\n" +
		"      type: object\n" +
		"      additionalProperties: false\n" +
		"      required: [component, owner, language, files]\n" +
		"      properties:\n" +
		"        component: {type: string}\n" +
		"        owner: {type: string}\n" +
		"        language: {type: string}\n" +
		"        files: {type: array, items: {type: string}}\n" +
		"    output_schema:\n" +
		"      type: object\n" +
		"      additionalProperties: false\n" +
		"      required: [content, format, line_count]\n" +
		"      properties:\n" +
		"        content: {type: string}\n" +
		"        format: {type: string}\n" +
		"        line_count: {type: integer}\n"
	if len(pythonSource) > 0 {
		pythonDigest := "sha256:" + hex.EncodeToString(sha256Sum(pythonSource))
		policy += "  python_renderer:\n" +
			"    path: modules/python_renderer.py\n" +
			"    kind: python\n" +
			"    abi: python-json-v1\n" +
			"    entry: handle\n" +
			"    digest: " + pythonDigest + "\n" +
			"    limits:\n" +
			"      gas: 2500000000\n" +
			"      memory_pages: 8192\n" +
			"      output_bytes: 4096\n" +
			"    input_schema:\n" +
			"      type: object\n" +
			"      additionalProperties: false\n" +
			"      required: [component, owner, language, files]\n" +
			"      properties:\n" +
			"        component: {type: string}\n" +
			"        owner: {type: string}\n" +
			"        language: {type: string}\n" +
			"        files: {type: array, items: {type: string}}\n" +
			"    output_schema:\n" +
			"      type: object\n" +
			"      additionalProperties: false\n" +
			"      required: [content, format, line_count]\n" +
			"      properties:\n" +
			"        content: {type: string}\n" +
			"        format: {type: string}\n" +
			"        line_count: {type: integer}\n"
	}
	return "api_version: swarm.bundle.register.v1\n" +
		"files:\n" +
		"  - path: package.yaml\n" +
		"    text: |\n" +
		"      name: policy-module-registration\n" +
		"      version: \"1.0.0\"\n" +
		"      platform_version: \">=0.7.0 <0.8.0\"\n" +
		"      flows:\n" +
		"        - id: render\n" +
		"          flow: render\n" +
		"  - path: flows/render/schema.yaml\n" +
		"    text: |\n" +
		"      name: render\n" +
		"      mode: static\n" +
		"      states: [ready]\n" +
		"      initial_state: ready\n" +
		"      terminal_states: [ready]\n" +
		"  - path: flows/render/policy.yaml\n" +
		"    text: |\n" + yamlBlockQuote(policy)
}

func sha256Sum(raw []byte) []byte {
	sum := sha256.Sum256(raw)
	return sum[:]
}

func yamlBlockQuote(text string) string {
	text = strings.TrimSuffix(text, "\n")
	return "      " + strings.ReplaceAll(text, "\n", "\n      ") + "\n"
}

func TestBundleRegistrationProjectionReturnsStructuredLoaderDiagnostic(t *testing.T) {
	repo := repoRoot(t)
	contentYAML := `
api_version: swarm.bundle.register.v1
files:
  - path: package.yaml
    text: |
      name: invalid-loader-shape
      version: "1.0.0"
      flows:
        - child
  - path: schema.yaml
    text: |
      name: invalid-loader-shape
`
	_, err := buildBundleRegistrationProjection(bundleRegistrationParams{
		ContentYAML: contentYAML,
	}, bundleRegistrationRuntimeContext{
		RepoRoot:         repo,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	})
	if err == nil {
		t.Fatal("buildBundleRegistrationProjection succeeded, want invalid params")
	}
	var invalid *InvalidParamsError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %T %v, want InvalidParamsError", err, err)
	}
	details, ok := invalid.Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %T, want map", invalid.Details)
	}
	if details["field"] != "content_yaml" {
		t.Fatalf("field = %v, want content_yaml", details["field"])
	}
	reason, _ := details["reason"].(string)
	if !strings.Contains(reason, "package.yaml flows entries must be mappings") {
		t.Fatalf("reason = %q, want package.yaml flows shape", reason)
	}
	diagnostic, ok := details["diagnostic"].(*runtimecontracts.LoaderDiagnostic)
	if !ok {
		t.Fatalf("diagnostic = %T, want *LoaderDiagnostic", details["diagnostic"])
	}
	if diagnostic.Code != "contract_loader.package_flows_shape" {
		t.Fatalf("diagnostic code = %q, want package flows shape", diagnostic.Code)
	}
	if strings.Contains(reason, "yaml:") || strings.Contains(reason, "ProjectFlowRef") {
		t.Fatalf("reason leaked raw loader internals: %q", reason)
	}
}
