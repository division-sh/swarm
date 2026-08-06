package apiv1

import (
	"errors"
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
