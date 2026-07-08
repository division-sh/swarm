package apiv1

import (
	"errors"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

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
