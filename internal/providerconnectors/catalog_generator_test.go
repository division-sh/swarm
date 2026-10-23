package providerconnectors

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	platformpacks "github.com/division-sh/swarm/packs"
	"gopkg.in/yaml.v3"
)

func TestGeneratedCatalogIsDeterministicCurrentAndProviderGeneric(t *testing.T) {
	root := os.DirFS(".")
	first, err := GenerateCatalog(root)
	if err != nil {
		t.Fatalf("GenerateCatalog first: %v", err)
	}
	second, err := GenerateCatalog(root)
	if err != nil {
		t.Fatalf("GenerateCatalog second: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("GenerateCatalog output is not deterministic")
	}
	checked, err := CheckGeneratedCatalog(root, platformpacks.FS())
	if err != nil {
		t.Fatalf("CheckGeneratedCatalog: %v", err)
	}
	if len(checked) != 2 {
		t.Fatalf("generated artifacts = %d, want GitHub plus synthetic Acme", len(checked))
	}

	artifacts := generatedArtifactsByProvider(checked)
	github := artifacts["github"]
	if github.Manifest.Provider != "github" || len(github.Manifest.Tools) != 3 {
		t.Fatalf("GitHub artifact = %#v, want three selected operations", github.Manifest)
	}
	for _, toolID := range []string{"github.add_labels_to_issue", "github.create_issue", "github.create_issue_comment"} {
		tool, ok := github.Manifest.Tools[toolID]
		if !ok {
			t.Fatalf("generated GitHub tool %q missing", toolID)
		}
		credential, ok := tool.ManagedCredential()
		if !ok || credential.Key != "github_app" || credential.InstallationIDInput != "installation_id" {
			t.Fatalf("generated GitHub tool %q credential = %#v", toolID, credential)
		}
		success, ok := tool.ResponseSuccess()
		if !ok || success.Kind != "http_status_2xx" {
			t.Fatalf("generated GitHub tool %q response_success = %#v", toolID, success)
		}
	}
	addLabels := github.Manifest.Tools["github.add_labels_to_issue"]
	if err := addLabels.OutputSchema().Validate([]any{map[string]any{"name": "bug"}}); err != nil {
		t.Fatalf("generated GitHub add-labels output rejects provider array: %v", err)
	}
	if err := addLabels.OutputSchema().Validate(map[string]any{"name": "bug"}); err == nil {
		t.Fatal("generated GitHub add-labels output accepts the stale object shape")
	}

	acme := artifacts["acme"]
	tool, ok := acme.Manifest.Tools["acme.create_widget"]
	httpSpec, hasHTTP := tool.HTTP()
	if !ok || !hasHTTP {
		t.Fatalf("synthetic generated tool = %#v, want acme.create_widget HTTP tool", tool)
	}
	if httpSpec.Method != "POST" || httpSpec.URL != "https://api.acme.test/accounts/{{input.account_id}}/widgets" {
		t.Fatalf("synthetic generated HTTP shape = %#v", httpSpec)
	}
	if got := httpSpec.Headers["Authorization"]; got != "Bearer {{credentials.acme_api_key}}" {
		t.Fatalf("synthetic generated Authorization = %q", got)
	}
	credentials := tool.Credentials()
	if len(credentials) != 1 || credentials[0] != "acme_api_key" {
		t.Fatalf("synthetic generated credentials = %#v", credentials)
	}
	if _, exists := testBuiltinTool(t, "acme", "acme.create_widget"); exists {
		t.Fatal("synthetic conformance connector became an ambient builtin tool")
	}
}

func TestGeneratedCatalogRejectsProfileOutputShapeDriftFromOpenAPI(t *testing.T) {
	files := catalogWorkingTreeFS(t)
	var profile GeneratorProfile
	mustYAMLUnmarshal(t, files["catalog/generator-profiles/github.yaml"].Data, &profile)
	profile.Operations[0].Output = GeneratorOutput{Type: "object"}
	files["catalog/generator-profiles/github.yaml"].Data = mustYAMLMarshal(t, profile)

	if _, err := GenerateCatalog(files); err == nil || !strings.Contains(err.Error(), `response type "array" does not match profile type "object"`) {
		t.Fatalf("GenerateCatalog output-shape drift error = %v", err)
	}
}

func TestGeneratedConnectorToolsUseClosedAdmissionAndExecutionContract(t *testing.T) {
	artifacts, err := GenerateCatalog(os.DirFS("."))
	if err != nil {
		t.Fatalf("GenerateCatalog: %v", err)
	}
	for _, artifact := range artifacts {
		for toolID, tool := range artifact.Manifest.Tools {
			before, err := tool.CanonicalHash()
			if err != nil {
				t.Fatalf("%s.%s admitted hash: %v", artifact.Manifest.Provider, toolID, err)
			}
			httpSpec, ok := tool.HTTP()
			if !ok {
				t.Fatalf("%s.%s has no admitted HTTP contract", artifact.Manifest.Provider, toolID)
			}
			httpSpec.URL = "https://mutated.invalid"
			httpSpec.Headers["X-Mutated"] = "true"
			after, err := tool.CanonicalHash()
			if err != nil {
				t.Fatalf("%s.%s hash after readback mutation: %v", artifact.Manifest.Provider, toolID, err)
			}
			if after != before {
				t.Fatalf("%s.%s retained caller-owned generated-tool mutation", artifact.Manifest.Provider, toolID)
			}
		}
	}
}

func TestGeneratorProfileAndSelectedOpenAPISubsetFailClosed(t *testing.T) {
	profileBody, err := os.ReadFile("catalog/generator-profiles/acme.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseGeneratorProfile(append(profileBody, []byte("unknown_field: true\n")...)); err == nil || !strings.Contains(err.Error(), "field unknown_field not found") {
		t.Fatalf("unknown profile field error = %v", err)
	}
	if _, err := ParseGeneratorProfile(append(profileBody, []byte("---\nschema_version: \"1\"\n")...)); err == nil || !strings.Contains(err.Error(), "multiple documents") {
		t.Fatalf("multiple profile documents error = %v", err)
	}
	outsideSource := bytes.Replace(profileBody, []byte("catalog/sources/acme-openapi.json"), []byte("../acme-openapi.json"), 1)
	if _, err := ParseGeneratorProfile(outsideSource); err == nil || !strings.Contains(err.Error(), "outside catalog/sources") {
		t.Fatalf("outside source path error = %v", err)
	}
	unstableSlug := bytes.Replace(profileBody, []byte("acme.create_widget"), []byte("acme.Create-Widget"), 1)
	if _, err := ParseGeneratorProfile(unstableSlug); err == nil || !strings.Contains(err.Error(), "stable acme.<action> form") {
		t.Fatalf("unstable tool slug error = %v", err)
	}

	one := jsonSchema(t, map[string]any{
		"type":       "object",
		"properties": map[string]any{"name": map[string]any{"type": "string"}},
	})
	root := jsonSchema(t, openAPISchema{OneOf: []json.RawMessage{one, one}})
	request := openAPIRequestBody{
		Required: true,
		Content: map[string]openAPIMediaType{
			"application/json": {Schema: root},
		},
	}
	_, err = selectRequestBodySchema(request, GeneratorRequestBody{
		Required: true,
		Variant:  GeneratorBodyVariant{Kind: "object", Fields: []string{"name"}},
	})
	if err == nil || !strings.Contains(err.Error(), "matched 2 schemas") {
		t.Fatalf("ambiguous body variant error = %v", err)
	}

	document := openAPIDocument{Parameters: map[string]json.RawMessage{"account-id": jsonSchema(t, map[string]any{
		"name": "account_id", "in": "path", "required": true, "schema": map[string]any{"type": "string"},
	})}}
	operation := openAPIOperation{Parameters: []openAPIParameterReference{{Ref: "https://schemas.example.test/account-id.json"}}}
	if _, err := resolveSelectedParameter(document, operation, "account-id"); err == nil || !strings.Contains(err.Error(), "selected local parameter ref") {
		t.Fatalf("external parameter ref error = %v", err)
	}

	files := catalogWorkingTreeFS(t)
	replaceMapFile(t, files, "catalog/generator-profiles/acme.yaml", "182ed500197c682bca81a5a72cb1399e53e18dd87e9cc890fd89df1987b6a38d", "\""+strings.Repeat("0", 64)+"\"")
	if _, err := GenerateCatalog(files); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("source hash mismatch error = %v", err)
	}
}

func TestGeneratedConformanceRejectsStaleOrIncompleteEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, files fstest.MapFS)
		want   string
	}{
		{
			name: "stale source hash",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceMapFile(t, files, "catalog/conformance/acme/widgets-create.yaml", "182ed500197c682bca81a5a72cb1399e53e18dd87e9cc890fd89df1987b6a38d", strings.Repeat("0", 64))
			},
			want: "freshness binding",
		},
		{
			name: "resolved header drift",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceMapFile(t, files, "catalog/conformance/acme/widgets-create.yaml", "Authorization: Bearer fixture-acme-api-key", "Authorization: Bearer wrong-key")
			},
			want: "expected resolved headers",
		},
		{
			name: "missing fixture credential context",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceMapFile(t, files, "catalog/conformance/acme/widgets-create.yaml", "credentials:\n  acme_api_key: fixture-acme-api-key", "credentials: {}")
			},
			want: `credential "acme_api_key" is unavailable`,
		},
		{
			name: "missing fixture managed credential context",
			mutate: func(t *testing.T, files fstest.MapFS) {
				replaceMapFile(t, files, "catalog/conformance/github/issues-add-labels.yaml", "managed_credentials:\n  github_app: fixture-github-installation-token", "managed_credentials: {}")
			},
			want: `managed credential "github_app" is unavailable`,
		},
		{
			name: "missing fixture",
			mutate: func(t *testing.T, files fstest.MapFS) {
				delete(files, "catalog/conformance/acme/widgets-create.yaml")
			},
			want: "read catalog conformance fixture",
		},
		{
			name: "unreferenced fixture",
			mutate: func(t *testing.T, files fstest.MapFS) {
				files["catalog/conformance/acme/unreferenced.yaml"] = &fstest.MapFile{Data: append([]byte(nil), files["catalog/conformance/acme/widgets-create.yaml"].Data...)}
			},
			want: "unreferenced catalog conformance fixtures",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := catalogWorkingTreeFS(t)
			tc.mutate(t, files)
			artifacts, err := GenerateCatalog(files)
			if err != nil {
				t.Fatalf("GenerateCatalog: %v", err)
			}
			err = ValidateCatalogConformance(files, artifacts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateCatalogConformance error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestGeneratedConformanceRejectsManagedCredentialHeaderDriftAfterFreshnessRepair(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runtimecontracts.ManagedCredentialRef)
	}{
		{
			name: "managed header drift",
			mutate: func(ref *runtimecontracts.ManagedCredentialRef) {
				ref.Header = "X-GitHub-Token"
			},
		},
		{
			name: "managed prefix drift",
			mutate: func(ref *runtimecontracts.ManagedCredentialRef) {
				ref.Header = "Authorization"
				ref.Prefix = "Token"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := catalogWorkingTreeFS(t)
			var profile GeneratorProfile
			mustYAMLUnmarshal(t, files["catalog/generator-profiles/github.yaml"].Data, &profile)
			if profile.Auth.ManagedCredential == nil {
				t.Fatal("GitHub generator profile lacks managed credential")
			}
			tc.mutate(profile.Auth.ManagedCredential)
			files["catalog/generator-profiles/github.yaml"].Data = mustYAMLMarshal(t, profile)

			artifacts, err := GenerateCatalog(files)
			if err != nil {
				t.Fatalf("GenerateCatalog: %v", err)
			}
			github := generatedArtifactsByProvider(artifacts)["github"]
			if github.Manifest.Generation == nil {
				t.Fatal("generated GitHub artifact lacks generation evidence")
			}
			if len(github.Manifest.Generation.Operations) != 3 {
				t.Fatalf("generated GitHub operations = %d, want 3", len(github.Manifest.Generation.Operations))
			}

			for _, operation := range github.Manifest.Generation.Operations {
				fixturePath, err := catalogFixturePath(operation.FixtureID)
				if err != nil {
					t.Fatal(err)
				}
				fixture, err := ParseCatalogConformanceFixture(files[fixturePath].Data)
				if err != nil {
					t.Fatalf("ParseCatalogConformanceFixture: %v", err)
				}
				fixture.ProfileSHA256 = github.ProfileHash
				fixture.ManifestSHA256 = sha256String(github.ConnectorBody)
				files[fixturePath].Data = mustYAMLMarshal(t, fixture)
			}
			if err := ValidateCatalogConformance(files, artifacts); err == nil || !strings.Contains(err.Error(), "expected resolved headers") {
				t.Fatalf("ValidateCatalogConformance error = %v, want managed header drift rejection after freshness repair", err)
			}

			for _, operation := range github.Manifest.Generation.Operations {
				operation := operation
				t.Run(operation.ToolID, func(t *testing.T) {
					fixturePath, err := catalogFixturePath(operation.FixtureID)
					if err != nil {
						t.Fatal(err)
					}
					fixture, err := ParseCatalogConformanceFixture(files[fixturePath].Data)
					if err != nil {
						t.Fatalf("ParseCatalogConformanceFixture: %v", err)
					}
					err = validateCatalogFixtureBinding(github, operation, fixture)
					if err == nil || !strings.Contains(err.Error(), "expected resolved headers") {
						t.Fatalf("validateCatalogFixtureBinding error = %v, want managed header drift rejection after freshness repair", err)
					}
				})
			}
		})
	}
}

func TestGenerationEvidenceResponseSuccessBindingPreservesScalarKinds(t *testing.T) {
	tests := []struct {
		name     string
		evidence any
		tool     any
		wantErr  bool
	}{
		{name: "boolean and string differ", evidence: true, tool: "true", wantErr: true},
		{name: "numeric and string differ", evidence: 1, tool: "1", wantErr: true},
		{name: "numeric representations agree", evidence: 1, tool: 1.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			evidencePolicy := runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.value", Equals: tc.evidence}
			toolPolicy := runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.value", Equals: tc.tool}
			evidence := GenerationEvidence{
				SchemaVersion:    CatalogSchemaVersion,
				GeneratorVersion: CatalogGeneratorVersion,
				Source: GenerationSourceEvidence{
					Path:           "catalog/sources/acme.json",
					OpenAPIVersion: "3.0.3",
					SHA256:         "sha256:" + strings.Repeat("0", 64),
				},
				Profile: GenerationProfileEvidence{
					Path:          "catalog/generator-profiles/acme.yaml",
					SchemaVersion: CatalogSchemaVersion,
					SHA256:        "sha256:" + strings.Repeat("1", 64),
				},
				Operations: []GenerationOperationEvidence{{
					OperationID:     "widgets/create",
					ToolID:          "acme.create_widget",
					Permissions:     []GenerationPermission{{ID: "widgets:write", Note: "write widgets"}},
					ResponseSuccess: evidencePolicy,
					FixtureID:       "acme/widgets-create",
					FixtureStatus:   GenerationFixturePassing,
					ReviewStatus:    GenerationReviewApproved,
				}},
			}
			objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
			toolEntry := runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
				runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
				runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://acme.example.test/widgets"}),
				runtimecontracts.WithToolResponseSuccess(toolPolicy),
			)
			err := evidence.Validate("acme", map[string]runtimecontracts.ToolSchemaEntry{
				"acme.create_widget": toolEntry,
			})
			if tc.wantErr && (err == nil || !strings.Contains(err.Error(), "response_success does not match")) {
				t.Fatalf("Validate error = %v, want type-preserving mismatch", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestResponseSuccessAndGeneratorOwnersHaveNoRetiredOrProviderSpecificInterpreters(t *testing.T) {
	files := []string{
		"providerconnectors.go",
		"../runtime/tools/executor_http.go",
		"../runtime/pipeline/activity_engine.go",
		"catalog_generator.go",
	}
	forbidden := []string{
		"evaluateHTTPResponseSuccess",
		"evaluateActivityHTTPResponseSuccess",
		"responseSuccessValuesEqual",
		"activityResponseSuccessValuesEqual",
		"requiredResponseSuccessForProviderAction",
		"applyManagedCredentialHeader",
		"applyActivityManagedCredentialHeader",
		`provider == "github"`,
		`provider == "acme"`,
		`provider == "slack"`,
	}
	for _, name := range files {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range forbidden {
			if bytes.Contains(body, []byte(needle)) {
				t.Fatalf("%s retains forbidden semantic interpreter %q", name, needle)
			}
		}
	}
}

func generatedArtifactsByProvider(artifacts []GeneratedCatalogArtifact) map[string]GeneratedCatalogArtifact {
	out := make(map[string]GeneratedCatalogArtifact, len(artifacts))
	for _, artifact := range artifacts {
		out[artifact.Manifest.Provider] = artifact
	}
	return out
}

func jsonSchema(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func builtinCatalogFS(t *testing.T) fstest.MapFS {
	t.Helper()
	return catalogWorkingTreeFS(t)
}

func catalogWorkingTreeFS(t *testing.T) fstest.MapFS {
	t.Helper()
	out := copyTestFS(t, os.DirFS("."))
	for name, file := range copyTestFS(t, platformpacks.FS()) {
		out[name] = file
	}
	return out
}

func copyTestFS(t *testing.T, source fs.FS) fstest.MapFS {
	t.Helper()
	out := fstest.MapFS{}
	err := fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		body, err := fs.ReadFile(source, name)
		if err != nil {
			return err
		}
		out[name] = &fstest.MapFile{Data: body}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func replaceMapFile(t *testing.T, files fstest.MapFS, name, old, replacement string) {
	t.Helper()
	file := files[name]
	if file == nil || !bytes.Contains(file.Data, []byte(old)) {
		t.Fatalf("%s does not contain %q", name, old)
	}
	file.Data = bytes.Replace(file.Data, []byte(old), []byte(replacement), 1)
}

func mustYAMLUnmarshal(t *testing.T, body []byte, target any) {
	t.Helper()
	if err := unmarshalToolTestYAML(body, target); err != nil {
		t.Fatal(err)
	}
}

func mustYAMLMarshal(t *testing.T, value any) []byte {
	t.Helper()
	body, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
