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

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
	checked, err := CheckGeneratedCatalog(root)
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
		if tool.ManagedCredential == nil || tool.ManagedCredential.Key != "github_app" || tool.ManagedCredential.InstallationIDInput != "installation_id" {
			t.Fatalf("generated GitHub tool %q credential = %#v", toolID, tool.ManagedCredential)
		}
		if tool.ResponseSuccess == nil || tool.ResponseSuccess.Kind != "http_status_2xx" {
			t.Fatalf("generated GitHub tool %q response_success = %#v", toolID, tool.ResponseSuccess)
		}
	}

	acme := artifacts["acme"]
	tool, ok := acme.Manifest.Tools["acme.create_widget"]
	if !ok || tool.HTTP == nil {
		t.Fatalf("synthetic generated tool = %#v, want acme.create_widget HTTP tool", tool)
	}
	if tool.HTTP.Method != "POST" || tool.HTTP.URL != "https://api.acme.test/accounts/{{input.account_id}}/widgets" {
		t.Fatalf("synthetic generated HTTP shape = %#v", tool.HTTP)
	}
	if got := tool.HTTP.Headers["Authorization"]; got != "Bearer {{credentials.acme_api_key}}" {
		t.Fatalf("synthetic generated Authorization = %q", got)
	}
	if len(tool.Credentials) != 1 || tool.Credentials[0] != "acme_api_key" {
		t.Fatalf("synthetic generated credentials = %#v", tool.Credentials)
	}
	if _, exists := BuiltinTool("acme", "acme.create_widget"); exists {
		t.Fatal("synthetic conformance connector became an ambient builtin tool")
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
	if _, err := ParseGeneratorProfile(append(profileBody, []byte("---\nschema_version: \"1\"\n")...)); err == nil || !strings.Contains(err.Error(), "multiple YAML documents are forbidden") {
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
	replaceMapFile(t, files, "catalog/generator-profiles/acme.yaml", "182ed500197c682bca81a5a72cb1399e53e18dd87e9cc890fd89df1987b6a38d", strings.Repeat("0", 64))
	if _, err := GenerateCatalog(files); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("source hash mismatch error = %v", err)
	}
}

func TestGeneratedBuiltinIdentityCannotBeDowngradedOrAliased(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, files fstest.MapFS)
		want   string
	}{
		{
			name: "generation removed with repaired manifest hash",
			mutate: func(t *testing.T, files fstest.MapFS) {
				var manifest ConnectorManifest
				mustYAMLUnmarshal(t, files["packs/github/connector.yaml"].Data, &manifest)
				manifest.Generation = nil
				body := mustYAMLMarshal(t, manifest)
				files["packs/github/connector.yaml"].Data = body
				rewritePackManifestHash(t, files, "packs/github/pack.yaml", body)
			},
			want: "indexed as generated but generation evidence is missing",
		},
		{
			name: "unknown indexed builtin",
			mutate: func(t *testing.T, files fstest.MapFS) {
				var index GeneratedPackIndex
				mustYAMLUnmarshal(t, files[generatedPackIndexFile].Data, &index)
				files["catalog/generator-profiles/unknown.yaml"] = &fstest.MapFile{Data: append([]byte(nil), files["catalog/generator-profiles/acme.yaml"].Data...)}
				index.Packs = append(index.Packs, GeneratedPackIndexEntry{ID: "provider.unknown.connector", Provider: "unknown", Profile: "catalog/generator-profiles/unknown.yaml", Kind: GeneratedPackKindBuiltin, Output: "packs/unknown"})
				files[generatedPackIndexFile].Data = mustYAMLMarshal(t, index)
			},
			want: "references unknown builtin pack id",
		},
		{
			name: "duplicate pack id",
			mutate: func(t *testing.T, files fstest.MapFS) {
				var index GeneratedPackIndex
				mustYAMLUnmarshal(t, files[generatedPackIndexFile].Data, &index)
				duplicate := index.Packs[0]
				duplicate.Profile = "catalog/generator-profiles/duplicate.yaml"
				duplicate.Output = "packs/duplicate"
				files[duplicate.Profile] = &fstest.MapFile{Data: append([]byte(nil), files[index.Packs[0].Profile].Data...)}
				index.Packs = append(index.Packs, duplicate)
				files[generatedPackIndexFile].Data = mustYAMLMarshal(t, index)
			},
			want: "duplicate pack id",
		},
		{
			name: "duplicate profile binding",
			mutate: func(t *testing.T, files fstest.MapFS) {
				var index GeneratedPackIndex
				mustYAMLUnmarshal(t, files[generatedPackIndexFile].Data, &index)
				duplicate := index.Packs[0]
				duplicate.ID = "provider.alias.connector"
				duplicate.Output = "packs/alias"
				index.Packs = append(index.Packs, duplicate)
				files[generatedPackIndexFile].Data = mustYAMLMarshal(t, index)
			},
			want: "duplicate profile binding",
		},
		{
			name: "indexed provider mismatch",
			mutate: func(t *testing.T, files fstest.MapFS) {
				var index GeneratedPackIndex
				mustYAMLUnmarshal(t, files[generatedPackIndexFile].Data, &index)
				for i := range index.Packs {
					if index.Packs[i].ID == "provider.github.connector" {
						index.Packs[i].Provider = "gitlab"
					}
				}
				files[generatedPackIndexFile].Data = mustYAMLMarshal(t, index)
			},
			want: "does not match indexed provider",
		},
		{
			name: "indexed profile bytes drift",
			mutate: func(t *testing.T, files fstest.MapFS) {
				files["catalog/generator-profiles/github.yaml"].Data = append(files["catalog/generator-profiles/github.yaml"].Data, '\n')
			},
			want: "profile hash mismatch",
		},
		{
			name: "source path evidence drift with repaired manifest hash",
			mutate: func(t *testing.T, files fstest.MapFS) {
				mutateGeneratedGitHubManifest(t, files, func(manifest *ConnectorManifest) {
					manifest.Generation.Source.Path = "catalog/sources/other.json"
				})
			},
			want: "source evidence does not match indexed profile",
		},
		{
			name: "source version evidence drift with repaired manifest hash",
			mutate: func(t *testing.T, files fstest.MapFS) {
				mutateGeneratedGitHubManifest(t, files, func(manifest *ConnectorManifest) {
					manifest.Generation.Source.OpenAPIVersion = "3.1.0"
				})
			},
			want: "source evidence does not match indexed profile",
		},
		{
			name: "source hash evidence drift with repaired manifest hash",
			mutate: func(t *testing.T, files fstest.MapFS) {
				mutateGeneratedGitHubManifest(t, files, func(manifest *ConnectorManifest) {
					manifest.Generation.Source.SHA256 = "sha256:" + strings.Repeat("0", 64)
				})
			},
			want: "source evidence does not match indexed profile",
		},
		{
			name: "generated builtin absent from index",
			mutate: func(t *testing.T, files fstest.MapFS) {
				var index GeneratedPackIndex
				mustYAMLUnmarshal(t, files[generatedPackIndexFile].Data, &index)
				kept := index.Packs[:0]
				for _, entry := range index.Packs {
					if entry.ID != "provider.github.connector" {
						kept = append(kept, entry)
					}
				}
				index.Packs = kept
				delete(files, "catalog/generator-profiles/github.yaml")
				files[generatedPackIndexFile].Data = mustYAMLMarshal(t, index)
			},
			want: "carries generation evidence but is not in the generated pack index",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := builtinCatalogFS(t)
			tc.mutate(t, files)
			_, err := loadBuiltinPackRegistryFS(files, "0.7.0")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("loadBuiltinPackRegistryFS error = %v, want containing %q", err, tc.want)
			}
		})
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
			err := evidence.Validate("acme", map[string]runtimecontracts.ToolSchemaEntry{
				"acme.create_widget": {ResponseSuccess: &toolPolicy},
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
	return copyTestFS(t, builtinConnectorPackFS)
}

func catalogWorkingTreeFS(t *testing.T) fstest.MapFS {
	t.Helper()
	return copyTestFS(t, os.DirFS("."))
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

func rewritePackManifestHash(t *testing.T, files fstest.MapFS, name string, manifestBody []byte) {
	t.Helper()
	var envelope packs.Envelope
	mustYAMLUnmarshal(t, files[name].Data, &envelope)
	envelope.ManifestHash = sha256String(manifestBody)
	files[name].Data = mustYAMLMarshal(t, envelope)
}

func mutateGeneratedGitHubManifest(t *testing.T, files fstest.MapFS, mutate func(*ConnectorManifest)) {
	t.Helper()
	var manifest ConnectorManifest
	mustYAMLUnmarshal(t, files["packs/github/connector.yaml"].Data, &manifest)
	mutate(&manifest)
	body := mustYAMLMarshal(t, manifest)
	files["packs/github/connector.yaml"].Data = body
	rewritePackManifestHash(t, files, "packs/github/pack.yaml", body)
}

func mustYAMLUnmarshal(t *testing.T, body []byte, target any) {
	t.Helper()
	if err := yaml.Unmarshal(body, target); err != nil {
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
