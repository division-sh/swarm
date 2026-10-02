package providerconnectors

import (
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGeneratorProfileAdmissionFieldPresenceMatrix(t *testing.T) {
	body, err := fs.ReadFile(generatedConnectorIdentityFS, "catalog/generator-profiles/acme.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []struct {
		path                                  string
		emptySequence, emptyMapping, optional bool
	}{
		{path: "schema_version"}, {path: "provider"}, {path: "source"}, {path: "source.path"},
		{path: "source.compression"}, {path: "source.sha256"}, {path: "source.openapi_version"}, {path: "source.server"},
		{path: "pack"}, {path: "pack.id"}, {path: "pack.version"}, {path: "pack.platform_version"}, {path: "pack.provenance"}, {path: "pack.tests"},
		{path: "auth"}, {path: "auth.mode"}, {path: "auth.credentials"}, {path: "static_headers", emptyMapping: true},
		{path: "operations"}, {path: "operations.0.operation_id"}, {path: "operations.0.path"}, {path: "operations.0.method"},
		{path: "operations.0.tool_id"}, {path: "operations.0.description"}, {path: "operations.0.effect_class"}, {path: "operations.0.review_status"},
		{path: "operations.0.path_parameters", emptySequence: true}, {path: "operations.0.path_parameters.0.source"},
		{path: "operations.0.path_parameters.0.input"}, {path: "operations.0.path_parameters.0.type"}, {path: "operations.0.path_parameters.0.required"},
		{path: "operations.0.request_body"}, {path: "operations.0.request_body.required"}, {path: "operations.0.request_body.variant"},
		{path: "operations.0.request_body.variant.kind"}, {path: "operations.0.request_body.variant.fields"}, {path: "operations.0.request_body.fields"},
		{path: "operations.0.injected_inputs", emptySequence: true}, {path: "operations.0.permissions"}, {path: "operations.0.permissions.0.id"}, {path: "operations.0.permissions.0.note"},
		{path: "operations.0.output"}, {path: "operations.0.output.type"}, {path: "operations.0.response_success"}, {path: "operations.0.response_success.kind"},
		{path: "operations.0.fixture"}, {path: "operations.0.fixture.id"}, {path: "operations.0.fixture.status"},
	} {
		for _, state := range []string{"missing", "null", "empty text", "empty sequence", "empty mapping", "wrong kind", "valid", "merge"} {
			t.Run(field.path+"/"+state, func(t *testing.T) {
				mutated := mutateCatalogField(t, body, field.path, state)
				_, err := ParseGeneratorProfile(mutated)
				wantValid := state == "valid" || state == "merge" || state == "empty sequence" && field.emptySequence || state == "empty mapping" && field.emptyMapping || state == "missing" && field.optional
				if (err == nil) != wantValid {
					t.Fatalf("field %s state %s: admit=%t want=%t: %v", field.path, state, err == nil, wantValid, err)
				}
			})
		}
	}
}

func TestGeneratorProfileAdmissionRejectsInactiveMembersByPresence(t *testing.T) {
	body, err := fs.ReadFile(generatedConnectorIdentityFS, "catalog/generator-profiles/acme.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"auth.managed_credential", "operations.0.output.items_type", "operations.0.path_parameters.0.items_type"} {
		for _, state := range []string{"null", "empty text", "empty sequence", "empty mapping", "wrong kind", "valid", "merge"} {
			t.Run(path+"/"+state, func(t *testing.T) {
				_, err := ParseGeneratorProfile(mutateCatalogField(t, body, path, state))
				if err == nil || !strings.Contains(err.Error(), "forbids") {
					t.Fatalf("inactive member admitted: %v", err)
				}
			})
		}
	}
}

func TestConnectorAndGeneratedIndexAdmissionPreservePresence(t *testing.T) {
	artifacts, err := GenerateCatalog(os.DirFS("."))
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range artifacts {
		for _, path := range []string{"provider", "generation", "generation.schema_version", "generation.source", "generation.source.path", "generation.source.sha256", "generation.profile", "generation.profile.path", "generation.operations", "generation.operations.0.permissions", "generation.operations.0.permissions.0.note", "generation.operations.0.response_success", "generation.operations.0.fixture_id"} {
			for _, state := range []string{"missing", "null", "empty text", "empty sequence", "empty mapping", "wrong kind", "valid", "merge"} {
				t.Run(artifact.Manifest.Provider+"/"+path+"/"+state, func(t *testing.T) {
					manifest, err := ParseConnectorManifest(mutateCatalogField(t, artifact.ConnectorBody, path, state))
					if err == nil {
						err = manifest.Validate()
					}
					if err == nil && manifest.Generation != nil {
						err = manifest.Generation.Validate(manifest.Provider, manifest.Tools)
					}
					wantValid := state == "valid" || state == "merge" || path == "generation" && state == "missing"
					if (err == nil) != wantValid {
						t.Fatalf("admit=%t want=%t: %v", err == nil, wantValid, err)
					}
				})
			}
		}
	}
	indexBody, err := fs.ReadFile(generatedConnectorIdentityFS, generatedPackIndexFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"schema_version", "packs", "packs.0.id", "packs.0.provider", "packs.0.profile", "packs.0.kind", "packs.0.output"} {
		for _, state := range []string{"missing", "null", "empty text", "empty sequence", "empty mapping", "wrong kind", "valid", "merge"} {
			t.Run("index/"+path+"/"+state, func(t *testing.T) {
				index, err := admitGeneratedPackIndex(mutateCatalogField(t, indexBody, path, state), generatedPackIndexFile)
				if err == nil {
					err = index.Validate(generatedConnectorIdentityFS)
				}
				wantValid := state == "valid" || state == "merge"
				if (err == nil) != wantValid {
					t.Fatalf("admit=%t want=%t: %v", err == nil, wantValid, err)
				}
			})
		}
	}
}

func mutateCatalogField(t *testing.T, body []byte, path, state string) []byte {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	segments := strings.Split(path, ".")
	parent := doc.Content[0]
	for _, segment := range segments[:len(segments)-1] {
		if parent.Kind == yaml.SequenceNode {
			i, err := strconv.Atoi(segment)
			if err != nil || i >= len(parent.Content) {
				t.Fatalf("invalid test path %s", path)
			}
			parent = parent.Content[i]
			continue
		}
		found := false
		for i := 0; i < len(parent.Content); i += 2 {
			if parent.Content[i].Value == segment {
				parent = parent.Content[i+1]
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("invalid test path %s", path)
		}
	}
	name := segments[len(segments)-1]
	index := -1
	for i := 0; i < len(parent.Content); i += 2 {
		if parent.Content[i].Value == name {
			index = i
			break
		}
	}
	if index < 0 {
		index = len(parent.Content)
		parent.Content = append(parent.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "forbidden"})
	}
	original := parent.Content[index+1]
	replacement := original
	switch state {
	case "missing":
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
	case "null":
		replacement = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}
	case "empty text":
		replacement = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: ""}
	case "empty sequence":
		replacement = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	case "empty mapping":
		replacement = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	case "wrong kind":
		replacement = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: "1.5"}
	case "merge":
		parent.Content[index] = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!merge", Value: "<<"}
		replacement = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}, original}}
	case "valid":
	default:
		t.Fatalf("invalid state %s", state)
	}
	if state != "missing" {
		parent.Content[index+1] = replacement
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
