package providerconnectors

import (
	"io/fs"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

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
