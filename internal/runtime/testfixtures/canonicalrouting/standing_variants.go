package canonicalrouting

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// WithoutStandingIngressPins removes both sides of the provider interface.
func WithoutStandingIngressPins(t testing.TB, schema string) string {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(schema), &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatal("standing ingress schema must be a mapping")
	}
	root := document.Content[0]
	found := false
	for index := 0; index < len(root.Content); index += 2 {
		if root.Content[index].Value == "pins" {
			root.Content = append(root.Content[:index], root.Content[index+2:]...)
			found = true
			break
		}
	}
	if !found {
		t.Fatal("standing ingress event pins not found")
	}
	raw, err := yaml.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
