package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

func admittedCatalogTestSpec(t testing.TB, catalog map[string]yaml.Node) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	for name, node := range catalog {
		if node.Kind == 0 {
			catalog[name] = yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		}
	}
	body, err := yaml.Marshal(map[string]any{"platform": map[string]any{"name": "test", "version": "1.0.0"}, "platform_events": map[string]any{"catalog": catalog}})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := runtimecontracts.ParsePlatformSpecDocument(body, "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return spec
}
