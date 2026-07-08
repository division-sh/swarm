package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProjectPackageDocumentConnectorPacksStrictDecode(t *testing.T) {
	var doc ProjectPackageDocument
	if err := yaml.Unmarshal([]byte(`
name: demo
connector_packs:
  imports:
    - provider: telegram
      tool: telegram.send_message
`), &doc); err != nil {
		t.Fatalf("Unmarshal connector_packs: %v", err)
	}
	if len(doc.ConnectorPacks.Imports) != 1 {
		t.Fatalf("imports = %#v, want one", doc.ConnectorPacks.Imports)
	}
	got := doc.ConnectorPacks.Imports[0]
	if got.Provider != "telegram" || got.Tool != "telegram.send_message" {
		t.Fatalf("import = %#v, want normalized telegram.send_message", got)
	}

	var invalid ProjectPackageDocument
	err := yaml.Unmarshal([]byte(`
name: demo
connector_packs:
  imports:
    - provider: telegram
      tool: telegram.send_message
      shadow: true
`), &invalid)
	if err == nil || !strings.Contains(err.Error(), `connector_packs.imports field "shadow" not in platform spec`) {
		t.Fatalf("Unmarshal invalid connector_packs error = %v, want strict field failure", err)
	}
}
