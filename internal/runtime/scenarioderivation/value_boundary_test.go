package scenarioderivation

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestDerivedDeclarationRejectsOutOfDomainResultsThroughArtifactLoading(t *testing.T) {
	base := "name: fixed\nderive: {flow: '.', input: request, payload: {generate: true%s}}\n%s"
	for _, row := range []struct{ name, set, extra string }{
		{"vars collision", "", "vars: {choice: \"${{1:'number','1':'text'}}\"}\n"},
		{"set collision", ", set: {value: \"${{true:'bool','true':'text'}}\"}", ""},
		{"set bytes", ", set: {value: \"${b'abc'}\"}", ""},
		{"witness collision", "", "connector_responses: {tool: \"${{'nested': [{1:'number','1':'text'}]}}\"}\n"},
		{"witness bytes", "", "connector_responses: {tool: \"${{'value': b'abc'}}\"}\n"},
		{"witness timestamp", "", "connector_responses: {tool: \"${{'value': timestamp('2026-01-01T00:00:00Z')}}\"}\n"},
	} {
		t.Run(row.name, func(t *testing.T) {
			raw := []byte(fmt.Sprintf(base, row.set, row.extra))
			if _, _, err := ParseDeclaration(raw, "tests/negative.yaml"); err == nil {
				t.Fatal("unsupported result accepted by derived parser")
			}
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "tests"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "tests", "negative.yaml"), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			if declarations, err := LoadDeclarations(artifact); err == nil || len(declarations) != 0 {
				t.Fatalf("unsupported result survived artifact load: %#v, %v", declarations, err)
			}
		})
	}
}

func TestDerivedDeclarationPreservesValidExactTextKeys(t *testing.T) {
	raw := []byte("name: fixed\nvars: {choice: \"${{'1':'number','true':'bool',' spaced ':'${1+1}'}}\"}\nderive: {flow: '.', input: request, payload: {generate: true, set: {value: '${vars.choice}', i: '${7}', d: '${7.0}'}}}\nconnector_responses: {tool: '${vars.choice}'}\n")
	declaration, found, err := ParseDeclaration(raw, "tests/positive.yaml")
	want := map[string]any{"1": "number", "true": "bool", " spaced ": "${1+1}"}
	if err != nil || !found || !reflect.DeepEqual(declaration.Set, map[string]any{"value": want, "i": int64(7), "d": float64(7)}) {
		t.Fatalf("valid declaration changed: %#v, %v", declaration, err)
	}
	response, err := canonicaljson.Decode(declaration.ConnectorResponses["tool"])
	if err != nil || !reflect.DeepEqual(response.Interface(), want) {
		t.Fatalf("valid witness changed: %#v, %v", response, err)
	}
}
