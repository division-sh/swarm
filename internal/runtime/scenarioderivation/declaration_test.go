package scenarioderivation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseDeclarationRejectsMissingOrNonTextIdentityFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing name",
			yaml: "derive:\n  flow: work\n  input: message\n  payload:\n    generate: true\n",
			want: "name is required",
		},
		{
			name: "non-text name",
			yaml: "name: 7\nderive:\n  flow: work\n  input: message\n  payload:\n    generate: true\n",
			want: "name must be text",
		},
		{
			name: "missing flow",
			yaml: "name: smoke\nderive:\n  input: message\n  payload:\n    generate: true\n",
			want: "derive.flow is required",
		},
		{
			name: "missing input",
			yaml: "name: smoke\nderive:\n  flow: work\n  payload:\n    generate: true\n",
			want: "derive.input is required",
		},
		{
			name: "empty input",
			yaml: "name: smoke\nderive:\n  flow: work\n  input: '  '\n  payload:\n    generate: true\n",
			want: "derive.input must be non-empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, found, err := ParseDeclaration([]byte(tc.yaml))
			if found || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseDeclaration() found=%v err=%v, want %q", found, err, tc.want)
			}
		})
	}
}

func TestLoadDeclarationsConsumesOnlySupportedScenarioRoots(t *testing.T) {
	root := t.TempDir()
	write := func(relative, body string) {
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("flows/work/fixtures/invalid.yaml", "not: [valid")
	write("flows/work/tests/smoke.yaml", "name: smoke\nderive:\n  flow: work\n  input: message\n  payload:\n    generate: true\n")
	write("tests/root.yaml", "name: root\nderive:\n  flow: root\n  input: request\n  payload:\n    generate: true\n")

	declarations, err := LoadDeclarations(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(declarations) != 2 || declarations[0].Declaration.Name != "smoke" || declarations[1].Declaration.Name != "root" {
		t.Fatalf("declarations = %#v", declarations)
	}
}
