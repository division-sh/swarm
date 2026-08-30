package scenarioderivation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
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
	write("work/data/not-a-scenario.yaml", "not: a scenario\n")
	write("data/tests/root-fixture.yaml", "name: wrong-root\nderive:\n  flow: root\n  input: request\n  payload:\n    generate: true\n")
	write("mocks/tests/root-case.yaml", "name: wrong-mock\nderive:\n  flow: root\n  input: request\n  payload:\n    generate: true\n")
	write("work/data/tests/child-fixture.yaml", "name: wrong-child\nderive:\n  flow: work\n  input: message\n  payload:\n    generate: true\n")
	write("work/tests/smoke.yaml", "name: smoke\nderive:\n  flow: work\n  input: message\n  payload:\n    generate: true\n")
	write("tests/root.yaml", "name: root\nderive:\n  flow: root\n  input: request\n  payload:\n    generate: true\n")

	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	declarations, err := LoadDeclarations(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(declarations) != 2 || declarations[0].Path != "tests/root.yaml" || declarations[0].Declaration.Name != "root" || declarations[1].Path != "work/tests/smoke.yaml" || declarations[1].Declaration.Name != "smoke" {
		t.Fatalf("declarations = %#v", declarations)
	}
}
