package main

import (
	"strconv"
	"strings"
	"testing"
)

func TestListRewritePreservesDataAndDeclarationOrder(t *testing.T) {
	before := "# keep this comment\nwork:\n  items: list<text>\n  groups: map[text]list<WorkItem>\n  optional: '[WorkItem]?'\n  explicit: {type: '[text]', initial: ['list<text>', '[text]']}\n"
	want := "# keep this comment\nwork:\n  items: [text]\n  groups: map[text][WorkItem]\n  optional: {type: \"[WorkItem]?\"}\n  explicit: {type: '[text]', initial: ['list<text>', '[text]']}\n"
	got := rewriteYAML(before, "entities.yaml")
	if got != want || rewriteYAML(got, "entities.yaml") != got {
		t.Fatalf("rewrite changed data/order or is not idempotent:\n%s", got)
	}
	for _, untouched := range []string{
		"stages:\n  pending: {}\n  done: {final: true}\n",
		"payload:\n  items: ['list<text>', '[text]']\n",
		"work:\n  items: &ref '[text]'\n  copy: *ref\n",
	} {
		if got := rewriteYAML(untouched, ""); got != untouched {
			t.Fatalf("unclassified/anchored source changed: %s", got)
		}
	}
}

func TestListRewriteUsesSourceRoleInGo(t *testing.T) {
	before := "package fixture\nfunc proof(t any) {\n" +
		"loadWave1EntityToolBundle(t, `types:\n  Work:\n    items: list<text>\n`, `work:\n  items: text\n`)\n" +
		"write(t, \"events.yaml\", \"work.ready:\\n  items: '[text]'\\n\")\n" +
		"_ = struct{ Type string; Initial string }{Type: \"list<text>\", Initial: \"list<text>\"}\n" +
		"_ = \"list<text>\"\n}\n"
	got := rewriteGo("fixture.go", before)
	for _, required := range []string{
		"items: [text]", "Type: \"[text]\"", "Initial: \"list<text>\"", "_ = \"list<text>\"",
	} {
		if !strings.Contains(got, required) {
			t.Fatalf("role-specific rewrite lost %q:\n%s", required, got)
		}
	}
	if rewriteGo("fixture.go", got) != got {
		t.Fatal("Go rewrite is not idempotent")
	}
}

func TestListRewriteGeneratedFragmentsHaveFiniteSourceScope(t *testing.T) {
	for path, replacements := range generatedSourceEdits {
		for _, replacement := range replacements {
			body := "package fixture\nvar source = " + strconv.Quote(replacement[0]+"\n") + "\n"
			want := "package fixture\nvar source = " + strconv.Quote(replacement[1]+"\n") + "\n"
			if got := rewriteGo(path, body); got != want || rewriteGo(path, got) != got {
				t.Fatalf("generated source %s is not exact/idempotent: %s", path, got)
			}
			if got := rewriteGo("data.go", body); got != body {
				t.Fatalf("unclassified data borrowed a generated source role: %s", got)
			}
		}
	}
}
