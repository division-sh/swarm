package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

func TestRewriteGeneratedPositivePins(t *testing.T) {
	before := "name: generated\nstages:\n  active: {initial: true}\npins:\n  inputs:\n    events:\n      - {event: work.requested, source: harness}\n  outputs:\n    events:\n      - {event: work.completed, sink: harness}\nconnect:\n  - {event: work.completed, from: ., to: .}\n"
	after, err := rewriteGoSource("producer.go", []byte("package fixture\nconst source = "+strconv.Quote(before)+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(before, "pins:\n  inputs:\n    events:\n      - {event: work.requested, source: harness}\n  outputs:\n    events:\n      - {event: work.completed, sink: harness}\n", "pins:\n  inputs:\n    - work.requested\n  outputs:\n    - work.completed\n", 1)
	if got := generatedSourceLiteral(t, after); got != want {
		t.Fatalf("rewritten source = %q, want %q", got, want)
	}
	second, err := rewriteGoSource("producer.go", after)
	if err != nil || string(second) != string(after) {
		t.Fatalf("second rewrite changed source: %s, %v", second, err)
	}
}

func TestRewriteGeneratedPinsSkipsMutationAndCompoundFragments(t *testing.T) {
	for _, source := range []string{
		"package fixture\nvar source = \"pins:\\n  inputs:\\n    events: \" + values\n",
		"package fixture\nvar source = strings.Replace(source, \"pins:\\n  inputs:\\n    events: [work.requested]\\n\", replacement, 1)\n",
	} {
		after, err := rewriteGoSource("producer.go", []byte(source))
		if err != nil || string(after) != source {
			t.Fatalf("non-source literal changed: %s, %v", after, err)
		}
	}
}

func TestRewriteGeneratedPinsRejectsUnratifiedOptions(t *testing.T) {
	for _, option := range []string{"source: external", "other: true", "resolution: {mode: select}", "initialize: {}"} {
		source := "pins:\n  inputs:\n    events:\n      - {event: work.requested, " + option + "}\n"
		if _, err := rewriteGoSource("producer.go", []byte("package fixture\nconst source = "+strconv.Quote(source)+"\n")); err == nil {
			t.Fatalf("rewrite accepted unratified option %s", option)
		}
	}
}

func generatedSourceLiteral(t testing.TB, source []byte) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "producer.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var literals []string
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			literals = append(literals, value)
		}
		return true
	})
	if len(literals) != 1 {
		t.Fatalf("got %d source literals, want one", len(literals))
	}
	return literals[0]
}
