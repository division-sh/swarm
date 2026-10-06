package main

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestRewrite2566TypedStageFieldsExcludeIndependentInitialData(t *testing.T) {
	before := []byte("package fixture\nvar _ = []FlowStageDeclaration{{ID: \"waiting\", Initial: true}, {ID: \"done\", Terminal: true}}\nvar _ = Field{Initial: 7, Terminal: false}\nvar _ = schema.FlowStageDeclaration{Initial: false, ID: \"other\", Terminal: false}\n")
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, "fixture.go", before, 0)
	if err != nil {
		t.Fatal(err)
	}
	after, count, err := rewriteTypedStages(before, positions, file)
	if err != nil || count != 4 || !strings.Contains(string(after), "Field{Initial: 7, Terminal: false}") || strings.Contains(string(after), "FlowStageDeclaration{Initial:") || !strings.Contains(string(after), "Final: true") {
		t.Fatalf("typed fixture/data cut: count=%d output=%s err=%v", count, after, err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "fixture.go", after, 0); err != nil {
		t.Fatal(err)
	}
	positions = token.NewFileSet()
	file, _ = parser.ParseFile(positions, "fixture.go", after, 0)
	second, count, err := rewriteTypedStages(after, positions, file)
	if err != nil || count != 0 || string(second) != string(after) {
		t.Fatalf("typed cut not idempotent: %d %v", count, err)
	}
}
