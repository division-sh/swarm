package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/budgetdiagnostic"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func servedSetupComparisonDeclaration(t *testing.T, pins []runphysical.Pin, path string, fn *ast.FuncDecl) *ast.FuncDecl {
	t.Helper()
	current := requestkey.Node(fn)
	actual := current
	var err error
	current, err = selectedreadrole.EarlierSource("../../..", path, fn.Name.Name, current)
	if err != nil {
		t.Fatal(err)
	}
	current, err = channelsettlement.EarlierSource("../../..", path, fn.Name.Name, current)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := servedsettlement.EarlierSource(pins, path, fn.Name.Name, current)
	if err != nil {
		t.Fatal(err)
	}
	if actual == prior {
		return fn
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, "package proof\n"+prior, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatal("setup comparison changed declaration cardinality:", err)
	}
	previous, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || previous.Name.Name != fn.Name.Name {
		t.Fatal("setup comparison changed declaration identity")
	}
	return previous
}

func servedSetupPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	pins, err := servedsettlement.ReadSetupPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := servedsettlement.ValidateSetupCurrentSource("../../..", pins); err != nil {
		t.Fatal(err)
	}
	return pins
}

func servedSetupPriorHistory(t *testing.T, recipes bool) []byte {
	t.Helper()
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	raw, err := exec.Command("git", "show", servedsettlement.SetupPredecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func servedSetupEarlierHistory(t *testing.T, current []byte, recipes bool) []byte {
	t.Helper()
	var err error
	current, err = selectedreadrole.EarlierStandingIngressHistory("../../..", current, recipes)
	if err != nil {
		t.Fatal(err)
	}
	current, err = selectedreadrole.EarlierDeploymentResourceHistory("../../..", current, recipes)
	if err != nil {
		t.Fatal(err)
	}
	current, err = selectedreadrole.EarlierEntityToolHistory("../../..", current, recipes)
	if err != nil {
		t.Fatal(err)
	}
	current, err = channelsettlement.EarlierHistory("../../..", current, recipes)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := servedsettlement.RestoreSetupHistory(current, servedSetupPriorHistory(t, recipes), servedSetupPins(t), recipes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func servedSetupEarlierSource(t *testing.T, file, function, current string) string {
	t.Helper()
	current = servedSetupPredecessorSource(t, file, function, current)
	pins, err := servedsettlement.ReadSetupPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := servedsettlement.ValidateSetupCurrentSource("../../..", pins); err != nil {
			t.Fatal(err)
		}
		prior, err := servedsettlement.EarlierSource(pins, file, function, current)
		if err != nil {
			t.Fatal(err)
		}
		return prior
	}
	return current
}

func servedSetupPredecessorSource(t *testing.T, file, function, current string) string {
	t.Helper()
	current, err := selectedreadrole.EarlierSource("../../..", file, function, current)
	if err == nil {
		current, err = budgetdiagnostic.EarlierSource("../../..", file, function, current)
	}
	if err == nil {
		current, err = channelsettlement.EarlierSource("../../..", file, function, current)
	}
	if err != nil {
		t.Fatal(err)
	}
	return current
}
