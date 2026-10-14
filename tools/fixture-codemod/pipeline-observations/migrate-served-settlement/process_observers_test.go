package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func TestProcessReadFiniteCandidateRefusesChangedInputsAndRetiresEverySQLCall(t *testing.T) {
	pins, err := selectedreadrole.ReadProcessPins("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		t.Run(pin.Function, func(t *testing.T) {
			raw, err := exec.Command("git", "show", selectedreadrole.ProcessPredecessor+":"+pin.File).Output()
			if err != nil {
				t.Fatal(err)
			}
			out, err := processReadCandidate(pin, raw)
			if err != nil {
				t.Fatal(err)
			}
			body, err := selectedreadrole.ProcessBody(out, pin.Function)
			if err != nil || body != pin.After {
				t.Fatal("finite rewrite changed its complete endpoint:", err)
			}
			repeated, err := processReadCandidate(pin, out)
			if err != nil || string(repeated) != string(out) {
				t.Fatal("finite rewrite is not idempotent:", err)
			}
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, pin.File, raw, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			fn, err := uniqueDestination(file, pin.Function)
			if err != nil {
				t.Fatal(err)
			}
			at := set.Position(fn.Body.Lbrace).Offset + 1
			mutant := append(append(append([]byte(nil), raw[:at]...), []byte("foreignAssertion();")...), raw[at:]...)
			if _, err := processReadCandidate(pin, mutant); err == nil {
				t.Fatal("changed complete predecessor was silently overwritten")
			}
			if pin.After != "" {
				assertProcessReadEndpointHasNoRawSQL(t, pin.After)
			}
		})
	}
}

func assertProcessReadEndpointHasNoRawSQL(t *testing.T, body string) {
	t.Helper()
	fn := readRoleProofFunction(t, body)
	if len(selectedReadRoleQueryMultiset(fn)) != 0 {
		t.Fatal("process observer retained a SELECT interpreter")
	}
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch method.Sel.Name {
		case "Query", "QueryRow", "QueryContext", "QueryRowContext", "Exec", "ExecContext", "Scan":
			t.Errorf("process observer retained raw SQL authority: %s", method.Sel.Name)
		}
		return true
	})
}
