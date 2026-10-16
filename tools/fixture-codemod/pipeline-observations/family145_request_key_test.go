package main

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

//go:embed family145-request-key-preimages.json
var family145RequestKeyBytes []byte

func requestKeyPins(t *testing.T) []requestkey.Pin {
	t.Helper()
	pins, err := requestkey.Decode(family145RequestKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145RequestKeyCallerInverse(function, current string) string {
	pins, err := requestkey.Decode(family145RequestKeyBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			current = family145RequestKeyCurrentPrevious(pin.File, function, current)
			return requestkey.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckRequestKeyWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145RequestKeyCurrentPrevious(file, function, current)
	for _, pin := range requestKeyPins(t) {
		if pin.File == file && strings.Split(pin.Function, "/")[0] == function {
			if current != pin.After {
				t.Fatal("request-key observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func requestKeyDeclaration(t *testing.T, raw []byte, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "request-key.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && requestkey.Key(fn) == name {
			return requestkey.Node(fn)
		}
	}
	return ""
}

func requestKeyHostiles(t *testing.T, pin requestkey.Pin) {
	t.Helper()
	pins := requestKeyPins(t)
	if requestkey.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || requestkey.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("request-key inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "ReadAPIIdempotencyCardinality", "ReadEventIdempotencyCardinality", "CountSourceEventPublications", "ReceiverStateReader", "reader", "captureSelectedRuntimePersistence", "requireReceiverProofStateReader", "p.deps.EventStore", "idempotencyKey", "key", "method", "got !=", "count !=", "n !=", "t.Fatal", "0", "want", "requirePublicInputRollbackNoResidue", "startPublicInputRollbackRuntime"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedRequestKeyCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || requestkey.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed request-key predicate/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func requestKeyOwnedFile(t *testing.T, pins []requestkey.Pin, helpers map[string]bool, path string, file *ast.File, proof sourceBlobDeclarationInvocation) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				if requestKeyRepairCaller(t, path, fn.Name.Name, requestkey.Node(fn)) {
					return true
				}
				current := family145RequestKeyCurrentPrevious(path, fn.Name.Name, proof.previous(t, path, fn.Name.Name, requestkey.Node(fn)))
				if requestkey.Previous(pins, path, fn.Name.Name, current) == current {
					t.Error("request-key caller escaped complete source-pinned owner chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && helpers[id.Name] && !allowed[id.Pos()] {
			t.Error("request-key helper escaped as alias/initializer/unclassified reference:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}

const requestKeyRepairCallerPath = "internal/serveapp/scenario_selected_fixture_test.go"
const requestKeyRepairCallerName = "TestPublicInputPublicationFaultRefusesWrongTemplateBothStores"

func requestKeyRepairCaller(t *testing.T, path, name, current string) bool {
	t.Helper()
	if path != requestKeyRepairCallerPath || name != requestKeyRepairCallerName {
		return false
	}
	signed, err := exec.Command("git", "show", selectedreadrole.Repair2698After+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return current != "" && current == requestKeyDeclaration(t, signed, name)
}

func verifyRequestKeyRollbackAdjacency(t *testing.T, source string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "cut.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, stmt := range block.List {
			if requestkey.Node(stmt) == "requirePublicInputRollbackNoResidue(t, db, backend, eventID, runID)" {
				if i+1 >= len(block.List) || requestkey.Node(block.List[i+1]) != requestkey.RollbackAssertion {
					t.Error("API-zero observation crossed a fault/cleanup/lifecycle cut")
				}
				found++
			}
		}
		return true
	})
	if found != 1 {
		t.Fatal("missing/duplicate retained rollback helper:", found)
	}
}
