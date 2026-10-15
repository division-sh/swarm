package main

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
)

func family145PublicA2EndpointCallerInverse(function, current string) string {
	for _, pin := range publicendpoint.Pins() {
		if pin.Function == function {
			return publicendpoint.Previous(pin.File, function, current)
		}
	}
	return current
}

func family145CheckPublicA2EndpointWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145PublicJoinCallerInverse(function, current)
	for _, pin := range publicendpoint.Pins() {
		if pin.File == file && pin.Function == function {
			if pin.After != current {
				t.Fatalf("public endpoint propagation escaped complete rule: %s/%s", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145PublicA2EndpointWholeRulePreservesAll67CallersAnd16Bodies(t *testing.T) {
	pins := publicendpoint.Pins()
	if publicendpoint.ManifestDigest() != publicendpoint.Digest || len(pins) != 16 || len(publicendpoint.Helpers) != 5 {
		t.Fatal("complete public endpoint rule inventory changed")
	}
	seen, calls, helpers := map[string]bool{}, 0, 0
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("duplicate/changed complete endpoint preimage:", key)
		}
		seen[key] = true
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "before.go", "package proof\n"+pin.Before, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		function := file.Decls[0].(*ast.FuncDecl)
		transformed, count, err := publicendpoint.Transform(fset, function, pin.Before)
		if err != nil || count != pin.Calls || family145ServedDebugNormalize(t, transformed) != pin.After {
			t.Fatalf("endpoint-only rule changed request/assertion/temporal/construction cut: %s/%v", key, err)
		}
		calls += count
		if _, helper := publicendpoint.Helpers[pin.Function]; helper {
			helpers++
		}
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		if family145PublicJoinCallerInverse(pin.Function, servedSetupEarlierSource(t, pin.File, pin.Function, family145PublicA2CurrentDeclaration(t, raw, pin.Function))) != pin.After {
			t.Fatal("current endpoint consumer escaped exact full-body rule:", key)
		}
		family145PublicA2EndpointHostiles(t, pin)
	}
	if calls != 67 || helpers != 5 {
		t.Fatalf("calls=%d helpers=%d, want67/5", calls, helpers)
	}
}

func family145PublicA2CurrentDeclaration(t *testing.T, source []byte, function string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "current.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == function {
			return family145NodeString(fn)
		}
	}
	return ""
}

func family145PublicA2EndpointHostiles(t *testing.T, pin publicendpoint.Pin) {
	t.Helper()
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"rt.Endpoint", "endpoint", "entity.list", "entity.get", "event.list", "event.get", "run_id", "event_name", "NextCursor", "SourceEventID", "EntityID", "DeadLetters", "NoDelivery", "reflect.DeepEqual", "NewRunCreated", "RuntimeInstanceID", "ReadRunDeliveries", "release", "time.Sleep"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignPublicReadCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatalf("endpoint rule accepted changed route/request/assertion/lifetime: %s/%s", pin.Function, cut)
		}
		if publicendpoint.Previous(pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed complete successor entered an historical inverse")
		}
	}
	if publicendpoint.Previous("foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("foreign source location entered endpoint composition")
	}
	if _, helper := publicendpoint.Helpers[pin.Function]; !helper {
		return
	}
	for _, mutant := range []string{
		strings.Replace(pin.Before, "rt servedControlProofRuntime", "rt foreignRuntime", 1),
		strings.Replace(pin.Before, "t.Helper()", "t.Helper(); _ = rt.DB", 1),
		strings.Replace(pin.Before, "t.Helper()", "t.Helper(); _ = rt.Runtime", 1),
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "hostile.go", "package proof\n"+mutant, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := publicendpoint.Transform(fset, file.Decls[0].(*ast.FuncDecl), mutant); err == nil {
			t.Fatal("endpoint-only transform accepted unrelated runtime/SQL authority")
		}
	}
}

func TestFamily145PublicA2EndpointOwnedInventoryHasNoBroadCarrierOrEscapedReference(t *testing.T) {
	root := "../../.."
	calls, helpers := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		foundCalls, foundHelpers := publicA2EndpointInventoryFile(t, path, file)
		calls += foundCalls
		helpers += foundHelpers
		return nil
	})
	if err != nil || calls != 67 || helpers != 5 {
		t.Fatalf("whole owned public inventory calls=%d helpers=%d err=%v", calls, helpers, err)
	}
}

func publicA2EndpointInventoryFile(t *testing.T, path string, file *ast.File) (calls, helpers int) {
	t.Helper()
	allowed := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncDecl:
			if _, selected := publicendpoint.Helpers[node.Name.Name]; selected {
				helpers++
				allowed[node.Name.Pos()] = true
				if len(node.Type.Params.List) < 2 {
					t.Errorf("public helper lost endpoint argument: %s", path)
					return false
				}
				field := node.Type.Params.List[1]
				if len(field.Names) != 1 || field.Names[0].Name != "endpoint" || family145NodeString(field.Type) != "string" {
					t.Errorf("public helper retains broad authority: %s", path)
				}
			}
		case *ast.CallExpr:
			if publicA2EndpointInventoryCall(t, path, node, allowed) {
				calls++
			}
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok {
			if _, selected := publicendpoint.Helpers[id.Name]; selected && !allowed[id.Pos()] {
				t.Errorf("public endpoint helper has unclassified/escaped reference: %s/%s", path, id.Name)
			}
		}
		return true
	})
	return calls, helpers
}

func publicA2EndpointInventoryCall(t *testing.T, path string, call *ast.CallExpr, allowed map[token.Pos]bool) bool {
	t.Helper()
	name, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	arity, selected := publicendpoint.Helpers[name.Name]
	if !selected {
		return false
	}
	allowed[name.Pos()] = true
	if len(call.Args) != arity || call.Ellipsis.IsValid() {
		t.Errorf("public endpoint helper arity escaped finite rule: %s/%s", path, name.Name)
		return true
	}
	argument := family145NodeString(call.Args[1])
	if argument != "endpoint" && argument != "rt.Endpoint" {
		t.Errorf("public helper still propagates raw/broad/foreign runtime: %s/%s=%s", path, name.Name, argument)
	}
	return true
}
