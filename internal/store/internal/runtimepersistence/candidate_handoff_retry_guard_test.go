package runtimepersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func handoffCallName(call *ast.CallExpr) string {
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
		if owner, ok := selector.X.(*ast.Ident); ok {
			return owner.Name + "." + selector.Sel.Name
		}
		return selector.Sel.Name
	}
	if name, ok := call.Fun.(*ast.Ident); ok {
		return name.Name
	}
	return ""
}

func forbiddenDomainHandoffCall(file *ast.File) string {
	var forbidden string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := handoffCallName(call)
		switch {
		case strings.HasSuffix(name, ".ReserveCandidateHandoff"),
			strings.HasSuffix(name, ".ResetAttempt"),
			strings.HasSuffix(name, ".WithCandidateHandoffOutcome"),
			strings.HasSuffix(name, ".WithCandidateHandoffOutcomeResult"):
			forbidden = name
			return false
		}
		return true
	})
	return forbidden
}

func TestCandidateHandoffRetryProtocolIsSoleAssembler(t *testing.T) {
	root := filepath.Join(repoRootForRuntimeWriterGuard(t), "internal/store/internal/backend")
	var protocol *ast.FuncDecl
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.ToSlash(rel) != "mutationprotocol/protocol.go" {
			if call := forbiddenDomainHandoffCall(file); call != "" {
				return fmt.Errorf("%s: domain owner reassembled candidate handoff through %s", rel, call)
			}
			return nil
		}
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "run" {
				protocol = fn
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if protocol == nil {
		t.Fatal("canonical mutation protocol runner is missing")
	}
	positions := map[string][]token.Pos{}
	ast.Inspect(protocol.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			positions[handoffCallName(call)] = append(positions[handoffCallName(call)], call.Pos())
		}
		return true
	})
	for _, name := range []string{"runhandoff.ReserveCandidateHandoff", "handoff.ResetAttempt", "write"} {
		if len(positions[name]) != 1 {
			t.Fatalf("protocol runner has %d %s calls, want one", len(positions[name]), name)
		}
	}
	if !(positions["runhandoff.ReserveCandidateHandoff"][0] < positions["handoff.ResetAttempt"][0] &&
		positions["handoff.ResetAttempt"][0] < positions["write"][0]) {
		t.Fatal("candidate reservation/reset must precede each domain write attempt")
	}
}

func TestCandidateHandoffGuardRejectsDomainAssembler(t *testing.T) {
	for _, name := range []string{"ReserveCandidateHandoff", "ResetAttempt", "WithCandidateHandoffOutcome"} {
		file, err := parser.ParseFile(token.NewFileSet(), "owner.go", "package p; func write() { handoff."+name+"() }", 0)
		if err != nil || forbiddenDomainHandoffCall(file) == "" {
			t.Fatalf("%s bypass accepted: %v", name, err)
		}
	}
}
