package authoractivity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func orderingScopeConstructors(file *ast.File) []string {
	aliases := map[string]bool{}
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		if path != "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity" {
			continue
		}
		alias := "authoractivity"
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		aliases[alias] = true
	}
	var calls []string
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "BindTransaction" {
				return true
			}
			if name, ok := selector.X.(*ast.Ident); ok && aliases[name.Name] {
				calls = append(calls, fn.Name.Name)
			}
			return true
		})
	}
	return calls
}

func TestOrderingScopeConstructorInventoryIsClosed(t *testing.T) {
	want := map[string]string{
		"backend/postgres/transaction.go":       "runTransactionOutcome",
		"backend/postgres/session_authority.go": "runAuthorityTransaction",
		"backend/sqlite/transaction.go":         "runTransactionOnceOutcome",
	}
	err := filepath.WalkDir("../..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel("../..", path)
		if err != nil {
			return err
		}
		for _, fn := range orderingScopeConstructors(file) {
			if want[rel] != fn {
				t.Errorf("%s:%s mints ordering possession outside native settlement", rel, fn)
			}
			delete(want, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("native settlement owners lost ordering scope: %v", want)
	}
}

func TestOrderingScopeGuardDetectsDomainConstructor(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "domain.go", `package domain
import private "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
func write() { private.BindTransaction(nil, nil, nil, "postgres") }
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if calls := orderingScopeConstructors(file); len(calls) != 1 || calls[0] != "write" {
		t.Fatalf("domain ordering-scope mint escaped guard: %v", calls)
	}
}
