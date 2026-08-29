//go:build darwin || linux

package startupownership

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestProductionAdvisoryLocksHaveNamedNonEngineTargets(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	allowedTargets := map[string]map[string]bool{
		filepath.FromSlash("internal/operatorchannel/proof_lock_unix.go"): {
			"int(file.Fd())": true,
		},
		filepath.FromSlash("internal/packartifact/project_lock_unix.go"): {
			"int(file.Fd())": true,
		},
		filepath.FromSlash("internal/runtime/credentials/file_lock_unix.go"): {
			"int(lock.Fd())": true,
		},
		filepath.FromSlash("internal/runtime/managedcredentials/file_lock_unix.go"): {
			"int(lock.Fd())": true,
		},
		filepath.FromSlash("internal/runtime/pythonmodule/artifact_lock_unix.go"): {
			"int(lock.Fd())": true,
		},
		filepath.FromSlash("internal/store/devscratch/epoch_unix.go"): {
			"fd":             true,
			"int(file.Fd())": true,
		},
		filepath.FromSlash("internal/store/internal/startupownership/sqlite_possession_unix.go"): {
			"int(coordinate.Fd())": true,
		},
		filepath.FromSlash("internal/testpostgres/file_lock_unix.go"): {
			"int(file.Fd())":   true,
			"int(l.file.Fd())": true,
		},
	}
	found := map[string]int{}
	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		set := token.NewFileSet()
		parsed, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			return err
		}
		var inspectErr error
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Flock" || len(call.Args) == 0 {
				return true
			}
			packageName, ok := selector.X.(*ast.Ident)
			if !ok || (packageName.Name != "unix" && packageName.Name != "syscall") {
				return true
			}
			var rendered bytes.Buffer
			if err := format.Node(&rendered, set, call.Args[0]); err != nil {
				inspectErr = err
				return false
			}
			target := rendered.String()
			if !allowedTargets[relative][target] {
				inspectErr = fmt.Errorf("unnamed production advisory-lock target %s:%s", relative, target)
				return false
			}
			found[relative+":"+target]++
			return true
		})
		return inspectErr
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]int{
		filepath.FromSlash("internal/operatorchannel/proof_lock_unix.go") + ":int(file.Fd())":                              2,
		filepath.FromSlash("internal/packartifact/project_lock_unix.go") + ":int(file.Fd())":                               2,
		filepath.FromSlash("internal/runtime/credentials/file_lock_unix.go") + ":int(lock.Fd())":                           2,
		filepath.FromSlash("internal/runtime/managedcredentials/file_lock_unix.go") + ":int(lock.Fd())":                    2,
		filepath.FromSlash("internal/runtime/pythonmodule/artifact_lock_unix.go") + ":int(lock.Fd())":                      2,
		filepath.FromSlash("internal/store/devscratch/epoch_unix.go") + ":fd":                                              1,
		filepath.FromSlash("internal/store/devscratch/epoch_unix.go") + ":int(file.Fd())":                                  1,
		filepath.FromSlash("internal/store/internal/startupownership/sqlite_possession_unix.go") + ":int(coordinate.Fd())": 2,
		filepath.FromSlash("internal/testpostgres/file_lock_unix.go") + ":int(file.Fd())":                                  1,
		filepath.FromSlash("internal/testpostgres/file_lock_unix.go") + ":int(l.file.Fd())":                                1,
	} {
		if found[key] != want {
			t.Fatalf("advisory-lock target %s count=%d, want %d", key, found[key], want)
		}
	}
}
