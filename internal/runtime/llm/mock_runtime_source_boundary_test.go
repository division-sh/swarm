package llm

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestProductionDoesNotImportCatalogOrFixtureRuntimeOwners(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve mock runtime source boundary test path")
	}
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	for _, root := range []string{filepath.Join(repo, "cmd"), filepath.Join(repo, "internal")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				rel, err := filepath.Rel(repo, path)
				if err != nil {
					return err
				}
				rel = filepath.ToSlash(rel)
				if rel == "internal/runtime/cataloge2e" || rel == "internal/runtime/testfixtures" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imported := range parsed.Imports {
				value, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				if strings.Contains(value, "/internal/runtime/cataloge2e") || strings.Contains(value, "/internal/runtime/testfixtures/") {
					t.Errorf("production source %s imports private test owner %q", filepath.ToSlash(strings.TrimPrefix(path, repo+string(filepath.Separator))), value)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan production source under %s: %v", root, err)
		}
	}
}
