package apispec

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSwarmExecutableRemainsAThinAcyclicCompositionShell(t *testing.T) {
	root := repoRoot(t)
	commandDir := filepath.Join(root, "cmd", "swarm")
	entries, err := os.ReadDir(commandDir)
	if err != nil {
		t.Fatalf("read cmd/swarm: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		if entry.Name() != "main.go" {
			t.Errorf("cmd/swarm production owner %s bypasses the thin executable boundary", entry.Name())
		}
	}

	mainPath := filepath.Join(commandDir, "main.go")
	mainSource, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatalf("read cmd/swarm/main.go: %v", err)
	}
	if lines := strings.Count(string(mainSource), "\n") + 1; lines > 500 {
		t.Fatalf("cmd/swarm/main.go has %d lines, want at most 500", lines)
	}
	mainImports := goFileImports(t, mainPath)
	for _, required := range []string{
		"github.com/division-sh/swarm/internal/cliapp",
		"github.com/division-sh/swarm/internal/serveapp",
	} {
		if !mainImports[required] {
			t.Errorf("cmd/swarm/main.go missing composition owner import %s", required)
		}
	}

	for path := range goDirectoryImports(t, filepath.Join(root, "internal", "cliapp")) {
		if path == "github.com/division-sh/swarm/internal/serveapp" {
			t.Fatal("internal/cliapp imports internal/serveapp; executable composition dependency must remain one-way")
		}
	}
}

func goDirectoryImports(t *testing.T, directory string) map[string]bool {
	t.Helper()
	imports := map[string]bool{}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read %s: %v", directory, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		for path := range goFileImports(t, filepath.Join(directory, entry.Name())) {
			imports[path] = true
		}
	}
	return imports
}

func goFileImports(t *testing.T, path string) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	imports := map[string]bool{}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.IMPORT {
			continue
		}
		for _, spec := range general.Specs {
			importSpec := spec.(*ast.ImportSpec)
			path, err := strconv.Unquote(importSpec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", path, err)
			}
			imports[path] = true
		}
	}
	return imports
}
