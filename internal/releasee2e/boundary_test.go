package releasee2e

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReleaseE2EPackageStaysAtPublicProcessBoundary(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get release E2E package directory: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read release E2E package directory: %v", err)
	}
	files := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			t.Fatalf("release E2E package contains production Go source %s", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		parsed, err := parser.ParseFile(files, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, imported := range parsed.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", entry.Name(), err)
			}
			if strings.Contains(name, "github.com/division-sh/swarm") {
				t.Fatalf("release E2E source %s imports Swarm implementation package %q", entry.Name(), name)
			}
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		if strings.Contains(string(raw), "//go:"+"linkname") {
			t.Fatalf("release E2E source %s uses go:linkname", entry.Name())
		}
		for _, forbidden := range []string{
			"ManagedProvider" + "PreflightAuthority",
			"NewClaude" + "CLIRuntime",
			"Serve" + "Options",
			"TestRuntime" + "ReadyHook",
			"cliapp." + "Execute",
			"serveapp." + "Run",
		} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("release E2E source %s names forbidden in-process seam %q", entry.Name(), forbidden)
			}
		}
	}
}
