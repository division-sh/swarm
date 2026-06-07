package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelectedStoreFacadeOwnsProducerBackendBranching(t *testing.T) {
	allowed := map[string][]string{
		filepath.Join("cmd", "swarm", "main.go"): {
			"Postgres            *store.PostgresStore",
			"RuntimeSQLDB        *sql.DB",
			"pg, err := store.NewPostgresStore(dsn)",
			"RuntimeSQLDB:        pg.DB",
			"sqliteStore, err := store.NewSQLiteRuntimeStore(selection.SQLitePath)",
		},
		filepath.Join("cmd", "swarm", "store_facade.go"): {"*"},
	}
	forbidden := []string{
		"stores.Postgres",
		"stores.SQLDB",
		"stores.RuntimeSQLDB",
		"*store.PostgresStore",
		"store.NewPostgresStore",
		"store.NewSQLiteRuntimeStore",
		"RuntimeSQLDB",
		"postgres bool",
	}
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cmd/swarm: %v", err)
	}
	for _, file := range files {
		name := file.Name()
		if file.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join("cmd", "swarm", name)
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			for _, token := range forbidden {
				if !strings.Contains(line, token) {
					continue
				}
				if storeFacadeGuardAllowed(path, line, allowed[path]) {
					continue
				}
				t.Fatalf("%s:%d contains producer-side backend reference %q outside selected store facade/construction boundary: %s", path, i+1, token, strings.TrimSpace(line))
			}
		}
	}
}

func storeFacadeGuardAllowed(path, line string, allowed []string) bool {
	if len(allowed) == 1 && allowed[0] == "*" {
		return true
	}
	for _, snippet := range allowed {
		if strings.Contains(line, snippet) {
			return true
		}
	}
	return false
}
