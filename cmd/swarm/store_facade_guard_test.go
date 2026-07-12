package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSelectedStoreFacadeOwnsProducerBackendBranching(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cmd/swarm: %v", err)
	}
	var failures []string
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
		failures = append(failures, selectedStoreFacadeProducerBackendReferences(path, string(body))...)
	}
	if len(failures) > 0 {
		t.Fatalf("producer-side backend references outside selected store facade/construction boundary:\n%s", strings.Join(failures, "\n"))
	}
}

func TestSelectedStoreFacadeBackendBranchingGuardRejectsProducerFixture(t *testing.T) {
	const src = `package main

func buildPublicSurfaceFromStore(stores storeBundle) {
	if stores.Postgres != nil {
		panic("producer inferred capability from concrete backend")
	}
}
`
	failures := selectedStoreFacadeProducerBackendReferences(filepath.Join("cmd", "swarm", "fixture.go"), src)
	if len(failures) == 0 {
		t.Fatal("expected producer-side concrete backend fixture to fail the guard")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "stores.Postgres") {
		t.Fatalf("expected failure to name stores.Postgres, got:\n%s", strings.Join(failures, "\n"))
	}
}

func selectedStoreFacadeProducerBackendReferences(path, body string) []string {
	allowed := map[string][]string{
		filepath.Join("cmd", "swarm", "main.go"): {
			"*store.PostgresStore",
			"RuntimeSQLDB",
			"store.NewPostgresStore",
			"store.NewSQLiteRuntimeStore",
		},
		filepath.Join("cmd", "swarm", "store_facade.go"): {
			"SQLDB:               s.RuntimeSQLDB",
			"f.stores.SQLDB == nil",
			"f.stores.SQLDB.Close()",
			"return f.stores.SQLDB",
		},
		filepath.Join("cmd", "swarm", "store_roles.go"): {
			"var _ selectedConcreteRuntimeStore = (*store.PostgresStore)(nil)",
			"Name: \"Postgres\"",
			"Name: \"RuntimeSQLDB\"",
		},
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
	var failures []string
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		for _, token := range forbidden {
			if !strings.Contains(line, token) {
				continue
			}
			if storeFacadeGuardAllowed(path, line, allowed[path]) {
				continue
			}
			failures = append(failures, filepath.ToSlash(path)+":"+strconv.Itoa(i+1)+" contains producer-side backend reference "+strconv.Quote(token)+": "+strings.TrimSpace(line))
		}
	}
	return failures
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
