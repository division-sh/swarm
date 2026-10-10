package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func catalogDialectDiscriminatorRetired(source []byte) bool {
	file, err := parser.ParseFile(token.NewFileSet(), "catalog.go", source, parser.AllErrors)
	if err != nil {
		return false
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && (fn.Name.Name == "catalogDialectQuery" || fn.Name.Name == "catalogIsSQLiteDB") {
			return false
		}
	}
	return true
}

func TestNativeCatalogDialectDiscriminationStaysAbsent(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "runtime", "cataloge2e", "assertions_harness_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !catalogDialectDiscriminatorRetired(source) {
		t.Fatal("local raw-store dialect interpreter survived owner migration")
	}
	for _, regression := range []string{
		`func catalogDialectQuery(db *sql.DB, postgres, sqlite string) string { if catalogIsSQLiteDB(db) { return sqlite }; return postgres }`,
		`func catalogIsSQLiteDB(db *sql.DB) bool { return db != nil && strings.Contains(strings.ToLower(fmt.Sprintf("%T", db.Driver())), "sqlite") }`,
	} {
		mutant := append(append([]byte(nil), source...), []byte("\n"+regression)...)
		if catalogDialectDiscriminatorRetired(mutant) {
			t.Fatal("obsolete dialect helper reintroduction accepted")
		}
	}
	if catalogDialectDiscriminatorRetired([]byte("package broken\nfunc (")) {
		t.Fatal("incomplete source accepted")
	}
}
