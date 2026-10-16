package main

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourceblob"
)

//go:embed family145-source-blob-preimages.json
var sourceBlobBytes []byte

func sourceBlobHostileCuts(t *testing.T, pin sourceblob.Pin, finite recipe) {
	t.Helper()
	for _, cut := range []string{"readServedPersistedSourceBlob", "sourceReader = p.sourceReader", "SourceArtifacts: sourceReader", "selected.SourceArtifactStore()", "proof.SourceArtifacts", "context.Background()", "BundleHash", "verified.Hash", "bytes.Equal", "originalSource", "retainedSource", "memberTable", "MemberTable", "waitServedRunDeliveryQuiescence", "h.stop", "h.start", "lease.Done", "requestServedJSONRPC", "later.RunID", "idempotency_key", "clear", "t.Fatal", "err != nil"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedArtifactReadCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("changed artifact owner/hash/capture/bytes/assertion/reset/restart cut accepted:", pin.Function, cut)
		}
	}
}

func sourceBlobNamedDeclarations(t *testing.T, raw []byte, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "owner.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == name {
			result = append(result, family145NodeString(fn))
		}
	}
	if len(result) == 0 {
		t.Fatal("native source owner declaration missing:", name)
	}
	return strings.Join(result, "\n")
}

func sourceBlobFixtureType(t *testing.T, raw []byte, remove bool) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if ok && typeSpec.Name.Name == "servedControlProofRuntime" {
				return sourceBlobTypeImage(t, typeSpec, remove)
			}
		}
	}
	t.Fatal("shared fixture type missing")
	return ""
}

func sourceBlobTypeImage(t *testing.T, spec *ast.TypeSpec, remove bool) string {
	t.Helper()
	structure, ok := spec.Type.(*ast.StructType)
	if !ok {
		t.Fatal("shared fixture lost its struct")
	}
	seen := 0
	var fields []*ast.Field
	for _, field := range structure.Fields.List {
		if len(field.Names) == 1 && field.Names[0].Name == "SourceArtifacts" {
			seen++
			if family145NodeString(field.Type) != "sourceArtifactReader" {
				t.Fatal("source artifact fixture role is not the original reader interface")
			}
			if remove {
				continue
			}
		}
		fields = append(fields, field)
	}
	if (seen == 1) != remove {
		t.Fatal("source artifact fixture role absent/duplicate/unreviewed")
	}
	structure.Fields.List = fields
	return family145NodeString(spec)
}

func sourceBlobDirectSQL(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "QueryRow" && selector.Sel.Name != "QueryRowContext" {
			return true
		}
		for _, arg := range call.Args {
			literal, ok := arg.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				found = true
				continue
			}
			if strings.Join(strings.Fields(strings.ToLower(value)), "") == "selectsource_blobfromsource_artifactswherebundle_hash=$1" {
				found = true
			}
		}
		return true
	})
	return found
}
