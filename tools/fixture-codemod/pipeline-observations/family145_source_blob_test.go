package main

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourceblob"
)

//go:embed family145-source-blob-preimages.json
var sourceBlobBytes []byte

func TestFamily145SourceBlobWholeRulePreservesCaptureAndEveryByteComparison(t *testing.T) {
	pins, err := sourceblob.Decode(sourceBlobBytes)
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	for index, pin := range pins {
		before, err := exec.Command("git", "show", sourceblob.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, before, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("source blob exact predecessor changed:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedObservationEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("source blob current endpoint changed:", pin.Function, err)
		}
		after, err := runphysical.Transform(sourceblob.Rules[index], pin.Before)
		if err != nil || after != pin.After || pin.Calls != sourceblob.Rules[index].Calls {
			t.Fatal("source blob escaped its finite read/capture edits:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("source blob finite rewrite refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("source blob rewrite is not inert:", pin.Function, err)
		}
		sourceBlobHostileCuts(t, pin, finite)
		reads += pin.Calls
	}
	if reads != 6 {
		t.Fatal("source blob read inventory changed:", reads)
	}
}

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

func TestFamily145SourceBlobConsumesUnchangedProductionOwnerAndPreservesFixtureType(t *testing.T) {
	for _, owner := range [][2]string{
		{"internal/serveapp/store_runtime.go", "projectServeRuntimePersistence"},
		{"internal/store/selected/accessors.go", "SourceArtifactStore"},
		{"internal/store/internal/runtimepersistence/source_artifact.go", "GetSourceArtifact"},
		{"internal/store/internal/sourceartifact/postgres.go", "GetSourceArtifact"},
		{"internal/store/internal/sourceartifact/postgres.go", "loadPostgresSourceArtifact"},
		{"internal/store/internal/sourceartifact/sqlite.go", "GetSourceArtifact"},
		{"internal/store/internal/sourceartifact/sqlite.go", "loadSQLiteSourceArtifact"},
	} {
		before, err := exec.Command("git", "show", sourceblob.Predecessor+":"+owner[0]).Output()
		current, readErr := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || readErr != nil || sourceBlobNamedDeclarations(t, before, owner[1]) != sourceBlobNamedDeclarations(t, current, owner[1]) {
			t.Fatal("existing source owner changed:", owner, err, readErr)
		}
	}
	path := "internal/serveapp/main_runtime_test.go"
	before, err := exec.Command("git", "show", sourceblob.Predecessor+":"+path).Output()
	current, readErr := os.ReadFile(filepath.Join("../../..", path))
	if err != nil || readErr != nil {
		t.Fatal(err, readErr)
	}
	current, err = selectedreadrole.CatchUp2542HistoricalSource("../../..", path, current)
	if err != nil {
		t.Fatal(err)
	}
	want, got := sourceBlobFixtureType(t, before, false), sourceBlobFixtureType(t, current, true)
	if want != got {
		t.Fatal("source artifact field changed any pre-existing fixture type/field")
	}
	bridge, err := os.ReadFile("../../../internal/serveapp/persisted_source_blob_observation_test.go")
	wantBridge := `func readServedPersistedSourceBlob(ctx context.Context,reader sourceArtifactReader,hash string)([]byte,error){if reader==nil{return nil,fmt.Errorf("persisted source bytes require the current selected artifact reader")};record,err:=reader.GetSourceArtifact(ctx,hash);if err!=nil{return nil,err};return record.SourceBlob,nil}`
	if err != nil || requestKeyDeclaration(t, bridge, "readServedPersistedSourceBlob") != requestKeyDeclaration(t, []byte("package proof\n"+wantBridge), "readServedPersistedSourceBlob") {
		t.Fatal("source bridge reinterpreted bytes or changed context/hash/failed record semantics:", err)
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

func TestFamily145SourceBlobDirectSQLInventoryIsClosed(t *testing.T) {
	err := checkoutsource.WalkDir("../../..", "../../../internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		if sourceBlobDirectSQL(file) {
			return fmt.Errorf("%s: persisted artifact blob bypasses the canonical source owner", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1", "SELECT source_blob FROM source_artifacts WHERE bundle_hash = $1"} {
		file, err := parser.ParseFile(token.NewFileSet(), "escape.go", "package proof\nvar escaped=func(){db.QueryRow("+strconv.Quote(query)+",hash)}", parser.AllErrors)
		if err != nil || !sourceBlobDirectSQL(file) {
			t.Fatal("source blob initializer SQL reintroduction admitted:", err)
		}
	}
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
