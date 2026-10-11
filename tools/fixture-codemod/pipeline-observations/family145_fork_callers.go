package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/tools/imports"
)

// This finite propagation starts at 109cc1338, after the shared owner repair.
// File digests pin the reviewed bindings, including nested callers. There is no
// inference from a variable spelling or from a caller-supplied pool expression.
var family145ForkCallerSources = []struct{ name, digest string }{
	{"execution_test.go", "44996b89ca731dc2c067e6bac1be0dfbbe9cf061b47fea8c66d6da7d9ab919ec"},
	{"constructed_source_fixture_test.go", "8b76d34890acb5e847c6df5179d30cb5385de483555c5d4e83738e4d72f5ff3d"},
	{"activity_fork_execution_test.go", "040d514e2e95cc53bac1df6ce2eb82bb66271cb90e3a584d2d6927a0105608e5"},
	{"runtime_outcome_test.go", "d664b3376240000683e25ea0ab0ddedd16970510e4a69d5adaf6d031995260d7"},
	{"selected_execution_authority_fixture_test.go", "38d537c2ac884ca7e200bd6857ca0b9bd578e31daa6a3ae3ada607e96592386b"},
}

type family145SourceEdit struct {
	start, end int
	text       string
}

func migrateFamily145ForkCallers(write bool) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	files, err := prepareFamily145ForkCallers(root)
	if err != nil {
		return err
	}
	// Reuse the complete candidate overlay, including test variants and every
	// rewritten package. A late source/type failure cannot partially apply it.
	if err := applyPendingFiles(root, files, write); err != nil {
		return err
	}
	fmt.Printf("family145-fork-callers: files=%d revision-calls=30 history-calls=21 write=%t\n", len(files), write)
	return nil
}

func prepareFamily145ForkCallers(root string) ([]pendingFile, error) {
	var files []pendingFile
	var revisionCalls, historyCalls, helpers int
	for _, source := range family145ForkCallerSources {
		path := filepath.Join(root, "internal/runtime/runforkexecution", source.name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != source.digest {
			return nil, fmt.Errorf("family145 source/binding changed: %s; no files written", source.name)
		}
		result, revisions, histories, owners, err := rewriteFamily145ForkCallerSource(source.name, data)
		if err != nil {
			return nil, err
		}
		revisionCalls += revisions
		historyCalls += histories
		helpers += owners
		files = append(files, pendingFile{path: path, data: result})
	}
	if revisionCalls != 30 || historyCalls != 21 || helpers != 3 {
		return nil, fmt.Errorf("family145 inventory changed: revisions=%d history=%d helpers=%d; no files written", revisionCalls, historyCalls, helpers)
	}
	return files, nil
}

func rewriteFamily145ForkCallerSource(name string, data []byte) ([]byte, int, int, int, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, name, data, parser.ParseComments)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	var edits []family145SourceEdit
	var revisions, histories, helpers int
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		changes, rev, history, owner, err := family145ForkFunctionEdits(set, fn, name)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		edits = append(edits, changes...)
		revisions += rev
		histories += history
		helpers += owner
	}
	result, err := applyFamily145SourceEdits(name, data, edits)
	return result, revisions, histories, helpers, err
}

func applyFamily145SourceEdits(name string, data []byte, edits []family145SourceEdit) ([]byte, error) {
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	result := bytes.Clone(data)
	end := len(data)
	for _, change := range edits {
		if change.start < 0 || change.start >= change.end || change.end > end {
			return nil, fmt.Errorf("overlapping family145 edits: %s", name)
		}
		result = append(append(append([]byte{}, result[:change.start]...), change.text...), result[change.end:]...)
		end = change.start
	}
	return imports.Process(name, result, nil)
}

func family145ForkFunctionEdits(set *token.FileSet, fn *ast.FuncDecl, name string) ([]family145SourceEdit, int, int, int, error) {
	owner, err := family145ForkHelperEdit(set, fn)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	var edits []family145SourceEdit
	var revisions, histories, helpers int
	if owner != nil {
		edits = append(edits, *owner)
		helpers = 1
		if fn.Name.Name == "captureSelectedExecutionSourceRevision" {
			return edits, 0, 0, helpers, nil
		}
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || err != nil {
			return err == nil
		}
		var change *family145SourceEdit
		var kind int
		change, kind, err = family145ForkCallEdit(set, call, name, fn.Name.Name)
		if change != nil {
			edits = append(edits, *change)
			switch kind {
			case 1:
				revisions++
			case 2:
				histories++
			}
		}
		return err == nil
	})
	return edits, revisions, histories, helpers, err
}

func family145ForkHelperEdit(set *token.FileSet, fn *ast.FuncDecl) (*family145SourceEdit, error) {
	switch fn.Name.Name {
	case "captureSelectedExecutionSourceRevision":
		return family145Edit(set, fn.Pos(), fn.End(), `func captureSelectedExecutionSourceRevision(t testing.TB, selected any, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	return storetest.CaptureSelectedRunForkRevision(t, context.Background(), selected, runID, families...)
}`), nil
	case "seedSelectedConstructedRootHistory", "seedSelectedConstructedAgentRootHistory":
		if len(fn.Type.Params.List) < 4 || len(fn.Type.Params.List[2].Names) != 1 || fn.Type.Params.List[2].Names[0].Name != "db" || family145NodeString(fn.Type.Params.List[2].Type) != "*sql.DB" {
			return nil, fmt.Errorf("unexpected history helper signature: %s", fn.Name.Name)
		}
		return family145Edit(set, fn.Type.Params.List[2].Pos(), fn.Type.Params.List[3].Pos(), ""), nil
	}
	return nil, nil
}

func family145ForkCallEdit(set *token.FileSet, call *ast.CallExpr, name, function string) (*family145SourceEdit, int, error) {
	helper, ok := call.Fun.(*ast.Ident)
	if !ok {
		return nil, 0, nil
	}
	switch helper.Name {
	case "captureSelectedExecutionSourceRevision":
		owner := family145ForkRevisionOwner(name, function)
		old := "db"
		if name == "activity_fork_execution_test.go" {
			old = "f.db.DB"
		}
		if len(call.Args) < 3 || family145NodeString(call.Args[1]) != old || owner == "" {
			return nil, 0, fmt.Errorf("unexpected revision caller binding: %s/%s", name, function)
		}
		return family145Edit(set, call.Args[1].Pos(), call.Args[1].End(), owner), 1, nil
	case "seedSelectedConstructedRootHistory", "seedSelectedConstructedAgentRootHistory":
		if len(call.Args) < 5 || family145NodeString(call.Args[2]) != "db" || (family145NodeString(call.Args[3]) != "pg" && family145NodeString(call.Args[3]) != "selected") {
			return nil, 0, fmt.Errorf("unexpected history caller binding: %s/%s", name, function)
		}
		return family145Edit(set, call.Args[2].Pos(), call.Args[3].Pos(), ""), 2, nil
	}
	return nil, 0, nil
}

func family145Edit(set *token.FileSet, start, end token.Pos, text string) *family145SourceEdit {
	return &family145SourceEdit{set.Position(start).Offset, set.Position(end).Offset, text}
}

func family145ForkRevisionOwner(file, function string) string {
	switch file {
	case "execution_test.go":
		if function == "buildSelectedForkProofContainer" || function == "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" {
			return "selected"
		}
		return "pg"
	case "runtime_outcome_test.go", "selected_execution_authority_fixture_test.go":
		return "selected"
	case "activity_fork_execution_test.go":
		return "f.owner.ports.fork"
	}
	return ""
}

func family145NodeString(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		return ""
	}
	return out.String()
}
