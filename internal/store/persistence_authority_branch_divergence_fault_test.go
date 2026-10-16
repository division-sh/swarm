package store_test

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// #642/6208640706: exact fixture provenance, never a new raw-authority allowance.
const branchFaultNative = "internal/store/internal/runtimepersistence/test_selected_branch_divergence_insert_fault.go"
const branchFaultPublic = "internal/store/storetest/selected_branch_divergence_insert_fault.go"
const branchFaultCaller = "internal/runtime/runforkexecution/branch_point_component_test.go"

func branchFaultBoundaryErrors(root string, overrides map[string][]byte) []string {
	var failures []string
	for path, pin := range map[string]string{
		branchFaultNative: "3db2e4d00501761141424a1e098ed4bc8a9d00a679fa75fa7ff117ce678a5cd0",
		branchFaultPublic: "1bf0b17b8657ce82cb7ebfb5b91fe003ed9bff3282e9b239defc4f6611a2ca07",
	} {
		data, err := branchFaultSource(root, path, overrides)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != pin {
			failures = append(failures, "changed exact native fault owner: "+path)
		}
	}
	paths, err := branchFaultPaths(root, overrides)
	if err != nil {
		return append(failures, err.Error())
	}
	actual := map[string]int{}
	for path := range paths {
		data, err := branchFaultSource(root, path, overrides)
		if err != nil {
			return append(failures, err.Error())
		}
		if err := branchFaultReferences(path, data, actual); err != nil {
			return append(failures, err.Error())
		}
	}
	want := map[string]int{
		branchFaultCaller + "::q1DivergenceInsertFault::public":                 2,
		branchFaultPublic + "::SetSelectedBranchDivergenceInsertFault::private": 1,
	}
	if !reflect.DeepEqual(actual, want) {
		failures = append(failures, fmt.Sprintf("fault caller census changed: actual=%v want=%v", actual, want))
	}
	return failures
}

func branchFaultPaths(root string, overrides map[string][]byte) (map[string][]byte, error) {
	paths := maps.Clone(overrides)
	if paths == nil {
		paths = map[string][]byte{}
	}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".go") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			if _, found := paths[filepath.ToSlash(rel)]; !found {
				paths[filepath.ToSlash(rel)] = nil
			}
		}
		return nil
	})
	return paths, err
}

func branchFaultSource(root, path string, overrides map[string][]byte) ([]byte, error) {
	if data, found := overrides[path]; found && data != nil {
		return data, nil
	}
	return os.ReadFile(filepath.Join(root, path))
}

func branchFaultReferences(path string, data []byte, references map[string]int) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
	if err != nil {
		return err
	}
	imports := map[string]string{}
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return err
		}
		name := filepath.Base(value)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = value
	}
	ignored := map[*ast.Ident]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncDecl:
			ignored[node.Name] = true
		case *ast.SelectorExpr:
			ignored[node.Sel] = true
			if qualifier, ok := node.X.(*ast.Ident); ok {
				branchFaultCountReference(file, path, imports[qualifier.Name], node.Sel, references)
			}
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && !ignored[id] {
			owner := "github.com/division-sh/swarm/" + filepath.ToSlash(filepath.Dir(path))
			if dot, found := imports["."]; found {
				owner = dot
			}
			branchFaultCountReference(file, path, owner, id, references)
		}
		return true
	})
	return nil
}

func branchFaultCountReference(file *ast.File, path, owner string, id *ast.Ident, references map[string]int) {
	role := ""
	if owner == "github.com/division-sh/swarm/internal/store/storetest" && id.Name == "SetSelectedBranchDivergenceInsertFault" {
		role = "public"
	}
	if owner == "github.com/division-sh/swarm/internal/store/internal/runtimepersistence" && id.Name == "SetSelectedBranchDivergenceInsertFaultForTest" {
		role = "private"
	}
	if role == "" {
		return
	}
	declaration := "<file>"
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && id.Pos() >= fn.Pos() && id.End() <= fn.End() {
			declaration = fn.Name.Name
			break
		}
	}
	references[path+"::"+declaration+"::"+role]++
}

func TestSelectedBranchDivergenceFaultBoundaryIsExact(t *testing.T) {
	if failures := branchFaultBoundaryErrors(persistenceAuthorityRepoRoot(t), nil); len(failures) != 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}

func verifyBranchPointComponentsDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	t.Helper()
	for _, finding := range findings {
		if branchPointComponentRawAuthority(finding) {
			t.Errorf("branch component recovered raw authority: %s", finding.registryLine())
		}
	}
	if failures := branchFaultBoundaryErrors(persistenceAuthorityRepoRoot(t), nil); len(failures) != 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}

func branchPointComponentRawAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case branchFaultPublic, branchFaultCaller,
		"internal/runtime/runforkexecution/branch_point_component_fixture_test.go",
		"internal/runtime/runforkexecution/branch_point_cleanup_component_test.go",
		"internal/runtime/runforkexecution/branch_point_public_admission_test.go":
		return true
	}
	return false
}

func TestSelectedBranchPointComponentFaultRejectsRawAuthority(t *testing.T) {
	for _, source := range []string{
		`package runforkexecution; import "database/sql"; func q1DivergenceInsertFault(db *sql.DB){ _=db }`,
		`package runforkexecution; import("context"; "database/sql"); func mutate(write func(context.Context,*sql.Tx)error){ _=write }`,
	} {
		refused := false
		for _, finding := range debtAuthorityFindingsFromSource(t, branchFaultCaller, source) {
			refused = refused || branchPointComponentRawAuthority(finding)
		}
		if !refused {
			t.Fatal("raw branch fault handle/callback was not refused")
		}
	}
}

func TestSelectedBranchDivergenceFaultBoundaryRejectsHostiles(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	native, err := os.ReadFile(filepath.Join(root, branchFaultNative))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, old, replacement string }{
		{"target", "run_fork_selected_contract_branch_divergences", "run_fork_operations"},
		{"operation", "AFTER INSERT", "AFTER UPDATE"},
		{"error", "typed_receiver_owner_fault", "other_fault"},
		{"owner", "runWorkflowProjectionFault(ctx, selected", "otherOwner(ctx, selected"},
		{"raw-return", ") error {", ") *sql.DB {"},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := strings.ReplaceAll(string(native), test.old, test.replacement)
			if changed == string(native) || len(branchFaultBoundaryErrors(root, map[string][]byte{branchFaultNative: []byte(changed)})) == 0 {
				t.Fatal("changed native fixture authority was accepted")
			}
		})
	}
	for _, test := range []struct{ name, path, source string }{
		{"foreign-caller", "internal/runtime/runforkexecution/foreign_fault_test.go", `package runforkexecution; import "github.com/division-sh/swarm/internal/store/storetest"; func foreign(){ _ = storetest.SetSelectedBranchDivergenceInsertFault }`},
		{"extra-call", branchFaultCaller, `package runforkexecution; import "github.com/division-sh/swarm/internal/store/storetest"; func q1DivergenceInsertFault(){ _ = storetest.SetSelectedBranchDivergenceInsertFault; _ = storetest.SetSelectedBranchDivergenceInsertFault; _ = storetest.SetSelectedBranchDivergenceInsertFault }`},
		{"foreign-writer", "internal/store/internal/runtimepersistence/foreign_fault.go", `package runtimepersistence; var escaped = SetSelectedBranchDivergenceInsertFaultForTest`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if len(branchFaultBoundaryErrors(root, map[string][]byte{test.path: []byte(test.source)})) == 0 {
				t.Fatal("extra/foreign fault consumer was accepted")
			}
		})
	}
}
