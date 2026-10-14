package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runidentity"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

//go:embed family145-run-identity-preimages.json
var runIdentityBytes []byte

var runIdentityPins = func() []runidentity.Pin {
	pins, err := runidentity.Decode(runIdentityBytes)
	if err != nil {
		panic(err)
	}
	return pins
}()

func family145CheckRunIdentityWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range runIdentityPins {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("run identity escaped its exact scope, cuts or assertions:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145RunIdentityWholeRulePreservesEveryAssertionAndCut(t *testing.T) {
	calls := 0
	for i, pin := range runIdentityPins {
		old, err := exec.Command("git", "show", runidentity.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, old, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("run identity complete signed predecessor changed:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || requestKeyDeclaration(t, current, pin.Function) != pin.After {
			t.Fatal("run identity current output changed:", pin.Function, err)
		}
		rule := runidentity.Rules[i]
		after, err := runphysical.Transform(rule, pin.Before)
		if err != nil || after != pin.After || pin.File != rule.File || pin.Function != rule.Function || pin.Calls != rule.Calls {
			t.Fatal("run identity escaped its mechanical transform:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		output, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("run identity finite transform refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, output, finite); err != nil || changed {
			t.Fatal("run identity finite transform is not inert:", pin.Function, err)
		}
		for _, cut := range []string{"readServedRunPhysicalIdentity", "ReceiverStateReader", "initial.RunID", "later.RunID", "seed.RunID", "err != nil", " != ", "lease.Done()", "stop()", "release.Do", "waitForServedEventPublishNodeDeliveryLifecycle", "requireReceiverProofStateReader"} {
			runIdentityHostile(t, pin, finite, cut)
		}
		calls += pin.Calls
	}
	if calls != 8 {
		t.Fatalf("run identity read sites=%d want8", calls)
	}
}

func runIdentityHostile(t *testing.T, pin runidentity.Pin, finite recipe, cut string) {
	t.Helper()
	if strings.Contains(pin.After, cut) {
		mutant := strings.Replace(pin.After, cut, "unreviewedRunIdentityCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("changed run identity owner/key/assertion/cut accepted:", pin.Function, cut)
		}
	}
	for _, edit := range runidentity.Rules {
		if edit.File != pin.File || edit.Function != pin.Function {
			continue
		}
		mutant := strings.Replace(pin.Before, edit.Edits[len(edit.Edits)-1].Before, "unreviewedRunPredicate()", 1)
		if _, err := runphysical.Transform(edit, mutant); err == nil {
			t.Fatal("unknown predecessor predicate accepted:", pin.Function)
		}
	}
}

func TestFamily145RunIdentityUsesUnchangedNativeOwnerAndOneBorrowedCut(t *testing.T) {
	for _, owner := range [][2]string{
		{"internal/store/storetest/api_event_storage.go", "CountPhysicalRunIdentity"},
		{"internal/store/internal/runtimepersistence/test_api_event_storage.go", "CountPhysicalRunIdentityForTest"},
		{"internal/store/internal/backend/runlifecycle/run_identity_cardinality.go", "CountRunIdentityStorageTx"},
	} {
		before, err := exec.Command("git", "show", runidentity.Predecessor+":"+owner[0]).Output()
		current, readErr := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || readErr != nil || requestKeyDeclaration(t, before, owner[1]) != requestKeyDeclaration(t, current, owner[1]) {
			t.Fatal("run identity changed its existing owner/predicate/error contract:", owner, err, readErr)
		}
	}
	raw, err := os.ReadFile("../../../internal/serveapp/run_identity_observation_test.go")
	if err != nil {
		t.Fatal(err)
	}
	want := `func readServedRunPhysicalIdentity(reader servedEventCountReader, runID string) (int,error) {
if reader==nil { return 0,fmt.Errorf("served run identity count requires the original selected reader") }
var count int
err:=reader.InspectSnapshot(context.Background(),func(ctx context.Context)error { var err error; count,err=storetest.CountPhysicalRunIdentity(ctx,reader,runID); return err })
if err!=nil { return 0,err }; return count,nil
}`
	normal, err := requestkey.Normalize(want)
	if err != nil || requestKeyDeclaration(t, raw, "readServedRunPhysicalIdentity") != normal {
		t.Fatal("run identity did not borrow exactly one snapshot or discarded failure:", err)
	}
}

func TestFamily145RunIdentityCallerInventoryIsClosed(t *testing.T) {
	root := "../../.."
	calls, definitions := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		c, d := runIdentityOwnedFile(t, filepath.ToSlash(rel), file)
		calls, definitions = calls+c, definitions+d
		return nil
	})
	if err != nil || calls != 8 || definitions != 1 {
		t.Fatalf("run identity inventory=%d/%d want8/1 err=%v", calls, definitions, err)
	}
	for _, source := range []string{
		"package proof\nvar alias=readServedRunPhysicalIdentity",
		"package proof\nfunc foreign(){ readServedRunPhysicalIdentity(foreign,run) }",
		"package proof\nfunc readServedRunPhysicalIdentity(){}",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "hostile.go", source, parser.AllErrors)
		if err != nil || runIdentityFileError("foreign.go", file) == nil {
			t.Fatal("unmanifested run identity consumer/alias/owner accepted:", source, err)
		}
	}
}

func runIdentityOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	if err := runIdentityFileError(path, file); err != nil {
		t.Fatal(err)
	}
	calls, definitions := 0, 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "readServedRunPhysicalIdentity" {
			definitions++
		}
		if fn.Name.Name == "TestServedRunIdentityRefusesMissingSnapshotOwner" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && requestkey.Node(call.Fun) == "readServedRunPhysicalIdentity" {
				calls++
			}
			return true
		})
	}
	return calls, definitions
}

func runIdentityFileError(path string, file *ast.File) error {
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "readServedRunPhysicalIdentity" {
			if path != "internal/serveapp/run_identity_observation_test.go" {
				return fmt.Errorf("run identity acquired a second owner: %s", path)
			}
			allowed[fn.Name.Pos()] = true
		}
		for _, pin := range runIdentityPins {
			if pin.File == path && pin.Function == fn.Name.Name && pin.After == requestkey.Node(fn) {
				recordRunIdentityReferences(allowed, fn)
			}
		}
		if path == "internal/serveapp/run_identity_observation_test.go" && fn.Name.Name == "TestServedRunIdentityRefusesMissingSnapshotOwner" {
			recordRunIdentityReferences(allowed, fn)
		}
	}
	var failure error
	ast.Inspect(file, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "readServedRunPhysicalIdentity" && !allowed[id.Pos()] {
			failure = fmt.Errorf("run identity unmanifested consumer/alias: %s", path)
		}
		return true
	})
	return failure
}

func recordRunIdentityReferences(allowed map[token.Pos]bool, fn *ast.FuncDecl) {
	ast.Inspect(fn, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "readServedRunPhysicalIdentity" {
			allowed[id.Pos()] = true
		}
		return true
	})
}

func runIdentityEarlierHistory(t *testing.T, raw []byte, recipes bool) []byte {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	prior, _ := restoreRunIdentityHistory(rows, recipes)
	result, err := json.Marshal(prior)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFamily145RunIdentityPreservesHistoricalRecordsAndOrder(t *testing.T) {
	for _, path := range []string{"tools/fixture-codemod/pipeline-observations/recipes.json", "tools/fixture-codemod/pipeline-observations/family145-transitions.json"} {
		prior, err := exec.Command("git", "show", runidentity.Predecessor+":"+path).Output()
		if err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil {
			t.Fatal(err)
		}
		var before, after []map[string]any
		if err := json.Unmarshal(prior, &before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(current, &after); err != nil {
			t.Fatal(err)
		}
		retained, additions := restoreRunIdentityHistory(after, strings.HasSuffix(path, "recipes.json"))
		if additions != 1 || !reflect.DeepEqual(before, retained) {
			t.Fatal("run identity changed immutable history/relative order outside five exact records:", path)
		}
	}
}

func restoreRunIdentityHistory(rows []map[string]any, recipes bool) ([]map[string]any, int) {
	restorePublicRollbackHistory(rows, recipes)
	retained, additions := []map[string]any{}, 0
	for _, row := range rows {
		for _, pin := range runIdentityPins {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			if pin.Function == "TestServedResetPendingStartupUsesAdmittedSourcesBothStores" && row["Before"] == pin.Before && row["After"] == pin.After {
				additions++
				row = nil
				break
			}
			if recipes && row["Successor"] == pin.After {
				delete(row, "Successor")
			} else if !recipes && row["After"] == pin.After {
				row["After"] = pin.Before
			}
		}
		if row != nil {
			retained = append(retained, row)
		}
	}
	return retained, additions
}
