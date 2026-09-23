package runtimepersistence

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const revisionProtocolPath = "internal/store/internal/backend/mutationprotocol/protocol.go"

func TestRunForkRevisionAttemptResetOwnersAreClosed(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	body, err := os.ReadFile(filepath.Join(root, revisionProtocolPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRevisionProtocolReset(string(body)); err != nil {
		t.Fatal(err)
	}
	got, err := revisionCallCensus(root, "AttemptReset")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{revisionProtocolPath + "|run": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("only mutationprotocol may reset the shared baseline at native retry entry: got=%v want=%v", got, want)
	}
}

func validateRevisionProtocolReset(source string) error {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	fn := functions["run"]
	if fn == nil {
		return fmt.Errorf("mutationprotocol.run is missing")
	}
	var capture *ast.AssignStmt
	var native *ast.CallExpr
	var callback *ast.FuncLit
	for _, statement := range fn.Body.List {
		if assign, ok := statement.(*ast.AssignStmt); ok && assign.Tok == token.DEFINE && len(assign.Rhs) == 1 && revisionGuardNode(assign.Rhs[0]) == "baseline.effects.AttemptReset()" {
			if len(assign.Lhs) != 1 || revisionGuardNode(assign.Lhs[0]) != "resetEffects" || capture != nil {
				return fmt.Errorf("shared baseline reset capture is not unique")
			}
			capture = assign
		}
		ast.Inspect(statement, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if ok && revisionGuardNode(call.Fun) == "native" && len(call.Args) == 2 {
				native = call
				callback, _ = call.Args[1].(*ast.FuncLit)
			}
			return true
		})
	}
	if capture == nil || native == nil || callback == nil || capture.End() >= native.Pos() {
		return fmt.Errorf("reset must be captured from the baseline before the native retry callback")
	}
	if len(callback.Body.List) < 3 {
		return fmt.Errorf("native retry callback is incomplete")
	}
	if revisionGuardNode(callback.Body.List[0]) != "if previous != nil {\n\tprevious.active = false\n}" || revisionGuardNode(callback.Body.List[1]) != "resetEffects()" {
		return fmt.Errorf("native retry must deactivate the old attempt then reset effects before mutation")
	}
	resetCalls, writes := 0, 0
	ast.Inspect(callback.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch revisionGuardNode(call.Fun) {
		case "resetEffects":
			resetCalls++
		case "write":
			writes++
			if call.Pos() < callback.Body.List[1].End() {
				writes = -100
			}
		}
		return true
	})
	if resetCalls != 1 || writes != 1 {
		return fmt.Errorf("native retry requires one reset before one domain write: resets=%d writes=%d", resetCalls, writes)
	}
	uses := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && ident.Name == "resetEffects" {
			uses++
		}
		return true
	})
	if uses != 2 {
		return fmt.Errorf("reset handle escaped the native retry entry: uses=%d", uses)
	}
	return nil
}

func TestRunForkRevisionFreshEffectsStayInsideRetryOwners(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	body, err := os.ReadFile(filepath.Join(root, revisionProtocolPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRevisionProtocolFreshBaseline(string(body)); err != nil {
		t.Fatal(err)
	}
	got, err := revisionCallCensus(root, "NewEffects")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		revisionProtocolPath + "|NewBaseline":                                           1,
		"internal/store/internal/backend/runforkrevision/effects.go|ForRun":             1,
		"internal/store/internal/backend/runforkrevision/finalizer.go|validateComplete": 1,
		"internal/store/testutil/runforkrevisionfixture/revision.go|Capture":            1,
		"internal/store/testutil/runforkrevisionfixture/revision.go|CaptureSQLite":      1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("new effects allocator escaped protocol, projection verifier, or test fixture: got=%v want=%v", got, want)
	}
}

func validateRevisionProtocolFreshBaseline(source string) error {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	baseline, run := functions["NewBaseline"], functions["run"]
	if baseline == nil || run == nil {
		return fmt.Errorf("mutationprotocol baseline or run owner is missing")
	}
	if err := validateRevisionWriterCalls(source, revisionExactWriterContract{symbol: "NewBaseline", calls: []string{"privatefork.NewEffects()"}}, false); err != nil {
		return err
	}
	found := false
	for _, statement := range run.Body.List {
		branch, ok := statement.(*ast.IfStmt)
		if !ok || revisionGuardNode(branch.Cond) != "baseline == nil" || len(branch.Body.List) != 1 {
			continue
		}
		if revisionGuardNode(branch.Body.List[0]) == "baseline = NewBaseline()" {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("nil baseline must allocate through NewBaseline before native retry")
	}
	return nil
}

func revisionCallCensus(root, name string) (map[string]int, error) {
	result := map[string]int{}
	err := filepath.WalkDir(filepath.Join(root, "internal/store"), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		functions, err := revisionGuardFunctions(string(body))
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for symbol, fn := range functions {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == name {
					result[filepath.ToSlash(rel)+"|"+symbol]++
				} else if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == name {
					result[filepath.ToSlash(rel)+"|"+symbol]++
				}
				return true
			})
		}
		return nil
	})
	return result, err
}

func TestRunForkRevisionRetryOwnerGuardHostileControls(t *testing.T) {
	valid := `package p; func run() { resetEffects := baseline.effects.AttemptReset(); native(ctx, func() { if previous != nil { previous.active = false }; resetEffects(); attempt := NewAttempt(); write(ctx, attempt) }) }`
	for _, tc := range []struct{ name, source string }{
		{"capture_inside", strings.Replace(valid, "resetEffects := baseline.effects.AttemptReset(); native(ctx, func() {", "native(ctx, func() { resetEffects := baseline.effects.AttemptReset();", 1)},
		{"missing_reset", strings.Replace(valid, "resetEffects();", "", 1)},
		{"reset_after_mutation", strings.Replace(valid, "resetEffects(); attempt := NewAttempt(); write(ctx, attempt)", "attempt := NewAttempt(); write(ctx, attempt); resetEffects()", 1)},
		{"deferred_reset", strings.Replace(valid, "resetEffects();", "defer resetEffects();", 1)},
		{"extra_reset", strings.Replace(valid, "resetEffects();", "resetEffects(); resetEffects();", 1)},
		{"wrong_baseline", strings.Replace(valid, "baseline.effects.AttemptReset()", "other.effects.AttemptReset()", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRevisionProtocolReset(tc.source); err == nil {
				t.Fatal("invalid protocol reset placement passed")
			}
		})
	}
	if err := validateRevisionProtocolReset(valid); err != nil {
		t.Fatal(err)
	}
	fresh := `package p; func NewBaseline() *Baseline { return &Baseline{effects: privatefork.NewEffects()} }; func run() { if baseline == nil { baseline = NewBaseline() } }`
	if err := validateRevisionProtocolFreshBaseline(fresh); err != nil {
		t.Fatal(err)
	}
	if err := validateRevisionProtocolFreshBaseline(strings.Replace(fresh, "baseline = NewBaseline()", "baseline = otherBaseline", 1)); err == nil {
		t.Fatal("missing fresh baseline was accepted")
	}
}
