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

type revisionRetryOwnerContract struct {
	path, symbol, transaction string
}

func revisionResetOwners() []revisionRetryOwnerContract {
	return []revisionRetryOwnerContract{
		{"entityruntime/persistence.go", "EntitySQLiteOwner.runPrivateAuthorActivityMutation", "s.backend.RunTransaction"},
		{"agentpersistence/directive_owner_support.go", "AgentSQLiteOwner.runPrivateAuthorActivityMutation", "s.runRuntimeMutation"},
		{"llmpersistence/owner.go", "LLMSQLiteOwner.runRuntimeMutationOutcome", "s.backend.RunTransactionOutcome"},
		{"runlifecycle/owner.go", "RunLifecycleSQLiteOwner.runPrivateAuthorActivityMutationObserved", "s.runRuntimeMutationOutcome"},
		{"pipelinepersistence/owner.go", "PipelineSQLiteOwner.runRuntimeMutationOutcome", "s.backend.RunTransactionOutcome"},
	}
}

func TestRunForkRevisionAttemptResetOwnersAreClosed(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	want := map[string]int{}
	for _, contract := range revisionResetOwners() {
		path := "internal/store/internal/backend/" + contract.path
		want[path+"|"+contract.symbol] = 1
		body, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionAttemptReset(string(body), contract); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	got := map[string]int{}
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
				if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "AttemptReset" {
					got[filepath.ToSlash(rel)+"|"+symbol]++
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retry reset must stay at exact outer owners, never subordinate writers: got=%v want=%v", got, want)
	}
}

func revisionRetryCallback(fn *ast.FuncDecl, callee string) (*ast.CallExpr, *ast.FuncLit, error) {
	var transaction *ast.CallExpr
	var callback *ast.FuncLit
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || revisionGuardNode(call.Fun) != callee {
			return true
		}
		count++
		transaction = call
		if len(call.Args) > 0 {
			callback, _ = call.Args[len(call.Args)-1].(*ast.FuncLit)
		}
		return true
	})
	if count != 1 || callback == nil {
		return nil, nil, fmt.Errorf("expected one literal callback at %s, got %d", callee, count)
	}
	return transaction, callback, nil
}

func validateRevisionAttemptReset(source string, contract revisionRetryOwnerContract) error {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	fn := functions[contract.symbol]
	if fn == nil {
		return fmt.Errorf("missing outer retry owner %s", contract.symbol)
	}
	transaction, callback, err := revisionRetryCallback(fn, contract.transaction)
	if err != nil {
		return err
	}
	var capture *ast.AssignStmt
	var resetName string
	for _, statement := range fn.Body.List {
		assignment, ok := statement.(*ast.AssignStmt)
		if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
			continue
		}
		if revisionGuardNode(assignment.Rhs[0]) == "effects.AttemptReset()" {
			if capture != nil {
				return fmt.Errorf("multiple retry baseline captures")
			}
			capture = assignment
			resetName = revisionGuardNode(assignment.Lhs[0])
		}
	}
	if capture == nil || capture.End() >= transaction.Pos() || resetName == "_" {
		return fmt.Errorf("%s must capture seeded baseline outside transaction callback", contract.symbol)
	}
	if len(callback.Body.List) == 0 {
		return fmt.Errorf("empty retry callback")
	}
	first, ok := callback.Body.List[0].(*ast.ExprStmt)
	if !ok || revisionGuardNode(first.X) != resetName+"()" {
		return fmt.Errorf("%s must invoke baseline reset first at callback entry", contract.symbol)
	}
	uses := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && identifier.Name == resetName {
			uses++
		}
		return true
	})
	if uses != 2 {
		return fmt.Errorf("reset handle escaped or was invoked outside sole callback entry: uses=%d", uses)
	}
	return nil
}

// These owners have no predeclared baseline: fresh effects belong inside the
// retry callback. They must not acquire a second subordinate reset instead.
func TestRunForkRevisionFreshEffectsStayInsideRetryOwners(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	for _, contract := range []revisionRetryOwnerContract{
		{"effectpersistence/owner.go", "EffectSQLiteOwner.runRuntimeMutationOutcome", "s.backend.RunTransactionOutcome"},
		{"eventpersistence/owner.go", "EventSQLiteOwner.runRuntimeMutationOutcome", "s.backend.RunTransactionOutcome"},
		{"runforkpersistence/run_fork_selected_contract_activation_owner.go", "sqliteRunForkSelectedContractActivationPort", "s.backend.RunTransactionOutcome"},
		{"runforkpersistence/run_fork_selected_contract_materialization_owner.go", "sqliteRunForkSelectedContractMaterializationPort", "s.backend.RunTransactionOutcome"},
		{"runforkpersistence/run_fork_selected_contract_discard_owner.go", "sqliteRunForkSelectedContractDiscardPort", "s.runRuntimeMutation"},
	} {
		body, err := os.ReadFile(filepath.Join(root, "internal/store/internal/backend", contract.path))
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRevisionFreshAttempt(string(body), contract); err != nil {
			t.Fatalf("%s/%s: %v", contract.path, contract.symbol, err)
		}
	}
}

func validateRevisionFreshAttempt(source string, contract revisionRetryOwnerContract) error {
	functions, err := revisionGuardFunctions(source)
	if err != nil {
		return err
	}
	fn := functions[contract.symbol]
	if fn == nil {
		return fmt.Errorf("missing fresh-attempt owner %s", contract.symbol)
	}
	_, callback, err := revisionRetryCallback(fn, contract.transaction)
	if err != nil {
		return err
	}
	count, inside := 0, 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "NewEffects" {
			count++
			if call.Pos() > callback.Body.Pos() && call.End() < callback.Body.End() {
				inside++
			}
		}
		return true
	})
	if count != 1 || inside != 1 {
		return fmt.Errorf("fresh effects must be allocated once per attempt, got total=%d inside=%d", count, inside)
	}
	return nil
}

func TestRunForkRevisionRetryOwnerGuardHostileControls(t *testing.T) {
	contract := revisionRetryOwnerContract{symbol: "write", transaction: "backend.RunTransaction"}
	valid := `package p; func write() error { reset := effects.AttemptReset(); return backend.RunTransaction(func() error { reset(); return mutate(effects) }) }`
	for _, tc := range []struct {
		name, source string
	}{
		{"capture_inside", strings.Replace(valid, "reset := effects.AttemptReset(); return backend.RunTransaction(func() error {", "return backend.RunTransaction(func() error { reset := effects.AttemptReset();", 1)},
		{"missing_reset", strings.Replace(valid, "reset();", "", 1)},
		{"reset_after_mutation", strings.Replace(valid, "reset(); return mutate(effects)", "err := mutate(effects); reset(); return err", 1)},
		{"deferred_reset", strings.Replace(valid, "reset();", "defer reset();", 1)},
		{"reset_outside", strings.Replace(valid, "return backend.RunTransaction", "reset(); return backend.RunTransaction", 1)},
		{"subordinate_reset", strings.Replace(valid, "return mutate(effects)", "return mutateWithReset(effects, reset)", 1)},
		{"wrong_baseline", strings.Replace(valid, "effects.AttemptReset()", "otherEffects.AttemptReset()", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRevisionAttemptReset(tc.source, contract); err == nil {
				t.Fatal("invalid retry reset placement passed")
			}
		})
	}
	if err := validateRevisionAttemptReset(valid, contract); err != nil {
		t.Fatal(err)
	}
	fresh := `package p; func write() error { return backend.RunTransaction(func() error { effects := revision.NewEffects(); return mutate(effects) }) }`
	if err := validateRevisionFreshAttempt(fresh, contract); err != nil {
		t.Fatal(err)
	}
	outside := `package p; func write() error { effects := revision.NewEffects(); return backend.RunTransaction(func() error { return mutate(effects) }) }`
	if err := validateRevisionFreshAttempt(outside, contract); err == nil {
		t.Fatal("stale generated IDs can survive retry through outer effects allocation")
	}
}
