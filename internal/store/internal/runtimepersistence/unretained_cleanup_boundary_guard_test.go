package runtimepersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

const (
	unretainedDeliveryFixturePath = "internal/store/internal/backend/delivery/test_unretained_cleanup_history.go"
	unretainedAgentFixturePath    = "internal/store/internal/backend/agentpersistence/test_unretained_cleanup_history.go"
	unretainedForkFixturePath     = "internal/store/internal/backend/runforkpersistence/test_unretained_cleanup_history.go"
	unretainedEventFixturePath    = "internal/store/internal/runtimepersistence/test_event_support.go"
	unretainedControlsPath        = "internal/store/internal/runtimepersistence/unretained_cleanup_history_controls_test.go"
	unretainedDiscardProofPath    = "internal/store/internal/runtimepersistence/selected_contract_runtime_execution_test.go"
)

// Every declaration and direct edge is finite. In particular, neither a
// test-looking file name nor membership in a private package grants custody.
var unretainedCleanupFixtureChain = map[operatorSnapshotFixtureDeclaration]map[string]int{
	{unretainedAgentFixturePath, "", "RequireUnretainedCleanupActorTx"}:            {},
	{unretainedForkFixturePath, "", "ValidateUnretainedCleanupHistoryTargetTx"}:    {},
	{unretainedDeliveryFixturePath, "", "InstallUnretainedCleanupHistoryTx"}:       {"installUnretainedCleanupAttempt": 1},
	{unretainedDeliveryFixturePath, "*Adapter", "installUnretainedCleanupAttempt"}: {},
	{unretainedDeliveryFixturePath, "", "RequireEmptyUnretainedCleanupHistoryTx"}:  {},
	{unretainedDeliveryFixturePath, "", "ReadUnretainedCleanupHistoryTx"}:          {},
	{unretainedEventFixturePath, "", "InstallUnretainedSelectedForkCleanupHistoryForTest"}: {
		"validateUnretainedCleanupHistoryInputsForTest": 1, "unretainedCleanupHistoryRecords": 1,
		"ValidateUnretainedCleanupHistoryTargetTx": 1, "unretainedCleanupActorIdentity": 1,
		"RequireUnretainedCleanupActorTx": 1, "RequireEmptyUnretainedCleanupHistoryTx": 1,
		"insertUnretainedCleanupEventTx": 1, "InstallUnretainedCleanupHistoryTx": 1,
	},
	{unretainedEventFixturePath, "", "validateUnretainedCleanupHistoryInputsForTest"}: {"unretainedCleanupActorIdentity": 1},
	{unretainedEventFixturePath, "", "unretainedCleanupActorIdentity"}:         {},
	{unretainedEventFixturePath, "", "unretainedCleanupHistoryRecords"}:        {},
	{unretainedEventFixturePath, "", "insertUnretainedCleanupEventTx"}:         {},
	{unretainedEventFixturePath, "", "ReadUnretainedSelectedForkCleanupHistoryForTest"}: {
		"ValidateUnretainedCleanupHistoryTargetTx": 1, "ReadUnretainedCleanupHistoryTx": 1,
	},
	{unretainedControlsPath, "", "unretainedCleanupHistoryInput"}: {},
	{unretainedControlsPath, "", "proveRetainedExecutionRefusesPhysicalCleanupHistory"}: {
		"unretainedCleanupHistoryInput": 1, "assertUnretainedCleanupHistoryRefusal": 1,
	},
	{unretainedControlsPath, "", "installAndProveUnretainedCleanupHistory"}: {
		"unretainedCleanupHistoryInput": 1, "proveUnretainedCleanupHistoryInputRefusals": 1,
		"proveUnretainedCleanupHistoryRollback": 1, "InstallUnretainedSelectedForkCleanupHistoryForTest": 1,
		"ReadUnretainedSelectedForkCleanupHistoryForTest": 1, "assertUnretainedCleanupHistoryRefusal": 1,
	},
	{unretainedControlsPath, "", "assertUnretainedCleanupHistoryRefusal"}: {
		"InstallUnretainedSelectedForkCleanupHistoryForTest": 1,
	},
	{unretainedControlsPath, "", "proveUnretainedCleanupHistoryInputRefusals"}: {
		"unretainedHistoryEvent": 3, "assertUnretainedCleanupHistoryRefusal": 1,
	},
	{unretainedControlsPath, "", "unretainedHistoryEvent"}: {},
	{unretainedControlsPath, "", "proveUnretainedCleanupHistoryRollback"}: {
		"InstallUnretainedSelectedForkCleanupHistoryForTest": 1,
	},
	{unretainedDiscardProofPath, "", "TestSelectedForkDiscardSelectedStoreParity"}: {
		"proveRetainedExecutionRefusesPhysicalCleanupHistory": 1,
	},
	{unretainedDiscardProofPath, "", "TestSelectedForkDiscardRollbackSelectedStoreParity"}: {
		"proveRetainedExecutionRefusesPhysicalCleanupHistory": 1,
	},
	{unretainedDiscardProofPath, "", "TestSelectedForkDiscardDeletesClaimedAndSettledDeliveryHistoryPostgres"}: {
		"installAndProveUnretainedCleanupHistory": 1,
	},
}

func unretainedCleanupFixtureName(name string) bool {
	for declaration := range unretainedCleanupFixtureChain {
		if declaration.name == name && declaration.path != unretainedDiscardProofPath {
			return true
		}
	}
	return false
}

func unretainedCleanupDirectCallees(node ast.Node) map[*ast.Ident]bool {
	callees := map[*ast.Ident]bool{}
	ast.Inspect(node, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			switch target := call.Fun.(type) {
			case *ast.Ident:
				callees[target] = true
			case *ast.SelectorExpr:
				callees[target.Sel] = true
			}
		}
		return true
	})
	return callees
}

func unretainedCleanupDeclarationViolations(path string, node ast.Decl) ([]string, operatorSnapshotFixtureDeclaration, map[string]int) {
	declaration := operatorSnapshotFixtureDeclaration{path: path, name: "package initializer"}
	var definition *ast.Ident
	if fn, ok := node.(*ast.FuncDecl); ok {
		declaration, definition = operatorSnapshotDeclaration(path, fn), fn.Name
	}
	allowed, owned := unretainedCleanupFixtureChain[declaration]
	var violations []string
	if definition != nil && unretainedCleanupFixtureName(definition.Name) && !owned {
		violations = append(violations, fmt.Sprintf("foreign fixture declaration: %+v", declaration))
	}
	callees, seen := unretainedCleanupDirectCallees(node), map[string]int{}
	violations = append(violations, unretainedCleanupClosureEscapes(path, node)...)
	ast.Inspect(node, func(node ast.Node) bool {
		id, ok := node.(*ast.Ident)
		if !ok || id == definition || !unretainedCleanupFixtureName(id.Name) {
			return true
		}
		if !owned || allowed[id.Name] == 0 || !callees[id] {
			violations = append(violations, fmt.Sprintf("%s/%s escapes fixture-only %s", path, declaration.name, id.Name))
		} else {
			seen[id.Name]++
		}
		return true
	})
	return violations, declaration, seen
}

func unretainedCleanupClosureEscapes(path string, node ast.Node) []string {
	allowed := map[*ast.FuncLit]bool{}
	ast.Inspect(node, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !unretainedCleanupSealedCallbackCall(call) {
			return true
		}
		for _, argument := range call.Args {
			if literal, ok := argument.(*ast.FuncLit); ok {
				allowed[literal] = true
			}
		}
		return true
	})
	var violations []string
	ast.Inspect(node, func(node ast.Node) bool {
		literal, ok := node.(*ast.FuncLit)
		if !ok || allowed[literal] {
			return true
		}
		ast.Inspect(literal, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && unretainedCleanupFixtureName(id.Name) {
				violations = append(violations, path+" escapes fixture closure "+id.Name)
			}
			return true
		})
		return false
	})
	return violations
}

func unretainedCleanupSealedCallbackCall(call *ast.CallExpr) bool {
	if target, ok := call.Fun.(*ast.Ident); ok {
		return target.Name == "runUnrevisionedEventFixtureTransactionForTest"
	}
	if target, ok := call.Fun.(*ast.SelectorExpr); ok {
		if receiver, ok := target.X.(*ast.Ident); ok && receiver.Name == "t" {
			return target.Sel.Name == "Run" || target.Sel.Name == "Cleanup"
		}
	}
	return false
}

func unretainedCleanupFileViolations(path string, raw []byte, declarations map[operatorSnapshotFixtureDeclaration]int) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var violations []string
	for _, node := range file.Decls {
		bad, declaration, seen := unretainedCleanupDeclarationViolations(path, node)
		violations = append(violations, bad...)
		if allowed, owned := unretainedCleanupFixtureChain[declaration]; owned {
			declarations[declaration]++
			for name, want := range allowed {
				if seen[name] != want {
					violations = append(violations, fmt.Sprintf("%s/%s has %d calls to %s, want %d", path, declaration.name, seen[name], name, want))
				}
			}
		}
	}
	return violations, nil
}

func TestUnretainedCleanupFixtureHasExactDeclarationsAndCallers(t *testing.T) {
	root := eventBoundaryRepositoryRoot(t)
	declarations := map[operatorSnapshotFixtureDeclaration]int{}
	err := checkoutsource.WalkDir(root, root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		violations, err := unretainedCleanupFileViolations(filepath.ToSlash(relative), raw, declarations)
		for _, violation := range violations {
			t.Error(violation)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for declaration := range unretainedCleanupFixtureChain {
		if declarations[declaration] != 1 {
			t.Errorf("fixture declaration %+v appears %d times, want 1", declaration, declarations[declaration])
		}
	}
}

func TestUnretainedCleanupCallerGuardRefusesProductionInitializersAndValues(t *testing.T) {
	for declaration, allowed := range unretainedCleanupFixtureChain {
		for target := range allowed {
			for _, path := range []string{"internal/serveapp/probe.go", unretainedDeliveryFixturePath, unretainedEventFixturePath, unretainedControlsPath} {
				for _, body := range []string{
					"func unlisted(){ fixture." + target + "() }",
					"var escape = fixture." + target,
					"var escape = func(){ fixture." + target + "() }()",
					"func " + declaration.name + "(){ use(fixture." + target + ") }",
					"func " + declaration.name + "(){ escape:=func(){ fixture." + target + "() }; use(escape) }",
				} {
					violations, err := unretainedCleanupFileViolations(path, []byte("package probe\n"+body), map[operatorSnapshotFixtureDeclaration]int{})
					if err != nil || len(violations) == 0 {
						t.Fatalf("fixture escape passed: %s: %s: %v", path, body, err)
					}
				}
			}
		}
	}
}

func classifiedUnretainedCleanupDeliverySQL(path string, file *ast.File) map[*ast.BasicLit]bool {
	classified := map[*ast.BasicLit]bool{}
	if path != unretainedDeliveryFixturePath {
		return classified
	}
	for _, node := range file.Decls {
		fn, ok := node.(*ast.FuncDecl)
		if !ok {
			continue
		}
		declaration := operatorSnapshotDeclaration(path, fn)
		want := map[operatorSnapshotFixtureDeclaration]int{
			{unretainedDeliveryFixturePath, "*Adapter", "installUnretainedCleanupAttempt"}: 3,
			{unretainedDeliveryFixturePath, "", "RequireEmptyUnretainedCleanupHistoryTx"}:  1,
			{unretainedDeliveryFixturePath, "", "ReadUnretainedCleanupHistoryTx"}:          1,
		}[declaration]
		if want == 0 {
			continue
		}
		literals := unretainedCleanupSQLLiterals(fn)
		if len(literals) != want {
			continue
		}
		for _, literal := range literals {
			classified[literal] = true
		}
	}
	return classified
}

func unretainedCleanupSQLLiterals(fn *ast.FuncDecl) []*ast.BasicLit {
	var literals []*ast.BasicLit
	ast.Inspect(fn, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if ok && literal.Kind == token.STRING {
			query, err := strconv.Unquote(literal.Value)
			if err == nil && executableDeliverySQL.MatchString(query) {
				literals = append(literals, literal)
			}
		}
		return true
	})
	return literals
}

func TestUnretainedCleanupGuardRefusesDeclarationAndSQLScopeExpansion(t *testing.T) {
	for _, body := range []string{
		"func (x *Foreign) installUnretainedCleanupAttempt(){}",
		"func InstallUnretainedSelectedForkCleanupHistoryForTest(){}",
	} {
		violations, err := unretainedCleanupFileViolations(unretainedDeliveryFixturePath, []byte("package probe\n"+body), map[operatorSnapshotFixtureDeclaration]int{})
		if err != nil || len(violations) == 0 {
			t.Fatalf("foreign fixture declaration passed: %s: %v", body, err)
		}
	}
	for _, body := range []string{
		"func unlisted(){ _=`SELECT * FROM event_deliveries` }",
		"func InstallUnretainedCleanupHistoryTx(){ _=`SELECT * FROM event_deliveries` }",
		"func (x *Foreign) installUnretainedCleanupAttempt(){ _=`UPDATE event_deliveries SET status='delivered'` }",
		"func RequireEmptyUnretainedCleanupHistoryTx(){ _=`SELECT * FROM event_deliveries`; _=`SELECT * FROM event_delivery_attempts` }",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), unretainedDeliveryFixturePath, "package probe\n"+body, parser.AllErrors)
		if err != nil || len(classifiedUnretainedCleanupDeliverySQL(unretainedDeliveryFixturePath, file)) != 0 {
			t.Fatalf("foreign or expanded fixture SQL passed: %s: %v", body, err)
		}
	}
}
