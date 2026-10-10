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
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

//go:embed family144-transitions.json
var runSetupTransitionBytes []byte

type runSetupTransition struct {
	File, Function, Before, After string
	Removed                       bool
}

func runSetupTransitions(t *testing.T) []runSetupTransition {
	t.Helper()
	var rows []runSetupTransition
	if err := json.Unmarshal(runSetupTransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rows)
	if err != nil || len(rows) != 149 || fmt.Sprintf("%x", sha256.Sum256(raw)) != "b12bb6d4cffc0c685be0a6f53f12a0077d4880ad541c4693818d2c03d466d168" {
		t.Fatalf("finite run-setup transformation changed: %d/%v", len(rows), err)
	}
	return rows
}

func TestNativeRunSetupWholeFamilySnapshotsAndHostility(t *testing.T) {
	var recipes []recipe
	if err := json.Unmarshal(recipeBytes, &recipes); err != nil {
		t.Fatal(err)
	}
	current, changes, err := prepareFiles("../../..", recipes)
	if err != nil || len(current) != 0 || len(changes) != 0 {
		t.Fatalf("whole current source is not inert: %v", err)
	}
	for _, row := range runSetupTransitions(t) {
		if row.Removed {
			continue
		}
		before := []byte("package proof\n" + row.Before)
		finite := recipe{File: row.File, Function: row.Function, Before: row.Before, After: row.After}
		after, changed, err := rewriteFunction(row.File, before, finite)
		if err != nil || !changed {
			t.Fatalf("finite rewrite failed: %s/%s/%v", row.File, row.Function, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, finite); err != nil || changed {
			t.Fatalf("repeat changed native source: %s/%v", row.Function, err)
		}
		hostile := strings.Replace(row.Before, "{", "{ unreviewedAuthority();", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatalf("unreviewed source accepted: %s", row.Function)
		}
	}
}

// These are the close-reviewed owner/schema/temporal repairs, not mechanical
// callers. Their assertions are independently exercised by the named receipts.
func semanticRunSetupFile(path string) bool {
	switch path {
	case "internal/testutil/runlifecyclefixture/fixture.go",
		"internal/runtime/runtime_shutdown_admission_test.go", "internal/runtime/runtime_shutdown_fan_out_test.go",
		"internal/runtime/runtime_log_native_external_test.go", "internal/runtime/author_activity_test_context_test.go",
		"internal/runtime/runtime_startup_readiness_test.go",
		"internal/runtime/manager/delivery_native_owner_external_test.go", "internal/runtime/manager/delivery_native_selected_external_test.go",
		"internal/runtime/bus/eventbus_agent_route_test.go", "internal/runtime/bus/source_artifact_mutation_roots_test.go",
		"internal/store/internal/backend/eventrecord/sqlite/single_event_reader_test.go",
		"internal/store/internal/backend/llmpersistence/postgres_exact_coordinates_test.go",
		"internal/store/internal/backend/runforkpersistence/run_fork_writer_settlement_test.go",
		"internal/store/internal/backend/pipelinepersistence/fan_out_owner_test.go",
		"internal/store/internal/backend/pipelinepersistence/fan_out_read_surface_test.go",
		"internal/store/internal/backend/pipelinepersistence/flow_instance_route_statements_test.go",
		"internal/store/internal/backend/pipelinepersistence/flow_instance_route_postgres_statements_native_test.go":
		return true
	}
	return false
}

func semanticRunSetupHelper(row runSetupTransition) bool {
	switch row.File + "/" + row.Function {
	case "internal/runtime/runforkexecution/operation_lifetime_parity_test.go/seedSelectedOperationSource",
		"internal/runtime/runforkexecution/runtime_outcome_test.go/seedSelectedRuntimeOutcomeSource",
		"internal/runtime/tools/executor_sqlite_persistence_test.go/seedReplyToolContext":
		return true
	}
	return false
}

func runSetupAssertions(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var result []string
	ast.Inspect(file, func(node ast.Node) bool {
		if statement, ok := node.(*ast.IfStmt); ok && runSetupOldCall(statement.Init) {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		member, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, ok := member.X.(*ast.Ident)
		if !ok || receiver.Name != "t" {
			return true
		}
		switch member.Sel.Name {
		case "Fatal", "Fatalf", "Error", "Errorf", "Fail", "FailNow":
			result = append(result, formattedNativeReadNode(call))
		}
		return true
	})
	return result, nil
}
func runSetupOldCall(node ast.Node) bool {
	if node == nil {
		return false
	}
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		member, ok := n.(*ast.SelectorExpr)
		if ok {
			id, ok := member.X.(*ast.Ident)
			if ok && id.Name == "runlifecyclefixture" {
				found = true
			}
		}
		return true
	})
	return found
}
func TestNativeRunSetupMechanicalCallersRetainBehavioralAssertions(t *testing.T) {
	checked := 0
	for _, row := range runSetupTransitions(t) {
		if row.Removed || semanticRunSetupFile(row.File) || semanticRunSetupHelper(row) {
			continue
		}
		before, err := runSetupAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		after, err := runSetupAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(before, "\n") != strings.Join(after, "\n") {
			t.Errorf("mechanical caller changed behavioral assertions: %s/%s\nbefore=%v\nafter=%v", row.File, row.Function, before, after)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("empty mechanical caller proof")
	}
}

func runSetupEscapes(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	aliases := map[string]bool{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		if path != "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture" {
			continue
		}
		alias := "runlifecyclefixture"
		if imp.Name != nil {
			alias = imp.Name.Name
		}
		if alias == "." {
			return []string{"dot run fixture import"}, nil
		}
		aliases[alias] = true
	}
	retired := map[string]bool{}
	for _, name := range strings.Fields("Fixture RequirePostgres RequireSQLite Materialize CreateSQLiteScenarioSchema CreatePostgresScenarioSchema TransitionActive PostgresCreateRunInMutation PostgresSyncCountersInMutation") {
		retired[name] = true
	}
	var escapes []string
	ast.Inspect(file, func(node ast.Node) bool {
		member, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := member.X.(*ast.Ident)
		if ok && aliases[id.Name] && retired[member.Sel.Name] {
			escapes = append(escapes, member.Sel.Name)
		}
		return true
	})
	return escapes, nil
}

func TestNativeRunSetupInterpreterAndConsumerCapabilityStayRetired(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, failure error) error {
		if failure != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return failure
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		escapes, err := runSetupEscapes(data)
		if len(escapes) != 0 {
			t.Errorf("retired run construction remains: %s/%v", path, escapes)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []string{
		"import fixture \"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture\"; func probe(){fixture.Materialize()}",
		"import fixture \"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture\"; func probe(){fixture.CreateSQLiteScenarioSchema()}",
		"import fixture \"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture\"; var x=func(){fixture.PostgresCreateRunInMutation()}()",
		"import . \"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture\"; func probe(){Materialize()}",
	} {
		if escapes, err := runSetupEscapes([]byte("package hostile\n" + probe)); err != nil || len(escapes) == 0 {
			t.Fatalf("restored run escape accepted: %s/%v", probe, err)
		}
	}
	if _, err := runSetupEscapes([]byte("package broken\nfunc")); err == nil {
		t.Fatal("broken owned source accepted")
	}
}
