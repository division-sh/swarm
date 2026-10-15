package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ActivityReadChainProofError(name, before, after string) error {
	if name == "runSelectedContractActivityForkCases" {
		if !strings.Contains(after, "selected := fixture.owner.ports.fork") || strings.Contains(after, "QueryRowContext") ||
			!strings.Contains(after, "if sourceBefore.AttemptStatus != tt.sourceAttemptStatus") {
			return fmt.Errorf("activity cases lost original owner or exact positive journal-status witness")
		}
		beforeAssertions, beforeErr := runSetupAssertions(before)
		afterAssertions, afterErr := runSetupAssertions(after)
		var kept []string
		var added int
		for _, call := range afterAssertions {
			if call == `t.Fatalf("source journal observation status=%q want=%q", sourceBefore.AttemptStatus, tt.sourceAttemptStatus)` {
				added++
			} else {
				kept = append(kept, call)
			}
		}
		if beforeErr != nil || afterErr != nil || added != 1 || strings.Join(beforeAssertions, "\n") != strings.Join(kept, "\n") {
			return fmt.Errorf("activity cases changed an original assertion or lost their added exact status control")
		}
		return nil
	}
	for _, raw := range []string{"activityForkFixtureDB", "QueryRowContext", "Scan(", "Database(", "ReadCanonicalEvent"} {
		if strings.Contains(after, raw) {
			return fmt.Errorf("activity read helper retained %s or substituted a decoded event", raw)
		}
	}
	if name == "readActivityForkSourceEvidence" {
		want := `func readActivityForkSourceEvidence(t *testing.T, selected any, runID, entityID, requestID string) activityForkSourceEvidence {
	t.Helper()
	out, err := storetest.ReadActivityForkSourceStorage(context.Background(), selected, runID, entityID, requestID)
	if err != nil { t.Fatal(err) }
	return out
}`
		actual, err := canonicalFunction(after)
		expected, wantErr := canonicalFunction(want)
		if err != nil || wantErr != nil || actual != expected {
			return fmt.Errorf("source storage helper changed original identity, cuts or returned evidence")
		}
		return nil
	}
	wantCalls := map[string]string{
		"loadSelectedContractActivityRequest":       "storetest.ReadActivityForkRequestPayloadStorage(context.Background(), selected, runID, eventID)",
		"assertSelectedContractForkActivityAttempt": "storetest.ReadActivityForkAttemptStorage(context.Background(), selected, requestEventID)",
		"loadSelectedContractActivityResult":        "storetest.ReadActivityForkResultStorage(context.Background(), selected, runID, eventID)",
	}
	if wantCalls[name] == "" || !strings.Contains(after, wantCalls[name]) {
		return fmt.Errorf("activity read helper lost exact selected coordinates: %s", name)
	}
	beforeAssertions, beforeErr := runSetupAssertions(before)
	afterAssertions, afterErr := runSetupAssertions(after)
	if beforeErr != nil || afterErr != nil || strings.Join(beforeAssertions, "\n") != strings.Join(afterAssertions, "\n") {
		return fmt.Errorf("activity read helper lost an original decode, type, identity or failure assertion: %s", name)
	}
	return nil
}

func TestFamily145ActivityReadChainRetiresRawReadersAndKeepsCompleteCallers(t *testing.T) {
	path := "../../../internal/runtime/runforkexecution/activity_fork_execution_test.go"
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "QueryRowContext") || strings.Contains(string(source), "QueryContext") {
		t.Fatal("activity fork read family retained a raw SQL reader or dialect delegate")
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	wants := map[string]int{
		"readActivityForkSourceEvidence": 3, "loadSelectedContractActivityRequest": 2,
		"assertSelectedContractForkActivityAttempt": 1, "loadSelectedContractActivityResult": 1, "CountActivityForkAttemptStorage": 2,
	}
	counts := map[string]int{}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch called := call.Fun.(type) {
		case *ast.Ident:
			name = called.Name
		case *ast.SelectorExpr:
			name = called.Sel.Name
		}
		if wants[name] == 0 {
			return true
		}
		counts[name]++
		if len(call.Args) <= 1 || family145NodeString(call.Args[1]) != "selected" {
			t.Fatalf("%s caller lost original selected ownership", name)
		}
		return true
	})
	for name, want := range wants {
		if counts[name] != want {
			t.Fatalf("%s caller inventory=%d want%d", name, counts[name], want)
		}
	}
}

func TestFamily145ActivityReadChainHostileCoordinatesAndAssertionsFailClosed(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var covered int
	for _, row := range rows {
		if !family145ActivityReadOwner(row.Function) {
			continue
		}
		covered++
		after := row.After
		if row.Successor != "" {
			after = row.Successor
		}
		if err := family145ActivityReadChainProofError(row.Function, row.Before, after); err != nil {
			t.Fatal(err)
		}
		if err := family145ActivityReadChainProofError(row.Function, row.Before, strings.ReplaceAll(after, "selected", "unreviewedOwner")); err == nil {
			t.Fatal("foreign read owner accepted")
		}
		if row.Function == "readActivityForkSourceEvidence" {
			if err := family145ActivityReadChainProofError(row.Function, row.Before, strings.ReplaceAll(after, "requestID", "foreignRequest")); err == nil {
				t.Fatal("foreign request identity accepted")
			}
		} else {
			assertions, err := runSetupAssertions(after)
			if err != nil || len(assertions) == 0 {
				t.Fatal("missing business assertions")
			}
			if err := family145ActivityReadChainProofError(row.Function, row.Before, strings.Replace(after, assertions[len(assertions)-1], "t.Log(\"lost assertion\")", 1)); err == nil {
				t.Fatal("lost behavior assertion accepted")
			}
		}
	}
	if covered != 5 {
		t.Fatalf("activity read owner inventory=%d want5", covered)
	}
}

func family145ActivityReadOwner(name string) bool {
	switch name {
	case "runSelectedContractActivityForkCases", "readActivityForkSourceEvidence", "loadSelectedContractActivityRequest", "assertSelectedContractForkActivityAttempt", "loadSelectedContractActivityResult":
		return true
	}
	return false
}
