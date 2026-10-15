package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

var family145FanInHelperCalls = map[string]string{
	"removeFanInBarrierSelectedOwner":  "storetest.DeleteFanInSelectedPeriodOwner(ctx, backend, runID, entityID)",
	"requireA2NestedPeriodReadback":    "storetest.ReadFanInPeriodReportedEventIDs(ctx, backend)",
	"requireFanInBarrierReportTargets": "storetest.ReadFanInOperatingReportedEventIDs(ctx, backend)",
}

var family145SemanticReadHelperCalls = map[string][]string{
	"verifyNoAttemptRows": {"storetest.CountFanOutAttemptResidueStorage(ctx, p.selected, command)"},
	"waitIntent":          {"storetest.ReadFanOutTriggerDeliveryDiagnosticStorage(f.ctx, f.selected, f.runID, trigger)", "storetest.ReadLastThreeFanOutRuntimeLogPayloads(f.ctx, f.selected, f.runID)", "storetest.ReadFanOutSourceProgressStorage(f.ctx, f.selected, f.runID, trigger)"},
	"assertOutcomes":      {"storetest.ReadFanOutSourceOutcomesStorage(f.ctx, f.selected, f.runID, trigger)"},
}

func family145SemanticReadHelperProofError(function, beforeSource, afterSource string) error {
	for _, call := range family145SemanticReadHelperCalls[function] {
		if strings.Count(afterSource, call) != 1 {
			return fmt.Errorf("semantic fixture lost exact original snapshot coordinates: %s", function)
		}
	}
	if strings.Contains(afterSource, ".db") || strings.Contains(afterSource, "*sql.DB") {
		return fmt.Errorf("semantic fixture restored raw authority: %s", function)
	}
	before, beforeErr := semanticReadBusinessAssertions(beforeSource)
	after, afterErr := semanticReadBusinessAssertions(afterSource)
	if beforeErr != nil || afterErr != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
		return fmt.Errorf("semantic fixture changed a business assertion: %s/%v/%v", function, beforeErr, afterErr)
	}
	if function == "waitIntent" && !strings.Contains(afterSource, "err == nil && !progress.Present") {
		return fmt.Errorf("pending semantic intent lost typed absence or hides failed reads")
	}
	if function == "waitIntent" && !strings.Contains(afterSource, "return got == cursor && state == status, err") {
		return fmt.Errorf("semantic intent lost its exact progress/status/error predicate")
	}
	if function == "verifyNoAttemptRows" {
		for _, cut := range []string{"if leaked != 0 {", `return fmt.Errorf("native safe-aggregate rollback leaked %d attempted event/outcome rows", leaked)`, `return fmt.Errorf("verify native safe-aggregate rollback: %w", err)`} {
			if strings.Count(afterSource, cut) != 1 {
				return fmt.Errorf("native aggregate proof lost exact residue refusal: %s", cut)
			}
		}
	}
	return nil
}

func TestFamily145SemanticCarrierHelpersRejectForeignReadsAndLostBusinessProof(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, row := range rows {
		calls, ok := family145SemanticReadHelperCalls[row.Function]
		if !ok {
			continue
		}
		checked++
		if err := family145SemanticReadHelperProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, call := range calls {
			if err := family145SemanticReadHelperProofError(row.Function, row.Before, strings.Replace(row.After, call, "foreignSnapshot()", 1)); err == nil {
				t.Fatalf("changed source coordinates accepted: %s", row.Function)
			}
		}
		hostile := strings.Replace(row.After, "{", "{ f.db.QueryContext(ctx, rawSQL);", 1)
		if err := family145SemanticReadHelperProofError(row.Function, row.Before, hostile); err == nil {
			t.Fatalf("raw carrier returned: %s", row.Function)
		}
		switch row.Function {
		case "verifyNoAttemptRows":
			hostile = strings.Replace(row.After, "leaked != 0", "false", 1)
		case "waitIntent":
			hostile = strings.Replace(row.After, "err == nil && !progress.Present", "!progress.Present", 1)
		case "assertOutcomes":
			hostile = strings.Replace(row.After, "t.Fatalf(", "t.Logf(", 1)
		}
		if err := family145SemanticReadHelperProofError(row.Function, row.Before, hostile); err == nil {
			t.Fatalf("semantic residue/absence/assertion proof was weakened: %s", row.Function)
		}
	}
	if checked != 3 {
		t.Fatalf("semantic carrier helper census changed: %d", checked)
	}
}

func semanticReadBusinessAssertions(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "semantic.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var kept []ast.Stmt
		for _, stmt := range block.List {
			conditional, ok := stmt.(*ast.IfStmt)
			if ok && conditional.Init != nil {
				init := family145NodeString(conditional.Init)
				if strings.HasPrefix(init, "err := result.Scan(") || init == "err := result.Err()" {
					continue
				}
			}
			kept = append(kept, stmt)
		}
		block.List = kept
		return true
	})
	return runSetupAssertions(family145NodeString(file.Decls[0]))
}

func family145FanInHelperProofError(function, beforeSource, afterSource string) error {
	want, ok := family145FanInHelperCalls[function]
	if !ok || strings.Count(afterSource, want) != 1 || strings.Contains(afterSource, "db.") || strings.Contains(afterSource, "*sql.DB") {
		return fmt.Errorf("fan-in helper lost its exact native operation or restored raw authority: %s", function)
	}
	before, beforeErr := family145DiagnosticBusinessAssertions(beforeSource)
	after, afterErr := family145DiagnosticBusinessAssertions(afterSource)
	if beforeErr != nil || afterErr != nil {
		return fmt.Errorf("parse fan-in business assertions: %v/%v", beforeErr, afterErr)
	}
	var preserved []string
	for _, assertion := range before {
		// RowsAffected error is now returned by the same named deletion and
		// reaches the original remove-error assertion, not a second SQL loan.
		if function == "removeFanInBarrierSelectedOwner" && assertion == `t.Fatalf("inspect removed fan-in selected owner: %v", err)` {
			continue
		}
		preserved = append(preserved, assertion)
	}
	if strings.Join(preserved, "\n") != strings.Join(after, "\n") {
		return fmt.Errorf("fan-in helper changed business assertions: %s", function)
	}
	return nil
}

func TestFamily145FanInHelpersPreserveExactOwnerAndRejectWeakenedAssertions(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, row := range rows {
		call, ok := family145FanInHelperCalls[row.Function]
		if !ok {
			continue
		}
		checked++
		if err := family145FanInHelperProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, hostile := range []string{
			strings.Replace(row.After, call, "foreignReadOwner()", 1),
			strings.Replace(row.After, call, call+"; "+call, 1),
			strings.Replace(row.After, "t.Fatalf(", "t.Logf(", 1),
			strings.Replace(row.After, "{", "{ db.QueryContext(ctx, hostileSQL);", 1),
		} {
			if err := family145FanInHelperProofError(row.Function, row.Before, hostile); err == nil {
				t.Fatalf("changed fan-in scope, observer, assertion or raw authority accepted: %s", row.Function)
			}
		}
	}
	if checked != 3 {
		t.Fatalf("fan-in shared helper inventory changed: %d", checked)
	}
}
