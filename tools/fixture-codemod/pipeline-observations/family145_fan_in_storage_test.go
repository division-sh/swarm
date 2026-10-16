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
