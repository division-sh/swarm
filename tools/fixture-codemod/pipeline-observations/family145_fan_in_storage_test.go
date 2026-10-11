package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var family145FanInHelperCalls = map[string]string{
	"removeFanInBarrierSelectedOwner":  "storetest.DeleteFanInSelectedPeriodOwner(ctx, backend, runID, entityID)",
	"requireA2NestedPeriodReadback":    "storetest.ReadFanInPeriodReportedEventIDs(ctx, backend)",
	"requireFanInBarrierReportTargets": "storetest.ReadFanInOperatingReportedEventIDs(ctx, backend)",
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
