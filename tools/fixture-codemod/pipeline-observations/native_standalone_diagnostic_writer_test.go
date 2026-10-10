package main

import (
	"encoding/json"
	"go/ast"
	"strings"
	"testing"
)

func TestNativeStandaloneDiagnosticWriterAndAllExternalCallsAreClosed(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched, callers := 0, 0
	for _, row := range rows {
		if row.Family != "native-standalone-diagnostic-writer" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		want, err := canonicalFunction(current)
		got, sourceErr := canonicalFunction(actual)
		if err != nil || sourceErr != nil || want != got {
			t.Fatalf("diagnostic source differs: %s", row.Function)
		}
		if row.Function != "InsertDiagnosticDirectEventRecord" {
			before := strings.ReplaceAll(row.Before, "storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, dialect,", "storetest.InsertDiagnosticDirectEventRecord(t, ctx, selected,")
			before = strings.ReplaceAll(before, "uuid.NewString(), \"runtime\",", "uuid.NewString(),")
			before = nativeObservabilityConsumerSignature(before)
			value, err := canonicalFunction(before)
			if err != nil || value != want {
				t.Fatalf("diagnostic payload/clock/API workload drift: %s", row.Function)
			}
			ast.Inspect(projectionShapeFunction(t, actual), func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok && formattedNativeReadNode(call.Fun) == "storetest.InsertDiagnosticDirectEventRecord" {
					callers++
					if len(call.Args) != 6 || formattedNativeReadNode(call.Args[2]) != "selected" {
						t.Fatal("diagnostic caller forwarded raw or foreign authority")
					}
				}
				return true
			})
			continue
		}
		for _, raw := range []string{"*sql.DB", "Dialect", "runCanonicalEventMutation", "mutationprotocol.", "eventfixture.DiagnosticDirect", "CommitSemanticEvent(", "AdmitForPersistence", "CommitRuntimeLogEvent"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("diagnostic helper escaped named writer: %s", raw)
			}
		}
		for _, cut := range []string{"eventtest.DiagnosticDirect(eventID, events.EventTypePlatformRuntimeLog, \"runtime\", \"\"", "events.EventEnvelope{Scope: events.EventScopeGlobal}, createdAt", "eventfixture.BindPayload(event)", "bound.PayloadAdmission()", "var writer runtimepkg.RuntimeLogPersistence", "case *private.PostgresStore:", "case *private.SQLiteRuntimeStore:", "if owner != nil", "if writer == nil", "writer.PersistRuntimeLog(ctx, runtimepkg.RuntimeLogPersistenceRecord{", "EventID: eventID, Payload: payload, PayloadAdmission: admission, CreatedAt: createdAt, ExecutionMode: bound.ExecutionMode()", "t.Fatalf("} {
			if !strings.Contains(actual, cut) {
				t.Fatalf("diagnostic writer lost %s", cut)
			}
			mutant := strings.Replace(current, cut, "unreviewedDiagnosticCut", 1)
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("diagnostic mutation accepted: %s", cut)
			}
		}
	}
	if matched != 3 || callers != 4 {
		t.Fatalf("diagnostic recipes/calls=%d/%d,want3/4", matched, callers)
	}
}
