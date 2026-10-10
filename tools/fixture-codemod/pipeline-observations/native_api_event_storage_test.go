package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func nativeAPIEventRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var result []recipe
	for _, row := range rows {
		switch row.Family {
		case "native-api-event-count-consumer", "native-api-event-name-cardinality", "native-api-publication-refusal-evidence", "native-api-replay-persistence-evidence":
			result = append(result, row)
		}
	}
	if len(result) != 30 {
		t.Fatalf("API event evidence recipes=%d, want30", len(result))
	}
	return result
}

func nativeAPIEventConsumerWorkload(t *testing.T, source string) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		if formattedNativeReadNode(call.Fun) == "countEventDeliveries" {
			call.Fun = ast.NewIdent("countEventDeliveriesForEvent")
			background := &ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent("context"), Sel: ast.NewIdent("Background")}}
			call.Args = append([]ast.Expr{call.Args[0], background}, call.Args[1:]...)
		}
		switch formattedNativeReadNode(call.Fun) {
		case "countEventDeliveriesForEvent", "countPipelineReceiptsForEvent", "countOperatorReplayEvents", "loadPipelineReceiptOutcomeAndFailure":
			if len(call.Args) >= 3 {
				switch formattedNativeReadNode(call.Args[2]) {
				case "db", "pg", "f.db", "f.store":
					call.Args[2] = ast.NewIdent("originalSelectedOwner")
				}
			}
		case "countEventsByName", "assertNoEventPublishPersistence", "assertReplayPersistence", "assertAgentReplayPersistence", "countAPIIdempotencyRows", "countAllRunRows", "countAllEventRows", "assertNoFlowScopedEventPublishPersistence", "assertNoRunStartPersistence", "countRunRowsByID", "countEventRowsByRunID", "countDirectiveEvents", "countAllEventDeliveries", "latestEventIDByName":
			switch formattedNativeReadNode(call.Args[1]) {
			case "db", "pg", "fixture.db", "fixture.pg", "f.db", "f.store", "sqliteStore", "selected":
				call.Args[1] = ast.NewIdent("originalSelectedOwner")
			}
		}
		return true
	})
	return nativeAPIConstructionSource(t, formattedNativeReadNode(fn))
}

func nativeAPIEventHelperAssertions(t *testing.T, source string) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	var assertions []string
	for _, statement := range fn.Body.List {
		check, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		text := formattedNativeReadNode(check)
		if strings.Contains(text, "db.QueryRow(") || (check.Init == nil && formattedNativeReadNode(check.Cond) == "err != nil") {
			continue
		}
		ast.Inspect(check, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var field string
			switch formattedNativeReadNode(call.Fun) {
			case "countAllRunRows":
				field = "Runs"
			case "countAPIIdempotencyRows":
				field = "APICompletions"
			case "countAllEventRows", "countEventRowsByRunID":
				field = "Events"
			case "countRunRowsByID":
				field = "Runs"
			case "countEventsByName":
				if formattedNativeReadNode(call.Args[2]) == `"scan.requested"` {
					field = "MatchingEvents"
				} else if formattedNativeReadNode(call.Args[2]) == `"event.replayed"` {
					field = "AuditEventCount"
				}
			case "countEventDeliveries":
				switch formattedNativeReadNode(call.Args[2]) {
				case "replayEventID":
					field = "ReplayAgentDeliveries"
				case "originalEventID":
					field = "OriginalAgentDeliveries"
				}
			}
			if field != "" {
				call.Fun = ast.NewIdent("observed_" + field)
				call.Args = nil
			}
			return true
		})
		// Normalize detached field reads to the same independent physical witness.
		ast.Inspect(check, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && formattedNativeReadNode(selector.X) == "evidence" {
				selector.X = ast.NewIdent("observed")
			}
			return true
		})
		text = formattedNativeReadNode(check)
		for _, field := range []string{"Runs", "Events", "APICompletions", "MatchingEvents", "AuditEventCount", "ReplayAgentDeliveries", "OriginalAgentDeliveries"} {
			text = strings.ReplaceAll(text, "observed_"+field+"()", "observed."+field)
		}
		assertions = append(assertions, text)
	}
	return strings.Join(assertions, "\n")
}

func TestNativeAPIEventEvidenceRetainsAllWorkloadsAssertionsAndOwnerBindings(t *testing.T) {
	for _, row := range nativeAPIEventRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeAPIEventConsumerWorkload(t, row.Before) != nativeAPIEventConsumerWorkload(t, row.After) {
				t.Fatalf("changed source/setup/API/workload/assertions: %s", row.Function)
			}
			continue
		}
		if row.Function == "countEventsByName" {
			want := `func countEventsByName(t *testing.T, selected any, eventName string) int {
    t.Helper()
    count,err:=storetest.CountEventNameStorage(context.Background(),selected,eventName)
    if err!=nil {t.Fatalf("count events %s: %v",eventName,err)}
    return count
   }`
			if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
				t.Fatal("event-name count key, context, owner or refusal changed")
			}
			continue
		}
		if nativeAPIEventHelperAssertions(t, row.Before) != nativeAPIEventHelperAssertions(t, row.After) {
			t.Fatalf("publication/replay assertion lost: %s", row.Function)
		}
		var bindings []string
		if row.Function == "assertNoEventPublishPersistence" {
			bindings = []string{`storetest.ReadAPIEventPublicationRefusalStorage(context.Background(), selected, "scan.requested")`}
		} else {
			bindings = []string{"storetest.ReadAPIEventReplayStorage(context.Background(), selected, originalEventID, replayEventID, auditEventID)", "sourceEventID, payloadRaw := evidence.ReplaySourceEventID, evidence.AuditPayload", "sourceEventID = evidence.AuditSourceEventID"}
		}
		for _, binding := range bindings {
			if !strings.Contains(formattedNativeReadNode(projectionShapeFunction(t, row.After)), binding) {
				t.Fatalf("physical coordinates/binding lost: %s: %s", row.Function, binding)
			}
		}
	}
}

func TestNativeAPIEventEvidenceRejectsLostAssertionsWrongOwnersAndInput(t *testing.T) {
	for _, row := range nativeAPIEventRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			changed := strings.Replace(row.After, "countEventsByName(t, pg,", "countEventsByName(t, anotherOwner,", 1)
			if changed != row.After && nativeAPIEventConsumerWorkload(t, row.Before) == nativeAPIEventConsumerWorkload(t, changed) {
				t.Fatalf("wrong selected owner admitted: %s", row.Function)
			}
		} else if row.Function != "countEventsByName" {
			changed := strings.Replace(row.After, " != ", " == ", 1)
			if changed == row.After || nativeAPIEventHelperAssertions(t, row.Before) == nativeAPIEventHelperAssertions(t, changed) {
				t.Fatalf("weakened physical assertions admitted: %s", row.Function)
			}
		}
	}
}

func TestNativeAPIEventEvidenceCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]map[string]int{}
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := formattedNativeReadNode(call.Fun)
				switch name {
				case "countEventsByName", "assertNoEventPublishPersistence", "assertReplayPersistence", "assertAgentReplayPersistence":
					if actual[fn.Name.Name] == nil {
						actual[fn.Name.Name] = map[string]int{}
					}
					actual[fn.Name.Name][name]++
					if len(call.Args) < 2 || (formattedNativeReadNode(call.Args[1]) != "pg" && formattedNativeReadNode(call.Args[1]) != "fixture.pg" && formattedNativeReadNode(call.Args[1]) != "sqliteStore" && formattedNativeReadNode(call.Args[1]) != "selected") {
						t.Fatalf("raw/foreign counter owner: %s", fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	expected := map[string]map[string]int{
		"TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock":            {"countEventsByName": 3},
		"TestOperatorEventPublishSQLitePayloadFailureLeavesNoIdempotencyCompletionOrRows":   {"countEventsByName": 1},
		"TestOperatorEventPublishSQLiteRejectsPrivateOrdinaryFlowEndpoint":                  {"countEventsByName": 1},
		"TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun":                  {"countEventsByName": 1},
		"TestOperatorRuntimeControlHandlersUseIngressOwnerAndIdempotency":                   {"countEventsByName": 4},
		"TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency":            {"countEventsByName": 2, "assertReplayPersistence": 1},
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":               {"countEventsByName": 4},
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError":            {"countEventsByName": 4},
		"TestOperatorEventReplaySubsetAndFailClosedCases":                                   {"countEventsByName": 6},
		"TestOperatorAgentReplayProjectsSingletonEventReplayOwner":                          {"countEventsByName": 1, "assertAgentReplayPersistence": 1},
		"TestOperatorAgentReplayFailClosedCases":                                            {"countEventsByName": 2},
		"TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency":                  {"countEventsByName": 4},
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                           {"countEventsByName": 1},
		"TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces":                 {"countEventsByName": 1},
		"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency":  {"countEventsByName": 3},
		"TestOperatorEventPublishRootEventNameWinsOverFlowLeafAliases":                      {"countEventsByName": 2},
		"TestOperatorEventPublishHandlersRequireCanonicalBundleHashForCreateNewWork":        {"assertNoEventPublishPersistence": 1},
		"TestOperatorEventPublishPostgresUsesPublisherScopeWithPlainRequestContext":         {"countEventsByName": 1},
		"TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback":          {"countEventsByName": 2},
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":           {"countEventsByName": 2},
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":        {"countEventsByName": 2},
		"TestOperatorEventPublishPreCommitFailureFailsClosedWithDeclaredError":              {"assertNoEventPublishPersistence": 1},
		"TestOperatorEventPublishIsUnavailableWithoutDurableAckPublisher":                   {"assertNoEventPublishPersistence": 1},
		"TestOperatorEventPublishExplicitRunTargetRequiresExistingNonterminalRun":           {"countEventsByName": 3},
		"TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance":               {"countEventsByName": 1},
		"TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence": {"countEventsByName": 1},
		"TestOperatorEventPublishHandlersFailClosedBeforePersistence":                       {"assertNoEventPublishPersistence": 6},
		"TestOperatorRuntimeContextManagerRoutesCreateNewWorkToSelectedBundle":              {"countEventsByName": 2},
		"TestOperatorEventPublishIdempotencyReplayDoesNotRequireLoadedRuntimeContext":       {"countEventsByName": 1},
		"TestOperatorRuntimeContextManagerRoutesEventReplayByOriginalRunBundle":             {"assertReplayPersistence": 1},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("API consumer inventory changed: got=%v want=%v", actual, expected)
	}
}
