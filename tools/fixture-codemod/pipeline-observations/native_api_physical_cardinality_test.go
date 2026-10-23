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

func nativeAPIPhysicalCardinalityRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{"TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency": true,
		"TestOperatorRuntimeContextManagerFailsClosedForUnloadedBundle":                           true,
		"TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle":                        true,
		"TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency":                        true,
		"TestOperatorTestSetupRejectsContractInvalidEntities":                                     true,
		"testRuntimeNukeLayeredPostgresCapacity":                                                  true,
		"TestAdministrativeOperationCancellationReleasesLeaseAndCapacity":                         true,
		"TestOperatorAgentControlHandlersUseCanonicalOwnerAndIdempotency":                         true,
		"TestOperatorAgentSendDirectiveRunTargetErrors":                                           true,
		"TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency":                        true,
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                                 true,
		"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency":        true,
		"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes":              true,
		"TestOperatorEventPublishRejectsPrivateFlowDespiteLiveRecipient":                          true,
		"TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed":                           true,
		"TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback":                true,
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":                 true,
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":              true,
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence":           true,
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication":                         true,
		"TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication": true,
		"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence":     true,
		"TestOperatorEventPublishExplicitRunIsUnavailableWithoutRecipientPlanChecker":             true,
		"TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence":           true,
		"TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance":                     true,
		"TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence":       true,
		"TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency":                  true,
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                             true,
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":                     true,
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError":                  true,
		"TestOperatorEventReplaySubsetAndFailClosedCases":                                         true,
		"TestOperatorAgentReplayProjectsSingletonEventReplayOwner":                                true,
		"TestOperatorRunStartHandlersRequireBundleScopeForCreateNewWorkWithoutActiveRuntimeFact":  true,
		"TestOperatorRunStartRejectsFlowScopedEventName":                                          true,
		"countAPIIdempotencyRows":                   true,
		"countAllRunRows":                           true,
		"countAllEventRows":                         true,
		"assertNoFlowScopedEventPublishPersistence": true,
		"assertNoRunStartPersistence":               true}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range all {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 39 {
		t.Fatalf("physical count recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}
func nativeAPIPhysicalCardinalityWorkload(t *testing.T, source string) string {
	t.Helper()
	if strings.HasPrefix(source, "func TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency(") {
		source = nativePublicRunControlSource(source)
	}
	source = strings.Replace(source, "\n\t\tdb    *sql.DB", "", 1)
	source = strings.Replace(source, "fixture{store: selected, db: storetest.DatabaseForTest(selected)}", "fixture{store: selected}", 1)
	source = strings.Replace(source, "_, db, _ := testutil.StartPostgres(t)\n\t\t\t\treturn fixture{store: storetest.AdmitPostgresRuntimeStore(t, db), db: db}", "selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)\n\t\t\t\treturn fixture{store: selected}", 1)
	source = strings.Replace(source, "\n\tdb := storetest.Database(sqliteStore)", "", 1)
	return nativeAPIEventConsumerWorkload(t, source)
}

func TestNativeAPIPhysicalCardinalityRejectsLostAssertionsOrWrongOwner(t *testing.T) {
	for _, row := range nativeAPIPhysicalCardinalityRecipes(t) {
		if row.Function == "assertNoRunStartPersistence" || row.Function == "assertNoFlowScopedEventPublishPersistence" {
			broken := strings.Replace(row.After, " != ", " == ", 1)
			if broken == row.After || nativeAPIEventHelperAssertions(t, row.Before) == nativeAPIEventHelperAssertions(t, broken) {
				t.Fatalf("weakened refusal assertion admitted: %s", row.Function)
			}
		} else if strings.HasPrefix(row.Function, "Test") {
			broken := strings.Replace(row.After, "(t, pg)", "(t, wrongOwner)", 1)
			if broken != row.After && nativeAPIPhysicalCardinalityWorkload(t, row.Before) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("wrong physical owner admitted: %s", row.Function)
			}
		}
	}
}
func TestNativeAPIPhysicalCardinalityCallerInventoryIsComplete(t *testing.T) {
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
				case "countAPIIdempotencyRows", "countAllRunRows", "countAllEventRows", "assertNoFlowScopedEventPublishPersistence", "assertNoRunStartPersistence":
					if actual[fn.Name.Name] == nil {
						actual[fn.Name.Name] = map[string]int{}
					}
					actual[fn.Name.Name][name]++
					switch formattedNativeReadNode(call.Args[1]) {
					case "pg", "fixture.pg", "f.store", "sqliteStore", "selected":
					default:
						t.Fatalf("raw/foreign physical-count owner: %s", fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	expected := map[string]map[string]int{
		"TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock":                  {"countAPIIdempotencyRows": 2},
		"TestOperatorEventPublishSQLitePayloadFailureLeavesNoIdempotencyCompletionOrRows":         {"countAPIIdempotencyRows": 1, "countAllRunRows": 1},
		"TestOperatorEventPublishSQLiteRejectsPrivateOrdinaryFlowEndpoint":                        {"countAPIIdempotencyRows": 1, "countAllRunRows": 1},
		"TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun":                        {"countAllRunRows": 1},
		"TestOperatorEventPublishSQLiteRejectsCallerEntityIDForCreateEntityBeforePersistence":     {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency":                           {"countAPIIdempotencyRows": 1},
		"TestOperatorRuntimeContextManagerFailsClosedForUnloadedBundle":                           {"countAllRunRows": 1},
		"TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle":                        {"countAllRunRows": 1},
		"TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency":                        {"countAPIIdempotencyRows": 3},
		"TestOperatorTestSetupRejectsContractInvalidEntities":                                     {"countAPIIdempotencyRows": 1},
		"testRuntimeNukeLayeredPostgresCapacity":                                                  {"countAPIIdempotencyRows": 1},
		"TestAdministrativeOperationCancellationReleasesLeaseAndCapacity":                         {"countAPIIdempotencyRows": 1},
		"TestOperatorAgentControlHandlersUseCanonicalOwnerAndIdempotency":                         {"countAPIIdempotencyRows": 1},
		"TestOperatorAgentSendDirectiveRunTargetErrors":                                           {"countAPIIdempotencyRows": 1},
		"TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency":                        {"countAPIIdempotencyRows": 1},
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                                 {"countAPIIdempotencyRows": 2, "countAllRunRows": 1, "assertNoRunStartPersistence": 9},
		"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency":        {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes":              {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishRejectsPrivateFlowDespiteLiveRecipient":                          {"countAPIIdempotencyRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed":                           {"assertNoFlowScopedEventPublishPersistence": 2},
		"TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback":                {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":                 {"countAPIIdempotencyRows": 2},
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":              {"countAPIIdempotencyRows": 2},
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence":           {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication":                         {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishMissingTemplateInputFailsClosedBeforeLowerPrecedencePublication": {"countAPIIdempotencyRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence":     {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishExplicitRunIsUnavailableWithoutRecipientPlanChecker":             {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence":           {"countAPIIdempotencyRows": 1, "countAllRunRows": 1, "countAllEventRows": 1},
		"TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance":                     {"countAPIIdempotencyRows": 1},
		"TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence":       {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency":                  {"countAPIIdempotencyRows": 1},
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                             {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":                     {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError":                  {"countAPIIdempotencyRows": 1},
		"TestOperatorEventReplaySubsetAndFailClosedCases":                                         {"countAPIIdempotencyRows": 2},
		"TestOperatorAgentReplayProjectsSingletonEventReplayOwner":                                {"countAPIIdempotencyRows": 1},
		"TestOperatorRunStartHandlersRequireBundleScopeForCreateNewWorkWithoutActiveRuntimeFact":  {"assertNoRunStartPersistence": 2},
		"TestOperatorRunStartRejectsFlowScopedEventName":                                          {"assertNoRunStartPersistence": 1}}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("physical count callers differ: got=%v want=%v", actual, expected)
	}
}
