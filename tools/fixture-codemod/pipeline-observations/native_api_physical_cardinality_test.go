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
