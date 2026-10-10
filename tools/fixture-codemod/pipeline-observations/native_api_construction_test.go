package main

import (
	"encoding/json"
	"strings"
	"testing"
)

var nativeAPIConstructionRoots = map[string]bool{
	"TestOperatorRuntimeControlHandlersUseIngressOwnerAndIdempotency":                        true,
	"TestOperatorRunStartHandlersRequireBundleScopeForCreateNewWorkWithoutActiveRuntimeFact": true,
	"TestOperatorRunStartRejectsFlowScopedEventName":                                         true,
	"TestOperatorEventReplayPublishesDistinctReplayEventAuditAndIdempotency":                 true,
	"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":                    true,
	"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError":                 true,
	"TestOperatorAgentReplayProjectsSingletonEventReplayOwner":                               true,
	"TestOperatorEventPublishHandlersRequireCanonicalBundleHashForCreateNewWork":             true,
	"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":                true,
	"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":             true,
	"TestOperatorEventPublishIsUnavailableWithoutDurableAckPublisher":                        true,
	"TestOperatorEventPublishExplicitRunTargetRequiresExistingNonterminalRun":                true,
	"TestOperatorEventPublishOperatorReferenceRejectsInvalidReferenceBeforePersistence":      true,
	"TestOperatorEventPublishRejectsPrivateFlowDespiteLiveRecipient":                         true,
	"TestOperatorEventPublishExplicitRunIsUnavailableWithoutRecipientPlanChecker":            true,
	"TestOperatorEventPublishRejectsCallerEntityIDForCreateEntityBeforePersistence":          true,
	"TestOperatorEventPublishQueuesWhileRuntimePaused":                                       true,
}

func nativeAPIConstructionSource(t *testing.T, source string) string {
	t.Helper()
	if nativeNestedAPIConstructionRoots[projectionShapeFunction(t, source).Name.Name] != 0 {
		return nativeNestedAPIConstructionSource(t, source)
	}
	if !nativeAPIConstructionRoots[projectionShapeFunction(t, source).Name.Name] {
		return source
	}
	source = strings.Replace(source, "\t_, db, _ := testutil.StartPostgres(t)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "\t_, db, cleanup := testutil.StartPostgres(t)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "\tt.Cleanup(cleanup)\n", "", 1)
	return strings.Replace(source, "\tpg := storetest.AdmitPostgresRuntimeStore(t, db)\n", "", 1)
}

func TestNativeAPIConstructionUsesOriginalOwnerBeforeConsumerCleanup(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if !nativeAPIConstructionRoots[row.Function] {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		want, err := canonicalFunction(row.After)
		got, sourceErr := canonicalFunction(actual)
		if err != nil || sourceErr != nil || want != got || strings.Contains(actual, "testutil.StartPostgres") ||
			strings.Contains(actual, "AdmitPostgresRuntimeStore") || strings.Count(actual, "pg := storetest.StartPostgresRuntimeStore(t)") != 1 {
			t.Fatalf("API root regained exposed or duplicate construction: %s", row.Function)
		}
		for _, replacement := range []string{"storetest.StartPostgresRuntimeStore(otherTest)", "storetest.StartSQLiteRuntimeStore(t)", "storetest.AdmitPostgresRuntimeStore(t, db)"} {
			mutant := strings.Replace(row.After, "storetest.StartPostgresRuntimeStore(t)", replacement, 1)
			changed, err := canonicalFunction(mutant)
			if mutant == row.After || (err == nil && changed == want) {
				t.Fatalf("foreign API constructor admitted: %s", replacement)
			}
		}
	}
	if matched != len(nativeAPIConstructionRoots) {
		t.Fatalf("API native construction recipes=%d,want%d", matched, len(nativeAPIConstructionRoots))
	}
}
