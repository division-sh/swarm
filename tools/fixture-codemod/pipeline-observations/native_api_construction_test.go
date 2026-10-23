package main

import (
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
