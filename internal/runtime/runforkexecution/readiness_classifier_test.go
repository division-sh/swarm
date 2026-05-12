package runforkexecution

import (
	"strings"
	"testing"
	"time"

	"swarm/internal/store"
)

func TestBuildSelectedContractReadinessClassifierEmitsCompleteOwnerMatrix(t *testing.T) {
	frontier := store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []store.RunForkContractFrontierEvent{{
			SourceEventID: "source-event",
			EventName:     "work.begin",
			DerivedRecipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "agent",
				SubscriberID:   "worker",
				Path:           "flow-a/worker",
				RouteSource:    "selected_contracts",
			}},
		}},
	}
	model := testSelectedContractExecutionModel(t, frontier)
	plan := store.RunForkPlan{
		SourceRunID: "source-run",
		ForkPoint: store.RunForkPoint{
			Input:     "source-event",
			EventID:   "source-event",
			EventName: "work.begin",
			Timestamp: time.Unix(1700000707, 0).UTC(),
		},
		ReplayResumeAdmission: store.RunForkReplayResumeAdmission{
			Owner:                    store.RunForkReplayResumeAdmissionOwner,
			DeliveryEventReplayReady: true,
			Dispositions: []store.RunForkReplayResumeDisposition{{
				Fact:           store.RunForkReplayResumeFactDeliveryPendingHistory,
				Disposition:    store.RunForkReplayResumeDispositionForkReplay,
				Classification: store.RunForkPendingClassificationPending,
				Message:        "pending delivery can be replayed",
			}, {
				Fact:        store.RunForkReplayResumeFactSourceAdvanced,
				Disposition: store.RunForkReplayResumeDispositionLineageOnly,
				Owner:       store.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner,
				Message:     "post-T source conversation history is branch divergence evidence",
			}},
		},
	}

	readiness, err := BuildSelectedContractReadinessClassifier(SelectedContractReadinessClassifierRequest{
		Plan:                      plan,
		ContractFrontierAdmission: frontier,
		SelectedContractExecution: model,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractReadinessClassifier: %v", err)
	}
	if readiness.Owner != store.RunForkSelectedContractReadinessClassifierOwner ||
		!readiness.NonMutating ||
		readiness.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner ||
		readiness.ContractFrontierAdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		readiness.RouteTopologyOwner != store.RunForkSelectedContractRouteTopologyOwner ||
		readiness.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		readiness.SelectedExecutionModelOwner != store.RunForkSelectedContractExecutionModelOwner {
		t.Fatalf("readiness ownership = %#v", readiness)
	}
	assertReadinessFactSet(t, readiness.FactMatrix)
	assertReadinessFact(t, readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSourceDeliveries, store.RunForkSelectedContractReadinessDispositionExecutableForkWork, store.RunForkHistoricalReplayExecutionOwner)
	assertReadinessFact(t, readiness.FactMatrix, store.RunForkSelectedContractReadinessFactForkEvents, store.RunForkSelectedContractReadinessDispositionExecutableForkWork, store.RunForkSelectedContractExecutionOwner)
	assertReadinessFact(t, readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes, store.RunForkSelectedContractReadinessDispositionReconstructedForkState, store.RunForkSelectedContractRecipientPlanningOwner)
	assertReadinessFact(t, readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSourcePostTFacts, store.RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence, store.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner)
	assertReadinessFact(t, readiness.FactMatrix, store.RunForkSelectedContractReadinessFactOperatorConsumers, store.RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling, store.RunForkHistoricalReplayExecutionAdmissionOwner)
	if !executionBoundaryHas(readiness.InvalidPaths, "explain_output_authorizes_mutation", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want explain output non-authoritative", readiness.InvalidPaths)
	}
	if !executionBoundaryHas(readiness.RequiredConsumers, "fork_local_runtime_container", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want fork-local runtime container prerequisite", readiness.RequiredConsumers)
	}
	if executionBoundaryHas(readiness.BlockedSiblings, "fork_local_runtime_container", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, fork-local runtime container should be an implemented required consumer", readiness.BlockedSiblings)
	}
}

func TestBuildSelectedContractReadinessClassifierRejectsLocalExecutionModel(t *testing.T) {
	_, err := BuildSelectedContractReadinessClassifier(SelectedContractReadinessClassifierRequest{
		Plan: store.RunForkPlan{
			ReplayResumeAdmission: store.RunForkReplayResumeAdmission{Owner: store.RunForkReplayResumeAdmissionOwner},
		},
		ContractFrontierAdmission: store.RunForkContractFrontierAdmission{
			Owner:       store.RunForkContractFrontierAdmissionOwner,
			NonMutating: true,
		},
		SelectedContractExecution: store.RunForkSelectedContractExecution{
			Owner:       "cmd.swarm.local_readiness",
			NonMutating: true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractExecutionModelOwner) {
		t.Fatalf("error = %v, want canonical execution model owner failure", err)
	}
}

func assertReadinessFactSet(t *testing.T, facts []store.RunForkSelectedContractReadinessFact) {
	t.Helper()
	required := []string{
		store.RunForkSelectedContractReadinessFactSourceEvents,
		store.RunForkSelectedContractReadinessFactForkEvents,
		store.RunForkSelectedContractReadinessFactSourceDeliveries,
		store.RunForkSelectedContractReadinessFactForkDeliveries,
		store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		store.RunForkSelectedContractReadinessFactTimers,
		store.RunForkSelectedContractReadinessFactSessions,
		store.RunForkSelectedContractReadinessFactTurns,
		store.RunForkSelectedContractReadinessFactAudits,
		store.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers,
		store.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		store.RunForkSelectedContractReadinessFactReceipts,
		store.RunForkSelectedContractReadinessFactDeadLetters,
		store.RunForkSelectedContractReadinessFactRetryIdempotency,
		store.RunForkSelectedContractReadinessFactEmittedFollowUps,
		store.RunForkSelectedContractReadinessFactSourcePostTFacts,
		store.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		store.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork,
		store.RunForkSelectedContractReadinessFactRestartRecovery,
		store.RunForkSelectedContractReadinessFactOperatorConsumers,
	}
	seen := map[string]struct{}{}
	for _, fact := range facts {
		if _, ok := seen[fact.Fact]; ok {
			t.Fatalf("readiness fact %s repeated in %#v", fact.Fact, facts)
		}
		seen[fact.Fact] = struct{}{}
	}
	for _, fact := range required {
		if _, ok := seen[fact]; !ok {
			t.Fatalf("readiness fact %s missing from %#v", fact, facts)
		}
	}
}

func assertReadinessFact(t *testing.T, facts []store.RunForkSelectedContractReadinessFact, fact, disposition, owner string) {
	t.Helper()
	for _, item := range facts {
		if item.Fact == fact {
			if item.Disposition != disposition || item.Owner != owner {
				t.Fatalf("readiness fact %s = %#v, want disposition=%s owner=%s", fact, item, disposition, owner)
			}
			return
		}
	}
	t.Fatalf("readiness fact %s missing from %#v", fact, facts)
}
