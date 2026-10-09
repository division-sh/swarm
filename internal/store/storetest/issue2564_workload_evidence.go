package storetest

import (
	"context"
	"testing"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/internal/testconstruction"
)

type Issue2564WorkloadObservation = private.Issue2564WorkloadObservation
type Issue2564WorkloadReader = private.Issue2564WorkloadReader

func OpenIssue2564WorkloadObservation(t testing.TB, backend, location string) *Issue2564WorkloadObservation {
	t.Helper()
	spec, plans := canonicalPlatformPlans(t)
	request := private.SchemaBootstrapRequest{
		PlatformPlans: plans,
		Origin:        private.RuntimeStoreOrigin{SwarmVersion: "storetest", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}
	observation, err := testconstruction.OpenIssue2564WorkloadObservationForTest(t.Context(), backend, location, request)
	if err != nil {
		t.Fatalf("open original native workload observation: %v", err)
	}
	t.Cleanup(func() {
		if err := observation.CloseForTest(); err != nil {
			t.Errorf("close native workload observation: %v", err)
		}
	})
	return observation
}

type H1FlowAccountingEvidence = private.H1FlowAccountingEvidence

func ObserveH1FlowAccounting(ctx context.Context, selected any, runID string) (H1FlowAccountingEvidence, error) {
	return private.ObserveH1FlowAccountingForTest(ctx, selected, runID)
}

type H1TurnAccountingEvidence = private.H1TurnAccountingEvidence

func ObserveH1TurnAccounting(ctx context.Context, selected any, runID string) (H1TurnAccountingEvidence, error) {
	return private.ObserveH1TurnAccountingForTest(ctx, selected, runID)
}

type H1BumpAccountingEvidence = private.H1BumpAccountingEvidence

func ObserveH1BumpAccounting(ctx context.Context, selected any, runID string) (H1BumpAccountingEvidence, error) {
	return private.ObserveH1BumpAccountingForTest(ctx, selected, runID)
}

type H1DeliveryAccountingEvidence = private.H1DeliveryAccountingEvidence

func ObserveH1DeliveryAccounting(ctx context.Context, selected any, runID string) (H1DeliveryAccountingEvidence, error) {
	return private.ObserveH1DeliveryAccountingForTest(ctx, selected, runID)
}

type H2DeliveryAccountingEvidence = private.H2DeliveryAccountingEvidence

func ObserveH2DeliveryAccounting(ctx context.Context, selected any, runID string) (H2DeliveryAccountingEvidence, error) {
	return private.ObserveH2DeliveryAccountingForTest(ctx, selected, runID)
}

type H2ConstructionAccountingEvidence = private.H2ConstructionAccountingEvidence

func ObserveH2ConstructionAccounting(ctx context.Context, selected any, runID string) (H2ConstructionAccountingEvidence, error) {
	return private.ObserveH2ConstructionAccountingForTest(ctx, selected, runID)
}

type H2EventAccountingEvidence = private.H2EventAccountingEvidence

func ObserveH2EventAccounting(ctx context.Context, selected any, runID string) (H2EventAccountingEvidence, error) {
	return private.ObserveH2EventAccountingForTest(ctx, selected, runID)
}

type H2PendingAccountingEvidence = private.H2PendingAccountingEvidence

func ObserveH2PendingAccounting(ctx context.Context, selected any, runID string) (H2PendingAccountingEvidence, error) {
	return private.ObserveH2PendingAccountingForTest(ctx, selected, runID)
}

type H1RunOverlapEvidence = private.H1RunOverlapEvidence

func ObserveH1RunOverlap(ctx context.Context, selected any, runID string) (H1RunOverlapEvidence, error) {
	return private.ObserveH1RunOverlapForTest(ctx, selected, runID)
}

type H1HubFieldsEvidence = private.H1HubFieldsEvidence

func ObserveH1HubFields(ctx context.Context, selected any, runID string) ([]H1HubFieldsEvidence, error) {
	return private.ObserveH1HubFieldsForTest(ctx, selected, runID)
}

type H1AttributedMutationsEvidence = private.H1AttributedMutationsEvidence

func ObserveH1AttributedMutations(ctx context.Context, selected any, runID, bundleHash string) ([]H1AttributedMutationsEvidence, error) {
	return private.ObserveH1AttributedMutationsForTest(ctx, selected, runID, bundleHash)
}

type H1SameEntityOverlapEvidence = private.H1SameEntityOverlapEvidence

func ObserveH1SameEntityOverlap(ctx context.Context, selected any, runID string) ([]H1SameEntityOverlapEvidence, error) {
	return private.ObserveH1SameEntityOverlapForTest(ctx, selected, runID)
}

type H1FailureMutationsEvidence = private.H1FailureMutationsEvidence

func ObserveH1FailureMutations(ctx context.Context, selected any, runID string) ([]H1FailureMutationsEvidence, error) {
	return private.ObserveH1FailureMutationsForTest(ctx, selected, runID)
}

type H1NodeFailuresEvidence = private.H1NodeFailuresEvidence

func ObserveH1NodeFailures(ctx context.Context, selected any, runID string) ([]H1NodeFailuresEvidence, error) {
	return private.ObserveH1NodeFailuresForTest(ctx, selected, runID)
}

type H1AttemptFailuresEvidence = private.H1AttemptFailuresEvidence

func ObserveH1AttemptFailures(ctx context.Context, selected any, runID string) ([]H1AttemptFailuresEvidence, error) {
	return private.ObserveH1AttemptFailuresForTest(ctx, selected, runID)
}

type H1DeadLettersEvidence = private.H1DeadLettersEvidence

func ObserveH1DeadLetters(ctx context.Context, selected any, runID string) ([]H1DeadLettersEvidence, error) {
	return private.ObserveH1DeadLettersForTest(ctx, selected, runID)
}

type H1BumpHistoryEvidence = private.H1BumpHistoryEvidence

func ObserveH1BumpHistory(ctx context.Context, selected any, runID string) ([]H1BumpHistoryEvidence, error) {
	return private.ObserveH1BumpHistoryForTest(ctx, selected, runID)
}

type H2HubsEvidence = private.H2HubsEvidence

func ObserveH2Hubs(ctx context.Context, selected any, runID string) ([]H2HubsEvidence, error) {
	return private.ObserveH2HubsForTest(ctx, selected, runID)
}

type H2OccurrencesEvidence = private.H2OccurrencesEvidence

func ObserveH2Occurrences(ctx context.Context, selected any, runID string) ([]H2OccurrencesEvidence, error) {
	return private.ObserveH2OccurrencesForTest(ctx, selected, runID)
}

type H2ResponseQueueEvidence = private.H2ResponseQueueEvidence

func ObserveH2ResponseQueue(ctx context.Context, selected any, runID string) ([]H2ResponseQueueEvidence, error) {
	return private.ObserveH2ResponseQueueForTest(ctx, selected, runID)
}

type H2CounterMutationsEvidence = private.H2CounterMutationsEvidence

func ObserveH2CounterMutations(ctx context.Context, selected any, runID string) ([]H2CounterMutationsEvidence, error) {
	return private.ObserveH2CounterMutationsForTest(ctx, selected, runID)
}

type H2NodeDeliveriesEvidence = private.H2NodeDeliveriesEvidence

func ObserveH2NodeDeliveries(ctx context.Context, selected any, runID string) ([]H2NodeDeliveriesEvidence, error) {
	return private.ObserveH2NodeDeliveriesForTest(ctx, selected, runID)
}

type H2WorkloadSnapshotEvidence = private.H2WorkloadSnapshotEvidence
type H2TransitionCutsEvidence = private.H2TransitionCutsEvidence
type H2SessionEvidence = private.H2SessionEvidence
type H2SessionObservation = private.H2SessionObservation

func ObserveH2WorkloadSnapshot(ctx context.Context, selected any, runID string) (H2WorkloadSnapshotEvidence, error) {
	return private.ObserveH2WorkloadSnapshotForTest(ctx, selected, runID)
}

func ObserveH2ServerCapacity(ctx context.Context, selected any) (int, error) {
	return private.ObserveH2ServerCapacityForTest(ctx, selected)
}

func BeginH2SessionObservation(ctx context.Context, selected any, runID string, count int) (H2SessionObservation, error) {
	return private.BeginH2SessionObservationForTest(ctx, selected, runID, count)
}
