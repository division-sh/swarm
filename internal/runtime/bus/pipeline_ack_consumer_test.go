package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type acknowledgedPipelineOwner struct {
	runtimepipelineobligation.Store
	markOutcome   runtimepipelineobligation.SettlementOutcome
	markErr       error
	settleOutcome runtimepipelineobligation.SettlementOutcome
	settleErr     error
	marks         int
	settles       int
	releases      int
}

func (o *acknowledgedPipelineOwner) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := runtimepipelineobligation.NewClaimIssuer().Issue(eventID, purpose)
	return runtimepipelineobligation.ClaimedWork{Claim: claim}, err
}

func (o *acknowledgedPipelineOwner) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) (runtimepipelineobligation.SettlementOutcome, error) {
	o.marks++
	return o.markOutcome, o.markErr
}

func (o *acknowledgedPipelineOwner) Settle(context.Context, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	o.settles++
	return o.settleOutcome, o.settleErr
}

func (o *acknowledgedPipelineOwner) Release(context.Context, runtimepipelineobligation.Claim) error {
	o.releases++
	return nil
}

func acknowledgedPipelineClaim(t *testing.T, purpose runtimepipelineobligation.Purpose) runtimepipelineobligation.Claim {
	t.Helper()
	claim, err := runtimepipelineobligation.NewClaimIssuer().Issue(uuid.NewString(), purpose)
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func TestPublicationDecisionMarkRetainsPostcommitErrorThroughSettlement(t *testing.T) {
	injectedMarkErr := errors.New("mark postcommit handoff failed")
	settleErr := errors.New("settlement postcommit handoff failed")
	owner := &acknowledgedPipelineOwner{
		markOutcome:   runtimepipelineobligation.CommittedSettlement(true),
		markErr:       injectedMarkErr,
		settleOutcome: runtimepipelineobligation.CommittedSettlement(true),
		settleErr:     settleErr,
	}
	bus := &EventBus{ephemeral: true, pipelineObligations: owner}
	claim := acknowledgedPipelineClaim(t, runtimepipelineobligation.PurposePublication)
	publication := &pipelinePublicationClaim{bus: bus, eventID: claim.EventID(), claim: claim}
	if err := bus.settleCommittedDecisionPublish(context.Background(), publication); !errors.Is(err, injectedMarkErr) || !errors.Is(err, settleErr) {
		t.Fatalf("settlement lost postcommit errors: %v", err)
	}
	if owner.marks != 1 || owner.settles != 1 || owner.releases != 0 {
		t.Fatalf("marks/settles/releases = %d/%d/%d", owner.marks, owner.settles, owner.releases)
	}
}

func TestOutboxRecoverySettlementRetainsAcknowledgedCleanupError(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "unacknowledged"
		if committed {
			name = "acknowledged"
		}
		t.Run(name, func(t *testing.T) {
			cleanupErr := errors.New("outbox settlement postcommit cleanup failed")
			owner := &acknowledgedPipelineOwner{settleErr: cleanupErr}
			if committed {
				owner.settleOutcome = runtimepipelineobligation.CommittedSettlement(true)
			}
			bus := &EventBus{ephemeral: true, pipelineObligations: owner}
			event := eventtest.PersistedProjection(uuid.NewString(), events.EventType("custom.outbox_ack"), "runtime", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			err := (engineDispatcher{bus: bus}).dispatchAndRecord(context.Background(), runtimeengine.EmitIntent{Event: event}, nil)
			if !errors.Is(err, cleanupErr) || owner.settles != 1 {
				t.Fatalf("settlement error=%v calls=%d", err, owner.settles)
			}
			wantRelease := 1
			if committed {
				wantRelease = 0
			}
			if owner.releases != wantRelease {
				t.Fatalf("release calls=%d, want %d", owner.releases, wantRelease)
			}
		})
	}
}

func TestPublicationDecisionMarkFailureStillRequiresRelease(t *testing.T) {
	markErr := errors.New("mark rolled back")
	owner := &acknowledgedPipelineOwner{markErr: markErr}
	bus := &EventBus{ephemeral: true, pipelineObligations: owner}
	claim := acknowledgedPipelineClaim(t, runtimepipelineobligation.PurposePublication)
	publication := &pipelinePublicationClaim{bus: bus, eventID: claim.EventID(), claim: claim}
	if _, err := publication.MarkDecisionProcessedOutcome(context.Background()); !errors.Is(err, markErr) {
		t.Fatalf("unacknowledged mark error = %v", err)
	}
	if err := publication.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if owner.settles != 0 || owner.releases != 1 {
		t.Fatalf("settles/releases = %d/%d", owner.settles, owner.releases)
	}
}

func TestAcknowledgedSweeperSettlementDoesNotReleaseOnCleanupError(t *testing.T) {
	cleanupErr := errors.New("settlement postcommit cleanup failed")
	owner := &acknowledgedPipelineOwner{
		settleOutcome: runtimepipelineobligation.CommittedSettlement(true),
		settleErr:     cleanupErr,
	}
	bus := &EventBus{ephemeral: true, pipelineObligations: owner}
	claim := acknowledgedPipelineClaim(t, runtimepipelineobligation.PurposeDecisionRoute)
	settled, retry, _, err := bus.processClaimedPipelineWork(context.Background(), runtimepipelineobligation.ClaimedWork{Claim: claim, Acknowledged: true})
	if !settled || retry || !errors.Is(err, cleanupErr) {
		t.Fatalf("settled=%t retry=%t error=%v", settled, retry, err)
	}
	if owner.settles != 1 || owner.releases != 0 {
		t.Fatalf("settles/releases = %d/%d", owner.settles, owner.releases)
	}
}
