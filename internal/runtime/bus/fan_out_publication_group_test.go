package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

// This double proves dispatch ownership transitions only. Physical transaction,
// session, rollback, history and recovery proofs use the real selected stores.
type publicationSettlementProbe struct {
	settlements [][]pipelineobligation.PublicationSettlementMember
	reads       int
	fail        error
	ackPrefix   int
}

func (*publicationSettlementProbe) Claim(context.Context, int, events.Event) (pipelineobligation.Claim, error) {
	return pipelineobligation.Claim{}, errors.New("probe does not admit publications")
}
func (*publicationSettlementProbe) ClaimBatch(context.Context, []pipelineobligation.PublicationClaimRequest) ([]pipelineobligation.Claim, error) {
	return nil, errors.New("probe does not admit publication batches")
}
func (*publicationSettlementProbe) RecordPrepared(context.Context, pipelineobligation.Claim, pipelineobligation.PublicationPreparation) error {
	return errors.New("probe does not prepare publications")
}
func (*publicationSettlementProbe) Seal(context.Context, int, []pipelineobligation.Claim) error {
	return errors.New("probe does not seal publication attempts")
}
func (*publicationSettlementProbe) ValidateCommitted(context.Context, []pipelineobligation.Claim) error {
	return errors.New("probe does not admit committed publication groups")
}
func (*publicationSettlementProbe) ValidateCommittedMembership([]pipelineobligation.Claim) error {
	return errors.New("probe does not admit committed membership")
}
func (p *publicationSettlementProbe) Settle(_ context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	p.settlements = append(p.settlements, append([]pipelineobligation.PublicationSettlementMember(nil), members...))
	result := pipelineobligation.PublicationGroupOutcome{}
	for i, member := range members {
		if p.fail != nil && i >= p.ackPrefix {
			break
		}
		result.Results = append(result.Results, pipelineobligation.PublicationSettlementResult{Claim: member.Claim, Outcome: pipelineobligation.CommittedSettlement(false)})
	}
	return result, p.fail
}
func (p *publicationSettlementProbe) ReadPublicationSettlement(_ context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationSettlementSnapshot, error) {
	p.reads++
	result := pipelineobligation.PublicationSettlementSnapshot{}
	for _, member := range members {
		result.Rows = append(result.Rows, pipelineobligation.PublicationSettlementObservation{Claim: member.Claim, State: pipelineobligation.PublicationSettlementSatisfied})
	}
	return result, nil
}
func (*publicationSettlementProbe) Close(context.Context) error { return nil }

func publicationCollectorClaim(t *testing.T, bus *EventBus) *pipelinePublicationClaim {
	t.Helper()
	claim, err := pipelineobligation.NewClaimIssuer().Issue(uuid.NewString(), pipelineobligation.PurposePublication)
	if err != nil {
		t.Fatal(err)
	}
	return &pipelinePublicationClaim{bus: bus, eventID: claim.EventID(), claim: claim}
}

func TestFanOutFailedGroupRetiresOnlyExactPendingCallbacks(t *testing.T) {
	bus := &EventBus{pendingOutboxByID: make(map[string][]pendingOutboxOperation)}
	claim := publicationCollectorClaim(t, bus)
	successor := publicationCollectorClaim(t, bus)
	foreign := publicationCollectorClaim(t, &EventBus{})
	event := eventtest.RunCreatingRootIngress(claim.eventID, "custom.emitted", "", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	committed := CommittedEnginePublication{plan: EnginePublicationPlan{
		command:  PublicationCommand{Commit: CommitPublishRequest{Event: admitted}},
		prepared: PreparedPublish{publicationClaim: claim},
	}}
	bus.pendingOutboxByID[claim.eventID] = []pendingOutboxOperation{
		{sequence: 1, publicationClaim: claim},
		{sequence: 2, publicationClaim: successor},
		{sequence: 3, publicationClaim: claim},
		{sequence: 4, publicationClaim: foreign},
	}
	bus.retireFanOutOutboxOperations([]engine.CommittedDurablePublication{committed})
	remaining := bus.pendingOutboxByID[claim.eventID]
	if len(remaining) != 2 || remaining[0].sequence != 2 || remaining[1].sequence != 4 {
		t.Fatalf("callback retirement changed successor/foreign work: %+v", remaining)
	}
	if claim.released.Load() || successor.released.Load() || foreign.released.Load() {
		t.Fatal("callback retirement invented claim release or settlement")
	}
	if !claim.retired.Load() || successor.retired.Load() || foreign.retired.Load() {
		t.Fatal("cleanup transfer did not retire only the exact local callback")
	}
	collector := &fanOutPublicationSettlement{bus: bus}
	if err := collector.collect(claim, pipelineobligation.Acknowledged("pipeline_persisted")); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
		t.Fatalf("retired callback regained collection authority: %v", err)
	}
	bus.retireFanOutOutboxOperations([]engine.CommittedDurablePublication{committed})
	if len(bus.pendingOutboxByID[claim.eventID]) != 2 {
		t.Fatal("repeated retirement consumed unrelated callbacks")
	}
}

func TestFanOutPublicationCollectionRetainsClaimsUntilSegmentCommit(t *testing.T) {
	bus := &EventBus{}
	group := &publicationSettlementProbe{}
	collector := &fanOutPublicationSettlement{ctx: context.Background(), bus: bus, group: group}
	first, second := publicationCollectorClaim(t, bus), publicationCollectorClaim(t, bus)
	for _, claim := range []*pipelinePublicationClaim{first, second} {
		if err := collector.collect(claim, pipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
			t.Fatal(err)
		}
	}
	if first.released.Load() || second.released.Load() || len(group.settlements) != 0 {
		t.Fatal("collection consumed a claim or fabricated a settlement")
	}
	if err := collector.flushBeforeNestedPublication(); err != nil {
		t.Fatal(err)
	}
	if !first.released.Load() || !second.released.Load() || len(group.settlements) != 1 || len(group.settlements[0]) != 2 || collector.closed {
		t.Fatal("segment did not consume exactly its members while retaining outer collection")
	}
	third := publicationCollectorClaim(t, bus)
	if err := collector.collect(third, pipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		t.Fatal(err)
	}
	if err := collector.finish(); err != nil {
		t.Fatal(err)
	}
	if !third.released.Load() || !collector.closed || len(group.settlements) != 2 || len(group.settlements[1]) != 1 {
		t.Fatal("final segment lost its independent lifetime")
	}
	if err := collector.collect(publicationCollectorClaim(t, bus), pipelineobligation.Acknowledged("pipeline_persisted")); err == nil {
		t.Fatal("closed collection accepted another member")
	}
}

func TestFanOutPublicationObservationNeverBecomesCommitAcknowledgement(t *testing.T) {
	bus := &EventBus{}
	uncertain := errors.New("commit acknowledgement lost")
	group := &publicationSettlementProbe{fail: uncertain}
	collector := &fanOutPublicationSettlement{ctx: context.Background(), bus: bus, group: group}
	claim := publicationCollectorClaim(t, bus)
	if err := collector.collect(claim, pipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := collector.flushBeforeNestedPublication(); !errors.Is(err, uncertain) {
			t.Fatalf("uncertainty was hidden: %v", err)
		}
	}
	if err := collector.finish(); !errors.Is(err, uncertain) {
		t.Fatalf("finish lost uncertainty: %v", err)
	}
	if claim.released.Load() || len(group.settlements) != 1 || group.reads != 1 {
		t.Fatal("observed satisfaction forged acknowledgement or retried uncertain settlement")
	}
}

func TestFanOutPublicationCollectionRejectsDuplicateAndForeignClaims(t *testing.T) {
	bus := &EventBus{}
	group := &publicationSettlementProbe{}
	collector := &fanOutPublicationSettlement{ctx: context.Background(), bus: bus, group: group}
	claim := publicationCollectorClaim(t, bus)
	if err := collector.collect(claim, pipelineobligation.Acknowledged("pipeline_persisted")); err != nil {
		t.Fatal(err)
	}
	if err := collector.collect(claim, pipelineobligation.Acknowledged("pipeline_persisted")); err == nil {
		t.Fatal("duplicate claim accepted")
	}
	if err := collector.collect(publicationCollectorClaim(t, &EventBus{}), pipelineobligation.Acknowledged("pipeline_persisted")); err == nil {
		t.Fatal("foreign bus claim accepted")
	}
	if len(group.settlements) != 0 || claim.released.Load() || len(collector.members) != 1 {
		t.Fatal("invalid member changed settlement ownership")
	}
}
