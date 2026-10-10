package bus_test

import (
	"reflect"
	"testing"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCompleteDispatchDecisionFixtureUsesOriginalDecisionOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			probe := storetest.CollectTransactions(t, fixture.store, storetest.TransactionProbeOptions{})
			card := fixture.insertDecisionObligationFor(t, fixture.event)
			counts := probe.Snapshot()
			if counts.Total.WriteCommits != 2 || counts.Active != 0 {
				t.Fatalf("card creation/decision escaped original coordinator: %+v", counts)
			}
			stored, err := fixture.store.GetDecisionCard(fixture.ctx, card.CardID)
			if err != nil || !reflect.DeepEqual(stored, card) || stored.Status != decisioncard.StatusDecided ||
				stored.RunID != fixture.event.RunID() || stored.DecisionEventID != fixture.event.ID() ||
				stored.Verdict != "approve" || stored.DecidedBy != "test" || !stored.DecidedAt.Equal(fixture.event.CreatedAt()) ||
				stored.BundleHash != authorActivityTestBundleHash || stored.CardContentHash == "card-hash" || stored.DecisionSchemaHash == "schema-hash" {
				t.Fatalf("canonical decided card lost fixture identity/content/clock: %+v err=%v", stored, err)
			}
			if status := fixture.decisionObligationStatus(t, fixture.event.ID()); status != "pending" {
				t.Fatalf("decided card did not atomically create pending routing: %q", status)
			}
			if _, err := fixture.store.PipelineObligations().ClaimEvent(fixture.ctx, uuid.NewString(), runtimepipelineobligation.PurposeDecisionRoute); err == nil {
				t.Fatal("foreign event borrowed decision routing authority")
			}
			work, err := fixture.store.PipelineObligations().ClaimEvent(fixture.ctx, fixture.event.ID(), runtimepipelineobligation.PurposeDecisionRoute)
			if err != nil || work.Claim.EventID() != fixture.event.ID() || work.Event.RunID() != fixture.event.RunID() {
				t.Fatalf("canonical decision route could not be claimed: %+v err=%v", work, err)
			}
			assertCompleteEventSnapshot(t, work.Event, fixture.event)
			if _, err := fixture.store.PipelineObligations().Settle(fixture.ctx, work.Claim, runtimepipelineobligation.Acknowledged("fixture_native_decision")); err != nil {
				t.Fatal(err)
			}
			if status := fixture.decisionObligationStatus(t, fixture.event.ID()); status != "completed" {
				t.Fatalf("canonical route settlement=%q, want completed", status)
			}
			if counts := probe.Snapshot(); counts.Active != 0 {
				t.Fatalf("fixture retained selected-store work: %+v", counts)
			}
		})
	}
}
