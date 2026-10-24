package runtimepersistence

import (
	"context"

	"errors"

	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedFanOutOutcomeCorrespondenceKeepsExactScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			ctx = originalStoreFixtureContext(t, ctx, fixture.store)
			active, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, cell := range []struct {
				run, kind string
				want      int
			}{
				{active.Claim.RunID(), "committed", 2},
				{sibling.Claim.RunID(), "committed", 1},
				{active.Claim.RunID(), "continued", 0},
				{uuid.NewString(), "committed", 0},
			} {
				observed, err := ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx, fixture.store, cell.run, cell.kind,
					active.Snapshot.EventID, active.Snapshot.EventID, sibling.Claim.RunID())
				if err != nil || observed.Outcomes != cell.want || observed.ExactContinued != 0 {
					t.Fatalf("correspondence crossed run/kind or invented continuation: run=%s kind=%s evidence=%+v error=%v", cell.run, cell.kind, observed, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 4 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("correspondence did not use the original read snapshot: %+v", counts)
			}
			for coordinate := 0; coordinate < 4; coordinate++ {
				ids := []string{active.Claim.RunID(), active.Snapshot.EventID, active.Snapshot.EventID, sibling.Claim.RunID()}
				ids[coordinate] = "not-a-run-or-event"
				observed, err := ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx, fixture.store, ids[0], "continued", ids[1], ids[2], ids[3])
				if err == nil || observed != (SelectedFanOutOutcomeCorrespondence{}) {
					t.Fatalf("invalid coordinate%d returned evidence: %+v %v", coordinate, observed, err)
				}
			}
			if got, err := ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx, fixture.store, active.Claim.RunID(), "other", active.Snapshot.EventID, active.Snapshot.EventID, sibling.Claim.RunID()); err == nil || got != (SelectedFanOutOutcomeCorrespondence{}) {
				t.Fatalf("unbounded outcome predicate accepted: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadSelectedFanOutOutcomeCorrespondenceForTest(cancelled, fixture.store, active.Claim.RunID(), "committed", active.Snapshot.EventID, active.Snapshot.EventID, sibling.Claim.RunID()); !errors.Is(err, context.Canceled) || got != (SelectedFanOutOutcomeCorrespondence{}) {
				t.Fatalf("cancelled observation returned evidence: %+v %v", got, err)
			}
			for _, foreign := range []any{nil, struct{}{}, (*SQLiteRuntimeStore)(nil), (*PostgresStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}} {
				if got, err := ReadSelectedFanOutOutcomeCorrespondenceForTest(ctx, foreign, active.Claim.RunID(), "committed", active.Snapshot.EventID, active.Snapshot.EventID, sibling.Claim.RunID()); err == nil || got != (SelectedFanOutOutcomeCorrespondence{}) {
					t.Fatalf("foreign/uninitialized owner%T returned evidence: %+v %v", foreign, got, err)
				}
			}
			if probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatal("read/refusal controls mutated durable state")
			}
		})
	}
}
