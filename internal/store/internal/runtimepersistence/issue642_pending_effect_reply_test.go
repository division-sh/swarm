package runtimepersistence

import (
	"context"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestIssue642PendingEffectRetainsOwnChildReplyBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			child, event, journal := seedActivityEvidenceForForkWithReply(t, fixture, backend.name == "postgres", false, "succeeded", 0, false, true, true)
			ctx := testAuthorActivityContext()
			cards := fixture.store.(interface {
				decisioncard.Store
				decisioncard.ProposedEffectStore
			})
			sourceCards, cursor, err := cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: event.RunID(), Limit: 10})
			if err != nil || cursor != "" || len(sourceCards) != 1 {
				t.Fatalf("exact original pending-card inventory: %+v cursor=%q error=%v", sourceCards, cursor, err)
			}
			source, err := cards.LoadProposedEffectContinuation(ctx, sourceCards[0].CardID)
			if err != nil || source.ReplyContextID != journal.ReplyContextID || source.ReplyContextID == "" {
				t.Fatalf("original pending effect lacks its actual incoming reply: %+v error=%v", source, err)
			}
			rows, err := ReadReplyContextRecordsForTest(ctx, fixture.store, child.ForkRunID)
			if err != nil || len(rows) != 1 || rows[0].ID == source.ReplyContextID {
				t.Fatalf("child lacks own captured row: %+v error=%v", rows, err)
			}
			childCards, cursor, err := cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: child.ForkRunID, Limit: 10})
			if err != nil || cursor != "" || len(childCards) != 1 {
				t.Fatalf("exact child pending-card inventory: %+v cursor=%q error=%v", childCards, cursor, err)
			}
			projected, err := cards.LoadProposedEffectContinuation(ctx, childCards[0].CardID)
			if err != nil {
				t.Fatal(err)
			}
			if projected.ReplyContextID != rows[0].ID {
				t.Fatalf("pending effect reply=%q, want exact own child row %q (source %q)", projected.ReplyContextID, rows[0].ID, source.ReplyContextID)
			}
			if childCards[0].Status != decisioncard.StatusPending || projected.State != decisioncard.ProposedEffectPending || projected.RequestEventID == source.RequestEventID || childCards[0].CardID == sourceCards[0].CardID || !projected.Input.Equal(source.Input) {
				t.Fatalf("projected reply borrowed approval or changed frozen input: source=%+v child=%+v card=%+v", source, projected, childCards[0])
			}
			readback, err := cards.ProposedEffectReadback(ctx, childCards[0].CardID)
			if err != nil || readback.DispatchState != "held" {
				t.Fatalf("pending effect gained dispatch permission from its reply: %+v error=%v", readback, err)
			}
			if copied, found, err := fixture.store.(activityTimestampJournal).LoadActivityAttempt(ctx, projected.RequestEventID); err != nil || found {
				t.Fatalf("pending effect borrowed activity dispatch evidence: %+v found=%t error=%v", copied, found, err)
			}
			plan, err := fixture.store.(selectedActivityProjectionStore).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: event.RunID(), At: event.ID()})
			if err != nil || len(plan.ReplyContexts) != 1 {
				t.Fatalf("actual captured pending-effect plan: %+v error=%v", plan.ReplyContexts, err)
			}
			projection, err := projectRunForkEntityOwnership(event.RunID(), child.ForkRunID, source.EntityID, source.FlowInstance)
			if err != nil {
				t.Fatal(err)
			}
			correspondence, err := loopruntime.NewForkCorrespondence(nil, child.ForkRunID, projection.Fork.EntityID)
			if err != nil {
				t.Fatal(err)
			}
			target, err := runtimecontracts.SourceExecutionIdentity(source.BundleHash)
			if err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			for _, fault := range []string{"missing", "duplicate", "foreign", "corrupt_original"} {
				t.Run(fault, func(t *testing.T) {
					bad := plan
					bad.ReplyContexts = append([]replycontext.Record(nil), plan.ReplyContexts...)
					switch fault {
					case "missing":
						bad.ReplyContexts = nil
					case "duplicate":
						bad.ReplyContexts = append(bad.ReplyContexts, bad.ReplyContexts[0])
					case "foreign":
						bad.ReplyContexts[0].RunID = uuid.NewString()
					case "corrupt_original":
						bad.ReplyContexts[0].SourceRequestEventID = "not-an-event"
					}
					err := runSelectedFixtureMutation(ctx, fixture.store, "reject uncaptured pending reply", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
						if s, ok := fixture.store.(*PostgresStore); ok {
							return s.runForkPostgresOwner.MaterializeRunForkProposedEffectCardsTx(txctx, attempt, bad, child.ForkRunID, target, projection, correspondence, childCards[0].CreatedAt, nil)
						}
						return fixture.store.(*SQLiteRuntimeStore).runForkSQLiteOwner.MaterializeRunForkProposedEffectCardsTx(txctx, attempt, bad, child.ForkRunID, target, projection, correspondence, childCards[0].CreatedAt, nil)
					})
					if err == nil || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatalf("uncaptured pending reply changed durable state/history: error=%v", err)
					}
				})
			}
		})
	}
}
