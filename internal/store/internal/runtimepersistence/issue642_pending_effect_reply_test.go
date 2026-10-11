package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
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
		})
	}
}
