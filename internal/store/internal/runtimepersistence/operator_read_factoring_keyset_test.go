package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestOperatorConversationKeysetInsertionParity(t *testing.T) {
	for _, name := range []string{"sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			backend := newOperatorConversationProjectionTestBackend(t, name)
			fixture := seedOperatorConversationProjectionFixture(t, backend)
			ctx := testAuthorActivityContext()
			first, err := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2})
			if err != nil || len(first.Turns) != 2 || first.NextCursor == "" {
				t.Fatalf("first page=%#v err=%v", first, err)
			}
			before, err := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2, Cursor: first.NextCursor})
			if err != nil || len(before.Turns) != 2 || before.NextCursor != "" {
				t.Fatalf("continuation before insertion=%#v err=%v", before, err)
			}
			const runID = "00000000-0000-4000-8000-000000000001"
			identity := mustTestAgentIdentityForRun(runID, "agent-public-conversation-parity", "conversation")
			turnID, eventID := uuid.NewString(), uuid.NewString()
			seedOperatorConversationProjectionTurn(t, backend, operatorConversationProjectionTurnSeed{
				identity: identity, runID: runID, sessionID: fixture.sessionID,
				turnID: turnID, triggerEventID: eventID, triggerType: "task.inserted",
				turnBlocks: "[]", parseOK: true, createdAt: fixture.tieAt.Add(2 * time.Minute),
			})
			if _, err := backend.settlement.SettleSuccess(ctx, backend.claims[eventID], nil, time.Millisecond, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			after, err := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 2, Cursor: first.NextCursor})
			if err != nil || !reflect.DeepEqual(after.Turns, before.Turns) || after.NextCursor != "" {
				t.Fatalf("insertion shifted the existing keyset: before=%#v after=%#v err=%v", before, after, err)
			}
			latest, err := backend.store.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: fixture.sessionID, Limit: 1})
			if err != nil || len(latest.Turns) != 1 || latest.Turns[0].TurnID != turnID {
				t.Fatalf("insert was not publicly visible: latest=%#v err=%v", latest, err)
			}
		})
	}
}
