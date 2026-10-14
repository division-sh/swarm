package serveapp

import (
	"cmp"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
)

func readStandingPublicTranscript(t *testing.T, baseURL, runID string, wantTurns int) map[string][]operatorread.OperatorPublicConversationTurn {
	t.Helper()
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/rpc"
	transcript := make(map[string][]operatorread.OperatorPublicConversationTurn)
	cursor := ""
	seenCursors := make(map[string]bool)
	for {
		var page operatorread.OperatorConversationListResult
		requireServedJSONRPCResult(t, endpoint, "conversation.list", map[string]any{"run_id": runID, "limit": 1, "cursor": cursor}, &page)
		for _, session := range page.Conversations {
			if session.SessionID == "" || session.RunID != runID || transcript[session.SessionID] != nil {
				t.Fatalf("public transcript session is missing, foreign or duplicate: %+v", session)
			}
			turnCursor := ""
			turnCursors := make(map[string]bool)
			seenTurns := make(map[string]bool)
			var turns []operatorread.OperatorPublicConversationTurn
			for {
				var turnPage operatorread.OperatorConversationTurnListResult
				requireServedJSONRPCResult(t, endpoint, "conversation.list_turns", map[string]any{"session_id": session.SessionID, "limit": 1, "cursor": turnCursor}, &turnPage)
				if turnPage.Conversation.SessionID != session.SessionID || turnPage.Conversation.MessageCount != session.MessageCount {
					t.Fatalf("turn page changed whole-session count or identity: %+v", turnPage.Conversation)
				}
				for _, turn := range turnPage.Turns {
					if turn.TurnID == "" || seenTurns[turn.TurnID] || turn.Ordinal <= 0 || !turn.ParseOK {
						t.Fatalf("public transcript has a missing, duplicate or incomplete turn: %+v", turn)
					}
					var exact operatorread.OperatorPublicConversationTurnDetail
					requireServedJSONRPCResult(t, endpoint, "conversation.get_turn", map[string]any{"session_id": session.SessionID, "turn_id": turn.TurnID}, &exact)
					shared := operatorread.OperatorConversationTurnListItem{
						CompletionEvidence: exact.Turn.CompletionEvidence.Clone(), ResolvedModel: exact.Turn.ResolvedModel,
						TurnID: exact.Turn.TurnID, ExecutionMode: exact.Turn.ExecutionMode, Ordinal: exact.Turn.Ordinal,
						CompletedAt: exact.Turn.CompletedAt, DurationMS: exact.Turn.DurationMS,
						TriggerEventID: exact.Turn.TriggerEventID, TriggerEventType: exact.Turn.TriggerEventType,
						Tokens: exact.Turn.Tokens, Outcome: exact.Turn.Outcome, ParseOK: exact.Turn.ParseOK, Failure: exact.Turn.Failure,
						ActivityCounts: turn.ActivityCounts,
					}
					counts := turn.ActivityCounts
					activityCount := counts.Dispatch + counts.Tool + counts.ToolResult + counts.Publish + counts.Output + counts.Failure
					if exact.Session.SessionID != session.SessionID || exact.Session.MessageCount != session.MessageCount ||
						!reflect.DeepEqual(turn, shared) || len(exact.Turn.Activity) == 0 || len(exact.Turn.Activity) != activityCount {
						t.Fatalf("exact turn differs from paginated transcript: page=%+v exact=%+v", turn, exact)
					}
					seenTurns[turn.TurnID] = true
					turns = append(turns, exact.Turn)
				}
				if turnPage.NextCursor == "" {
					break
				}
				if turnCursors[turnPage.NextCursor] {
					t.Fatalf("turn page repeated cursor %q", turnPage.NextCursor)
				}
				turnCursors[turnPage.NextCursor] = true
				turnCursor = turnPage.NextCursor
			}
			if session.MessageCount != len(turns) {
				t.Fatalf("public message count %d differs from complete session turns %d", session.MessageCount, len(turns))
			}
			slices.SortFunc(turns, func(a, b operatorread.OperatorPublicConversationTurn) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
			for i, turn := range turns {
				if turn.Ordinal != i+1 {
					t.Fatalf("session %s lost canonical turn ordinal: %+v", session.SessionID, turns)
				}
			}
			transcript[session.SessionID] = turns
		}
		if page.NextCursor == "" {
			break
		}
		if seenCursors[page.NextCursor] {
			t.Fatalf("conversation page repeated cursor %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
	count := 0
	for _, turns := range transcript {
		count += len(turns)
	}
	if len(transcript) != 2 || count != wantTurns {
		t.Fatalf("public transcript sessions=%d turns=%d, want two isolated sessions and %d turns", len(transcript), count, wantTurns)
	}
	return transcript
}
