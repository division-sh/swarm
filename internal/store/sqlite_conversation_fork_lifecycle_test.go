package store

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/testutil"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreConversationForkLifecycleParity(t *testing.T) {
	ctx := context.Background()
	s := newBootstrappedSQLiteRuntimeStoreForTest(t, testutil.SQLiteDefaultTemp())
	now := activeConversationForkTestClock()
	source := seedSQLiteConversationForkSource(t, s, now)

	turnFork, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnIndex: 1},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork turn: %v", err)
	}
	if turnFork.SourceRunID != source.runID || turnFork.SourceAgentID != source.agentID || turnFork.ForkPoint.TurnID != source.turn1ID {
		t.Fatalf("turn fork lineage = %#v", turnFork)
	}
	if !turnFork.ExpiresAt.Equal(now.Add(ConversationForkLifecycleTTL)) || turnFork.State != "active" {
		t.Fatalf("turn fork lifecycle = %#v", turnFork)
	}

	eventFork, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "event", EventID: source.event2ID},
		CreatedBy:       "actor-token",
		Now:             now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork event: %v", err)
	}
	if eventFork.ForkPoint.TurnIndex != 2 || eventFork.ForkPoint.TurnID != source.turn2ID || eventFork.ForkPoint.EventID != source.event2ID {
		t.Fatalf("event fork point = %#v", eventFork.ForkPoint)
	}

	timePoint := source.turn1At.Add(30 * time.Second)
	timeFork, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "time", At: &timePoint},
		CreatedBy:       "actor-token",
		Now:             now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork time: %v", err)
	}
	if timeFork.ForkPoint.TurnIndex != 1 || timeFork.ForkPoint.TurnID != source.turn1ID || timeFork.ForkPoint.At == nil || !timeFork.ForkPoint.At.Equal(timePoint) {
		t.Fatalf("time fork point = %#v", timeFork.ForkPoint)
	}

	page, err := s.ListOperatorConversationForks(ctx, ConversationForkListOptions{SourceSessionID: source.sessionID, Limit: 2, Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks page 1: %v", err)
	}
	if len(page.Forks) != 2 || page.NextCursor == "" {
		t.Fatalf("page 1 = %#v", page)
	}
	page2, err := s.ListOperatorConversationForks(ctx, ConversationForkListOptions{SourceSessionID: source.sessionID, Limit: 2, Cursor: page.NextCursor, Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks page 2: %v", err)
	}
	if len(page2.Forks) != 1 || page2.NextCursor != "" {
		t.Fatalf("page 2 = %#v", page2)
	}

	prepared, err := s.PrepareOperatorConversationForkChat(ctx, ConversationForkChatPrepareRequest{ForkID: turnFork.ForkID, Now: now.Add(4 * time.Second)})
	if err != nil {
		t.Fatalf("PrepareOperatorConversationForkChat: %v", err)
	}
	if prepared.Snapshot.SourceTurn.TurnID != source.turn1ID || prepared.Snapshot.SnapshotOwner != ConversationForkChatSnapshotOwner {
		t.Fatalf("prepared snapshot = %#v", prepared.Snapshot)
	}

	const chatCount = 4
	turnIndexes := make(chan int, chatCount)
	errs := make(chan error, chatCount)
	var wg sync.WaitGroup
	for i := 0; i < chatCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, err := s.RecordOperatorConversationForkChat(ctx, ConversationForkChatRecordRequest{
				ForkID:       turnFork.ForkID,
				Message:      "inspect fork",
				ActorTokenID: "actor-token",
				Execution: ConversationForkChatExecution{
					AssistantMessage: "sandbox result",
					AvailableTools:   prepared.AvailableTools,
					ToolCalls: []OperatorConversationToolCall{{
						ToolUseID: "tool-" + uuid.NewString(),
						Name:      "emit_event",
						Arguments: json.RawMessage(`{"event_name":"forkchat.note"}`),
						Result:    json.RawMessage(`{"status":"stubbed","live_mutation":false}`),
					}},
				},
				Now: now.Add(time.Duration(5+i) * time.Second),
			})
			if err != nil {
				errs <- err
				return
			}
			turnIndexes <- result.Turn.TurnIndex
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("RecordOperatorConversationForkChat concurrent: %v", err)
	}
	close(turnIndexes)
	gotIndexes := make([]int, 0, chatCount)
	for index := range turnIndexes {
		gotIndexes = append(gotIndexes, index)
	}
	sort.Ints(gotIndexes)
	for i, got := range gotIndexes {
		if want := i + 1; got != want {
			t.Fatalf("turn indexes = %v, want adjacent 1..%d", gotIndexes, chatCount)
		}
	}

	loaded, err := s.LoadOperatorConversationFork(ctx, turnFork.ForkID)
	if err != nil {
		t.Fatalf("LoadOperatorConversationFork: %v", err)
	}
	if len(loaded.Turns) != chatCount {
		t.Fatalf("loaded turns = %d, want %d", len(loaded.Turns), chatCount)
	}
	var snapshots int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM conversation_fork_snapshots WHERE fork_id = ?`, turnFork.ForkID).Scan(&snapshots); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapshots != 1 {
		t.Fatalf("snapshot rows = %d, want 1", snapshots)
	}
	var normalTurns int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE session_id = ?`, turnFork.ForkID).Scan(&normalTurns); err != nil {
		t.Fatalf("count leaked normal turns: %v", err)
	}
	if normalTurns != 0 {
		t.Fatalf("fork chat leaked %d normal turns", normalTurns)
	}

	deleted, err := s.DeleteOperatorConversationFork(ctx, turnFork.ForkID, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("DeleteOperatorConversationFork: %v", err)
	}
	if !deleted.Deleted || deleted.AlreadyDeleted {
		t.Fatalf("delete result = %#v", deleted)
	}
	deletedAgain, err := s.DeleteOperatorConversationFork(ctx, turnFork.ForkID, now.Add(11*time.Second))
	if err != nil {
		t.Fatalf("DeleteOperatorConversationFork replay: %v", err)
	}
	if deletedAgain.Deleted || !deletedAgain.AlreadyDeleted {
		t.Fatalf("delete replay = %#v", deletedAgain)
	}
}

func seedSQLiteConversationForkSource(t *testing.T, s *SQLiteRuntimeStore, base time.Time) conversationForkSourceFixture {
	t.Helper()
	source := conversationForkSourceFixture{
		runID:     uuid.NewString(),
		agentID:   "agent-fork-source",
		sessionID: uuid.NewString(),
		turn1ID:   uuid.NewString(),
		turn2ID:   uuid.NewString(),
		event1ID:  uuid.NewString(),
		event2ID:  uuid.NewString(),
		turn1At:   base.Add(-2 * time.Minute),
		turn2At:   base.Add(-1 * time.Minute),
	}
	ctx := context.Background()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, []any{source.runID, base.Add(-3 * time.Minute)}},
		{`INSERT INTO agents (agent_id, role, model, conversation_mode) VALUES (?, 'researcher', 'haiku', 'session')`, []any{source.agentID}},
		{`INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status, created_at, updated_at) VALUES (?, ?, ?, 'global', 'global', 'session', 'active', ?, ?)`, []any{source.sessionID, source.runID, source.agentID, base.Add(-3 * time.Minute), base.Add(-3 * time.Minute)}},
		{`INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, trigger_event_id, trigger_event_type, parse_ok, created_at) VALUES (?, ?, ?, ?, 'session', 'global', ?, 'task.ready', true, ?), (?, ?, ?, ?, 'session', 'global', ?, 'task.done', true, ?)`, []any{source.turn1ID, source.runID, source.agentID, source.sessionID, source.event1ID, source.turn1At, source.turn2ID, source.runID, source.agentID, source.sessionID, source.event2ID, source.turn2At}},
	}
	for _, statement := range statements {
		if _, err := s.DB.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed SQLite conversation fork source: %v\nquery: %s", err, statement.query)
		}
	}
	return source
}
