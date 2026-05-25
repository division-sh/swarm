package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/testutil"
)

func TestPostgresStore_ConversationForkLifecycleOwnsCreateListViewDelete(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	source := seedConversationForkSource(t, db, now)

	created, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnIndex: 1},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork turn: %v", err)
	}
	if created.ForkID == "" || created.SourceSessionID != source.sessionID || created.SourceRunID != source.runID || created.SourceAgentID != source.agentID {
		t.Fatalf("created fork lineage = %#v", created)
	}
	if created.ForkPoint.Kind != "turn" || created.ForkPoint.TurnIndex != 1 || created.ForkPoint.TurnID != source.turn1ID || created.ForkPoint.EventID != "" {
		t.Fatalf("created fork point = %#v", created.ForkPoint)
	}
	if created.CreatedBy != "actor-token" || !created.CreatedAt.Equal(now) || !created.ExpiresAt.Equal(now.Add(ConversationForkLifecycleTTL)) || created.State != "active" {
		t.Fatalf("created fork lifecycle fields = %#v", created)
	}
	if created.Turns == nil || len(created.Turns) != 0 {
		t.Fatalf("created fork turns = %#v, want empty array", created.Turns)
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
	if eventFork.ForkPoint.Kind != "event" || eventFork.ForkPoint.TurnIndex != 2 || eventFork.ForkPoint.TurnID != source.turn2ID || eventFork.ForkPoint.EventID != source.event2ID {
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
	if timeFork.ForkPoint.Kind != "time" || timeFork.ForkPoint.TurnIndex != 1 || timeFork.ForkPoint.TurnID != source.turn1ID || timeFork.ForkPoint.At == nil || !timeFork.ForkPoint.At.Equal(timePoint) {
		t.Fatalf("time fork point = %#v", timeFork.ForkPoint)
	}

	page, err := s.ListOperatorConversationForks(ctx, ConversationForkListOptions{
		SourceSessionID: source.sessionID,
		Limit:           2,
		Now:             now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks page1: %v", err)
	}
	if len(page.Forks) != 2 || page.NextCursor == "" {
		t.Fatalf("page1 = %#v, want 2 forks and cursor", page)
	}
	page2, err := s.ListOperatorConversationForks(ctx, ConversationForkListOptions{
		SourceSessionID: source.sessionID,
		Limit:           2,
		Cursor:          page.NextCursor,
		Now:             now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks page2: %v", err)
	}
	if len(page2.Forks) != 1 || page2.NextCursor != "" {
		t.Fatalf("page2 = %#v, want 1 final fork", page2)
	}

	loaded, err := s.LoadOperatorConversationFork(ctx, created.ForkID)
	if err != nil {
		t.Fatalf("LoadOperatorConversationFork: %v", err)
	}
	if loaded.ForkID != created.ForkID || loaded.State != "active" {
		t.Fatalf("loaded fork = %#v", loaded)
	}

	deleted, err := s.DeleteOperatorConversationFork(ctx, created.ForkID, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("DeleteOperatorConversationFork: %v", err)
	}
	if !deleted.Deleted || deleted.AlreadyDeleted || deleted.ForkID != created.ForkID {
		t.Fatalf("deleted result = %#v", deleted)
	}
	deletedAgain, err := s.DeleteOperatorConversationFork(ctx, created.ForkID, now.Add(5*time.Second))
	if err != nil {
		t.Fatalf("DeleteOperatorConversationFork again: %v", err)
	}
	if deletedAgain.Deleted || !deletedAgain.AlreadyDeleted {
		t.Fatalf("deleted again result = %#v", deletedAgain)
	}

	pageAfterDelete, err := s.ListOperatorConversationForks(ctx, ConversationForkListOptions{SourceSessionID: source.sessionID, Limit: 10, Now: now.Add(6 * time.Second)})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks after delete: %v", err)
	}
	for _, item := range pageAfterDelete.Forks {
		if item.ForkID == created.ForkID {
			t.Fatalf("deleted fork survived active list: %#v", pageAfterDelete.Forks)
		}
	}

	var normalSessionRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE session_id = $1::uuid`, created.ForkID).Scan(&normalSessionRows); err != nil {
		t.Fatalf("count normal session rows for fork id: %v", err)
	}
	if normalSessionRows != 0 {
		t.Fatalf("fork lifecycle leaked into agent_sessions rows = %d", normalSessionRows)
	}
}

func TestPostgresStore_ConversationForkLifecycleFailsClosedForSelectors(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	source := seedConversationForkSource(t, db, now)

	_, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "event", EventID: uuid.NewString()},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("event selector mismatch error = %v, want ErrEventNotFound", err)
	}

	_, err = s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnIndex: 1, EventID: source.event1ID},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	var paramErr *EntityReadParamError
	if !errors.As(err, &paramErr) || paramErr.Field != "fork_point" {
		t.Fatalf("mixed selector error = %v, want fork_point param error", err)
	}

	beforeFirstTurn := source.turn1At.Add(-time.Second)
	_, err = s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "time", At: &beforeFirstTurn},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if !errors.As(err, &paramErr) || paramErr.Field != "fork_point.at" {
		t.Fatalf("time selector before first turn error = %v, want fork_point.at param error", err)
	}

	_, err = s.ListOperatorConversationForks(ctx, ConversationForkListOptions{Cursor: "not-a-cursor", Now: now})
	if !errors.Is(err, ErrInvalidConversationForkCursor) {
		t.Fatalf("invalid cursor error = %v, want ErrInvalidConversationForkCursor", err)
	}
}

func TestPostgresStore_ConversationForkChatOwnsSnapshotTranscriptAndIsolation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	source := seedConversationForkSource(t, db, now)
	entityID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow/forkchat', 'default',
			'after', '{}'::jsonb, '{"name":"After"}'::jsonb, '{}'::jsonb, 2,
			$3, $3, $3
		)
	`, source.runID, entityID, source.turn1At.Add(10*time.Second)); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (run_id, entity_id, field, old_value, new_value, writer_type, writer_id, created_at)
		VALUES
			($1::uuid, $2::uuid, 'current_state', NULL, '"draft"'::jsonb, 'platform', 'test', $3),
			($1::uuid, $2::uuid, 'name', NULL, '"Before"'::jsonb, 'platform', 'test', $3),
			($1::uuid, $2::uuid, 'current_state', '"draft"'::jsonb, '"after"'::jsonb, 'platform', 'test', $4),
			($1::uuid, $2::uuid, 'name', '"Before"'::jsonb, '"After"'::jsonb, 'platform', 'test', $4)
	`, source.runID, entityID, source.turn1At.Add(-30*time.Second), source.turn1At.Add(10*time.Second)); err != nil {
		t.Fatalf("seed source entity mutations: %v", err)
	}
	var mutationsBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, source.runID).Scan(&mutationsBefore); err != nil {
		t.Fatalf("count source mutations before chat: %v", err)
	}

	fork, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnIndex: 1},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork: %v", err)
	}
	result, err := s.ChatOperatorConversationFork(ctx, ConversationForkChatRequest{
		ForkID:       fork.ForkID,
		Message:      "inspect the fork",
		ActorTokenID: "actor-token",
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("ChatOperatorConversationFork: %v", err)
	}
	if result.ForkID != fork.ForkID || result.Turn.TurnIndex != 1 || result.Turn.TurnID == "" || !result.Turn.ParseOK {
		t.Fatalf("chat result turn = %#v", result)
	}
	if result.Snapshot.SnapshotOwner != ConversationForkChatSnapshotOwner || result.SandboxPolicy.Owner != ConversationForkChatSandboxOwner {
		t.Fatalf("owners = snapshot %q policy %q", result.Snapshot.SnapshotOwner, result.SandboxPolicy.Owner)
	}
	if len(result.Snapshot.EntitySnapshot) != 1 {
		t.Fatalf("snapshot entities = %#v, want one reconstructed entity", result.Snapshot.EntitySnapshot)
	}
	entity := result.Snapshot.EntitySnapshot[0]
	if entity.EntityID != entityID || entity.CurrentState != "draft" || entity.Fields["name"] != "Before" {
		t.Fatalf("entity snapshot = %#v, want source-at-fork projection", entity)
	}

	loaded, err := s.LoadOperatorConversationFork(ctx, fork.ForkID)
	if err != nil {
		t.Fatalf("LoadOperatorConversationFork after chat: %v", err)
	}
	if len(loaded.Turns) != 1 || loaded.Turns[0].TurnID != result.Turn.TurnID {
		t.Fatalf("fork_view turns = %#v, want fork-local chat turn", loaded.Turns)
	}

	var mutationsAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, source.runID).Scan(&mutationsAfter); err != nil {
		t.Fatalf("count source mutations after chat: %v", err)
	}
	if mutationsAfter != mutationsBefore {
		t.Fatalf("source mutations changed from %d to %d; forkchat must not live-mutate", mutationsBefore, mutationsAfter)
	}
	var normalTurns int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE session_id = $1::uuid`, fork.ForkID).Scan(&normalTurns); err != nil {
		t.Fatalf("count normal agent_turns for fork id: %v", err)
	}
	if normalTurns != 0 {
		t.Fatalf("forkchat leaked into agent_turns rows = %d", normalTurns)
	}
	var runtimeEvents int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_name = 'platform.runtime_log' AND produced_by = $1`, fork.ForkID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count runtime log events for fork id: %v", err)
	}
	if runtimeEvents != 0 {
		t.Fatalf("forkchat leaked runtime log events = %d", runtimeEvents)
	}

	if _, err := s.DeleteOperatorConversationFork(ctx, fork.ForkID, now.Add(2*time.Second)); err != nil {
		t.Fatalf("DeleteOperatorConversationFork: %v", err)
	}
	_, err = s.ChatOperatorConversationFork(ctx, ConversationForkChatRequest{
		ForkID:       fork.ForkID,
		Message:      "should fail",
		ActorTokenID: "actor-token",
		Now:          now.Add(3 * time.Second),
	})
	var paramErr *EntityReadParamError
	if !errors.As(err, &paramErr) || paramErr.Field != "fork_id" {
		t.Fatalf("deleted fork chat error = %v, want fork_id invalid params", err)
	}
}

type conversationForkSourceFixture struct {
	runID     string
	agentID   string
	sessionID string
	turn1ID   string
	turn2ID   string
	event1ID  string
	event2ID  string
	turn1At   time.Time
	turn2At   time.Time
}

func seedConversationForkSource(t *testing.T, db execQueryer, base time.Time) conversationForkSourceFixture {
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
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, source.runID, base.Add(-3*time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model_tier, conversation_mode)
		VALUES ($1, 'researcher', 'haiku', 'session')
	`, source.agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope,
			runtime_mode, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, 'global', 'global', 'session', 'active', $4, $4)
	`, source.sessionID, source.runID, source.agentID, base.Add(-3*time.Minute)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key,
			trigger_event_id, trigger_event_type, parse_ok, created_at
		)
		VALUES
			($1::uuid, $2::uuid, $3, $4::uuid, 'session', 'global', $5::uuid, 'task.ready', true, $6),
			($7::uuid, $2::uuid, $3, $4::uuid, 'session', 'global', $8::uuid, 'task.done', true, $9)
	`, source.turn1ID, source.runID, source.agentID, source.sessionID, source.event1ID, source.turn1At, source.turn2ID, source.event2ID, source.turn2At); err != nil {
		t.Fatalf("seed turns: %v", err)
	}
	return source
}
