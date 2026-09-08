package runtimepersistence

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeStoreConversationForkLifecycleParity(t *testing.T) {
	ctx := testAuthorActivityContext()
	s := newBootstrappedSQLiteRuntimeStoreForTest(t)
	if _, err := s.backend.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		t.Fatalf("enable production SQLite journal mode: %v", err)
	}
	now := activeConversationForkTestClock()
	source := seedSQLiteConversationForkSource(t, s, now)
	entityID := uuid.NewString()
	if _, err := s.backend.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES (?, ?, 'flow/forkchat', 'default', 'draft', '{}', '{}', '{}', '{}', 1, ?, ?, ?)
	`, source.runID, entityID, source.turn1At.Add(-30*time.Second), source.turn1At.Add(-30*time.Second), source.turn1At.Add(-30*time.Second)); err != nil {
		t.Fatalf("seed SQLite fork entity state: %v", err)
	}
	if _, err := s.backend.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, writer_type, writer_id, created_at
		) VALUES
			(?, ?, 'lifecycle_state', '', NULL, '"draft"', 'platform', 'test', ?),
			(?, ?, 'authored_field', 'removed', NULL, '"temporary"', 'platform', 'test', ?),
			(?, ?, 'authored_field', 'removed', '"temporary"', NULL, 'platform', 'test', ?),
			(?, ?, 'accumulator', 'removed', NULL, '9', 'platform', 'test', ?),
			(?, ?, 'accumulator', 'removed', '9', NULL, 'platform', 'test', ?),
			(?, ?, 'bookkeeping', 'removed', NULL, '"temporary"', 'platform', 'test', ?),
			(?, ?, 'bookkeeping', 'removed', '"temporary"', NULL, 'platform', 'test', ?),
			(?, ?, 'gate', 'removed', NULL, 'true', 'platform', 'test', ?),
			(?, ?, 'gate', 'removed', 'true', NULL, 'platform', 'test', ?)
	`,
		source.runID, entityID, source.turn1At.Add(-30*time.Second),
		source.runID, entityID, source.turn1At.Add(-25*time.Second),
		source.runID, entityID, source.turn1At.Add(-20*time.Second),
		source.runID, entityID, source.turn1At.Add(-25*time.Second),
		source.runID, entityID, source.turn1At.Add(-20*time.Second),
		source.runID, entityID, source.turn1At.Add(-25*time.Second),
		source.runID, entityID, source.turn1At.Add(-20*time.Second),
		source.runID, entityID, source.turn1At.Add(-25*time.Second),
		source.runID, entityID, source.turn1At.Add(-20*time.Second),
	); err != nil {
		t.Fatalf("seed SQLite fork deletion mutations: %v", err)
	}

	turnFork, err := s.CreateOperatorConversationFork(ctx, runfork.ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       runfork.ConversationForkPointSelector{Kind: "turn", TurnID: source.turn1ID},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork turn: %v", err)
	}
	if turnFork.SourceRunID != source.runID || turnFork.SourceAgentID != source.agentID || turnFork.ForkPoint.TurnID != source.turn1ID {
		t.Fatalf("turn fork lineage = %#v", turnFork)
	}
	if !turnFork.ExpiresAt.Equal(now.Add(runfork.ConversationForkLifecycleTTL)) || turnFork.State != "active" {
		t.Fatalf("turn fork lifecycle = %#v", turnFork)
	}

	eventFork, err := s.CreateOperatorConversationFork(ctx, runfork.ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       runfork.ConversationForkPointSelector{Kind: "event", EventID: source.event2ID},
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
	timeFork, err := s.CreateOperatorConversationFork(ctx, runfork.ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       runfork.ConversationForkPointSelector{Kind: "time", At: &timePoint},
		CreatedBy:       "actor-token",
		Now:             now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork time: %v", err)
	}
	if timeFork.ForkPoint.TurnIndex != 1 || timeFork.ForkPoint.TurnID != source.turn1ID || timeFork.ForkPoint.At == nil || !timeFork.ForkPoint.At.Equal(timePoint) {
		t.Fatalf("time fork point = %#v", timeFork.ForkPoint)
	}

	page, err := s.ListOperatorConversationForks(ctx, runfork.ConversationForkListOptions{SourceSessionID: source.sessionID, Limit: 2, Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks page 1: %v", err)
	}
	if len(page.Forks) != 2 || page.NextCursor == "" {
		t.Fatalf("page 1 = %#v", page)
	}
	page2, err := s.ListOperatorConversationForks(ctx, runfork.ConversationForkListOptions{SourceSessionID: source.sessionID, Limit: 2, Cursor: page.NextCursor, Now: now.Add(3 * time.Second)})
	if err != nil {
		t.Fatalf("ListOperatorConversationForks page 2: %v", err)
	}
	if len(page2.Forks) != 1 || page2.NextCursor != "" {
		t.Fatalf("page 2 = %#v", page2)
	}

	firstMessage := "inspect fork 0"
	prepared, err := s.PrepareOperatorConversationForkChat(ctx, runfork.ConversationForkChatPrepareRequest{
		ExecutionPosture: executionposture.Live,
		ForkID:           turnFork.ForkID, Message: firstMessage, Method: "conversation.fork_chat", ActorTokenID: "actor-token",
		RequestHash: runtimeeffects.Fingerprint([]byte(firstMessage)), Now: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("PrepareOperatorConversationForkChat: %v", err)
	}
	if prepared.Snapshot.SourceTurn.TurnID != source.turn1ID || prepared.Snapshot.SnapshotOwner != runfork.ConversationForkChatSnapshotOwner {
		t.Fatalf("prepared snapshot = %#v", prepared.Snapshot)
	}
	if len(prepared.Snapshot.EntitySnapshot) != 1 {
		t.Fatalf("SQLite snapshot entities = %#v", prepared.Snapshot.EntitySnapshot)
	}
	entity := prepared.Snapshot.EntitySnapshot[0]
	if entity.EntityID != entityID || entity.CurrentState != "draft" {
		t.Fatalf("SQLite reconstructed entity = %#v", entity)
	}
	if _, ok := entity.Fields["removed"]; ok {
		t.Fatalf("SQLite deleted authored field survived reconstruction: %#v", entity.Fields)
	}
	if _, ok := entity.Accumulator["removed"]; ok {
		t.Fatalf("SQLite deleted accumulator field survived reconstruction: %#v", entity.Accumulator)
	}
	if _, ok := entity.Bookkeeping["removed"]; ok {
		t.Fatalf("SQLite deleted bookkeeping fact survived reconstruction: %#v", entity.Bookkeeping)
	}
	if _, ok := entity.Gates["removed"]; ok {
		t.Fatalf("SQLite deleted gate survived reconstruction: %#v", entity.Gates)
	}

	const chatCount = 4
	turnIndexes := make(chan int, chatCount)
	errs := make(chan error, chatCount)
	var wg sync.WaitGroup
	for i := 0; i < chatCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			message := "inspect fork " + strconv.Itoa(i)
			turnPrepared := prepared
			if i > 0 {
				var err error
				turnPrepared, err = s.PrepareOperatorConversationForkChat(ctx, runfork.ConversationForkChatPrepareRequest{
					ExecutionPosture: executionposture.Live,
					ForkID:           turnFork.ForkID, Message: message, Method: "conversation.fork_chat", ActorTokenID: "actor-token",
					RequestHash: runtimeeffects.Fingerprint([]byte(message)), Now: now.Add(time.Duration(4+i) * time.Second),
				})
				if err != nil {
					errs <- err
					return
				}
			}
			settleForkChatCompletionForTest(t, ctx, s, turnPrepared, 1, now.Add(time.Duration(5+i)*time.Second))
			result, err := s.RecordOperatorConversationForkChat(ctx, runfork.ConversationForkChatRecordRequest{
				ForkID:       turnFork.ForkID,
				Message:      message,
				ActorTokenID: "actor-token",
				Prepared:     turnPrepared,
				Execution: runfork.ConversationForkChatExecution{
					AssistantMessage: "sandbox result",
					AvailableTools:   turnPrepared.AvailableTools,
					ExecutionOwner:   turnPrepared.ExecutionOwner,
					FenceGeneration:  turnPrepared.FenceGeneration,
					ToolCalls: []operatorread.OperatorConversationToolCall{{
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
	if err := s.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM conversation_fork_snapshots WHERE fork_id = ?`, turnFork.ForkID).Scan(&snapshots); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapshots != 1 {
		t.Fatalf("snapshot rows = %d, want 1", snapshots)
	}
	var normalTurns int
	if err := s.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE session_id = ?`, turnFork.ForkID).Scan(&normalTurns); err != nil {
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
		runID:      uuid.NewString(),
		bundleHash: authorActivityTestBundleHash,
		agentID:    "agent-fork-source",
		sessionID:  uuid.NewString(),
		turn1ID:    uuid.NewString(),
		turn2ID:    uuid.NewString(),
		event1ID:   uuid.NewString(),
		event2ID:   uuid.NewString(),
		turn1At:    base.Add(-2 * time.Minute),
		turn2At:    base.Add(-1 * time.Minute),
	}
	ctx := testAuthorActivityContext()
	requireRunFixtureForTest(t, ctx, s, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID: source.runID, BundleHash: source.bundleHash, StartedAt: base.Add(-3 * time.Minute),
	})
	identity := mustTestAgentIdentityForRun(source.runID, source.agentID, conversationForkSourceFlowInstance)
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("conversation fork source identity: %v", err)
	}
	seedTestAgentRow(t, ctx, s.backend.ConstructionHandle(), false, identity, "active")
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO agent_sessions (session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', 'active', ?, ?)`,
			[]any{source.sessionID, source.runID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, base.Add(-3 * time.Minute), base.Add(-3 * time.Minute)}},
	}
	for _, statement := range statements {
		if _, err := s.backend.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed SQLite conversation fork source: %v\nquery: %s", err, statement.query)
		}
	}
	for _, turn := range []struct {
		id, eventID, eventType string
		at                     time.Time
	}{
		{source.turn1ID, source.event1ID, "task.ready", source.turn1At},
		{source.turn2ID, source.event2ID, "task.done", source.turn2At},
	} {
		if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, ctx, s, runtimellm.AgentTurnRecord{
			AgentID: source.agentID, Identity: testAgentMemoryIdentity(t, source.runID, source.agentID, conversationForkSourceFlowInstance),
			Memory: agentmemory.Authored(true), SessionID: source.sessionID, RunID: source.runID,
			FlowInstance: conversationForkSourceFlowInstance, TriggerEventID: turn.eventID,
			TriggerEventType: turn.eventType, ParseOK: true,
		}, managedAgentTurnFixtureOptions{TurnID: turn.id, Now: turn.at}); err != nil {
			t.Fatalf("seed SQLite conversation fork turn %s: %v", turn.id, err)
		}
	}
	return source
}
