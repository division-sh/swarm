package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_ConversationForkLifecycleOwnsCreateListViewDelete(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()
	now := activeConversationForkTestClock()
	source := seedConversationForkSource(t, db, now)

	created, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnID: source.turn1ID},
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

func activeConversationForkTestClock() time.Time {
	return time.Now().UTC().Truncate(time.Second)
}

func TestPostgresStore_ConversationForkLifecycleFailsClosedForSelectors(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()
	now := activeConversationForkTestClock()
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
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnID: source.turn1ID, EventID: source.event1ID},
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
	ctx := testAuthorActivityContext()
	now := activeConversationForkTestClock()
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
	var eventsBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventsBefore); err != nil {
		t.Fatalf("count events before chat: %v", err)
	}
	var mailboxBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox`).Scan(&mailboxBefore); err != nil {
		t.Fatalf("count mailbox before chat: %v", err)
	}
	var runsBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&runsBefore); err != nil {
		t.Fatalf("count runs before chat: %v", err)
	}

	fork, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnID: source.turn1ID},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork: %v", err)
	}
	prepared, err := s.PrepareOperatorConversationForkChat(ctx, ConversationForkChatPrepareRequest{
		ForkID: fork.ForkID, Message: "inspect the fork", Method: "conversation.fork_chat",
		ActorTokenID: "actor-token", RequestHash: runtimeeffects.Fingerprint([]byte("inspect the fork")),
		Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("PrepareOperatorConversationForkChat: %v", err)
	}
	settleForkChatCompletionForTest(t, ctx, s, prepared, 1, now.Add(1500*time.Millisecond))
	result, err := s.RecordOperatorConversationForkChat(ctx, ConversationForkChatRecordRequest{
		ForkID:       fork.ForkID,
		Message:      "inspect the fork",
		ActorTokenID: "actor-token",
		Prepared:     prepared,
		Execution: ConversationForkChatExecution{
			AssistantMessage: "snapshot says Before; requested writes were stubbed",
			AvailableTools:   prepared.AvailableTools,
			ExecutionOwner:   prepared.ExecutionOwner,
			FenceGeneration:  prepared.FenceGeneration,
			ToolCalls: []OperatorConversationToolCall{
				{
					ToolUseID: "tool-1",
					Name:      "fork_snapshot_read_entities",
					Arguments: json.RawMessage(`{"entity_id":"` + entityID + `"}`),
					Result:    json.RawMessage(`{"status":"read_from_snapshot","read_policy":"fork_snapshot_only","snapshot_owner":"conversation.fork_chat.snapshot.v1","source_agent_id":"` + source.agentID + `","entity_count":1}`),
				},
				{
					ToolUseID: "tool-2",
					Name:      "save_entity_field",
					Arguments: json.RawMessage(`{"entity_id":"` + entityID + `","field":"name","value":"Sandbox"}`),
					Result:    json.RawMessage(`{"status":"stubbed","owner":"conversation.fork_chat.sandbox.v1","write_policy":"stub_record_only_no_live_mutation","requested_tool":"save_entity_field","live_mutation":false}`),
				},
				{
					ToolUseID: "tool-3",
					Name:      "emit_event",
					Arguments: json.RawMessage(`{"event_name":"forkchat.note"}`),
					Result:    json.RawMessage(`{"status":"stubbed","owner":"conversation.fork_chat.sandbox.v1","write_policy":"stub_record_only_no_live_mutation","requested_tool":"emit_event","live_mutation":false}`),
				},
				{
					ToolUseID: "tool-4",
					Name:      "run_start",
					Arguments: json.RawMessage(`{"event_name":"scan.requested"}`),
					Result:    json.RawMessage(`{"status":"stubbed","owner":"conversation.fork_chat.sandbox.v1","write_policy":"stub_record_only_no_live_mutation","requested_tool":"run.start","live_mutation":false}`),
				},
			},
		},
		Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("RecordOperatorConversationForkChat: %v", err)
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
	readCall := requireConversationForkToolCall(t, result.Turn.ToolCalls, "fork_snapshot_read_entities")
	readArgs := conversationForkToolCallMap(t, readCall.Arguments)
	if readArgs["entity_id"] != entityID {
		t.Fatalf("snapshot read tool args = %#v", readArgs)
	}
	readResult := conversationForkToolCallMap(t, readCall.Result)
	if readResult["status"] != "read_from_snapshot" || readResult["snapshot_owner"] != ConversationForkChatSnapshotOwner || readResult["entity_count"] != float64(1) {
		t.Fatalf("snapshot read tool result = %#v", readResult)
	}
	for _, toolName := range []string{"save_entity_field", "emit_event", "run_start"} {
		call := requireConversationForkToolCall(t, result.Turn.ToolCalls, toolName)
		stub := conversationForkToolCallMap(t, call.Result)
		if stub["status"] != "stubbed" || stub["owner"] != ConversationForkChatSandboxOwner || stub["live_mutation"] != false {
			t.Fatalf("%s stub result = %#v", toolName, stub)
		}
	}
	if len(result.Turn.ToolCalls) != 4 {
		t.Fatalf("tool calls = %#v, want only requested snapshot/stub tool evidence", result.Turn.ToolCalls)
	}
	if call := findConversationForkToolCall(result.Turn.ToolCalls, "run_stop"); call != nil {
		t.Fatalf("unrequested tool call persisted: %#v", *call)
	}
	var responsePayload struct {
		ToolCalls []OperatorConversationToolCall `json:"tool_calls"`
	}
	if err := json.Unmarshal(result.Turn.ResponsePayload, &responsePayload); err != nil {
		t.Fatalf("decode forkchat response payload: %v", err)
	}
	if len(responsePayload.ToolCalls) != len(result.Turn.ToolCalls) {
		t.Fatalf("response payload tool_calls = %d, want %d", len(responsePayload.ToolCalls), len(result.Turn.ToolCalls))
	}

	loaded, err := s.LoadOperatorConversationFork(ctx, fork.ForkID)
	if err != nil {
		t.Fatalf("LoadOperatorConversationFork after chat: %v", err)
	}
	if len(loaded.Turns) != 1 || loaded.Turns[0].TurnID != result.Turn.TurnID {
		t.Fatalf("fork_view turns = %#v, want fork-local chat turn", loaded.Turns)
	}
	if len(loaded.Turns[0].ToolCalls) != len(result.Turn.ToolCalls) {
		t.Fatalf("fork_view tool calls = %#v, want persisted sandbox evidence", loaded.Turns[0].ToolCalls)
	}

	var mutationsAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, source.runID).Scan(&mutationsAfter); err != nil {
		t.Fatalf("count source mutations after chat: %v", err)
	}
	if mutationsAfter != mutationsBefore {
		t.Fatalf("source mutations changed from %d to %d; forkchat must not live-mutate", mutationsBefore, mutationsAfter)
	}
	var eventsAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventsAfter); err != nil {
		t.Fatalf("count events after chat: %v", err)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf("events changed from %d to %d; forkchat must not emit live events", eventsBefore, eventsAfter)
	}
	var mailboxAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox`).Scan(&mailboxAfter); err != nil {
		t.Fatalf("count mailbox after chat: %v", err)
	}
	if mailboxAfter != mailboxBefore {
		t.Fatalf("mailbox changed from %d to %d; forkchat must not mutate mailbox decisions", mailboxBefore, mailboxAfter)
	}
	var runsAfter int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&runsAfter); err != nil {
		t.Fatalf("count runs after chat: %v", err)
	}
	if runsAfter != runsBefore {
		t.Fatalf("runs changed from %d to %d; forkchat must not start or mutate live runs", runsBefore, runsAfter)
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
	_, err = s.PrepareOperatorConversationForkChat(ctx, ConversationForkChatPrepareRequest{
		ForkID: fork.ForkID, Message: "after delete", Method: "conversation.fork_chat",
		ActorTokenID: "actor-token", RequestHash: runtimeeffects.Fingerprint([]byte("after delete")),
		Now: now.Add(3 * time.Second),
	})
	var paramErr *EntityReadParamError
	if !errors.As(err, &paramErr) || paramErr.Field != "fork_id" {
		t.Fatalf("deleted fork chat error = %v, want fork_id invalid params", err)
	}
}

func TestPostgresStore_ConversationForkChatAllocatesConcurrentTurns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()
	now := activeConversationForkTestClock()
	source := seedConversationForkSource(t, db, now)
	fork, err := s.CreateOperatorConversationFork(ctx, ConversationForkCreateRequest{
		SourceSessionID: source.sessionID,
		ForkPoint:       ConversationForkPointSelector{Kind: "turn", TurnID: source.turn1ID},
		CreatedBy:       "actor-token",
		Now:             now,
	})
	if err != nil {
		t.Fatalf("CreateOperatorConversationFork: %v", err)
	}
	const count = 4
	indexes := make(chan int, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			message := fmt.Sprintf("concurrent fork chat %d", i)
			prepared, err := s.PrepareOperatorConversationForkChat(ctx, ConversationForkChatPrepareRequest{
				ForkID: fork.ForkID, Message: message, Method: "conversation.fork_chat", ActorTokenID: "actor-token",
				RequestHash: runtimeeffects.Fingerprint([]byte(message)), Now: now.Add(time.Duration(i+1) * time.Second),
			})
			if err != nil {
				errs <- err
				return
			}
			settleForkChatCompletionForTest(t, ctx, s, prepared, 1, now.Add(time.Duration(i+2)*time.Second))
			result, err := s.RecordOperatorConversationForkChat(ctx, ConversationForkChatRecordRequest{
				ForkID:       fork.ForkID,
				Message:      message,
				ActorTokenID: "actor-token",
				Prepared:     prepared,
				Execution: ConversationForkChatExecution{
					AssistantMessage: "concurrent result",
					AvailableTools:   prepared.AvailableTools,
					ExecutionOwner:   prepared.ExecutionOwner,
					FenceGeneration:  prepared.FenceGeneration,
				},
				Now: now.Add(time.Duration(i+2) * time.Second),
			})
			if err != nil {
				errs <- err
				return
			}
			indexes <- result.Turn.TurnIndex
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("RecordOperatorConversationForkChat concurrent: %v", err)
	}
	close(indexes)
	got := make([]int, 0, count)
	for index := range indexes {
		got = append(got, index)
	}
	sort.Ints(got)
	for i, index := range got {
		if want := i + 1; index != want {
			t.Fatalf("turn indexes = %v, want adjacent 1..%d", got, count)
		}
	}
}

type forkChatCompletionTestStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
}

func settleForkChatCompletionForTest(t *testing.T, ctx context.Context, s forkChatCompletionTestStore, prepared ConversationForkChatPrepared, ordinal int, now time.Time) {
	t.Helper()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityConversationForkChat, ID: prepared.ForkTurnID,
		ExecutionOwner: prepared.ExecutionOwner, LeaseExpiresAt: prepared.LeaseExpiresAt, FenceGeneration: prepared.FenceGeneration,
		ExecutionMode: prepared.Snapshot.SourceAgent.ExecutionMode,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{
			ForkTurnID: prepared.ForkTurnID, ForkID: prepared.Fork.ForkID, BundleHash: prepared.SourceBundleHash, ActorTokenID: prepared.ActorTokenID,
			RequestOccurrenceID: prepared.RequestOccurrenceID, RequestHash: prepared.RequestHash,
		},
		Target: runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetConversationForkCompletion, ID: prepared.ForkTurnID, Ordinal: ordinal},
	}
	completionCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), runtimeeffects.NewController(s))
	completionCtx = runtimeeffects.WithLogicalOperationIdentity(completionCtx, fmt.Sprintf("forkchat-test:%s:%d", prepared.RequestOccurrenceID, ordinal))
	handle, err := runtimeeffects.BeginCompletion(completionCtx, "anthropic_api", []byte(prepared.RequestHash), nil)
	if err != nil {
		t.Fatalf("authorize forkchat completion: %v", err)
	}
	if err := handle.MarkLaunched(completionCtx); err != nil {
		t.Fatalf("launch forkchat completion: %v", err)
	}
	if err := handle.MarkResponseObserved(completionCtx, map[string]any{"test": true}); err != nil {
		t.Fatalf("observe forkchat completion: %v", err)
	}
	input, output := int64(2), int64(1)
	err = handle.SettleCompletion(completionCtx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"test": true}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: prepared.Fork.ForkID, AgentID: prepared.Fork.SourceAgentID, Model: "test-model",
			ModelAlias: "regular", BackendProfile: "anthropic", Provider: "anthropic", Transport: "http",
			ResolvedModel: "test-model", InvocationType: "forkchat",
		},
		Now: now.UTC(),
	})
	if err != nil {
		t.Fatalf("settle forkchat completion: %v", err)
	}
}

func requireConversationForkToolCall(t *testing.T, calls []OperatorConversationToolCall, name string) OperatorConversationToolCall {
	t.Helper()
	if call := findConversationForkToolCall(calls, name); call != nil {
		if len(call.Result) == 0 {
			t.Fatalf("%s tool call has no result evidence: %#v", name, *call)
		}
		return *call
	}
	t.Fatalf("tool call %s missing from %#v", name, calls)
	return OperatorConversationToolCall{}
}

func findConversationForkToolCall(calls []OperatorConversationToolCall, name string) *OperatorConversationToolCall {
	for i := range calls {
		if calls[i].Name == name {
			return &calls[i]
		}
	}
	return nil
}

func conversationForkToolCallMap(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode tool call JSON %s: %v", string(raw), err)
	}
	return out
}

type conversationForkSourceFixture struct {
	runID      string
	bundleHash string
	agentID    string
	sessionID  string
	turn1ID    string
	turn2ID    string
	event1ID   string
	event2ID   string
	turn1At    time.Time
	turn2At    time.Time
}

const conversationForkSourceFlowInstance = "review"

func seedConversationForkSource(t *testing.T, db *sql.DB, base time.Time) conversationForkSourceFixture {
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
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, bundle_hash, started_at) VALUES ($1::uuid, 'running', $2, $3)`, source.runID, source.bundleHash, base.Add(-3*time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, flow_instance, role, model, memory_enabled, memory_source, runtime_descriptor)
		VALUES ($1, $2, 'researcher', 'cheap', TRUE, 'authored', '{"type":"researcher","execution_mode":"live"}'::jsonb)
	`, source.agentID, conversationForkSourceFlowInstance); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, TRUE, 'authored', 'active', $5, $5)
	`, source.sessionID, source.runID, source.agentID, conversationForkSourceFlowInstance, base.Add(-3*time.Minute)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	capability1 := seedManagedAgentTurnCapabilitySurface(t, &PostgresStore{DB: db}, source.runID, source.agentID, source.sessionID, source.turn1ID, "session", "global")
	capability2 := seedManagedAgentTurnCapabilitySurface(t, &PostgresStore{DB: db}, source.runID, source.agentID, source.sessionID, source.turn2ID, "session", "global")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source,
			trigger_event_id, trigger_event_type, capability_surface_id, parse_ok, execution_mode, created_at
		)
		VALUES
			($1::uuid, $2::uuid, $3, $4::uuid, $5, TRUE, 'authored', $6::uuid, 'task.ready', $7::uuid, true, 'live', $8),
			($9::uuid, $2::uuid, $3, $4::uuid, $5, TRUE, 'authored', $10::uuid, 'task.done', $11::uuid, true, 'live', $12)
	`, source.turn1ID, source.runID, source.agentID, source.sessionID, conversationForkSourceFlowInstance, source.event1ID, capability1, source.turn1At, source.turn2ID, source.event2ID, capability2, source.turn2At); err != nil {
		t.Fatalf("seed turns: %v", err)
	}
	return source
}
