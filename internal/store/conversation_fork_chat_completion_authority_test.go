package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type forkChatCompletionAuthorityStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.RecoveryStore
	CreateOperatorConversationFork(context.Context, ConversationForkCreateRequest) (OperatorConversationForkSession, error)
	PrepareOperatorConversationForkChat(context.Context, ConversationForkChatPrepareRequest) (ConversationForkChatPrepared, error)
	HeartbeatOperatorConversationForkChat(context.Context, ConversationForkChatPrepared, time.Time) error
	RecordOperatorConversationForkChat(context.Context, ConversationForkChatRecordRequest) (ConversationForkChatResult, error)
	FailOperatorConversationForkChat(context.Context, ConversationForkChatFailureRequest) error
}

type forkChatCompletionAuthorityFixture struct {
	store  forkChatCompletionAuthorityStore
	db     *sql.DB
	sqlite bool
	now    time.Time
	source conversationForkSourceFixture
	fork   OperatorConversationForkSession
}

func TestForkChatCompletionGroupLifecycle(t *testing.T) {
	cases := []struct {
		name string
		test func(*testing.T, forkChatCompletionAuthorityFixture)
	}{
		{name: "cap_rejection", test: proveForkChatCompletionGroupCapRejection},
		{name: "prepared_orphan", test: proveForkChatCompletionGroupPreparedOrphanRecovery},
		{name: "prelaunch_failure", test: proveForkChatCompletionGroupPrelaunchFailure},
		{name: "partial_rounds", test: proveForkChatCompletionGroupPartialRounds},
		{name: "finalization_failure", test: proveForkChatCompletionGroupFinalizationFailure},
		{name: "same_key_live_replay", test: proveForkChatCompletionGroupSameKeyLiveReplay},
		{name: "same_key_succeeded_replay", test: proveForkChatCompletionGroupSucceededReplay},
		{name: "conflicting_request_reuse", test: proveForkChatCompletionGroupConflictingRequestReuse},
		{name: "unkeyed_identical_requests", test: proveForkChatCompletionGroupUnkeyedIdenticalRequests},
	}
	for _, tc := range cases {
		tc := tc
		for _, backend := range []string{"sqlite", "postgres"} {
			backend := backend
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				tc.test(t, newForkChatCompletionAuthorityFixture(t, backend == "sqlite"))
			})
		}
	}
}

func proveForkChatCompletionGroupCapRejection(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "cap-key", "cap rejection")
	seedForkChatRetainedSpend(t, fixture, 1)
	authority := forkChatCompletionAuthority(prepared, 1)
	authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}
	ctx := forkChatCompletionContext(fixture, authority, "cap-rejection")
	_, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(prepared.RequestHash), nil)
	if err == nil {
		t.Fatal("exhausted forkchat completion budget was admitted")
	}
	if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassBudgetExhausted {
		t.Fatalf("cap rejection=%v, want budget exhausted", err)
	}
	if err := fixture.store.FailOperatorConversationForkChat(ctx, ConversationForkChatFailureRequest{Prepared: prepared, Cause: err, Now: fixture.now.Add(2 * time.Second)}); err != nil {
		t.Fatalf("terminalize cap-rejected forkchat group: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "failed", true)
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 0, 0)
	requireForkChatReplayState(t, fixture, prepared, "cap rejection", "failed")
}

func proveForkChatCompletionGroupPreparedOrphanRecovery(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "orphan-key", "prepared orphan")
	expireForkChatGroupLease(t, fixture, prepared.ForkTurnID)
	if _, err := fixture.store.ReconcileExternalEffectAttempts(context.Background(), fixture.now.Add(10*time.Minute)); err != nil {
		t.Fatalf("recover prepared forkchat group: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "abandoned", true)
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 0, 0)
	requireForkChatReplayState(t, fixture, prepared, "prepared orphan", "abandoned")
}

func proveForkChatCompletionGroupPrelaunchFailure(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "prelaunch-key", "prelaunch failure")
	ctx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "prelaunch")
	failureErr := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "provider_prelaunch_rejected", "forkchat-test", "launch", nil)
	settleForkChatPrelaunchFailure(t, ctx, handle, prepared, failureErr, fixture.now.Add(2*time.Second))
	if err := fixture.store.FailOperatorConversationForkChat(ctx, ConversationForkChatFailureRequest{Prepared: prepared, Cause: failureErr, Now: fixture.now.Add(3 * time.Second)}); err != nil {
		t.Fatalf("terminalize prelaunch forkchat group: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "failed", true)
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 1, 0)
	requireForkChatAttemptNeverLaunched(t, fixture, handle.Attempt().AttemptID)
	requireForkChatReplayState(t, fixture, prepared, "prelaunch failure", "failed")
}

func proveForkChatCompletionGroupPartialRounds(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "partial-key", "partial rounds")
	ctx1, first := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "round-1")
	launchAndObserveForkChatCompletion(t, ctx1, first, "round-1")
	settleForkChatCompletionAttempt(t, ctx1, first, prepared, runtimeeffects.StateSettled, nil, fixture.now.Add(2*time.Second))

	ctx2, second := beginForkChatCompletionAttempt(t, fixture, prepared, 2, "round-2")
	launchAndObserveForkChatCompletion(t, ctx2, second, "round-2")
	failureErr := runtimefailures.New(runtimefailures.ClassSchemaInvalid, "provider_round_parse_failed", "forkchat-test", "parse", nil)
	settleForkChatCompletionAttempt(t, ctx2, second, prepared, runtimeeffects.StateTerminalFailure, failureErr, fixture.now.Add(3*time.Second))
	if err := fixture.store.FailOperatorConversationForkChat(ctx2, ConversationForkChatFailureRequest{Prepared: prepared, Cause: failureErr, Now: fixture.now.Add(4 * time.Second)}); err != nil {
		t.Fatalf("terminalize partial-round forkchat group: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "failed", true)
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 2, 2)
	requireForkChatChildStates(t, fixture, prepared.ForkTurnID, []string{"succeeded", "failed"})
	requireForkChatReplayState(t, fixture, prepared, "partial rounds", "failed")
}

func proveForkChatCompletionGroupFinalizationFailure(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "finalization-key", "finalization failure")
	ctx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "finalization")
	launchAndObserveForkChatCompletion(t, ctx, handle, "finalization")
	settleForkChatCompletionAttempt(t, ctx, handle, prepared, runtimeeffects.StateSettled, nil, fixture.now.Add(2*time.Second))
	markConversationForkDeleted(t, fixture)
	_, err := fixture.store.RecordOperatorConversationForkChat(ctx, successfulForkChatRecord(prepared, "finalization failure", fixture.now.Add(3*time.Second)))
	if err == nil {
		t.Fatal("forkchat finalization unexpectedly succeeded after fork invalidation")
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "executing", false)
	expireForkChatGroupLease(t, fixture, prepared.ForkTurnID)
	if _, err := fixture.store.ReconcileExternalEffectAttempts(context.Background(), fixture.now.Add(10*time.Minute)); err != nil {
		t.Fatalf("recover finalization-failed forkchat group: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "outcome_uncertain", true)
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 1, 1)
	requireForkChatReplayState(t, fixture, prepared, "finalization failure", "outcome_uncertain")
}

func proveForkChatCompletionGroupSameKeyLiveReplay(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "live-key", "live replay")
	requireForkChatReplayState(t, fixture, prepared, "live replay", "prepared")
	if err := fixture.store.HeartbeatOperatorConversationForkChat(context.Background(), prepared, fixture.now.Add(30*time.Second)); err != nil {
		t.Fatalf("heartbeat prepared forkchat group: %v", err)
	}
	_, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "live")
	requireForkChatAttemptUsesCurrentLease(t, fixture, handle.Attempt().AttemptID, prepared.LeaseExpiresAt)
	requireForkChatReplayState(t, fixture, prepared, "live replay", "executing")
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 0, 0)
	var count int
	query := `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`
	}
	if err := fixture.db.QueryRow(query, handle.Attempt().AttemptID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("live keyed replay attempts=%d err=%v, want 1", count, err)
	}
}

func proveForkChatCompletionGroupSucceededReplay(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "success-key", "successful replay")
	ctx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "success")
	launchAndObserveForkChatCompletion(t, ctx, handle, "success")
	settleForkChatCompletionAttempt(t, ctx, handle, prepared, runtimeeffects.StateSettled, nil, fixture.now.Add(2*time.Second))
	if _, err := fixture.store.RecordOperatorConversationForkChat(ctx, successfulForkChatRecord(prepared, "successful replay", fixture.now.Add(3*time.Second))); err != nil {
		t.Fatalf("record successful forkchat group: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "succeeded", false)
	requireForkChatReplayState(t, fixture, prepared, "successful replay", "succeeded")
}

func proveForkChatCompletionGroupConflictingRequestReuse(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	prepared := prepareForkChatCompletionGroup(t, fixture, "conflict-key", "original request")
	_, err := fixture.store.PrepareOperatorConversationForkChat(context.Background(), ConversationForkChatPrepareRequest{
		ForkID: fixture.fork.ForkID, Message: "changed request", Method: "conversation.fork_chat",
		ActorTokenID: prepared.ActorTokenID, RequestHash: runtimeeffects.Fingerprint([]byte("changed request")),
		IdempotencyKey: prepared.IdempotencyKey, Now: fixture.now.Add(2 * time.Second),
	})
	var conflict *APIIdempotencyConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflicting keyed forkchat request error=%v, want APIIdempotencyConflictError", err)
	}
	requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 0, 0)
}

func proveForkChatCompletionGroupUnkeyedIdenticalRequests(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	first := prepareForkChatCompletionGroup(t, fixture, "", "unkeyed identical")
	second := prepareForkChatCompletionGroup(t, fixture, "", "unkeyed identical")
	if first.ForkTurnID == second.ForkTurnID || first.RequestOccurrenceID == second.RequestOccurrenceID || first.TurnIndex == second.TurnIndex {
		t.Fatalf("unkeyed requests collapsed first=%#v second=%#v", first, second)
	}
}

func newForkChatCompletionAuthorityFixture(t *testing.T, sqlite bool) forkChatCompletionAuthorityFixture {
	t.Helper()
	now := activeConversationForkTestClock()
	var store forkChatCompletionAuthorityStore
	var db *sql.DB
	var source conversationForkSourceFixture
	if sqlite {
		s := newBootstrappedSQLiteRuntimeStoreForTest(t)
		store, db = s, s.DB
		source = seedSQLiteConversationForkSource(t, s, now)
	} else {
		_, pgDB, _ := testutil.StartPostgres(t)
		s := &PostgresStore{DB: pgDB}
		store, db = s, pgDB
		source = seedConversationForkSource(t, db, now)
	}
	fork, err := store.CreateOperatorConversationFork(context.Background(), ConversationForkCreateRequest{
		SourceSessionID: source.sessionID, ForkPoint: ConversationForkPointSelector{Kind: "turn", TurnIndex: 1},
		CreatedBy: "actor-token", Now: now,
	})
	if err != nil {
		t.Fatalf("create forkchat authority fixture: %v", err)
	}
	return forkChatCompletionAuthorityFixture{store: store, db: db, sqlite: sqlite, now: now, source: source, fork: fork}
}

func prepareForkChatCompletionGroup(t *testing.T, fixture forkChatCompletionAuthorityFixture, key, message string) ConversationForkChatPrepared {
	t.Helper()
	prepared, err := fixture.store.PrepareOperatorConversationForkChat(context.Background(), ConversationForkChatPrepareRequest{
		ForkID: fixture.fork.ForkID, Message: message, Method: "conversation.fork_chat", ActorTokenID: "actor-token",
		RequestHash: runtimeeffects.Fingerprint([]byte(message)), IdempotencyKey: key, Now: fixture.now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("prepare forkchat completion group: %v", err)
	}
	return prepared
}

func forkChatCompletionAuthority(prepared ConversationForkChatPrepared, ordinal int) runtimeeffects.Authority {
	return runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityConversationForkChat, ID: prepared.ForkTurnID,
		ExecutionOwner: prepared.ExecutionOwner, LeaseExpiresAt: prepared.LeaseExpiresAt, FenceGeneration: prepared.FenceGeneration,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{
			ForkTurnID: prepared.ForkTurnID, ForkID: prepared.Fork.ForkID, ActorTokenID: prepared.ActorTokenID,
			RequestOccurrenceID: prepared.RequestOccurrenceID, RequestHash: prepared.RequestHash,
		},
		Target: runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetConversationForkCompletion, ID: prepared.ForkTurnID, Ordinal: ordinal},
	}
}

func forkChatCompletionContext(fixture forkChatCompletionAuthorityFixture, authority runtimeeffects.Authority, suffix string) context.Context {
	ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), authority), runtimeeffects.NewController(fixture.store))
	return runtimeeffects.WithLogicalOperationIdentity(ctx, "forkchat:"+authority.ForkChat.RequestOccurrenceID+":"+suffix)
}

func beginForkChatCompletionAttempt(t *testing.T, fixture forkChatCompletionAuthorityFixture, prepared ConversationForkChatPrepared, ordinal int, suffix string) (context.Context, *runtimeeffects.Handle) {
	t.Helper()
	authority := forkChatCompletionAuthority(prepared, ordinal)
	ctx := forkChatCompletionContext(fixture, authority, suffix)
	handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte(prepared.RequestHash+":"+suffix), nil)
	if err != nil {
		t.Fatalf("authorize forkchat completion %s: %v", suffix, err)
	}
	return ctx, handle
}

func launchAndObserveForkChatCompletion(t *testing.T, ctx context.Context, handle *runtimeeffects.Handle, suffix string) {
	t.Helper()
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch forkchat completion %s: %v", suffix, err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"response": suffix}); err != nil {
		t.Fatalf("observe forkchat completion %s: %v", suffix, err)
	}
}

func settleForkChatCompletionAttempt(t *testing.T, ctx context.Context, handle *runtimeeffects.Handle, prepared ConversationForkChatPrepared, state runtimeeffects.State, cause error, now time.Time) {
	t.Helper()
	input, output := int64(3), int64(2)
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: state, Evidence: map[string]any{"round": handle.Attempt().Authority.Target.Ordinal}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "forkchat-test", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: prepared.Fork.ForkID, AgentID: prepared.Fork.SourceAgentID, Model: "regular", ModelAlias: "regular",
			BackendProfile: "test", Provider: "anthropic", Transport: "http", ResolvedModel: "forkchat-test",
			CostUSD: 0.01, InvocationType: "forkchat",
		},
		Now: now,
	}
	if cause != nil {
		failure := runtimefailures.FromError(cause, "forkchat-test", "settle")
		settlement.Settlement.Failure = &failure.Failure
	}
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle forkchat completion: %v", err)
	}
}

func settleForkChatPrelaunchFailure(t *testing.T, ctx context.Context, handle *runtimeeffects.Handle, prepared ConversationForkChatPrepared, cause error, now time.Time) {
	t.Helper()
	failure := runtimefailures.FromError(cause, "forkchat-test", "settle")
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{
			State: runtimeeffects.StateTerminalFailure, Failure: &failure.Failure,
			Evidence: map[string]any{"launch_rejected": true},
		},
		Usage: runtimeeffects.CompletionUsage{ResolvedModel: "forkchat-test", Exactness: runtimeeffects.CompletionUsageUnavailable},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: prepared.Fork.ForkID, AgentID: prepared.Fork.SourceAgentID, Model: "regular", ModelAlias: "regular",
			BackendProfile: "test", Provider: "anthropic", Transport: "http", ResolvedModel: "forkchat-test",
			InvocationType: "forkchat",
		},
		Now: now,
	}
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle prelaunch forkchat completion: %v", err)
	}
}

func successfulForkChatRecord(prepared ConversationForkChatPrepared, message string, now time.Time) ConversationForkChatRecordRequest {
	return ConversationForkChatRecordRequest{
		ForkID: prepared.Fork.ForkID, Message: message, ActorTokenID: prepared.ActorTokenID, Prepared: prepared,
		Execution: ConversationForkChatExecution{
			AssistantMessage: "forkchat complete", AvailableTools: prepared.AvailableTools,
			ExecutionOwner: prepared.ExecutionOwner, FenceGeneration: prepared.FenceGeneration,
		},
		Now: now,
	}
}

func requireForkChatReplayState(t *testing.T, fixture forkChatCompletionAuthorityFixture, prepared ConversationForkChatPrepared, message, wantState string) {
	t.Helper()
	_, err := fixture.store.PrepareOperatorConversationForkChat(context.Background(), ConversationForkChatPrepareRequest{
		ForkID: prepared.Fork.ForkID, Message: message, Method: "conversation.fork_chat", ActorTokenID: prepared.ActorTokenID,
		RequestHash: prepared.RequestHash, IdempotencyKey: prepared.IdempotencyKey, Now: fixture.now.Add(20 * time.Second),
	})
	var replay *ConversationForkChatReplayStateError
	if !errors.As(err, &replay) || replay.ForkTurnID != prepared.ForkTurnID || replay.State != wantState {
		t.Fatalf("forkchat replay error=%v replay=%#v, want %s/%s", err, replay, prepared.ForkTurnID, wantState)
	}
}

func requireForkChatGroupState(t *testing.T, fixture forkChatCompletionAuthorityFixture, forkTurnID, want string, requireFailure bool) {
	t.Helper()
	query := `SELECT state,failure IS NOT NULL FROM conversation_fork_turns WHERE fork_turn_id=?`
	if !fixture.sqlite {
		query = `SELECT state,failure IS NOT NULL FROM conversation_fork_turns WHERE fork_turn_id=$1::uuid`
	}
	var state string
	var hasFailure bool
	if err := fixture.db.QueryRow(query, forkTurnID).Scan(&state, &hasFailure); err != nil || state != want || (requireFailure && !hasFailure) {
		t.Fatalf("forkchat group state=%q failure=%v err=%v, want %q failure=%v", state, hasFailure, err, want, requireFailure)
	}
}

func requireForkChatGroupRows(t *testing.T, fixture forkChatCompletionAuthorityFixture, forkTurnID string, wantChildren, wantSpend int) {
	t.Helper()
	childQuery := `SELECT COUNT(*) FROM conversation_fork_turn_completions WHERE fork_turn_id=?`
	spendQuery := `SELECT COUNT(*) FROM spend_ledger s JOIN runtime_external_effect_attempts a ON a.attempt_id=s.external_effect_attempt_id JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=?`
	if !fixture.sqlite {
		childQuery = `SELECT COUNT(*) FROM conversation_fork_turn_completions WHERE fork_turn_id=$1::uuid`
		spendQuery = `SELECT COUNT(*) FROM spend_ledger s JOIN runtime_external_effect_attempts a ON a.attempt_id=s.external_effect_attempt_id JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.fork_turn_id=$1::uuid`
	}
	var children, spend int
	if err := fixture.db.QueryRow(childQuery, forkTurnID).Scan(&children); err != nil {
		t.Fatalf("count forkchat completion children: %v", err)
	}
	if err := fixture.db.QueryRow(spendQuery, forkTurnID).Scan(&spend); err != nil {
		t.Fatalf("count forkchat completion spend: %v", err)
	}
	if children != wantChildren || spend != wantSpend {
		t.Fatalf("forkchat rows children=%d spend=%d, want %d/%d", children, spend, wantChildren, wantSpend)
	}
}

func requireForkChatChildStates(t *testing.T, fixture forkChatCompletionAuthorityFixture, forkTurnID string, want []string) {
	t.Helper()
	query := `SELECT state FROM conversation_fork_turn_completions WHERE fork_turn_id=? ORDER BY ordinal`
	if !fixture.sqlite {
		query = `SELECT state FROM conversation_fork_turn_completions WHERE fork_turn_id=$1::uuid ORDER BY ordinal`
	}
	rows, err := fixture.db.Query(query, forkTurnID)
	if err != nil {
		t.Fatalf("list forkchat child states: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var state string
		if err := rows.Scan(&state); err != nil {
			t.Fatalf("scan forkchat child state: %v", err)
		}
		got = append(got, state)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("forkchat child states=%v, want %v", got, want)
	}
}

func requireForkChatAttemptNeverLaunched(t *testing.T, fixture forkChatCompletionAuthorityFixture, attemptID string) {
	t.Helper()
	query := `SELECT launched_at IS NULL FROM runtime_external_effect_attempts WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT launched_at IS NULL FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`
	}
	var neverLaunched bool
	if err := fixture.db.QueryRow(query, attemptID).Scan(&neverLaunched); err != nil || !neverLaunched {
		t.Fatalf("prelaunch forkchat attempt launched=%v err=%v", !neverLaunched, err)
	}
}

func requireForkChatAttemptUsesCurrentLease(t *testing.T, fixture forkChatCompletionAuthorityFixture, attemptID string, originalLease time.Time) {
	t.Helper()
	query := `
		SELECT a.lease_expires_at,f.lease_expires_at
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN conversation_fork_turns f ON f.fork_turn_id=o.fork_turn_id
		WHERE a.attempt_id=?
	`
	if fixture.sqlite {
		var attemptLease, authorityLease conversationForkTimeValue
		if err := fixture.db.QueryRow(query, attemptID).Scan(&attemptLease, &authorityLease); err != nil {
			t.Fatalf("read sqlite forkchat attempt lease: %v", err)
		}
		if !attemptLease.Valid || !authorityLease.Valid || !attemptLease.Time.Equal(authorityLease.Time) || !attemptLease.Time.After(originalLease) {
			t.Fatalf("sqlite forkchat attempt lease=%v authority=%v original=%v", attemptLease.Time, authorityLease.Time, originalLease)
		}
		return
	}
	query = `
		SELECT a.lease_expires_at,f.lease_expires_at
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN conversation_fork_turns f ON f.fork_turn_id=o.fork_turn_id
		WHERE a.attempt_id=$1::uuid
	`
	var attemptLease, authorityLease time.Time
	if err := fixture.db.QueryRow(query, attemptID).Scan(&attemptLease, &authorityLease); err != nil {
		t.Fatalf("read forkchat attempt lease: %v", err)
	}
	if !attemptLease.Equal(authorityLease) || !attemptLease.After(originalLease) {
		t.Fatalf("forkchat attempt lease=%v authority=%v original=%v", attemptLease, authorityLease, originalLease)
	}
}

func expireForkChatGroupLease(t *testing.T, fixture forkChatCompletionAuthorityFixture, forkTurnID string) {
	t.Helper()
	expired := fixture.now.Add(-time.Minute)
	query := `UPDATE conversation_fork_turns SET lease_expires_at=? WHERE fork_turn_id=?`
	if !fixture.sqlite {
		query = `UPDATE conversation_fork_turns SET lease_expires_at=$1 WHERE fork_turn_id=$2::uuid`
	}
	if _, err := fixture.db.Exec(query, expired, forkTurnID); err != nil {
		t.Fatalf("expire forkchat group lease: %v", err)
	}
}

func markConversationForkDeleted(t *testing.T, fixture forkChatCompletionAuthorityFixture) {
	t.Helper()
	query := `UPDATE conversation_forks SET deleted_at=? WHERE fork_id=?`
	if !fixture.sqlite {
		query = `UPDATE conversation_forks SET deleted_at=$1 WHERE fork_id=$2::uuid`
	}
	if _, err := fixture.db.Exec(query, fixture.now.Add(2*time.Second), fixture.fork.ForkID); err != nil {
		t.Fatalf("invalidate fork before finalization: %v", err)
	}
}

func seedForkChatRetainedSpend(t *testing.T, fixture forkChatCompletionAuthorityFixture, cost float64) {
	t.Helper()
	args := []any{uuid.NewString(), fixture.source.agentID, cost, fixture.now}
	query := `INSERT INTO spend_ledger (ledger_id,flow_instance,agent_id,model,model_alias,backend_profile,provider,transport,resolved_model,input_tokens,output_tokens,cost_usd,invocation_type,usage_accounting,created_at) VALUES (?,'global',?,'regular','regular','test','test','http','test',0,0,?,'test','exact',?)`
	if !fixture.sqlite {
		query = `INSERT INTO spend_ledger (ledger_id,flow_instance,agent_id,model,model_alias,backend_profile,provider,transport,resolved_model,input_tokens,output_tokens,cost_usd,invocation_type,usage_accounting,created_at) VALUES ($1::uuid,'global',$2,'regular','regular','test','test','http','test',0,0,$3,'test','exact',$4)`
	}
	if _, err := fixture.db.Exec(query, args...); err != nil {
		t.Fatalf("seed retained completion spend: %v", err)
	}
}
