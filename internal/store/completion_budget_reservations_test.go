package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type completionBudgetTestStore interface {
	selectedCompletionAuthorityStore
	CreateOperatorConversationFork(context.Context, ConversationForkCreateRequest) (OperatorConversationForkSession, error)
	PrepareOperatorConversationForkChat(context.Context, ConversationForkChatPrepareRequest) (ConversationForkChatPrepared, error)
}

type completionBudgetRaceFixture struct {
	primary   completionBudgetTestStore
	secondary completionBudgetTestStore
	db        *sql.DB
	sqlite    bool
	normal    completionSettlementFixture
}

func TestCompletionBudgetAdmissionLinearizableAcrossAuthorities(t *testing.T) {
	cases := []struct {
		name      string
		rightKind runtimeeffects.AuthorityKind
		scopes    []runtimeeffects.BudgetAdmissionScope
		want      int
	}{
		{name: "system_ordinary_vs_selected", rightKind: runtimeeffects.AuthoritySelectedContractFork, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}, want: 1},
		{name: "global_ordinary_vs_forkchat", rightKind: runtimeeffects.AuthorityConversationForkChat, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "global", CapUSD: 1}}, want: 1},
		{name: "entity_ordinary_vs_ordinary", rightKind: runtimeeffects.AuthorityNormalAgent, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "entity", Key: uuid.NewString(), CapUSD: 1}}, want: 1},
		{name: "overlap_system_entity", rightKind: runtimeeffects.AuthoritySelectedContractFork, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "entity", Key: uuid.NewString(), CapUSD: 1}, {Kind: "system", CapUSD: 1}}, want: 1},
		{name: "no_cap_does_not_serialize", rightKind: runtimeeffects.AuthoritySelectedContractFork, want: 2},
	}
	for _, tc := range cases {
		tc := tc
		for _, backend := range []string{"sqlite", "postgres"} {
			backend := backend
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				fixture := newCompletionBudgetRaceFixture(t, backend == "sqlite")
				left := fixture.normal.authority
				left.Target.ID = uuid.NewString()
				left.BudgetScopes = append([]runtimeeffects.BudgetAdmissionScope(nil), tc.scopes...)
				right := completionBudgetRaceAuthority(t, fixture, tc.rightKind)
				right.BudgetScopes = append([]runtimeeffects.BudgetAdmissionScope(nil), tc.scopes...)
				for _, scope := range tc.scopes {
					if scope.Kind == "entity" {
						left.Target.EntityID = scope.Key
						right.Target.EntityID = scope.Key
					}
				}
				proveCompletionBudgetAdmissionRace(t, fixture, left, right, tc.want, len(tc.scopes))
			})
		}
	}
}

func TestMockCompletionSpendDoesNotConsumeLiveAdmissionCap(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := newCompletionBudgetRaceFixture(t, backend == "sqlite")
			mockAuthority := fixture.normal.authority
			mockAuthority.ExecutionMode = runtimeeffects.ExecutionModeMock
			mockAuthority.Target.ID = uuid.NewString()
			mockCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), mockAuthority), runtimeeffects.NewController(fixture.primary))
			mockCtx = runtimeeffects.WithLogicalOperationIdentity(mockCtx, "mock-spend-before-live-cap")
			mockHandle, err := runtimeeffects.BeginCompletion(mockCtx, "mock_python", []byte("mock spend"), nil)
			if err != nil {
				t.Fatalf("authorize mock completion: %v", err)
			}
			if err := mockHandle.MarkLaunched(mockCtx); err != nil {
				t.Fatalf("launch mock completion: %v", err)
			}
			if err := mockHandle.MarkResponseObserved(mockCtx, map[string]any{"execution_mode": "mock"}); err != nil {
				t.Fatalf("observe mock completion: %v", err)
			}
			if err := mockHandle.SettleCompletion(mockCtx, budgetAccountingSettlement(mockAuthority.Target, runtimeeffects.CompletionUsageEstimated, runtimeeffects.StateSettled, 10)); err != nil {
				t.Fatalf("settle mock completion: %v", err)
			}

			liveAuthority := fixture.normal.authority
			liveAuthority.Target.ID = uuid.NewString()
			liveAuthority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}
			liveCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), liveAuthority), runtimeeffects.NewController(fixture.primary))
			liveCtx = runtimeeffects.WithLogicalOperationIdentity(liveCtx, "live-cap-after-mock-spend")
			liveHandle, err := runtimeeffects.BeginCompletion(liveCtx, "openai_compatible", []byte("live admission"), nil)
			if err != nil {
				t.Fatalf("mock estimate consumed live admission cap: %v", err)
			}

			var mockSpend, liveReservations int
			if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM spend_ledger WHERE execution_mode='mock' AND cost_usd=10`).Scan(&mockSpend); err != nil {
				t.Fatalf("count mock spend: %v", err)
			}
			placeholder := "?"
			if backend == "postgres" {
				placeholder = "$1::uuid"
			}
			if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=`+placeholder, liveHandle.Attempt().AttemptID).Scan(&liveReservations); err != nil {
				t.Fatalf("count live reservations: %v", err)
			}
			if mockSpend != 1 || liveReservations != 1 {
				t.Fatalf("mock spend rows=%d live reservations=%d, want 1/1", mockSpend, liveReservations)
			}
		})
	}
}

func proveCompletionBudgetAdmissionRace(t *testing.T, fixture completionBudgetRaceFixture, left, right runtimeeffects.Authority, wantSuccesses, wantReservations int) {
	t.Helper()
	type result struct {
		handle *runtimeeffects.Handle
		err    error
		label  string
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i, candidate := range []struct {
		store     completionBudgetTestStore
		authority runtimeeffects.Authority
		identity  string
	}{{fixture.primary, left, "left"}, {fixture.secondary, right, "right"}} {
		i, candidate := i, candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), candidate.authority), runtimeeffects.NewController(candidate.store))
			ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, fmt.Sprintf("budget-race:%s:%d", candidate.identity, i))
			handle, err := runtimeeffects.BeginCompletion(ctx, "openai_responses", []byte(candidate.identity), nil)
			results <- result{handle: handle, err: err, label: candidate.identity}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var successes []*runtimeeffects.Handle
	var failures []result
	for result := range results {
		if result.err != nil {
			failures = append(failures, result)
			continue
		}
		successes = append(successes, result.handle)
	}
	if len(successes) != wantSuccesses || len(failures) != 2-wantSuccesses {
		t.Fatalf("budget race successes=%d failures=%d, want %d/%d: %v", len(successes), len(failures), wantSuccesses, 2-wantSuccesses, failures)
	}
	for _, result := range failures {
		failure, ok := runtimefailures.As(result.err)
		if !ok || failure.Failure.Class != runtimefailures.ClassBudgetExhausted {
			t.Fatalf("budget race loser %s error=%v, want budget exhausted", result.label, result.err)
		}
	}
	var operations, reservations int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_operations WHERE effect_kind='provider_turn'`).Scan(&operations); err != nil {
		t.Fatalf("count budget race operations: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations`).Scan(&reservations); err != nil {
		t.Fatalf("count budget race reservations: %v", err)
	}
	if operations != wantSuccesses || reservations != wantSuccesses*wantReservations {
		t.Fatalf("budget race operations=%d reservations=%d, want %d/%d", operations, reservations, wantSuccesses, wantSuccesses*wantReservations)
	}
}

func newCompletionBudgetRaceFixture(t *testing.T, sqlite bool) completionBudgetRaceFixture {
	t.Helper()
	if sqlite {
		primary := newBootstrappedSQLiteRuntimeStoreForTest(t)
		secondary, err := NewSQLiteRuntimeStore(primary.SQLiteSchemaStore.path)
		if err != nil {
			t.Fatalf("open independent SQLite completion budget handle: %v", err)
		}
		t.Cleanup(func() { _ = secondary.Close() })
		normal := newCompletionSettlementFixture(t, primary, primary.DB, true)
		return completionBudgetRaceFixture{primary: primary, secondary: secondary, db: primary.DB, sqlite: true, normal: normal}
	}
	_, db, _ := testutil.StartPostgres(t)
	primary := &PostgresStore{DB: db}
	secondary := &PostgresStore{DB: db}
	normal := newCompletionSettlementFixture(t, primary, db, false)
	return completionBudgetRaceFixture{primary: primary, secondary: secondary, db: db, normal: normal}
}

func completionBudgetRaceAuthority(t *testing.T, fixture completionBudgetRaceFixture, kind runtimeeffects.AuthorityKind) runtimeeffects.Authority {
	t.Helper()
	switch kind {
	case runtimeeffects.AuthorityNormalAgent:
		authority := fixture.normal.authority
		authority.Target.ID = uuid.NewString()
		return authority
	case runtimeeffects.AuthoritySelectedContractFork:
		selected := newSelectedCompletionFixture(t, fixture.primary, fixture.db, fixture.sqlite)
		issued, err := fixture.primary.IssueRunForkSelectedContractRuntimeExecution(context.Background(), selected.request)
		if err != nil {
			t.Fatalf("issue budget-race selected authority: %v", err)
		}
		authority, err := fixture.primary.ClaimRunForkSelectedContractRuntimeExecution(context.Background(), issued, "budget-selected", time.Minute)
		if err != nil {
			t.Fatalf("claim budget-race selected authority: %v", err)
		}
		authority.Target = selectedAgentTurnTarget(selected.forkRun)
		return authority
	case runtimeeffects.AuthorityConversationForkChat:
		now := time.Now().UTC()
		var source conversationForkSourceFixture
		if fixture.sqlite {
			source = seedSQLiteConversationForkSource(t, fixture.primary.(*SQLiteRuntimeStore), now)
		} else {
			source = seedConversationForkSource(t, fixture.db, now)
		}
		fork, err := fixture.primary.CreateOperatorConversationFork(context.Background(), ConversationForkCreateRequest{
			SourceSessionID: source.sessionID, ForkPoint: ConversationForkPointSelector{Kind: "turn", TurnID: source.turn1ID}, CreatedBy: "budget-actor", Now: now,
		})
		if err != nil {
			t.Fatalf("create budget-race forkchat fork: %v", err)
		}
		prepared, err := fixture.primary.PrepareOperatorConversationForkChat(context.Background(), ConversationForkChatPrepareRequest{
			ForkID: fork.ForkID, Message: "budget race", Method: "conversation.fork_chat", ActorTokenID: "budget-actor",
			RequestHash: runtimeeffects.Fingerprint([]byte("budget race")), Now: now.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("prepare budget-race forkchat authority: %v", err)
		}
		return forkChatCompletionAuthority(prepared, 1)
	default:
		t.Fatalf("unsupported budget race authority kind %q", kind)
		return runtimeeffects.Authority{}
	}
}

func TestCompletionBudgetSettlementAccounting(t *testing.T) {
	cases := []struct {
		name           string
		exactness      runtimeeffects.CompletionUsageExactness
		state          runtimeeffects.State
		scopes         []runtimeeffects.BudgetAdmissionScope
		cost           float64
		wantSpend      int
		wantCost       float64
		wantAccounting string
		wantBasis      string
	}{
		{name: "exact", exactness: runtimeeffects.CompletionUsageExact, state: runtimeeffects.StateSettled, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}, cost: 0.25, wantSpend: 1, wantCost: 0.25, wantAccounting: "exact"},
		{name: "estimated", exactness: runtimeeffects.CompletionUsageEstimated, state: runtimeeffects.StateSettled, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "global", CapUSD: 1}}, cost: 0.20, wantSpend: 1, wantCost: 0.20, wantAccounting: "estimated"},
		{name: "unavailable_overlap", exactness: runtimeeffects.CompletionUsageUnavailable, state: runtimeeffects.StateSettled, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "global", CapUSD: 0.6}, {Kind: "system", CapUSD: 1}}, wantSpend: 1, wantCost: 1, wantAccounting: "estimated", wantBasis: "accounting_unavailable_exhaustion"},
		{name: "outcome_uncertain", exactness: runtimeeffects.CompletionUsageUnavailable, state: runtimeeffects.StateOutcomeUncertain, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 0.8}}, wantSpend: 1, wantCost: 0.8, wantAccounting: "estimated", wantBasis: "accounting_unavailable_exhaustion"},
		{name: "postprovider_failure", exactness: runtimeeffects.CompletionUsageExact, state: runtimeeffects.StateTerminalFailure, scopes: []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}, cost: 0.3, wantSpend: 1, wantCost: 0.3, wantAccounting: "exact"},
		{name: "unavailable_no_cap", exactness: runtimeeffects.CompletionUsageUnavailable, state: runtimeeffects.StateOutcomeUncertain, wantSpend: 0},
		{name: "exact_no_cap", exactness: runtimeeffects.CompletionUsageExact, state: runtimeeffects.StateSettled, cost: 0.15, wantSpend: 1, wantCost: 0.15, wantAccounting: "exact"},
	}
	for _, tc := range cases {
		tc := tc
		for _, backend := range []string{"sqlite", "postgres"} {
			backend := backend
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				proveCompletionBudgetSettlementAccounting(t, backend == "sqlite", tc.exactness, tc.state, tc.scopes, tc.cost, tc.wantSpend, tc.wantCost, tc.wantAccounting, tc.wantBasis)
			})
		}
	}
}

func proveCompletionBudgetSettlementAccounting(t *testing.T, sqlite bool, exactness runtimeeffects.CompletionUsageExactness, state runtimeeffects.State, scopes []runtimeeffects.BudgetAdmissionScope, cost float64, wantSpend int, wantCost float64, wantAccounting, wantBasis string) {
	t.Helper()
	var fixture completionSettlementFixture
	if sqlite {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		fixture = newCompletionSettlementFixture(t, store, store.DB, true)
	} else {
		_, db, _ := testutil.StartPostgres(t)
		fixture = newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false)
	}
	authority := fixture.authority
	authority.BudgetScopes = append([]runtimeeffects.BudgetAdmissionScope(nil), scopes...)
	ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), authority), runtimeeffects.NewController(fixture.store))
	ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "budget-accounting:"+string(exactness)+":"+string(state))
	handle, err := runtimeeffects.BeginCompletion(ctx, "openai_compatible", []byte("budget accounting"), nil)
	if err != nil {
		t.Fatalf("authorize budget-accounted completion: %v", err)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch budget-accounted completion: %v", err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"accounting": exactness}); err != nil {
		t.Fatalf("observe budget-accounted completion: %v", err)
	}
	settlement := budgetAccountingSettlement(authority.Target, exactness, state, cost)
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle budget-accounted completion: %v", err)
	}

	query := `SELECT COUNT(*),COALESCE(MAX(cost_usd),0),COALESCE(MAX(usage_accounting),''),COALESCE(MAX(accounting_basis),'') FROM spend_ledger WHERE external_effect_attempt_id=?`
	if !sqlite {
		query = `SELECT COUNT(*),COALESCE(MAX(cost_usd),0),COALESCE(MAX(usage_accounting),''),COALESCE(MAX(accounting_basis),'') FROM spend_ledger WHERE external_effect_attempt_id=$1::uuid`
	}
	var spend int
	var gotCost float64
	var accounting, basis string
	if err := fixture.db.QueryRow(query, handle.Attempt().AttemptID).Scan(&spend, &gotCost, &accounting, &basis); err != nil {
		t.Fatalf("load completion accounting: %v", err)
	}
	if spend != wantSpend || gotCost != wantCost || accounting != wantAccounting || basis != wantBasis {
		t.Fatalf("completion accounting rows=%d cost=%v accounting=%q basis=%q, want %d/%v/%q/%q", spend, gotCost, accounting, basis, wantSpend, wantCost, wantAccounting, wantBasis)
	}
	var reservations int
	placeholder := "?"
	if !sqlite {
		placeholder = "$1::uuid"
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=`+placeholder, handle.Attempt().AttemptID).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("completion reservations after settlement=%d err=%v, want 0", reservations, err)
	}
}

func budgetAccountingSettlement(target runtimeeffects.UsageTarget, exactness runtimeeffects.CompletionUsageExactness, state runtimeeffects.State, cost float64) runtimeeffects.CompletionSettlement {
	input, output := int64(10), int64(4)
	usage := runtimeeffects.CompletionUsage{ResolvedModel: "budget-test", Exactness: exactness}
	if exactness != runtimeeffects.CompletionUsageUnavailable {
		usage.InputTokens = &input
		usage.OutputTokens = &output
	}
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: state, Evidence: map[string]any{"accounting": exactness}},
		Usage:      usage,
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID, SessionID: target.SessionID,
			Memory: target.Memory, FlowInstance: target.FlowInstance, ParseOK: state == runtimeeffects.StateSettled,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: target.FlowInstance, AgentID: target.AgentID, Model: "regular", ModelAlias: "regular",
			BackendProfile: "test", Provider: "openai", Transport: "http", ResolvedModel: "budget-test",
			CostUSD: cost, InvocationType: "agent_turn",
		},
		Now: time.Now().UTC(),
	}
	if settlement.AgentTurn.Memory == (agentmemory.Plan{}) {
		settlement.AgentTurn.Memory = agentmemory.PlatformDefault()
	}
	if state != runtimeeffects.StateSettled {
		failure := runtimefailures.FromError(
			runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "budget_test_completion_failed", "budget-test", "settle", nil),
			"budget-test", "settle",
		)
		settlement.Settlement.Failure = &failure.Failure
		settlement.AgentTurn.Failure = &failure.Failure
	}
	return settlement
}
