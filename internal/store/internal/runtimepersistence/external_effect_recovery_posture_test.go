package runtimepersistence

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestExternalEffectRecoveryPostureAdmissionGenericSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) neutralEffectParityFixture
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) neutralEffectParityFixture {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return newNeutralEffectParityFixture(t, store, store.backend.ConstructionHandle(), true)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) neutralEffectParityFixture {
				_, db, _ := testutil.StartPostgres(t)
				return newNeutralEffectParityFixture(t, admitTestPostgresStore(t, db), db, false)
			},
		},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			now := time.Now().UTC().Add(time.Minute)

			mock := beginGenericRecoveryPostureMatrix(t, fixture, executionmode.Mock)
			summary, err := fixture.store.ReconcileExternalEffectAttempts(
				testAuthorActivityContext(),
				runtimeeffects.NewRecoveryRequest(now, executionposture.MockOnly),
			)
			if err != nil {
				t.Fatalf("recover exact mock generic attempts: %v", err)
			}
			if summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 2 {
				t.Fatalf("mock generic recovery summary = %#v, want 1/2", summary)
			}
			assertRecoveredPostureMatrix(t, fixture.db, fixture.sqlite, mock)

			live := beginGenericRecoveryPostureMatrix(t, fixture, executionmode.Live)
			before := snapshotExternalEffectRecoveryMatrix(t, fixture.db, fixture.sqlite, live)
			_, err = fixture.store.ReconcileExternalEffectAttempts(
				testAuthorActivityContext(),
				runtimeeffects.NewRecoveryRequest(now.Add(time.Minute), executionposture.MockOnly),
			)
			if err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
				t.Fatalf("recover live generic attempts under mock_only = %v, want rejection", err)
			}
			after := snapshotExternalEffectRecoveryMatrix(t, fixture.db, fixture.sqlite, live)
			if after != before {
				t.Fatalf("rejected generic recovery mutated selected store:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func TestExternalEffectRecoveryPostureAdmissionProviderTurnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) completionSettlementFixture
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) completionSettlementFixture {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) completionSettlementFixture {
				_, db, _ := testutil.StartPostgres(t)
				return newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false)
			},
		},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			now := time.Now().UTC().Add(time.Minute)

			mock := beginProviderRecoveryPostureMatrix(t, fixture, executionmode.Mock, now)
			summary, err := fixture.store.ReconcileExternalEffectAttempts(
				testAuthorActivityContext(),
				runtimeeffects.NewRecoveryRequest(now, executionposture.MockOnly),
			)
			if err != nil {
				t.Fatalf("recover exact mock provider attempts: %v", err)
			}
			if summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 2 {
				t.Fatalf("mock provider recovery summary = %#v, want 1/2", summary)
			}
			assertRecoveredPostureMatrix(t, fixture.db, fixture.sqlite, mock)

			live := beginProviderRecoveryPostureMatrix(t, fixture, executionmode.Live, now.Add(time.Minute))
			before := snapshotExternalEffectRecoveryMatrix(t, fixture.db, fixture.sqlite, live)
			_, err = fixture.store.ReconcileExternalEffectAttempts(
				testAuthorActivityContext(),
				runtimeeffects.NewRecoveryRequest(now.Add(2*time.Minute), executionposture.MockOnly),
			)
			if err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
				t.Fatalf("recover live provider attempts under mock_only = %v, want rejection", err)
			}
			after := snapshotExternalEffectRecoveryMatrix(t, fixture.db, fixture.sqlite, live)
			if after != before {
				t.Fatalf("rejected provider recovery mutated selected store:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func TestExternalEffectRecoveryPostureAdmissionTerminalRunProviderSQLiteAndPostgres(t *testing.T) {
	type runQuiescenceStore interface {
		ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
	}
	for _, backend := range []struct {
		name string
		open func(*testing.T) completionSettlementFixture
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) completionSettlementFixture {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return newCompletionSettlementFixture(t, store, store.backend.ConstructionHandle(), true)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) completionSettlementFixture {
				_, db, _ := testutil.StartPostgres(t)
				return newCompletionSettlementFixture(t, admitTestPostgresStore(t, db), db, false)
			},
		},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := providerDrainContext(t, fixture, "posture-terminal-run")
			handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "posture-terminal-run")
			transition := supersedeProviderDrainFixture(t, fixture, runtimemanager.AgentLifecycleRunning)
			selected, ok := fixture.store.(runQuiescenceStore)
			if !ok {
				t.Fatalf("completion store %T does not expose run quiescence", fixture.store)
			}
			quiesced, err := selected.ApplyActiveRunQuiescence(testAuthorActivityContext(), runtimerunquiescence.Request{
				OperationName: "provider_attempt_terminal_run_posture", RequestedAt: time.Now().UTC(),
				RunIDs: []string{fixture.authority.Target.RunID}, ReasonCode: runtimerunquiescence.ServeAbandonReasonCode,
				ControlledBy: "external-effect-recovery-posture-test", DeliveryNote: "terminal provider posture admission proof",
			})
			if err != nil || len(quiesced.Runs) != 1 || !quiesced.Runs[0].Changed {
				t.Fatalf("terminalize provider run: result=%+v err=%v", quiesced, err)
			}

			attempts := []recoveryPostureAttempt{{
				AttemptID: handle.Attempt().AttemptID,
				RunID:     fixture.authority.Target.RunID,
				Initial:   runtimeeffects.StateResponseObserved,
				Expected:  runtimeeffects.StateResponseObserved,
			}}
			before := snapshotExternalEffectRecoveryMatrix(t, fixture.db, fixture.sqlite, attempts)
			_, err = fixture.store.ReconcileExternalEffectAttempts(
				testAuthorActivityContext(),
				runtimeeffects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly),
			)
			if err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
				t.Fatalf("recover live terminal-run provider attempt under mock_only = %v, want rejection", err)
			}
			after := snapshotExternalEffectRecoveryMatrix(t, fixture.db, fixture.sqlite, attempts)
			if after != before {
				t.Fatalf("rejected terminal-run provider recovery mutated selected store:\nbefore=%#v\nafter=%#v", before, after)
			}
			requireProviderDrainState(t, fixture, handle.Attempt().AttemptID, "pending")
			requireAgentLifecycleState(t, fixture, runtimemanager.AgentLifecycleRunning, transition.Generation)
			requireTerminalizedOriginDelivery(t, fixture, runtimerunquiescence.ServeAbandonReasonCode)
		})
	}
}

type recoveryPostureAttempt struct {
	AttemptID string
	RunID     string
	Initial   runtimeeffects.State
	Expected  runtimeeffects.State
}

func beginGenericRecoveryPostureMatrix(t *testing.T, fixture neutralEffectParityFixture, mode executionmode.Mode) []recoveryPostureAttempt {
	t.Helper()
	registration, ok := runtimeeffects.RegistrationFor("authored_http_tool")
	if !ok {
		t.Fatal("authored_http_tool registration is missing")
	}
	runID := managedNormalEffectStoreTestRunID(fixture.authority.Normal.AgentID)
	capabilitySurface, ok := managedcapabilities.FromContext(fixture.ctx)
	if !ok {
		t.Fatal("generic recovery fixture capability surface is missing")
	}
	cases := []struct {
		name    string
		launch  bool
		observe bool
		initial runtimeeffects.State
		want    runtimeeffects.State
	}{
		{name: "authorized", initial: runtimeeffects.StateAuthorized, want: runtimeeffects.StateTerminalFailure},
		{name: "launched", launch: true, initial: runtimeeffects.StateLaunched, want: runtimeeffects.StateOutcomeUncertain},
		{name: "response_observed", launch: true, observe: true, initial: runtimeeffects.StateResponseObserved, want: runtimeeffects.StateOutcomeUncertain},
	}
	result := make([]recoveryPostureAttempt, 0, len(cases))
	for _, tc := range cases {
		authority := fixture.authority
		authority.ExecutionMode = mode
		operationID := uuid.NewString()
		attempt, err := fixture.store.AuthorizeExternalAttempt(testAuthorActivityContext(), authority, runtimeeffects.AuthorizeRequest{
			OperationID: operationID, AttemptID: uuid.NewString(), Kind: registration.Kind, Class: registration.Class,
			Adapter: registration.Adapter, Transport: registration.Transport,
			RequestFingerprint: runtimeeffects.Fingerprint([]byte(tc.name)),
			CapabilitySurface:  &capabilitySurface,
			Lineage:            map[string]string{"run_id": runID},
			Now:                time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("authorize %s generic %s: %v", mode, tc.name, err)
		}
		if tc.launch {
			if err := fixture.store.MarkExternalAttemptLaunched(testAuthorActivityContext(), attempt, time.Now().UTC()); err != nil {
				t.Fatalf("launch %s generic %s: %v", mode, tc.name, err)
			}
		}
		if tc.observe {
			if err := fixture.store.MarkExternalAttemptResponseObserved(testAuthorActivityContext(), attempt, map[string]any{"posture": mode}, time.Now().UTC()); err != nil {
				t.Fatalf("observe %s generic %s: %v", mode, tc.name, err)
			}
		}
		result = append(result, recoveryPostureAttempt{
			AttemptID: attempt.AttemptID,
			RunID:     runID,
			Initial:   tc.initial,
			Expected:  tc.want,
		})
	}
	return result
}

func beginProviderRecoveryPostureMatrix(t *testing.T, fixture completionSettlementFixture, mode executionmode.Mode, expiredAt time.Time) []recoveryPostureAttempt {
	t.Helper()
	cases := []struct {
		name    string
		launch  bool
		observe bool
		initial runtimeeffects.State
		want    runtimeeffects.State
	}{
		{name: "authorized", initial: runtimeeffects.StateAuthorized, want: runtimeeffects.StateTerminalFailure},
		{name: "launched", launch: true, initial: runtimeeffects.StateLaunched, want: runtimeeffects.StateOutcomeUncertain},
		{name: "response_observed", launch: true, observe: true, initial: runtimeeffects.StateResponseObserved, want: runtimeeffects.StateOutcomeUncertain},
	}
	result := make([]recoveryPostureAttempt, 0, len(cases))
	for _, tc := range cases {
		authority := fixture.authority
		authority.ExecutionMode = mode
		authority.Target.ID = uuid.NewString()
		authority.BudgetScopes = nil
		origin := claimCompletionOriginForTest(t, testAuthorActivityContext(), fixture.store, authority, time.Now().UTC())
		adapter := "anthropic_api"
		if mode == executionmode.Mock {
			adapter = "mock_python"
		}
		ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContext(), authority), newCompletionControllerForTest(fixture.store))
		ctx = runtimedelivery.WithClaim(ctx, origin)
		ctx = runtimeeffects.WithExecutionMode(ctx, mode)
		ctx = runtimeeffects.WithLogicalOperationIdentity(ctx, "posture-provider:"+string(mode)+":"+tc.name+":"+uuid.NewString())
		ctx = withManagedCompletionTestSurface(t, ctx, authority, adapter)
		handle, err := beginManagedCompletionForTest(t, ctx, adapter, []byte(tc.name))
		if err != nil {
			t.Fatalf("authorize %s provider %s: %v", mode, tc.name, err)
		}
		if tc.launch {
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatalf("launch %s provider %s: %v", mode, tc.name, err)
			}
		}
		if tc.observe {
			if err := handle.MarkResponseObserved(ctx, map[string]any{"posture": mode}); err != nil {
				t.Fatalf("observe %s provider %s: %v", mode, tc.name, err)
			}
		}
		setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, expiredAt.Add(-time.Minute))
		result = append(result, recoveryPostureAttempt{
			AttemptID: handle.Attempt().AttemptID,
			RunID:     authority.Target.RunID,
			Initial:   tc.initial,
			Expected:  tc.want,
		})
	}
	return result
}

func assertRecoveredPostureMatrix(t *testing.T, db *sql.DB, sqlite bool, attempts []recoveryPostureAttempt) {
	t.Helper()
	for _, attempt := range attempts {
		requireExternalAttemptState(t, db, sqlite, attempt.AttemptID, attempt.Expected)
		snapshot := readExternalEffectRecoverySnapshot(t, db, sqlite, attempt)
		if snapshot.OperationMode != string(executionmode.Mock) || snapshot.AttemptMode != string(executionmode.Mock) {
			t.Fatalf("recovered mock attempt %s modes = %s/%s", attempt.AttemptID, snapshot.OperationMode, snapshot.AttemptMode)
		}
	}
}

type externalEffectRecoverySnapshot struct {
	OperationState       string
	OperationMode        string
	AttemptState         string
	AttemptMode          string
	AttemptFailure       string
	OperationCompletedAt string
	AttemptCompletedAt   string
	OperationUpdatedAt   string
	AttemptUpdatedAt     string
	BudgetReservations   int
	AgentTurns           int
	SpendRows            int
	StoryRows            int
	RunStatus            string
	CompletionDueAt      string
	CompletionRevision   int64
}

func snapshotExternalEffectRecoveryMatrix(t *testing.T, db *sql.DB, sqlite bool, attempts []recoveryPostureAttempt) string {
	t.Helper()
	var builder strings.Builder
	for _, attempt := range attempts {
		snapshot := readExternalEffectRecoverySnapshot(t, db, sqlite, attempt)
		builder.WriteString(attempt.AttemptID)
		builder.WriteString("=")
		builder.WriteString(snapshot.OperationState + "|" + snapshot.OperationMode + "|" + snapshot.AttemptState + "|" + snapshot.AttemptMode + "|")
		builder.WriteString(snapshot.AttemptFailure + "|" + snapshot.OperationCompletedAt + "|" + snapshot.AttemptCompletedAt + "|")
		builder.WriteString(snapshot.OperationUpdatedAt + "|" + snapshot.AttemptUpdatedAt + "|")
		builder.WriteString(snapshot.RunStatus + "|" + snapshot.CompletionDueAt + "|")
		builder.WriteString(strings.Join([]string{
			int64Text(int64(snapshot.BudgetReservations)), int64Text(int64(snapshot.AgentTurns)),
			int64Text(int64(snapshot.SpendRows)), int64Text(int64(snapshot.StoryRows)),
			int64Text(snapshot.CompletionRevision),
		}, ","))
		builder.WriteString("\n")
	}
	return builder.String()
}

func readExternalEffectRecoverySnapshot(t *testing.T, db *sql.DB, sqlite bool, attempt recoveryPostureAttempt) externalEffectRecoverySnapshot {
	t.Helper()
	query := `
		SELECT o.state, o.execution_mode, a.state, a.execution_mode,
		       COALESCE(CAST(a.failure AS TEXT), ''),
		       COALESCE(CAST(o.completed_at AS TEXT), ''), COALESCE(CAST(a.completed_at AS TEXT), ''),
		       CAST(o.updated_at AS TEXT), CAST(a.updated_at AS TEXT)
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		WHERE a.attempt_id=?`
	if !sqlite {
		query = `
			SELECT o.state, o.execution_mode, a.state, a.execution_mode,
			       COALESCE(a.failure::text, ''),
			       COALESCE(o.completed_at::text, ''), COALESCE(a.completed_at::text, ''),
			       o.updated_at::text, a.updated_at::text
			FROM runtime_external_effect_operations o
			JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
			WHERE a.attempt_id=$1::uuid`
	}
	var snapshot externalEffectRecoverySnapshot
	if err := db.QueryRow(query, attempt.AttemptID).Scan(
		&snapshot.OperationState, &snapshot.OperationMode, &snapshot.AttemptState, &snapshot.AttemptMode,
		&snapshot.AttemptFailure, &snapshot.OperationCompletedAt, &snapshot.AttemptCompletedAt,
		&snapshot.OperationUpdatedAt, &snapshot.AttemptUpdatedAt,
	); err != nil {
		t.Fatalf("read recovery snapshot for %s: %v", attempt.AttemptID, err)
	}
	countQuery := func(sqliteQuery, postgresQuery string) int {
		t.Helper()
		query := sqliteQuery
		if !sqlite {
			query = postgresQuery
		}
		var count int
		if err := db.QueryRow(query, attempt.AttemptID).Scan(&count); err != nil {
			t.Fatalf("count recovery projection for %s: %v", attempt.AttemptID, err)
		}
		return count
	}
	snapshot.BudgetReservations = countQuery(
		`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=?`,
		`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`,
	)
	snapshot.AgentTurns = countQuery(
		`SELECT COUNT(*) FROM agent_turns WHERE completion_attempt_id=?`,
		`SELECT COUNT(*) FROM agent_turns WHERE completion_attempt_id=$1::uuid`,
	)
	snapshot.SpendRows = countQuery(
		`SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=?`,
		`SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=$1::uuid`,
	)
	snapshot.StoryRows = countQuery(
		`SELECT COUNT(*) FROM author_activity_occurrences WHERE source_owner='runtime_external_effect_attempts' AND source_identity LIKE ? || ':%'`,
		`SELECT COUNT(*) FROM author_activity_occurrences WHERE source_owner='runtime_external_effect_attempts' AND source_identity LIKE $1 || ':%'`,
	)
	if strings.TrimSpace(attempt.RunID) != "" {
		runQuery := `SELECT status, COALESCE(CAST(completion_due_at AS TEXT), ''), completion_revision FROM runs WHERE run_id=?`
		if !sqlite {
			runQuery = `SELECT status, COALESCE(completion_due_at::text, ''), completion_revision FROM runs WHERE run_id=$1::uuid`
		}
		if err := db.QueryRow(runQuery, attempt.RunID).Scan(&snapshot.RunStatus, &snapshot.CompletionDueAt, &snapshot.CompletionRevision); err != nil {
			t.Fatalf("read recovery run snapshot for %s: %v", attempt.AttemptID, err)
		}
	}
	return snapshot
}

func int64Text(value int64) string {
	return strconv.FormatInt(value, 10)
}
