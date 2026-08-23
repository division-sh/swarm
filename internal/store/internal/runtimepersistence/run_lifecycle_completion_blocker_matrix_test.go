package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	runtimegates "github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestRunLifecycleCompletionBlockerMatrixParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivityBundleSourceContext()
			seedCompletionBlockerAgent(t, fixture, ctx)
			testCompletionSessionBlockers(t, fixture, ctx)
			testCompletionGateBlockers(t, fixture, ctx)
			testCompletionEffectBlockers(t, fixture, ctx)
			testCompletionEntityBlockers(t, fixture, ctx)
		})
	}
}

func testCompletionSessionBlockers(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) {
	t.Helper()
	observedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		holder    any
		expiry    any
		foreign   bool
		wantBlock bool
		malformed bool
	}{
		{name: "released"},
		{name: "active_future", holder: "worker", expiry: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), wantBlock: true},
		{name: "active_expired", holder: "worker", expiry: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{name: "holder_only", holder: "worker", wantBlock: true, malformed: true},
		{name: "expiry_only", expiry: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), wantBlock: true, malformed: true},
		{name: "malformed_expiry", holder: "worker", expiry: "not-a-timestamp", wantBlock: true, malformed: true},
		{name: "foreign_run", holder: "worker", expiry: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), foreign: true},
	}
	for _, test := range tests {
		test := test
		t.Run("session_"+test.name, func(t *testing.T) {
			runID := seedCompletionBlockerRun(t, fixture, ctx)
			sessionRunID := runID
			if test.foreign {
				sessionRunID = seedCompletionBlockerRun(t, fixture, ctx)
			}
			err := insertCompletionBlockerSession(t, fixture, ctx, sessionRunID, test.holder, test.expiry)
			if test.name == "malformed_expiry" && fixture.postgres {
				if err == nil {
					t.Fatal("PostgreSQL accepted malformed typed session expiry")
				}
				return
			}
			if err != nil {
				t.Fatalf("insert session blocker: %v", err)
			}
			summaries := readCompletionBlockerSummaries(t, fixture, ctx, runID, observedAt)
			if got := summaries.Sessions.BlocksCompletion(); got != test.wantBlock {
				t.Fatalf("session blocks completion = %v, want %v: %#v", got, test.wantBlock, summaries.Sessions)
			}
			if got := summaries.Sessions.MalformedLease > 0; got != test.malformed {
				t.Fatalf("session malformed = %v, want %v: %#v", got, test.malformed, summaries.Sessions)
			}
			assertCompletionBlockerExecution(t, fixture, ctx, runID, test.wantBlock)
		})
	}
}

func testCompletionGateBlockers(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) {
	t.Helper()
	type gateCase struct {
		name      string
		mutate    func(*runtimegates.Activation)
		key       string
		rawBucket any
		wantBlock bool
		malformed bool
	}
	tests := []gateCase{
		{name: "open", wantBlock: true},
		{name: "decision_committed_without_persisted_event", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.Status = runtimegates.StatusDecisionCommitted
			a.DecisionEventID = uuid.NewString()
		}},
		{name: "routed_without_persisted_event", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.Status = runtimegates.StatusRouted
			a.DecisionEventID = uuid.NewString()
		}},
		{name: "superseded", mutate: func(a *runtimegates.Activation) {
			a.Status = runtimegates.StatusSuperseded
			a.SupersededReason = "retired"
		}},
		{name: "unknown_status", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.Status = runtimegates.Status("unknown")
		}},
		{name: "committed_without_event", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.Status = runtimegates.StatusDecisionCommitted
		}},
		{name: "superseded_without_reason", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.Status = runtimegates.StatusSuperseded
		}},
		{name: "incomplete_routes", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.RoutesJSON = ""
		}},
		{name: "incomplete_identity", wantBlock: true, malformed: true, mutate: func(a *runtimegates.Activation) {
			a.CardID = ""
		}},
		{name: "wrong_bucket_key", key: "wrong", wantBlock: true, malformed: true},
		{name: "malformed_bucket", rawBucket: "not-an-object", wantBlock: true, malformed: true},
	}
	for _, test := range tests {
		test := test
		t.Run("gate_"+test.name, func(t *testing.T) {
			runID := seedCompletionBlockerRun(t, fixture, ctx)
			activation, err := runtimegates.New(
				runID, semanticRunFixtureFlow, runID, "matrix", "review", "approve",
				runLifecycleCandidateParityBundleHash, testGateRoutes(t), uuid.NewString(),
				time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(&activation)
			}
			key := activation.Key()
			if test.key != "" {
				key = test.key
			}
			bucket := test.rawBucket
			if bucket == nil {
				bucket = map[string]any{key: activation}
			}
			updateCompletionBlockerAccumulator(t, fixture, ctx, runID, map[string]any{
				runtimegates.BucketKey: bucket,
			})
			summaries := readCompletionBlockerSummaries(
				t, fixture, ctx, runID,
				time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
			)
			if got := summaries.Decisions.BlocksCompletion(); got != test.wantBlock {
				t.Fatalf("gate blocks completion = %v, want %v: %#v", got, test.wantBlock, summaries.Decisions)
			}
			if got := summaries.Decisions.MalformedObligations > 0; got != test.malformed {
				t.Fatalf("gate malformed = %v, want %v: %#v", got, test.malformed, summaries.Decisions)
			}
			assertCompletionBlockerExecution(t, fixture, ctx, runID, test.wantBlock)
		})
	}
}

func testCompletionEffectBlockers(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) {
	t.Helper()
	tests := []struct {
		name             string
		operationState   string
		attemptStates    []string
		reservationScope string
		reservationKey   string
		conflictingRun   bool
		active           int
		orphan           int
		malformed        bool
	}{
		{name: "prepared_without_attempt", operationState: "prepared", active: 1},
		{name: "authorized", operationState: "authorized", attemptStates: []string{"authorized"}, active: 1},
		{name: "launched", operationState: "launched", attemptStates: []string{"launched"}, active: 1},
		{name: "response_observed", operationState: "response_observed", attemptStates: []string{"response_observed"}, active: 1},
		{name: "settled", operationState: "settled", attemptStates: []string{"settled"}},
		{name: "terminal_failure", operationState: "terminal_failure", attemptStates: []string{"terminal_failure"}},
		{name: "outcome_uncertain", operationState: "outcome_uncertain", attemptStates: []string{"outcome_uncertain"}},
		{name: "terminal_retry_history", operationState: "settled", attemptStates: []string{"terminal_failure", "settled"}},
		{name: "active_latest_after_terminal_retry", operationState: "authorized", attemptStates: []string{"terminal_failure", "authorized"}, active: 1},
		{name: "active_historical_attempt", operationState: "settled", attemptStates: []string{"authorized", "settled"}, malformed: true},
		{name: "latest_state_mismatch", operationState: "settled", attemptStates: []string{"authorized"}, malformed: true},
		{name: "conflicting_run_binding", operationState: "settled", attemptStates: []string{"settled"}, conflictingRun: true, malformed: true},
		{name: "terminal_reservation", operationState: "settled", attemptStates: []string{"settled"}, reservationScope: "system", orphan: 1},
		{name: "reservation_identity_mismatch", operationState: "authorized", attemptStates: []string{"authorized"}, reservationScope: "entity", reservationKey: "wrong-entity", malformed: true},
	}
	for _, test := range tests {
		test := test
		t.Run("effect_"+test.name, func(t *testing.T) {
			runID := seedCompletionBlockerRun(t, fixture, ctx)
			insertCompletionBlockerEffect(
				t, fixture, ctx, runID, test.operationState, test.attemptStates,
				test.reservationScope, test.reservationKey, test.conflictingRun,
			)
			summaries := readCompletionBlockerSummaries(
				t, fixture, ctx, runID,
				time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
			)
			if summaries.Effects.ActiveAttempts != test.active ||
				summaries.Effects.OrphanReservations != test.orphan ||
				(summaries.Effects.MalformedBindings > 0) != test.malformed {
				t.Fatalf("external-effect summary = %#v", summaries.Effects)
			}
			assertCompletionBlockerExecution(
				t, fixture, ctx, runID,
				test.active > 0 || test.orphan > 0 || test.malformed,
			)
		})
	}
}

func testCompletionEntityBlockers(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) {
	t.Helper()
	t.Run("entity_malformed_descriptor", func(t *testing.T) {
		runID := seedCompletionBlockerRun(t, fixture, ctx)
		update := `UPDATE entity_state SET flow_instance = ? WHERE run_id = ?`
		args := []any{"unknown/instance", runID}
		if fixture.postgres {
			update = `UPDATE entity_state SET flow_instance = $1 WHERE run_id = $2::uuid`
		}
		if _, err := fixture.db.ExecContext(ctx, update, args...); err != nil {
			t.Fatal(err)
		}
		summaries := readCompletionBlockerSummaries(
			t, fixture, ctx, runID,
			time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
		)
		if summaries.Entities.ReadyForCompletion() || summaries.Entities.Malformed != 1 {
			t.Fatalf("unknown entity terminal descriptor did not fail closed: %#v", summaries.Entities)
		}
		assertCompletionBlockerExecution(t, fixture, ctx, runID, true)
	})
}

func seedCompletionBlockerRun(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) string {
	t.Helper()
	runID := uuid.NewString()
	ensureRunLifecycleCandidateParityRun(
		t, fixture, ctx, runID,
		time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC),
	)
	query := `INSERT OR IGNORE INTO flow_instances (instance_id, flow_template, mode, config, status) VALUES (?, ?, 'static', '{}', 'active')`
	if fixture.postgres {
		query = `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status) VALUES ($1, $2, 'static', '{}'::jsonb, 'active') ON CONFLICT (instance_id) DO NOTHING`
	}
	if _, err := fixture.db.ExecContext(ctx, query, semanticRunFixtureFlow, semanticRunFixtureFlow); err != nil {
		t.Fatalf("seed completion blocker flow instance: %v", err)
	}
	if err := materializeCompletedRunEntityForTest(ctx, fixture.store, runID); err != nil {
		t.Fatalf("seed completion blocker entity: %v", err)
	}
	return runID
}

func seedCompletionBlockerAgent(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) {
	t.Helper()
	seedTestAgentRow(t, ctx, fixture.db, fixture.postgres, testAgentIdentity(t, "completion-matrix-agent", semanticRunFixtureFlow), "active")
}

func insertCompletionBlockerSession(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	holder any,
	expiry any,
) error {
	t.Helper()
	fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, "completion-matrix-agent", semanticRunFixtureFlow))
	query := `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			lease_holder, lease_expires_at, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, 'authored', ?, ?, 'active')`
	args := []any{
		uuid.NewString(), runID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		holder, expiry,
	}
	if fixture.postgres {
		query = `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
				lease_holder, lease_expires_at, status
			) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', $10, $11, 'active')`
	}
	_, err := fixture.db.ExecContext(ctx, query, args...)
	return err
}

func updateCompletionBlockerAccumulator(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	value any,
) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	query := `UPDATE entity_state SET accumulator = ? WHERE run_id = ?`
	args := []any{string(raw), runID}
	if fixture.postgres {
		query = `UPDATE entity_state SET accumulator = $1::jsonb WHERE run_id = $2::uuid`
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("update completion blocker accumulator: %v", err)
	}
}

func insertCompletionBlockerEffect(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	operationState string,
	attemptStates []string,
	reservationScope string,
	reservationKey string,
	conflictingRun bool,
) {
	t.Helper()
	identity := testAgentIdentity(t, "completion-matrix-agent", semanticRunFixtureFlow)
	fields := testAgentIdentityStorageFields(t, identity)
	operationID := uuid.NewString()
	targetID := uuid.NewString()
	sessionID := uuid.NewString()
	authorityRunID := runID
	if conflictingRun {
		authorityRunID = uuid.NewString()
	}
	authority := effects.NormalAgentAuthority(effects.LifecycleToken{
		RuntimeEpoch: 1, Identity: identity, AgentID: identity.AgentID(), Generation: 1,
	}, "matrix-worker", time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC))
	authority.Target = effects.UsageTarget{
		Kind: effects.UsageTargetAgentTurn, ID: targetID, RunID: runID,
		AgentID: identity.AgentID(), AgentIdentity: identity, SessionID: sessionID,
		Memory: agentmemory.PlatformDefault(), FlowInstance: identity.FlowInstance(), EntityID: runID,
	}
	surface := managedCompletionTestSurface(t, authority, "mock_python")
	if err := fixture.store.(interface {
		SaveManagedCapabilitySurface(context.Context, managedcapabilities.Surface) error
	}).SaveManagedCapabilitySurface(ctx, surface); err != nil {
		t.Fatalf("persist completion blocker capability: %v", err)
	}
	planFingerprint, err := surface.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint completion blocker capability: %v", err)
	}
	frameBytes, err := agentframe.EncodeDurable(managedCompletionTestFrame(t, authority, "mock_python"))
	if err != nil {
		t.Fatalf("encode completion blocker frame: %v", err)
	}
	evidence := authority.Evidence()
	if conflictingRun {
		evidence["usage_target"].(map[string]any)["run_id"] = authorityRunID
	}
	authorityEvidence, _ := json.Marshal(evidence)
	lineage, _ := json.Marshal(map[string]any{"run_id": runID})
	query := `
		INSERT INTO runtime_external_effect_operations (
			operation_id, effect_kind, effect_class, execution_mode, bundle_hash,
			authority_kind, authority_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, runtime_epoch, generation,
			capability_plan_fingerprint, agent_frame_bytes, authority_evidence, lineage, request_fingerprint, state
		) VALUES (?, 'provider_turn', 'write_or_unknown', 'live', ?,
		          'normal_agent', 'completion-matrix-agent', ?, ?, ?, ?, ?, ?, ?, 1, 1,
		          ?, ?, ?, ?, 'matrix-request', ?)`
	args := []any{
		operationID, runLifecycleCandidateParityBundleHash,
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		planFingerprint, frameBytes, string(authorityEvidence), string(lineage), operationState,
	}
	if fixture.postgres {
		query = `
			INSERT INTO runtime_external_effect_operations (
				operation_id, effect_kind, effect_class, execution_mode, bundle_hash,
				authority_kind, authority_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, runtime_epoch, generation,
				capability_plan_fingerprint, agent_frame_bytes, authority_evidence, lineage, request_fingerprint, state
			) VALUES ($1::uuid, 'provider_turn', 'write_or_unknown', 'live', $2,
			          'normal_agent', 'completion-matrix-agent', $3, $4, $5, $6, $7, $8, $9, 1, 1,
			          $10, $11, $12::jsonb, $13::jsonb, 'matrix-request', $14)`
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("insert completion blocker operation: %v", err)
	}
	var latestAttemptID string
	for index, state := range attemptStates {
		latestAttemptID = uuid.NewString()
		ordinal := index + 1
		query = `
			INSERT INTO runtime_external_effect_attempts (
				attempt_id, operation_id, attempt_ordinal, adapter, transport,
				execution_mode, generation, execution_owner, lease_expires_at, fence_generation,
				usage_target_kind, usage_target_id, target_ordinal, capability_surface_id, state, authorized_at
			) VALUES (?, ?, ?, 'matrix', 'in_process', 'live', 1, 'matrix-worker', ?, 1,
			          'agent_turn', ?, NULL, ?, ?, ?)`
		now := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC)
		args = []any{latestAttemptID, operationID, ordinal, now.Add(time.Hour), targetID, surface.ID, state, now}
		if fixture.postgres {
			query = `
				INSERT INTO runtime_external_effect_attempts (
					attempt_id, operation_id, attempt_ordinal, adapter, transport,
					execution_mode, generation, execution_owner, lease_expires_at, fence_generation,
					usage_target_kind, usage_target_id, target_ordinal, capability_surface_id, state, authorized_at
				) VALUES ($1::uuid, $2::uuid, $3, 'matrix', 'in_process', 'live', 1, 'matrix-worker', $4, 1,
				          'agent_turn', $5::uuid, NULL, $6::uuid, $7, $8)`
		}
		if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("insert completion blocker attempt: %v", err)
		}
	}
	if reservationScope == "" {
		return
	}
	period := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	query = `INSERT INTO budget_admission_scopes (period_start_utc, scope_kind, scope_key) VALUES (?, ?, ?)`
	args = []any{period, reservationScope, reservationKey}
	if fixture.postgres {
		query = `INSERT INTO budget_admission_scopes (period_start_utc, scope_kind, scope_key) VALUES ($1, $2, $3)`
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("insert completion blocker budget scope: %v", err)
	}
	query = `
		INSERT INTO runtime_effect_budget_reservations (
			attempt_id, period_start_utc, scope_kind, scope_key, cap_usd, amount_usd
		) VALUES (?, ?, ?, ?, 1, 0.5)`
	args = []any{latestAttemptID, period, reservationScope, reservationKey}
	if fixture.postgres {
		query = `
			INSERT INTO runtime_effect_budget_reservations (
				attempt_id, period_start_utc, scope_kind, scope_key, cap_usd, amount_usd
			) VALUES ($1::uuid, $2, $3, $4, 1, 0.5)`
	}
	if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("insert completion blocker reservation: %v", err)
	}
}

func assertCompletionBlockerExecution(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	wantBlock bool,
) {
	t.Helper()
	request := requestRunLifecycleCandidateParity(t, fixture, ctx, runID)
	result, err := fixture.store.ExecuteCompletionCandidate(
		ctx,
		request.Candidate,
		runtimerunlifecycle.NewTerminalCatalog(
			nil,
			map[string][]string{semanticRunFixtureFlow: {"completed"}},
		),
	)
	if err != nil {
		t.Fatalf("execute completion blocker candidate: %v", err)
	}
	snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("load completion blocker lifecycle: %v", err)
	}
	if wantBlock {
		if result.Outcome != runtimerunlifecycle.OutcomeAwaitMutation &&
			result.Outcome != runtimerunlifecycle.OutcomeRearmAt {
			t.Fatalf("blocked completion outcome = %s", result.Outcome)
		}
		if snapshot.Status != string(runtimerunlifecycle.StateRunning) || snapshot.EndedAt != nil {
			t.Fatalf("blocked completion mutated lifecycle: %#v", snapshot)
		}
		return
	}
	if result.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
		t.Fatalf("eligible completion outcome = %s", result.Outcome)
	}
	if snapshot.Status != string(runtimerunlifecycle.StateCompleted) || snapshot.EndedAt == nil {
		t.Fatalf("eligible completion did not terminalize lifecycle: %#v", snapshot)
	}
}

func readCompletionBlockerSummaries(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	observedAt time.Time,
) runCompletionOwnerSummaries {
	t.Helper()
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	catalog := runtimerunlifecycle.NewTerminalCatalog(
		nil,
		map[string][]string{semanticRunFixtureFlow: {"completed"}},
	)
	if fixture.postgres {
		summaries, err := fixture.store.(*PostgresStore).runLifecyclePostgresOwner.LoadRunCompletionOwnerSummariesTx(ctx, tx, runID, observedAt, catalog)
		if err != nil {
			t.Fatalf("read PostgreSQL completion owner summaries: %v", err)
		}
		return summaries
	}
	summaries, err := fixture.store.(*SQLiteRuntimeStore).runLifecycleSQLiteOwner.LoadRunCompletionOwnerSummariesTx(ctx, tx, runID, observedAt, catalog)
	if err != nil {
		t.Fatalf("read SQLite completion owner summaries: %v", err)
	}
	return summaries
}
