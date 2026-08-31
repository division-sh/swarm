package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type runOriginReadStore interface {
	LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	ListRunHeaders(context.Context, operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error)
}

type standingExecutionCountingStore struct {
	delegate   runtimerunlifecycle.CandidateStore
	executions atomic.Int64
}

func (s *standingExecutionCountingStore) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	return s.delegate.ListCompletionCandidates(ctx, scope, cursor, limit)
}

func (s *standingExecutionCountingStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	s.executions.Add(1)
	return s.delegate.ExecuteCompletionCandidate(ctx, candidate, catalog)
}

func TestEventAndScenarioRunOriginLifecycleParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			reader := fixture.store.(runOriginReadStore)
			ctx := testAuthorActivityContextForBundle(runLifecycleCandidateParityBundleHash)
			at := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)

			t.Run("event_origin_survives_later_event", func(t *testing.T) {
				runID := uuid.NewString()
				trigger := eventtest.RunCreatingRootIngress(
					uuid.NewString(), "origin.trigger", "ingress", "", json.RawMessage(`{}`), 0,
					runID, "", events.EventEnvelope{}, at,
				)
				if err := commitSemanticEventFixture(ctx, fixture.store, trigger); err != nil {
					t.Fatalf("commit run-creating event: %v", err)
				}
				want, err := runtimerunlifecycle.EventRunOrigin(trigger.ID(), string(trigger.Type()))
				if err != nil {
					t.Fatal(err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)

				later := eventtest.ExistingRunRootIngress(
					uuid.NewString(), "origin.later", "ingress", "", json.RawMessage(`{}`), 0,
					runID, events.EventEnvelope{}, at.Add(time.Second),
				)
				if err := commitSemanticEventFixture(ctx, fixture.store, later); err != nil {
					t.Fatalf("commit later event: %v", err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 2)
			})

			t.Run("scenario_origin_survives_publication_and_lifecycle", func(t *testing.T) {
				runID := uuid.NewString()
				source := mustStoreTestEphemeralBundleSourceFact(runLifecycleCandidateParityBundleHash)
				request := runtimerunlifecycle.CreateRequest{
					RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
					Source: source, StartedAt: at.Add(time.Minute),
				}
				if disposition, err := createRunLifecycleParity(fixture, ctx, request); err != nil ||
					disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("create scenario run = %s/%v", disposition, err)
				}
				if disposition, err := createRunLifecycleParity(fixture, ctx, request); err != nil ||
					disposition != runtimerunlifecycle.MutationExactNoop {
					t.Fatalf("replay scenario run creation = %s/%v", disposition, err)
				}
				want := runtimerunlifecycle.ScenarioSetupRunOrigin()
				requireRunOriginHeader(t, ctx, reader, runID, want, 0)
				requireListedRunOrigin(t, ctx, reader, runID, want)

				later := eventtest.ExistingRunRootIngress(
					uuid.NewString(), "scenario.later", "setup", "", json.RawMessage(`{}`), 0,
					runID, events.EventEnvelope{}, at.Add(time.Minute+time.Second),
				)
				if err := commitSemanticEventFixture(ctx, fixture.store, later); err != nil {
					t.Fatalf("commit scenario event: %v", err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)

				replacement := mustStoreTestEphemeralBundleSourceFact(runLifecycleCandidateParityReplacementHash)
				if disposition, err := reviseRunLifecycleSourceParity(fixture, ctx, runID, replacement); err != nil ||
					disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("revise scenario source = %s/%v", disposition, err)
				}
				ctx = testAuthorActivityContextForBundle(runLifecycleCandidateParityReplacementHash)
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				for _, state := range []runtimerunlifecycle.State{
					runtimerunlifecycle.StatePaused,
					runtimerunlifecycle.StateRunning,
				} {
					if disposition, err := transitionRunLifecycleParity(fixture, ctx, runID, state); err != nil ||
						disposition != runtimerunlifecycle.MutationApplied {
						t.Fatalf("transition scenario run to %s = %s/%v", state, disposition, err)
					}
					requireRunOriginHeader(t, ctx, reader, runID, want, 1)
				}
				if _, disposition, err := terminalizeRunLifecycleCandidateParity(
					fixture, ctx, runID, runtimerunlifecycle.StateCancelled, at.Add(2*time.Minute),
				); err != nil || disposition != runtimerunlifecycle.MutationApplied {
					t.Fatalf("cancel scenario run = %s/%v", disposition, err)
				}
				requireRunOriginHeader(t, ctx, reader, runID, want, 1)
			})
		})
	}
}

func TestStandingGenerationRunOriginNamedOperationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			var (
				db       *sql.DB
				selected workflowTestSelectedStore
				workflow *runtimepipeline.PipelineCoordinator
			)
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				selected = store
				workflow = newSQLiteWorkflowTestCoordinator(t, db, store)
			} else {
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := admitTestPostgresStore(t, postgresDB)
				db = postgresDB
				selected = store
				workflow = newPostgresWorkflowTestCoordinator(t, db, store)
			}
			reader := selected.(runOriginReadStore)
			ctx := testAuthorActivityRuntimeContext()
			serviceID := runtimeflowidentity.StandingServiceID("origin-parity", backend)
			firstHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
			candidate := runtimepipeline.StandingServiceCandidate{
				ServiceID: serviceID, PackageKey: "origin-parity", FlowID: backend,
				InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
				Source: mustStoreTestPersistedBundleSourceFact(firstHash),
			}
			seedStoreTestPersistedBundle(t, db, firstHash)

			fresh, err := workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing generation: %v", err)
			}
			requireStandingGenerationOrigin(t, ctx, reader, db, backend, fresh.RunID, serviceID, 1)
			replayedFresh, err := workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("replay standing generation creation: %v", err)
			}
			if replayedFresh.RunID != fresh.RunID || replayedFresh.Generation != fresh.Generation {
				t.Fatalf("replayed standing generation = %#v, want run %s generation %d", replayedFresh, fresh.RunID, fresh.Generation)
			}
			requireStandingGenerationOrigin(t, ctx, reader, db, backend, fresh.RunID, serviceID, 1)
			commitStandingOriginIngress(t, selected, firstHash, fresh.RunID, "fresh")
			requireStandingGenerationOrigin(t, ctx, reader, db, backend, fresh.RunID, serviceID, 1)

			reset, err := workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{
				ServiceID: serviceID, Actor: "origin-parity",
			})
			if err != nil {
				t.Fatalf("reset standing generation: %v", err)
			}
			requireStandingGenerationOrigin(t, ctx, reader, db, backend, reset.RunID, serviceID, 2)
			commitStandingOriginIngress(t, selected, firstHash, reset.RunID, "reset")
			requireStandingGenerationOrigin(t, ctx, reader, db, backend, reset.RunID, serviceID, 2)

			wrongServiceID := uuid.NewString()
			wrongOrigin, err := runtimerunlifecycle.StandingGenerationRunOrigin(wrongServiceID, 2)
			if err != nil {
				t.Fatal(err)
			}
			if backend == "postgres" {
				runlifecyclefixture.CorruptPostgresOrigin(t, ctx, db, reset.RunID, wrongOrigin)
			} else {
				runlifecyclefixture.CorruptSQLiteOrigin(t, ctx, db, reset.RunID, wrongOrigin)
			}
			if _, err := reader.LoadRunHeader(ctx, reset.RunID); err == nil ||
				!strings.Contains(err.Error(), "standing generation origin relation is invalid") {
				t.Fatalf("mismatched standing relation readback error = %v", err)
			}
		})
	}
}

func TestTerminalStandingGenerationDoesNotSeedCompletionCandidateParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			var (
				db       *sql.DB
				selected workflowTestSelectedStore
				workflow *runtimepipeline.PipelineCoordinator
			)
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				selected = store
				workflow = newSQLiteWorkflowTestCoordinator(t, db, store)
			} else {
				_, postgresDB, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := admitTestPostgresStore(t, postgresDB)
				db = postgresDB
				selected = store
				workflow = newPostgresWorkflowTestCoordinator(t, db, store)
			}
			candidateStore := selected.(runLifecycleCandidateParityStore)
			registrar := selected.(runtimerunlifecycle.CandidateRegistrar)
			ctx := testAuthorActivityRuntimeContext()
			serviceID := runtimeflowidentity.StandingServiceID("repair-candidate", "standing")
			firstHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
			secondHash := "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444"
			candidate := runtimepipeline.StandingServiceCandidate{
				ServiceID: serviceID, PackageKey: "repair-candidate", FlowID: "standing",
				InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
				Source: mustStoreTestPersistedBundleSourceFact(firstHash),
			}
			seedStoreTestPersistedBundle(t, db, firstHash)
			fresh, err := workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing generation: %v", err)
			}
			if _, err := workflow.PublishStandingService(ctx, serviceID, fresh.RunID, fresh.Generation); err != nil {
				t.Fatalf("publish standing generation: %v", err)
			}
			seedStandingRepairEntityState(t, ctx, db, backend, fresh.RunID, candidate.EntityID)
			query := `UPDATE entity_state SET current_state = 'completed' WHERE run_id = ? AND entity_id = ?`
			args := []any{fresh.RunID, candidate.EntityID}
			if backend == "postgres" {
				query = `UPDATE entity_state SET current_state = 'completed' WHERE run_id = $1::uuid AND entity_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("make standing entity terminal: %v", err)
			}
			if _, err := markRunTerminalStatusForTest(ctx, selected, fresh.RunID, string(runtimerunlifecycle.StateCancelled), nil, time.Now().UTC()); err != nil {
				t.Fatalf("cancel abandoned standing generation: %v", err)
			}
			insertStandingRestartAbandonControl(t, ctx, db, backend, fresh.RunID)

			candidate.Source = mustStoreTestPersistedBundleSourceFact(secondHash)
			seedStoreTestPersistedBundle(t, db, secondHash)
			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrenceForBundle(t, process, secondHash)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			countingStore := &standingExecutionCountingStore{delegate: candidateStore}
			executor := newRunLifecycleParityExecutorForScope(
				t,
				countingStore,
				occurrence,
				secondHash,
				runtimerunlifecycle.NewTerminalCatalog(nil, map[string][]string{"standing/root": {"completed"}}),
			)
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: secondHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register repair candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start repair candidate executor: %v", err)
			}

			stopped, err := workflow.ReconcileStandingService(runtimeCtx, candidate)
			if err != nil {
				t.Fatalf("reconcile terminal standing generation: %v", err)
			}
			if stopped.Transition != "revised" || stopped.RunID != fresh.RunID || stopped.Generation != fresh.Generation || stopped.BundleHash != secondHash ||
				stopped.RestartDisposition.Kind != runtimepipeline.StandingRestartTerminalDeclared {
				t.Fatalf("terminal standing result = %#v", stopped)
			}
			awaitRunLifecycleState(t, candidateStore, fresh.RunID, runtimerunlifecycle.StateCancelled)
			fixture := runLifecycleCandidateParityFixture{store: candidateStore, db: db, postgres: backend == "postgres"}
			state, duePresent, _ := loadRunLifecycleCandidateFacts(t, fixture, ctx, fresh.RunID)
			if state != string(runtimerunlifecycle.StateCancelled) || duePresent {
				t.Fatalf("terminal candidate posture = state:%s due:%v, want cancelled/false", state, duePresent)
			}
			awaitRunLifecycleExecutorCandidates(t, executor, 0)

			suspendedServiceID := runtimeflowidentity.StandingServiceID("repair-candidate-suspended", "standing")
			suspendedCandidate := runtimepipeline.StandingServiceCandidate{
				ServiceID: suspendedServiceID, PackageKey: "repair-candidate-suspended", FlowID: "standing",
				InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
				Source: mustStoreTestPersistedBundleSourceFact(firstHash),
			}
			suspendedFresh, err := workflow.ReconcileStandingService(ctx, suspendedCandidate)
			if err != nil {
				t.Fatalf("create suspended standing generation: %v", err)
			}
			if _, err := workflow.PublishStandingService(ctx, suspendedServiceID, suspendedFresh.RunID, suspendedFresh.Generation); err != nil {
				t.Fatalf("publish suspended standing generation: %v", err)
			}
			seedStandingRepairEntityState(t, ctx, db, backend, suspendedFresh.RunID, suspendedCandidate.EntityID)
			query = `UPDATE entity_state SET current_state = 'completed' WHERE run_id = ? AND entity_id = ?`
			args = []any{suspendedFresh.RunID, suspendedCandidate.EntityID}
			if backend == "postgres" {
				query = `UPDATE entity_state SET current_state = 'completed' WHERE run_id = $1::uuid AND entity_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("make suspended standing entity terminal: %v", err)
			}
			if _, err := workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{
				ServiceID: suspendedServiceID, Actor: "repair-parity", Reason: "maintenance",
			}); err != nil {
				t.Fatalf("suspend standing generation before repair: %v", err)
			}
			if _, err := markRunTerminalStatusForTest(ctx, selected, suspendedFresh.RunID, string(runtimerunlifecycle.StateCancelled), nil, time.Now().UTC()); err != nil {
				t.Fatalf("cancel suspended abandoned generation: %v", err)
			}
			insertStandingRestartAbandonControl(t, ctx, db, backend, suspendedFresh.RunID)

			suspendedCandidate.Source = mustStoreTestPersistedBundleSourceFact(secondHash)
			beforeSuspendedRepair := countingStore.executions.Load()
			suspendedStopped, err := workflow.ReconcileStandingService(runtimeCtx, suspendedCandidate)
			if err != nil {
				t.Fatalf("reconcile terminal suspended standing generation: %v", err)
			}
			if suspendedStopped.Transition != "revised" || suspendedStopped.EffectiveState != "suspended" || suspendedStopped.BundleHash != secondHash ||
				suspendedStopped.RunID != suspendedFresh.RunID || suspendedStopped.Generation != suspendedFresh.Generation ||
				suspendedStopped.RestartDisposition.Kind != runtimepipeline.StandingRestartTerminalDeclared {
				t.Fatalf("terminal suspended result = %#v", suspendedStopped)
			}
			state, duePresent, _ = loadRunLifecycleCandidateFacts(t, fixture, ctx, suspendedFresh.RunID)
			if state != string(runtimerunlifecycle.StateCancelled) || duePresent {
				t.Fatalf("terminal suspended candidate = state:%s due:%v, want cancelled/false", state, duePresent)
			}
			awaitRunLifecycleExecutorCandidates(t, executor, 0)
			if got := countingStore.executions.Load(); got != beforeSuspendedRepair {
				t.Fatalf("candidate executions while terminal standing remained parked = %d, want %d", got, beforeSuspendedRepair)
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire repair candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func seedStandingRepairEntityState(t *testing.T, ctx context.Context, db *sql.DB, backend, runID, entityID string) {
	t.Helper()
	at := time.Date(2026, 7, 29, 18, 0, 0, 0, time.UTC)
	query := `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES (?, ?, 'standing/root', 'standing_service', 'serving',
			'{"ready":true}', '{"name":"standing"}', '{"generation":2}', '{"metrics":{"requests":3}}', 4,
			?, ?, ?)
		ON CONFLICT(run_id, entity_id) DO UPDATE SET
			current_state = excluded.current_state,
			gates = excluded.gates,
			fields = excluded.fields,
			bookkeeping = excluded.bookkeeping,
			accumulator = excluded.accumulator,
			revision = excluded.revision,
			updated_at = excluded.updated_at`
	args := []any{runID, entityID, at, at, at}
	if backend == "postgres" {
		query = `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, current_state,
				gates, fields, bookkeeping, accumulator, revision,
				entered_state_at, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, 'standing/root', 'standing_service', 'serving',
				'{"ready":true}'::jsonb, '{"name":"standing"}'::jsonb, '{"generation":2}'::jsonb, '{"metrics":{"requests":3}}'::jsonb, 4,
				$3, $3, $3)
			ON CONFLICT(run_id, entity_id) DO UPDATE SET
				current_state = EXCLUDED.current_state,
				gates = EXCLUDED.gates,
				fields = EXCLUDED.fields,
				bookkeeping = EXCLUDED.bookkeeping,
				accumulator = EXCLUDED.accumulator,
				revision = EXCLUDED.revision,
				updated_at = EXCLUDED.updated_at`
		args = []any{runID, entityID, at}
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("seed standing repair entity state: %v", err)
	}
}

func requireStandingRepairMutationProjection(t *testing.T, ctx context.Context, db *sql.DB, backend, runID, entityID string) {
	t.Helper()
	postgres := backend == "postgres"
	stateQuery := `SELECT entity_type, current_state, gates, fields, bookkeeping, accumulator FROM entity_state WHERE run_id = ? AND entity_id = ?`
	mutationQuery := `SELECT domain, path, new_value, writer_type, writer_id, COALESCE(handler_step, '') FROM entity_mutations WHERE run_id = ? AND entity_id = ? ORDER BY created_at, mutation_id`
	if postgres {
		stateQuery = `SELECT entity_type, current_state, gates, fields, bookkeeping, accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
		mutationQuery = `SELECT domain, path, new_value, writer_type, writer_id, COALESCE(handler_step, '') FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid ORDER BY created_at, mutation_id`
	}
	var entityType, currentState string
	var gatesRaw, fieldsRaw, bookkeepingRaw, accumulatorRaw any
	if err := db.QueryRowContext(ctx, stateQuery, runID, entityID).Scan(&entityType, &currentState, &gatesRaw, &fieldsRaw, &bookkeepingRaw, &accumulatorRaw); err != nil {
		t.Fatalf("load repaired standing entity state: %v", err)
	}
	if entityType != "standing_service" {
		t.Fatalf("repaired standing entity type = %q, want standing_service", entityType)
	}
	live := runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState),
		Gates:        decodeMutationProjectionMap(t, gatesRaw),
		Fields:       decodeMutationProjectionMap(t, fieldsRaw),
		Bookkeeping:  decodeMutationProjectionMap(t, bookkeepingRaw),
		Accumulator:  decodeMutationProjectionMap(t, accumulatorRaw),
	}
	rows, err := db.QueryContext(ctx, mutationQuery, runID, entityID)
	if err != nil {
		t.Fatalf("load repaired standing entity mutations: %v", err)
	}
	defer rows.Close()
	mutations := []runtimemutationlog.ProjectionMutation{}
	for rows.Next() {
		var domain, path, writerType, writerID, handlerStep string
		var raw any
		if err := rows.Scan(&domain, &path, &raw, &writerType, &writerID, &handlerStep); err != nil {
			t.Fatal(err)
		}
		if writerType != "platform" || writerID != "standing_service" || handlerStep != "repair_generation" {
			t.Fatalf("standing repair mutation owner = %s/%s/%s", writerType, writerID, handlerStep)
		}
		mutations = append(mutations, runtimemutationlog.ProjectionMutation{
			Domain: runtimemutationlog.Domain(domain), Path: path, NewValue: decodeMutationProjectionValue(t, raw),
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(mutations) == 0 {
		t.Fatal("standing repair recorded no entity mutations")
	}
	reconstructed, err := runtimemutationlog.ReconstructEntityStateProjection(mutations)
	if err != nil {
		t.Fatalf("reconstruct standing repair mutations: %v", err)
	}
	if !reflect.DeepEqual(reconstructed, live) {
		t.Fatalf("standing repair history = %#v, live state = %#v", reconstructed, live)
	}
}

func decodeMutationProjectionMap(t *testing.T, raw any) map[string]any {
	t.Helper()
	value := decodeMutationProjectionValue(t, raw)
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("mutation projection map = %#v", value)
	}
	return result
}

func decodeMutationProjectionValue(t *testing.T, raw any) any {
	t.Helper()
	var encoded []byte
	switch typed := raw.(type) {
	case nil:
		return nil
	case []byte:
		encoded = typed
	case string:
		encoded = []byte(typed)
	default:
		t.Fatalf("unexpected mutation projection JSON type %T", raw)
	}
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatalf("decode mutation projection JSON %q: %v", string(encoded), err)
	}
	return value
}

func TestRunOriginStorageConstraintParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := context.Background()
			eventID := uuid.NewString()
			sourceRunID := uuid.NewString()
			sourceEventID := uuid.NewString()
			serviceID := uuid.NewString()
			cases := []struct {
				name                                string
				kind, eventID, eventType, serviceID string
				generation                          int64
				sourceRunID, sourceEventID          string
			}{
				{name: "unknown_kind", kind: "unknown"},
				{name: "event_missing_id", kind: string(runtimerunlifecycle.OriginEvent), eventType: "origin.event"},
				{name: "event_missing_type", kind: string(runtimerunlifecycle.OriginEvent), eventID: eventID},
				{name: "scenario_with_trigger", kind: string(runtimerunlifecycle.OriginScenarioSetup), eventID: eventID, eventType: "origin.event"},
				{name: "standing_missing_service", kind: string(runtimerunlifecycle.OriginStandingGeneration), generation: 1},
				{name: "standing_nonpositive_generation", kind: string(runtimerunlifecycle.OriginStandingGeneration), serviceID: serviceID},
				{name: "fork_missing_source_run", kind: string(runtimerunlifecycle.OriginForkMaterialization), sourceEventID: sourceEventID},
				{name: "fork_missing_source_event", kind: string(runtimerunlifecycle.OriginForkMaterialization), sourceRunID: sourceRunID},
				{name: "scenario_with_fork_pair", kind: string(runtimerunlifecycle.OriginScenarioSetup), sourceRunID: sourceRunID, sourceEventID: sourceEventID},
			}
			for _, test := range cases {
				test := test
				t.Run(test.name, func(t *testing.T) {
					err := insertRawRunOrigin(
						ctx, fixture.db, backend, uuid.NewString(),
						test.kind, test.eventID, test.eventType, test.serviceID, test.generation,
						test.sourceRunID, test.sourceEventID,
					)
					if err == nil {
						t.Fatalf("invalid run origin %s was persisted", test.name)
					}
				})
			}

			runID := uuid.NewString()
			if err := insertRawRunOrigin(
				ctx, fixture.db, backend, runID,
				string(runtimerunlifecycle.OriginStandingGeneration), "", "", serviceID, 1, "", "",
			); err != nil {
				t.Fatalf("insert standing origin without relation: %v", err)
			}
			reader := fixture.store.(runOriginReadStore)
			if _, err := reader.LoadRunHeader(ctx, runID); err == nil ||
				!strings.Contains(err.Error(), "standing generation origin relation is invalid") {
				t.Fatalf("missing standing relation readback error = %v", err)
			}
		})
	}
}

func requireRunOriginHeader(
	t *testing.T,
	ctx context.Context,
	reader runOriginReadStore,
	runID string,
	want runtimerunlifecycle.RunOrigin,
	wantEventCount int,
) {
	t.Helper()
	header, err := reader.LoadRunHeader(ctx, runID)
	if err != nil {
		t.Fatalf("load run header %s: %v", runID, err)
	}
	if !header.Origin.Equal(want) || header.EventCount != wantEventCount {
		t.Fatalf("run %s header origin/count = %#v/%d, want %#v/%d", runID, header.Origin, header.EventCount, want, wantEventCount)
	}
}

func requireListedRunOrigin(
	t *testing.T,
	ctx context.Context,
	reader runOriginReadStore,
	runID string,
	want runtimerunlifecycle.RunOrigin,
) {
	t.Helper()
	headers, _, err := reader.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 500})
	if err != nil {
		t.Fatalf("list run headers: %v", err)
	}
	for _, header := range headers {
		if header.RunID == runID {
			if !header.Origin.Equal(want) {
				t.Fatalf("listed run %s origin = %#v, want %#v", runID, header.Origin, want)
			}
			return
		}
	}
	t.Fatalf("run %s missing from canonical listing", runID)
}

func requireStandingGenerationOrigin(
	t *testing.T,
	ctx context.Context,
	reader runOriginReadStore,
	db *sql.DB,
	backend string,
	runID string,
	serviceID string,
	generation int64,
) {
	t.Helper()
	want, err := runtimerunlifecycle.StandingGenerationRunOrigin(serviceID, generation)
	if err != nil {
		t.Fatal(err)
	}
	header, err := reader.LoadRunHeader(ctx, runID)
	if err != nil {
		t.Fatalf("load standing generation %d header: %v", generation, err)
	}
	if !header.Origin.Equal(want) {
		t.Fatalf("standing generation %d origin = %#v, want %#v", generation, header.Origin, want)
	}
	requireListedRunOrigin(t, ctx, reader, runID, want)
	query := `
		SELECT COUNT(*)
		FROM standing_service_generations
		WHERE service_id = ? AND generation = ? AND run_id = ?
	`
	if backend == "postgres" {
		query = `
			SELECT COUNT(*)
			FROM standing_service_generations
			WHERE service_id = $1::uuid AND generation = $2 AND run_id = $3::uuid
		`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, serviceID, generation, runID).Scan(&count); err != nil {
		t.Fatalf("load standing generation %d relation: %v", generation, err)
	}
	if count != 1 {
		t.Fatalf("standing generation %d relation count = %d, want 1", generation, count)
	}
}

func commitStandingOriginIngress(t *testing.T, selected any, bundleHash, runID, suffix string) {
	t.Helper()
	ctx := testAuthorActivityContextForBundle(bundleHash)
	event := eventtest.ExistingRunRootIngress(
		uuid.NewString(), events.EventType("standing.origin."+suffix), "ingress", "", json.RawMessage(`{}`), 0,
		runID, events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := commitSemanticEventFixture(ctx, selected, event); err != nil {
		t.Fatalf("commit %s standing ingress: %v", suffix, err)
	}
	if err := acknowledgePipelineEventFixture(ctx, selected, event.ID()); err != nil {
		t.Fatalf("acknowledge %s standing ingress: %v", suffix, err)
	}
}

func insertStandingRestartAbandonControl(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	backend string,
	runID string,
) {
	t.Helper()
	now := time.Now().UTC()
	query := `
		INSERT INTO run_control_state (
			run_id, control_status, reason, controlled_by, paused_at, stopped_at, updated_at
		)
		VALUES (?, 'stopped', 'server_restart_abandon', 'origin-parity', NULL, ?, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = excluded.control_status,
			reason = excluded.reason,
			controlled_by = excluded.controlled_by,
			paused_at = NULL,
			stopped_at = excluded.stopped_at,
			updated_at = excluded.updated_at
	`
	if backend == "postgres" {
		query = `
			INSERT INTO run_control_state (
				run_id, control_status, reason, controlled_by, paused_at, stopped_at, updated_at
			)
			VALUES ($1::uuid, 'stopped', 'server_restart_abandon', 'origin-parity', NULL, $2, $3)
			ON CONFLICT(run_id) DO UPDATE SET
				control_status = EXCLUDED.control_status,
				reason = EXCLUDED.reason,
				controlled_by = EXCLUDED.controlled_by,
				paused_at = NULL,
				stopped_at = EXCLUDED.stopped_at,
				updated_at = EXCLUDED.updated_at
		`
	}
	if _, err := db.ExecContext(ctx, query, runID, now, now); err != nil {
		t.Fatalf("insert restart-abandon control: %v", err)
	}
}

func insertRawRunOrigin(
	ctx context.Context,
	db *sql.DB,
	backend string,
	runID string,
	kind string,
	eventID string,
	eventType string,
	serviceID string,
	generation int64,
	sourceRunID string,
	sourceEventID string,
) error {
	snapshot := runlifecyclefixture.CorruptSnapshot{
		RunID: runID, State: string(runtimerunlifecycle.StateRunning),
		BundleHash: runLifecycleCandidateParityBundleHash, BundleSource: runtimerunlifecycle.BundleSourceEphemeral,
		OriginKind: kind, TriggerEventID: eventID, TriggerEventType: eventType,
		OriginServiceID: serviceID, OriginGeneration: generation,
		ForkedFromRunID: sourceRunID, ForkedFromEventID: sourceEventID,
		StartedAt: time.Date(2026, 7, 29, 17, 0, 0, 0, time.UTC),
	}
	if backend == "postgres" {
		return runlifecyclefixture.AttemptCorruptPostgresSnapshot(ctx, db, snapshot)
	}
	return runlifecyclefixture.AttemptCorruptSQLiteSnapshot(ctx, db, snapshot)
}
