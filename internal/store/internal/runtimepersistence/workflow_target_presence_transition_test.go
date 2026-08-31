package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func TestWorkflowEngineStateOnlyCompanionTransitionAtomicOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			owner, ok := selected.(runtimepipeline.WorkflowEngineMutationOwner)
			if !ok {
				t.Fatalf("%s selected store does not expose the workflow mutation owner", backend)
			}

			t.Run("state only creates exact companion", func(t *testing.T) {
				flowID := "state-only-success-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)

				if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
					t.Fatalf("commit state-only companion transition: %v", err)
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "done", 2, 1)
			})

			t.Run("absent target creates exact state and companion", func(t *testing.T) {
				flowID := "absent-target-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "", 0, createdAt)
				record.Transition = runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion

				if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
					t.Fatalf("commit absent target transition: %v", err)
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "done", 1, 1)
			})

			t.Run("stale state rolls back companion", func(t *testing.T) {
				flowID := "state-only-stale-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 2, createdAt)

				_, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
				failure, typed := runtimefailures.As(err)
				if err == nil || !typed || failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" || !failure.Failure.Retryable {
					t.Fatalf("stale state-only transition error = %v", err)
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, "", "active", 1, 0)
			})

			t.Run("preexisting companion contradiction rolls back state", func(t *testing.T) {
				flowID := "state-only-race-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)
				seedStateOnlyAcquisitionLifecycle(t, backend, db, runID, instancePath, "active")
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)

				if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err == nil {
					t.Fatal("concurrent companion winner was accepted")
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, "review", "active", 1, 1)
			})

			t.Run("two simultaneous first mutations commit exactly one companion", func(t *testing.T) {
				flowID := "state-only-contenders-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)

				start := make(chan struct{})
				results := make(chan error, 2)
				var contenders sync.WaitGroup
				for range 2 {
					contenders.Add(1)
					go func() {
						defer contenders.Done()
						<-start
						_, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
						results <- err
					}()
				}
				close(start)
				contenders.Wait()
				close(results)

				successes := 0
				failures := 0
				for err := range results {
					if err == nil {
						successes++
					} else {
						failures++
					}
				}
				if successes != 1 || failures != 1 {
					t.Fatalf("simultaneous first mutations succeeded/failed = %d/%d, want 1/1", successes, failures)
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "done", 2, 1)
			})

			t.Run("committed first mutation reloads complete and uses paired update", func(t *testing.T) {
				flowID := "state-only-retry-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, instancePath, "active", 1, createdAt)
				first := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)
				if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: first}); err != nil {
					t.Fatalf("commit first state-only mutation: %v", err)
				}

				reader, ok := selected.(runtimepipeline.WorkflowTargetPersistenceReader)
				if !ok {
					t.Fatalf("%s selected store does not expose the workflow target reader", backend)
				}
				persisted, err := reader.LoadWorkflowTargetPersistence(ctx, first.Identity, runtimeidentity.NormalizeEntityID(entityID))
				if err != nil {
					t.Fatalf("reload committed target: %v", err)
				}
				if err := persisted.Validate(first.Identity.Route, runtimeidentity.NormalizeEntityID(entityID)); err != nil {
					t.Fatalf("validate reloaded committed target: %v", err)
				}
				if persisted.Presence != runtimepipeline.WorkflowTargetPersistenceComplete {
					t.Fatalf("reloaded target presence = %d, want complete", persisted.Presence)
				}
				transition, err := runtimepipeline.WorkflowEngineStateTransitionForPresence(persisted.Presence)
				if err != nil || transition != runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion {
					t.Fatalf("reloaded target transition = %d error = %v, want paired update", transition, err)
				}

				retry := first
				retry.CurrentState = "settled"
				retry.ExpectedState = persisted.State.CurrentState
				retry.ExpectedRevision = persisted.State.Revision
				retry.EnteredStageAt = first.EnteredStageAt.Add(time.Minute)
				retry.UpdatedAt = first.UpdatedAt.Add(time.Minute)
				retry.Transition = transition
				if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: retry}); err != nil {
					t.Fatalf("commit mutation after complete reload: %v", err)
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "settled", 3, 1)
			})
		})
	}
}

func TestWorkflowTargetPersistenceReadNeverFabricatesMixedSnapshotOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			reader, ok := selected.(runtimepipeline.WorkflowTargetPersistenceReader)
			if !ok {
				t.Fatalf("%s selected store does not expose the workflow target reader", backend)
			}
			owner, ok := selected.(runtimepipeline.WorkflowEngineMutationOwner)
			if !ok {
				t.Fatalf("%s selected store does not expose the workflow mutation owner", backend)
			}

			for attempt := range 12 {
				flowID := "snapshot-target-" + uuid.NewString()
				instancePath := flowID + "/receiver"
				entityID := uuid.NewString()
				createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "", 0, createdAt)
				record.Transition = runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion
				routeEntityID := runtimeidentity.NormalizeEntityID(entityID)

				initial, err := reader.LoadWorkflowTargetPersistence(ctx, record.Identity, routeEntityID)
				if err != nil || initial.Presence != runtimepipeline.WorkflowTargetPersistenceAbsent {
					t.Fatalf("attempt %d initial target presence = %d error = %v, want absent", attempt, initial.Presence, err)
				}

				start := make(chan struct{})
				errors := make(chan error, 49)
				var readers sync.WaitGroup
				for range 3 {
					readers.Add(1)
					go func() {
						defer readers.Done()
						<-start
						for range 16 {
							target, err := reader.LoadWorkflowTargetPersistence(ctx, record.Identity, routeEntityID)
							if err != nil {
								errors <- err
								continue
							}
							if target.Presence != runtimepipeline.WorkflowTargetPersistenceAbsent && target.Presence != runtimepipeline.WorkflowTargetPersistenceComplete {
								errors <- fmt.Errorf("observed impossible atomic create presence %d", target.Presence)
							}
						}
					}()
				}
				close(start)
				if _, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
					t.Fatalf("attempt %d commit atomic target pair: %v", attempt, err)
				}
				readers.Wait()
				close(errors)
				for err := range errors {
					t.Fatalf("attempt %d target snapshot read: %v", attempt, err)
				}
				persisted, err := reader.LoadWorkflowTargetPersistence(ctx, record.Identity, routeEntityID)
				if err != nil || persisted.Presence != runtimepipeline.WorkflowTargetPersistenceComplete {
					t.Fatalf("attempt %d final target presence = %d error = %v, want complete", attempt, persisted.Presence, err)
				}
			}
		})
	}
}

func TestWorkflowTargetPresenceOwnsEveryValidTransition(t *testing.T) {
	tests := []struct {
		name       string
		presence   runtimepipeline.WorkflowTargetPersistencePresence
		transition runtimepipeline.WorkflowEngineStateTransition
		wantError  string
	}{
		{name: "absent", presence: runtimepipeline.WorkflowTargetPersistenceAbsent, transition: runtimepipeline.WorkflowEngineStateTransitionCreateStateAndCompanion},
		{name: "state only", presence: runtimepipeline.WorkflowTargetPersistenceStateOnly, transition: runtimepipeline.WorkflowEngineStateTransitionUpdateStateCreateCompanion},
		{name: "complete", presence: runtimepipeline.WorkflowTargetPersistenceComplete, transition: runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion},
		{name: "lifecycle only", presence: runtimepipeline.WorkflowTargetPersistenceLifecycleOnly, wantError: "rejects lifecycle companion without state"},
		{name: "unknown", presence: runtimepipeline.WorkflowTargetPersistencePresenceUnknown, wantError: "requires closed target persistence presence"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runtimepipeline.WorkflowEngineStateTransitionForPresence(tc.presence)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || got != runtimepipeline.WorkflowEngineStateTransitionUnknown {
					t.Fatalf("transition = %d error = %v, want unknown/%q", got, err, tc.wantError)
				}
				return
			}
			if err != nil || got != tc.transition {
				t.Fatalf("transition = %d error = %v, want %d", got, err, tc.transition)
			}
		})
	}
}

func TestSupportedStateOnlyProducersReachWorkflowCompanionTransitionOnBothStores(t *testing.T) {
	type producerStore interface {
		runtimepipeline.WorkflowEngineMutationOwner
		SetupScenarioEntities(context.Context, runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error)
		CreateEntity(context.Context, runtimetools.EntityCreateRecord) error
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			store, ok := selected.(producerStore)
			if !ok {
				t.Fatalf("%s selected store does not expose supported state-only producers", backend)
			}
			for _, producer := range []string{"scenario_setup", "entity_tool"} {
				t.Run(producer, func(t *testing.T) {
					flowID := producer + "-" + uuid.NewString()
					instancePath := flowID + "/receiver"
					entityID := uuid.NewString()
					createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					switch producer {
					case "scenario_setup":
						_, err := store.SetupScenarioEntities(ctx, runtimepipeline.ScenarioSetupRequest{
							RunID: runID, CreatedAt: createdAt,
							Entities: []runtimepipeline.ScenarioSetupEntityRequest{{
								Alias: "receiver", EntityID: entityID, FlowInstance: instancePath,
								EntityType: "review_item", CurrentState: "active", Fields: map[string]any{"account_id": "preserved"},
							}},
						})
						if err != nil {
							t.Fatalf("setup scenario state-only target: %v", err)
						}
					case "entity_tool":
						if err := store.CreateEntity(ctx, runtimetools.EntityCreateRecord{
							RunID: runID, EntityID: entityID, FlowInstance: instancePath,
							EntityType: "review_item", CurrentState: "active", FieldsJSON: json.RawMessage(`{"account_id":"preserved"}`),
							CreatedAt: createdAt, Writer: runtimetools.EntityMutationWriter{Type: "agent", ID: "producer-proof", HandlerStep: "create_entity"},
						}); err != nil {
							t.Fatalf("create entity-tool state-only target: %v", err)
						}
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, "", "active", 1, 0)
					record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)
					if _, err := store.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
						t.Fatalf("execute %s state-only target: %v", producer, err)
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, flowID, "done", 2, 1)
				})
			}
		})
	}
}

func stateOnlyWorkflowEngineMutationRecord(t *testing.T, runID, flowID, instancePath, entityID, expectedState string, expectedRevision int64, createdAt time.Time) runtimepipeline.WorkflowEngineStateRecord {
	t.Helper()
	route := runtimeflowidentity.StoredRoute(flowID, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
	config, err := json.Marshal(map[string]any{
		"flow_path": instancePath, "instance_id": route.InstanceID, "storage_ref": instancePath,
		"workflow_version": "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtimepipeline.WorkflowEngineStateRecord{
		Identity: runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: route}, EntityID: entityID,
		WorkflowName: flowID, WorkflowVersion: "1", Mode: "template", Status: "active",
		CurrentState: "done", EntityType: "review_item",
		Fields: json.RawMessage(`{"account_id":"preserved","handled":true}`), Bookkeeping: json.RawMessage(`{}`),
		Gates: json.RawMessage(`{}`), Accumulator: json.RawMessage(`{}`), Config: config, InitialFields: json.RawMessage(`{}`),
		EnteredStageAt: createdAt.Add(time.Minute), CreatedAt: createdAt, UpdatedAt: createdAt.Add(time.Minute),
		ExpectedState: expectedState, ExpectedRevision: expectedRevision,
		Transition: runtimepipeline.WorkflowEngineStateTransitionUpdateStateCreateCompanion,
	}
}

func seedWorkflowTargetStateForTransition(t *testing.T, backend string, db *sql.DB, runID, entityID, instancePath, state string, revision int64, at time.Time) {
	t.Helper()
	query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, 'review_item', ?, '{}', '{"account_id":"preserved"}', '{}', '{}', ?, ?, ?, ?)`
	args := []any{runID, entityID, instancePath, state, revision, at, at, at}
	if backend == "postgres" {
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'review_item', $4, '{}'::jsonb, '{"account_id":"preserved"}'::jsonb, '{}'::jsonb, '{}'::jsonb, $5, $6, $6, $6)`
		args = []any{runID, entityID, instancePath, state, revision, at}
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed workflow target state: %v", err)
	}
}

func assertWorkflowTargetTransitionRows(t *testing.T, backend string, db *sql.DB, runID, entityID, instancePath, wantWorkflow, wantState string, wantRevision, wantCompanions int) {
	t.Helper()
	stateQuery := `SELECT current_state, revision FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?`
	companionQuery := `SELECT COUNT(*), COALESCE(MAX(flow_template), '') FROM flow_instances WHERE run_id = ? AND instance_path = ?`
	if backend == "postgres" {
		stateQuery = `SELECT current_state, revision FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = $3`
		companionQuery = `SELECT COUNT(*), COALESCE(MAX(flow_template), '') FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2`
	}
	var state string
	var revision int
	if err := db.QueryRowContext(context.Background(), stateQuery, runID, entityID, instancePath).Scan(&state, &revision); err != nil {
		t.Fatalf("load workflow target state: %v", err)
	}
	if state != wantState || revision != wantRevision {
		t.Fatalf("workflow target state = %q/revision %d, want %q/%d", state, revision, wantState, wantRevision)
	}
	var companions int
	var workflow string
	if err := db.QueryRowContext(context.Background(), companionQuery, runID, instancePath).Scan(&companions, &workflow); err != nil {
		t.Fatalf("load workflow target companion: %v", err)
	}
	if companions != wantCompanions || workflow != wantWorkflow {
		t.Fatalf("workflow target companion = %d/%q, want %d/%q", companions, workflow, wantCompanions, wantWorkflow)
	}
}
