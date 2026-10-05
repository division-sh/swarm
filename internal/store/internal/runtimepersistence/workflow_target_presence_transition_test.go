package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestWorkflowEngineConstructedTransitionAtomicOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"exact", "stale_revision", "invalid_transition", "foreign_header_owner", "contenders", "paired_reload"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f, plan := newWorkflowTargetConstructionFixture(t, backend)
				if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
					t.Fatal(err)
				}
				persisted, err := plan.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				record := persisted.State
				record.ExpectedRevision, record.ExpectedState = 1, record.CurrentState
				record.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
				record.CurrentState = "done"
				record.Fields = json.RawMessage(`{"account_id":"preserved","handled":true}`)
				record.EnteredStageAt, record.UpdatedAt = record.CreatedAt.Add(time.Minute), record.CreatedAt.Add(time.Minute)
				record = workflowTargetMutationEntry(t, f, record, "finish.requested")
				owner := f.store.(runtimepipeline.WorkflowEngineMutationOwner)
				historyCount := func(step string) int {
					var count int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND writer_id='workflow_engine' AND handler_step=$3`, record.Identity.RunID, record.EntityID, step).Scan(&count); err != nil {
						t.Fatal(err)
					}
					return count
				}
				constructionHistory := historyCount("create")
				if constructionHistory == 0 || historyCount("mutate") != 0 {
					t.Fatal("canonical constructor did not record exactly initial history")
				}
				if change == "foreign_header_owner" {
					if _, err := f.db.ExecContext(f.ctx, `UPDATE flow_instances SET flow_template='foreign' WHERE run_id=$1 AND instance_path=$2`, record.Identity.RunID, record.Identity.Route.InstancePath); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				switch change {
				case "stale_revision":
					record.ExpectedRevision++
				case "invalid_transition":
					record.Transition = runtimepipeline.WorkflowEngineStateTransition(255)
				}
				if change == "contenders" {
					start := make(chan struct{})
					results := make(chan error, 2)
					var contenders sync.WaitGroup
					for range 2 {
						contenders.Add(1)
						go func() {
							defer contenders.Done()
							<-start
							_, err := owner.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
							results <- err
						}()
					}
					close(start)
					contenders.Wait()
					close(results)
					successes := 0
					for err := range results {
						if err == nil {
							successes++
						} else if failure, typed := runtimefailures.As(err); !typed || failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" || !failure.Failure.Retryable {
							t.Fatalf("contending constructed mutation error: %v", err)
						}
					}
					if successes != 1 {
						t.Fatalf("constructed mutation winners = %d, want 1", successes)
					}
				} else {
					_, err := owner.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
					if change == "stale_revision" || change == "foreign_header_owner" || change == "invalid_transition" {
						if err == nil {
							t.Fatal("invalid constructed mutation committed")
						}
						if change == "stale_revision" {
							failure, typed := runtimefailures.As(err)
							if !typed || failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" || !failure.Failure.Retryable {
								t.Fatalf("stale constructed mutation error: %v", err)
							}
						}
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
							t.Fatal("refused mutation changed persisted construction or fields")
						}
						if historyCount("mutate") != 0 {
							t.Fatal("refused ordinary mutation recorded new history")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				assertWorkflowTargetTransitionRows(t, backend, f.db, record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath, "review", "done", 2, 1)
				if historyCount("create") != constructionHistory || historyCount("mutate") == 0 {
					t.Fatal("ordinary mutation repeated construction or lost mutation history")
				}
				if change == "paired_reload" {
					reader := f.store.(runtimepipeline.WorkflowTargetPersistenceReader)
					target, err := reader.LoadWorkflowTargetPersistence(f.ctx, record.Identity, runtimeidentity.NormalizeEntityID(record.EntityID))
					if err != nil {
						t.Fatal(err)
					}
					if err := target.Validate(record.Identity.Route, runtimeidentity.NormalizeEntityID(record.EntityID)); err != nil {
						t.Fatal(err)
					}
					transition, err := runtimepipeline.WorkflowEngineStateTransitionForPresence(target.Presence)
					if err != nil || transition != runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion {
						t.Fatalf("constructed reload lost paired transition: %d %v", transition, err)
					}
					record.ExpectedState, record.ExpectedRevision = target.State.CurrentState, target.State.Revision
					record.CurrentState = "settled"
					record.EnteredStageAt, record.UpdatedAt = record.UpdatedAt.Add(time.Minute), record.UpdatedAt.Add(time.Minute)
					record = workflowTargetMutationEntry(t, f, record, "settle.requested")
					if _, err := owner.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
						t.Fatal(err)
					}
					assertWorkflowTargetTransitionRows(t, backend, f.db, record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath, "review", "settled", 3, 1)
					if historyCount("create") != constructionHistory {
						t.Fatal("paired reload repeated construction")
					}
				}
			})
		}
	}
}

func newWorkflowTargetConstructionFixture(t *testing.T, backend string) (receiverConfigActivationFixture, runtimepipeline.FlowInstanceActivationPlan) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":          "name: constructed-target-transition\n",
		"review/schema.yaml":   "name: review\nstages:\n  active: {initial: true}\n  done: {}\n  settled: {}\n",
		"review/entities.yaml": "review_item:\n  account_id: {type: text, initial: preserved}\n  handled: {type: boolean, initial: false}\n",
		"review/events.yaml":   "finish.requested:\nsettle.requested:\n",
		"review/nodes.yaml": `progress:
  execution_type: system_node
  subscribes_to: [finish.requested, settle.requested]
  event_handlers:
    finish.requested:
      advances_to: done
    settle.requested:
      advances_to: settled
`,
	}, nil)
	f.ctx = runtimecorrelation.WithSourceArtifactFact(f.ctx, sourceartifactfixture.FactFor(f.bundle.SourceArtifact))
	runID := runtimecorrelation.RunIDFromContext(f.ctx)
	root := runtimeflowidentity.Stored(semanticview.Wrap(f.bundle), ".", runID, runID, runID, "")
	child, err := runtimeflowidentity.KeylessChild(semanticview.Wrap(f.bundle), root, "review")
	if err != nil {
		t.Fatal(err)
	}
	req := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
	req.Instance = child
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	return f, plan
}

func workflowTargetMutationEntry(t *testing.T, f receiverConfigActivationFixture, record runtimepipeline.WorkflowEngineStateRecord, localEvent string) runtimepipeline.WorkflowEngineStateRecord {
	t.Helper()
	// This is the native writer/contender proof, not handler execution: retain
	// stage-entry authority from an actual declared and claimed occurrence.
	node := mustPersistenceNode("review", "progress")
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("review/"+localEvent), "fixture", "", []byte(`{}`), 0, record.Identity.RunID, events.EventEnvelope{}, record.UpdatedAt)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID})}
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, f.store, event, route)
	if err != nil {
		t.Fatal(err)
	}
	graph, found := f.bundle.WorkflowStageTopology("review")
	if !found {
		t.Fatal("transition fixture lost its declared lifecycle")
	}
	compiled, err := graph.AdmitTransition(runtimecontracts.WorkflowTransitionSite{Node: node, HandlerEvent: localEvent, AdvanceCarrier: runtimecontracts.HandlerAdvanceCarrierHandler}, record.ExpectedState, record.CurrentState)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := workflowlifecycle.NewCompiledTransition(compiled, handlerselection.NotApplicable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := workflowlifecycle.NewAcceptedEvent(record.Identity.Route, runtimeidentity.NormalizeEntityID(record.EntityID), event.ID(), string(event.Type()), event.ExecutionMode(), event.CreatedAt(), &transition)
	if err != nil {
		t.Fatal(err)
	}
	effect, err = effect.WithExecutionOccurrence("delivery", claimed.Claim.DeliveryID())
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := effect.StageEntry(record.Identity)
	if err != nil || !found {
		t.Fatalf("prepare claimed transition entry: found=%t err=%v", found, err)
	}
	var bookkeeping map[string]any
	if err := json.Unmarshal(record.Bookkeeping, &bookkeeping); err != nil {
		t.Fatal(err)
	}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	record.Bookkeeping, err = json.Marshal(bookkeeping)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestWorkflowTargetPersistenceReadNeverFabricatesMixedSnapshotOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for attempt := range 12 {
				f, plan := newWorkflowTargetConstructionFixture(t, backend)
				persisted, err := plan.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				record, ctx := persisted.State, f.ctx
				reader := f.store.(runtimepipeline.WorkflowTargetPersistenceReader)
				routeEntityID := runtimeidentity.NormalizeEntityID(record.EntityID)

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
				if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan); err != nil {
					t.Fatalf("attempt %d commit atomic target pair: %v", attempt, err)
				}
				readers.Wait()
				close(errors)
				for err := range errors {
					t.Fatalf("attempt %d target snapshot read: %v", attempt, err)
				}
				target, err := reader.LoadWorkflowTargetPersistence(ctx, record.Identity, routeEntityID)
				if err != nil || target.Presence != runtimepipeline.WorkflowTargetPersistenceComplete {
					t.Fatalf("attempt %d final target presence = %d error = %v, want complete", attempt, target.Presence, err)
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
		{name: "absent", presence: runtimepipeline.WorkflowTargetPersistenceAbsent, wantError: "requires a constructed header"},
		{name: "state only", presence: runtimepipeline.WorkflowTargetPersistenceStateOnly, wantError: "cannot repair imported state without construction"},
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

func TestSupportedStateOnlyProducersCannotAcquireWorkflowConstructionOnBothStores(t *testing.T) {
	type producerStore interface {
		runtimepipeline.WorkflowEngineMutationOwner
		runtimerunlifecycle.OperationOwner
		sourceartifactfixture.Writer
		SetupScenarioEntities(context.Context, runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, producer := range []string{"scenario_setup"} {
				t.Run(producer, func(t *testing.T) {
					selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
					store, ok := selected.(producerStore)
					if !ok {
						t.Fatalf("%s selected store does not expose supported state-only producers", backend)
					}
					flowID := producer + "-" + uuid.NewString()
					instancePath := flowID + "/receiver"
					source := stateOnlyAcquisitionSourceWithMode(t, flowID, runtimecontracts.FlowModeTemplate)
					bundle, ok := semanticview.Bundle(source)
					if !ok || bundle.SourceArtifact == nil {
						t.Fatal("state-only producer source artifact is required")
					}
					sourceartifactfixture.RequireArtifact(t, ctx, store, bundle.SourceArtifact)
					if _, err := store.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{
						RunID: runID, Source: sourceartifactfixture.FactFor(bundle.SourceArtifact),
					}); err != nil {
						t.Fatalf("bind state-only producer source: %v", err)
					}
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(bundle.SourceArtifact))
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
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, "", "active", 1, 0)
					before := snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")
					record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instancePath, entityID, "active", 1, createdAt)
					if _, err := store.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err == nil || !strings.Contains(err.Error(), "constructed target") {
						t.Fatalf("ordinary execution admitted %s state-only target: %v", producer, err)
					}
					// The remaining ordinary-update variant must also refuse the
					// missing header, not manufacture construction from imported fields.
					record.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					if _, err := store.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record}); err == nil {
						t.Fatalf("ordinary update repaired %s state-only target", producer)
					} else if !strings.Contains(err.Error(), "workflow engine state route is missing: "+instancePath) {
						t.Fatalf("missing-header update lacked exact route refusal: %v", err)
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instancePath, "", "active", 1, 0)
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, backend == "postgres")) {
						t.Fatal("refused imported execution changed persisted state")
					}
				})
			}
		})
	}
}

func stateOnlyWorkflowEngineMutationRecord(t *testing.T, runID, flowID, instancePath, entityID, expectedState string, expectedRevision int64, createdAt time.Time) runtimepipeline.WorkflowEngineStateRecord {
	t.Helper()
	route := runtimeflowidentity.StoredRoute(flowID, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
	payload, err := runtimepipeline.WorkflowInstanceConfigPayloadForRoute(route, "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(payload)
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
		// The deleted companion-repair variant has no replacement authority.
		Transition: runtimepipeline.WorkflowEngineStateTransition(3),
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

func assertNoWorkflowEngineHistory(t *testing.T, backend string, db *sql.DB, runID, entityID string) {
	t.Helper()
	query := `SELECT COUNT(*) FROM entity_mutations WHERE run_id = ? AND entity_id = ? AND writer_id = 'workflow_engine'`
	if backend == "postgres" {
		query = `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid AND writer_id = 'workflow_engine'`
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, runID, entityID).Scan(&count); err != nil {
		t.Fatalf("load workflow engine history: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed workflow engine mutation wrote %d history rows", count)
	}
}
