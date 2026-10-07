package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type selectionCASPersistence struct {
	pipeline.WorkflowPersistenceOwner
	db        *sql.DB
	fired     bool
	selection handlerselection.HandlerRuleSelectionFact
	err       error
	losses    int
}

func (p *selectionCASPersistence) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	if command.DeliverySuccess != nil && p.losses < 9 &&
		(command.DeliverySuccess.RuleSelection.DisplayLabel() == "first" || command.DeliverySuccess.RuleSelection.DisplayLabel() == "second") {
		if !p.fired {
			p.selection = command.DeliverySuccess.RuleSelection
		}
		// Model a different writer after the real engine's read, not a forged
		// command revision or a synthetic CAS error returned by the test.
		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			return pipeline.CommittedWorkflowEngineMutation{}, err
		}
		defer tx.Rollback()
		result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET revision=revision+1 WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, command.State.Identity.RunID, command.State.EntityID, command.State.ExpectedRevision)
		if err != nil {
			return pipeline.CommittedWorkflowEngineMutation{}, err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return pipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("competing writer rows=%d: %v", n, err)
		}
		result, err = tx.ExecContext(ctx, `UPDATE entity_state SET revision=revision+1, fields='{"marker":"second"}' WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, command.State.Identity.RunID, command.State.EntityID, command.State.ExpectedRevision)
		if err != nil {
			return pipeline.CommittedWorkflowEngineMutation{}, err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return pipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("competing field writer rows=%d: %v", n, err)
		}
		if err := tx.Commit(); err != nil {
			return pipeline.CommittedWorkflowEngineMutation{}, err
		}
		p.fired = true
		p.losses++
	}
	result, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if err != nil {
		p.err = err
	}
	return result, err
}

func TestSelectionRetryAfterRealCASConflictBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopySelectionRetry(t), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			var nodes []pipeline.WorkflowNode
			for _, row := range []struct{ id, event string }{{"seed", "seed"}, {"select", "select"}, {"final", "selected"}} {
				nodes = append(nodes, pipeline.WorkflowNode{Node: externalPipelineSourceNode(t, source, ".", row.id), Subscriptions: []events.EventType{events.EventType(row.event)}, ExecutionType: contracts.SystemNodeExecutionType})
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			fault := &selectionCASPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), db: selected.db}
			selected.persistence = pipeline.NewWorkflowPersistence(fault)
			module := proposedEffectProofModule{source: source, nodes: nodes}
			initialCoordinator := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module})
			commitKeylessConstructorComponent(t, ctx, selected, initialCoordinator, source)
			bus.SetInterceptors(initialCoordinator)
			publish := func(name string) events.Event {
				t.Helper()
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(name), "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatal(err)
				}
				wait, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				if err := bus.WaitForQuiescence(wait); err != nil {
					t.Fatal(err)
				}
				return event
			}
			publish("seed")
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", runID, runID))
			if err != nil {
				t.Fatal(err)
			}
			initial, found, err := selected.persistence.LoadWorkflowInstance(ctx, owner)
			if err != nil || !found || initial.Fields["marker"] != "first" {
				t.Fatalf("seed state=%+v found=%v err=%v", initial, found, err)
			}
			event := publish("select")
			failure, typed := failures.EnvelopeFromError(fault.err)
			if !fault.fired || !typed || failure.Detail.Code != "workflow_engine_state_revision_conflict" || fault.selection.DisplayLabel() != "first" {
				t.Fatalf("did not reach exact selected CAS rejection: fired=%v selection=%#v err=%v diagnostic=%s", fault.fired, fault.selection, fault.err, proposedEffectProofFailure(t, selected, event.ID()))
			}
			evidence := storetest.ObserveDeliveryEventEvidence(t, ctx, selected.events, event.ID())
			if len(evidence.Deliveries) != 1 {
				t.Fatalf("exact selected delivery count=%d", len(evidence.Deliveries))
			}
			delivery := evidence.Deliveries[0]
			if delivery.Status != "delivered" || delivery.RetryCount != 0 || fault.losses != 9 {
				t.Fatalf("contention charged delivery budget: status=%s retries=%d losses=%d", delivery.Status, delivery.RetryCount, fault.losses)
			}
			if len(delivery.Attempts) != 1 {
				t.Fatalf("contention created attempts: %d", len(delivery.Attempts))
			}
			if delivery.HandlerSelections != 1 {
				t.Fatalf("final selection facts: %d", delivery.HandlerSelections)
			}
			if evidence.DeadLetters != 0 {
				t.Fatalf("state contention dead letter: %d", evidence.DeadLetters)
			}
			assertPersistedHandlerRuleSelection(t, selected, ctx, event.ID(), handlerselection.ContextRules, handlerselection.DispositionSelected, `nodes["select"].handlers["select"].rules[1]`, "second")
			assertTraceHandlerRuleSelection(t, selected, ctx, runID, event.ID(), handlerselection.ContextRules, handlerselection.DispositionSelected, `nodes["select"].handlers["select"].rules[1]`, "second")
			report, err := reader.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{EventLimit: 100})
			if err != nil {
				t.Fatal(err)
			}
			var payload []byte
			count = 0
			for _, row := range report.Events {
				if row.EventName == "ack" {
					payload = row.Payload
					count++
				}
			}
			if count != 1 {
				t.Fatalf("final effect events=%d, want 1", count)
			}
			var value map[string]any
			if err := json.Unmarshal(payload, &value); err != nil || value["marker"] != "second" {
				t.Fatalf("wrong final effect: %s %v", payload, err)
			}
			if err := bus.PublishAcknowledged(ctx, event); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := selected.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='ack'`, runID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("duplicate final effect: %d %v", count, err)
			}
		})
	}
}
