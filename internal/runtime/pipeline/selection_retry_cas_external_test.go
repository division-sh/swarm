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
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
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
}

func (p *selectionCASPersistence) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	if command.DeliverySuccess != nil && command.DeliverySuccess.RuleSelection.DisplayLabel() == "first" && !p.fired {
		p.selection = command.DeliverySuccess.RuleSelection
		// Model a different writer after the real engine's read, not a forged
		// command revision or a synthetic CAS error returned by the test.
		result, err := p.db.ExecContext(ctx, `UPDATE entity_state SET revision=revision+1, fields='{"marker":"second"}' WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, command.State.Identity.RunID, command.State.EntityID, command.State.ExpectedRevision)
		if err != nil {
			return pipeline.CommittedWorkflowEngineMutation{}, err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return pipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("competing writer rows=%d: %v", n, err)
		}
		p.fired = true
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
				nodes = append(nodes, pipeline.WorkflowNode{Node: externalPipelineSourceNode(t, source, ".", row.id), Subscriptions: []events.EventType{events.EventType(row.event)}, ExecutionType: contracts.SystemNodeExecutionType, Policies: map[string]pipeline.WorkflowEventPolicy{row.event: {Consume: true}}})
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			fault := &selectionCASPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), db: selected.db}
			selected.persistence = pipeline.NewWorkflowPersistence(fault)
			module := proposedEffectProofModule{source: source, nodes: nodes}
			bus.SetInterceptors(newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module}))
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
			var initialFields []byte
			if err := selected.db.QueryRow(`SELECT fields FROM entity_state WHERE run_id=$1`, runID).Scan(&initialFields); err != nil {
				t.Fatal(err)
			}
			var initial map[string]any
			if err := json.Unmarshal(initialFields, &initial); err != nil || initial["marker"] != "first" {
				t.Fatalf("seed state=%s err=%v", initialFields, err)
			}
			event := publish("select")
			failure, typed := failures.EnvelopeFromError(fault.err)
			if !fault.fired || !typed || failure.Detail.Code != "workflow_engine_state_revision_conflict" || fault.selection.DisplayLabel() != "first" {
				t.Fatalf("did not reach exact selected CAS rejection: fired=%v selection=%#v err=%v diagnostic=%s", fault.fired, fault.selection, fault.err, proposedEffectProofFailure(t, selected, event.ID()))
			}
			var id, status string
			if err := selected.db.QueryRow(`SELECT delivery_id,status FROM event_deliveries WHERE event_id=$1`, event.ID()).Scan(&id, &status); err != nil || status != "failed" {
				t.Fatalf("CAS conflict lost retry: %s %v", status, err)
			}
			var count int
			if err := selected.db.QueryRow(`SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, id).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed CAS froze fact: %d %v", count, err)
			}
			if err := selected.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name IN ('selected','ack')`, runID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed CAS leaked effects: %d %v", count, err)
			}
			rows, _, err := selected.trace.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			seen := false
			for _, row := range rows {
				if row.EventID == event.ID() {
					seen = true
					if row.HandlerRuleSelection != nil {
						t.Fatal("retry trace fabricated final selection")
					}
				}
			}
			if !seen {
				t.Fatal("trace omitted retry")
			}
			// Reconstruct the coordinator, then reclaim the same durable route.
			// SQL accelerates only the finite eligibility delay in this component proof.
			if _, err := selected.db.Exec(`UPDATE event_deliveries SET next_eligible_at=$1 WHERE delivery_id=$2`, time.Now().UTC().Add(-time.Second), id); err != nil {
				t.Fatal(err)
			}
			coordinator := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module})
			bus.SetInterceptors(coordinator)
			prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, event.ID())
			if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
				t.Fatalf("recovered route: %#v %v", prepared, err)
			}
			if err := bus.ReleaseDeliveryContinuation(id); err != nil {
				t.Fatal(err)
			}
			proof, err := selected.events.ProveHandoff(ctx, event.ID(), prepared.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := bus.AcceptCommittedDeliveryHandoffs([]deliverylifecycle.DurableHandoffProof{proof}); err != nil {
				t.Fatal(err)
			}
			if result := bus.DispatchDeliveryContinuation(ctx, prepared.Event.Event(), prepared.DeliveryRoutes[0]); result.Failure() != nil {
				t.Fatal(result.Failure())
			}
			wait, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := bus.WaitForQuiescence(wait); err != nil {
				t.Fatal(err)
			}
			assertPersistedHandlerRuleSelection(t, selected, ctx, event.ID(), handlerselection.ContextRules, handlerselection.DispositionSelected, `nodes["select"].handlers["select"].rules[1]`, "second")
			assertTraceHandlerRuleSelection(t, selected, ctx, runID, event.ID(), handlerselection.ContextRules, handlerselection.DispositionSelected, `nodes["select"].handlers["select"].rules[1]`, "second")
			var payload []byte
			if err := selected.db.QueryRow(`SELECT payload FROM events WHERE run_id=$1 AND event_name='ack'`, runID).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			var value map[string]any
			if err := json.Unmarshal(payload, &value); err != nil || value["marker"] != "second" {
				t.Fatalf("wrong final effect: %s %v", payload, err)
			}
			if err := bus.PublishAcknowledged(ctx, event); err != nil {
				t.Fatal(err)
			}
			if err := selected.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='ack'`, runID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("duplicate final effect: %d %v", count, err)
			}
		})
	}
}
