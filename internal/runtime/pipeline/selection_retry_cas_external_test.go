package pipeline_test

import (
	"context"
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
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type selectionCASPersistence struct {
	pipeline.WorkflowPersistenceOwner
	fired     bool
	selection handlerselection.HandlerRuleSelectionFact
	err       error
	losses    int
	rules     [2]identity.DeclarationIdentity
}

func (p *selectionCASPersistence) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	if command.DeliverySuccess != nil && p.losses < 9 &&
		(command.DeliverySuccess.RuleSelection.Ref().Equal(p.rules[0]) || command.DeliverySuccess.RuleSelection.Ref().Equal(p.rules[1])) {
		if !p.fired {
			p.selection = command.DeliverySuccess.RuleSelection
		}
		// Commit the competing field write through the same atomic header/data
		// owner, without publishing or settling the original delivery.
		before, found, err := p.WorkflowPersistenceOwner.LoadWorkflowInstance(ctx, command.State.Identity)
		if err != nil || !found || before.Revision != command.State.ExpectedRevision {
			return pipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("load competing writer predecessor: found=%v revision=%d: %w", found, before.Revision, err)
		}
		state := command.State
		state.CurrentState = before.CurrentState
		state.ExpectedState = before.CurrentState
		state.EnteredStageAt = before.EnteredStageAt
		state.Fields = json.RawMessage(`{"marker":"second"}`)
		state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
		for _, projection := range []struct {
			value any
			dest  *json.RawMessage
		}{{before.Bookkeeping, &state.Bookkeeping}, {before.Gates, &state.Gates}, {before.StateBuckets, &state.Accumulator}} {
			*projection.dest, err = json.Marshal(projection.value)
			if err != nil {
				return pipeline.CommittedWorkflowEngineMutation{}, err
			}
		}
		if _, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
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
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopySelectionRetry(t), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			ctx, sourceFact := workflowLifecycleSourceContext(t, selected, bundle, runID)
			source := semanticview.Wrap(bundle)
			var nodes []pipeline.WorkflowNode
			for _, row := range []struct{ id, event string }{{"seed", "seed"}, {"select", "select"}, {"final", "selected"}} {
				nodes = append(nodes, pipeline.WorkflowNode{Node: externalPipelineSourceNode(t, source, ".", row.id), Subscriptions: []events.EventType{events.EventType(row.event)}, ExecutionType: contracts.SystemNodeExecutionType})
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, SourceArtifactFact: sourceFact})
			if err != nil {
				t.Fatal(err)
			}
			fault := &selectionCASPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner)}
			handler, found := source.ExecutableNodeEventHandler(externalPipelineSourceNode(t, source, ".", "select"), "select")
			if !found || len(handler.Rules) != 2 || handler.Rules[0].Condition != "entity.marker == 'first'" || handler.Rules[1].Condition != "entity.marker == 'second'" || handler.Rules[0].PolicyRow.Kind != contracts.PolicySheetRowKindWhen || handler.Rules[1].PolicyRow.Kind != contracts.PolicySheetRowKindWhen {
				t.Fatal("CAS proof requires both exact admitted selection predicates without an empty default")
			}
			for index := range fault.rules {
				ref, admitted := handler.Rules[index].DeclarationIdentity()
				if !admitted {
					t.Fatal("CAS proof lacks exact admitted selection identity")
				}
				fault.rules[index] = ref
			}
			selected.persistence = pipeline.NewWorkflowPersistence(fault)
			module := proposedEffectProofModule{source: source, nodes: nodes}
			initialCoordinator := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, SourceArtifactFact: sourceFact})
			commitKeylessConstructorComponent(t, ctx, selected, initialCoordinator, source)
			bus.SetInterceptors(initialCoordinator)
			publish := func(name string) events.Event {
				t.Helper()
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(name), "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				t.Logf("Q10-CAS-PHASE publish=%s event=%s started=%s", name, event.ID(), time.Now().UTC().Format(time.RFC3339Nano))
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatal(err)
				}
				t.Logf("Q10-CAS-PHASE publish=%s event=%s acknowledged=%s", name, event.ID(), time.Now().UTC().Format(time.RFC3339Nano))
				wait, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				deadline, _ := wait.Deadline()
				t.Logf("Q10-CAS-PHASE publish=%s event=%s wait_started=%s deadline=%s", name, event.ID(), time.Now().UTC().Format(time.RFC3339Nano), deadline.UTC().Format(time.RFC3339Nano))
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
			if !fault.fired || !typed || failure.Detail.Code != "workflow_engine_state_revision_conflict" || !fault.selection.Ref().Equal(fault.rules[0]) || fault.selection.DisplayLabel() != "rules[0]: when entity.marker == 'first'" {
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
			assertPersistedHandlerRuleSelection(t, selected, ctx, event.ID(), handlerselection.ContextRules, handlerselection.DispositionSelected, `nodes["select"].handlers["select"].rules[1]`, "rules[1]: when entity.marker == 'second'")
			assertTraceHandlerRuleSelection(t, selected, ctx, runID, event.ID(), handlerselection.ContextRules, handlerselection.DispositionSelected, `nodes["select"].handlers["select"].rules[1]`, "rules[1]: when entity.marker == 'second'")
			reader := selected.events.(interface {
				LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
			})
			report, err := reader.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{EventLimit: 100})
			if err != nil {
				t.Fatal(err)
			}
			var payload []byte
			count := 0
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
			report, err = reader.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			count = 0
			for _, row := range report.EventCounts {
				if row.EventName == "ack" {
					count += row.Count
				}
			}
			if count != 1 {
				t.Fatalf("duplicate final effect: %d", count)
			}
		})
	}
}
