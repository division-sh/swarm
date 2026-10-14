package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCommittedAgentAttachmentRequiresExactAcceptedOwner(t *testing.T) {
	for _, fault := range []string{"accepted", "planned", "agents_registered", "ready", "retired", "aborted", "superseded", "terminal",
		"missing_row", "wrong_hash", "wrong_attempt", "missing_actor", "wrong_generation", "no_executor", "executor_completed",
		"executor_retiring", "successor", "pending_admission", "retiring_owner", "completed_owner", "stopped_generation", "canceled_generation", "missing_workflow_owner", "shutting_down", "resetting"} {
		t.Run(fault, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			instances := &flowActivationTestInstanceStore{}
			am := newFlowActivationManager(t, &flowActivationTestBus{}, instances)
			am.lifecycle.phase, am.lifecycle.runCtx = runtimeLifecycleRunning, context.Background()
			defer func() { am.lifecycle.phase, am.lifecycle.runCtx = runtimeLifecycleStopped, nil }()
			bundle := testFlowBundle(t, "")
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "one", "unused", "review/one")
			plan, err := am.PrepareFlowInstanceActivation(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if committed, err := am.roles.FlowActivation.CommitFlowInstanceActivation(ctx, plan); err != nil || !committed.Acknowledged {
				t.Fatalf("component constructor: %v", err)
			}
			row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || len(row.Plan.Agents) != 1 {
				t.Fatalf("planned census: %+v found=%t err=%v", row, found, err)
			}
			key := dynamicFlowRuntimeReadinessKey{runID: row.Plan.RunID, instancePath: row.InstancePath}
			active, _, err := am.beginDynamicFlowActiveAttempt(ctx, key, row.Plan, row.AttemptOrdinal, row.AttemptState)
			if err != nil {
				t.Fatal(err)
			}
			// These component projections own no installed resources. Native
			// retirement and executor joins are proved by the both-store cut.
			defer func() {
				am.dynamicFlowActiveAttempts = nil
				am.dynamicFlowReadinessAttempts = nil
			}()
			executor := &dynamicFlowRuntimeReadinessAttempt{attemptOrdinal: active.receipt.Ordinal(), planHash: active.planHash, done: make(chan struct{}), retiring: make(chan struct{})}
			am.dynamicFlowReadinessAttempts = map[dynamicFlowRuntimeReadinessKey]*dynamicFlowRuntimeReadinessAttempt{key: executor}
			row, _, err = instances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, req.Instance.Route())
			if err != nil {
				t.Fatal(err)
			}
			identity := row.Plan.Agents[0].Identity
			switch fault {
			case "planned", "retired", "aborted", "superseded":
				row.AttemptState = fault
			case "agents_registered":
				row.Phase = pipeline.FlowAttachmentAgentsRegistered
			case "ready":
				row.Phase = pipeline.FlowAttachmentReady
			case "terminal":
				row.InstanceStatus = "terminated"
			case "missing_row":
				delete(instances.readiness, flowActivationReadinessKey(key.runID, key.instancePath))
			case "wrong_hash":
				row.PlanHash = "contradictory"
			case "wrong_attempt":
				row.AttemptOrdinal++
			case "missing_actor":
				name, err := agentidentity.DeclaredName("unrelated", "swarm://review/unrelated")
				if err != nil {
					t.Fatal(err)
				}
				identity, err = agentidentity.New(identity.RunID, name, identity.Route)
				if err != nil {
					t.Fatal(err)
				}
			case "wrong_generation":
				old := am.lifecycle.persistence().(*flowActivationTestGenerationStore)
				next := *old
				next.grantID += "-foreign"
				am.lifecycle.replacePersistence(&next)
			case "no_executor":
				delete(am.dynamicFlowReadinessAttempts, key)
			case "executor_completed":
				close(executor.done)
			case "executor_retiring":
				close(executor.retiring)
			case "successor":
				executor.successorRequired = true
			case "pending_admission":
				active.pending = &pipeline.DynamicFlowRuntimeActivationRequest{}
			case "retiring_owner":
				active.retiring = true
			case "completed_owner":
				active.complete = true
			case "stopped_generation":
				am.lifecycle.phase = runtimeLifecycleStopped
			case "canceled_generation":
				canceled, cancel := context.WithCancel(context.Background())
				cancel()
				am.lifecycle.runCtx = canceled
			case "missing_workflow_owner":
				am.workflowInstances = nil
			case "shutting_down":
				am.lifecycle.phase = runtimeLifecycleShuttingDown
			case "resetting":
				am.lifecycle.phase = runtimeLifecycleResetting
			}
			if fault != "missing_row" {
				instances.readiness[flowActivationReadinessKey(key.runID, key.instancePath)] = row
			}
			before := make(map[string]pipeline.DynamicFlowRuntimeReadiness)
			for key, value := range instances.readiness {
				before[key] = value
			}
			if fault == "missing_workflow_owner" || fault == "stopped_generation" || fault == "no_executor" {
				if observed, planned, err := am.observeCommittedAgentAttachment(ctx, identity); err != nil || observed || planned {
					t.Fatalf("missing observation became positive census evidence: observed=%t planned=%t err=%v", observed, planned, err)
				}
			}
			if fault == "accepted" || fault == "missing_actor" || fault == "shutting_down" || fault == "resetting" {
				bounded, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				am.lifecycle.executionPublishMu.Lock()
				result := make(chan error, 1)
				go func() {
					result <- am.FinalizeCommittedAgentReadiness(bounded, req.TriggerEvent, []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}})
				}()
				select {
				case err = <-result:
					am.lifecycle.executionPublishMu.Unlock()
				case <-bounded.Done():
					am.lifecycle.executionPublishMu.Unlock()
					<-result
					t.Fatal("exact attachment observation waited for a candidate publication")
				}
			} else {
				err = am.FinalizeCommittedAgentReadiness(ctx, req.TriggerEvent, []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}})
			}
			if fault == "accepted" {
				if !errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
					t.Fatalf("accepted installing owner: %v", err)
				}
			} else if err == nil || errors.Is(err, runtimebus.ErrCommittedAgentRouteTransition) {
				t.Fatalf("%s borrowed attachment wake: %v", fault, err)
			}
			if fault == "missing_actor" && !errors.Is(err, ErrAgentNotFound) {
				t.Fatalf("unrelated agent did not refuse with exact lifecycle absence: %v", err)
			}
			if (fault == "shutting_down" || fault == "resetting") && !errors.Is(err, errRuntimeShuttingDown) {
				t.Fatalf("closed Manager did not return its lifecycle refusal: %v", err)
			}
			if !reflect.DeepEqual(before, instances.readiness) || len(am.ListAgentConfigs()) != 0 {
				t.Fatal("readiness observation constructed an actor or changed evidence")
			}
		})
	}
}
