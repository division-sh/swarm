package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

// This matrix tests typed result consumption; native and served proofs cover commitment and dispatch.
func TestCommittedEngineTransfersDeclaredLifecyclePublications(t *testing.T) {
	for _, handler := range []bool{false, true} {
		for _, lifecycle := range []bool{false, true} {
			for _, activity := range []bool{false, true} {
				for _, cleanup := range []string{"success", "error", "panic", "cancellation"} {
					t.Run(fmt.Sprintf("handler=%t/lifecycle=%t/activity=%t/%s", handler, lifecycle, activity, cleanup), func(t *testing.T) {
						mutation, command := outcomeConstructedMutation(t)
						var emissions []runtimeengine.EmitIntent
						appendEmission := func(name string) {
							event := eventtest.RuntimeControl(eventtest.UUID(name), events.EventType(name), "platform", "", []byte(`{}`), 0,
								mutation.Address.FlowInstance.RunID, "", events.EventEnvelope{}, time.Now().UTC())
							emissions = append(emissions, runtimeengine.EmitIntent{Event: event})
						}
						if handler {
							appendEmission("fixture.handler_emitted")
							mutation.EmitIntents = append([]runtimeengine.EmitIntent(nil), emissions...)
						}
						if lifecycle {
							appendEmission("mailbox.card_superseded")
						}
						publications := append([]runtimeengine.EmitIntent(nil), emissions...)
						var requests []runtimeengine.EmitIntent
						if activity {
							mutation.ActivityIntents = []runtimeengine.ActivityIntent{testActivityIntent("https://example.com/handoff")}
							var err error
							requests, err = activityRequestEmitIntents(mutation.ActivityIntents)
							if err != nil {
								t.Fatal(err)
							}
							publications = append(publications, requests...)
						}
						storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}}
						for _, intent := range publications {
							command.Publications = append(command.Publications, pipelineTestPublicationPlan{intent: intent})
							storeOwner.result.Publications = append(storeOwner.result.Publications, pipelineTestCommittedPublication{eventID: intent.Event.ID(), intent: intent})
						}
						planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						var cleanupErr error
						switch cleanup {
						case "error":
							cleanupErr = errors.New("handoff cleanup failed")
							planner.finalizeErr = cleanupErr
						case "panic":
							planner.finalizePanic = true
						case "cancellation":
							cleanupErr = context.Canceled
							planner.finalizeErr = cleanupErr
							storeOwner.afterCommit = cancel
						}
						owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
						result, err := owner.commitPreparedEngineMutation(ctx, mutation, command, command.Publications, nil)
						if !result.Committed || planner.finalizes != 1 || planner.releases != 0 {
							t.Fatalf("lost committed ownership: result=%+v err=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
						}
						if cleanup == "success" && err != nil || cleanupErr != nil && !errors.Is(err, cleanupErr) || cleanup == "panic" && (err == nil || !strings.Contains(err.Error(), "publication cleanup panic")) {
							t.Fatalf("lost cleanup evidence: %v", err)
						}
						if len(result.EmitIntents) != len(emissions) {
							t.Fatalf("committed follow-up emitted %d intents, want all %d declared handler/lifecycle publications", len(result.EmitIntents), len(emissions))
						}
						for index, intent := range emissions {
							if result.EmitIntents[index].Event.ID() != intent.Event.ID() {
								t.Fatalf("emission %d lost exact declared identity", index)
							}
						}
						if len(result.ActivityRequestIntents) != len(requests) {
							t.Fatalf("activity requests = %d, want %d", len(result.ActivityRequestIntents), len(requests))
						}
						for index, request := range requests {
							if result.ActivityRequestIntents[index].Event.ID() != request.Event.ID() {
								t.Fatalf("activity request %d lost exact committed identity", index)
							}
						}
					})
				}
			}
		}
	}
}

func TestCommittedEngineRejectsChangedPublicationReceipts(t *testing.T) {
	for _, defect := range []string{"missing", "unexpected", "duplicate", "reordered", "nil", "wrong_intent", "duplicate_plan", "missing_activity"} {
		t.Run(defect, func(t *testing.T) {
			mutation, command := outcomeConstructedMutation(t)
			at := time.Now().UTC()
			first := runtimeengine.EmitIntent{Event: eventtest.RuntimeControl(eventtest.UUID("receipt-first"), "mailbox.card_superseded", "platform", "", []byte(`{}`), 0, mutation.Address.FlowInstance.RunID, "", events.EventEnvelope{}, at)}
			second := runtimeengine.EmitIntent{Event: eventtest.RuntimeControl(eventtest.UUID("receipt-second"), "fixture.handler_emitted", "platform", "", []byte(`{}`), 0, mutation.Address.FlowInstance.RunID, "", events.EventEnvelope{}, at)}
			command.Publications = []runtimeengine.DurablePublicationPlan{pipelineTestPublicationPlan{intent: first}, pipelineTestPublicationPlan{intent: second}}
			committed := []runtimeengine.CommittedDurablePublication{pipelineTestCommittedPublication{eventID: first.Event.ID(), intent: first}, pipelineTestCommittedPublication{eventID: second.Event.ID(), intent: second}}
			switch defect {
			case "missing":
				committed = committed[:1]
			case "unexpected":
				committed = append(committed, committed[0])
			case "duplicate":
				committed[1] = committed[0]
			case "reordered":
				committed[0], committed[1] = committed[1], committed[0]
			case "nil":
				committed[1] = nil
			case "wrong_intent":
				committed[1] = pipelineTestCommittedPublication{eventID: second.Event.ID(), intent: first}
			case "duplicate_plan":
				command.Publications[1], committed[1] = command.Publications[0], committed[0]
			case "missing_activity":
				mutation.ActivityIntents = []runtimeengine.ActivityIntent{testActivityIntent("https://example.com/missing")}
			}
			store := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Publications: committed, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}}
			planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
			owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: store}, publication: planner}
			result, err := owner.commitPreparedEngineMutation(context.Background(), mutation, command, command.Publications, nil)
			if !result.Committed || err == nil || len(result.EmitIntents) != 0 || len(result.ActivityRequestIntents) != 0 || planner.finalizes != 1 || planner.releases != 0 {
				t.Fatalf("invalid receipt authorized dispatch or lost acknowledged cleanup: result=%+v err=%v planner=%+v", result, err, planner)
			}
		})
	}
}
