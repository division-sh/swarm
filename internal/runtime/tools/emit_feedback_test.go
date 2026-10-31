package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// Controlled publisher evidence for tool component tests, not persistence or
// handler-completion qualification.
func (b *publishBusCapture) PublishEmit(ctx context.Context, event events.Event, request pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	if err := request.ValidateEvent(event); err != nil {
		return pipeline.WorkflowEmitResult{}, err
	}
	if err := b.Publish(ctx, event); err != nil {
		return pipeline.WorkflowEmitResult{}, err
	}
	return componentEmitFeedback(event, request)
}

func componentEmitFeedback(event events.Event, request pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	entityID := request.EntityID
	if entityID == "" {
		entityID = "component-constructed-entity"
	}
	receipt, err := pipelineobligation.StageReceiptEvidence(event.ID(), engine.CommittedStage{
		Instance: request.Instance, EntityID: entityID, Stage: "component-fixture", StageDefined: true, Revision: 1, UpdatedAt: event.CreatedAt(),
	})
	return pipeline.WorkflowEmitResult{Accepted: true, FeedbackCommitted: err == nil,
		Feedback: pipeline.WorkflowEmitFeedback{Receipt: receipt, StageOrigin: pipeline.EmitStageAcceptance, Dispatch: pipeline.EmitDispatchQueued}}, err
}

func (s *emitRoutePlanStore) ReadWorkflowPublicationStages(_ context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowPublicationStageEvidence, bool, error) {
	evidence, found := s.stages[eventID]
	if found && evidence.Acceptance.Stage().Instance != instance {
		return pipeline.WorkflowPublicationStageEvidence{}, false, fmt.Errorf("component stage belongs to another instance")
	}
	return evidence, found, nil
}

func (s *emitRoutePlanStore) ReadWorkflowEmitFeedback(_ context.Context, eventID string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowEmitFeedback, bool, error) {
	feedback, found := s.feedback[eventID]
	if found && feedback.Receipt.Stage().Instance != instance {
		return pipeline.WorkflowEmitFeedback{}, false, fmt.Errorf("component feedback belongs to another instance")
	}
	return feedback, found, nil
}

func (s *emitRoutePlanStore) CommitWorkflowEmitFeedback(_ context.Context, candidate pipeline.WorkflowEmitFeedback) (pipeline.WorkflowEmitFeedbackCommit, error) {
	evidence, found := s.stages[candidate.Receipt.EventID()]
	if err := candidate.Validate(); err != nil || !found || evidence.Acceptance != candidate.Receipt || candidate.StageOrigin != pipeline.EmitStageAcceptance {
		return pipeline.WorkflowEmitFeedbackCommit{}, fmt.Errorf("component feedback lacks publication evidence: %v", err)
	}
	if existing, found := s.feedback[candidate.Receipt.EventID()]; found {
		return pipeline.WorkflowEmitFeedbackCommit{Acknowledged: true, Replay: true, Feedback: existing}, nil
	}
	s.feedback[candidate.Receipt.EventID()] = candidate
	return pipeline.WorkflowEmitFeedbackCommit{Acknowledged: true, Feedback: candidate}, nil
}

func (b *telemetryBusStub) PublishEmit(ctx context.Context, event events.Event, request pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	if err := request.ValidateEvent(event); err != nil {
		return pipeline.WorkflowEmitResult{}, err
	}
	if err := b.Publish(ctx, event); err != nil {
		return pipeline.WorkflowEmitResult{}, err
	}
	return componentEmitFeedback(event, request)
}

type acknowledgedEmitFaultPublisher struct {
	publishBusCapture
	fault error
}

func (b *acknowledgedEmitFaultPublisher) PublishEmit(ctx context.Context, event events.Event, request pipeline.WorkflowPublicationStageRequest) (pipeline.WorkflowEmitResult, error) {
	result, err := b.publishBusCapture.PublishEmit(ctx, event, request)
	if err != nil {
		return result, err
	}
	result.Feedback.Dispatch = pipeline.EmitDispatchError
	return result, b.fault
}

func TestHandleEmitToolRetainsAcknowledgedStageOnDispatchError(t *testing.T) {
	bundle := &contracts.WorkflowContractBundle{Events: map[string]contracts.EventCatalogEntry{"root.done": {}}}
	source := toolTestSourceWithDeclaredAgent(t, bundle, "root-agent", ".", "root.done")
	fault := errors.New("post-commit dispatch failed")
	publisher := &acknowledgedEmitFaultPublisher{fault: fault}
	executor := NewExecutorWithOptions(publisher, ExecutorOptions{WorkflowSource: source, EmitRegistry: NewEmitRegistry(source, nil)})
	actor := actors.AgentConfig{ExecutionMode: "live", ID: "root-agent", Identity: toolTestRootAgentIdentity(t, "root-agent"), FlowID: ".", EntityID: eventtest.UUID("emit-feedback-source"), EmitEvents: []string{"root.done"}}
	value, err := executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_done", map[string]any{})
	if !errors.Is(err, fault) || publisher.count != 1 {
		t.Fatalf("post-commit failure was lost or repeated: count=%d err=%v", publisher.count, err)
	}
	output, ok := value.(map[string]any)
	if !ok || output["accepted"] != true || output["feedback_committed"] != true || output["retry_write"] != false || output["stage"] != "component-fixture" || output["stage_origin"] != pipeline.EmitStageAcceptance || output["dispatch"] != pipeline.EmitDispatchError || output["event_id"] != publisher.event.ID() {
		t.Fatalf("tool discarded exact committed feedback: %#v", value)
	}
	for _, joined := range []bool{false, true} {
		refusal := &pipeline.NoHandlerError{FlowID: ".", FlowInstance: toolTestRunID, Stage: "done"}
		publisher.fault = refusal
		if joined {
			publisher.fault = errors.Join(refusal, fault)
		}
		value, err := executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_done", map[string]any{})
		typed := failures.FromError(err, "test", "emit")
		if joined {
			if !errors.Is(err, fault) || typed.Failure.Class != failures.ClassDependencyUnavailable {
				t.Fatalf("semantic refusal hid an independent failure: %v", err)
			}
			continue
		}
		output := value.(map[string]any)
		if typed.Failure.Class != failures.ClassUnexpectedArrival || failures.Matches(failures.SelectorAnyTaskFailure, typed.Failure.Class) || typed.Failure.Detail.Code != "no_route" ||
			output["status"] != "refused" || output["accepted"] != true || output["stage"] != "component-fixture" ||
			!strings.Contains(typed.Failure.Message, "done") || !strings.Contains(typed.Failure.Message, "handled events") {
			t.Fatalf("late emit lost its ordinary refusal or committed result: %#v, %v", value, err)
		}
	}
	first := &pipeline.NoHandlerError{FlowID: ".", FlowInstance: toolTestRunID, Stage: "done"}
	second := &pipeline.NoHandlerError{FlowID: "sibling", FlowInstance: "sibling/one", Stage: "closed"}
	publisher.fault = errors.Join(first, second)
	_, err = executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_done", map[string]any{})
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("joined receiver refusals were collapsed: %v", err)
	}
}
