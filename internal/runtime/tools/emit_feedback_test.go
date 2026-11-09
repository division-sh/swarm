package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

func captureEmitPublicationDiagnostic(t *testing.T) (*bytes.Buffer, func() map[string]any) {
	t.Helper()
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &output, func() map[string]any {
		t.Helper()
		for _, line := range strings.Split(output.String(), "\n") {
			if !strings.Contains(line, `"message":"Emit publication failure diagnostic"`) {
				continue
			}
			var result map[string]any
			if err := json.Unmarshal([]byte(line[strings.Index(line, "detail=")+len("detail="):]), &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
		t.Fatal("missing native emit publication diagnostic")
		return nil
	}
}

type emitDiagnosticStateError struct{ error }

func (emitDiagnosticStateError) SQLState() string { return "57014" }

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
	logs  []pipeline.RuntimeLogEntry
}

func (b *acknowledgedEmitFaultPublisher) LogRuntime(_ context.Context, entry pipeline.RuntimeLogEntry) error {
	b.logs = append(b.logs, entry)
	return nil
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
	captured, diagnostic := captureEmitPublicationDiagnostic(t)
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
	evidence := diagnostic()
	ack := evidence["ack"].(map[string]any)
	if evidence["event_id"] != publisher.event.ID() || evidence["run_id"] != toolTestRunID || evidence["agent_id"] != actor.ID ||
		ack["accepted"] != true || ack["feedback_committed"] != true || ack["replay"] != false || ack["stage"] != "component-fixture" || ack["dispatch"] != string(pipeline.EmitDispatchError) {
		t.Fatalf("diagnostic invented or lost native acknowledgement: %#v", evidence)
	}
	private := "credential-value /private/native-error https://private.invalid/secret"
	native := failures.New(failures.ClassInternalFailure, "unclassified_runtime_error", "native-publication", "feedback.commit", map[string]any{"password": private})
	publisher.fault = errors.Join(context.Canceled, fmt.Errorf("private wrapper %s: %w", private, native), emitDiagnosticStateError{errors.New(private)})
	captured.Reset()
	_, err = executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_done", map[string]any{})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, native) {
		t.Fatalf("capture changed independent publication causes: %v", err)
	}
	diagnostic()
	for _, required := range []string{`"sentinel":"context canceled"`, `"component":"native-publication"`, `"operation":"feedback.commit"`, `"sqlstate":"57014"`, `"opaque":true`, `"message_sha256":`} {
		if !strings.Contains(captured.String(), required) {
			t.Fatalf("native branch evidence lost %s: %s", required, captured.String())
		}
	}
	if strings.Contains(captured.String(), private) || strings.Contains(captured.String(), "password") {
		t.Fatal("native diagnostic exposed private text or failure attributes")
	}
	publisher.fault = fault
	for i := 0; i < 12; i++ {
		publisher.fault = fmt.Errorf("opaque wrapper: %w", publisher.fault)
	}
	captured.Reset()
	_, _ = executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_done", map[string]any{})
	diagnostic()
	if !strings.Contains(captured.String(), `"truncated":true`) || len(captured.Bytes()) > 8192 {
		t.Fatalf("native diagnostic did not disclose its bound: %d bytes", len(captured.Bytes()))
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
	publisher.fault = errors.Join(first, second, first)
	publisher.logs = nil
	value, err = executor.handleEmitTool(toolEventTestContext(actor), actor, "emit_root_done", map[string]any{})
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("joined receiver refusals were collapsed: %v", err)
	}
	output = value.(map[string]any)
	refusals, ok := output["refusals"].([]failures.Envelope)
	if !ok || len(refusals) != 2 || len(publisher.logs) != 2 || output["accepted"] != true || output["event_id"] != publisher.event.ID() {
		t.Fatalf("joined receivers lost displayed or logged outcomes: output=%#v logs=%+v", output, publisher.logs)
	}
	for i, receiver := range []*pipeline.NoHandlerError{first, second} {
		for _, refusal := range []*failures.Envelope{&refusals[i], publisher.logs[i].Failure} {
			if refusal == nil || refusal.Class != failures.ClassUnexpectedArrival || refusal.Detail.Code != "no_route" ||
				refusal.Detail.Attributes["flow_instance"] != receiver.FlowInstance || refusal.Detail.Attributes["stage"] != receiver.Stage {
				t.Fatalf("receiver %q lost its exact envelope: %+v", receiver.FlowInstance, refusal)
			}
		}
		detail, ok := publisher.logs[i].Detail.(map[string]any)
		if !ok || publisher.logs[i].Action != emitToolOutcomeAction || detail["emitted_event_id"] != publisher.event.ID() {
			t.Fatalf("receiver outcome lost its emitted event: %+v", publisher.logs[i])
		}
	}
}
