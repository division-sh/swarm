package apiv1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type selectedControlRefusalFixture struct {
	runID     string
	bindingID string
	operation runfork.SelectedControl
	calls     int
}

func (s *selectedControlRefusalFixture) RequireNormalControl(_ context.Context, runID string, operation runfork.SelectedControl) error {
	s.runID, s.operation = runID, operation
	s.calls++
	return &runfork.SelectedForkControlUnsupported{RunID: runID, BindingID: s.bindingID, Operation: operation}
}

func TestSelectedForkPublicControlRefusesBeforeLoadedAcquisitionOrMutation(t *testing.T) {
	runID := uuid.NewString()
	for _, method := range []string{"run.pause", "run.continue", "agent.restart", "agent.send_directive"} {
		t.Run(method, func(t *testing.T) {
			owner := &selectedControlRefusalFixture{bindingID: uuid.NewString()}
			req := Request{Method: method, Params: map[string]any{"run_id": runID, "agent_id": "same-name", "directive": "must not execute"}}
			var err error
			// No mutation/controller dependencies are installed: reaching one is a
			// test failure, not an acceptable alternate refusal.
			switch method {
			case "run.pause":
				_, err = executeRunControl(context.Background(), req, RunControlHandlerOptions{SelectedForkControls: owner}, time.Now(), "pause")
			case "run.continue":
				_, err = executeRunControl(context.Background(), req, RunControlHandlerOptions{SelectedForkControls: owner}, time.Now(), "continue")
			case "agent.restart":
				_, err = executeAgentRestart(context.Background(), req, AgentControlHandlerOptions{SelectedForkControls: owner}, time.Now())
			case "agent.send_directive":
				_, err = executeAgentSendDirective(context.Background(), req, AgentControlHandlerOptions{SelectedForkControls: owner}, time.Now())
			}
			var application *ApplicationError
			if !errors.As(err, &application) || application.Code != "SELECTED_FORK_CONTROL_UNSUPPORTED" || application.Retryable || owner.calls != 1 || owner.runID != runID || string(owner.operation) != method {
				t.Fatalf("wrong selected refusal: %v, owner=%+v", err, owner)
			}
		})
	}
}
