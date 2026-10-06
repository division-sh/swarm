package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type committedTurnCancellationProbe struct {
	calls int
	fault error
	panic bool
}

func (p *committedTurnCancellationProbe) ApplyCommittedTurnCancellations(_ context.Context, intents []effects.TurnCancellation) error {
	p.calls++
	if len(intents) != 1 || intents[0].ValidateIntent() != nil {
		return errors.New("missing committed exact cancellation")
	}
	if p.panic {
		panic("owned dispatch fault")
	}
	return p.fault
}

func TestCommittedTurnCancellationDispatchRequiresExactAcknowledgment(t *testing.T) {
	for _, mode := range []string{"healthy", "unacknowledged", "missing_intent_ack", "missing_owner", "cleanup_error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			origin, err := effects.DirectiveCompletionOrigin(agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			intent := effects.TurnCancellation{Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: uuid.NewString(), RequestedAt: time.Now().UTC()}
			pending := CommittedWorkflowLifecycleMutation{TurnCancellations: []effects.TurnCancellation{intent}}
			committed := pending.WithCommitAcknowledgment()
			if pending.Committed || pending.TurnCancellations[0].Committed {
				t.Fatal("acknowledgment mutated uncommitted intent facts")
			}
			probe := &committedTurnCancellationProbe{fault: errors.New("owned dispatch cleanup")}
			if mode != "cleanup_error" {
				probe.fault = nil
			}
			probe.panic = mode == "panic"
			pc := &PipelineCoordinator{}
			if mode != "missing_owner" {
				if err := pc.BindTurnCancellationDispatcher(probe); err != nil {
					t.Fatal(err)
				}
				if err := pc.BindTurnCancellationDispatcher(probe); err == nil {
					t.Fatal("runtime cancellation dispatch admitted dual owners")
				}
			}
			switch mode {
			case "unacknowledged":
				committed = pending
			case "missing_intent_ack":
				committed.TurnCancellations[0].Committed = false
			}
			err = pc.finalizeWorkflowLifecycleMutation(context.Background(), committed)
			if (err != nil) != (mode != "healthy") {
				t.Fatalf("%s acknowledgment/dispatch disposition: %v", mode, err)
			}
			wantCalls := 0
			if mode == "healthy" || mode == "cleanup_error" || mode == "panic" {
				wantCalls = 1
			}
			if probe.calls != wantCalls || mode == "cleanup_error" && !errors.Is(err, probe.fault) {
				t.Fatalf("dispatch ownership: calls=%d err=%v", probe.calls, err)
			}
		})
	}
}
