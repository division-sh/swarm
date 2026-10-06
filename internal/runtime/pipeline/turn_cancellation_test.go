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
	calls       int
	queuedCalls int
	fault       error
	panic       bool
}

func (p *committedTurnCancellationProbe) ApplyCommittedQueuedCancellations(_ context.Context, snapshots []deliverylifecycle.Snapshot) error {
	p.queuedCalls++
	if len(snapshots) != 1 || snapshots[0].Status != deliverylifecycle.StatusCanceled || snapshots[0].ReasonCode != "terminate" {
		return errors.New("missing exact queued cancellation")
	}
	if p.panic {
		panic("owned queued dispatch fault")
	}
	return p.fault
}

func TestCommittedQueuedCancellationDispatchRequiresAcknowledgment(t *testing.T) {
	for _, mode := range []string{"healthy", "unacknowledged", "missing_owner", "cleanup_error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			fault := errors.New("continuation cleanup failed")
			probe := &committedTurnCancellationProbe{panic: mode == "panic"}
			if mode == "cleanup_error" {
				probe.fault = fault
			}
			pc := &PipelineCoordinator{}
			if mode != "missing_owner" {
				if err := pc.BindTurnCancellationDispatcher(probe); err != nil {
					t.Fatal(err)
				}
			}
			pending := CommittedWorkflowLifecycleMutation{QueuedCancellations: []deliverylifecycle.Snapshot{{
				DeliveryID: uuid.NewString(), RunID: uuid.NewString(), SubscriberID: "queued-agent", SubscriberClass: deliverylifecycle.SubscriberAgent,
				Status: deliverylifecycle.StatusCanceled, ReasonCode: "terminate", SettledAt: time.Now().UTC(),
			}}}
			committed := pending
			if mode != "unacknowledged" {
				committed = pending.WithCommitAcknowledgment()
			}
			err := pc.finalizeWorkflowLifecycleMutation(context.Background(), committed)
			wantCalls := 1
			if mode == "missing_owner" || mode == "unacknowledged" {
				wantCalls = 0
			}
			if probe.queuedCalls != wantCalls || (err != nil) != (mode != "healthy") || mode == "cleanup_error" && !errors.Is(err, fault) {
				t.Fatalf("queued post-commit dispatch: calls=%d err=%v", probe.queuedCalls, err)
			}
			if pending.Committed {
				t.Fatal("acknowledgment changed uncommitted evidence")
			}
		})
	}
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
