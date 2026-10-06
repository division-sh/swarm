package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

// These are exact execution-lease dispatch controls, not durable cancellation
// settlement or public-provider qualification.
func TestManagerTerminationDispatchIncludesOwnedRetiringExecutions(t *testing.T) {
	for _, mode := range []string{"current", "retirement", "terminal_set", "missing_ack"} {
		t.Run(mode, func(t *testing.T) {
			am := newTestAgentManagerWithOptions(t, &canceledTurnConsumerBus{}, nil, AgentManagerOptions{})
			identity := agentidentitytest.RootRuntime(t, "termination-agent", "termination-test")
			origin := agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()}
			ctx, turn := effects.WithTurnExecution(effects.WithDirectiveCompletionOrigin(context.Background(), origin))
			defer func() { _, _ = turn.Finish() }()
			otherOrigin := agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()}
			otherCtx, other := effects.WithTurnExecution(effects.WithDirectiveCompletionOrigin(context.Background(), otherOrigin))
			defer func() { _, _ = other.Finish() }()
			lease := &agentExecutionLease{}
			execution := &agentExecutionProjection{leases: map[*agentExecutionLease]struct{}{lease: {}, {turn: other}: {}}}
			cell := &agentLifecycleCell{identity: identity, execution: execution}
			am.lifecycle.cells[identity] = cell
			if err := am.attachLogicalTurn(context.WithValue(ctx, agentExecutionLeaseContextKey{}, lease), turn); err != nil {
				t.Fatal(err)
			}
			if mode == "retirement" || mode == "terminal_set" {
				cell.execution = &agentExecutionProjection{}
				retirement := &agentRetirement{cell: cell, execution: execution}
				if mode == "retirement" {
					cell.retirement = retirement
				} else {
					cell.terminalSet = &terminalFlowRetirement{retirements: []*agentRetirement{retirement}}
				}
			}
			completion, err := effects.DirectiveCompletionOrigin(origin)
			if err != nil {
				t.Fatal(err)
			}
			intent := effects.TurnCancellation{Committed: mode != "missing_ack", Requested: true, Origin: completion, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: uuid.NewString(), RequestedAt: time.Now().UTC()}
			err = am.ApplyCommittedTurnCancellations(context.Background(), []effects.TurnCancellation{intent})
			if mode == "missing_ack" {
				if err == nil || ctx.Err() != nil {
					t.Fatalf("unacknowledged intent reached execution: cause=%v err=%v", context.Cause(ctx), err)
				}
			} else if err != nil || ctx.Err() == nil {
				t.Fatalf("owned %s execution missed intent: cause=%v err=%v", mode, context.Cause(ctx), err)
			}
			if otherCtx.Err() != nil || len(execution.leases) != 2 {
				t.Fatal("termination canceled sibling work or released cleanup ownership")
			}
		})
	}
}

func TestManagerQueuedCancellationReleasesOnlyCommittedOrigin(t *testing.T) {
	for _, mode := range []string{"healthy", "cleanup_error", "wrong_reason", "pending"} {
		t.Run(mode, func(t *testing.T) {
			bus := &receiptOutcomeBus{}
			fault := errors.New("continuation release failed")
			if mode == "cleanup_error" {
				bus.failure = fault
			}
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{})
			snapshot := deliverylifecycle.Snapshot{DeliveryID: uuid.NewString(), RunID: uuid.NewString(), SubscriberID: "queued-agent", SubscriberClass: deliverylifecycle.SubscriberAgent, Status: deliverylifecycle.StatusCanceled, ReasonCode: "terminate", SettledAt: time.Now().UTC()}
			if mode == "wrong_reason" {
				snapshot.ReasonCode = "shutdown"
			}
			if mode == "pending" {
				snapshot.Status = deliverylifecycle.StatusPending
			}
			err := am.ApplyCommittedQueuedCancellations(context.Background(), []deliverylifecycle.Snapshot{snapshot})
			wantRelease := mode == "healthy" || mode == "cleanup_error"
			if (len(bus.released) == 1) != wantRelease || wantRelease && bus.released[0] != snapshot.DeliveryID || (err != nil) != (mode != "healthy") || mode == "cleanup_error" && !errors.Is(err, fault) {
				t.Fatalf("queued continuation release: %+v err=%v", bus.released, err)
			}
		})
	}
}
