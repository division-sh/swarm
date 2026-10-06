package manager

import (
	"context"
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
