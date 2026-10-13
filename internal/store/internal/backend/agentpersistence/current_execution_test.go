package agentpersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

func TestAgentExecutionObservationRequiresExactRunningRow(t *testing.T) {
	identity := agentidentitytest.RootRuntime(t, "worker", "row-authority-test")
	state := manager.AgentLifecycleState{Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 9, Generation: 4, Phase: manager.AgentLifecycleRunning}
	exact := effects.LifecycleToken{Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 9, Generation: 4}
	if !agentExecutionMatches(state, exact) {
		t.Fatal("exact running row refused its token")
	}
	for _, name := range []string{"absent", "epoch", "generation", "identity", "registered", "failed", "terminated"} {
		t.Run(name, func(t *testing.T) {
			row, token := state, exact
			switch name {
			case "absent":
				token = effects.LifecycleToken{}
			case "epoch":
				token.RuntimeEpoch++
			case "generation":
				token.Generation++
			case "identity":
				token.Identity = agentidentitytest.RootRuntime(t, "sibling", "row-authority-test")
				token.AgentID = token.Identity.AgentID()
			default:
				row.Phase = manager.AgentLifecyclePhase(name)
			}
			if agentExecutionMatches(row, token) {
				t.Fatal("noncurrent row/token admitted execution")
			}
		})
	}
	if _, err := LockAgentExecutionTx(context.Background(), nil, identity, exact, false); err == nil {
		t.Fatal("mutation without its original transaction was admitted")
	}
}
