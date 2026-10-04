package manager

import (
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

// ProveUnpublishedActivation is an observation of the existing lifecycle owner,
// not a second admission grant. An old generation or published route cannot use
// the pre-publication probe entrance.
func (am *AgentManager) ProveUnpublishedActivation(token effects.LifecycleToken) error {
	if am != nil && am.lifecycle != nil && token.Valid() {
		am.lifecycle.mu.Lock()
		defer am.lifecycle.mu.Unlock()
		cell := am.lifecycle.cells[token.Identity]
		if am.lifecycle.phase == runtimeLifecycleRunning && cell != nil && cell.phase == AgentLifecycleRunning && cell.execution != nil {
			execution := cell.execution
			if execution.token == token && !execution.fenced && execution.routeToken == (effects.LifecycleToken{}) && execution.route == nil && execution.loopDone != nil && execution.generationCtx != nil && execution.generationCtx.Err() == nil {
				return nil
			}
		}
	}
	return failures.New(failures.ClassLifecycleConflict, "activation_probe_generation_not_current", "agent-lifecycle", "observe_workspace_gateway", nil)
}
