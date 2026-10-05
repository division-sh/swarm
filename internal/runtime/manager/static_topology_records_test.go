package manager

import "github.com/division-sh/swarm/internal/runtime/semanticview"

func (am *AgentManager) resolvedStaticTopologyRecords(runID string, source semanticview.Source) ([]PersistedAgent, error) {
	blueprints, err := am.resolvedStaticTopologyBlueprints(source)
	if err != nil {
		return nil, err
	}
	return materializeStaticAgentBlueprints(runID, blueprints)
}
