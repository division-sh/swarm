package agentpersistence

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/manager"
)

// The completion inventory uses this fixed join and predicate only to select
// recovery candidates. Settlement still locks and validates the current row.
func CompletionRecoveryAgentJoinSQL() string {
	return `LEFT JOIN agents g ON o.authority_kind='normal_agent'
		AND g.run_id=o.agent_run_id AND g.agent_id=o.agent_id
		AND g.agent_name_owner=o.agent_name_owner AND g.agent_name_source=o.agent_name_source
		AND g.agent_route_presence=o.agent_route_presence AND g.flow_scope_key=o.flow_scope_key
		AND g.flow_instance_id=o.flow_instance_id AND g.flow_instance=o.flow_instance`
}

func CompletionRecoveryAgentStaleSQL(sqlite bool) string {
	query := fmt.Sprintf(`g.agent_id IS NULL OR g.lifecycle_runtime_epoch IS DISTINCT FROM a.runtime_epoch OR
		g.lifecycle_generation IS DISTINCT FROM a.generation OR g.lifecycle_phase <> '%s'`, manager.AgentLifecycleRunning)
	if sqlite {
		query = strings.ReplaceAll(query, "IS DISTINCT FROM", "IS NOT")
	}
	return query
}
