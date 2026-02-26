package runtime

import (
	"strings"

	"empireai/internal/models"
)

// budgetExecutionScopeKey controls intra-process serialization for LLM budget
// preflight/recording. Vertical-scoped agents keep per-vertical locking.
// Factory-shard agents (no vertical_id) get per-agent scope keys so sharded
// scans can execute concurrently instead of funneling through one global lock.
func budgetExecutionScopeKey(actor models.AgentConfig) string {
	verticalID := strings.TrimSpace(actor.VerticalID)
	if verticalID != "" {
		return verticalID
	}
	mode := strings.ToLower(strings.TrimSpace(actor.Mode))
	if mode == "factory" {
		if agentID := strings.TrimSpace(actor.ID); agentID != "" {
			return "__factory_agent__:" + agentID
		}
	}
	return ""
}
