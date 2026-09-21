package canonicalrouting

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// ReceiverAgentCollisionValues are inert business data, never actor authority.
func ReceiverAgentCollisionValues() map[string]any {
	values := map[string]any{}
	for _, name := range strings.Fields("type mode model model_tier llm_backend resolved_llm_backend resolved_model resolved_llm_provider resolved_llm_transport conversation_mode session_scope session_scope_authority memory max_turns_per_task subscriptions emit_events tools permissions native_tools workspace_class manager_fallback flow_path flow_id flow_instance flow_data_access budget_envelope execution_mode mock intent criteria entity_id parent_agent_id") {
		values[name] = "business-" + name
	}
	values["constraints"] = map[string]any{"memory": "business-memory", "mode": "business-mode", "max_turns_per_task": 999}
	values["nested"] = map[string]any{"archived_record": map[string]any{"system_prompt": "INERT_ARCHIVED_PROMPT"}}
	values["records"] = []any{map[string]any{"system_prompt": "INERT_LIST_PROMPT"}, int64(7), false}
	return values
}

// CopyReceiverAgentCollision retains real external ingress, connect resolution,
// managed agent execution and a state-owning receiver in the same instance.
func CopyReceiverAgentCollision(t testing.TB) string {
	t.Helper()
	root := CopyServedReceiverInitialization(t)
	keys := make([]string, 0)
	for key := range ReceiverAgentCollisionValues() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var types, variables, initialize strings.Builder
	types.WriteString("types:\n  InitializationValues:\n")
	for _, key := range keys {
		fmt.Fprintf(&types, "    %s: json?\n", key)
		fmt.Fprintf(&variables, "    %s: json\n", key)
		fmt.Fprintf(&initialize, "          %s: payload.values.%s\n", key, key)
	}
	writeClosedVariantFile(t, root, "types.yaml", types.String())
	writeClosedVariantFile(t, root, "account/schema.yaml", "name: account\nmode: template\ninstance: account_id\ninitial_state: active\nstates: [active]\ninstance_variables:\n  variables:\n"+variables.String()+"pins:\n  inputs:\n    events:\n      - event: work.ready\n        resolution: {mode: select-or-create}\n        initialize:\n"+initialize.String())
	writeClosedVariantFile(t, root, "account/agents.yaml", `observer:
  id: observer
  role: observer
  model: regular
  intent: {inline: 'AUTHORED_COLLISION_OBSERVER: acknowledge the received account work.'}
  subscriptions: [work.ready]
  memory: false
`)
	return root
}
