package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const maxUsageHintRunes = 1200

var builtinToolUsageHints = map[string]string{
	"agent_message":      "Use for agent-to-agent messages only. Provide the exact target agent ID and message content. Do not use this to publish workflow events; use emit_* tools for events.",
	"mailbox_send":       "Use for human mailbox items only. Provide the task kind, subject/summary, priority when relevant, and structured context. Do not use this for agent-to-agent messages.",
	"schedule":           "Use only when scheduling a future event/action for the current entity or actor. Provide concrete RFC3339 time or delay-derived timing and the event/action payload expected by the workflow.",
	"agent_hire":         "Use only with explicit permission to spawn a runtime agent. Provide the agent config fields required by the target role; do not invent product-specific tool names.",
	"agent_fire":         "Use only with explicit permission to terminate an agent session. Provide the target agent ID and a concrete reason.",
	"agent_reconfigure":  "Use only with explicit permission to modify an existing agent. Provide only supported agent config fields such as model alias, runtime mode, tools, emit events, native tools, or prompt config.",
	"get_entity":         "Read one existing entity by entity_id. Use flow_instance only as the owning-flow guard/address when needed; it must match the entity's owning flow root or instance. Returns envelope fields plus declared entity fields.",
	"create_entity":      "Create a new entity in the inferred flow-owned contract. Do not provide entity_id, entity_type, subject_id, or other envelope fields. Put authored contract fields under fields using declared field names and declared value shapes.",
	"save_entity_field":  "Write exactly one delivered writable field on an entity owned by your current flow. Do not write upstream/root/source entities from triggering events. Use only field names from this tool's enum/session writable-path summary. Value must match the declared field shape; lists require arrays and objects require objects.",
	"query_entities":     "Read/query entity_state rows. entity_type, select, group_by, and filter paths must use delivered enum/declared scalar or enum leaf names. filter is CEL, so equality is ==, strings are quoted, and assignment = is invalid.",
	"search_entities":    "Search entity_state rows with object field filters. entity_type and filter keys must use delivered declared field names for the target entity contract. Use query_entities when you need CEL, select, or group_by.",
	"query_metrics":      "Aggregate entity_state rows. metric must be one of the delivered enum values. field and group_by must use delivered scalar or enum selector names. filter is CEL, so equality is == and strings are quoted.",
	"human_task_request": "Create a human task only when human input is required. Provide a clear summary/context and deadline fields in supported shapes.",
	"human_task_decide":  "Record a decision on an existing human task. Use only supported decision values such as approved, rejected, or deferred and include the target task reference.",
	"read_flow_data":     "Read only declared deploy-time reference files from your owning flow data root. Provide one filename from the delivered enum; do not use host paths or this tool for mutable artifacts.",
}

var nativeFallbackUsageHints = map[string]string{
	"bash":       "Use only for local workspace commands. Provide command as a string and timeout_seconds when needed. Docker-backed bash exposes /workspace, /data, and /opt/swarm/contracts as OS paths; trusted host bash is full host-user shell execution from the workspace backing directory. Use relative paths for workspace files; absolute paths follow the host deployment namespace and OS permissions. Do not use for workflow event emission or entity persistence.",
	"web_search": "Use for external web research only. Provide a concise query and max_results when needed. Do not use it to read Swarm entity state; use entity tools for that.",
	"read_file":  "Read files by exact path from the workspace or mounted read-only data/contracts paths. Do not use for entity_state reads.",
	"write_file": "Write files only within the agent workspace. Do not use file writes as a substitute for emit_* event publication or save_entity_field persistence.",
}

var emitToolUsageHint = "Call this emit_* tool only to publish the named workflow event. Provide concrete JSON payload values matching the input schema. Do not include envelope-owned fields unless the schema declares them. Arguments are concrete payload values, not workflow expressions."

func runtimeOwnedToolUsage(name string) string {
	name = strings.TrimSpace(name)
	if usage := strings.TrimSpace(builtinToolUsageHints[name]); usage != "" {
		return usage
	}
	if usage := strings.TrimSpace(nativeFallbackUsageHints[name]); usage != "" {
		return usage
	}
	return ""
}

func NativeFallbackToolUsage(name string) string {
	return runtimeOwnedToolUsage(name)
}

func EmitToolUsage() string {
	return emitToolUsageHint
}

func PlatformOwnedToolUsage(name string) string {
	return runtimeOwnedToolUsage(name)
}

func DescriptionWithUsage(description, usage string) string {
	return llm.DescriptionWithUsage(description, usage)
}

type UsageHintFinding struct {
	ToolName string
	Severity string
	Message  string
}

func ValidateUsageHintCoverage(source semanticview.Source, discovered map[string]runtimemcp.DiscoveredTool) []UsageHintFinding {
	findings := make([]UsageHintFinding, 0)
	entries, err := registeredToolsForRuntime(source, discovered)
	if err != nil {
		return []UsageHintFinding{{
			Severity: "error",
			Message:  fmt.Sprintf("resolve runtime tool registry: %v", err),
		}}
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := entries[name]
		if runtimeToolHiddenFromAgents(name) {
			continue
		}
		usage := strings.TrimSpace(entry.Usage)
		switch entry.HandlerType {
		case implementationPlatformBuiltin:
			if usage == "" {
				findings = append(findings, UsageHintFinding{
					ToolName: name,
					Severity: "error",
					Message:  fmt.Sprintf("platform-owned delivered tool %s is missing a usage hint", name),
				})
				continue
			}
		case implementationMCP:
			if usage == "" {
				findings = append(findings, UsageHintFinding{
					ToolName: name,
					Severity: "lint",
					Message:  fmt.Sprintf("external MCP/discovered tool %s has no platform-owned usage hint; advisory only", name),
				})
			}
		}
		if runeLen(usage) > maxUsageHintRunes {
			findings = append(findings, UsageHintFinding{
				ToolName: name,
				Severity: "lint",
				Message:  fmt.Sprintf("tool %s usage hint is over %d characters; keep provider-facing usage concise", name, maxUsageHintRunes),
			})
		}
	}
	findings = append(findings, validateEmitToolUsageHintCoverage(source)...)
	return findings
}

func validateEmitToolUsageHintCoverage(source semanticview.Source) []UsageHintFinding {
	if source == nil {
		return nil
	}
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	findings := make([]UsageHintFinding, 0)
	for _, actor := range providerSchemaValidationActors(source) {
		for _, def := range registry.GenerateEmitToolsForActor(actor, nil) {
			usage := strings.TrimSpace(def.Usage)
			if usage == "" {
				findings = append(findings, UsageHintFinding{
					ToolName: def.Name,
					Severity: "error",
					Message:  fmt.Sprintf("generated platform-owned emit tool %s is missing the shared usage hint", def.Name),
				})
				continue
			}
			if strings.Contains(strings.ToLower(usage), "cel") {
				findings = append(findings, UsageHintFinding{
					ToolName: def.Name,
					Severity: "error",
					Message:  fmt.Sprintf("generated platform-owned emit tool %s usage hint must not instruct agent-facing CEL arguments", def.Name),
				})
			}
			if runeLen(usage) > maxUsageHintRunes {
				findings = append(findings, UsageHintFinding{
					ToolName: def.Name,
					Severity: "lint",
					Message:  fmt.Sprintf("generated emit tool %s usage hint is over %d characters; keep provider-facing usage concise", def.Name, maxUsageHintRunes),
				})
			}
		}
	}
	return findings
}

func coalesceNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func runeLen(value string) int {
	return len([]rune(strings.TrimSpace(value)))
}
