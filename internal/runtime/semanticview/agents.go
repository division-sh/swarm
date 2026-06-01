package semanticview

import (
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

type agentRecord struct {
	logicalID string
	entry     runtimecontracts.AgentRegistryEntry
	flowID    string
}

func ResolveAgentRegistryEntry(source Source, cfg models.AgentConfig) (string, runtimecontracts.AgentRegistryEntry, bool) {
	if source == nil {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	if matched := resolveAgentRegistryByID(source, strings.TrimSpace(cfg.ID)); matched != "" {
		for _, record := range agentRecords(source) {
			if strings.TrimSpace(record.logicalID) == matched {
				return matched, record.entry, true
			}
		}
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}

	role := canonicalLookupValue(cfg.Role)
	if role == "" {
		return "", runtimecontracts.AgentRegistryEntry{}, false
	}
	mode := canonicalLookupValue(cfg.Mode)
	for _, record := range agentRecords(source) {
		if canonicalLookupValue(record.entry.Role) != role {
			continue
		}
		if mode != "" {
			if flowMode := flowModeByID(source, record.flowID); flowMode != "" && flowMode != mode {
				continue
			}
		}
		return strings.TrimSpace(record.logicalID), record.entry, true
	}
	return "", runtimecontracts.AgentRegistryEntry{}, false
}

func resolveAgentRegistryByID(source Source, agentID string) string {
	agentID = strings.TrimSpace(agentID)
	if source == nil || agentID == "" {
		return ""
	}
	for _, record := range agentRecords(source) {
		if strings.TrimSpace(record.logicalID) == agentID || registryIDMatches(record.entry.ID, agentID) {
			return strings.TrimSpace(record.logicalID)
		}
	}
	return ""
}

func agentRecords(source Source) []agentRecord {
	if source == nil {
		return nil
	}
	projectScopes := source.ProjectScopes()
	flowScopes := source.FlowScopes()
	records := make([]agentRecord, 0, len(projectScopes)+len(flowScopes))
	for _, scope := range projectScopes {
		for _, logicalID := range sortedKeys(scope.Agents) {
			records = append(records, agentRecord{
				logicalID: logicalID,
				entry:     scope.Agents[logicalID],
			})
		}
	}
	for _, scope := range flowScopes {
		for _, logicalID := range sortedKeys(scope.Agents) {
			records = append(records, agentRecord{
				logicalID: logicalID,
				entry:     scope.Agents[logicalID],
				flowID:    strings.TrimSpace(scope.ID),
			})
		}
	}
	return records
}

func flowModeByID(source Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" || source == nil {
		return ""
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return ""
	}
	return canonicalLookupValue(scope.Mode)
}

func registryIDMatches(template, candidate string) bool {
	template = strings.TrimSpace(template)
	candidate = strings.TrimSpace(candidate)
	if template == "" || candidate == "" {
		return false
	}
	if template == candidate {
		return true
	}
	matched, err := regexp.MatchString(templateMatchPattern(template), candidate)
	return err == nil && matched
}

func templateMatchPattern(template string) string {
	matches := promptTemplateFieldPattern.FindAllStringIndex(template, -1)
	if len(matches) == 0 {
		return "^" + regexp.QuoteMeta(template) + "$"
	}
	var builder strings.Builder
	builder.WriteString("^")
	last := 0
	for _, match := range matches {
		builder.WriteString(regexp.QuoteMeta(template[last:match[0]]))
		builder.WriteString(".+")
		last = match[1]
	}
	builder.WriteString(regexp.QuoteMeta(template[last:]))
	builder.WriteString("$")
	return builder.String()
}

func canonicalLookupValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	return value
}

func sortedKeys[V any](m map[string]V) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return keys
}

var promptTemplateFieldPattern = regexp.MustCompile(`\{[^{}]+\}`)
