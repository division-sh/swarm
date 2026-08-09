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
	flowID := canonicalLookupValue(cfg.FlowID)
	for _, record := range agentRecords(source) {
		if canonicalLookupValue(record.entry.Role) != role {
			continue
		}
		if flowID != "" && canonicalLookupValue(record.flowID) != flowID {
			continue
		}
		return strings.TrimSpace(record.logicalID), record.entry, true
	}
	return "", runtimecontracts.AgentRegistryEntry{}, false
}

func AgentDeclarationOwner(source Source, flowID, logicalID string) (string, bool) {
	return agentDeclarationOwner(source, AgentDeclaration{OwnerFlowID: flowID, LocalID: logicalID})
}

func ScopedAgentDeclarationOwner(source Source, declaration AgentDeclaration) (string, bool) {
	declarations := AgentDeclarations(source)
	for _, candidate := range declarations {
		if strings.TrimSpace(candidate.ScopeKind) != strings.TrimSpace(declaration.ScopeKind) ||
			strings.TrimSpace(candidate.ScopeID) != strings.TrimSpace(declaration.ScopeID) ||
			strings.TrimSpace(candidate.LocalID) != strings.TrimSpace(declaration.LocalID) {
			continue
		}
		if strings.TrimSpace(candidate.ScopeKind) != "" && strings.TrimSpace(candidate.OwnerURI) == "" {
			return "", false
		}
		ownerURI := strings.TrimSpace(candidate.OwnerURI)
		for _, other := range declarations {
			if strings.TrimSpace(other.ScopeKind) == strings.TrimSpace(candidate.ScopeKind) &&
				strings.TrimSpace(other.ScopeID) == strings.TrimSpace(candidate.ScopeID) &&
				strings.TrimSpace(other.LocalID) == strings.TrimSpace(candidate.LocalID) {
				continue
			}
			if ownerURI != "" && strings.TrimSpace(other.OwnerURI) == ownerURI {
				return "", false
			}
		}
		return agentDeclarationOwner(source, candidate)
	}
	return "", false
}

func agentDeclarationOwner(source Source, declaration AgentDeclaration) (string, bool) {
	if source == nil {
		return "", false
	}
	flowID := strings.TrimSpace(declaration.OwnerFlowID)
	logicalID := strings.TrimSpace(declaration.LocalID)
	if logicalID == "" {
		return "", false
	}

	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return "", false
	}
	if owner := strings.TrimSpace(declaration.OwnerURI); owner != "" {
		ref, exists := bundle.URIRegistry.ByURI[owner]
		if exists && strings.TrimSpace(ref.Kind) == "agent" && strings.TrimSpace(ref.LocalID) == logicalID {
			return owner, true
		}
		return "", false
	}
	owners := map[string]struct{}{}
	for _, ref := range bundle.URIRegistry.Agents {
		if strings.TrimSpace(ref.LocalID) != logicalID {
			continue
		}
		switch strings.TrimSpace(declaration.ScopeKind) {
		case "flow":
			if strings.TrimSpace(ref.FlowID) != flowID {
				continue
			}
		case "project":
			if strings.TrimSpace(ref.FlowID) != "" || !projectAgentRefOwnedByFlow(source, ref, flowID, logicalID) {
				continue
			}
		default:
			refFlowID := strings.TrimSpace(ref.FlowID)
			if refFlowID != flowID {
				if refFlowID != "" || !projectAgentRefOwnedByFlow(source, ref, flowID, logicalID) {
					continue
				}
			}
		}
		owner := strings.TrimSpace(ref.Full)
		if owner == "" {
			owner = strings.TrimSpace(ref.Absolute)
		}
		if owner != "" {
			owners[owner] = struct{}{}
		}
	}
	if len(owners) == 1 {
		for owner := range owners {
			return owner, true
		}
	}
	return "", false
}

func projectAgentRefOwnedByFlow(source Source, ref runtimecontracts.ContractURIRef, flowID, logicalID string) bool {
	refPath := strings.Trim(strings.TrimSpace(ref.Path), "/")
	matchingScopes := 0
	for _, scope := range source.ProjectScopes() {
		if strings.TrimSpace(scope.OwningFlowID) != strings.TrimSpace(flowID) {
			continue
		}
		if _, ok := scope.Agents[strings.TrimSpace(logicalID)]; !ok {
			continue
		}
		matchingScopes++
		if strings.Trim(strings.TrimSpace(scope.Key), "/") == refPath {
			return true
		}
	}
	if matchingScopes != 1 {
		return false
	}
	matchingRefs := 0
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		return false
	}
	for _, candidate := range bundle.URIRegistry.Agents {
		if strings.TrimSpace(candidate.FlowID) == "" && strings.TrimSpace(candidate.LocalID) == strings.TrimSpace(logicalID) {
			matchingRefs++
		}
	}
	return matchingRefs == 1
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
