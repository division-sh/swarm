package authority

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

type sourceProvider struct {
	humanTaskRoles   []string
	mailboxSendRoles []string
	producerRoles    []string
	agentEvents      map[string][]string
}

func NewSourceProvider(source semanticview.Source) Provider {
	return buildSourceProvider(source)
}

func buildSourceProvider(source semanticview.Source) Provider {
	if source == nil {
		return noopProvider{}
	}
	allRoles, toolGrants := sourceRolesAndToolGrants(source)
	humanTaskRoles := make([]string, 0, len(allRoles))
	mailboxSendRoles := append([]string(nil), allRoles...)

	for _, role := range allRoles {
		grants := toolGrants[role]
		if hasToolGrant(grants, "human_task_decide") {
			humanTaskRoles = append(humanTaskRoles, role)
		}
	}

	agentEvents := buildProducerRegistry(source)
	producerRoles := make([]string, 0, len(agentEvents))
	for role := range agentEvents {
		producerRoles = append(producerRoles, role)
	}
	sort.Strings(producerRoles)

	return sourceProvider{
		humanTaskRoles:   cloneRoles(humanTaskRoles),
		mailboxSendRoles: cloneRoles(mailboxSendRoles),
		producerRoles:    producerRoles,
		agentEvents:      agentEvents,
	}
}

func (p sourceProvider) CanonicalRole(role string) string {
	return canonicalRole(role)
}

func (p sourceProvider) ProducerRoles() []string {
	return append([]string(nil), p.producerRoles...)
}

func (p sourceProvider) ProducerEventsForRole(role string) []string {
	role = canonicalRole(role)
	if role == "" {
		return nil
	}
	events := p.agentEvents[role]
	out := make([]string, 0, len(events))
	seen := make(map[string]struct{}, len(events))
	for _, evt := range events {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		if _, ok := seen[evt]; ok {
			continue
		}
		seen[evt] = struct{}{}
		out = append(out, evt)
	}
	sort.Strings(out)
	return out
}

func (p sourceProvider) HasMessageAuthority(actor, target models.AgentConfig) bool {
	sender := canonicalRole(actor.Role)
	recipient := canonicalRole(target.Role)
	if sender == "" || recipient == "" {
		return false
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return true
	}
	if !SameFlowInstance(actor, target) {
		return false
	}
	switch strongestMessagePermission(permissionSet(actor.Permissions)) {
	case "message_flow":
		return true
	case "message_peers":
		return PeerManagerFallback(actor, target)
	default:
		return false
	}
}

func (p sourceProvider) AuthorizeRouting(actor, target models.AgentConfig, status string) error {
	_ = strings.TrimSpace(strings.ToLower(status))
	if !hasToolGrant(permissionSet(actor.Permissions), "configure_routing") {
		return fmt.Errorf("role %s is not authorized to configure routing", actor.Role)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return nil
	}
	if err := p.AuthorizeManagement(actor, target); err != nil {
		return fmt.Errorf("role %s is not authorized to configure routing", actor.Role)
	}
	return nil
}

func (p sourceProvider) AuthorizeManagement(actor, target models.AgentConfig) error {
	if !hasAnyToolGrant(permissionSet(actor.Permissions), "agent_hire", "agent_fire", "agent_reconfigure") {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if !SameFlowInstance(actor, target) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if strings.TrimSpace(target.ParentAgent) == strings.TrimSpace(actor.ID) || ManagerFallbackFromConfig(target.Config) == strings.TrimSpace(actor.ID) {
		return nil
	}
	return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
}

func (p sourceProvider) AuthorizeMailboxSend(actor models.AgentConfig) error {
	if containsCanonical(p.mailboxSendRoles, actor.Role) {
		return nil
	}
	return fmt.Errorf("role %s is not authorized to send mailbox items", actor.Role)
}

func (p sourceProvider) CanDecideHumanTasks(role string) bool {
	role = canonicalRole(role)
	if role == "" {
		return false
	}
	for _, candidate := range p.humanTaskRoles {
		if canonicalRole(candidate) == role {
			return true
		}
	}
	return false
}

func sourceRolesAndToolGrants(source semanticview.Source) ([]string, map[string]map[string]struct{}) {
	if source == nil {
		return nil, map[string]map[string]struct{}{}
	}
	roles := make([]string, 0, len(source.AgentEntries()))
	grants := make(map[string]map[string]struct{}, len(source.AgentEntries()))
	for key, entry := range source.AgentEntries() {
		role := canonicalRole(firstNonEmpty(entry.Role, key))
		if role == "" {
			continue
		}
		if _, ok := grants[role]; !ok {
			roles = append(roles, role)
			grants[role] = map[string]struct{}{}
		}
		for _, toolName := range entry.ConfiguredTools() {
			toolName = strings.TrimSpace(toolName)
			if toolName == "" {
				continue
			}
			grants[role][toolName] = struct{}{}
		}
	}
	sort.Strings(roles)
	return roles, grants
}

func buildProducerRegistry(source semanticview.Source) map[string][]string {
	if source == nil {
		return map[string][]string{}
	}
	agentEvents := make(map[string][]string)
	for role, entry := range source.AgentEntries() {
		role = canonicalRole(firstNonEmpty(role, entry.Role))
		if role == "" {
			continue
		}
		for _, eventType := range entry.EmitEvents {
			agentEvents[role] = appendUniqueSortedEvent(agentEvents[role], eventType)
		}
	}
	for nodeID, node := range source.NodeEntries() {
		role := canonicalRole(nodeID)
		if role == "" {
			continue
		}
		for _, eventType := range node.Produces {
			agentEvents[role] = appendUniqueSortedEvent(agentEvents[role], eventType)
		}
	}
	for _, timer := range source.WorkflowTimers() {
		_ = strings.TrimSpace(timer.Event)
	}
	return agentEvents
}

func strongestMessagePermission(grants map[string]struct{}) string {
	switch {
	case hasToolGrant(grants, "message_flow"):
		return "message_flow"
	case hasToolGrant(grants, "message_peers"):
		return "message_peers"
	default:
		return ""
	}
}

func permissionSet(perms []string) map[string]struct{} {
	out := make(map[string]struct{}, len(perms))
	for _, perm := range perms {
		perm = strings.TrimSpace(perm)
		if perm == "" {
			continue
		}
		out[perm] = struct{}{}
	}
	return out
}

func hasToolGrant(grants map[string]struct{}, toolName string) bool {
	if len(grants) == 0 {
		return false
	}
	_, ok := grants[strings.TrimSpace(toolName)]
	return ok
}

func hasAnyToolGrant(grants map[string]struct{}, toolNames ...string) bool {
	for _, toolName := range toolNames {
		if hasToolGrant(grants, toolName) {
			return true
		}
	}
	return false
}

func canonicalRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.Join(strings.Fields(role), "-")
	return role
}

func cloneRoles(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, role := range in {
		role = canonicalRole(role)
		if role == "" {
			continue
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

func containsCanonical(items []string, target string) bool {
	target = canonicalRole(target)
	return slices.ContainsFunc(items, func(item string) bool {
		return canonicalRole(item) == target
	})
}

func containsNormalized(items []string, target string) bool {
	target = strings.TrimSpace(strings.ToLower(target))
	return slices.ContainsFunc(items, func(item string) bool {
		return strings.TrimSpace(strings.ToLower(item)) == target
	})
}

func appendUniqueSortedEvent(events []string, eventType string) []string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return events
	}
	for _, existing := range events {
		if strings.TrimSpace(existing) == eventType {
			return events
		}
	}
	events = append(events, eventType)
	sort.Strings(events)
	return events
}

func SameFlowInstance(actor, target models.AgentConfig) bool {
	actorFlow := flowPathFromConfig(actor.Config)
	targetFlow := flowPathFromConfig(target.Config)
	return actorFlow != "" && actorFlow == targetFlow
}

func PeerManagerFallback(actor, target models.AgentConfig) bool {
	actorFallback := managerFallbackFromConfig(actor.Config)
	targetFallback := managerFallbackFromConfig(target.Config)
	return actorFallback != "" && actorFallback == targetFallback
}

func FlowPathFromConfig(raw []byte) string {
	return flowPathFromConfig(raw)
}

func flowPathFromConfig(raw []byte) string {
	payload := decodeConfigMap(raw)
	if payload == nil {
		return ""
	}
	if value, ok := payload["flow_path"].(string); ok {
		return strings.Trim(strings.TrimSpace(value), "/")
	}
	return ""
}

func ManagerFallbackFromConfig(raw []byte) string {
	return managerFallbackFromConfig(raw)
}

func managerFallbackFromConfig(raw []byte) string {
	payload := decodeConfigMap(raw)
	if payload == nil {
		return ""
	}
	if value, ok := payload["manager_fallback"].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func decodeConfigMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
