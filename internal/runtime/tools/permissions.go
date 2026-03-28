package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

const systemAdminPermission = "system_admin"

var defaultPlatformPermissions = []string{
	"agent_fire",
	"agent_hire",
	"agent_reconfigure",
	"approve_spend",
	"configure_routing",
	"create_flow_instance",
	"human_task_decide",
	"human_task_request",
	"mailbox_send",
	"message_flow",
	"message_peers",
	"schedule",
}

var toolPermissionRequirements = map[string]string{
	"agent_fire":           "agent_fire",
	"agent_hire":           "agent_hire",
	"agent_reconfigure":    "agent_reconfigure",
	"configure_routing":    "configure_routing",
	"create_flow_instance": "create_flow_instance",
	"human_task_request":   "human_task_request",
	"human_task_decide":    "human_task_decide",
	"schedule":             "schedule",
}

func agentHasPermission(agent models.AgentConfig, perm string) bool {
	perm = strings.TrimSpace(perm)
	if perm == "" {
		return false
	}
	for _, candidate := range agent.Permissions {
		if strings.TrimSpace(candidate) == perm {
			return true
		}
	}
	return false
}

func ResolveAgentPermissions(source semanticview.Source, flowID string, entry runtimecontracts.AgentRegistryEntry) ([]string, error) {
	policy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{}}
	if source != nil {
		policy = source.ResolvedPolicyForFlow(strings.TrimSpace(flowID))
	}
	return resolveAgentPermissionsFromPolicy(entry, policy)
}

func ValidateAgentPermissions(source semanticview.Source) (int, []error) {
	agents := scopedAgentEntries(source)
	known := knownPermissionNames(source)
	errs := make([]error, 0)
	for _, agent := range agents {
		policy := agent.policy
		if len(policy.Values) == 0 && source != nil {
			policy = source.ResolvedPolicyForFlow(agent.flowID)
		}
		perms, err := resolveAgentPermissionsFromPolicy(agent.entry, policy)
		if err != nil {
			errs = append(errs, fmt.Errorf("agent %s: %w", agent.id, err))
			continue
		}
		cfg := models.AgentConfig{
			ID:          agent.id,
			Role:        strings.TrimSpace(agent.entry.Role),
			Permissions: perms,
		}
		for _, perm := range perms {
			if _, ok := known[perm]; ok {
				continue
			}
			errs = append(errs, fmt.Errorf("agent %s declares unknown permission %q", agent.id, perm))
		}
		for _, toolName := range agent.entry.ConfiguredTools() {
			toolName = strings.TrimSpace(toolName)
			requiredPerm, ok := toolPermissionRequirements[toolName]
			if !ok {
				continue
			}
			if agentHasPermission(cfg, requiredPerm) {
				continue
			}
			errs = append(errs, fmt.Errorf("agent %s declares tool %s without required permission %q", agent.id, toolName, requiredPerm))
		}
	}
	return len(agents), errs
}

type scopedAgentEntry struct {
	id     string
	flowID string
	entry  runtimecontracts.AgentRegistryEntry
	policy runtimecontracts.PolicyDocument
}

func scopedAgentEntries(source semanticview.Source) []scopedAgentEntry {
	if source == nil {
		return nil
	}
	entries := make([]scopedAgentEntry, 0)
	for _, id := range sortedAgentIDs(source.AgentEntries()) {
		flowID := ""
		if sourceInfo, ok := source.AgentContractSource(id); ok {
			flowID = strings.TrimSpace(sourceInfo.FlowID)
		}
		entries = append(entries, scopedAgentEntry{
			id:     id,
			flowID: flowID,
			entry:  source.AgentEntries()[id],
			policy: source.ResolvedPolicyForFlow(flowID),
		})
	}
	return entries
}

func sortedAgentIDs(entries map[string]runtimecontracts.AgentRegistryEntry) []string {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func resolveAgentPermissionsFromPolicy(entry runtimecontracts.AgentRegistryEntry, policy runtimecontracts.PolicyDocument) ([]string, error) {
	perms := make([]string, 0, len(entry.Permissions)+4)
	bundleName := strings.TrimSpace(entry.PermissionsBundle)
	if bundleName != "" {
		bundlePerms, ok, err := permissionBundlePermissionsFromPolicy(policy, bundleName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("unknown permissions_bundle %q", bundleName)
		}
		perms = append(perms, bundlePerms...)
	}
	perms = append(perms, entry.Permissions...)
	return dedupePermissionList(perms), nil
}

func permissionBundlePermissionsFromPolicy(policy runtimecontracts.PolicyDocument, bundle string) ([]string, bool, error) {
	bundle = strings.TrimSpace(bundle)
	if bundle == "" {
		return nil, false, nil
	}
	root, ok := policy.Values["permission_bundles"]
	if !ok {
		return nil, false, nil
	}
	bundles, ok := normalizePolicyMap(root.Value)
	if !ok {
		return nil, false, fmt.Errorf("permission_bundles must be a mapping")
	}
	rawBundle, ok := bundles[bundle]
	if !ok {
		return nil, false, nil
	}
	bundleMap, ok := normalizePolicyMap(rawBundle)
	if !ok {
		return nil, false, fmt.Errorf("permission_bundles.%s must be a mapping", bundle)
	}
	rawPerms, ok := bundleMap["permissions"]
	if !ok {
		return nil, false, fmt.Errorf("permission_bundles.%s.permissions is required", bundle)
	}
	perms, err := stringsFromPolicyValue(rawPerms)
	if err != nil {
		return nil, false, fmt.Errorf("permission_bundles.%s.permissions: %w", bundle, err)
	}
	return dedupePermissionList(perms), true, nil
}

func normalizePolicyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case runtimecontracts.PolicyValue:
		return normalizePolicyMap(typed.Value)
	case map[string]any:
		return typed, true
	case map[string]runtimecontracts.PolicyValue:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item.Value
		}
		return out, true
	default:
		return nil, false
	}
}

func stringsFromPolicyValue(value any) ([]string, error) {
	switch typed := value.(type) {
	case runtimecontracts.PolicyValue:
		return stringsFromPolicyValue(typed.Value)
	case []string:
		return dedupePermissionList(typed), nil
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("expected string list")
			}
			out = append(out, text)
		}
		return dedupePermissionList(out), nil
	default:
		return nil, fmt.Errorf("expected list of strings")
	}
}

func knownPermissionNames(source semanticview.Source) map[string]struct{} {
	out := make(map[string]struct{}, len(defaultPlatformPermissions)+1)
	for _, perm := range defaultPlatformPermissions {
		perm = strings.TrimSpace(perm)
		if perm != "" {
			out[perm] = struct{}{}
		}
	}
	if source != nil {
		for _, perm := range source.PlatformSpec().PermissionsModel.Permissions {
			perm = strings.TrimSpace(perm)
			if perm != "" {
				out[perm] = struct{}{}
			}
		}
		for _, scope := range source.ProjectScopes() {
			collectPermissionBundleExtensions(out, scope.Policy)
		}
		for _, scope := range source.FlowScopes() {
			collectPermissionBundleExtensions(out, source.ResolvedPolicyForFlow(scope.ID))
		}
		if len(source.ProjectScopes()) == 0 && len(source.FlowScopes()) == 0 {
			collectPermissionBundleExtensions(out, source.ResolvedPolicyForFlow(""))
		}
	}
	out[systemAdminPermission] = struct{}{}
	return out
}

func collectPermissionBundleExtensions(out map[string]struct{}, policy runtimecontracts.PolicyDocument) {
	bundles, ok := policy.Values["permission_bundles"]
	if !ok {
		return
	}
	items, ok := normalizePolicyMap(bundles.Value)
	if !ok {
		return
	}
	for _, rawBundle := range items {
		bundleMap, ok := normalizePolicyMap(rawBundle)
		if !ok {
			continue
		}
		perms, err := stringsFromPolicyValue(bundleMap["permissions"])
		if err != nil {
			continue
		}
		for _, perm := range perms {
			perm = strings.TrimSpace(perm)
			if perm != "" {
				out[perm] = struct{}{}
			}
		}
	}
}

func dedupePermissionList(perms []string) []string {
	if len(perms) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(perms))
	out := make([]string, 0, len(perms))
	for _, perm := range perms {
		perm = strings.TrimSpace(perm)
		if perm == "" {
			continue
		}
		if _, ok := seen[perm]; ok {
			continue
		}
		seen[perm] = struct{}{}
		out = append(out, perm)
	}
	return out
}
