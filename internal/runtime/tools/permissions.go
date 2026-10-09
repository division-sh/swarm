package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var defaultPlatformPermissions = []string{
	"approve_spend",
	"configure_routing",
	"create_flow_instance",
	"ask_human",
	"schedule",
}

var toolPermissionRequirements = map[string]string{
	"configure_routing": "configure_routing",
	"schedule":          "schedule",
}

func isRetiredMessagePermission(name string) bool {
	switch strings.TrimSpace(name) {
	case "message_flow", "message_peers":
		return true
	default:
		return false
	}
}

func permissionReferenceError(name, location string) error {
	if isRetiredMessagePermission(name) {
		return fmt.Errorf("%s: permission %q is unsupported", location, strings.TrimSpace(name))
	}
	return hitlIdentityReferenceError(name, location)
}

// Vocabulary membership cannot re-enable a retired permission. Authored use
// is rejected separately by source admission and permission expansion.
func addKnownPermission(out map[string]struct{}, name string) {
	name = strings.TrimSpace(name)
	if name != "" && !isRetiredMessagePermission(name) {
		out[name] = struct{}{}
	}
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
	agents, errs := scopedAgentEntries(source)
	errs = append(errs, ValidateRetiredDynamicAgentToolReferences(source)...)
	if lifecycleErrors := ValidateHITLIdentityLifecycleReferences(source); len(lifecycleErrors) > 0 {
		errs = append(errs, lifecycleErrors...)
		return len(agents), errs
	}
	known, err := knownPermissionNames(source)
	if err != nil {
		return len(agents), append(errs, err)
	}
	for _, agent := range agents {
		perms, err := ResolveAgentPermissions(source, agent.flowID, agent.entry)
		if err != nil {
			errs = append(errs, fmt.Errorf("agent %s: %w", agent.id, err))
			continue
		}
		cfg := models.AgentConfig{
			ID:          agent.id,
			Role:        agent.role,
			Permissions: perms,
		}
		for _, perm := range perms {
			if IsRetiredDynamicAgentToolName(perm) {
				continue
			}
			if _, ok := known[perm]; ok {
				continue
			}
			errs = append(errs, fmt.Errorf("agent %s declares unknown permission %q", agent.id, perm))
		}
		for _, toolName := range agent.entry.ConfiguredTools() {
			toolName = strings.TrimSpace(toolName)
			if err := agentModuleGrantError(source, agent.flowID, toolName); err != nil {
				errs = append(errs, fmt.Errorf("agent %s: %w", agent.id, err))
				continue
			}
			if IsRetiredDynamicAgentToolName(toolName) {
				continue
			}
			requiredPerm, ok := requiredPermissionForTool(toolName)
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

func agentModuleGrantError(source semanticview.Source, flowID, name string) error {
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil
	}
	if tool, found := bundle.ToolEntryForFlow(flowID, name); found && !tool.AgentExposable() {
		if tool.Handler() == runtimecontracts.ToolHandlerInProcess {
			return fmt.Errorf("declares native provider tool %s; provider operations are private activities", name)
		}
		return fmt.Errorf("declares module tool %s; modules are compute_module-only", name)
	}
	return nil
}

type scopedAgentEntry struct {
	id     string
	role   string
	flowID string
	entry  runtimecontracts.AgentRegistryEntry
}

func scopedAgentEntries(source semanticview.Source) ([]scopedAgentEntry, []error) {
	if source == nil {
		return nil, nil
	}
	entries := make([]scopedAgentEntry, 0)
	var errs []error
	for _, declaration := range semanticview.AgentDeclarations(source) {
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		entries = append(entries, scopedAgentEntry{
			id:     plan.AgentID,
			role:   plan.EffectiveRole(declaration.Entry),
			flowID: plan.OwnerFlowID,
			entry:  declaration.Entry,
		})
	}
	return entries, errs
}

func resolveAgentPermissionsFromPolicy(entry runtimecontracts.AgentRegistryEntry, policy runtimecontracts.PolicyDocument) ([]string, error) {
	bundles, err := permissionBundles(policy)
	if err != nil {
		return nil, err
	}
	perms := make([]string, 0, len(entry.Permissions)+4)
	bundleName := strings.TrimSpace(entry.PermissionsBundle)
	if bundleName != "" {
		bundlePerms, ok := bundles[bundleName]
		if !ok {
			return nil, fmt.Errorf("unknown permissions_bundle %q", bundleName)
		}
		for _, permission := range bundlePerms {
			if err := permissionReferenceError(permission, fmt.Sprintf("permission_bundles.%s.permissions", bundleName)); err != nil {
				return nil, err
			}
		}
		perms = append(perms, bundlePerms...)
	}
	for _, permission := range entry.Permissions {
		if err := permissionReferenceError(permission, "permissions"); err != nil {
			return nil, err
		}
	}
	perms = append(perms, entry.Permissions...)
	return dedupePermissionList(perms), nil
}

// Every declaration is checked, including unused bundles and empty-agent scopes.
func permissionBundles(policy runtimecontracts.PolicyDocument) (map[string][]string, error) {
	root, ok := policy.Values["permission_bundles"]
	if !ok {
		return nil, nil
	}
	bundles, ok := normalizePolicyMap(root.Value)
	if !ok {
		return nil, fmt.Errorf("permission_bundles must be a mapping")
	}
	names := make([]string, 0, len(bundles))
	for name := range bundles {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make(map[string][]string, len(bundles))
	for _, name := range names {
		bundle, ok := normalizePolicyMap(bundles[name])
		if !ok {
			return nil, fmt.Errorf("permission_bundles.%s must be a mapping", name)
		}
		raw, ok := bundle["permissions"]
		if !ok {
			return nil, fmt.Errorf("permission_bundles.%s.permissions is required", name)
		}
		permissions, err := stringsFromPolicyValue(raw)
		if err != nil {
			return nil, fmt.Errorf("permission_bundles.%s.permissions: %w", name, err)
		}
		out[name] = permissions
	}
	return out, nil
}

func normalizePolicyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	default:
		return nil, false
	}
}

func stringsFromPolicyValue(value any) ([]string, error) {
	switch typed := value.(type) {
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

func knownPermissionNames(source semanticview.Source) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(defaultPlatformPermissions)+8)
	for _, perm := range defaultPlatformPermissions {
		addKnownPermission(out, perm)
	}
	if source != nil {
		for _, perm := range source.PlatformSpec().PermissionsModel.Permissions {
			addKnownPermission(out, perm)
		}
		for _, scope := range source.FlowScopes() {
			if err := collectPermissionBundleExtensions(out, source.ResolvedPolicyForFlow(scope.ID)); err != nil {
				return nil, fmt.Errorf("flow %s %w", scope.ID, err)
			}
		}
		if len(source.FlowScopes()) == 0 {
			if err := collectPermissionBundleExtensions(out, source.ResolvedPolicyForFlow("")); err != nil {
				return nil, fmt.Errorf("root %w", err)
			}
		}
		collectToolPermissionExtensions(out, source)
	}
	return out, nil
}

func collectToolPermissionExtensions(out map[string]struct{}, source semanticview.Source) {
	if source == nil {
		return
	}
	for _, entry := range source.ToolEntries() {
		addKnownPermission(out, entry.Permission().String())
	}
}

func collectPermissionBundleExtensions(out map[string]struct{}, policy runtimecontracts.PolicyDocument) error {
	bundles, err := permissionBundles(policy)
	if err != nil {
		return err
	}
	for _, perms := range bundles {
		for _, perm := range perms {
			addKnownPermission(out, perm)
		}
	}
	return nil
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
