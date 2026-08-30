package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var retiredDynamicAgentToolNames = map[string]struct{}{
	"agent_hire":        {},
	"agent_fire":        {},
	"agent_reconfigure": {},
}

// IsRetiredDynamicAgentToolName is the single lexical owner for the removed
// provider-facing topology mutation family. It intentionally rejects exact
// names only; internal lifecycle facts such as agent_reconfigured are distinct.
func IsRetiredDynamicAgentToolName(name string) bool {
	_, ok := retiredDynamicAgentToolNames[strings.TrimSpace(name)]
	return ok
}

func retiredDynamicAgentToolError(name, location string) error {
	return fmt.Errorf(
		"%s: RETIRED: %s is unsupported; declare managed agents in agents.yaml, let flow lifecycle/readiness own residency and teardown, and use typed fan-out for per-item work",
		strings.TrimSpace(location),
		strings.TrimSpace(name),
	)
}

// ValidateRetiredDynamicAgentToolReferences rejects every authored spelling
// before handler-class interpretation or runtime inventory projection.
func ValidateRetiredDynamicAgentToolReferences(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	type authoredScope struct {
		label  string
		tools  map[string]runtimecontracts.ToolSchemaEntry
		policy runtimecontracts.PolicyDocument
	}

	scopes := []authoredScope{{
		label:  "root",
		tools:  source.ToolEntries(),
		policy: source.ResolvedPolicyForFlow(""),
	}}
	for _, flow := range semanticview.FlowScopes(source) {
		scopes = append(scopes, authoredScope{
			label:  "flow " + strings.TrimSpace(flow.ID),
			tools:  flow.Tools,
			policy: flow.Policy,
		})
	}

	errs := make([]error, 0)
	seen := map[string]struct{}{}
	add := func(name, location string) {
		name = strings.TrimSpace(name)
		location = strings.TrimSpace(location)
		if !IsRetiredDynamicAgentToolName(name) {
			return
		}
		key := location + "\x00" + name
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		errs = append(errs, retiredDynamicAgentToolError(name, location))
	}

	for _, declaration := range semanticview.AgentDeclarations(source) {
		label := declaration.Label(true)
		if strings.TrimSpace(declaration.ScopeKind) == "" {
			label = "root agent " + strings.TrimSpace(declaration.LocalID)
		}
		for _, name := range declaration.Entry.ConfiguredTools() {
			add(name, label+" tools")
		}
		for _, name := range declaration.Entry.Permissions {
			add(name, label+" permissions")
		}
	}
	for _, scope := range scopes {
		for name := range scope.tools {
			add(name, scope.label+" tool entry")
		}
		for bundle, names := range permissionBundles(scope.policy) {
			for _, name := range names {
				add(name, fmt.Sprintf("%s permission_bundles.%s.permissions", scope.label, bundle))
			}
		}
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return errs
}

func permissionBundles(policy runtimecontracts.PolicyDocument) map[string][]string {
	root, ok := policy.Values["permission_bundles"]
	if !ok {
		return nil
	}
	bundles, ok := normalizePolicyMap(root.Value)
	if !ok {
		return nil
	}
	out := make(map[string][]string, len(bundles))
	for name, raw := range bundles {
		bundle, ok := normalizePolicyMap(raw)
		if !ok {
			continue
		}
		rawPermissions, ok := bundle["permissions"]
		if !ok {
			continue
		}
		permissions, err := stringsFromPolicyValue(rawPermissions)
		if err != nil {
			continue
		}
		out[strings.TrimSpace(name)] = permissions
	}
	return out
}
