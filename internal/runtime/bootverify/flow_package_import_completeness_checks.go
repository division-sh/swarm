package bootverify

import (
	"fmt"
	"path"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkFlowPackageImportCompleteness(c *checkerContext) []Finding {
	if c.flowPackageImportLoaded {
		return c.flowPackageImportFindings
	}
	c.flowPackageImportLoaded = true
	c.flowPackageImportFindings = flowPackageImportCompleteness(c.source)
	return c.flowPackageImportFindings
}

func checkFlowPackagePinBindAliasValidation(c *checkerContext) []Finding {
	var findings []Finding
	for _, issue := range semanticview.ImportBoundaryPinAliasIssues(c.source) {
		findings = append(findings, Finding{
			CheckID:  "flow_package_pin_bind_alias_validation",
			Severity: SeverityHardInvalidity,
			Message:  flowPackagePinBindAliasMessage(issue),
			Location: strings.TrimSpace(issue.ChildPackageKey),
		})
	}
	return findings
}

func checkFlowPackageDependencyBinding(c *checkerContext) []Finding {
	var findings []Finding
	for _, issue := range semanticview.ImportBoundaryDependencyIssues(c.source) {
		findings = append(findings, Finding{
			CheckID:  "flow_package_dependency_binding",
			Severity: SeverityHardInvalidity,
			Message:  flowPackageDependencyBindingMessage(issue),
			Location: strings.TrimSpace(issue.ChildPackageKey),
		})
	}
	return findings
}

func checkFlowPackageWildcardObserveGrant(c *checkerContext) []Finding {
	var findings []Finding
	for _, issue := range semanticview.ImportBoundaryWildcardGrantIssues(c.source) {
		findings = append(findings, Finding{
			CheckID:  "flow_package_wildcard_observe_grant",
			Severity: SeverityHardInvalidity,
			Message:  flowPackageWildcardObserveGrantMessage(issue),
			Location: strings.TrimSpace(issue.ChildPackageKey),
		})
	}
	for _, issue := range semanticview.ImportBoundaryWildcardSubscriptionIssues(c.source) {
		findings = append(findings, Finding{
			CheckID:  "flow_package_wildcard_observe_grant",
			Severity: SeverityHardInvalidity,
			Message:  flowPackageWildcardObserveGrantMessage(issue),
			Location: strings.TrimSpace(issue.ChildPackageKey),
		})
	}
	return findings
}

func flowPackageWildcardObserveGrantMessage(issue semanticview.ImportBoundaryWildcardIssue) string {
	source := strings.TrimSpace(issue.Source)
	if source == "" {
		source = "<empty>"
	}
	eventPattern := strings.TrimSpace(issue.EventPattern)
	if eventPattern == "" {
		eventPattern = "<empty>"
	}
	detail := strings.TrimSpace(issue.Message)
	if detail == "" {
		detail = strings.TrimSpace(issue.Kind)
	}
	return fmt.Sprintf("imported package %s observe grant from %s event %s via %s is invalid: %s", strings.TrimSpace(issue.ChildPackageKey), source, eventPattern, strings.TrimSpace(issue.ImportLabel), detail)
}

func flowPackageDependencyBindingMessage(issue semanticview.ImportBoundaryDependencyIssue) string {
	dependency := strings.TrimSpace(issue.Dependency)
	if dependency == "" {
		dependency = "<empty>"
	}
	message := strings.TrimSpace(issue.Message)
	if message == "" {
		message = strings.TrimSpace(issue.Kind)
	}
	ref := strings.TrimSpace(issue.Reference)
	if ref != "" {
		return fmt.Sprintf("imported package %s dependency %s binding %s via %s is invalid: %s", strings.TrimSpace(issue.ChildPackageKey), dependency, ref, strings.TrimSpace(issue.ImportLabel), message)
	}
	return fmt.Sprintf("imported package %s dependency %s via %s is invalid: %s", strings.TrimSpace(issue.ChildPackageKey), dependency, strings.TrimSpace(issue.ImportLabel), message)
}

func flowPackagePinBindAliasMessage(issue semanticview.ImportBoundaryPinAliasIssue) string {
	direction := strings.TrimSpace(issue.Direction)
	if direction == "" {
		direction = "pin"
	}
	pin := strings.TrimSpace(issue.Pin)
	if pin == "" {
		pin = "<empty>"
	}
	parentEvent := strings.TrimSpace(issue.ParentEvent)
	if parentEvent == "" {
		parentEvent = "<empty>"
	}
	detail := strings.TrimSpace(issue.Message)
	if detail == "" {
		detail = strings.TrimSpace(issue.Kind)
	}
	return fmt.Sprintf("imported package %s %s bind for pin %s to parent event %s is invalid: %s", strings.TrimSpace(issue.ChildPackageKey), direction, pin, parentEvent, detail)
}

func flowPackageImportCompleteness(source semanticview.Source) []Finding {
	scopes := source.ProjectScopes()
	if len(scopes) == 0 {
		return nil
	}
	scopeByKey := make(map[string]semanticview.ProjectScope, len(scopes))
	for _, scope := range scopes {
		key := normalizePackageKey(scope.Key)
		if key == "" {
			continue
		}
		scope.Key = key
		scopeByKey[key] = scope
	}

	var findings []Finding
	for _, parent := range scopes {
		parent.Key = normalizePackageKey(parent.Key)
		for _, importSite := range flowPackageImportSites(parent) {
			child, ok := scopeByKey[importSite.PackageKey]
			if !ok || flowPackageRequiresEmpty(child.Manifest.Requires) {
				continue
			}
			findings = append(findings, flowPackageImportCompletenessFindings(parent, child, importSite)...)
		}
	}
	return findings
}

type flowPackageImportSite struct {
	Kind       string
	PackageKey string
	Label      string
	Bind       runtimecontracts.FlowPackageBind
}

func flowPackageImportSites(scope semanticview.ProjectScope) []flowPackageImportSite {
	var sites []flowPackageImportSite
	for _, flow := range scope.Manifest.Flows {
		flowDir := strings.TrimSpace(flow.Flow)
		if flowDir == "" {
			continue
		}
		key := joinPackageKey(scope.Key, "flows", flowDir)
		sites = append(sites, flowPackageImportSite{
			Kind:       "flow",
			PackageKey: key,
			Label:      fmt.Sprintf("flow %s", strings.TrimSpace(flow.ID)),
			Bind:       flow.Bind,
		})
	}
	for _, ref := range scope.Manifest.ChildPackages() {
		location := importPackageLocation(ref)
		if location == "" {
			continue
		}
		key := joinPackageKey(scope.Key, location)
		sites = append(sites, flowPackageImportSite{
			Kind:       "package",
			PackageKey: key,
			Label:      fmt.Sprintf("package %s", location),
			Bind:       ref.Bind,
		})
	}
	return sites
}

func importPackageLocation(ref runtimecontracts.ProjectPackageRef) string {
	location := strings.TrimSpace(ref.ResolveLocation())
	if location == "" {
		return ""
	}
	location = strings.Trim(path.Clean(strings.ReplaceAll(location, "\\", "/")), "/")
	if strings.HasSuffix(strings.ToLower(location), ".yaml") {
		location = path.Dir(location)
	}
	if location == "." {
		return ""
	}
	return location
}

func flowPackageImportCompletenessFindings(parent, child semanticview.ProjectScope, site flowPackageImportSite) []Finding {
	requires := child.Manifest.Requires
	checks := []struct {
		kind     string
		required []string
		bindings map[string]string
		defaults map[string]runtimecontracts.PolicyValue
	}{
		{kind: "input", required: requires.Inputs, bindings: site.Bind.Inputs},
		{kind: "output", required: requires.Outputs, bindings: site.Bind.Outputs},
		{kind: "policy", required: requires.Policy, bindings: site.Bind.Policy, defaults: requires.PolicyDefaults},
		{kind: "credential", required: requires.Credentials, bindings: site.Bind.Credentials},
	}

	var findings []Finding
	for _, check := range checks {
		missing := missingFlowPackageBindings(check.required, check.bindings, check.defaults)
		if len(missing) == 0 {
			continue
		}
		findings = append(findings, Finding{
			CheckID:  "flow_package_import_completeness",
			Severity: SeverityHardInvalidity,
			Message:  fmt.Sprintf("imported package %s declares required %s bindings %s, but parent %s import site %s does not bind them", child.Key, check.kind, strings.Join(missing, ", "), parent.Key, site.Label),
			Location: child.Key,
		})
	}
	return findings
}

func missingFlowPackageBindings(required []string, bindings map[string]string, defaults map[string]runtimecontracts.PolicyValue) []string {
	out := make([]string, 0)
	for _, item := range required {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.TrimSpace(bindings[item]) == "" {
			if _, ok := defaults[item]; ok {
				continue
			}
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func flowPackageRequiresEmpty(requires runtimecontracts.FlowPackageRequires) bool {
	return len(requires.Inputs) == 0 &&
		len(requires.Outputs) == 0 &&
		len(requires.Policy) == 0 &&
		len(requires.Credentials) == 0 &&
		strings.TrimSpace(requires.PlatformVersion) == ""
}

func joinPackageKey(base string, segments ...string) string {
	parts := make([]string, 0, 1+len(segments))
	if base = normalizePackageKey(base); base != "" && base != "." {
		parts = append(parts, base)
	}
	for _, segment := range segments {
		segment = strings.Trim(path.Clean(strings.ReplaceAll(strings.TrimSpace(segment), "\\", "/")), "/")
		if segment == "" || segment == "." {
			continue
		}
		parts = append(parts, segment)
	}
	if len(parts) == 0 {
		return "."
	}
	return normalizePackageKey(path.Join(parts...))
}

func normalizePackageKey(key string) string {
	key = strings.Trim(path.Clean(strings.ReplaceAll(strings.TrimSpace(key), "\\", "/")), "/")
	if key == "" || key == "." {
		return "."
	}
	return key
}
