package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	NotifyHumanToolName        = "notify_human"
	AskHumanToolName           = "ask_human"
	WithheldAgentMessageTool   = "agent_message"
	NotifyHumanMailboxItemType = "operator_notice"
)

const agentMessageUnavailableTeaching = "not available: agent-to-agent messaging ships with typed recipient authority (#2154)"

type hitlIdentityLifecycle uint8

const (
	hitlIdentityActive hitlIdentityLifecycle = iota + 1
	hitlIdentityWithheld
	hitlIdentityRetired
)

type hitlIdentityLifecycleDescriptor struct {
	name        string
	lifecycle   hitlIdentityLifecycle
	replacement string
}

func hitlIdentityLifecycleForName(name string) (hitlIdentityLifecycleDescriptor, bool) {
	switch toolidentity.CanonicalName(name) {
	case NotifyHumanToolName:
		return hitlIdentityLifecycleDescriptor{name: NotifyHumanToolName, lifecycle: hitlIdentityActive}, true
	case AskHumanToolName:
		return hitlIdentityLifecycleDescriptor{name: AskHumanToolName, lifecycle: hitlIdentityActive}, true
	case WithheldAgentMessageTool:
		return hitlIdentityLifecycleDescriptor{name: WithheldAgentMessageTool, lifecycle: hitlIdentityWithheld}, true
	case "mailbox_send":
		return hitlIdentityLifecycleDescriptor{name: "mailbox_send", lifecycle: hitlIdentityRetired, replacement: NotifyHumanToolName}, true
	case "human_task_request":
		return hitlIdentityLifecycleDescriptor{name: "human_task_request", lifecycle: hitlIdentityRetired, replacement: AskHumanToolName}, true
	default:
		return hitlIdentityLifecycleDescriptor{}, false
	}
}

func hitlIdentityReferenceError(name, location string) error {
	descriptor, ok := hitlIdentityLifecycleForName(name)
	if !ok || descriptor.lifecycle == hitlIdentityActive {
		return nil
	}
	location = strings.TrimSpace(location)
	if location == "" {
		location = "tool reference"
	}
	switch descriptor.lifecycle {
	case hitlIdentityWithheld:
		return fmt.Errorf("%s: tool %s %s", location, descriptor.name, agentMessageUnavailableTeaching)
	case hitlIdentityRetired:
		return fmt.Errorf("%s: RETIRED: %s is unsupported; use %s", location, descriptor.name, descriptor.replacement)
	default:
		return nil
	}
}

func hitlIdentityDefinitionError(name, location string) error {
	descriptor, ok := hitlIdentityLifecycleForName(name)
	if !ok {
		return nil
	}
	if descriptor.lifecycle != hitlIdentityActive {
		return hitlIdentityReferenceError(name, location)
	}
	return fmt.Errorf("%s: tool %s is owned by the platform HITL contract and cannot be redefined", strings.TrimSpace(location), descriptor.name)
}

func hitlIdentityMergeError(name, owner string, canonicalPlatformBuiltin bool) error {
	descriptor, ok := hitlIdentityLifecycleForName(name)
	if !ok {
		return nil
	}
	if descriptor.lifecycle != hitlIdentityActive {
		return hitlIdentityReferenceError(name, strings.TrimSpace(owner))
	}
	if canonicalPlatformBuiltin {
		return nil
	}
	return fmt.Errorf("tool %s is owned by the platform HITL contract and cannot be redefined by %s", descriptor.name, strings.TrimSpace(owner))
}

func hitlIdentityExecutionError(name string) error {
	return hitlIdentityReferenceError(name, "tool execution")
}

// ValidateHITLIdentityLifecycleReferences rejects declarations and references
// from every authored scope, including scopes with no current agent.
func ValidateHITLIdentityLifecycleReferences(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	type authoredScope struct {
		label  string
		tools  map[string]runtimecontracts.ToolSchemaEntry
		policy runtimecontracts.PolicyDocument
	}
	scopes := []authoredScope{{label: "root", tools: source.ToolEntries(), policy: source.ResolvedPolicyForFlow("")}}
	for _, flow := range semanticview.FlowScopes(source) {
		scopes = append(scopes, authoredScope{
			label:  "flow " + strings.TrimSpace(flow.ID),
			tools:  flow.Tools,
			policy: flow.Policy,
		})
	}

	errs := make([]error, 0)
	seen := map[string]struct{}{}
	add := func(err error) {
		if err == nil {
			return
		}
		key := err.Error()
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		errs = append(errs, err)
	}
	for _, declaration := range semanticview.AgentDeclarations(source) {
		label := declaration.Label(true)
		if strings.TrimSpace(declaration.ScopeKind) == "" {
			label = "root agent " + strings.TrimSpace(declaration.LocalID)
		}
		for _, name := range declaration.Entry.ConfiguredTools() {
			add(hitlIdentityReferenceError(name, label+" tools"))
		}
		for _, name := range declaration.Entry.Permissions {
			add(hitlIdentityReferenceError(name, label+" permissions"))
		}
	}
	for _, scope := range scopes {
		for name := range scope.tools {
			add(hitlIdentityDefinitionError(name, scope.label+" tool entry"))
		}
		for bundle, names := range permissionBundles(scope.policy) {
			for _, name := range names {
				add(hitlIdentityReferenceError(name, fmt.Sprintf("%s permission_bundles.%s.permissions", scope.label, bundle)))
			}
		}
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return errs
}

type hitlGrantKind uint8

const (
	hitlGrantUniversal hitlGrantKind = iota + 1
	hitlGrantPermission
)

type hitlToolDescriptor struct {
	name               string
	description        string
	usage              string
	grant              hitlGrantKind
	requiredPermission string
	draft              func() builtinToolDraft
}

func managedHITLToolDescriptors() []hitlToolDescriptor {
	return []hitlToolDescriptor{
		{
			name:        NotifyHumanToolName,
			description: "Sends an informational notice to the human operator. Does NOT request approval and does not pause the flow - to ask for a decision that gates the flow, use ask_human.",
			usage:       "Use for an informational operator notice only. Provide summary and optional context. The flow continues without waiting for a reply; use ask_human when a human verdict must gate the flow.",
			grant:       hitlGrantUniversal,
			draft:       notifyHumanContractSchema,
		},
		{
			name:               AskHumanToolName,
			description:        "Create a typed decision card when admitted work requires a human verdict.",
			usage:              "Create a typed human decision card only when human input is required. Provide an explicit entity, flow, or global scope and the single deadline_at spelling when overriding the stamped expiry policy.",
			grant:              hitlGrantPermission,
			requiredPermission: AskHumanToolName,
			draft:              askHumanContractSchema,
		},
	}
}

func hitlToolDescriptorForName(name string) (hitlToolDescriptor, bool) {
	name = strings.TrimSpace(name)
	for _, descriptor := range managedHITLToolDescriptors() {
		if descriptor.name == name {
			return descriptor, true
		}
	}
	return hitlToolDescriptor{}, false
}

func hitlRuntimeContractSchemas() map[string]builtinToolDraft {
	out := make(map[string]builtinToolDraft, len(managedHITLToolDescriptors()))
	for _, descriptor := range managedHITLToolDescriptors() {
		draft := descriptor.draft()
		draft.Description = descriptor.description
		out[descriptor.name] = draft
	}
	return out
}

func requiredPermissionForTool(name string) (string, bool) {
	if descriptor, ok := hitlToolDescriptorForName(name); ok {
		if descriptor.grant != hitlGrantPermission {
			return "", false
		}
		return descriptor.requiredPermission, true
	}
	required, ok := toolPermissionRequirements[strings.TrimSpace(name)]
	return required, ok
}
