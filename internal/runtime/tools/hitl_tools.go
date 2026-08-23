package tools

import "strings"

const (
	NotifyHumanToolName        = "notify_human"
	AskHumanToolName           = "ask_human"
	WithheldAgentMessageTool   = "agent_message"
	NotifyHumanMailboxItemType = "operator_notice"
)

const agentMessageUnavailableTeaching = "not available: agent-to-agent messaging ships with typed recipient authority (#2154)"

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

func isWithheldAgentMessage(name string) bool {
	return strings.TrimSpace(name) == WithheldAgentMessageTool
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
