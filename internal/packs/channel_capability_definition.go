package packs

import (
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

var channelNativeOperations = []string{
	"install_inbox_entry", "read_inbox_entry", "install_shared_inbox_entry", "read_shared_inbox_entry",
	"identify_inbox_address", "read_inbox_launcher", "read_default_inbox_launcher",
}

func selectChannelCapabilityDefinition(definition runtimecontracts.PackInterfaceDefinition,
	capabilities CompiledChannelCapabilities, native bool) (runtimecontracts.PackInterfaceDefinition, error) {
	definition = cloneInterfaceDefinition(definition)
	for operation, capability := range map[string]ChannelCapability{
		"edit": ChannelCapabilityEdit, "acknowledge_interaction": ChannelCapabilityAcknowledgment,
	} {
		if capabilities.Require(capability) != nil {
			delete(definition.Operations, operation)
		}
	}
	if !native {
		for _, operation := range channelNativeOperations {
			delete(definition.Operations, operation)
		}
	}
	if !capabilities.Vector().ActionsAsButtons && !capabilities.Vector().Acknowledgment {
		delete(definition.Events, "action")
	}
	if !capabilities.Vector().ActionsAsButtons {
		for _, operation := range []string{"deliver", "edit"} {
			value, found := definition.Operations[operation]
			if found {
				delete(value.Input, "actions")
				definition.Operations[operation] = value
			}
		}
	}
	text, found := definition.Events["text"]
	if !found || text.OptionalFields["reply_to_message_reference"].Opaque != "delivery_reference" {
		return runtimecontracts.PackInterfaceDefinition{}, fmt.Errorf("text/reply baseline requires the exact optional delivery reply reference")
	}
	return definition, nil
}

func (p SatisfactionPlan) requireOperation(name string) error {
	switch name {
	case "deliver":
		return p.capabilities.Require(ChannelCapabilityCardRender)
	case "edit":
		return p.capabilities.Require(ChannelCapabilityEdit)
	case "acknowledge_interaction":
		return p.capabilities.Require(ChannelCapabilityAcknowledgment)
	default:
		for _, operation := range channelNativeOperations {
			if name == operation && !p.HasNativeInbox() {
				return &UnsupportedChannelNativeExtensionError{Operation: name}
			}
		}
		return nil
	}
}

type UnsupportedChannelNativeExtensionError struct {
	Operation string
}

func (e *UnsupportedChannelNativeExtensionError) Error() string {
	return fmt.Sprintf("channel native_inbox extension does not provide %q", e.Operation)
}

func (p OutboundBindingPlan) RequireOperation(name string) error {
	return p.structural.requireOperation(name)
}

func (p SatisfactionPlan) RequireExecutableProvider() error {
	if p.transport == ChannelTransportSession {
		return &operatorchannel.SessionProviderUnavailableError{Provider: p.Provider()}
	}
	return nil
}

func (p OutboundBindingPlan) RequireExecutableProvider() error {
	return p.structural.RequireExecutableProvider()
}
