package packs

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type ChannelCapability string

const (
	ChannelCapabilityCardRender       ChannelCapability = "card_render"
	ChannelCapabilityReplyToReference ChannelCapability = "reply_to_reference"
	ChannelCapabilityActionsAsButtons ChannelCapability = "actions_as_buttons"
	ChannelCapabilityActionsAsText    ChannelCapability = "actions_as_text"
	ChannelCapabilityEdit             ChannelCapability = "edit"
	ChannelCapabilityAcknowledgment   ChannelCapability = "acknowledgment"
	ChannelCapabilityInboxListing     ChannelCapability = "inbox_listing"
)

func ChannelCapabilityNames() []ChannelCapability {
	return []ChannelCapability{
		ChannelCapabilityCardRender, ChannelCapabilityReplyToReference,
		ChannelCapabilityActionsAsButtons, ChannelCapabilityActionsAsText,
		ChannelCapabilityEdit, ChannelCapabilityAcknowledgment, ChannelCapabilityInboxListing,
	}
}

func ChannelTextReplyBaselineCapabilities() []ChannelCapability {
	return []ChannelCapability{
		ChannelCapabilityCardRender, ChannelCapabilityReplyToReference,
		ChannelCapabilityActionsAsText, ChannelCapabilityInboxListing,
	}
}

// ChannelCapabilityVector is input/readback data, not admitted authority.
type ChannelCapabilityVector struct {
	CardRender       bool `json:"card_render" yaml:"card_render"`
	ReplyToReference bool `json:"reply_to_reference" yaml:"reply_to_reference"`
	ActionsAsButtons bool `json:"actions_as_buttons" yaml:"actions_as_buttons"`
	ActionsAsText    bool `json:"actions_as_text" yaml:"actions_as_text"`
	Edit             bool `json:"edit" yaml:"edit"`
	Acknowledgment   bool `json:"acknowledgment" yaml:"acknowledgment"`
	InboxListing     bool `json:"inbox_listing" yaml:"inbox_listing"`
}

type CompiledChannelCapabilities struct {
	vector ChannelCapabilityVector
}

type RequiredChannelBaselineCapabilityError struct {
	Capability ChannelCapability
}

func (e *RequiredChannelBaselineCapabilityError) Error() string {
	return fmt.Sprintf("channel text/reply baseline requires capability %q", e.Capability)
}

func CompileChannelCapabilities(vector ChannelCapabilityVector) (CompiledChannelCapabilities, error) {
	for _, capability := range ChannelTextReplyBaselineCapabilities() {
		if supported, _ := vector.supports(capability); !supported {
			return CompiledChannelCapabilities{}, &RequiredChannelBaselineCapabilityError{Capability: capability}
		}
	}
	return CompiledChannelCapabilities{vector: vector}, nil
}

func AdmitChannelCapabilities(value yamlsource.Value) (CompiledChannelCapabilities, error) {
	if err := value.ValidateExpansion(); err != nil {
		return CompiledChannelCapabilities{}, err
	}
	names := ChannelCapabilityNames()
	allowed := make([]string, len(names))
	for i, name := range names {
		allowed[i] = string(name)
	}
	fields, err := channelFields(value, allowed...)
	if err != nil {
		return CompiledChannelCapabilities{}, err
	}
	var values [7]bool
	for i, capability := range names {
		field, err := channelRequired(value, fields, string(capability))
		if err != nil {
			return CompiledChannelCapabilities{}, err
		}
		scalar, err := field.Scalar()
		if err != nil || scalar.Tag != "!!bool" {
			return CompiledChannelCapabilities{}, channelError(field, "channel capability must be an explicit boolean")
		}
		if err := field.Project(&values[i]); err != nil {
			return CompiledChannelCapabilities{}, channelError(field, "invalid channel capability boolean")
		}
	}
	vector := ChannelCapabilityVector{
		CardRender: values[0], ReplyToReference: values[1], ActionsAsButtons: values[2],
		ActionsAsText: values[3], Edit: values[4], Acknowledgment: values[5], InboxListing: values[6],
	}
	compiled, err := CompileChannelCapabilities(vector)
	if err != nil {
		if missing, ok := err.(*RequiredChannelBaselineCapabilityError); ok {
			field := fields[string(missing.Capability)]
			return CompiledChannelCapabilities{}, fmt.Errorf("%s at %s: %w", field.SemanticPath(), field.Location(), err)
		}
		return CompiledChannelCapabilities{}, err
	}
	return compiled, nil
}

func (c CompiledChannelCapabilities) Vector() ChannelCapabilityVector { return c.vector }

type UnsupportedChannelCapabilityError struct {
	Capability ChannelCapability
}

func (e *UnsupportedChannelCapabilityError) Error() string {
	return fmt.Sprintf("channel capability %q is unsupported", e.Capability)
}

func (c CompiledChannelCapabilities) Require(capability ChannelCapability) error {
	if _, err := CompileChannelCapabilities(c.vector); err != nil {
		return err
	}
	supported, err := c.vector.supports(capability)
	if err != nil {
		return err
	}
	if !supported {
		return &UnsupportedChannelCapabilityError{Capability: capability}
	}
	return nil
}

func (c CompiledChannelCapabilities) MarshalJSON() ([]byte, error) {
	if _, err := CompileChannelCapabilities(c.vector); err != nil {
		return nil, err
	}
	return canonicaljson.Bytes(c.vector)
}

func (*CompiledChannelCapabilities) UnmarshalJSON([]byte) error {
	return fmt.Errorf("channel capabilities require source or typed admission")
}

func (v ChannelCapabilityVector) supports(capability ChannelCapability) (bool, error) {
	switch capability {
	case ChannelCapabilityCardRender:
		return v.CardRender, nil
	case ChannelCapabilityReplyToReference:
		return v.ReplyToReference, nil
	case ChannelCapabilityActionsAsButtons:
		return v.ActionsAsButtons, nil
	case ChannelCapabilityActionsAsText:
		return v.ActionsAsText, nil
	case ChannelCapabilityEdit:
		return v.Edit, nil
	case ChannelCapabilityAcknowledgment:
		return v.Acknowledgment, nil
	case ChannelCapabilityInboxListing:
		return v.InboxListing, nil
	default:
		return false, fmt.Errorf("unknown channel capability %q", capability)
	}
}
