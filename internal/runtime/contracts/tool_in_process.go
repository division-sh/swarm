package contracts

import "fmt"

// ToolInProcessTarget names a shipped provider operation, not Go code or a
// callback. It grants intent only; the selected connection/effect owners admit execution.
type ToolInProcessTarget uint8

const (
	ToolInProcessUnspecified ToolInProcessTarget = iota
	ToolInProcessWhatsAppReadAccount
	ToolInProcessWhatsAppSendText
)

func (t ToolInProcessTarget) String() string {
	switch t {
	case ToolInProcessWhatsAppReadAccount:
		return "whatsapp.read_account"
	case ToolInProcessWhatsAppSendText:
		return "whatsapp.send_text"
	default:
		return ""
	}
}

func ParseToolInProcessTarget(raw string) (ToolInProcessTarget, error) {
	switch raw {
	case "whatsapp.read_account":
		return ToolInProcessWhatsAppReadAccount, nil
	case "whatsapp.send_text":
		return ToolInProcessWhatsAppSendText, nil
	default:
		return ToolInProcessUnspecified, fmt.Errorf("in_process must name a shipped target: whatsapp.read_account or whatsapp.send_text; got %q", raw)
	}
}

func (t ToolInProcessTarget) Provider() string {
	switch t {
	case ToolInProcessWhatsAppReadAccount, ToolInProcessWhatsAppSendText:
		return "whatsapp"
	default:
		return ""
	}
}

func (t ToolInProcessTarget) contract() (ToolCategory, ActivityEffectClass) {
	switch t {
	case ToolInProcessWhatsAppReadAccount:
		return ToolCategoryProviderRegistration, ActivityEffectClassReadOnly
	case ToolInProcessWhatsAppSendText:
		return ToolCategoryProviderConnector, ActivityEffectClassNonIdempotentWrite
	default:
		return ToolCategoryUnspecified, ""
	}
}

func WithToolInProcessTarget(target ToolInProcessTarget) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		if target.String() == "" {
			return fmt.Errorf("in_process requires an exact shipped provider target")
		}
		draft.value.inProcess = target
		return nil
	})
}

func (e ToolSchemaEntry) InProcess() (ToolInProcessTarget, bool) {
	if e.value == nil || e.value.inProcess == ToolInProcessUnspecified {
		return ToolInProcessUnspecified, false
	}
	return e.value.inProcess, true
}

func (e ToolSchemaEntry) validateInProcess() error {
	target, present := e.InProcess()
	if !present {
		if e.value.handler == ToolHandlerInProcess {
			return fmt.Errorf("handler_type in_process requires its exact in_process target")
		}
		return nil
	}
	if e.value.handler != ToolHandlerInProcess {
		return fmt.Errorf("in_process target requires handler_type in_process")
	}
	category, effect := target.contract()
	if target.String() == "" || e.value.category != category || e.value.effect != effect {
		return fmt.Errorf("in_process %s requires category %s and effect_class %s", target.String(), category.String(), effect)
	}
	if len(e.value.credentials) != 0 {
		return fmt.Errorf("in_process %s uses its exact admitted session, not authored credential substitutes", target.String())
	}
	return nil
}
