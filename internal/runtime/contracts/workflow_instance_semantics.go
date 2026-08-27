package contracts

import (
	"fmt"
	"strings"
)

// TemplateInstanceField is the admitted identity field for a template flow.
// Its representation remains private so routing code cannot manufacture an
// unchecked identity owner from a free string.
type TemplateInstanceField struct {
	value string
}

func ParseTemplateInstanceField(raw string) (TemplateInstanceField, error) {
	field := strings.TrimSpace(raw)
	switch {
	case field == "":
		return TemplateInstanceField{}, fmt.Errorf("template instance identity is required; use `instance: <field>`")
	case strings.Contains(field, "."):
		return TemplateInstanceField{}, fmt.Errorf("template instance identity %q must be one top-level field; use `instance: <field>`", field)
	default:
		return TemplateInstanceField{value: field}, nil
	}
}

func (f TemplateInstanceField) Empty() bool {
	return f.value == ""
}

func (f TemplateInstanceField) Path() string {
	return f.value
}

type FlowInputResolutionMode uint8

const (
	FlowInputResolutionModeNone FlowInputResolutionMode = iota
	FlowInputResolutionModeCreate
	FlowInputResolutionModeSelect
	FlowInputResolutionModeSelectOrCreate
	FlowInputResolutionModeFanIn
	FlowInputResolutionModeFanOut
	FlowInputResolutionModeReply
)

func ParseFlowInputResolutionMode(raw string) (FlowInputResolutionMode, error) {
	switch raw {
	case "create":
		return FlowInputResolutionModeCreate, nil
	case "select":
		return FlowInputResolutionModeSelect, nil
	case "select-or-create":
		return FlowInputResolutionModeSelectOrCreate, nil
	case "fan-in":
		return FlowInputResolutionModeFanIn, nil
	case "fan-out":
		return FlowInputResolutionModeFanOut, nil
	case "reply":
		return FlowInputResolutionModeReply, nil
	case "":
		return FlowInputResolutionModeNone, nil
	default:
		return FlowInputResolutionModeNone, fmt.Errorf("unsupported resolution mode %q", raw)
	}
}

// FlowInputResolutionModeCode is the explicit diagnostic/storage projection
// for the closed resolution variant. Routing behavior compares the variant.
func FlowInputResolutionModeCode(m FlowInputResolutionMode) string {
	switch m {
	case FlowInputResolutionModeCreate:
		return "create"
	case FlowInputResolutionModeSelect:
		return "select"
	case FlowInputResolutionModeSelectOrCreate:
		return "select-or-create"
	case FlowInputResolutionModeFanIn:
		return "fan-in"
	case FlowInputResolutionModeFanOut:
		return "fan-out"
	case FlowInputResolutionModeReply:
		return "reply"
	default:
		return ""
	}
}

func (m FlowInputResolutionMode) Valid() bool {
	return m >= FlowInputResolutionModeCreate && m <= FlowInputResolutionModeReply
}
