package events

import (
	"fmt"
	"strings"
)

const EventContextRoot = "event"

func EventContextFieldSupported(field string) bool {
	switch strings.TrimSpace(field) {
	case "id",
		"type",
		"trigger_event_type",
		"source_agent",
		"task_id",
		"source",
		"target",
		"target_set",
		"source_event_id",
		"emitted_at",
		"current_state",
		"run_id",
		"scope":
		return true
	default:
		return false
	}
}

func EventContextFieldUnsupported(field string) bool {
	switch strings.TrimSpace(field) {
	case "entity_id", "flow_instance":
		return true
	default:
		return false
	}
}

func ValidateEventContextReference(ref string) error {
	ref = strings.Trim(strings.TrimSpace(ref), ".")
	if ref == "" {
		return nil
	}
	field, tail, _ := strings.Cut(ref, ".")
	field = strings.TrimSpace(field)
	switch field {
	case "entity_id":
		return fmt.Errorf("event.entity_id is unsupported on handler expression surfaces; use _entity.id for current receiver context or event.source.entity_id/event.target.entity_id for route identity")
	case "flow_instance":
		return fmt.Errorf("event.flow_instance is unsupported on handler expression surfaces; use _entity.flow_instance for current receiver context or event.source.flow_instance/event.target.flow_instance for route identity")
	case "source", "target":
		return validateRouteIdentityReference(field, tail)
	case "target_set":
		if strings.TrimSpace(tail) != "" {
			return fmt.Errorf("event.target_set is a list of route identities and does not support dotted field %q", tail)
		}
		return nil
	default:
		if EventContextFieldSupported(field) {
			if strings.TrimSpace(tail) != "" {
				return fmt.Errorf("event.%s is a platform scalar and does not support nested path %q", field, ref)
			}
			return nil
		}
		return fmt.Errorf("event.%s is not a supported handler event context field", field)
	}
}

func validateRouteIdentityReference(root, tail string) error {
	tail = strings.Trim(tail, ".")
	if tail == "" {
		return nil
	}
	field, rest, _ := strings.Cut(tail, ".")
	field = strings.TrimSpace(field)
	switch field {
	case "entity_id", "flow_instance", "flow_id":
		if strings.TrimSpace(rest) != "" {
			return fmt.Errorf("event.%s.%s is a route identity scalar and does not support nested path %q", root, field, tail)
		}
		return nil
	default:
		return fmt.Errorf("event.%s.%s is not a supported route identity field", root, field)
	}
}
