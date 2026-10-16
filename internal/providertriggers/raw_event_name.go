package providertriggers

import (
	"fmt"
	"strings"
)

func validateRawEventName(provider string, name EventNameManifest) error {
	literal := strings.TrimSpace(name.Literal)
	template := strings.TrimSpace(name.Template)
	if literal == "" && template == "" {
		return fmt.Errorf("%s manifest event_name is required", provider)
	}
	if literal != "" && template != "" {
		return fmt.Errorf("%s manifest event_name must use literal or template, not both", provider)
	}
	if template == "" {
		return nil
	}
	prefix := "inbound." + provider + "."
	switch name.Template {
	case prefix + "{event_type}", prefix + "raw.{event_type}":
		return nil
	default:
		return fmt.Errorf("%s manifest event_name.template must use the provider-local {event_type} or raw.{event_type} policy under its exact directory key; got %q", provider, name.Template)
	}
}
