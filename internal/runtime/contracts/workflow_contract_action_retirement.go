package contracts

import (
	"fmt"
	"strings"
)

func retiredHandlerActionFieldError(context, key string) error {
	switch context {
	case "handler", "rule", "rules", "on_complete", "on_success", "join", "join.on_complete", "join.on_deadline":
	default:
		return nil
	}
	switch strings.TrimSpace(key) {
	case "condition":
		if context == "rules" {
			return fmt.Errorf("RETIRED-POLICY-SHEET-ROW: handler.rules.condition is retired; use when for a predicate or else: true for a default row")
		}
		return nil
	case "action", "evidence_target", "template", "instance_id_from", "config_from":
		return fmt.Errorf("RETIRED-HANDLER-ACTION: %s field %q is retired, including null or empty values; remove authored actions and their options; use declarative emit/connect with receiver input initialize for creation, typed data_accumulation for state writes, or supported activities; emit template specialization and timer actions remain supported", context, key)
	default:
		return nil
	}
}
