package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func retiredHandlerActionFieldError(context, key string) error {
	switch strings.TrimSpace(key) {
	case "action", "evidence_target", "template", "instance_id_from", "config_from":
		return fmt.Errorf("RETIRED-HANDLER-ACTION: %s field %q is retired, including null or empty values; remove authored actions and their options; use declarative emit/connect with receiver input initialize for creation, typed data_accumulation for state writes, or supported activities; emit template specialization and timer actions remain supported", context, key)
	default:
		return nil
	}
}

// Inspect presence before decoding values; YAML aliases and merged mappings
// cannot erase a retired declaration, even when another key overrides it.
func validateRetiredHandlerActionFields(node *yaml.Node, context string) error {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	if resolved == nil || resolved.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(resolved.Content); i += 2 {
		key, err := resolveHandlerRuleYAMLNode(resolved.Content[i])
		if err != nil {
			return err
		}
		if err := retiredHandlerActionFieldError(context, key.Value); err != nil {
			return err
		}
		if key.Value != "<<" || key.Tag != "!!merge" {
			continue
		}
		merged, err := resolveHandlerRuleYAMLNode(resolved.Content[i+1])
		if err != nil {
			return err
		}
		if merged.Kind == yaml.SequenceNode {
			for _, entry := range merged.Content {
				if err := validateRetiredHandlerActionFields(entry, context); err != nil {
					return err
				}
			}
		} else if err := validateRetiredHandlerActionFields(merged, context); err != nil {
			return err
		}
	}
	return nil
}
