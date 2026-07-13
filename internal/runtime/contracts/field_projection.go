package contracts

import "strings"

const FieldProjectionConvertNumberToText = "number_to_text"

// FieldProjection is the shared declaration model for a typed field selected
// from another value. Consumers decide which optional/conversion combinations
// are legal for their semantic context.
type FieldProjection struct {
	From     string `yaml:"from" json:"from"`
	Type     string `yaml:"type" json:"type"`
	Optional bool   `yaml:"optional,omitempty" json:"optional,omitempty"`
	Convert  string `yaml:"convert,omitempty" json:"convert,omitempty"`
}

func (p FieldProjection) Normalized() FieldProjection {
	return FieldProjection{
		From:     strings.TrimSpace(p.From),
		Type:     strings.TrimSpace(p.Type),
		Optional: p.Optional,
		Convert:  strings.ToLower(strings.TrimSpace(p.Convert)),
	}
}

func ValidateWave1TypeReference(raw, context string) error {
	return validateWave1TypeRef(raw, context)
}
