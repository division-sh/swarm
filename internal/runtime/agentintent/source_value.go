package agentintent

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// AdmitSource consumes the owning declaration's scoped syntax, retaining exact
// inline bytes and refusing coercion before semantic source resolution.
func AdmitSource(value yamlsource.Value) (Source, error) {
	text := func(value yamlsource.Value) (string, error) {
		scalar, err := value.Scalar()
		if err != nil {
			return "", err
		}
		if scalar.Tag != "!!str" {
			return "", fmt.Errorf("intent at %s (%s) must be text", value.Location(), value.SemanticPath())
		}
		return scalar.Value, nil
	}
	var out Source
	if value.Presence() == yamlsource.PresenceScalar || value.Presence() == yamlsource.PresenceEmptyScalar {
		local, err := text(value)
		if err != nil {
			return out, err
		}
		out = Source{Kind: SourceLocal, Local: strings.TrimSpace(local)}
	} else {
		fields, err := value.Mapping()
		if err != nil {
			return out, err
		}
		values := map[string]string{}
		for _, field := range fields {
			if _, exists := values[field.Name]; exists {
				return out, fmt.Errorf("intent source repeats key %q at %s", field.Name, field.KeyLocation)
			}
			if field.Name != "inline" && field.Name != "import" && field.Name != "override" {
				return out, fmt.Errorf("intent source key %q at %s is unsupported", field.Name, field.KeyLocation)
			}
			values[field.Name], err = text(field.Value)
			if err != nil {
				return out, err
			}
		}
		_, inline := values["inline"]
		_, imported := values["import"]
		_, override := values["override"]
		switch {
		case inline && !imported && !override:
			out = Source{Kind: SourceInline, Inline: values["inline"]}
		case imported && !inline:
			out = Source{Kind: SourceImport, Import: strings.TrimSpace(values["import"]), Override: strings.TrimSpace(values["override"])}
		default:
			return out, fmt.Errorf("intent at %s must be exactly inline, import, or import with override", value.Location())
		}
	}
	return out, out.ValidateSyntax()
}
