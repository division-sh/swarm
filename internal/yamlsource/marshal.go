package yamlsource

import (
	"bytes"

	"gopkg.in/yaml.v3"
)

// QuotedString requests a double-quoted string scalar when marshaled.
type QuotedString string

func (value QuotedString) MarshalYAML() (any, error) {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: string(value), Style: yaml.DoubleQuotedStyle}, nil
}

// Marshal emits one two-space-indented document without loading source or caches.
func Marshal(value any) ([]byte, error) {
	var document yaml.Node
	if err := document.Encode(value); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
