package scenarioderivation

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"gopkg.in/yaml.v3"
)

type Declaration struct {
	Name               string
	FlowID             string
	Input              string
	Set                map[string]any
	ConnectorResponses map[string]json.RawMessage
}

type LocatedDeclaration struct {
	Path        string
	Declaration Declaration
}

func ParseDeclaration(raw []byte) (Declaration, bool, error) {
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return Declaration{}, false, fmt.Errorf("parse scenario YAML: %w", err)
	}
	rawDerive, hasDerive := document["derive"]
	if !hasDerive {
		return Declaration{}, false, nil
	}
	if _, hasSteps := document["steps"]; hasSteps {
		return Declaration{}, false, fmt.Errorf("derive and steps are mutually exclusive")
	}
	for key := range document {
		switch key {
		case "version", "name", "seed", "vars", "setup", "derive", "connector_responses", "expect", "invalid":
		default:
			return Declaration{}, false, fmt.Errorf("unsupported top-level field %q", key)
		}
	}
	derive, ok := rawDerive.(map[string]any)
	if !ok {
		return Declaration{}, false, fmt.Errorf("derive must be a mapping")
	}
	for key := range derive {
		switch key {
		case "flow", "input", "payload":
		default:
			return Declaration{}, false, fmt.Errorf("unsupported derive field %q", key)
		}
	}
	name, err := requiredCanonicalText(document, "name", "name")
	if err != nil {
		return Declaration{}, false, err
	}
	flowID, err := requiredCanonicalText(derive, "flow", "derive.flow")
	if err != nil {
		return Declaration{}, false, err
	}
	input, err := requiredCanonicalText(derive, "input", "derive.input")
	if err != nil {
		return Declaration{}, false, err
	}
	flowID = strings.Trim(flowID, "/")
	if flowID == "" {
		return Declaration{}, false, fmt.Errorf("derive.flow must name an exact flow")
	}
	declaration := Declaration{
		Name:               name,
		FlowID:             flowID,
		Input:              input,
		ConnectorResponses: map[string]json.RawMessage{},
	}
	payload, ok := derive["payload"].(map[string]any)
	if !ok {
		return Declaration{}, false, fmt.Errorf("derive.payload must be a mapping")
	}
	for key := range payload {
		if key != "generate" && key != "set" {
			return Declaration{}, false, fmt.Errorf("unsupported derive.payload field %q", key)
		}
	}
	generate, ok := payload["generate"].(bool)
	if !ok || !generate {
		return Declaration{}, false, fmt.Errorf("derive.payload.generate must be true")
	}
	if rawSet, exists := payload["set"]; exists {
		set, ok := rawSet.(map[string]any)
		if !ok {
			return Declaration{}, false, fmt.Errorf("derive.payload.set must be a mapping")
		}
		declaration.Set = cloneDeclarationMap(set)
	}
	if rawResponses, exists := document["connector_responses"]; exists {
		responses, ok := rawResponses.(map[string]any)
		if !ok {
			return Declaration{}, false, fmt.Errorf("connector_responses must be a mapping")
		}
		for rawID, value := range responses {
			id := strings.TrimSpace(rawID)
			if id == "" || id != rawID {
				return Declaration{}, false, fmt.Errorf("connector response tool id %q is not canonical", rawID)
			}
			encoded, err := canonicaljson.Bytes(value)
			if err != nil {
				return Declaration{}, false, fmt.Errorf("connector_responses.%s: %w", id, err)
			}
			declaration.ConnectorResponses[id] = encoded
		}
	}
	return declaration, true, nil
}

func requiredCanonicalText(object map[string]any, key, path string) (string, error) {
	raw, exists := object[key]
	if !exists {
		return "", fmt.Errorf("%s is required", path)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s must be text", path)
	}
	canonical := strings.TrimSpace(value)
	if canonical == "" {
		return "", fmt.Errorf("%s must be non-empty", path)
	}
	return canonical, nil
}

func LoadDeclarations(artifact *sourceartifact.AdmittedSourceArtifact) ([]LocatedDeclaration, error) {
	if artifact == nil {
		return nil, fmt.Errorf("scenario declaration discovery requires an admitted source artifact")
	}
	root := artifact.Root()
	if root == nil {
		return nil, fmt.Errorf("scenario declaration discovery requires an admitted flow tree")
	}
	out := make([]LocatedDeclaration, 0)
	var visit func(*sourceartifact.FlowNode) error
	visit = func(flow *sourceartifact.FlowNode) error {
		for _, label := range flow.Resources("tests") {
			ext := strings.ToLower(path.Ext(label))
			if ext != ".yaml" && ext != ".yml" {
				continue
			}
			entry, ok := artifact.Entry(label)
			if !ok {
				return fmt.Errorf("scenario resource %q is missing from its admitted source artifact", label)
			}
			declaration, found, err := ParseDeclaration(entry.Bytes())
			if err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			if found {
				out = append(out, LocatedDeclaration{Path: label, Declaration: declaration})
			}
		}
		for _, child := range flow.Children() {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func cloneDeclarationMap(input map[string]any) map[string]any {
	raw, _ := json.Marshal(input)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}
