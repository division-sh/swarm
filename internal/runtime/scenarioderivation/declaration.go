package scenarioderivation

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/scenariodocument"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// Declaration contains materialized data, not authored expression sites.
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

func ParseDeclaration(raw []byte, label string) (Declaration, bool, error) {
	document, found, err := scenariodocument.Discover(raw, label)
	if err != nil || !found {
		return Declaration{}, false, err
	}
	projection, err := document.Projection()
	if err != nil {
		return Declaration{}, false, err
	}
	if projection.Derive == nil {
		return Declaration{}, false, nil
	}
	evaluator, err := scenariodocument.NewEvaluator(scenariodocument.Seed(label, projection.Name, projection.Seed), projection.Vars)
	if err != nil {
		return Declaration{}, false, err
	}
	declaration, err := MaterializeDeclaration(*projection.Derive, evaluator)
	return declaration, true, err
}

func MaterializeDeclaration(authored scenariodocument.Derive, evaluator *scenariodocument.Evaluator) (Declaration, error) {
	declaration := Declaration{Name: authored.Name, FlowID: authored.FlowID, Input: authored.Input, ConnectorResponses: map[string]json.RawMessage{}}
	if authored.Set != nil {
		value, err := evaluator.Evaluate(authored.Set)
		if err != nil {
			return Declaration{}, fmt.Errorf("derive.payload.set: %w", err)
		}
		data, err := scenariodocument.Materialize(value)
		if err != nil {
			return Declaration{}, err
		}
		value, err = data.Interface()
		if err != nil {
			return Declaration{}, err
		}
		declaration.Set = value.(map[string]any)
	}
	keys := make([]string, 0, len(authored.ConnectorResponses))
	for key := range authored.ConnectorResponses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, id := range keys {
		value, err := evaluator.Evaluate(authored.ConnectorResponses[id])
		if err != nil {
			return Declaration{}, fmt.Errorf("connector_responses.%s: %w", id, err)
		}
		encoded, err := canonicaljson.Bytes(value)
		if err != nil {
			return Declaration{}, fmt.Errorf("connector_responses.%s: %w", id, err)
		}
		declaration.ConnectorResponses[id] = encoded
	}
	return declaration, nil
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
			declaration, found, err := ParseDeclaration(entry.Bytes(), label)
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
