package contracts

import (
	"path"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type FlowSource struct {
	FlowPath  string
	Schema    string
	Types     string
	Entities  string
	Nodes     string
	Events    string
	Agents    string
	Tools     string
	Policy    string
	Manifest  string
	Resources map[string][]string
	Documents []string
	Children  []string
}

func (s FlowSource) ID() string { return strings.TrimSpace(s.FlowPath) }

func (s FlowSource) LocalName() string {
	if s.FlowPath == "." {
		return "."
	}
	return path.Base(s.FlowPath)
}

func (s FlowSource) DeclarationLabel(fileName string) string {
	switch fileName {
	case "schema.yaml":
		return s.Schema
	case "types.yaml":
		return s.Types
	case "entities.yaml":
		return s.Entities
	case "nodes.yaml":
		return s.Nodes
	case "events.yaml":
		return s.Events
	case "agents.yaml":
		return s.Agents
	case "tools.yaml":
		return s.Tools
	case "policy.yaml":
		return s.Policy
	case "manifest.yaml":
		return s.Manifest
	default:
		return ""
	}
}

func indexFlowSources(artifact *sourceartifact.AdmittedSourceArtifact) (map[string]FlowSource, error) {
	if artifact == nil || artifact.Root() == nil {
		return nil, NewSourceArtifactRequiredDiagnostic()
	}
	out := map[string]FlowSource{}
	var walk func(*sourceartifact.FlowNode)
	walk = func(node *sourceartifact.FlowNode) {
		if node == nil {
			return
		}
		source := FlowSource{FlowPath: node.Path(), Resources: map[string][]string{}}
		for _, fileName := range []string{"schema.yaml", "types.yaml", "entities.yaml", "nodes.yaml", "events.yaml", "agents.yaml", "tools.yaml", "policy.yaml"} {
			label, ok := node.Declaration(fileName)
			if !ok {
				continue
			}
			switch fileName {
			case "schema.yaml":
				source.Schema = label
			case "types.yaml":
				source.Types = label
			case "entities.yaml":
				source.Entities = label
			case "nodes.yaml":
				source.Nodes = label
			case "events.yaml":
				source.Events = label
			case "agents.yaml":
				source.Agents = label
			case "tools.yaml":
				source.Tools = label
			case "policy.yaml":
				source.Policy = label
			}
		}
		if label, ok := node.Manifest(); ok {
			source.Manifest = label
		}
		for _, branch := range []string{"prompts", "tests", "data", "mocks", "modules", "packs", "docs"} {
			if labels := node.Resources(branch); len(labels) > 0 {
				source.Resources[branch] = labels
			}
		}
		source.Documents = node.Documents()
		for _, child := range node.Children() {
			source.Children = append(source.Children, child.Path())
		}
		sort.Strings(source.Children)
		out[source.FlowPath] = source
		for _, child := range node.Children() {
			walk(child)
		}
	}
	walk(artifact.Root())
	return out, nil
}

func sortedFlowSources(sources map[string]FlowSource) []FlowSource {
	paths := make([]string, 0, len(sources))
	for flowPath := range sources {
		paths = append(paths, flowPath)
	}
	sort.Slice(paths, func(i, j int) bool {
		if paths[i] == "." {
			return true
		}
		if paths[j] == "." {
			return false
		}
		return paths[i] < paths[j]
	})
	out := make([]FlowSource, 0, len(paths))
	for _, flowPath := range paths {
		out = append(out, sources[flowPath])
	}
	return out
}
