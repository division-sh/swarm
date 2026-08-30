package sourceartifact

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

var declarationFiles = map[string]struct{}{
	"schema.yaml": {}, "types.yaml": {}, "entities.yaml": {}, "nodes.yaml": {},
	"events.yaml": {}, "agents.yaml": {}, "tools.yaml": {}, "policy.yaml": {},
}

var resourceBranches = map[string]struct{}{
	"prompts": {}, "tests": {}, "data": {}, "mocks": {}, "modules": {}, "packs": {}, "docs": {},
}

var excludedDirectories = map[string]struct{}{
	".swarm": {}, ".git": {}, ".github": {},
}

var excludedFiles = map[string]struct{}{
	".gitignore": {}, ".gitattributes": {}, ".editorconfig": {}, ".DS_Store": {},
	"swarm.yaml": {}, "swarm.live.yaml": {}, "platform-spec.yaml": {},
}

var inertDocuments = map[string]struct{}{
	"README": {}, "README.md": {}, "LICENSE": {}, "LICENSE.md": {}, "NOTICE": {},
	"NOTICE.md": {}, "LICENSE.txt": {}, "LICENSE-MIT": {}, "LICENSE-APACHE": {},
}

var windowsReservedBasenames = map[string]struct{}{
	"CON": {}, "PRN": {}, "AUX": {}, "NUL": {},
	"COM1": {}, "COM2": {}, "COM3": {}, "COM4": {}, "COM5": {}, "COM6": {}, "COM7": {}, "COM8": {}, "COM9": {},
	"LPT1": {}, "LPT2": {}, "LPT3": {}, "LPT4": {}, "LPT5": {}, "LPT6": {}, "LPT7": {}, "LPT8": {}, "LPT9": {},
}

func IsExcludedDirectory(name string) bool { _, ok := excludedDirectories[name]; return ok }
func IsExcludedFile(name string) bool      { _, ok := excludedFiles[name]; return ok }

func ValidateFlowSegment(segment string) error {
	if len(segment) == 0 || len(segment) > MaxSegmentBytes {
		return fmt.Errorf("flow segment %q must be 1..%d bytes", segment, MaxSegmentBytes)
	}
	if isWindowsReserved(segment) {
		return fmt.Errorf("flow segment %q is a reserved portable basename", segment)
	}
	for index, char := range []byte(segment) {
		if char > 0x7f || !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (index > 0 && (char == '_' || char == '-'))) {
			return fmt.Errorf("flow segment %q must match [a-z0-9][a-z0-9_-]*", segment)
		}
	}
	return nil
}

func validatePortableSegment(segment string) error {
	if len(segment) == 0 || len(segment) > MaxSegmentBytes || segment == "." || segment == ".." {
		return fmt.Errorf("segment %q must be 1..%d bytes and not dot syntax", segment, MaxSegmentBytes)
	}
	if isWindowsReserved(segment) {
		return fmt.Errorf("segment %q is a reserved portable basename", segment)
	}
	for index, char := range []byte(segment) {
		if char > 0x7f || !((char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (index > 0 && (char == '.' || char == '_' || char == '-'))) {
			return fmt.Errorf("segment %q must match [A-Za-z0-9][A-Za-z0-9._-]*", segment)
		}
	}
	return nil
}

func isWindowsReserved(segment string) bool {
	base := segment
	if before, _, ok := strings.Cut(base, "."); ok {
		base = before
	}
	_, ok := windowsReservedBasenames[strings.ToUpper(base)]
	return ok
}

func classifyLabel(label string) (Disposition, error) {
	segments := strings.Split(label, "/")
	for _, segment := range segments {
		if IsExcludedDirectory(segment) || IsExcludedFile(segment) {
			return 0, fmt.Errorf("excluded local/tooling path %q cannot appear in a source artifact", label)
		}
		if segment == "package.yaml" {
			return 0, fmt.Errorf("RETIRED: package.yaml is not admitted; rename distribution metadata to manifest.yaml and derive topology from directories")
		}
		if segment == "flows" {
			return 0, fmt.Errorf("RETIRED: flows/ is not admitted; make each flow an ordinary child directory")
		}
	}

	flowSegments, local, branch, err := splitLabelTopology(segments)
	if err != nil {
		return 0, err
	}
	for _, segment := range flowSegments {
		if err := ValidateFlowSegment(segment); err != nil {
			return 0, fmt.Errorf("source artifact label %q: %w", label, err)
		}
	}
	if branch != "" {
		if branch == "packs" && len(flowSegments) != 0 {
			return 0, fmt.Errorf("source artifact label %q uses packs/ below flow %q; packs/ is selected-root-only", label, strings.Join(flowSegments, "/"))
		}
		if branch == "docs" {
			return DispositionDocument, nil
		}
		return DispositionResource, nil
	}
	if local == "manifest.yaml" {
		return DispositionManifest, nil
	}
	if _, ok := declarationFiles[local]; ok {
		return DispositionDeclaration, nil
	}
	if _, ok := inertDocuments[local]; ok {
		return DispositionDocument, nil
	}
	return 0, fmt.Errorf("unclassified source file %q; use a finite declaration name or typed resource branch", label)
}

func splitLabelTopology(segments []string) (flowSegments []string, local, branch string, err error) {
	for index, segment := range segments {
		if _, ok := resourceBranches[segment]; ok {
			if index == len(segments)-1 {
				return nil, "", "", fmt.Errorf("typed resource branch %q contains no file", strings.Join(segments, "/"))
			}
			return append([]string(nil), segments[:index]...), segments[len(segments)-1], segment, nil
		}
	}
	if len(segments) == 0 {
		return nil, "", "", fmt.Errorf("empty source label")
	}
	return append([]string(nil), segments[:len(segments)-1]...), segments[len(segments)-1], "", nil
}

func buildFlowTree(entries []Entry) (*FlowNode, error) {
	root := &FlowNode{flowPath: ".", declarations: map[string]string{}, resources: map[string][]string{}}
	nodes := map[string]*FlowNode{".": root}
	for _, entry := range entries {
		segments := strings.Split(entry.label, "/")
		flowSegments, local, branch, err := splitLabelTopology(segments)
		if err != nil {
			return nil, err
		}
		current := root
		for index := range flowSegments {
			flowPath := strings.Join(flowSegments[:index+1], "/")
			next := nodes[flowPath]
			if next == nil {
				next = &FlowNode{flowPath: flowPath, declarations: map[string]string{}, resources: map[string][]string{}}
				nodes[flowPath] = next
				current.children = append(current.children, next)
			}
			current = next
		}
		switch entry.disposition {
		case DispositionDeclaration:
			current.declarations[local] = entry.label
		case DispositionManifest:
			current.manifest = entry.label
		case DispositionResource:
			current.resources[branch] = append(current.resources[branch], entry.label)
		case DispositionDocument:
			if branch == "docs" {
				current.resources[branch] = append(current.resources[branch], entry.label)
			} else {
				current.documents = append(current.documents, entry.label)
			}
		}
	}
	for _, node := range nodes {
		sort.Slice(node.children, func(i, j int) bool { return node.children[i].flowPath < node.children[j].flowPath })
		for branch := range node.resources {
			sort.Strings(node.resources[branch])
		}
		sort.Strings(node.documents)
	}
	if !flowNodeLive(root) {
		return nil, fmt.Errorf("selected source root has no semantic declarations, typed resources, or live descendants")
	}
	for flowPath, node := range nodes {
		if flowPath != "." && !flowNodeLive(node) {
			return nil, fmt.Errorf("flow %q is empty; add local semantics/resources or remove the dead directory", flowPath)
		}
	}
	return root, nil
}

func flowNodeLive(node *FlowNode) bool {
	if node == nil {
		return false
	}
	if len(node.declarations) > 0 || len(node.resources) > 0 {
		return true
	}
	for _, child := range node.children {
		if flowNodeLive(child) {
			return true
		}
	}
	return false
}

func isAdmittedYAML(label string, disposition Disposition) bool {
	if disposition == DispositionDeclaration {
		return true
	}
	return disposition == DispositionResource && strings.HasSuffix(strings.ToLower(path.Base(label)), ".yaml")
}

func asciiFold(value string) string {
	raw := []byte(value)
	for index, char := range raw {
		if char >= 'A' && char <= 'Z' {
			raw[index] = char + ('a' - 'A')
		}
	}
	return string(raw)
}
