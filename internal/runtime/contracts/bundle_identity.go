package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type BundleIdentity struct {
	WorkflowName    string `json:"workflow_name"`
	WorkflowVersion string `json:"workflow_version"`
	Fingerprint     string `json:"fingerprint"`
}

type bundleIdentityEntry struct {
	Label string
	Path  string
}

func BootBundleIdentity(bundle *WorkflowContractBundle) (BundleIdentity, error) {
	if bundle == nil {
		return BundleIdentity{}, fmt.Errorf("workflow contract bundle is required")
	}
	fingerprint, err := BundleFingerprint(bundle)
	if err != nil {
		return BundleIdentity{}, err
	}
	return BundleIdentity{
		WorkflowName:    strings.TrimSpace(bundle.Semantics.Name),
		WorkflowVersion: strings.TrimSpace(bundle.Semantics.Version),
		Fingerprint:     fingerprint,
	}, nil
}

func BundleFingerprint(bundle *WorkflowContractBundle) (string, error) {
	entries, err := bundleIdentityEntries(bundle)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("workflow contract bundle has no identity inputs")
	}
	hasher := sha256.New()
	for _, entry := range entries {
		canonical, err := canonicalBundleIdentityContent(entry.Path)
		if err != nil {
			return "", fmt.Errorf("canonicalize %s: %w", entry.Label, err)
		}
		hasher.Write([]byte(entry.Label))
		hasher.Write([]byte{0})
		hasher.Write(canonical)
		hasher.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func bundleIdentityEntries(bundle *WorkflowContractBundle) ([]bundleIdentityEntry, error) {
	if bundle == nil {
		return nil, fmt.Errorf("workflow contract bundle is required")
	}
	seen := map[string]struct{}{}
	var entries []bundleIdentityEntry
	addFile := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		clean, err := filepath.Abs(path)
		if err != nil {
			clean = filepath.Clean(path)
		}
		if _, ok := seen[clean]; ok {
			return
		}
		if stat, err := os.Stat(clean); err != nil || stat.IsDir() {
			return
		}
		seen[clean] = struct{}{}
		entries = append(entries, bundleIdentityEntry{
			Label: bundleIdentityLabel(bundle.Paths, clean),
			Path:  clean,
		})
	}
	addDir := func(dir string) error {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			return nil
		}
		if stat, err := os.Stat(dir); err != nil || !stat.IsDir() {
			return nil
		}
		return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d == nil || d.IsDir() {
				return nil
			}
			addFile(path)
			return nil
		})
	}

	paths := bundle.Paths
	addFile(paths.PlatformSpecFile)
	addFile(paths.ProjectPackageFile)
	addFile(paths.RootSchemaFile)
	addFile(paths.RootTypesFile)
	addFile(paths.RootEntitiesFile)
	addFile(paths.ProjectNodesFile)
	addFile(paths.ProjectEventsFile)
	addFile(paths.ProjectAgentsFile)
	addFile(paths.ProjectToolsFile)
	addFile(paths.ProjectPolicyFile)
	if err := addDir(paths.ProjectPromptsDir); err != nil {
		return nil, err
	}
	for _, pkg := range paths.ProjectPackages {
		addFile(pkg.PackageFile)
		addFile(pkg.ProjectNodesFile)
		addFile(pkg.ProjectEventsFile)
		addFile(pkg.ProjectAgentsFile)
		addFile(pkg.ProjectToolsFile)
		addFile(pkg.ProjectPolicyFile)
		if err := addDir(pkg.ProjectPromptsDir); err != nil {
			return nil, err
		}
		for _, flow := range pkg.Flows {
			addFlowIdentityFiles(flow, addFile)
			if err := addDir(flow.PromptsDir); err != nil {
				return nil, err
			}
		}
	}
	for _, flow := range paths.Flows {
		addFlowIdentityFiles(flow, addFile)
		if err := addDir(flow.PromptsDir); err != nil {
			return nil, err
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Label == entries[j].Label {
			return entries[i].Path < entries[j].Path
		}
		return entries[i].Label < entries[j].Label
	})
	return entries, nil
}

func addFlowIdentityFiles(flow FlowContractPaths, addFile func(string)) {
	addFile(flow.SchemaFile)
	addFile(flow.TypesFile)
	addFile(flow.EntitiesFile)
	addFile(flow.NodesFile)
	addFile(flow.EventsFile)
	addFile(flow.AgentsFile)
	addFile(flow.ToolsFile)
	addFile(flow.PolicyFile)
}

func bundleIdentityLabel(paths ContractPaths, path string) string {
	if root := strings.TrimSpace(paths.ContractsRoot); root != "" {
		if rel, err := filepath.Rel(root, path); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "bundle/" + filepath.ToSlash(filepath.Clean(rel))
		}
	}
	if platform := strings.TrimSpace(paths.PlatformSpecFile); platform != "" {
		if sameFilePath(platform, path) {
			return "platform/" + filepath.Base(path)
		}
	}
	return "external/" + filepath.Base(path)
}

func sameFilePath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr == nil && rightErr == nil {
		return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func canonicalBundleIdentityContent(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		var decoded any
		if err := yaml.Unmarshal(raw, &decoded); err != nil {
			return nil, err
		}
		normalized := normalizeYAMLForJSON(decoded)
		out, err := json.Marshal(normalized)
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return []byte(normalizeTextContent(string(raw))), nil
	}
}

func normalizeYAMLForJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = normalizeYAMLForJSON(value)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[fmt.Sprint(key)] = normalizeYAMLForJSON(value)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, value := range typed {
			out[i] = normalizeYAMLForJSON(value)
		}
		return out
	default:
		return typed
	}
}

func normalizeTextContent(raw string) string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	lines := strings.Split(raw, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return strings.Join(lines, "\n") + "\n"
}
