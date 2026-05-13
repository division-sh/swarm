package flowdata

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

const ToolName = "read_flow_data"

type ResolvedFile struct {
	Filename    string
	Path        string
	ContentType string
	SizeBytes   int64
}

type Finding struct {
	AgentLabel string
	Filename   string
	Message    string
}

func NormalizeAccessList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, raw := range values {
		filename, err := NormalizeFilename(raw)
		if err != nil {
			continue
		}
		if _, ok := seen[filename]; ok {
			continue
		}
		seen[filename] = struct{}{}
		out = append(out, filename)
	}
	sort.Strings(out)
	return out
}

func NormalizeFilename(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("filename is required")
	}
	if filepath.IsAbs(value) || strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	if strings.Contains(value, "\\") || strings.Contains(value, ":") {
		return "", fmt.Errorf("platform-specific paths are not allowed")
	}
	if strings.HasPrefix(value, "~") {
		return "", fmt.Errorf("home-relative paths are not allowed")
	}
	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("path traversal is not allowed")
		}
	}
	clean := path.Clean(value)
	if clean == "." || clean != value {
		return "", fmt.Errorf("path must be normalized relative to the flow data root")
	}
	return clean, nil
}

func ContentType(filename string) string {
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".yaml", ".yml":
		return "yaml"
	case ".json":
		return "json"
	case ".md", ".markdown":
		return "markdown"
	default:
		return "text"
	}
}

func AllowedFilenames(source semanticview.Source, actor models.AgentConfig) []string {
	if source == nil {
		return nil
	}
	if logicalID, entry, ok := semanticview.ResolveAgentRegistryEntry(source, actor); ok {
		if contractSource, sourceOK := source.AgentContractSource(logicalID); sourceOK && strings.TrimSpace(contractSource.FlowID) != "" {
			return FlowDataAccessFromEntry(entry)
		}
	}
	return nil
}

func ValidateSource(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	var findings []Finding
	for _, scope := range source.ProjectScopes() {
		scopeLabel := projectScopeLabel(scope.Key, scope.Manifest.Name)
		for agentID, agent := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			for _, filename := range agent.FlowDataAccess {
				findings = append(findings, Finding{
					AgentLabel: scopedLabel(scopeLabel, agentID),
					Filename:   strings.TrimSpace(filename),
					Message:    "flow_data_access is only valid on flow-scoped agents",
				})
			}
		}
	}
	for _, scope := range source.FlowScopes() {
		scopeLabel := flowScopeLabel(scope.ID, scope.Path)
		for agentID, agent := range scope.Agents {
			agentID = strings.TrimSpace(agentID)
			for _, raw := range agent.FlowDataAccess {
				filename, err := NormalizeFilename(raw)
				if err != nil {
					findings = append(findings, Finding{
						AgentLabel: scopedLabel(scopeLabel, agentID),
						Filename:   strings.TrimSpace(raw),
						Message:    fmt.Sprintf("invalid flow_data_access path: %v", err),
					})
					continue
				}
				if _, err := resolveUnderDataRoot(scope.DataDir, filename); err != nil {
					findings = append(findings, Finding{
						AgentLabel: scopedLabel(scopeLabel, agentID),
						Filename:   filename,
						Message:    err.Error(),
					})
				}
			}
		}
	}
	return findings
}

func Resolve(source semanticview.Source, actor models.AgentConfig, rawFilename string) (ResolvedFile, error) {
	if source == nil {
		return ResolvedFile{}, fmt.Errorf("semantic source is required for %s", ToolName)
	}
	filename, err := NormalizeFilename(rawFilename)
	if err != nil {
		return ResolvedFile{}, err
	}
	if !filenameAllowed(filename, AllowedFilenames(source, actor)) {
		return ResolvedFile{}, fmt.Errorf("flow data file %q is not declared for agent %s", filename, strings.TrimSpace(actor.ID))
	}
	scope, err := actorFlowScope(source, actor)
	if err != nil {
		return ResolvedFile{}, err
	}
	resolvedPath, err := resolveUnderDataRoot(scope.DataDir, filename)
	if err != nil {
		return ResolvedFile{}, err
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return ResolvedFile{}, err
	}
	return ResolvedFile{
		Filename:    filename,
		Path:        resolvedPath,
		ContentType: ContentType(filename),
		SizeBytes:   info.Size(),
	}, nil
}

func actorFlowScope(source semanticview.Source, actor models.AgentConfig) (semanticview.FlowScope, error) {
	flowID := strings.TrimSpace(actor.Mode)
	if flowID != "" {
		if scope, ok := source.FlowScopeByID(flowID); ok {
			return scope, nil
		}
	}
	if contractSource, ok := source.AgentContractSource(strings.TrimSpace(actor.ID)); ok {
		flowID = strings.TrimSpace(contractSource.FlowID)
	}
	if flowID == "" {
		if logicalID, _, ok := semanticview.ResolveAgentRegistryEntry(source, actor); ok {
			if contractSource, ok := source.AgentContractSource(logicalID); ok {
				flowID = strings.TrimSpace(contractSource.FlowID)
			}
		}
	}
	if flowID == "" {
		return semanticview.FlowScope{}, fmt.Errorf("agent %s has no owning flow for flow data", strings.TrimSpace(actor.ID))
	}
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		return semanticview.FlowScope{}, fmt.Errorf("owning flow %s for agent %s was not found", flowID, strings.TrimSpace(actor.ID))
	}
	return scope, nil
}

func resolveUnderDataRoot(dataDir, filename string) (string, error) {
	filename, err := NormalizeFilename(filename)
	if err != nil {
		return "", err
	}
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		return "", fmt.Errorf("flow data root is missing")
	}
	root, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return "", fmt.Errorf("flow data root is not readable: %w", err)
	}
	root = filepath.Clean(root)
	target := filepath.Join(root, filepath.FromSlash(filename))
	realTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		return "", fmt.Errorf("flow data file %q is not readable: %w", filename, err)
	}
	realTarget = filepath.Clean(realTarget)
	rel, err := filepath.Rel(root, realTarget)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("flow data file %q escapes the flow data root", filename)
	}
	info, err := os.Stat(realTarget)
	if err != nil {
		return "", fmt.Errorf("flow data file %q is not readable: %w", filename, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("flow data file %q is not a regular file", filename)
	}
	return realTarget, nil
}

func filenameAllowed(filename string, allowed []string) bool {
	for _, item := range allowed {
		if item == filename {
			return true
		}
	}
	return false
}

func projectScopeLabel(key, name string) string {
	key = strings.TrimSpace(key)
	name = strings.TrimSpace(name)
	switch {
	case key != "" && name != "":
		return fmt.Sprintf("project:%s:%s", key, name)
	case key != "":
		return "project:" + key
	case name != "":
		return "project:" + name
	default:
		return "project"
	}
}

func flowScopeLabel(id, path string) string {
	id = strings.TrimSpace(id)
	path = strings.Trim(strings.TrimSpace(path), "/")
	switch {
	case id != "" && path != "":
		return fmt.Sprintf("flow:%s:%s", id, path)
	case id != "":
		return "flow:" + id
	case path != "":
		return "flow:" + path
	default:
		return "flow"
	}
}

func scopedLabel(scopeLabel, localID string) string {
	localID = strings.TrimSpace(localID)
	if localID == "" {
		return scopeLabel
	}
	if strings.TrimSpace(scopeLabel) == "" {
		return localID
	}
	return scopeLabel + "/" + localID
}

func FlowDataAccessFromEntry(entry runtimecontracts.AgentRegistryEntry) []string {
	return NormalizeAccessList(entry.FlowDataAccess)
}
