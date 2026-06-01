package flowdata

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

type agentFlowDataDeclaration struct {
	LogicalID string
	Entry     runtimecontracts.AgentRegistryEntry
	FlowID    string
	Scope     semanticview.FlowScope
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
	if decl, ok := resolveAgentFlowDataDeclaration(source, actor); ok {
		return FlowDataAccessFromEntry(decl.Entry)
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
	decl, ok := resolveAgentFlowDataDeclaration(source, actor)
	if !ok {
		return ResolvedFile{}, fmt.Errorf("agent %s has no flow-scoped contract declaration for flow data", strings.TrimSpace(actor.ID))
	}
	if !filenameAllowed(filename, FlowDataAccessFromEntry(decl.Entry)) {
		return ResolvedFile{}, fmt.Errorf("flow data file %q is not declared for agent %s", filename, strings.TrimSpace(actor.ID))
	}
	resolvedPath, err := resolveUnderDataRoot(decl.Scope.DataDir, filename)
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

func resolveAgentFlowDataDeclaration(source semanticview.Source, actor models.AgentConfig) (agentFlowDataDeclaration, bool) {
	if source == nil {
		return agentFlowDataDeclaration{}, false
	}
	actorID := strings.TrimSpace(actor.ID)
	if actorID == "" {
		return agentFlowDataDeclaration{}, false
	}
	var matches []agentFlowDataDeclaration
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		if flowID == "" {
			continue
		}
		for _, logicalID := range sortedAgentIDs(scope.Agents) {
			entry := scope.Agents[logicalID]
			if !agentIdentityMatches(logicalID, entry.ID, actorID) {
				continue
			}
			matches = append(matches, agentFlowDataDeclaration{
				LogicalID: strings.TrimSpace(logicalID),
				Entry:     entry,
				FlowID:    flowID,
				Scope:     scope,
			})
		}
	}
	if len(matches) != 1 {
		return agentFlowDataDeclaration{}, false
	}
	return matches[0], true
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

func sortedAgentIDs(agents map[string]runtimecontracts.AgentRegistryEntry) []string {
	ids := make([]string, 0, len(agents))
	for id := range agents {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func agentIdentityMatches(logicalID, declaredID, actorID string) bool {
	logicalID = strings.TrimSpace(logicalID)
	declaredID = strings.TrimSpace(declaredID)
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return false
	}
	if logicalID != "" && logicalID == actorID {
		return true
	}
	if declaredID == "" {
		return false
	}
	if declaredID == actorID {
		return true
	}
	matched, err := regexp.MatchString(flowDataTemplateMatchPattern(declaredID), actorID)
	return err == nil && matched
}

func flowDataTemplateMatchPattern(template string) string {
	matches := flowDataTemplateFieldPattern.FindAllStringIndex(template, -1)
	if len(matches) == 0 {
		return "^" + regexp.QuoteMeta(template) + "$"
	}
	var builder strings.Builder
	builder.WriteString("^")
	last := 0
	for _, match := range matches {
		builder.WriteString(regexp.QuoteMeta(template[last:match[0]]))
		builder.WriteString(".+")
		last = match[1]
	}
	builder.WriteString(regexp.QuoteMeta(template[last:]))
	builder.WriteString("$")
	return builder.String()
}

var flowDataTemplateFieldPattern = regexp.MustCompile(`\{[^{}]+\}`)
