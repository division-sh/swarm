package flowdata

import (
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const ToolName = "read_flow_data"

type ResolvedStaticData struct {
	StaticID      durabledata.StaticDataID
	StaticRef     durabledata.StaticDataRef
	PackageKey    string
	OwnerFlowID   string
	RelativePath  string
	ContentDigest string
	ContentType   string
	MountPath     string
	Content       []byte
}

type Finding struct {
	AgentLabel string
	Filename   string
	Message    string
}

type agentFlowDataDeclaration struct {
	LogicalID  string
	FlowID     string
	PackageKey string
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

func AllowedStaticData(source semanticview.Source, actor models.AgentConfig) []ResolvedStaticData {
	decl, ok := resolveAgentFlowDataDeclaration(source, actor)
	if !ok {
		return nil
	}
	values := source.StaticDataForAgent(decl.PackageKey, decl.FlowID, decl.LogicalID)
	out := make([]ResolvedStaticData, 0, len(values))
	for _, value := range values {
		mountPath, err := durabledata.StaticMountPath(value.StaticID)
		if err != nil {
			return nil
		}
		out = append(out, ResolvedStaticData{
			StaticID:      value.StaticID,
			StaticRef:     value.Ref,
			PackageKey:    value.PackageKey,
			OwnerFlowID:   value.OwnerFlowID,
			RelativePath:  value.RelativePath,
			ContentDigest: value.ContentDigest,
			ContentType:   value.ContentType,
			MountPath:     mountPath,
			Content:       append([]byte(nil), value.Content...),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StaticID < out[j].StaticID })
	return out
}

func AllowedResourceData(source semanticview.Source, actor models.AgentConfig) []durabledata.DeclarationRef {
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok {
		return nil
	}
	return source.DurableDataForAgent(declaration.Source.PackageKey, declaration.OwnerFlowID, declaration.LocalID)
}

func ValidateSource(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	var findings []Finding
	for _, declaration := range semanticview.AgentDeclarations(source) {
		agentID := strings.TrimSpace(declaration.LocalID)
		scopeLabel := declaration.Label(true)
		if strings.TrimSpace(declaration.OwnerFlowID) == "" {
			for _, filename := range declaration.Entry.FlowDataAccess {
				findings = append(findings, Finding{
					AgentLabel: scopeLabel,
					Filename:   strings.TrimSpace(filename),
					Message:    "flow_data_access is only valid on flow-scoped agents",
				})
			}
			continue
		}
		if _, ok := source.FlowScopeByID(declaration.OwnerFlowID); !ok {
			findings = append(findings, Finding{AgentLabel: scopeLabel, Message: fmt.Sprintf("agent %s references missing owning flow %s", agentID, declaration.OwnerFlowID)})
			continue
		}
		compiled := source.StaticDataForAgent(declaration.Source.PackageKey, declaration.OwnerFlowID, declaration.LocalID)
		if len(compiled) != len(NormalizeAccessList(declaration.Entry.FlowDataAccess)) {
			findings = append(findings, Finding{
				AgentLabel: scopeLabel,
				Message:    "flow_data_access does not have a complete compiled static-data projection",
			})
		}
	}
	return findings
}

func Resolve(source semanticview.Source, actor models.AgentConfig, staticID durabledata.StaticDataID) (ResolvedStaticData, error) {
	if source == nil {
		return ResolvedStaticData{}, fmt.Errorf("semantic source is required for %s", ToolName)
	}
	if err := staticID.Validate(); err != nil {
		return ResolvedStaticData{}, fmt.Errorf("static_id: %w", err)
	}
	for _, item := range AllowedStaticData(source, actor) {
		if item.StaticID == staticID {
			item.Content = append([]byte(nil), item.Content...)
			return item, nil
		}
	}
	return ResolvedStaticData{}, fmt.Errorf("static data %q is not declared for agent %s", staticID, strings.TrimSpace(actor.ID))
}

func resolveAgentFlowDataDeclaration(source semanticview.Source, actor models.AgentConfig) (agentFlowDataDeclaration, bool) {
	if source == nil {
		return agentFlowDataDeclaration{}, false
	}
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok || strings.TrimSpace(declaration.OwnerFlowID) == "" {
		return agentFlowDataDeclaration{}, false
	}
	flowID := strings.TrimSpace(declaration.OwnerFlowID)
	if _, ok := source.FlowScopeByID(flowID); !ok {
		return agentFlowDataDeclaration{}, false
	}
	return agentFlowDataDeclaration{
		LogicalID:  strings.TrimSpace(declaration.LocalID),
		FlowID:     flowID,
		PackageKey: strings.TrimSpace(declaration.Source.PackageKey),
	}, true
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
