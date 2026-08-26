package contracts

import (
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/durabledata"
)

// BuildDurableDataCatalog projects loader-owned declaration and static-byte
// facts for selected-store admission. Runtime consumers never receive paths.
func BuildDurableDataCatalog(bundle *WorkflowContractBundle) (durabledata.Catalog, error) {
	if bundle == nil {
		return durabledata.Catalog{}, fmt.Errorf("workflow contract bundle is required")
	}
	bundleHash, err := BundleHash(bundle)
	if err != nil {
		return durabledata.Catalog{}, err
	}
	catalog := durabledata.Catalog{BundleHash: bundleHash}
	for _, declaration := range bundle.DurableDataDeclarations() {
		catalog.Declarations = append(catalog.Declarations, durabledata.Declaration{
			Name: declaration.Name, Ref: declaration.Ref, OwnerFlowID: declaration.OwnerFlowID,
			BusinessKey:  declaration.BusinessKey,
			SchemaDigest: declaration.SchemaDigest, CanonicalSchema: append([]byte(nil), declaration.CanonicalSchema...),
		})
	}
	entries, err := bundleHashEntries(bundle)
	if err != nil {
		return durabledata.Catalog{}, err
	}
	entriesByCoordinate := make(map[string]bundleHashEntry)
	for _, entry := range entries {
		flow, relative, ok := staticDataEntryOwner(bundle, entry.Path)
		if ok {
			entriesByCoordinate[staticDataCoordinate(flow.PackageKey, flow.ID, relative)] = entry
		}
	}
	references, err := referencedStaticData(bundle)
	if err != nil {
		return durabledata.Catalog{}, err
	}
	for _, reference := range references {
		entry, ok := entriesByCoordinate[staticDataCoordinate(reference.PackageKey, reference.OwnerFlowID, reference.RelativePath)]
		if !ok {
			return durabledata.Catalog{}, fmt.Errorf("flow_data_access file %q for flow %s in package %s is missing from the exact bundle catalog", reference.RelativePath, reference.OwnerFlowID, reference.PackageKey)
		}
		content, err := canonicalBundleHashEntryContent(entry)
		if err != nil {
			return durabledata.Catalog{}, err
		}
		if !utf8.Valid(content) {
			return durabledata.Catalog{}, fmt.Errorf("static data %s must contain valid UTF-8 bytes", entry.Label)
		}
		ref := durabledata.StaticDataRef{BundleHash: bundleHash, CanonicalInputLabel: entry.Label}
		staticID, err := durabledata.NewStaticDataID(ref)
		if err != nil {
			return durabledata.Catalog{}, err
		}
		catalog.StaticData = append(catalog.StaticData, durabledata.StaticData{
			StaticID: staticID, Ref: ref, PackageKey: reference.PackageKey, OwnerFlowID: reference.OwnerFlowID,
			RelativePath: reference.RelativePath, ContentDigest: durabledata.StaticContentDigest(content),
			ContentType: staticDataContentType(reference.RelativePath), Content: content,
		})
	}
	sort.Slice(catalog.StaticData, func(i, j int) bool {
		return catalog.StaticData[i].Ref.CanonicalInputLabel < catalog.StaticData[j].Ref.CanonicalInputLabel
	})
	bundle.staticData = cloneStaticData(catalog.StaticData)
	bundle.staticDataAccess = make(map[string][]durabledata.StaticDataID)
	staticByCoordinate := make(map[string]durabledata.StaticDataID, len(catalog.StaticData))
	for _, item := range catalog.StaticData {
		staticByCoordinate[staticDataCoordinate(item.PackageKey, item.OwnerFlowID, item.RelativePath)] = item.StaticID
	}
	for _, record := range bundle.AgentDeclarationRecords() {
		key := staticAgentDeclarationKey(record.Source.PackageKey, record.OwnerFlowID, record.LogicalID)
		for _, raw := range record.Entry.FlowDataAccess {
			relative, err := normalizeStaticDataRelativePath(raw)
			if err != nil {
				return durabledata.Catalog{}, err
			}
			id, ok := staticByCoordinate[staticDataCoordinate(record.Source.PackageKey, record.OwnerFlowID, relative)]
			if !ok {
				return durabledata.Catalog{}, fmt.Errorf("agent %s static data access %q has no compiled static identity", record.LogicalID, relative)
			}
			bundle.staticDataAccess[key] = append(bundle.staticDataAccess[key], id)
		}
		sort.Slice(bundle.staticDataAccess[key], func(i, j int) bool {
			return bundle.staticDataAccess[key][i] < bundle.staticDataAccess[key][j]
		})
	}
	return catalog, nil
}

func staticDataEntryOwner(bundle *WorkflowContractBundle, path string) (FlowContractPaths, string, bool) {
	path = strings.TrimSpace(path)
	if bundle == nil || path == "" {
		return FlowContractPaths{}, "", false
	}
	for _, view := range bundle.FlowViews() {
		flow := view.Paths
		root := strings.TrimSpace(flow.DataDir)
		if root == "" {
			continue
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		return flow, filepath.ToSlash(relative), true
	}
	return FlowContractPaths{}, "", false
}

type staticDataReference struct {
	PackageKey   string
	OwnerFlowID  string
	RelativePath string
}

func referencedStaticData(bundle *WorkflowContractBundle) ([]staticDataReference, error) {
	if bundle == nil {
		return nil, nil
	}
	seen := map[string]struct{}{}
	var out []staticDataReference
	for _, record := range bundle.AgentDeclarationRecords() {
		for _, raw := range record.Entry.FlowDataAccess {
			relative, err := normalizeStaticDataRelativePath(raw)
			if err != nil {
				return nil, fmt.Errorf("agent %s flow_data_access %q: %w", record.LogicalID, raw, err)
			}
			if strings.TrimSpace(record.OwnerFlowID) == "" {
				return nil, fmt.Errorf("agent %s flow_data_access is only valid on flow-scoped agents", record.LogicalID)
			}
			reference := staticDataReference{
				PackageKey:   strings.TrimSpace(record.Source.PackageKey),
				OwnerFlowID:  strings.TrimSpace(record.OwnerFlowID),
				RelativePath: relative,
			}
			key := staticDataCoordinate(reference.PackageKey, reference.OwnerFlowID, reference.RelativePath)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, reference)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return staticDataCoordinate(out[i].PackageKey, out[i].OwnerFlowID, out[i].RelativePath) <
			staticDataCoordinate(out[j].PackageKey, out[j].OwnerFlowID, out[j].RelativePath)
	})
	return out, nil
}

func normalizeStaticDataRelativePath(raw string) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return "", fmt.Errorf("path must be non-empty without surrounding whitespace")
	}
	if filepath.IsAbs(raw) || strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "~") || strings.Contains(raw, "\\") || strings.Contains(raw, ":") {
		return "", fmt.Errorf("path must be a portable relative path")
	}
	for _, part := range strings.Split(raw, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("path traversal is not allowed")
		}
	}
	if clean := path.Clean(raw); clean == "." || clean != raw {
		return "", fmt.Errorf("path must be normalized relative to the flow data root")
	}
	return raw, nil
}

func staticDataCoordinate(packageKey, flowID, relative string) string {
	return strings.TrimSpace(packageKey) + "\x00" + strings.TrimSpace(flowID) + "\x00" + strings.TrimSpace(relative)
}

func staticAgentDeclarationKey(packageKey, flowID, logicalID string) string {
	return strings.TrimSpace(packageKey) + "\x00" + strings.TrimSpace(flowID) + "\x00" + strings.TrimSpace(logicalID)
}

func staticDataContentType(relative string) string {
	switch strings.ToLower(filepath.Ext(relative)) {
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

func cloneStaticData(values []durabledata.StaticData) []durabledata.StaticData {
	if len(values) == 0 {
		return nil
	}
	out := make([]durabledata.StaticData, len(values))
	for index, value := range values {
		value.Content = append([]byte(nil), value.Content...)
		out[index] = value
	}
	return out
}

func (b *WorkflowContractBundle) StaticData() []durabledata.StaticData {
	if b == nil {
		return nil
	}
	return cloneStaticData(b.staticData)
}

func (b *WorkflowContractBundle) StaticDataForAgent(packageKey, flowID, logicalID string) []durabledata.StaticData {
	if b == nil {
		return nil
	}
	ids := b.staticDataAccess[staticAgentDeclarationKey(packageKey, flowID, logicalID)]
	if len(ids) == 0 {
		return nil
	}
	byID := make(map[durabledata.StaticDataID]durabledata.StaticData, len(b.staticData))
	for _, item := range b.staticData {
		byID[item.StaticID] = item
	}
	out := make([]durabledata.StaticData, 0, len(ids))
	for _, id := range ids {
		item, ok := byID[id]
		if !ok {
			return nil
		}
		item.Content = append([]byte(nil), item.Content...)
		out = append(out, item)
	}
	return out
}
