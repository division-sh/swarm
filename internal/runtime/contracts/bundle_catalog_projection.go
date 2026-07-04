package contracts

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const bundleCatalogProjectionVersion = "swarm.bundle.catalog.v1"

type BundleCatalogProjection struct {
	BundleHash  string
	ContentYAML string
	ParsedJSON  map[string]any
	DataBlob    []byte
	Metadata    map[string]any
}

type BundleCatalogProjectionOptions struct {
	Source             string
	PlatformSpecSHA256 string
}

type bundleCatalogProjectedFile struct {
	Label     string `json:"label" yaml:"label"`
	Policy    string `json:"policy" yaml:"policy"`
	SizeBytes int    `json:"size_bytes" yaml:"size_bytes"`
}

type bundleCatalogDataArchive struct {
	ProjectionVersion string                   `json:"projection_version"`
	Entries           []bundleCatalogDataEntry `json:"entries"`
}

type bundleCatalogDataEntry struct {
	Label         string `json:"label" yaml:"label"`
	SizeBytes     int    `json:"size_bytes" yaml:"size_bytes"`
	ContentBase64 string `json:"content_base64" yaml:"content_base64"`
}

// BuildBundleCatalogProjection is the shared owner for persisted bundle rows.
// It stores canonical definition bytes and definition-only JSON; runtime state
// and server-local paths are intentionally excluded.
func BuildBundleCatalogProjection(bundle *WorkflowContractBundle) (BundleCatalogProjection, error) {
	return BuildBundleCatalogProjectionWithOptions(bundle, BundleCatalogProjectionOptions{
		Source: "swarm serve --contracts",
	})
}

func BuildBundleCatalogProjectionWithOptions(bundle *WorkflowContractBundle, opts BundleCatalogProjectionOptions) (BundleCatalogProjection, error) {
	if err := ValidateBundlePlatformVersionCompatibility(bundle); err != nil {
		return BundleCatalogProjection{}, err
	}
	bundleHash, err := BundleHash(bundle)
	if err != nil {
		return BundleCatalogProjection{}, err
	}
	entries, err := bundleHashEntries(bundle)
	if err != nil {
		return BundleCatalogProjection{}, err
	}
	files := make([]bundleCatalogProjectedFile, 0, len(entries))
	dataEntries := make([]bundleCatalogDataEntry, 0)
	contentFiles := make([]bundleCatalogDataEntry, 0)
	for _, entry := range entries {
		content, err := canonicalBundleHashContent(entry.Path, entry.Policy)
		if err != nil {
			return BundleCatalogProjection{}, fmt.Errorf("canonicalize bundle catalog input %s: %w", entry.Label, err)
		}
		policy := bundleCatalogPolicyName(entry.Policy)
		files = append(files, bundleCatalogProjectedFile{
			Label:     entry.Label,
			Policy:    policy,
			SizeBytes: len(content),
		})
		projected := bundleCatalogDataEntry{
			Label:         entry.Label,
			SizeBytes:     len(content),
			ContentBase64: base64.StdEncoding.EncodeToString(content),
		}
		if entry.Policy == bundleHashRaw {
			dataEntries = append(dataEntries, projected)
		} else {
			contentFiles = append(contentFiles, projected)
		}
	}

	contentYAML := renderBundleCatalogContentYAML(contentFiles, files)
	dataBlob, err := renderBundleCatalogDataBlob(dataEntries)
	if err != nil {
		return BundleCatalogProjection{}, err
	}
	parsed := map[string]any{
		"projection_version": bundleCatalogProjectionVersion,
		"package":            bundleCatalogPackageJSON(bundle.Package),
		"workflow": map[string]any{
			"name":    strings.TrimSpace(bundle.Semantics.Name),
			"version": strings.TrimSpace(bundle.Semantics.Version),
		},
		"files":  bundleCatalogFilesJSON(files),
		"agents": bundleCatalogAgentsJSON(bundle),
	}
	metadata := map[string]any{
		"projection_version": bundleCatalogProjectionVersion,
		"source":             firstNonEmpty(opts.Source, "swarm serve --contracts"),
		"workflow_name":      strings.TrimSpace(bundle.Semantics.Name),
		"workflow_version":   strings.TrimSpace(bundle.Semantics.Version),
		"file_count":         len(files),
		"data_file_count":    len(dataEntries),
	}
	addBundleCatalogPackageMetadata(metadata, bundle.Package)
	if hash := strings.TrimSpace(opts.PlatformSpecSHA256); hash != "" {
		metadata["platform_spec_sha256"] = hash
	}
	return BundleCatalogProjection{
		BundleHash:  bundleHash,
		ContentYAML: contentYAML,
		ParsedJSON:  parsed,
		DataBlob:    dataBlob,
		Metadata:    metadata,
	}, nil
}

func bundleCatalogPolicyName(policy bundleHashContentPolicy) string {
	switch policy {
	case bundleHashYAML:
		return "yaml"
	case bundleHashPrompt:
		return "prompt_text"
	case bundleHashRaw:
		return "raw_data"
	default:
		return "unknown"
	}
}

func renderBundleCatalogContentYAML(contentFiles []bundleCatalogDataEntry, files []bundleCatalogProjectedFile) string {
	var b strings.Builder
	b.WriteString("projection_version: ")
	b.WriteString(bundleCatalogProjectionVersion)
	b.WriteString("\nfiles:\n")
	for _, file := range contentFiles {
		b.WriteString("  - label: ")
		b.WriteString(strconv.Quote(file.Label))
		b.WriteString("\n    content_base64: ")
		b.WriteString(strconv.Quote(file.ContentBase64))
		b.WriteString("\n    size_bytes: ")
		b.WriteString(strconv.Itoa(file.SizeBytes))
		b.WriteString("\n")
	}
	b.WriteString("canonical_inputs:\n")
	for _, file := range files {
		b.WriteString("  - label: ")
		b.WriteString(strconv.Quote(file.Label))
		b.WriteString("\n    policy: ")
		b.WriteString(file.Policy)
		b.WriteString("\n    size_bytes: ")
		b.WriteString(strconv.Itoa(file.SizeBytes))
		b.WriteString("\n")
	}
	return b.String()
}

func renderBundleCatalogDataBlob(entries []bundleCatalogDataEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(bundleCatalogDataArchive{
		ProjectionVersion: bundleCatalogProjectionVersion,
		Entries:           entries,
	})
	if err != nil {
		return nil, fmt.Errorf("encode bundle catalog data blob: %w", err)
	}
	return raw, nil
}

func bundleCatalogFilesJSON(files []bundleCatalogProjectedFile) []map[string]any {
	out := make([]map[string]any, 0, len(files))
	for _, file := range files {
		out = append(out, map[string]any{
			"label":      file.Label,
			"policy":     file.Policy,
			"size_bytes": file.SizeBytes,
		})
	}
	return out
}

func bundleCatalogPackageJSON(pkg ProjectPackageDocument) map[string]any {
	out := map[string]any{}
	addStringField(out, "name", pkg.Name)
	addStringField(out, "version", pkg.Version)
	addStringField(out, "platform_version", pkg.PlatformVersion)
	addPackageStringListField(out, "keywords", pkg.Keywords)
	addStringField(out, "license", pkg.License)
	addStringField(out, "repository", pkg.Repository)
	if extra := packageExtraJSON(pkg.Extra); len(extra) > 0 {
		out["extra"] = extra
	}
	return out
}

func addBundleCatalogPackageMetadata(metadata map[string]any, pkg ProjectPackageDocument) {
	addStringField(metadata, "package_name", pkg.Name)
	addStringField(metadata, "package_version", pkg.Version)
	addStringField(metadata, "package_platform_version", pkg.PlatformVersion)
	addPackageStringListField(metadata, "package_keywords", pkg.Keywords)
	addStringField(metadata, "package_license", pkg.License)
	addStringField(metadata, "package_repository", pkg.Repository)
	if extra := packageExtraJSON(pkg.Extra); len(extra) > 0 {
		metadata["package_extra"] = extra
	}
}

func addPackageStringListField(values map[string]any, key string, raw []string) {
	items := make([]string, 0, len(raw))
	for _, item := range raw {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return
	}
	values[key] = items
}

func packageExtraJSON(extra map[string]string) map[string]string {
	if len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(extra))
	for key, value := range extra {
		if key = strings.TrimSpace(key); key != "" {
			out[key] = value
		}
	}
	return out
}

func bundleCatalogAgentsJSON(bundle *WorkflowContractBundle) map[string]any {
	entries := bundle.AgentEntries()
	agentIDs := make([]string, 0, len(entries))
	for agentID := range entries {
		agentIDs = append(agentIDs, agentID)
	}
	sort.Strings(agentIDs)
	out := make(map[string]any, len(agentIDs))
	for _, agentID := range agentIDs {
		entry := entries[agentID]
		def := map[string]any{
			"agent_id": agentID,
		}
		addStringField(def, "role", entry.Role)
		addStringField(def, "type", firstNonEmpty(entry.Type, entry.NodeType))
		addStringField(def, "model", entry.Model)
		addStringField(def, "mode", entry.ConversationMode)
		addStringField(def, "session_scope", entry.SessionScope)
		addStringField(def, "prompt_path", entry.PromptRef)
		if source, ok := bundle.AgentContractSource(agentID); ok {
			addStringField(def, "flow_instance", source.FlowID)
		}
		addStringListField(def, "subscriptions", entry.Subscriptions)
		addStringListField(def, "tools", entry.ConfiguredTools())
		out[agentID] = def
	}
	return out
}

func addStringField(values map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values[key] = value
	}
}

func addStringListField(values map[string]any, key string, raw []string) {
	items := make([]string, 0, len(raw))
	for _, item := range raw {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return
	}
	sort.Strings(items)
	values[key] = items
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
