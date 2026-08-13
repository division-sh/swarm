package bundlecatalog

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
)

type bundleCatalogCursor struct {
	IngestedAt string `json:"ingested_at"`
	BundleHash string `json:"bundle_hash"`
}

type bundleCatalogRow struct {
	BundleHash    string
	ContentYAML   string
	ParsedJSONRaw []byte
	MetadataRaw   []byte
	HasData       bool
	DataSizeBytes int64
	IngestedAt    time.Time
}

func defaultBundleCatalogListOptions(opts bundlecatalogcontract.ListOptions) bundlecatalogcontract.ListOptions {
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts
}

func (s *Postgres) requireBundleCatalogAccess() error {
	return s.requireCurrentSchema()
}

func (s *Postgres) ListBundleCatalog(ctx context.Context, opts bundlecatalogcontract.ListOptions) (bundlecatalogcontract.ListResult, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return bundlecatalogcontract.ListResult{}, err
	}
	opts = defaultBundleCatalogListOptions(opts)
	args := make([]any, 0, 3)
	where := []string{"TRUE"}
	if opts.Cursor != "" {
		ingestedAt, bundleHash, err := decodeBundleCatalogCursor(opts.Cursor)
		if err != nil {
			return bundlecatalogcontract.ListResult{}, err
		}
		args = append(args, ingestedAt.UTC(), bundleHash)
		where = append(where, fmt.Sprintf("(ingested_at < $%d OR (ingested_at = $%d AND bundle_hash < $%d))", len(args)-1, len(args)-1, len(args)))
	}
	args = append(args, opts.Limit+1)
	rows, err := s.backend.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			bundle_hash,
			content_yaml,
			COALESCE(parsed_json, '{}'::jsonb),
			COALESCE(metadata, '{}'::jsonb),
			data_blob IS NOT NULL,
			COALESCE(octet_length(data_blob), 0)::bigint,
			ingested_at
		FROM bundles
		WHERE %s
		ORDER BY ingested_at DESC, bundle_hash DESC
		LIMIT $%d
	`, strings.Join(where, " AND "), len(args)), args...)
	if err != nil {
		return bundlecatalogcontract.ListResult{}, fmt.Errorf("list bundle catalog: %w", err)
	}
	defer rows.Close()

	bundles := make([]bundlecatalogcontract.Summary, 0, opts.Limit)
	for rows.Next() {
		row, err := scanBundleCatalogRow(rows)
		if err != nil {
			return bundlecatalogcontract.ListResult{}, err
		}
		detail, err := row.toDetail()
		if err != nil {
			return bundlecatalogcontract.ListResult{}, err
		}
		bundles = append(bundles, bundlecatalogcontract.Summary{
			BundleHash:    detail.BundleHash,
			AgentCount:    detail.AgentCount,
			HasData:       detail.HasData,
			DataSizeBytes: detail.DataSizeBytes,
			Metadata:      detail.Metadata,
			IngestedAt:    detail.IngestedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return bundlecatalogcontract.ListResult{}, fmt.Errorf("read bundle catalog: %w", err)
	}

	nextCursor := ""
	if len(bundles) > opts.Limit {
		bundles = bundles[:opts.Limit]
		nextCursor = encodeBundleCatalogCursor(bundles[len(bundles)-1])
	}
	if bundles == nil {
		bundles = []bundlecatalogcontract.Summary{}
	}
	return bundlecatalogcontract.ListResult{Bundles: bundles, NextCursor: nextCursor}, nil
}

func (s *Postgres) LoadBundleCatalog(ctx context.Context, bundleHash string) (bundlecatalogcontract.Detail, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return bundlecatalogcontract.Detail{}, bundlecatalogcontract.ErrNotFound
	}
	row := s.backend.QueryRowContext(ctx, `
		SELECT
			bundle_hash,
			content_yaml,
			COALESCE(parsed_json, '{}'::jsonb),
			COALESCE(metadata, '{}'::jsonb),
			data_blob IS NOT NULL,
			COALESCE(octet_length(data_blob), 0)::bigint,
			ingested_at
		FROM bundles
		WHERE bundle_hash = $1
	`, bundleHash)
	scanned, err := scanBundleCatalogRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return bundlecatalogcontract.Detail{}, bundlecatalogcontract.ErrNotFound
	}
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	return scanned.toDetail()
}

func (s *Postgres) LoadBundleCatalogRuntimeRecord(ctx context.Context, bundleHash string) (runtimerunbundle.BundleCatalogRuntimeRecord, error) {
	if err := s.requireBundleCatalogAccess(); err != nil {
		return runtimerunbundle.BundleCatalogRuntimeRecord{}, err
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return runtimerunbundle.BundleCatalogRuntimeRecord{}, runtimerunbundle.ErrBundleNotFound
	}
	var out runtimerunbundle.BundleCatalogRuntimeRecord
	var dataBlob []byte
	var hasData bool
	err := s.backend.QueryRowContext(ctx, `
		SELECT
			bundle_hash,
			content_yaml,
			COALESCE(data_blob, ''::bytea),
			data_blob IS NOT NULL
		FROM bundles
		WHERE bundle_hash = $1
	`, bundleHash).Scan(&out.BundleHash, &out.ContentYAML, &dataBlob, &hasData)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunbundle.BundleCatalogRuntimeRecord{}, runtimerunbundle.ErrBundleNotFound
	}
	if err != nil {
		return runtimerunbundle.BundleCatalogRuntimeRecord{}, fmt.Errorf("load bundle catalog runtime record: %w", err)
	}
	out.BundleHash = strings.TrimSpace(out.BundleHash)
	if hasData {
		out.DataBlob = append([]byte(nil), dataBlob...)
	}
	return out, nil
}

func (s *Postgres) ListBundleCatalogAgents(ctx context.Context, bundleHash string, opts bundlecatalogcontract.AgentListOptions) (bundlecatalogcontract.AgentsResult, error) {
	detail, err := s.LoadBundleCatalog(ctx, bundleHash)
	if err != nil {
		return bundlecatalogcontract.AgentsResult{}, err
	}
	return pageBundleCatalogAgents(detail.BundleHash, detail.ParsedJSON, opts)
}

type bundleCatalogScanner interface {
	Scan(dest ...any) error
}

func scanBundleCatalogRow(row bundleCatalogScanner) (bundleCatalogRow, error) {
	var out bundleCatalogRow
	if err := row.Scan(
		&out.BundleHash,
		&out.ContentYAML,
		&out.ParsedJSONRaw,
		&out.MetadataRaw,
		&out.HasData,
		&out.DataSizeBytes,
		&out.IngestedAt,
	); err != nil {
		return bundleCatalogRow{}, err
	}
	out.BundleHash = strings.TrimSpace(out.BundleHash)
	out.IngestedAt = out.IngestedAt.UTC()
	return out, nil
}

func (r bundleCatalogRow) toDetail() (bundlecatalogcontract.Detail, error) {
	parsed, err := decodeBundleCatalogJSONMap(r.ParsedJSONRaw, "parsed_json")
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	metadata, err := decodeBundleCatalogJSONMap(r.MetadataRaw, "metadata")
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	agents, err := projectBundleCatalogAgents(parsed)
	if err != nil {
		return bundlecatalogcontract.Detail{}, err
	}
	return bundlecatalogcontract.Detail{
		BundleHash:    r.BundleHash,
		ContentYAML:   r.ContentYAML,
		ParsedJSON:    parsed,
		Metadata:      metadata,
		AgentCount:    len(agents),
		HasData:       r.HasData,
		DataSizeBytes: r.DataSizeBytes,
		IngestedAt:    r.IngestedAt,
	}, nil
}

func decodeBundleCatalogJSONMap(raw []byte, field string) (map[string]any, error) {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("bundle catalog %s must be a JSON object: %w", field, err)
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func encodeBundleCatalogCursor(summary bundlecatalogcontract.Summary) string {
	raw, _ := json.Marshal(bundleCatalogCursor{
		IngestedAt: summary.IngestedAt.UTC().Format(time.RFC3339Nano),
		BundleHash: strings.TrimSpace(summary.BundleHash),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeBundleCatalogCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return time.Time{}, "", bundlecatalogcontract.ErrInvalidCursor
	}
	var decoded bundleCatalogCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return time.Time{}, "", bundlecatalogcontract.ErrInvalidCursor
	}
	ingestedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.IngestedAt))
	if err != nil {
		return time.Time{}, "", bundlecatalogcontract.ErrInvalidCursor
	}
	bundleHash := strings.TrimSpace(decoded.BundleHash)
	if bundleHash == "" {
		return time.Time{}, "", bundlecatalogcontract.ErrInvalidCursor
	}
	return ingestedAt.UTC(), bundleHash, nil
}

func projectBundleCatalogAgentDefinition(agentID, flowInstance string, def map[string]any) (bundlecatalogcontract.AgentDefinition, error) {
	if _, retired := def["prompt_path"]; retired {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: RETIRED field prompt_path is not accepted; use canonical intent metadata")
	}
	for key := range def {
		if bundleCatalogRuntimeAgentFields[key] {
			return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: runtime field %q is not allowed", key)
		}
		if !bundleCatalogAgentDefinitionFields[key] {
			return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: unknown field %q is not allowed", key)
		}
	}
	if agentID == "" {
		agentID = stringFromMap(def, "agent_id")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: agent_id is required")
	}
	if flowInstance == "" {
		flowInstance = stringFromMap(def, "flow_instance")
	}
	agentNameOwner := stringFromMap(def, "agent_name_owner")
	if agentNameOwner == "" {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: agent_name_owner is required")
	}
	memory, ok := def["memory"].(bool)
	if !ok {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: memory is required and must be a boolean")
	}
	memorySource := stringFromMap(def, "memory_source")
	if memorySource == "" {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: memory_source is required")
	}
	subscriptions, err := optionalStringListFromMap(def, "subscriptions")
	if err != nil {
		return bundlecatalogcontract.AgentDefinition{}, err
	}
	tools, err := optionalStringListFromMap(def, "tools")
	if err != nil {
		return bundlecatalogcontract.AgentDefinition{}, err
	}
	criteria, err := optionalStringListFromMap(def, "criteria")
	if err != nil {
		return bundlecatalogcontract.AgentDefinition{}, err
	}
	intent := runtimeagentintent.Resolved{
		Kind:        runtimeagentintent.SourceKind(stringFromMap(def, "intent_kind")),
		Coordinate:  stringFromMap(def, "intent_source"),
		Provenance:  stringFromMap(def, "intent_provenance"),
		ContentHash: stringFromMap(def, "intent_content_hash"),
		Identity:    stringFromMap(def, "intent_identity"),
		Content:     exactStringFromMap(def, "intent_content"),
	}
	if err := intent.Validate(); err != nil {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agent %s intent: %w", agentID, err)
	}
	return bundlecatalogcontract.AgentDefinition{
		AgentID:           agentID,
		AgentNameOwner:    agentNameOwner,
		FlowInstance:      strings.TrimSpace(flowInstance),
		Role:              stringFromMap(def, "role"),
		Type:              stringFromMap(def, "type"),
		Model:             stringFromMap(def, "model"),
		LLMBackend:        stringFromMap(def, "llm_backend"),
		Memory:            memory,
		MemorySource:      memorySource,
		IntentKind:        string(intent.Kind),
		IntentSource:      intent.Coordinate,
		IntentProvenance:  intent.Provenance,
		IntentContentHash: intent.ContentHash,
		IntentIdentity:    intent.Identity,
		IntentContent:     intent.Content,
		Criteria:          criteria,
		ProviderPrompt:    exactStringFromMap(def, "provider_prompt"),
		Subscriptions:     subscriptions,
		Tools:             tools,
	}, nil
}

var bundleCatalogRuntimeAgentFields = map[string]bool{
	"status":                     true,
	"runtime_state":              true,
	"queue":                      true,
	"active":                     true,
	"last_tool_outcome":          true,
	"session_id":                 true,
	"turn_id":                    true,
	"task_id":                    true,
	"pending_deliveries":         true,
	"delivery_lifecycle":         true,
	"watchdog":                   true,
	"oldest_pending_age_seconds": true,
}

var bundleCatalogAgentDefinitionFields = map[string]bool{
	"agent_id":            true,
	"agent_name_owner":    true,
	"flow_instance":       true,
	"role":                true,
	"type":                true,
	"model":               true,
	"llm_backend":         true,
	"memory":              true,
	"memory_source":       true,
	"intent_kind":         true,
	"intent_source":       true,
	"intent_provenance":   true,
	"intent_content_hash": true,
	"intent_identity":     true,
	"intent_content":      true,
	"criteria":            true,
	"provider_prompt":     true,
	"subscriptions":       true,
	"tools":               true,
}

func stringFromMap(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func exactStringFromMap(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func optionalStringListFromMap(values map[string]any, key string) ([]string, error) {
	raw, ok := values[key]
	if !ok || raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("bundle catalog agents projection failed: %s must be an array of strings", key)
	}
	out := make([]string, 0, len(items))
	for i, item := range items {
		text, ok := item.(string)
		text = strings.TrimSpace(text)
		if !ok || text == "" {
			return nil, fmt.Errorf("bundle catalog agents projection failed: %s[%d] must be a non-empty string", key, i)
		}
		out = append(out, text)
	}
	return out, nil
}
