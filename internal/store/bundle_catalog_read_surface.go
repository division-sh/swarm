package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type BundleCatalogListOptions struct {
	Limit  int
	Cursor string
}

type BundleCatalogListResult struct {
	Bundles    []BundleCatalogSummary `json:"bundles"`
	NextCursor string                 `json:"next_cursor,omitempty"`
}

type BundleCatalogSummary struct {
	BundleHash    string         `json:"bundle_hash"`
	AgentCount    int            `json:"agent_count"`
	HasData       bool           `json:"has_data"`
	DataSizeBytes int64          `json:"data_size_bytes"`
	Metadata      map[string]any `json:"metadata"`
	IngestedAt    time.Time      `json:"ingested_at"`
}

type BundleCatalogDetail struct {
	BundleHash    string         `json:"bundle_hash"`
	ContentYAML   string         `json:"content_yaml"`
	ParsedJSON    map[string]any `json:"parsed_json"`
	Metadata      map[string]any `json:"metadata"`
	AgentCount    int            `json:"agent_count"`
	HasData       bool           `json:"has_data"`
	DataSizeBytes int64          `json:"data_size_bytes"`
	IngestedAt    time.Time      `json:"ingested_at"`
}

type BundleCatalogAgentsResult struct {
	Agents []BundleCatalogAgentDefinition `json:"agents"`
}

type BundleCatalogAgentDefinition struct {
	AgentID          string   `json:"agent_id"`
	FlowInstance     string   `json:"flow_instance,omitempty"`
	Role             string   `json:"role,omitempty"`
	Type             string   `json:"type,omitempty"`
	ModelTier        string   `json:"model_tier,omitempty"`
	LLMBackend       string   `json:"llm_backend,omitempty"`
	ConversationMode string   `json:"conversation_mode,omitempty"`
	SessionScope     string   `json:"session_scope,omitempty"`
	PromptPath       string   `json:"prompt_path,omitempty"`
	Subscriptions    []string `json:"subscriptions,omitempty"`
	Tools            []string `json:"tools,omitempty"`
}

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

func defaultBundleCatalogListOptions(opts BundleCatalogListOptions) BundleCatalogListOptions {
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts
}

func (s *PostgresStore) requireBundleCatalogCapabilities(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if catalog.hasColumns("bundles", "bundle_hash", "content_yaml", "parsed_json", "data_blob", "metadata", "ingested_at") {
		return nil
	}
	return fmt.Errorf("bundle catalog read surface requires bundles columns [bundle_hash content_yaml parsed_json data_blob metadata ingested_at]")
}

func (s *PostgresStore) ListBundleCatalog(ctx context.Context, opts BundleCatalogListOptions) (BundleCatalogListResult, error) {
	if err := s.requireBundleCatalogCapabilities(ctx); err != nil {
		return BundleCatalogListResult{}, err
	}
	opts = defaultBundleCatalogListOptions(opts)
	args := make([]any, 0, 3)
	where := []string{"TRUE"}
	if opts.Cursor != "" {
		ingestedAt, bundleHash, err := decodeBundleCatalogCursor(opts.Cursor)
		if err != nil {
			return BundleCatalogListResult{}, err
		}
		args = append(args, ingestedAt.UTC(), bundleHash)
		where = append(where, fmt.Sprintf("(ingested_at < $%d OR (ingested_at = $%d AND bundle_hash < $%d))", len(args)-1, len(args)-1, len(args)))
	}
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
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
		return BundleCatalogListResult{}, fmt.Errorf("list bundle catalog: %w", err)
	}
	defer rows.Close()

	bundles := make([]BundleCatalogSummary, 0, opts.Limit)
	for rows.Next() {
		row, err := scanBundleCatalogRow(rows)
		if err != nil {
			return BundleCatalogListResult{}, err
		}
		detail, err := row.toDetail()
		if err != nil {
			return BundleCatalogListResult{}, err
		}
		bundles = append(bundles, BundleCatalogSummary{
			BundleHash:    detail.BundleHash,
			AgentCount:    detail.AgentCount,
			HasData:       detail.HasData,
			DataSizeBytes: detail.DataSizeBytes,
			Metadata:      detail.Metadata,
			IngestedAt:    detail.IngestedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return BundleCatalogListResult{}, fmt.Errorf("read bundle catalog: %w", err)
	}

	nextCursor := ""
	if len(bundles) > opts.Limit {
		bundles = bundles[:opts.Limit]
		nextCursor = encodeBundleCatalogCursor(bundles[len(bundles)-1])
	}
	if bundles == nil {
		bundles = []BundleCatalogSummary{}
	}
	return BundleCatalogListResult{Bundles: bundles, NextCursor: nextCursor}, nil
}

func (s *PostgresStore) LoadBundleCatalog(ctx context.Context, bundleHash string) (BundleCatalogDetail, error) {
	if err := s.requireBundleCatalogCapabilities(ctx); err != nil {
		return BundleCatalogDetail{}, err
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return BundleCatalogDetail{}, ErrBundleNotFound
	}
	row := s.DB.QueryRowContext(ctx, `
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
		return BundleCatalogDetail{}, ErrBundleNotFound
	}
	if err != nil {
		return BundleCatalogDetail{}, err
	}
	return scanned.toDetail()
}

func (s *PostgresStore) ListBundleCatalogAgents(ctx context.Context, bundleHash string) (BundleCatalogAgentsResult, error) {
	detail, err := s.LoadBundleCatalog(ctx, bundleHash)
	if err != nil {
		return BundleCatalogAgentsResult{}, err
	}
	agents, err := projectBundleCatalogAgents(detail.ParsedJSON, detail.ContentYAML)
	if err != nil {
		return BundleCatalogAgentsResult{}, err
	}
	if agents == nil {
		agents = []BundleCatalogAgentDefinition{}
	}
	return BundleCatalogAgentsResult{Agents: agents}, nil
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

func (r bundleCatalogRow) toDetail() (BundleCatalogDetail, error) {
	parsed, err := decodeBundleCatalogJSONMap(r.ParsedJSONRaw, "parsed_json")
	if err != nil {
		return BundleCatalogDetail{}, err
	}
	metadata, err := decodeBundleCatalogJSONMap(r.MetadataRaw, "metadata")
	if err != nil {
		return BundleCatalogDetail{}, err
	}
	agents, err := projectBundleCatalogAgents(parsed, r.ContentYAML)
	if err != nil {
		return BundleCatalogDetail{}, err
	}
	return BundleCatalogDetail{
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

func encodeBundleCatalogCursor(summary BundleCatalogSummary) string {
	raw, _ := json.Marshal(bundleCatalogCursor{
		IngestedAt: summary.IngestedAt.UTC().Format(time.RFC3339Nano),
		BundleHash: strings.TrimSpace(summary.BundleHash),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeBundleCatalogCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return time.Time{}, "", ErrInvalidBundleCatalogCursor
	}
	var decoded bundleCatalogCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return time.Time{}, "", ErrInvalidBundleCatalogCursor
	}
	ingestedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.IngestedAt))
	if err != nil {
		return time.Time{}, "", ErrInvalidBundleCatalogCursor
	}
	bundleHash := strings.TrimSpace(decoded.BundleHash)
	if bundleHash == "" {
		return time.Time{}, "", ErrInvalidBundleCatalogCursor
	}
	return ingestedAt.UTC(), bundleHash, nil
}

func projectBundleCatalogAgents(parsed map[string]any, contentYAML string) ([]BundleCatalogAgentDefinition, error) {
	if len(parsed) > 0 {
		agents, found, err := extractBundleCatalogAgents(parsed)
		if err != nil {
			return nil, err
		}
		if found {
			return agents, nil
		}
	}
	contentYAML = strings.TrimSpace(contentYAML)
	if contentYAML == "" {
		return []BundleCatalogAgentDefinition{}, nil
	}
	var decoded any
	if err := yaml.Unmarshal([]byte(contentYAML), &decoded); err != nil {
		return nil, fmt.Errorf("bundle catalog content_yaml projection failed: %w", err)
	}
	root, ok := normalizeBundleYAMLValue(decoded).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("bundle catalog content_yaml projection failed: root must be an object")
	}
	agents, _, err := extractBundleCatalogAgents(root)
	if err != nil {
		return nil, err
	}
	return agents, nil
}

func extractBundleCatalogAgents(root map[string]any) ([]BundleCatalogAgentDefinition, bool, error) {
	var out []BundleCatalogAgentDefinition
	found := false
	if raw, ok := root["agents"]; ok {
		found = true
		agents, err := extractBundleCatalogAgentCollection(raw, "")
		if err != nil {
			return nil, true, err
		}
		out = append(out, agents...)
	}
	if raw, ok := root["flows"]; ok {
		found = true
		flowAgents, err := extractBundleCatalogFlowAgents(raw)
		if err != nil {
			return nil, true, err
		}
		out = append(out, flowAgents...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowInstance != out[j].FlowInstance {
			return out[i].FlowInstance < out[j].FlowInstance
		}
		return out[i].AgentID < out[j].AgentID
	})
	if out == nil {
		out = []BundleCatalogAgentDefinition{}
	}
	return out, found, nil
}

func extractBundleCatalogFlowAgents(raw any) ([]BundleCatalogAgentDefinition, error) {
	switch flows := raw.(type) {
	case map[string]any:
		names := make([]string, 0, len(flows))
		for name := range flows {
			names = append(names, name)
		}
		sort.Strings(names)
		var out []BundleCatalogAgentDefinition
		for _, name := range names {
			flow, ok := flows[name].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bundle catalog agents projection failed: flows.%s must be an object", name)
			}
			rawAgents, ok := flow["agents"]
			if !ok {
				continue
			}
			agents, err := extractBundleCatalogAgentCollection(rawAgents, name)
			if err != nil {
				return nil, err
			}
			out = append(out, agents...)
		}
		return out, nil
	case []any:
		var out []BundleCatalogAgentDefinition
		for i, item := range flows {
			flow, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bundle catalog agents projection failed: flows[%d] must be an object", i)
			}
			rawAgents, ok := flow["agents"]
			if !ok {
				continue
			}
			flowInstance := stringFromMap(flow, "flow_instance")
			agents, err := extractBundleCatalogAgentCollection(rawAgents, flowInstance)
			if err != nil {
				return nil, err
			}
			out = append(out, agents...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("bundle catalog agents projection failed: flows must be an object or array")
	}
}

func extractBundleCatalogAgentCollection(raw any, flowInstance string) ([]BundleCatalogAgentDefinition, error) {
	switch agents := raw.(type) {
	case map[string]any:
		names := make([]string, 0, len(agents))
		for name := range agents {
			names = append(names, name)
		}
		sort.Strings(names)
		out := make([]BundleCatalogAgentDefinition, 0, len(names))
		for _, name := range names {
			def, ok := agents[name].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bundle catalog agents projection failed: agents.%s must be an object", name)
			}
			agent, err := projectBundleCatalogAgentDefinition(name, flowInstance, def)
			if err != nil {
				return nil, err
			}
			out = append(out, agent)
		}
		return out, nil
	case []any:
		out := make([]BundleCatalogAgentDefinition, 0, len(agents))
		for i, item := range agents {
			def, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bundle catalog agents projection failed: agents[%d] must be an object", i)
			}
			agent, err := projectBundleCatalogAgentDefinition("", flowInstance, def)
			if err != nil {
				return nil, err
			}
			out = append(out, agent)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("bundle catalog agents projection failed: agents must be an object or array")
	}
}

func projectBundleCatalogAgentDefinition(agentID, flowInstance string, def map[string]any) (BundleCatalogAgentDefinition, error) {
	for key := range def {
		if bundleCatalogRuntimeAgentFields[key] {
			return BundleCatalogAgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: runtime field %q is not allowed", key)
		}
	}
	if agentID == "" {
		agentID = stringFromMap(def, "agent_id")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return BundleCatalogAgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: agent_id is required")
	}
	if flowInstance == "" {
		flowInstance = stringFromMap(def, "flow_instance")
	}
	subscriptions, err := optionalStringListFromMap(def, "subscriptions")
	if err != nil {
		return BundleCatalogAgentDefinition{}, err
	}
	tools, err := optionalStringListFromMap(def, "tools")
	if err != nil {
		return BundleCatalogAgentDefinition{}, err
	}
	return BundleCatalogAgentDefinition{
		AgentID:          agentID,
		FlowInstance:     strings.TrimSpace(flowInstance),
		Role:             stringFromMap(def, "role"),
		Type:             stringFromMap(def, "type"),
		ModelTier:        stringFromMap(def, "model_tier"),
		LLMBackend:       stringFromMap(def, "llm_backend"),
		ConversationMode: stringFromMap(def, "conversation_mode"),
		SessionScope:     stringFromMap(def, "session_scope"),
		PromptPath:       stringFromMap(def, "prompt_path"),
		Subscriptions:    subscriptions,
		Tools:            tools,
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

func stringFromMap(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
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

func normalizeBundleYAMLValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[strings.TrimSpace(key)] = normalizeBundleYAMLValue(item)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[strings.TrimSpace(fmt.Sprint(key))] = normalizeBundleYAMLValue(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, normalizeBundleYAMLValue(item))
		}
		return out
	default:
		return value
	}
}
