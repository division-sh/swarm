package bundlecatalog

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

	bundlecatalogcontract "github.com/division-sh/swarm/internal/bundlecatalog"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	"gopkg.in/yaml.v3"
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

func (s *Postgres) ListBundleCatalogAgents(ctx context.Context, bundleHash string) (bundlecatalogcontract.AgentsResult, error) {
	detail, err := s.LoadBundleCatalog(ctx, bundleHash)
	if err != nil {
		return bundlecatalogcontract.AgentsResult{}, err
	}
	agents, err := projectBundleCatalogAgents(detail.ParsedJSON, detail.ContentYAML)
	if err != nil {
		return bundlecatalogcontract.AgentsResult{}, err
	}
	if agents == nil {
		agents = []bundlecatalogcontract.AgentDefinition{}
	}
	return bundlecatalogcontract.AgentsResult{Agents: agents}, nil
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
	agents, err := projectBundleCatalogAgents(parsed, r.ContentYAML)
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

func projectBundleCatalogAgents(parsed map[string]any, contentYAML string) ([]bundlecatalogcontract.AgentDefinition, error) {
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
		return []bundlecatalogcontract.AgentDefinition{}, nil
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

func extractBundleCatalogAgents(root map[string]any) ([]bundlecatalogcontract.AgentDefinition, bool, error) {
	var out []bundlecatalogcontract.AgentDefinition
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
		out = []bundlecatalogcontract.AgentDefinition{}
	}
	return out, found, nil
}

func extractBundleCatalogFlowAgents(raw any) ([]bundlecatalogcontract.AgentDefinition, error) {
	switch flows := raw.(type) {
	case map[string]any:
		names := make([]string, 0, len(flows))
		for name := range flows {
			names = append(names, name)
		}
		sort.Strings(names)
		var out []bundlecatalogcontract.AgentDefinition
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
		var out []bundlecatalogcontract.AgentDefinition
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

func extractBundleCatalogAgentCollection(raw any, flowInstance string) ([]bundlecatalogcontract.AgentDefinition, error) {
	switch agents := raw.(type) {
	case map[string]any:
		names := make([]string, 0, len(agents))
		for name := range agents {
			names = append(names, name)
		}
		sort.Strings(names)
		out := make([]bundlecatalogcontract.AgentDefinition, 0, len(names))
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
		out := make([]bundlecatalogcontract.AgentDefinition, 0, len(agents))
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

func projectBundleCatalogAgentDefinition(agentID, flowInstance string, def map[string]any) (bundlecatalogcontract.AgentDefinition, error) {
	if _, retired := def["prompt_path"]; retired {
		return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: RETIRED field prompt_path is not accepted; use canonical intent metadata")
	}
	for key := range def {
		if bundleCatalogRuntimeAgentFields[key] {
			return bundlecatalogcontract.AgentDefinition{}, fmt.Errorf("bundle catalog agents projection failed: runtime field %q is not allowed", key)
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
	subscriptions, err := optionalStringListFromMap(def, "subscriptions")
	if err != nil {
		return bundlecatalogcontract.AgentDefinition{}, err
	}
	tools, err := optionalStringListFromMap(def, "tools")
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
		FlowInstance:      strings.TrimSpace(flowInstance),
		Role:              stringFromMap(def, "role"),
		Type:              stringFromMap(def, "type"),
		Model:             stringFromMap(def, "model"),
		LLMBackend:        stringFromMap(def, "llm_backend"),
		Memory:            boolFromMap(def, "memory"),
		MemorySource:      stringFromMap(def, "memory_source"),
		IntentKind:        string(intent.Kind),
		IntentSource:      intent.Coordinate,
		IntentProvenance:  intent.Provenance,
		IntentContentHash: intent.ContentHash,
		IntentIdentity:    intent.Identity,
		IntentContent:     intent.Content,
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

func stringFromMap(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func exactStringFromMap(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func boolFromMap(values map[string]any, key string) bool {
	value, _ := values[key].(bool)
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
