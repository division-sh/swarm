package bundlecatalog

import (
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("bundle not found")
	ErrInvalidCursor = errors.New("invalid bundle catalog cursor")
	ErrConflict      = errors.New("bundle catalog conflict")
)

type ListOptions struct {
	Limit  int
	Cursor string
}

type ListResult struct {
	Bundles    []Summary `json:"bundles"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

type Summary struct {
	BundleHash    string         `json:"bundle_hash"`
	AgentCount    int            `json:"agent_count"`
	HasData       bool           `json:"has_data"`
	DataSizeBytes int64          `json:"data_size_bytes"`
	Metadata      map[string]any `json:"metadata"`
	IngestedAt    time.Time      `json:"ingested_at"`
}

type Detail struct {
	BundleHash    string         `json:"bundle_hash"`
	ContentYAML   string         `json:"content_yaml"`
	ParsedJSON    map[string]any `json:"parsed_json"`
	Metadata      map[string]any `json:"metadata"`
	AgentCount    int            `json:"agent_count"`
	HasData       bool           `json:"has_data"`
	DataSizeBytes int64          `json:"data_size_bytes"`
	IngestedAt    time.Time      `json:"ingested_at"`
}

type AgentsResult struct {
	Agents []AgentDefinition `json:"agents"`
}

type AgentDefinition struct {
	AgentID           string   `json:"agent_id"`
	FlowInstance      string   `json:"flow_instance,omitempty"`
	Role              string   `json:"role,omitempty"`
	Type              string   `json:"type,omitempty"`
	Model             string   `json:"model,omitempty"`
	LLMBackend        string   `json:"llm_backend,omitempty"`
	Memory            bool     `json:"memory"`
	MemorySource      string   `json:"memory_source"`
	IntentKind        string   `json:"intent_kind"`
	IntentSource      string   `json:"intent_source"`
	IntentProvenance  string   `json:"intent_provenance"`
	IntentContentHash string   `json:"intent_content_hash"`
	IntentIdentity    string   `json:"intent_identity"`
	IntentContent     string   `json:"intent_content"`
	Subscriptions     []string `json:"subscriptions,omitempty"`
	Tools             []string `json:"tools,omitempty"`
}

type Upsert struct {
	BundleHash  string
	ContentYAML string
	ParsedJSON  map[string]any
	DataBlob    []byte
	Metadata    map[string]any
}

type UpsertResult struct {
	Detail     Detail `json:"bundle"`
	Registered bool   `json:"registered"`
}

type ConflictError struct{ BundleHash string }

func (e *ConflictError) Error() string {
	return "bundle catalog row already exists with different content"
}

func (e *ConflictError) Is(target error) bool { return target == ErrConflict }
