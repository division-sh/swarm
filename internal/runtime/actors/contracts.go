package actors

import "encoding/json"

// MandateDocument is the handoff artifact from factory to an operating flow.
type MandateDocument struct {
	VerticalID        string          `json:"vertical_id"`
	Geography         string          `json:"geography,omitempty"`
	LaunchTargets     json.RawMessage `json:"launch_targets,omitempty"`
	FounderDirectives string          `json:"founder_directives,omitempty"`
	BusinessBrief     json.RawMessage `json:"business_brief,omitempty"`
	MVPSpec           json.RawMessage `json:"mvp_spec,omitempty"`
	Brand             json.RawMessage `json:"brand,omitempty"`
	CTOFeasibility    json.RawMessage `json:"cto_feasibility,omitempty"`
	FounderNotes      string          `json:"founder_notes,omitempty"`
	Budget            json.RawMessage `json:"budget,omitempty"`
	Infrastructure    json.RawMessage `json:"infrastructure,omitempty"`
}

// DeployManifest is the deployment contract exchanged between runtime agents.
type DeployManifest struct {
	VerticalID        string            `json:"vertical_id"`
	VerticalName      string            `json:"vertical_name,omitempty"`
	Environment       string            `json:"environment"`
	BinaryPath        string            `json:"binary_path,omitempty"`
	MigrationSQL      string            `json:"migration_sql,omitempty"`
	ConfigOverrides   map[string]string `json:"config,omitempty"`
	HealthEndpoint    string            `json:"health_endpoint,omitempty"`
	SkipStaging       bool              `json:"skip_staging,omitempty"`
	Version           int               `json:"version"`
	RollbackMigration string            `json:"rollback_migration,omitempty"`
}
