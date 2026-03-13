package actors

import (
	"encoding/json"
	"strings"
)

// MandateDocument is the handoff artifact from factory to an operating flow.
type MandateDocument struct {
	EntityID          string          `json:"entity_id,omitempty"`
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

func (m MandateDocument) EffectiveEntityID() string { return strings.TrimSpace(m.EntityID) }

func (m *MandateDocument) NormalizeEntityID() {
	if m == nil {
		return
	}
	entityID := m.EffectiveEntityID()
	m.EntityID = entityID
}

// DeployManifest is the deployment contract exchanged between runtime agents.
type DeployManifest struct {
	EntityID          string            `json:"entity_id,omitempty"`
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

func (m DeployManifest) EffectiveEntityID() string { return strings.TrimSpace(m.EntityID) }

func (m *DeployManifest) NormalizeEntityID() {
	if m == nil {
		return
	}
	entityID := m.EffectiveEntityID()
	m.EntityID = entityID
}
