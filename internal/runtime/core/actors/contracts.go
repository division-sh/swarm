package actors

import (
	"encoding/json"
	"strings"
)

// MandateDocument is a generic handoff artifact carried as opaque metadata.
type MandateDocument struct {
	EntityID string          `json:"entity_id,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func (m MandateDocument) EffectiveEntityID() string { return strings.TrimSpace(m.EntityID) }

func (m *MandateDocument) NormalizeEntityID() {
	if m == nil {
		return
	}
	entityID := m.EffectiveEntityID()
	m.EntityID = entityID
}

// DeployManifest is the generic deployment contract exchanged between runtime agents.
type DeployManifest struct {
	EntityID          string            `json:"entity_id,omitempty"`
	Environment       string            `json:"environment"`
	BinaryPath        string            `json:"binary_path,omitempty"`
	MigrationSQL      string            `json:"migration_sql,omitempty"`
	ConfigOverrides   map[string]string `json:"config,omitempty"`
	HealthEndpoint    string            `json:"health_endpoint,omitempty"`
	SkipStaging       bool              `json:"skip_staging,omitempty"`
	Version           int               `json:"version"`
	RollbackMigration string            `json:"rollback_migration,omitempty"`
	Metadata          json.RawMessage   `json:"metadata,omitempty"`
}

func (m DeployManifest) EffectiveEntityID() string { return strings.TrimSpace(m.EntityID) }

func (m *DeployManifest) NormalizeEntityID() {
	if m == nil {
		return
	}
	entityID := m.EffectiveEntityID()
	m.EntityID = entityID
}
