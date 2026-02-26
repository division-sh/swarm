package models

import "encoding/json"

type AgentConfig struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Role           string          `json:"role"`
	Mode           string          `json:"mode"`
	Subscriptions  []string        `json:"subscriptions,omitempty"`
	VerticalID     string          `json:"vertical_id,omitempty"`
	ParentAgent    string          `json:"parent_agent_id,omitempty"`
	Config         json.RawMessage `json:"config,omitempty"`
	BudgetEnvelope float64         `json:"budget_envelope,omitempty"`
}

// MandateDocument is the handoff artifact from factory to operating company spinup.
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

// DeployManifest is the deployment contract emitted by build/release paths.
// Kept intentionally explicit so orchestration and audit consumers can rely on
// a stable typed payload instead of ad-hoc maps.
type DeployManifest struct {
	VerticalID      string          `json:"vertical_id"`
	Environment     string          `json:"environment"`
	Version         string          `json:"version"`
	Image           string          `json:"image,omitempty"`
	URL             string          `json:"url,omitempty"`
	HealthcheckURL  string          `json:"healthcheck_url,omitempty"`
	RollbackVersion string          `json:"rollback_version,omitempty"`
	Config          json.RawMessage `json:"config,omitempty"`
}
