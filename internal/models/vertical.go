package models

import (
	"encoding/json"
	"time"
)

const (
	VerticalModeFactory   = "factory"
	VerticalModeOperating = "operating"
)

type Vertical struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Slug            string          `json:"slug"`
	Geography       string          `json:"geography"`
	Stage           string          `json:"stage"`
	Mode            string          `json:"mode"`
	TemplateVersion string          `json:"template_version,omitempty"`
	HumanNotes      string          `json:"human_notes,omitempty"`
	BusinessBrief   json.RawMessage `json:"business_brief,omitempty"`
	MVPSpec         json.RawMessage `json:"mvp_spec,omitempty"`
	Brand           json.RawMessage `json:"brand,omitempty"`
	DeployConfig    json.RawMessage `json:"deploy_config,omitempty"`
	Credentials     json.RawMessage `json:"credentials,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}
