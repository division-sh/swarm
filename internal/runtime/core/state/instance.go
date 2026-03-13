package state

import (
	"encoding/json"
	"time"

	coreidentity "empireai/internal/runtime/core/identity"
)

// Instance is the platform-owned workflow instance record defined by the MAS
// platform spec. Product/domain data belongs in Metadata, not as top-level
// platform fields.
type Instance struct {
	InstanceID      coreidentity.EntityID `json:"instance_id"`
	WorkflowName    string                `json:"workflow_name"`
	WorkflowVersion string                `json:"workflow_version,omitempty"`
	CurrentState    string                `json:"current_state"`
	EnteredStageAt  time.Time             `json:"entered_stage_at,omitempty"`
	Metadata        json.RawMessage       `json:"metadata,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
}
