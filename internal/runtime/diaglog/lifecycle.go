package diaglog

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// LifecycleDiagnostic carries the immutable producer identity, never the
// current projector's runtime identity.
type LifecycleDiagnostic struct {
	OutboxID    string
	OperationID string
	Identity    agentidentity.Identity
	AgentID     string
	EventName   string
	Payload     map[string]any
	CreatedAt   time.Time
}

func (d LifecycleDiagnostic) Validate() error {
	if err := d.Identity.Validate(); err != nil {
		return fmt.Errorf("lifecycle diagnostic identity: %w", err)
	}
	if d.OutboxID == "" || d.OperationID == "" || d.AgentID != d.Identity.AgentID() ||
		d.EventName != "platform.agent_lifecycle_transition" || d.CreatedAt.IsZero() || d.Payload == nil {
		return fmt.Errorf("incomplete lifecycle diagnostic")
	}
	return nil
}
