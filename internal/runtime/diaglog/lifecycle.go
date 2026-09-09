package diaglog

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
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

// ProducerLineage decodes only the immutable producer snapshot, never the
// context of whichever consumer settles the diagnostic.
func (d LifecycleDiagnostic) ProducerLineage() (correlation.RuntimeLineage, error) {
	var lineage correlation.RuntimeLineage
	stored, ok := d.Payload["producer_lineage"]
	if !ok {
		return lineage, nil
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return lineage, err
	}
	if err := json.Unmarshal(raw, &lineage); err != nil {
		return lineage, err
	}
	if lineage.RunID != d.Identity.RunID {
		return lineage, fmt.Errorf("lifecycle diagnostic producer lineage differs from immutable run")
	}
	return lineage, nil
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
