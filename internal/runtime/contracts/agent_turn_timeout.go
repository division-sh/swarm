package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

func validateAgentTurnTimeoutContracts(bundle *WorkflowContractBundle) []error {
	var errs []error
	for _, declaration := range bundle.AgentDeclarationRecords() {
		timeout := declaration.Entry.TurnTimeout
		if timeout == nil {
			continue
		}
		if err := timeout.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("agent %s turn_timeout: %w", declaration.OwnerURI, err))
			continue
		}
		schema, _, found := EventSchemaForFlowEvent(bundle, declaration.OwnerFlowID, timeout.Emit)
		if !found {
			errs = append(errs, fmt.Errorf("agent %s turn_timeout.emit references unknown event %s", declaration.OwnerURI, timeout.Emit))
			continue
		}
		if err := eventschema.ValidatePayloadAgainstSchema(schema.Schema, map[string]any{}); err != nil {
			errs = append(errs, fmt.Errorf("agent %s turn_timeout.emit %s cannot admit its empty payload: %w", declaration.OwnerURI, timeout.Emit, err))
		}
	}
	return errs
}

// ProducedEvents includes the platform-owned timeout reaction without granting
// the agent an additional emit tool.
func (a AgentRegistryEntry) ProducedEvents() []string {
	produced := append([]string(nil), a.EmitEvents...)
	if a.TurnTimeout != nil {
		produced = append(produced, a.TurnTimeout.Emit)
	}
	return normalizeStrings(produced)
}
