package runtime

import runtimecontracts "empireai/internal/runtime/contracts"

var (
	EventSchemaRegistry       = runtimecontracts.EventSchemaRegistry()
	contractEventPayloadFields = runtimecontracts.EventPayloadFields()
)
