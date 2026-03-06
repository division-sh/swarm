package runtime

import runtimecontracts "empireai/internal/runtime/contracts"

// contractEventPayloadFields is generated from contracts/event-catalog.yaml.
// It is used to enforce exhaustive-exact payload key parity in EventSchemaRegistry.
var contractEventPayloadFields = runtimecontracts.EventPayloadFields()
