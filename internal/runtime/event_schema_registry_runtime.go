package runtime

import runtimecontracts "empireai/internal/runtime/contracts"

// EventSchemaRegistry is the active emit-schema registry used by runtime emit
// tools. Base schemas are generated from contracts/event-catalog.yaml, then a
// narrow override set layers in stricter enums/required fields/bounds where
// runtime behavior is intentionally stricter than the current catalog.
var EventSchemaRegistry = runtimecontracts.EventSchemaRegistry()
