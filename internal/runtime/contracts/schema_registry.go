package contracts

type EventSchema struct {
	Description string
	Schema      map[string]any
}

func EventSchemaRegistry() map[string]EventSchema {
	out := make(map[string]EventSchema, len(generatedContractEventSchemaRegistry))
	for eventType, schema := range generatedContractEventSchemaRegistry {
		out[eventType] = schema
	}
	return out
}
