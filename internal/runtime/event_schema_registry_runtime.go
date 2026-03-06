package runtime

// EventSchemaRegistry is the active emit-schema registry used by runtime emit
// tools. Base schemas are generated from contracts/event-catalog.yaml, then a
// narrow override set layers in stricter enums/required fields/bounds where
// runtime behavior is intentionally stricter than the current catalog.
var EventSchemaRegistry = buildEventSchemaRegistry()

var eventSchemaOverrideKeys = []string{
	"scan.requested",
	"pipeline.dead_letter",
	"category.assessed",
	"trend.identified",
	"source.scraped",
	"market_research.scan_complete",
	"trend_research.scan_complete",
	"scanner.google_maps.scan_complete",
	"scanner.instagram.scan_complete",
	"scanner.reviews.scan_complete",
	"scanner.directories.scan_complete",
	"scanner.yelp.scan_complete",
	"scoring.requested",
	"score.dimension_complete",
	"vertical.shortlisted",
	"vertical.marginal",
}

func buildEventSchemaRegistry() map[string]EventSchema {
	out := make(map[string]EventSchema, len(generatedContractEventSchemaRegistry)+len(eventSchemaOverrideKeys))
	for eventType, schema := range generatedContractEventSchemaRegistry {
		out[eventType] = schema
	}
	for _, eventType := range eventSchemaOverrideKeys {
		if schema, ok := legacyEventSchemaRegistry[eventType]; ok {
			out[eventType] = schema
		}
	}
	return out
}
