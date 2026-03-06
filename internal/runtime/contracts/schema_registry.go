package contracts

import runtimetools "empireai/internal/runtime/tools"

type EventSchema = runtimetools.EmitSchema

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

func EventSchemaRegistry() map[string]EventSchema {
	out := make(map[string]EventSchema, len(generatedContractEventSchemaRegistry)+len(eventSchemaOverrideKeys))
	for eventType, schema := range generatedContractEventSchemaRegistry {
		out[eventType] = schema
	}
	for _, eventType := range eventSchemaOverrideKeys {
		if schema, ok := runtimetools.LegacyEventSchemaRegistry[eventType]; ok {
			out[eventType] = schema
		}
	}
	return out
}
