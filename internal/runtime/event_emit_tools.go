package runtime

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"empireai/internal/commgraph"
)

var (
	emitToolIndexOnce sync.Once
	emitToolToEvent   map[string]string
	generatedSchemas  map[string]struct{}
)

type EventSchema struct {
	Description string
	Schema      map[string]any
}

func ValidateEventPayloadAgainstSchema(eventType string, payload map[string]any) error {
	s := schemaForEventType(eventType)
	root := s.Schema
	if root == nil {
		return nil
	}
	return validateSchemaObject("$", root, payload)
}

func validateSchemaObject(path string, schema map[string]any, payload map[string]any) error {
	if schemaType := strings.TrimSpace(asString(schema["type"])); schemaType != "" && schemaType != "object" {
		return nil
	}
	required := requiredList(schema["required"])
	for _, key := range required {
		if _, ok := payload[key]; !ok {
			return fmt.Errorf("schema validation failed: %s.%s is required", path, key)
		}
	}
	props := schemaProperties(schema["properties"])
	allowAdditional := schemaAdditionalProps(schema["additionalProperties"])
	for k, v := range payload {
		propSchema, known := props[k]
		if !known {
			if allowAdditional {
				continue
			}
			return fmt.Errorf("schema validation failed: %s.%s is not allowed", path, k)
		}
		if err := validateValue(path+"."+k, propSchema, v); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(path string, schema map[string]any, value any) error {
	st := strings.TrimSpace(asString(schema["type"]))
	if st == "" {
		st = "object"
	}
	if enumRaw, ok := schema["enum"]; ok {
		if !valueInEnum(value, enumRaw) {
			return fmt.Errorf("schema validation failed: %s has invalid enum value %v", path, value)
		}
	}
	switch st {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("schema validation failed: %s must be string", path)
		}
	case "number":
		if !isNumeric(value) {
			return fmt.Errorf("schema validation failed: %s must be number", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "integer":
		if !isInteger(value) {
			return fmt.Errorf("schema validation failed: %s must be integer", path)
		}
		if err := validateNumericBounds(path, schema, value); err != nil {
			return err
		}
	case "array":
		arr, ok := asArray(value)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be array", path)
		}
		if itemsRaw, ok := schema["items"]; ok {
			if itemSchema, ok := itemsRaw.(map[string]any); ok {
				for i, it := range arr {
					if err := validateValue(fmt.Sprintf("%s[%d]", path, i), itemSchema, it); err != nil {
						return err
					}
				}
			}
		}
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("schema validation failed: %s must be object", path)
		}
		if err := validateSchemaObject(path, schema, obj); err != nil {
			return err
		}
	}
	return nil
}

func validateNumericBounds(path string, schema map[string]any, value any) error {
	n, ok := asFloat64(value)
	if !ok {
		return fmt.Errorf("schema validation failed: %s must be numeric", path)
	}
	if minRaw, ok := schema["minimum"]; ok {
		min, ok := asFloat64(minRaw)
		if ok && n < min {
			return fmt.Errorf("schema validation failed: %s must be >= %v", path, min)
		}
	}
	if maxRaw, ok := schema["maximum"]; ok {
		max, ok := asFloat64(maxRaw)
		if ok && n > max {
			return fmt.Errorf("schema validation failed: %s must be <= %v", path, max)
		}
	}
	return nil
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func schemaProperties(raw any) map[string]map[string]any {
	out := map[string]map[string]any{}
	switch t := raw.(type) {
	case map[string]any:
		for k, v := range t {
			if s, ok := v.(map[string]any); ok {
				out[k] = s
			}
		}
	}
	return out
}

func schemaAdditionalProps(raw any) bool {
	if raw == nil {
		return true
	}
	if b, ok := raw.(bool); ok {
		return b
	}
	return true
}

func requiredList(raw any) []string {
	switch t := raw.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, v := range t {
			if s := strings.TrimSpace(asString(v)); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func valueInEnum(value any, enumRaw any) bool {
	enum, ok := enumRaw.([]any)
	if !ok {
		switch t := enumRaw.(type) {
		case []string:
			for _, v := range t {
				if strings.EqualFold(strings.TrimSpace(asString(value)), strings.TrimSpace(v)) {
					return true
				}
			}
			return false
		default:
			return true
		}
	}
	for _, v := range enum {
		if strings.EqualFold(strings.TrimSpace(asString(value)), strings.TrimSpace(asString(v))) {
			return true
		}
	}
	return false
}

func isNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32, uint, uint64, uint32:
		return true
	default:
		return false
	}
}

func isInteger(v any) bool {
	switch v.(type) {
	case int, int64, int32, uint, uint64, uint32:
		return true
	case float64:
		return v.(float64) == float64(int64(v.(float64)))
	case float32:
		return v.(float32) == float32(int64(v.(float32)))
	default:
		return false
	}
}

func asArray(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case []string:
		out := make([]any, 0, len(t))
		for _, s := range t {
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}

// EventSchemaRegistry is the single source of truth for runtime-generated
// emit_* tool schemas. Missing entries are auto-filled from known produced
// events so every emit tool has an explicit schema record.
var EventSchemaRegistry = map[string]EventSchema{
	"scan.requested": {
		Description: "Request a market scan for a geography.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"mode": map[string]any{
					"type": "string",
					"enum": []string{"saas_gap", "saas_trend", "local_services"},
				},
				"geography_id": map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"categories": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"taxonomy_categories": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"priority": map[string]any{
					"type": "string",
					"enum": []string{"low", "normal", "high", "critical"},
				},
				"campaign_context": map[string]any{
					"type":                 "object",
					"additionalProperties": true,
				},
				"directive_id": map[string]any{"type": "string"},
				"strategic_context": map[string]any{
					"type":                 "object",
					"additionalProperties": true,
				},
				"requested_at": map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
			},
			"required":             []string{"mode"},
			"additionalProperties": false,
		},
	},
	"opco.spinup_requested": {
		Description: "Request creation of a new OpCo from approved validation output.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id": map[string]any{"type": "string"},
				"mandate": map[string]any{
					"type":                 "object",
					"additionalProperties": true,
				},
				"task_id": map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id"},
			"additionalProperties": false,
		},
	},
	"portfolio.digest_compiled": {
		Description: "Portfolio digest compiled for board/operator visibility.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"digest_text":           map[string]any{"type": "string"},
				"trigger_reason":        map[string]any{"type": "string"},
				"trigger_event_id":      map[string]any{"type": "string"},
				"action_required_count": map[string]any{"type": "integer"},
				"snapshot":              map[string]any{"type": "object", "additionalProperties": true},
				"compiled_at":           map[string]any{"type": "string"},
				"task_id":               map[string]any{"type": "string"},
				"vertical_id":           map[string]any{"type": "string"},
			},
			"required":             []string{"digest_text"},
			"additionalProperties": false,
		},
	},
	"vertical.health_warning": {
		Description: "Critical vertical health warning requiring attention.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":      map[string]any{"type": "string"},
				"severity":         map[string]any{"type": "string"},
				"breached_metrics": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"trend_data":       map[string]any{"type": "object", "additionalProperties": true},
				"recommendation":   map[string]any{"type": "string"},
				"notes":            map[string]any{"type": "string"},
				"task_id":          map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id"},
			"additionalProperties": false,
		},
	},
	"template.migration_planned": {
		Description: "Template migration plan prepared for one or more running verticals.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"migration_id":  map[string]any{"type": "string"},
				"from_version":  map[string]any{"type": "string"},
				"to_version":    map[string]any{"type": "string"},
				"vertical_ids":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"plan_summary":  map[string]any{"type": "string"},
				"task_id":       map[string]any{"type": "string"},
				"requested_by":  map[string]any{"type": "string"},
				"requested_at":  map[string]any{"type": "string"},
				"template_diff": map[string]any{"type": "object", "additionalProperties": true},
			},
			"required":             []string{"migration_id", "from_version", "to_version"},
			"additionalProperties": false,
		},
	},
	"template.migration_completed": {
		Description: "Template migration completed successfully.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"migration_id": map[string]any{"type": "string"},
				"from_version": map[string]any{"type": "string"},
				"to_version":   map[string]any{"type": "string"},
				"vertical_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"summary":      map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"completed_at": map[string]any{"type": "string"},
			},
			"required":             []string{"migration_id", "from_version", "to_version"},
			"additionalProperties": false,
		},
	},
	"template.migration_failed": {
		Description: "Template migration failed and may require human remediation.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"migration_id":  map[string]any{"type": "string"},
				"from_version":  map[string]any{"type": "string"},
				"to_version":    map[string]any{"type": "string"},
				"error":         map[string]any{"type": "string"},
				"failed_step":   map[string]any{"type": "string"},
				"recoverable":   map[string]any{"type": "boolean"},
				"vertical_ids":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"task_id":       map[string]any{"type": "string"},
				"failed_at":     map[string]any{"type": "string"},
				"remediation":   map[string]any{"type": "string"},
				"template_diff": map[string]any{"type": "object", "additionalProperties": true},
			},
			"required":             []string{"migration_id", "from_version", "to_version", "error"},
			"additionalProperties": false,
		},
	},
	"category.assessed": {
		Description: "Market Research Agent reports one assessed category for a scan shard.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":                map[string]any{"type": "string"},
				"campaign_id":            map[string]any{"type": "string"},
				"mode":                   map[string]any{"type": "string"},
				"geography":              map[string]any{"type": "string"},
				"category":               map[string]any{"type": "string"},
				"subcategory":            map[string]any{"type": "string"},
				"opportunity_hypothesis": map[string]any{"type": "string"},
				"evidence":               map[string]any{"type": "string"},
				"signal_strength": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"task_id":     map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"scan_id", "category", "subcategory", "opportunity_hypothesis", "evidence", "signal_strength"},
			"additionalProperties": false,
		},
	},
	"trend.identified": {
		Description: "Trend Research Agent reports one trend finding for a scan shard.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":                map[string]any{"type": "string"},
				"campaign_id":            map[string]any{"type": "string"},
				"mode":                   map[string]any{"type": "string"},
				"geography":              map[string]any{"type": "string"},
				"trend_category":         map[string]any{"type": "string"},
				"trend_description":      map[string]any{"type": "string"},
				"opportunity_hypothesis": map[string]any{"type": "string"},
				"evidence":               map[string]any{"type": "string"},
				"signal_strength": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"task_id":     map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"scan_id", "trend_category", "trend_description", "opportunity_hypothesis", "evidence", "signal_strength"},
			"additionalProperties": false,
		},
	},
	"source.scraped": {
		Description: "Scanner agent reports one scraped source item for discovery synthesis.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":                map[string]any{"type": "string"},
				"campaign_id":            map[string]any{"type": "string"},
				"mode":                   map[string]any{"type": "string"},
				"geography":              map[string]any{"type": "string"},
				"category":               map[string]any{"type": "string"},
				"subcategory":            map[string]any{"type": "string"},
				"source":                 map[string]any{"type": "string"},
				"url":                    map[string]any{"type": "string"},
				"title":                  map[string]any{"type": "string"},
				"snippet":                map[string]any{"type": "string"},
				"opportunity_hypothesis": map[string]any{"type": "string"},
				"evidence":               map[string]any{"type": "string"},
				"signal_strength": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"task_id":     map[string]any{"type": "string"},
				"vertical_id": map[string]any{"type": "string"},
			},
			"required":             []string{"scan_id", "source", "evidence", "signal_strength"},
			"additionalProperties": false,
		},
	},
	"market_research.scan_complete": {
		Description: "Market research scan shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"trend_research.scan_complete": {
		Description: "Trend research scan shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.google_maps.scan_complete": {
		Description: "Google Maps scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.instagram.scan_complete": {
		Description: "Instagram scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.reviews.scan_complete": {
		Description: "Reviews scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.directories.scan_complete": {
		Description: "Directories scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scanner.job_boards.scan_complete": {
		Description: "Job boards scanner shard is complete.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"scan_id":      map[string]any{"type": "string"},
				"campaign_id":  map[string]any{"type": "string"},
				"mode":         map[string]any{"type": "string"},
				"geography":    map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"vertical_id":  map[string]any{"type": "string"},
				"shard":        map[string]any{"type": "object", "additionalProperties": true},
				"completion":   map[string]any{"type": "object", "additionalProperties": true},
				"report_count": map[string]any{"type": "integer", "minimum": 0},
			},
			"required":             []string{"scan_id"},
			"additionalProperties": false,
		},
	},
	"scoring.requested": {
		Description: "Runtime requests dimension scoring from Analysis Agent.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":   map[string]any{"type": "string"},
				"vertical_name": map[string]any{"type": "string"},
				"geography":     map[string]any{"type": "string"},
				"mode": map[string]any{
					"type": "string",
					"enum": []string{"saas_gap", "saas_trend", "local_services"},
				},
				"signal_strength": map[string]any{"type": "integer"},
				"campaign_id":     map[string]any{"type": "string"},
				"rubric": map[string]any{
					"type": "string",
					"enum": []string{"saas", "local_services"},
				},
				"dimensions_requested": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"task_id": map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "vertical_name", "geography", "mode", "rubric", "dimensions_requested"},
			"additionalProperties": false,
		},
	},
	"scoring.contest_resolved": {
		Description: "Empire Coordinator resolves a contested scoring dimension.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id": map[string]any{"type": "string"},
				"dimension":   map[string]any{"type": "string"},
				"resolved_score": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"reasoning": map[string]any{"type": "string"},
				"task_id":   map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "dimension", "resolved_score"},
			"additionalProperties": false,
		},
	},
	"score.dimension_complete": {
		Description: "Analysis Agent reports score for one dimension of one vertical.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id": map[string]any{"type": "string"},
				"dimension":   map[string]any{"type": "string"},
				"score": map[string]any{
					"type":    "integer",
					"minimum": 0,
					"maximum": 100,
				},
				"evidence": map[string]any{"type": "string"},
				"confidence": map[string]any{
					"type": "string",
					"enum": []string{"high", "medium", "low"},
				},
			},
			"required":             []string{"vertical_id", "dimension", "score", "evidence"},
			"additionalProperties": false,
		},
	},
	"vertical.shortlisted": {
		Description: "Promote a marginal/scored vertical into the validation pipeline.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":     map[string]any{"type": "string"},
				"composite_score": map[string]any{"type": "number"},
				"viability_score": map[string]any{"type": "number"},
				"scoring_payload": map[string]any{"type": "object", "additionalProperties": true},
				"reasoning":       map[string]any{"type": "string"},
				"task_id":         map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "composite_score", "scoring_payload"},
			"additionalProperties": false,
		},
	},
	"vertical.marginal": {
		Description: "Vertical scored 50-74 composite and needs Empire Coordinator judgment.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":        map[string]any{"type": "string"},
				"composite_score":    map[string]any{"type": "number"},
				"viability_score":    map[string]any{"type": "number"},
				"dimensions":         map[string]any{"type": "object", "additionalProperties": true},
				"promotion_eligible": map[string]any{"type": "boolean"},
				"reasoning":          map[string]any{"type": "string"},
				"reconsideration_triggers": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"task_id": map[string]any{"type": "string"},
			},
			"required":             []string{"vertical_id", "composite_score", "viability_score", "dimensions"},
			"additionalProperties": false,
		},
	},
	"vertical.ready_for_review": {
		Description: "Validation package is complete and ready for human review.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"vertical_id":  map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
				"spec_version": map[string]any{"type": "integer"},
				"summary":      map[string]any{"type": "string"},
				"research":     map[string]any{"type": "object", "additionalProperties": true},
				"spec":         map[string]any{"type": "object", "additionalProperties": true},
				"cto_notes":    map[string]any{"type": "object", "additionalProperties": true},
				"brand":        map[string]any{"type": "object", "additionalProperties": true},
			},
			"required":             []string{"vertical_id"},
			"additionalProperties": false,
		},
	},
	"spec.validation_passed": {
		Description: "Spec auditor validation passed.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"severity": map[string]any{
					"type": "string",
					"enum": []string{"clean", "medium", "high"},
				},
				"issues": map[string]any{
					"type":                 "array",
					"items":                map[string]any{"type": "object"},
					"minItems":             0,
					"additionalProperties": true,
				},
				"status":       map[string]any{"type": "string"},
				"passed":       map[string]any{"type": "boolean"},
				"spec_version": map[string]any{"type": "integer"},
				"vertical_id":  map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
			},
			"additionalProperties": false,
		},
	},
	"spec.validation_failed": {
		Description: "Spec auditor validation failed with blocker issues.",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"severity": map[string]any{
					"type": "string",
					"enum": []string{"blocker", "high"},
				},
				"issues": map[string]any{
					"type":                 "array",
					"items":                map[string]any{"type": "object"},
					"minItems":             1,
					"additionalProperties": true,
				},
				"status":       map[string]any{"type": "string"},
				"passed":       map[string]any{"type": "boolean"},
				"spec_version": map[string]any{"type": "integer"},
				"vertical_id":  map[string]any{"type": "string"},
				"task_id":      map[string]any{"type": "string"},
			},
			"required":             []string{"severity"},
			"additionalProperties": false,
		},
	},
}

func ensureEventSchemaRegistry() {
	emitToolIndexOnce.Do(func() {
		generatedSchemas = make(map[string]struct{})
		seedAgentEventSchemaDefaults()
		generated := make([]string, 0, 16)
		for eventType := range commgraph.KnownProducedEvents() {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := EventSchemaRegistry[eventType]; !ok {
				EventSchemaRegistry[eventType] = defaultAgentEventSchema(eventType)
				generatedSchemas[eventType] = struct{}{}
				generated = append(generated, eventType)
			}
		}
		sort.Strings(generated)
		if len(generated) > 0 {
			runtimeWarnOnce(
				"event-schema-generated-defaults",
				"event-schema-registry",
				"generated strict fallback schemas for %d known produced events: %s",
				len(generated),
				summarizeLogList(generated, 20),
			)
		}
		ensureSchemaContextFields()
		emitToolToEvent = make(map[string]string, len(EventSchemaRegistry))
		for eventType := range EventSchemaRegistry {
			emitToolToEvent[emitToolName(eventType)] = eventType
		}
	})
}

func ensureSchemaContextFields() {
	for eventType, entry := range EventSchemaRegistry {
		root := entry.Schema
		if root == nil {
			continue
		}
		rootType := strings.TrimSpace(asString(root["type"]))
		if rootType != "" && rootType != "object" {
			continue
		}
		props, ok := root["properties"].(map[string]any)
		if !ok || props == nil {
			props = map[string]any{}
		}
		if _, ok := props["task_id"]; !ok {
			props["task_id"] = map[string]any{"type": "string"}
		}
		if _, ok := props["vertical_id"]; !ok {
			props["vertical_id"] = map[string]any{"type": "string"}
		}
		root["properties"] = props
		entry.Schema = root
		EventSchemaRegistry[eventType] = entry
	}
}

func seedAgentEventSchemaDefaults() {
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := EventSchemaRegistry[eventType]; ok {
				continue
			}
			EventSchemaRegistry[eventType] = defaultAgentEventSchema(eventType)
		}
	}
}

func defaultAgentEventSchema(eventType string) EventSchema {
	props := map[string]any{
		"vertical_id":     map[string]any{"type": "string"},
		"task_id":         map[string]any{"type": "string"},
		"scan_id":         map[string]any{"type": "string"},
		"campaign_id":     map[string]any{"type": "string"},
		"mode":            map[string]any{"type": "string"},
		"geography":       map[string]any{"type": "string"},
		"geography_id":    map[string]any{"type": "string"},
		"name":            map[string]any{"type": "string"},
		"vertical_name":   map[string]any{"type": "string"},
		"priority":        map[string]any{"type": "string"},
		"status":          map[string]any{"type": "string"},
		"severity":        map[string]any{"type": "string"},
		"action":          map[string]any{"type": "string"},
		"reason":          map[string]any{"type": "string"},
		"notes":           map[string]any{"type": "string"},
		"summary":         map[string]any{"type": "string"},
		"message":         map[string]any{"type": "string"},
		"evidence":        map[string]any{"type": "string"},
		"score":           map[string]any{"type": "number"},
		"composite_score": map[string]any{"type": "number"},
		"viability_score": map[string]any{"type": "number"},
		"signal_strength": map[string]any{"type": "number"},
		"confidence":      map[string]any{"type": "string"},
		"passed":          map[string]any{"type": "boolean"},
		"version":         map[string]any{"type": "string"},
		"from_version":    map[string]any{"type": "string"},
		"to_version":      map[string]any{"type": "string"},
		"migration_id":    map[string]any{"type": "string"},
		"error":           map[string]any{"type": "string"},
		"requested_by":    map[string]any{"type": "string"},
		"requested_at":    map[string]any{"type": "string"},
		"completed_at":    map[string]any{"type": "string"},
		"failed_at":       map[string]any{"type": "string"},
		"digest_text":     map[string]any{"type": "string"},
		"recommendation":  map[string]any{"type": "string"},
		"snapshot":        map[string]any{"type": "object", "additionalProperties": true},
		"payload":         map[string]any{"type": "object", "additionalProperties": true},
		"metadata":        map[string]any{"type": "object", "additionalProperties": true},
		"context":         map[string]any{"type": "object", "additionalProperties": true},
		"details":         map[string]any{"type": "object", "additionalProperties": true},
		"trend_data":      map[string]any{"type": "object", "additionalProperties": true},
		"mandate":         map[string]any{"type": "object", "additionalProperties": true},
		"spec":            map[string]any{"type": "object", "additionalProperties": true},
		"business_brief":  map[string]any{"type": "object", "additionalProperties": true},
		"scoring_payload": map[string]any{"type": "object", "additionalProperties": true},
		"dimensions":      map[string]any{"type": "object", "additionalProperties": true},
		"template_diff":   map[string]any{"type": "object", "additionalProperties": true},
		"issues":          map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"items":           map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"candidates":      map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"events":          map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
	}
	required := []string{}

	switch eventType {
	case "dedup.resolved":
		props["dedup_event_id"] = map[string]any{"type": "string"}
		props["action"] = map[string]any{"type": "string", "enum": []string{"merge", "keep_both"}}
		required = append(required, "dedup_event_id", "action")
	case "synthesis.resolved":
		props["resolution"] = map[string]any{"type": "string"}
		props["rationale"] = map[string]any{"type": "string"}
	case "portfolio.digest_compiled":
		props["digest_text"] = map[string]any{"type": "string"}
		props["trigger_reason"] = map[string]any{"type": "string"}
		props["snapshot"] = map[string]any{"type": "object", "additionalProperties": true}
		required = append(required, "digest_text")
	case "template.version_published":
		props["version"] = map[string]any{"type": "string"}
		required = append(required, "version")
	case "template.migration_planned", "template.migration_completed", "template.migration_failed":
		props["migration_id"] = map[string]any{"type": "string"}
		props["from_version"] = map[string]any{"type": "string"}
		props["to_version"] = map[string]any{"type": "string"}
		props["error"] = map[string]any{"type": "string"}
	case "human_task.requested", "human_task.approved", "human_task.rejected", "human_task.deferred", "human_task.completed", "human_task.expired":
		props["task_id"] = map[string]any{"type": "string"}
		required = append(required, "task_id")
	case "brand.candidates_ready":
		props["candidates"] = map[string]any{"type": "array", "items": map[string]any{"type": "object"}}
		required = append(required, "vertical_id", "candidates")
	}
	if strings.Contains(eventType, ".scan_complete") {
		required = append(required, "scan_id")
	}
	if strings.Contains(eventType, ".scan_assigned") {
		required = append(required, "scan_id")
	}
	if strings.HasPrefix(eventType, "opco.") && !strings.Contains(eventType, "teardown_complete") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "spec.") || strings.HasPrefix(eventType, "cto.spec_") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "vertical.") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "budget.") {
		props["state"] = map[string]any{"type": "string"}
		props["next_event_type"] = map[string]any{"type": "string"}
	}
	required = uniqueNonEmpty(required)

	schema := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return EventSchema{
		Description: "Emit " + eventType + " event",
		Schema:      schema,
	}
}

func GenerateEmitTools(role string) []ToolDefinition {
	allowed := commgraph.ProducerEventsForRole(role)
	if len(allowed) == 0 {
		return nil
	}
	ensureEventSchemaRegistry()
	tools := make([]ToolDefinition, 0, len(allowed))
	for _, eventType := range allowed {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		schema := schemaForEventType(eventType)
		tools = append(tools, ToolDefinition{
			Name:        emitToolName(eventType),
			Description: schema.Description,
			Schema:      schema.Schema,
		})
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	return tools
}

// GeneratedEmitSchemas returns event types that currently rely on permissive,
// auto-generated schemas rather than explicit spec-authored definitions.
func GeneratedEmitSchemas() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, len(generatedSchemas))
	for eventType := range generatedSchemas {
		out = append(out, eventType)
	}
	sort.Strings(out)
	return out
}

// GeneratedEmitSchemasForAgentRoles returns generated/permissive schemas that
// are reachable through at least one agent role's emit tool allowlist.
func GeneratedEmitSchemasForAgentRoles() []string {
	ensureEventSchemaRegistry()
	out := make([]string, 0, 64)
	seen := make(map[string]struct{}, 128)
	for _, role := range commgraph.ProducerRoles() {
		for _, eventType := range commgraph.ProducerEventsForRole(role) {
			if _, ok := generatedSchemas[eventType]; !ok {
				continue
			}
			if _, dup := seen[eventType]; dup {
				continue
			}
			seen[eventType] = struct{}{}
			out = append(out, eventType)
		}
	}
	sort.Strings(out)
	return out
}

func IsEmitToolAllowedForRole(role, toolName string) bool {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return false
	}
	for _, evt := range commgraph.ProducerEventsForRole(role) {
		if strings.TrimSpace(evt) == eventType {
			return true
		}
	}
	return false
}

func emitToolName(eventType string) string {
	return "emit_" + strings.ReplaceAll(strings.TrimSpace(eventType), ".", "_")
}

func eventTypeFromEmitToolName(toolName string) (string, bool) {
	toolName = strings.TrimSpace(toolName)
	if !strings.HasPrefix(toolName, "emit_") {
		return "", false
	}
	ensureEventSchemaRegistry()
	if eventType, ok := emitToolToEvent[toolName]; ok {
		return eventType, true
	}
	return "", false
}

func schemaForEventType(eventType string) EventSchema {
	ensureEventSchemaRegistry()
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		runtimeWarnOnce("schema-for-empty-event-type", "event-schema-registry", "schema requested for empty event type")
		return EventSchema{
			Description: "Emit event",
			Schema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		}
	}
	if s, ok := EventSchemaRegistry[eventType]; ok {
		return s
	}
	// Unknown event types are blocked before execution; keep a defensive schema.
	runtimeWarnOnce(
		"schema-for-unknown-"+eventType,
		"event-schema-registry",
		"schema requested for unknown event type %q; returning strict defensive empty schema",
		eventType,
	)
	return EventSchema{
		Description: "Emit " + eventType + " event",
		Schema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}
}

func uniqueNonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
