package canonicalrouting

import "testing"

type LocalWildcardPayloadVariant uint8

const (
	LocalWildcardPayloadValid LocalWildcardPayloadVariant = iota
	LocalWildcardPayloadMissing
	LocalWildcardPayloadIncompatibleType
	LocalWildcardPayloadIncompatiblePresence
)

// CopyLocalWildcardPayload constructs the closed schema-consumption matrix for
// local wildcard subscriptions, not retired imported wildcard grants.
func CopyLocalWildcardPayload(t testing.TB, variant LocalWildcardPayloadVariant) string {
	t.Helper()
	pattern, secondType, secondValue := "task.*", "text", "\"work-2\""
	switch variant {
	case LocalWildcardPayloadValid:
	case LocalWildcardPayloadMissing:
		pattern = "missing.*"
	case LocalWildcardPayloadIncompatibleType:
		secondType, secondValue = "integer", "7"
	case LocalWildcardPayloadIncompatiblePresence:
		secondType = "text?"
	default:
		t.Fatalf("unsupported wildcard payload variant %d", variant)
	}
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml":          "name: wildcard-payload-proof\npins:\n  inputs: {events: [start]}\n  outputs: {events: [start]}\nconnect:\n  - {event: start, from: ., to: worker}\n",
		"events.yaml":          "start:\n",
		"worker/schema.yaml":   "name: worker\nmode: static\npins:\n  inputs:\n    events: [start]\n",
		"worker/entities.yaml": "work: {}\n",
		"worker/events.yaml":   "task.done:\n  work_id: text\ntask.failed:\n  work_id: " + secondType + "\n",
		"worker/nodes.yaml":    "observer:\n  execution_type: system_node\n  subscribes_to: [\"" + pattern + "\"]\n  event_handlers:\n    \"" + pattern + "\":\n      rules:\n        accept:\n          when: payload.work_id != \"\"\n        unmatched:\n          else: true\n",
	}
	files["worker/nodes.yaml"] += "producer:\n  execution_type: system_node\n  subscribes_to: [start, task.done]\n  produces: [task.done, task.failed]\n  event_handlers:\n    start:\n      emit:\n        event: task.done\n        fields:\n          work_id: {literal: work-1}\n    task.done:\n      emit:\n        event: task.failed\n        fields:\n          work_id: {literal: " + secondValue + "}\n"
	for path, contents := range files {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}

// CopyScalarFanOutPayloadReader proves a scalar item alias and list expression
// through an admitted source artifact and a same-flow consumer.
func CopyScalarFanOutPayloadReader(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml":           "name: scalar-fan-out-proof\npins:\n  inputs: {events: [scan.requested]}\n  outputs: {events: [scan.requested]}\nconnect:\n  - {event: scan.requested, from: ., to: scanner}\n",
		"events.yaml":           "scan.requested:\n  industries: \"[text]\"\n",
		"scanner/schema.yaml":   "name: scanner\nmode: static\npins:\n  inputs:\n    events: [scan.requested]\n",
		"scanner/entities.yaml": "scan: {}\n",
		"scanner/events.yaml":   "market_research.industry_assigned:\n  industry: text\n  taxonomy_categories: \"[text]\"\n",
		"scanner/nodes.yaml": `scan-orchestrator:
  execution_type: system_node
  subscribes_to: [scan.requested]
  produces: [market_research.industry_assigned]
  event_handlers:
    scan.requested:
      fan_out:
        items_from: payload.industries
        as: industry
        identity: industry
        emit:
          event: market_research.industry_assigned
          fields:
            industry: ${industry}
            taxonomy_categories: ["${industry}"]
observer:
  execution_type: system_node
  subscribes_to: [market_research.industry_assigned]
  event_handlers:
    market_research.industry_assigned: {}
`,
	} {
		writeClosedVariantFile(t, root, path, contents)
	}
	return root
}
