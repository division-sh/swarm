package canonicalrouting

import (
	"path/filepath"
	"strings"
	"testing"
)

// SingletonCoordinatorPilotVariant is the closed set of accumulation shapes
// exercised by the singleton coordinator pilot.
type SingletonCoordinatorPilotVariant uint8

const (
	SingletonCoordinatorPilotDefault SingletonCoordinatorPilotVariant = iota
	SingletonCoordinatorPilotDynamicBracketTarget
	SingletonCoordinatorPilotMissingMapKey
	SingletonCoordinatorPilotWrongValueShape
	SingletonCoordinatorPilotUndeclaredTarget
	SingletonCoordinatorPilotUnsupportedOperation
	SingletonCoordinatorPilotBadListIndex
	SingletonCoordinatorPilotDemandProjection
	SingletonCoordinatorPilotRetiredFanIn
	SingletonCoordinatorPilotStatelessCountJoin
)

// CopySingletonCoordinatorPilot materializes the canonical singleton
// coordinator bundle with one typed accumulation variant.
func CopySingletonCoordinatorPilot(t testing.TB, variant SingletonCoordinatorPilotVariant) string {
	t.Helper()
	root := t.TempDir()
	writeSingletonCoordinatorFile(t, root, "schema.yaml", "name: singleton-coordinator-pilot\n")
	writeSingletonCoordinatorFlow(t, root, variant)
	return root
}

// CopyDuplicateScopedSingletonDemand materializes two singleton flow scopes
// that intentionally reuse one local node ID. Only flow a consumes contained
// state, so a flattened-node traversal loses the exact demand.
func CopyDuplicateScopedSingletonDemand(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeSingletonCoordinatorFile(t, root, "schema.yaml", "name: duplicate-scoped-singleton-demand\n")
	for _, flowID := range []string{"a", "b"} {
		writeSingletonCoordinatorFile(t, root, filepath.Join(flowID, "schema.yaml"), `
name: `+flowID+`
pins:
  inputs:
    events:
      - {event: item.received, source: harness}
`)
		writeSingletonCoordinatorFile(t, root, filepath.Join(flowID, "events.yaml"), "item.received:\n  items: '[text]'\n")
		entities := "state: {}\n"
		nodes := `
shared-node:
  execution_type: system_node
  subscribes_to: [item.received]
  event_handlers:
    item.received: {}
`
		if flowID == "a" {
			entities = "state:\n  items:\n    type: '[text]'\n    initial: []\n"
			nodes = `
shared-node:
  execution_type: system_node
  subscribes_to: [item.received]
  event_handlers:
    item.received:
      data_accumulation:
        writes:
          - {source_field: items, target_field: items}
`
		}
		writeSingletonCoordinatorFile(t, root, filepath.Join(flowID, "entities.yaml"), entities)
		writeSingletonCoordinatorFile(t, root, filepath.Join(flowID, "nodes.yaml"), nodes)
	}
	return root
}

func writeSingletonCoordinatorFlow(t testing.TB, root string, variant SingletonCoordinatorPilotVariant) {
	t.Helper()
	if variant == SingletonCoordinatorPilotRetiredFanIn {
		writeRetiredFanInSingletonCoordinatorFlow(t, root)
		return
	}
	if variant == SingletonCoordinatorPilotStatelessCountJoin {
		writeStatelessCountJoinSingletonCoordinatorFlow(t, root)
		return
	}
	writeSingletonCoordinatorFile(t, root, "schema.yaml", "name: singleton-coordinator-pilot\npins:\n  inputs:\n    events: [lead.observed]\n  outputs:\n    events: [lead.observed]\nconnect:\n  - event: lead.observed\n    from: .\n    to: coordinator\n")
	writeSingletonCoordinatorFile(t, root, "coordinator/schema.yaml", `name: coordinator
stages: []
pins:
  inputs:
    events:
      - lead.observed
`)
	writeSingletonCoordinatorFile(t, root, "types.yaml", `
types:
  LeadScore:
    status: text
    score: integer
    observations: "[Observation]"
  Observation:
    source: text
    note: text
  AuditEntry:
    ref: text
    action: text
`)
	entities := `
coordinator_state:
  coordinator_id: text
  lead_index: map[text]LeadScore
  audit_log: "[AuditEntry]"
`
	if variant == SingletonCoordinatorPilotDemandProjection {
		entities += "  unused_index: map[text]json\n"
	}
	writeSingletonCoordinatorFile(t, root, "coordinator/entities.yaml", entities)
	writeSingletonCoordinatorFile(t, root, "events.yaml", `
lead.observed:
  coordinator_id: text
  lead_id: text
  observation: Observation
  audit: AuditEntry
  followup_audit: AuditEntry
  corrected_audit: AuditEntry
`)
	nodes := `
coordinator-indexer:
  execution_type: system_node
  subscribes_to: [lead.observed]
  event_handlers:
    lead.observed:
      data_accumulation:
        writes:
` + singletonCoordinatorWritesYAML(t, variant)
	if variant == SingletonCoordinatorPilotDemandProjection {
		nodes += `      fan_out:
        items_from: entity.audit_log
        as: entry
        identity: entry.ref
        emit: lead.observed
`
	}
	writeSingletonCoordinatorFile(t, root, "coordinator/nodes.yaml", nodes)
}

func writeStatelessCountJoinSingletonCoordinatorFlow(t testing.TB, root string) {
	t.Helper()
	writeSingletonCoordinatorFile(t, root, "coordinator/schema.yaml", `name: coordinator
stages:
  active: {initial: true}
  done: {terminal: true}
  failed: {terminal: true}
pins:
  inputs:
    events:
      - {event: job.received, source: harness}
`)
	writeSingletonCoordinatorFile(t, root, "coordinator/types.yaml", singletonCoordinatorTypesYAMLForFixture())
	writeSingletonCoordinatorFile(t, root, "coordinator/entities.yaml", "coordinator_state: {}\n")
	writeSingletonCoordinatorFile(t, root, "coordinator/events.yaml", `
job.received:
  vertical_id: text
  job: Job
`)
	writeSingletonCoordinatorFile(t, root, "coordinator/nodes.yaml", `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      join:
        stage: active
        members: {count: 1, by: payload.vertical_id}
        output: payload.job
        on_complete: {advances_to: done}
        deadline: {after: 1h, from: stage_entry}
        on_deadline: {advances_to: failed}
`)
}

func singletonCoordinatorTypesYAMLForFixture() string {
	return `
types:
  Job:
    id: text
    title: text
`
}

// This source is a retired-grammar rejection specimen. It does not establish
// contained-state demand or a supported singleton coordinator variant.
func writeRetiredFanInSingletonCoordinatorFlow(t testing.TB, root string) {
	t.Helper()
	writeSingletonCoordinatorFile(t, root, "coordinator/schema.yaml", RetiredFanInCoordinatorSchema())
	writeSingletonCoordinatorFile(t, root, "coordinator/entities.yaml", "coordinator_state: {}\n")
	writeSingletonCoordinatorFile(t, root, "coordinator/events.yaml", "job.received:\n  vertical_id: text\n")
}

// RetiredFanInCoordinatorSchema exposes the exact rejection specimen for
// admission and syntax-guard proof; it is not a supported coordinator shape.
func RetiredFanInCoordinatorSchema() string {
	return `name: coordinator
pins:
  inputs:
    events:
      - event: job.received
        source: harness
        resolution:
          mode: fan-in
          aggregation: stream
          window: payload.vertical_id
          dedup_by: [event.id]
          singleton: coordinator
`
}

func singletonCoordinatorWritesYAML(t testing.TB, variant SingletonCoordinatorPilotVariant) string {
	t.Helper()
	switch variant {
	case SingletonCoordinatorPilotDefault:
		return singletonCoordinatorValidWritesYAML()
	case SingletonCoordinatorPilotDynamicBracketTarget:
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.lead_index[payload.lead_id]", "key: \"${payload.lead_id}\"", `
            value:
              status: active
              score: 0
              observations: []
`)
	case SingletonCoordinatorPilotMissingMapKey:
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.lead_index", "", `
            value:
              status: active
              score: 0
              observations: []
`)
	case SingletonCoordinatorPilotWrongValueShape:
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.lead_index", "key: \"${payload.lead_id}\"", `
            value:
              undeclared: true
`)
	case SingletonCoordinatorPilotUndeclaredTarget:
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.missing_index", "key: \"${payload.lead_id}\"", `
            value:
              status: active
              score: 0
              observations: []
`)
	case SingletonCoordinatorPilotUnsupportedOperation:
		return singletonCoordinatorFirstMapWriteYAML("replace", "entity.lead_index", "key: \"${payload.lead_id}\"", `
            value:
              status: active
              score: 0
              observations: []
`)
	case SingletonCoordinatorPilotBadListIndex:
		return singletonCoordinatorDirectWriteYAML() + singletonCoordinatorValidWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: -1
            value: "${payload.corrected_audit}"
`
	case SingletonCoordinatorPilotDemandProjection:
		return `          - source_field: audit
            target_field: audit_log
`
	default:
		t.Fatalf("unsupported singleton coordinator pilot variant %d", variant)
		return ""
	}
}

func singletonCoordinatorValidWritesYAML() string {
	return singletonCoordinatorDirectWriteYAML() + singletonCoordinatorValidWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: 0
            value: "${payload.corrected_audit}"
`
}

func singletonCoordinatorValidWritesPrefixYAML() string {
	return `          - op: set
            target: entity.lead_index
            key: "${payload.lead_id}"
            value:
              status: active
              score: 0
              observations: []
          - op: merge
            target: entity.lead_index
            key: "${payload.lead_id}"
            value:
              score: 1
          - op: append
            target: entity.lead_index.observations
            key: "${payload.lead_id}"
            value: "${payload.observation}"
          - op: append
            target: entity.audit_log
            value: "${payload.audit}"
          - op: append
            target: entity.audit_log
            value: "${payload.followup_audit}"
`
}

func singletonCoordinatorDirectWriteYAML() string {
	return `          - source_field: coordinator_id
            target_field: coordinator_id
`
}

func singletonCoordinatorFirstMapWriteYAML(operation, target, keyBlock, valueBlock string) string {
	out := singletonCoordinatorDirectWriteYAML() + `          - op: ` + operation + `
            target: ` + target + `
`
	if strings.TrimSpace(keyBlock) != "" {
		out += "            " + strings.ReplaceAll(strings.TrimRight(keyBlock, "\n"), "\n", "\n            ") + "\n"
	}
	return out + strings.TrimLeft(valueBlock, "\n")
}

func writeSingletonCoordinatorFile(t testing.TB, root, relativePath, contents string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := writeFixtureFile(t, path, strings.TrimLeft(contents, "\n")); err != nil {
		t.Fatalf("write singleton coordinator pilot fixture %s: %v", path, err)
	}
}
