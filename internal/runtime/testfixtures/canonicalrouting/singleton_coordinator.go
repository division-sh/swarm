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
	SingletonCoordinatorPilotStatelessFanIn
	SingletonCoordinatorPilotStatelessPayloadJoin
)

// CopySingletonCoordinatorPilot materializes the canonical singleton
// coordinator bundle with one typed accumulation variant.
func CopySingletonCoordinatorPilot(t testing.TB, variant SingletonCoordinatorPilotVariant) string {
	t.Helper()
	root := t.TempDir()
	writeSingletonCoordinatorFile(t, root, "package.yaml", `
name: singleton-coordinator-pilot
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: coordinator
    flow: coordinator
    mode: singleton
`)
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
	writeSingletonCoordinatorFile(t, root, "package.yaml", `
name: duplicate-scoped-singleton-demand
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: a, flow: a, mode: singleton}
  - {id: b, flow: b, mode: singleton}
`)
	writeSingletonCoordinatorFile(t, root, "schema.yaml", "name: duplicate-scoped-singleton-demand\n")
	for _, flowID := range []string{"a", "b"} {
		writeSingletonCoordinatorFile(t, root, filepath.Join("flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: singleton
pins:
  inputs:
    events:
      - {name: item_received, event: item.received, source: harness}
  outputs:
    events: []
`)
		writeSingletonCoordinatorFile(t, root, filepath.Join("flows", flowID, "events.yaml"), "item.received:\n  items: '[text]'\n")
		entities := "state: {}\n"
		nodes := `
shared-node:
  id: shared-node
  execution_type: system_node
  subscribes_to: [item.received]
  event_handlers:
    item.received: {}
`
		if flowID == "a" {
			entities = "state:\n  items:\n    type: '[text]'\n    initial: []\n"
			nodes = `
shared-node:
  id: shared-node
  execution_type: system_node
  subscribes_to: [item.received]
  event_handlers:
    item.received:
      data_accumulation:
        writes:
          - {source_field: items, target_field: items}
`
		}
		writeSingletonCoordinatorFile(t, root, filepath.Join("flows", flowID, "entities.yaml"), entities)
		writeSingletonCoordinatorFile(t, root, filepath.Join("flows", flowID, "nodes.yaml"), nodes)
	}
	return root
}

func writeSingletonCoordinatorFlow(t testing.TB, root string, variant SingletonCoordinatorPilotVariant) {
	t.Helper()
	if variant == SingletonCoordinatorPilotStatelessFanIn {
		writeStatelessFanInSingletonCoordinatorFlow(t, root)
		return
	}
	if variant == SingletonCoordinatorPilotStatelessPayloadJoin {
		writeStatelessPayloadJoinSingletonCoordinatorFlow(t, root)
		return
	}
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/schema.yaml", `
name: coordinator
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: lead_observed
        event: lead.observed
        source: external
  outputs:
    events: []
`)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/types.yaml", `
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
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/entities.yaml", entities)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/events.yaml", `
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
  id: coordinator-indexer
  execution_type: system_node
  subscribes_to: [lead.observed]
  event_handlers:
    lead.observed:
      select_entity:
        by:
          coordinator_id: payload.coordinator_id
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
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/nodes.yaml", nodes)
}

func writeStatelessPayloadJoinSingletonCoordinatorFlow(t testing.TB, root string) {
	t.Helper()
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/schema.yaml", `
name: coordinator
mode: singleton
initial_state: active
states: [active, done, failed]
pins:
  inputs:
    events:
      - {name: job_received, event: job.received, source: harness}
  outputs:
    events: []
`)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/types.yaml", singletonCoordinatorTypesYAMLForFixture())
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/entities.yaml", "coordinator_state: {}\n")
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/events.yaml", `
job.received:
  vertical_id: text
  job: Job
`)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/nodes.yaml", `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      join:
        stage: active
        members: {from: payload.job, by: payload.vertical_id}
        output: payload.job
        on_complete: {element_id: 00000000-0000-4000-8000-000000000017, advances_to: done}
        timeout: {element_id: 00000000-0000-4000-8000-000000000018, after: 1h, advances_to: failed}
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

func writeStatelessFanInSingletonCoordinatorFlow(t testing.TB, root string) {
	t.Helper()
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/schema.yaml", `
name: coordinator
mode: singleton
pins:
  inputs:
    events:
      - name: job_received
        event: job.received
        source: harness
        resolution:
          mode: fan-in
          aggregation: stream
          window: payload.vertical_id
          dedup_by: event.id
          singleton: coordinator
  outputs:
    events: []
`)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/entities.yaml", "coordinator_state: {}\n")
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/events.yaml", "job.received:\n  vertical_id: text\n")
}

func singletonCoordinatorWritesYAML(t testing.TB, variant SingletonCoordinatorPilotVariant) string {
	t.Helper()
	switch variant {
	case SingletonCoordinatorPilotDefault:
		return singletonCoordinatorValidWritesYAML()
	case SingletonCoordinatorPilotDynamicBracketTarget:
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.lead_index[payload.lead_id]", "key:\n              ref: payload.lead_id", `
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
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.lead_index", "key:\n              ref: payload.lead_id", `
            value:
              undeclared: true
`)
	case SingletonCoordinatorPilotUndeclaredTarget:
		return singletonCoordinatorFirstMapWriteYAML("set", "entity.missing_index", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	case SingletonCoordinatorPilotUnsupportedOperation:
		return singletonCoordinatorFirstMapWriteYAML("replace", "entity.lead_index", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	case SingletonCoordinatorPilotBadListIndex:
		return singletonCoordinatorDirectWriteYAML() + singletonCoordinatorValidWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: -1
            value:
              ref: payload.corrected_audit
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
            value:
              ref: payload.corrected_audit
`
}

func singletonCoordinatorValidWritesPrefixYAML() string {
	return `          - op: set
            target: entity.lead_index
            key:
              ref: payload.lead_id
            value:
              status: active
              score: 0
              observations: []
          - op: merge
            target: entity.lead_index
            key:
              ref: payload.lead_id
            value:
              score: 1
          - op: append
            target: entity.lead_index.observations
            key:
              ref: payload.lead_id
            value:
              ref: payload.observation
          - op: append
            target: entity.audit_log
            value:
              ref: payload.audit
          - op: append
            target: entity.audit_log
            value:
              ref: payload.followup_audit
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
