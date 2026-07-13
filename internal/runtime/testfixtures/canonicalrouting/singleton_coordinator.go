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
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeSingletonCoordinatorFile(t, root, name, "{}\n")
	}
	writeSingletonCoordinatorFlow(t, root, variant)
	return root
}

func writeSingletonCoordinatorFlow(t testing.TB, root string, variant SingletonCoordinatorPilotVariant) {
	t.Helper()
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
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeSingletonCoordinatorFile(t, root, filepath.Join("flows", "coordinator", name), "{}\n")
	}
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
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/entities.yaml", `
coordinator_state:
  coordinator_id: text
  lead_index: map[text]LeadScore
  audit_log: "[AuditEntry]"
`)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/events.yaml", `
lead.observed:
  coordinator_id: text
  lead_id: text
  observation: Observation
  audit: AuditEntry
  followup_audit: AuditEntry
  corrected_audit: AuditEntry
`)
	writeSingletonCoordinatorFile(t, root, "flows/coordinator/nodes.yaml", `
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
`+singletonCoordinatorWritesYAML(t, variant))
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
	if err := writeFixtureFile(path, strings.TrimLeft(contents, "\n")); err != nil {
		t.Fatalf("write singleton coordinator pilot fixture %s: %v", path, err)
	}
}
