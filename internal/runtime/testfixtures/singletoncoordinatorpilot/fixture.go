package singletoncoordinatorpilot

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	FlowID       = "coordinator"
	NodeID       = "coordinator-indexer"
	EntityType   = "coordinator_state"
	InputEvent   = "lead.observed"
	FlowInstance = "coordinator"
)

// Options toggles the singleton coordinator pilot variants used by
// conformance and runtime tests.
type Options struct {
	DynamicBracketTarget bool
	MissingMapKey        bool
	WrongValueShape      bool
	UndeclaredTarget     bool
	UnsupportedOperation bool
	BadListIndex         bool
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t, opts)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB, opts Options) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := Write(t, opts)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "package.yaml"), `
name: singleton-coordinator-pilot
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: coordinator
    flow: coordinator
    mode: singleton
`)
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: singleton-coordinator-pilot\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeCoordinator(t, root, opts)
	return root
}

func writeCoordinator(t testing.TB, root string, opts Options) {
	// routing-example-census: different-concept issue=none owner=flow_instance_authoring.singleton_coordinator proof=TestSingletonCoordinatorPilotConformance_CoversSingletonMapCoordinatorOwners
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "coordinator", "schema.yaml"), `
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
	writeFile(t, filepath.Join(root, "flows", "coordinator", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "coordinator", "types.yaml"), `
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
	writeFile(t, filepath.Join(root, "flows", "coordinator", "entities.yaml"), `
coordinator_state:
  coordinator_id: text
  lead_index: map[text]LeadScore
  audit_log: "[AuditEntry]"
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "events.yaml"), `
lead.observed:
  coordinator_id: text
  lead_id: text
  observation: Observation
  audit: AuditEntry
  followup_audit: AuditEntry
  corrected_audit: AuditEntry
`)
	writeFile(t, filepath.Join(root, "flows", "coordinator", "nodes.yaml"), `
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
`+writesYAML(opts))
}

func writesYAML(opts Options) string {
	if opts.DynamicBracketTarget {
		return firstMapWriteYAML("set", "entity.lead_index[payload.lead_id]", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	}
	if opts.MissingMapKey {
		return firstMapWriteYAML("set", "entity.lead_index", "", `
            value:
              status: active
              score: 0
              observations: []
`)
	}
	if opts.WrongValueShape {
		return firstMapWriteYAML("set", "entity.lead_index", "key:\n              ref: payload.lead_id", `
            value:
              undeclared: true
`)
	}
	if opts.UndeclaredTarget {
		return firstMapWriteYAML("set", "entity.missing_index", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	}
	if opts.UnsupportedOperation {
		return firstMapWriteYAML("replace", "entity.lead_index", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	}
	if opts.BadListIndex {
		return directCoordinatorWriteYAML() + validWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: -1
            value:
              ref: payload.corrected_audit
`
	}
	return validWritesYAML()
}

func validWritesYAML() string {
	return directCoordinatorWriteYAML() + validWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: 0
            value:
              ref: payload.corrected_audit
`
}

func validWritesPrefixYAML() string {
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

func directCoordinatorWriteYAML() string {
	return `          - source_field: coordinator_id
            target_field: coordinator_id
`
}

func firstMapWriteYAML(op, target, keyBlock, valueBlock string) string {
	out := directCoordinatorWriteYAML() + `          - op: ` + op + `
            target: ` + target + `
`
	if strings.TrimSpace(keyBlock) != "" {
		out += "            " + strings.ReplaceAll(strings.TrimRight(keyBlock, "\n"), "\n", "\n            ") + "\n"
	}
	out += strings.TrimLeft(valueBlock, "\n")
	return out
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func writeFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
