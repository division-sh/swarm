package finalflowinstanceauthoring

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
	PackageName = "final-flow-instance-authoring"

	ProducerFlowID    = "intake"
	ProducerNodeID    = "intake-router"
	ProducerInputPin  = "request_received"
	ProducerOutputPin = "account_ready"
	ProducerInput     = "request.received"
	ProducerOutput    = "account.ready"

	TemplateFlowID       = "account_case"
	TemplateNodeID       = "account-case-worker"
	TemplateEntityType   = "account_case_state"
	TemplateInputPin     = "account_ready"
	TemplateInstanceBy   = "account_id"
	TemplatePayloadKey   = "source_account_id"
	TemplateFlowInstance = TemplateFlowID

	CoordinatorFlowID     = "coordinator"
	CoordinatorNodeID     = "coordinator-indexer"
	CoordinatorEntityType = "coordinator_state"
	CoordinatorInput      = "lead.observed"
	CoordinatorInstance   = CoordinatorFlowID

	LegacyFlowID = "legacy_static"
)

// Options toggles final sealed authoring variants used by conformance,
// EventBus, pipeline, and authoring-view tests. The positive fixture combines a
// template flow-instance route and singleton/coordinator contained state in one
// package; negative options only introduce declared fail-closed seams.
type Options struct {
	MissingOutputKey            bool
	MissingOutputCarries        bool
	BadConnectMapping           bool
	DuplicateConnectMapping     bool
	UnsupportedReceiverSelector bool
	ProducerTarget              bool
	ProducerBroadcast           bool

	DynamicBracketTarget bool
	MissingMapKey        bool
	WrongMapValueShape   bool
	UndeclaredMapTarget  bool
	UnsupportedMapOp     bool
	BadListIndex         bool

	StaticCreateEntity        bool
	StaticSelectEntity        bool
	StaticSelectOrCreate      bool
	StaticMissingAcquisition  bool
	RootDefaultEntityIDSource bool
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
	writeRoot(t, root, opts)
	writeProducer(t, root, opts)
	writeTemplate(t, root, opts)
	writeCoordinator(t, root, opts)
	if opts.StaticCreateEntity || opts.StaticSelectEntity || opts.StaticSelectOrCreate || opts.StaticMissingAcquisition {
		writeLegacyStatic(t, root, opts)
	}
	return root
}

func writeRoot(t testing.TB, root string, opts Options) {
	t.Helper()
	legacyFlow := ""
	if opts.StaticCreateEntity || opts.StaticSelectEntity || opts.StaticSelectOrCreate || opts.StaticMissingAcquisition {
		legacyFlow = `  - id: legacy_static
    flow: legacy_static
    mode: static
`
	}
	writeFile(t, filepath.Join(root, "package.yaml"), `
name: final-flow-instance-authoring
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: intake
    flow: intake
    mode: static
  - id: account_case
    flow: account_case
    mode: template
  - id: coordinator
    flow: coordinator
    mode: singleton
`+legacyFlow+`connect:
  - from: intake.account_ready
    to: account_case.account_ready
    delivery: one
`+connectAdapterYAML(opts))
	if opts.RootDefaultEntityIDSource {
		writeRootDefaultStaticMaterializer(t, root)
		return
	}
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: final-flow-instance-authoring\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
}

func connectAdapterYAML(opts Options) string {
	switch {
	case opts.BadConnectMapping:
		return `    using:
      instance:
        source: missing_source_account_id
        target: account_id
`
	case opts.DuplicateConnectMapping:
		return `    using:
      instance:
        source: [source_account_id, source_account_id]
        target: [account_id, account_id]
`
	default:
		return `    using:
      instance:
        source: source_account_id
        target: account_id
`
	}
}

func writeProducer(t testing.TB, root string, opts Options) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "intake", "schema.yaml"), `
name: intake
mode: static
pins:
  inputs:
    events:
      - name: request_received
        event: request.received
  outputs:
    events:
      - name: account_ready
        event: account.ready
`+producerOutputEvidenceYAML(opts))
	writeFile(t, filepath.Join(root, "flows", "intake", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "intake", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "intake", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "intake", "entities.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "intake", "events.yaml"), `
request.received:
  account_id: string
  score: string
  decision: string
account.ready:
  source_account_id: string
  score: string
  decision: string
`)
	writeFile(t, filepath.Join(root, "flows", "intake", "nodes.yaml"), `
intake-router:
  id: intake-router
  execution_type: system_node
  subscribes_to: [request.received]
  produces: [account.ready]
  event_handlers:
    request.received:
      emit:
        event: account.ready
`+producerEmitRouteYAML(opts)+`        fields:
          source_account_id: payload.account_id
          score: payload.score
          decision: payload.decision
`)
}

func producerOutputEvidenceYAML(opts Options) string {
	var b strings.Builder
	if !opts.MissingOutputKey {
		b.WriteString("        key: source_account_id\n")
	}
	if opts.MissingOutputCarries {
		b.WriteString("        carries: [score, decision]\n")
	} else {
		b.WriteString("        carries: [source_account_id, score, decision]\n")
	}
	return b.String()
}

func producerEmitRouteYAML(opts Options) string {
	if opts.ProducerTarget {
		return `        target:
          flow: account_case
          match:
            account_id: payload.account_id
`
	}
	if opts.ProducerBroadcast {
		return "        broadcast: true\n"
	}
	return ""
}

func writeTemplate(t testing.TB, root string, opts Options) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "account_case", "schema.yaml"), `
name: account_case
mode: template
initial_state: pending
states: [pending, reviewed]
terminal_states: [reviewed]
instance:
  by: account_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: account_ready
        event: account.ready
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "flows", "account_case", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account_case", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account_case", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "account_case", "events.yaml"), `
account.ready:
  source_account_id: string
  score: string
  decision: string
`)
	writeFile(t, filepath.Join(root, "flows", "account_case", "entities.yaml"), `
account_case_state:
  account_id:
    type: string
  score:
    type: string
  decision:
    type: string
`)
	writeFile(t, filepath.Join(root, "flows", "account_case", "nodes.yaml"), `
account-case-worker:
  id: account-case-worker
  execution_type: system_node
  subscribes_to: [account.ready]
  event_handlers:
    account.ready:
`+receiverSelectorYAML(opts)+`      data_accumulation:
        writes:
          - source_field: source_account_id
            target_field: account_id
          - source_field: score
            target_field: score
          - source_field: decision
            target_field: decision
      advances_to: reviewed
`)
}

func receiverSelectorYAML(opts Options) string {
	if !opts.UnsupportedReceiverSelector {
		return ""
	}
	return `      select_entity:
        by:
          account_id: payload.source_account_id
`
}

func writeCoordinator(t testing.TB, root string, opts Options) {
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
`+coordinatorWritesYAML(opts))
}

func coordinatorWritesYAML(opts Options) string {
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
	if opts.WrongMapValueShape {
		return firstMapWriteYAML("set", "entity.lead_index", "key:\n              ref: payload.lead_id", `
            value:
              undeclared: true
`)
	}
	if opts.UndeclaredMapTarget {
		return firstMapWriteYAML("set", "entity.missing_index", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	}
	if opts.UnsupportedMapOp {
		return firstMapWriteYAML("replace", "entity.lead_index", "key:\n              ref: payload.lead_id", `
            value:
              status: active
              score: 0
              observations: []
`)
	}
	if opts.BadListIndex {
		return directCoordinatorWriteYAML() + validCoordinatorWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: -1
            value:
              ref: payload.corrected_audit
`
	}
	return directCoordinatorWriteYAML() + validCoordinatorWritesPrefixYAML() + `          - op: update
            target: entity.audit_log
            index: 0
            value:
              ref: payload.corrected_audit
`
}

func directCoordinatorWriteYAML() string {
	return `          - source_field: coordinator_id
            target_field: coordinator_id
`
}

func validCoordinatorWritesPrefixYAML() string {
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

func writeLegacyStatic(t testing.TB, root string, opts Options) {
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "schema.yaml"), `
name: legacy_static
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events:
      - name: legacy_seen
        event: legacy.seen
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "events.yaml"), `
legacy.seen:
  legacy_id: string
  amount: number
`)
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "entities.yaml"), `
legacy_record:
  legacy_id:
    type: string
    indexed: true
  amount:
    type: number
    initial: 0
`)
	writeFile(t, filepath.Join(root, "flows", "legacy_static", "nodes.yaml"), `
legacy-writer:
  id: legacy-writer
  execution_type: system_node
  subscribes_to: [legacy.seen]
  event_handlers:
    legacy.seen:
`+legacyHandlerBody(opts))
}

func legacyHandlerBody(opts Options) string {
	switch {
	case opts.StaticCreateEntity:
		return `      create_entity: true
      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
`
	case opts.StaticSelectEntity:
		return `      select_entity:
        by:
          legacy_id: payload.legacy_id
      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
`
	case opts.StaticSelectOrCreate:
		return `      select_or_create_entity:
        by:
          legacy_id: payload.legacy_id
      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
`
	case opts.StaticMissingAcquisition:
		return `      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
`
	default:
		return `      emit:
        event: legacy.seen
        fields:
          legacy_id: payload.legacy_id
`
	}
}

func writeRootDefaultStaticMaterializer(t testing.TB, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "schema.yaml"), `
name: final-flow-instance-authoring
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: subject_created
        event: subject.created
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), `
subject.created:
  entity_id: string
  display_name: string
`)
	writeFile(t, filepath.Join(root, "entities.yaml"), `
subject:
  display_name: text
`)
	writeFile(t, filepath.Join(root, "nodes.yaml"), `
root-node:
  id: root-node
  execution_type: system_node
  subscribes_to: [subject.created]
  event_handlers:
    subject.created:
      data_accumulation:
        writes:
          - source_field: display_name
            target_field: display_name
`)
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
