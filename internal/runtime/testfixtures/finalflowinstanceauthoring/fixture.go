package finalflowinstanceauthoring

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	PackageName = "template-select-or-create"

	ProducerFlowID    = "producer"
	ProducerNodeID    = "producer-node"
	ProducerInputPin  = "account_requested"
	ProducerOutputPin = "account_ready"
	ProducerInput     = "account.requested"
	ProducerOutput    = "account.ready"

	TemplateFlowID       = "account"
	TemplateNodeID       = "account-node"
	TemplateEntityType   = "account_state"
	TemplateInputPin     = "account_ready"
	TemplateInstanceBy   = "account_id"
	TemplatePayloadKey   = "account_id"
	TemplateFlowInstance = TemplateFlowID

	CoordinatorFlowID     = "coordinator"
	CoordinatorNodeID     = "coordinator-indexer"
	CoordinatorEntityType = "coordinator_state"
	CoordinatorInput      = "lead.observed"
	CoordinatorInstance   = CoordinatorFlowID

	LegacyFlowID = "legacy_static"
)

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
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectOrCreate)
	addTemplateStateOverlay(t, root)
	addCoordinatorOverlay(t, root, opts)
	if opts.StaticCreateEntity || opts.StaticSelectEntity || opts.StaticSelectOrCreate || opts.StaticMissingAcquisition {
		addLegacyStaticOverlay(t, root, opts)
	}
	if opts.RootDefaultEntityIDSource {
		addRootDefaultStaticMaterializer(t, root)
	}
	applyRoutingMutation(t, root, opts)
	return root
}

func addTemplateStateOverlay(t testing.TB, root string) {
	t.Helper()
	producerEvents := filepath.Join(root, "flows", ProducerFlowID, "events.yaml")
	for _, event := range []string{ProducerInput, ProducerOutput} {
		canonicalrouting.ReplaceFile(t, producerEvents,
			event+":\n  account_id: text\n",
			event+":\n  account_id: text\n  score: text\n  decision: text\n")
	}
	producerNodes := filepath.Join(root, "flows", ProducerFlowID, "nodes.yaml")
	canonicalrouting.ReplaceFile(t, producerNodes,
		"          account_id: payload.account_id\n",
		"          account_id: payload.account_id\n          score: payload.score\n          decision: payload.decision\n")

	templateSchema := filepath.Join(root, "flows", TemplateFlowID, "schema.yaml")
	canonicalrouting.ReplaceFile(t, templateSchema,
		"mode: template\n",
		"mode: template\ninitial_state: pending\nstates: [pending, reviewed]\nterminal_states: [reviewed]\n")
	templateEvents := filepath.Join(root, "flows", TemplateFlowID, "events.yaml")
	canonicalrouting.ReplaceFile(t, templateEvents,
		"account_id: text\n",
		"account_id: text\n  score: text\n  decision: text\n")
	templateEntities := filepath.Join(root, "flows", TemplateFlowID, "entities.yaml")
	canonicalrouting.ReplaceFile(t, templateEntities,
		"    _unused_reason: receiver instance identity\n",
		"    _unused_reason: receiver instance identity\n  score:\n    type: text\n  decision:\n    type: text\n")
	templateNodes := filepath.Join(root, "flows", TemplateFlowID, "nodes.yaml")
	canonicalrouting.ReplaceFile(t, templateNodes,
		"    account.ready: {}\n",
		`    account.ready:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: score
            target_field: score
          - source_field: decision
            target_field: decision
      advances_to: reviewed
`)
}

func addCoordinatorOverlay(t testing.TB, root string, opts Options) {
	t.Helper()
	packageFile := filepath.Join(root, "package.yaml")
	canonicalrouting.ReplaceFile(t, packageFile,
		"  - id: account\n    flow: account\n    mode: template\n",
		"  - id: account\n    flow: account\n    mode: template\n  - id: coordinator\n    flow: coordinator\n    mode: singleton\n")
	canonicalrouting.WriteFile(t, root, "flows/coordinator/policy.yaml", "{}\n")
	canonicalrouting.WriteFile(t, root, "flows/coordinator/tools.yaml", "{}\n")
	canonicalrouting.WriteFile(t, root, "flows/coordinator/agents.yaml", "{}\n")
	canonicalrouting.WriteFile(t, root, "flows/coordinator/types.yaml", `
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
	canonicalrouting.WriteFile(t, root, "flows/coordinator/entities.yaml", `
coordinator_state:
  coordinator_id: text
  lead_index: map[text]LeadScore
  audit_log: "[AuditEntry]"
`)
	canonicalrouting.WriteFile(t, root, "flows/coordinator/events.yaml", `
lead.observed:
  coordinator_id: text
  lead_id: text
  observation: Observation
  audit: AuditEntry
  followup_audit: AuditEntry
  corrected_audit: AuditEntry
`)
	// routing-example-census: different-concept issue=none owner=flow_instance_authoring.contained_state_model proof=TestFinalFlowInstanceAuthoringFixture_CoversSealedContractOwners
	canonicalrouting.WriteFile(t, root, "flows/coordinator/schema.yaml", `
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
	canonicalrouting.WriteFile(t, root, "flows/coordinator/nodes.yaml", `
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

func applyRoutingMutation(t testing.TB, root string, opts Options) {
	// routing-example-census: negative-mutation issue=none owner=examples.routing.template_select_or_create proof=TestFinalFlowInstanceAuthoringFixture_FailClosedMatrix
	t.Helper()
	inputSchema := filepath.Join(root, "flows", TemplateFlowID, "schema.yaml")
	if opts.MissingOutputKey {
		canonicalrouting.ReplaceFile(t, inputSchema, "          instance_key: account_id\n", "")
	}
	if opts.MissingOutputCarries {
		canonicalrouting.ReplaceFile(t, inputSchema,
			"        carries:\n          account_id:\n            from: payload.account_id\n            type: text\n", "")
	}
	packageFile := filepath.Join(root, "package.yaml")
	if opts.BadConnectMapping || opts.DuplicateConnectMapping {
		source := "missing_account_id"
		target := "account_id"
		if opts.DuplicateConnectMapping {
			source = "[account_id, account_id]"
			target = "[account_id, account_id]"
		}
		canonicalrouting.ReplaceFile(t, packageFile,
			"  - from: producer.account_ready\n    to: account.account_ready\n",
			"  - from: producer.account_ready\n    to: account.account_ready\n    using:\n      instance:\n        source: "+source+"\n        target: "+target+"\n")
	}
	templateNodes := filepath.Join(root, "flows", TemplateFlowID, "nodes.yaml")
	if opts.UnsupportedReceiverSelector {
		canonicalrouting.ReplaceFile(t, templateNodes,
			"    account.ready:\n      data_accumulation:\n",
			"    account.ready:\n      select_entity:\n        by:\n          account_id: payload.account_id\n      data_accumulation:\n")
	}
	producerNodes := filepath.Join(root, "flows", ProducerFlowID, "nodes.yaml")
	if opts.ProducerTarget {
		canonicalrouting.ReplaceFile(t, producerNodes,
			"        event: account.ready\n        fields:\n",
			"        event: account.ready\n        target:\n          flow: account\n          match:\n            account_id: payload.account_id\n        fields:\n")
	}
	if opts.ProducerBroadcast {
		canonicalrouting.ReplaceFile(t, producerNodes,
			"        event: account.ready\n        fields:\n",
			"        event: account.ready\n        broadcast: true\n        fields:\n")
	}
}

func addLegacyStaticOverlay(t testing.TB, root string, opts Options) {
	t.Helper()
	packageFile := filepath.Join(root, "package.yaml")
	canonicalrouting.ReplaceFile(t, packageFile,
		"  - id: coordinator\n    flow: coordinator\n    mode: singleton\n",
		"  - id: coordinator\n    flow: coordinator\n    mode: singleton\n  - id: legacy_static\n    flow: legacy_static\n    mode: static\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		canonicalrouting.WriteFile(t, root, "flows/legacy_static/"+file, "{}\n")
	}
	canonicalrouting.WriteFile(t, root, "flows/legacy_static/schema.yaml", `
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
	canonicalrouting.WriteFile(t, root, "flows/legacy_static/events.yaml", `
legacy.seen:
  legacy_id: text
  amount: number
`)
	canonicalrouting.WriteFile(t, root, "flows/legacy_static/entities.yaml", `
legacy_record:
  legacy_id:
    type: text
    indexed: true
  amount:
    type: number
    initial: 0
`)
	canonicalrouting.WriteFile(t, root, "flows/legacy_static/nodes.yaml", `
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
		return "      create_entity: true\n      data_accumulation:\n        writes:\n          - source_field: amount\n            target_field: amount\n"
	case opts.StaticSelectEntity:
		return "      select_entity:\n        by:\n          legacy_id: payload.legacy_id\n      data_accumulation:\n        writes:\n          - source_field: amount\n            target_field: amount\n"
	case opts.StaticSelectOrCreate:
		return "      select_or_create_entity:\n        by:\n          legacy_id: payload.legacy_id\n      data_accumulation:\n        writes:\n          - source_field: amount\n            target_field: amount\n"
	default:
		return "      data_accumulation:\n        writes:\n          - source_field: amount\n            target_field: amount\n"
	}
}

func addRootDefaultStaticMaterializer(t testing.TB, root string) {
	t.Helper()
	canonicalrouting.WriteFile(t, root, "entities.yaml", "subject:\n  display_name: text\n")
	canonicalrouting.WriteFile(t, root, "events.yaml", "subject.created:\n  entity_id: text\n  display_name: text\n")
	canonicalrouting.WriteFile(t, root, "schema.yaml", `
name: template-select-or-create
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
	canonicalrouting.WriteFile(t, root, "nodes.yaml", `
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

func coordinatorWritesYAML(opts Options) string {
	if opts.DynamicBracketTarget {
		return firstMapWriteYAML("set", "entity.lead_index[payload.lead_id]", "key:\n              ref: payload.lead_id", leadScoreValueYAML())
	}
	if opts.MissingMapKey {
		return firstMapWriteYAML("set", "entity.lead_index", "", leadScoreValueYAML())
	}
	if opts.WrongMapValueShape {
		return firstMapWriteYAML("set", "entity.lead_index", "key:\n              ref: payload.lead_id", "\n            value:\n              undeclared: true\n")
	}
	if opts.UndeclaredMapTarget {
		return firstMapWriteYAML("set", "entity.missing_index", "key:\n              ref: payload.lead_id", leadScoreValueYAML())
	}
	if opts.UnsupportedMapOp {
		return firstMapWriteYAML("replace", "entity.lead_index", "key:\n              ref: payload.lead_id", leadScoreValueYAML())
	}
	index := 0
	if opts.BadListIndex {
		index = -1
	}
	return directCoordinatorWriteYAML() + validCoordinatorWritesPrefixYAML() + "          - op: update\n            target: entity.audit_log\n            index: " + stringIndex(index) + "\n            value:\n              ref: payload.corrected_audit\n"
}

func leadScoreValueYAML() string {
	return "\n            value:\n              status: active\n              score: 0\n              observations: []\n"
}

func directCoordinatorWriteYAML() string {
	return "          - source_field: coordinator_id\n            target_field: coordinator_id\n"
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
	out := directCoordinatorWriteYAML() + "          - op: " + op + "\n            target: " + target + "\n"
	if strings.TrimSpace(keyBlock) != "" {
		out += "            " + strings.ReplaceAll(strings.TrimRight(keyBlock, "\n"), "\n", "\n            ") + "\n"
	}
	return out + strings.TrimLeft(valueBlock, "\n")
}

func stringIndex(index int) string {
	if index < 0 {
		return "-1"
	}
	return "0"
}
