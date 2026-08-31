package canonicalrouting

import (
	"path/filepath"
	"testing"
)

const (
	NotifyAllChildrenOwnerFlowID       = "portfolio"
	NotifyAllChildrenOwnerOutputPin    = "account.notify.requested"
	NotifyAllChildrenOwnerTriggerEvent = "portfolio.notify.requested"
	NotifyAllChildrenEvent             = "account.notify.requested"
	NotifyAllChildrenChildFlowID       = "account"
	NotifyAllChildrenChildInputPin     = "account.notify.requested"
)

const canonicalNotifyAllChildrenAgents = `account-worker:
  type: generic
  role: account_worker
  intent: prompts/account-worker.md
  model: regular
  memory: false
  subscriptions:
    - account.notify.requested
  emit_events:
    - account.notification.completed
  mock:
    kind: python
    module: mocks/account-worker.py
`

// NotifyAllChildrenOptions identifies the closed negative and topology
// overlays derived from the checked-in notify-all-children example.
type NotifyAllChildrenOptions struct {
	OmitOutputPin               bool
	OmitConnect                 bool
	MissingEmitField            bool
	ProducerTarget              bool
	ProducerBroadcast           bool
	ObjectMembership            bool
	UndeclaredPayloadMembership bool
	ExplicitAgentName           bool
	AgentTopologyRevision       int
	AutoEmitOnCreate            bool
	AutoEmitEventRevision       int
	FanOutDeliveryBarrier       bool
	NumericRegistrationRows     bool
	NumericReporterSink         bool
}

// CopyNotifyAllChildren derives one closed variant from the checked-in owner.
func CopyNotifyAllChildren(t testing.TB, opts NotifyAllChildrenOptions) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "examples", "routing", "notify-all-children"), root)
	packageFile := filepath.Join(root, "package.yaml")
	ownerSchema := filepath.Join(root, "flows", NotifyAllChildrenOwnerFlowID, "schema.yaml")
	ownerNodes := filepath.Join(root, "flows", NotifyAllChildrenOwnerFlowID, "nodes.yaml")
	ownerEntities := filepath.Join(root, "flows", NotifyAllChildrenOwnerFlowID, "entities.yaml")
	accountAgents := filepath.Join(root, "flows", NotifyAllChildrenChildFlowID, "agents.yaml")
	accountNodes := filepath.Join(root, "flows", NotifyAllChildrenChildFlowID, "nodes.yaml")
	accountEntities := filepath.Join(root, "flows", NotifyAllChildrenChildFlowID, "entities.yaml")
	accountEvents := filepath.Join(root, "flows", NotifyAllChildrenChildFlowID, "events.yaml")
	accountSchema := filepath.Join(root, "flows", NotifyAllChildrenChildFlowID, "schema.yaml")
	if opts.OmitConnect {
		applyClosedReplacement(t, packageFile, `  - event: account.notify.requested
    from: portfolio
    to: account
`, "")
	}
	if opts.OmitOutputPin {
		applyClosedReplacement(t, ownerSchema, "      - account.notify.requested\n", "")
	}
	if opts.MissingEmitField {
		applyClosedReplacement(t, ownerNodes, "            command: payload.command\n", "")
	}
	if opts.ProducerTarget {
		applyClosedReplacement(t, ownerNodes, "          event: account.notify.requested\n", `          event: account.notify.requested
          target:
            flow: account
            match:
              account_id: account_id
`)
	}
	if opts.ProducerBroadcast {
		applyClosedReplacement(t, ownerNodes, "          event: account.notify.requested\n", "          event: account.notify.requested\n          broadcast: true\n")
	}
	if opts.ObjectMembership {
		applyClosedReplacement(t, ownerEntities, "  account_ids: \"[text]\"\n", "  account_ids: \"[AccountRef]\"\n")
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join("flows", NotifyAllChildrenOwnerFlowID, "types.yaml")), `types:
  AccountRef:
    account_id: text
`)
		applyClosedReplacement(t, ownerNodes, "            account_id: account_id\n", "            account_id: account_id.account_id\n")
	}
	if opts.UndeclaredPayloadMembership {
		applyClosedReplacement(t, ownerNodes, "        items_from: entity.account_ids\n", `        items_from: payload.account_ids
        identity: account_id
`)
	}
	if opts.AutoEmitOnCreate {
		applyClosedReplacement(t, accountEvents, "account.notification.completed: {}\n", `account.notification.completed: {}
account.created:
  account_id: text
  template_instance_key: text?
  template_instance_source_event: text?
`)
		applyClosedReplacement(t, accountSchema, "states: [active, completed]\n", `states: [active, completed]
auto_emit_on_create:
  event: account.created
`)
		applyClosedReplacement(t, accountSchema, `      - event: account.notify.requested
        resolution:
          mode: select
`, `      - event: account.notify.requested
        resolution:
          mode: select
  outputs:
    events:
      - account.created
`)
	}
	if opts.FanOutDeliveryBarrier {
		applyClosedReplacement(t, ownerNodes, `            command: payload.command
`, `            command: payload.command
      join:
        id: all-account-notifications-delivered
        members:
          from_fan_out: cf377b4f-e952-4ddb-9ecc-a1f380af032d
        on_complete:
          element_id: 4c6f93a5-21f9-40d0-8b2a-7b074a11e30d
          emit:
            event: portfolio.notify.completed
            fields:
              total: join.total
              succeeded: join.dispositions.succeeded
              dead_lettered: join.dispositions.dead_lettered
              no_route: join.dispositions.no_route
              semantic_rejected: join.dispositions.semantic_rejected
              canceled: join.dispositions.canceled
`)
		applyClosedReplacement(t, filepath.Join(root, "flows", NotifyAllChildrenOwnerFlowID, "events.yaml"), `account.notify.requested:
  key: account_id
  account_id: text
  command: text
`, `account.notify.requested:
  key: account_id
  account_id: text
  command: text
portfolio.notify.completed:
  swarm:
    consumer: external
  total: integer
  succeeded: integer
  dead_lettered: integer
  no_route: integer
  semantic_rejected: integer
  canceled: integer
`)
		applyClosedReplacement(t, ownerSchema, `      - account.notify.requested
`, `      - account.notify.requested
      - portfolio.notify.completed
`)
	}
	if opts.NumericRegistrationRows {
		applyClosedReplacement(t, filepath.Join(root, "flows", NotifyAllChildrenOwnerFlowID, "events.yaml"), `  account_ids: "[text]"
`, `  account_ids: "[json]"
`)
		applyClosedReplacement(t, ownerEntities, `  account_ids: "[text]"
`, `  account_ids: "[json]"
`)
		applyClosedReplacement(t, ownerNodes, `        as: account_id
        max_items: 100
        emit:
          event: account.registered
          fields:
            account_id: account_id
`, `        as: account
        identity: account.account_id
        max_items: 100
        emit:
          event: account.registered
          fields:
            portfolio_id: payload.portfolio_id
            account_id: account.account_id
            eng_roles: account.eng_roles
            gem_score: account.gem_score
`)
		applyClosedReplacement(t, ownerNodes, `        as: account_id
        max_items: 100
        emit:
          event: account.notify.requested
          fields:
            account_id: account_id
`, `        as: account
        identity: account.account_id
        max_items: 100
        emit:
          event: account.notify.requested
          fields:
            account_id: account.account_id
`)
		applyClosedReplacement(t, filepath.Join(root, "flows", NotifyAllChildrenOwnerFlowID, "events.yaml"), `account.registered:
  key: account_id
  account_id: text
`, `account.registered:
  key: account_id
  portfolio_id: text
  account_id: text
  eng_roles: integer
  gem_score: number
`)
		applyClosedReplacement(t, accountNodes, `    account.registered:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
`, `    account.registered:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: eng_roles
            target_field: eng_roles
          - source_field: gem_score
            target_field: gem_score
`)
		applyClosedReplacement(t, accountEntities, `account_state:
  account_id: text
  last_command: text
`, `account_state:
  account_id: text
  eng_roles: integer
  gem_score: number
  last_command: text
`)
		if opts.NumericReporterSink {
			applyClosedReplacement(t, packageFile, `  - event: account.registered
    from: portfolio
    to: account
`, "")
			applyClosedReplacement(t, ownerEntities, `  account_ids: "[json]"
`, `  account_ids: "[json]"
  observed_account_id: text
  observed_eng_roles: integer
  observed_gem_score: number
`)
			applyClosedReplacement(t, ownerNodes, "portfolio-coordinator:\n", `numeric-registration-observer:
  id: numeric-registration-observer
  execution_type: system_node
  subscribes_to:
    - account.registered
  event_handlers:
    account.registered:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: observed_account_id
          - source_field: eng_roles
            target_field: observed_eng_roles
          - source_field: gem_score
            target_field: observed_gem_score
portfolio-coordinator:
`)
		}
	}
	if opts.AutoEmitEventRevision == 2 {
		applyClosedReplacement(t, accountEvents, "account.created", "account.revised")
		applyClosedReplacement(t, accountSchema, "account.created", "account.revised")
		applyClosedReplacement(t, accountSchema, "account.created", "account.revised")
	} else if opts.AutoEmitEventRevision != 0 && opts.AutoEmitEventRevision != 1 {
		t.Fatalf("unsupported auto-emit event revision %d", opts.AutoEmitEventRevision)
	}
	switch opts.AgentTopologyRevision {
	case 0:
	case 1:
		applyClosedReplacement(t, accountAgents, canonicalNotifyAllChildrenAgents, `reader:
  id: account-reader
  type: generic
  role: reader-v1
  intent: {inline: "Read account registration events."}
  model: regular
  subscriptions:
    - account.registered
retired:
  id: account-retired
  type: generic
  role: retired
  intent: {inline: "Handle account notification requests."}
  model: regular
  subscriptions:
    - account.notify.requested
`)
	case 2:
		applyClosedReplacement(t, packageFile, `version: "1.0.0"`, `version: "2.0.0"`)
		applyClosedReplacement(t, accountAgents, canonicalNotifyAllChildrenAgents, `reader:
  id: account-reader
  type: generic
  role: reader-v2
  intent: {inline: "Read account registration and notification events."}
  model: regular
  subscriptions:
    - account.registered
    - account.notify.requested
writer:
  id: account-writer
  type: generic
  role: writer
  intent: {inline: "Write account notification results."}
  model: regular
  subscriptions:
    - account.notify.requested
`)
	case 3:
		applyClosedReplacement(t, packageFile, `version: "1.0.0"`, `version: "3.0.0"`)
		applyClosedReplacement(t, accountAgents, canonicalNotifyAllChildrenAgents, `reader:
  id: account-reader
  type: generic
  role: reader-v3
  intent: {inline: "Read account registration and notification events."}
  model: regular
  subscriptions:
    - account.registered
    - account.notify.requested
writer:
  id: account-writer
  type: generic
  role: writer
  intent: {inline: "Write account notification results."}
  model: regular
  subscriptions:
    - account.notify.requested
retired:
  id: account-retired
  type: generic
  role: returned
  intent: {inline: "Handle account notification requests."}
  model: regular
  subscriptions:
    - account.notify.requested
`)
	default:
		t.Fatalf("unsupported agent topology revision %d", opts.AgentTopologyRevision)
	}
	if opts.ExplicitAgentName {
		applyClosedReplacement(t, accountAgents, "account-worker:\n", "account-worker:\n  id: account-handler\n")
	}
	return root
}
