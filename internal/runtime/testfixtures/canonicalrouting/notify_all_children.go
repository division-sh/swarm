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
}

// CopyNotifyAllChildren derives one closed variant from the checked-in owner.
func CopyNotifyAllChildren(t testing.TB, opts NotifyAllChildrenOptions) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "examples", "routing", "notify-all-children"), root)
	manifestFile := filepath.Join(root, "manifest.yaml")
	connectFile := filepath.Join(root, "schema.yaml")
	ownerSchema := filepath.Join(root, NotifyAllChildrenOwnerFlowID, "schema.yaml")
	ownerNodes := filepath.Join(root, NotifyAllChildrenOwnerFlowID, "nodes.yaml")
	ownerEntities := filepath.Join(root, NotifyAllChildrenOwnerFlowID, "entities.yaml")
	accountAgents := filepath.Join(root, NotifyAllChildrenChildFlowID, "agents.yaml")
	accountEvents := filepath.Join(root, NotifyAllChildrenChildFlowID, "events.yaml")
	accountSchema := filepath.Join(root, NotifyAllChildrenChildFlowID, "schema.yaml")
	if opts.OmitConnect {
		applyClosedReplacement(t, connectFile, `  - event: account.notify.requested
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
		writeClosedVariantFile(t, root, filepath.ToSlash(filepath.Join(NotifyAllChildrenOwnerFlowID, "types.yaml")), `types:
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
          from_fan_out: true
        on_complete:
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
		applyClosedReplacement(t, filepath.Join(root, NotifyAllChildrenOwnerFlowID, "events.yaml"), `account.notify.requested:
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
		applyClosedReplacement(t, manifestFile, `version: "1.0.0"`, `version: "2.0.0"`)
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
		applyClosedReplacement(t, manifestFile, `version: "1.0.0"`, `version: "3.0.0"`)
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
