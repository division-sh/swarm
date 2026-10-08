package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

func CopyKeyedRootRawIngress(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for label, body := range map[string]string{
		"schema.yaml": `name: keyed-shop
instance: provider_event_id
stages: []
pins:
  inputs: [account.opened]
ingress:
  providers:
    - provider: partner
      signing_secret: webhook_signing.partner
      admission:
        kind: raw
        event: account.opened
        payload: json
        authentication: {kind: hmac_sha256, header: X-Signature, prefix: "sha256=", encoding: hex}
        delivery_id: {source: json_path, json_path: "$.delivery_id"}
`,
		"events.yaml":   "account.opened:\n  provider: text\n  provider_event_id: text\n  provider_event_type: text\n  data: json\n",
		"entities.yaml": "account_state:\n  provider_event_id: text\n  processed_count: {type: integer, initial: 0}\n",
		"nodes.yaml": `receiver:
  execution_type: system_node
  subscribes_to: [account.opened]
  event_handlers:
    account.opened:
      data_accumulation:
        source_event: account.opened
        writes:
          - {target_field: processed_count, value: entity.processed_count + 1}
`,
		"audit/schema.yaml": "name: audit\n",
	} {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

func CopyKeyedRootRawIngressCreationEvent(t testing.TB) string {
	t.Helper()
	root := CopyKeyedRootRawIngress(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "stages: []\n", "stages: []\nauto_emit_on_create: {event: root.created}\n")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "account.opened:\n", "root.created:\n  provider_event_id: text\n  processed_count: integer\n  creation_count: integer\naccount.opened:\n")
	applyClosedReplacement(t, filepath.Join(root, "entities.yaml"), "  processed_count: {type: integer, initial: 0}\n", "  processed_count: {type: integer, initial: 0}\n  creation_count: {type: integer, initial: 0}\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "subscribes_to: [account.opened]", "subscribes_to: [account.opened, root.created]")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "  event_handlers:\n", `  event_handlers:
    root.created:
      data_accumulation:
        source_event: root.created
        writes:
          - {target_field: creation_count, value: entity.creation_count + 1}
`)
	return root
}

func CopyKeyedRootTelegramIngress(t testing.TB) string {
	t.Helper()
	root := CopyKeyedRootRawIngress(t)
	writeClosedVariantFile(t, root, "schema.yaml", `name: keyed-chat
instance: conversation_reference
stages: []
pins: {inputs: [inbound.telegram, inbound.telegram.text_message]}
ingress:
  providers:
    - provider: telegram
      signing_secret: webhook_signing.telegram
      admission: {kind: pack, pack: {id: provider.telegram}}
`)
	removeClosedVariantFiles(t, root, "events.yaml")
	writeClosedVariantFile(t, root, "entities.yaml", "chat_state:\n  conversation_reference: text\n  processed_count: {type: integer, initial: 0}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "receiver:\n  execution_type: system_node\n  subscribes_to: [inbound.telegram.text_message]\n  event_handlers:\n    inbound.telegram.text_message:\n      data_accumulation:\n        source_event: inbound.telegram.text_message\n        writes: [{target_field: processed_count, value: entity.processed_count + 1}]\n")
	return root
}

func CopySchemaOmittedConstructionTree(t testing.TB) string {
	t.Helper()
	root := CopyNestedKeyedConnectionSelection(t)
	removeClosedVariantFiles(t, root, "schema.yaml", "parent/middle/schema.yaml")
	writeClosedVariantFile(t, root, "resources/data/probe.json", `{"value": 1}`)
	return root
}

// CopyNestedKeyedConnectionSelection gives two independent per-edge keys to
// a keyed parent and a keyed leaf beneath an eager keyless intermediate.
func CopyNestedKeyedConnectionSelection(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for label, body := range map[string]string{
		"schema.yaml": `name: nested-construction
pins: {inputs: [start]}
connect:
  - {event: start, from: ., to: parent, resolution: select-or-create, key_from: payload.parent_key}
  - {event: start, from: ., to: parent/middle/leaf, resolution: select-or-create, key_from: payload.leaf_key}
`,
		"events.yaml":                      "start:\n  parent_key: text\n  leaf_key: text\n",
		"parent/schema.yaml":               "name: parent\ninstance: id\npins: {inputs: [start]}\n",
		"parent/entities.yaml":             "parent_state:\n  id: text\n",
		"parent/nodes.yaml":                "receiver:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      guard: {check: payload.parent_key != ''}\n",
		"parent/middle/schema.yaml":        "name: middle\n",
		"parent/middle/leaf/schema.yaml":   "name: leaf\ninstance: id\npins: {inputs: [start]}\n",
		"parent/middle/leaf/entities.yaml": "leaf_state:\n  id: text\n",
		"parent/middle/leaf/nodes.yaml":    "receiver:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      guard: {check: payload.leaf_key != ''}\n",
	} {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

// CopyNestedKeyedRawIngress keeps the same tree but exercises authenticated
// transport, state mutation and original construction evidence on both stores.
func CopyNestedKeyedRawIngress(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for label, body := range map[string]string{
		"schema.yaml": `name: nested-shop
stages: []
pins: {inputs: [account.opened]}
ingress:
  providers:
    - provider: partner
      signing_secret: webhook_signing.partner
      admission:
        kind: raw
        event: account.opened
        payload: json
        authentication: {kind: hmac_sha256, header: X-Signature, prefix: "sha256=", encoding: hex}
        delivery_id: {source: json_path, json_path: "$.delivery_id"}
connect:
  - {event: account.opened, from: ., to: parent, resolution: select-or-create, key_from: payload.provider_event_id}
  - {event: account.opened, from: ., to: parent/middle/leaf, resolution: select-or-create, key_from: payload.provider}
`,
		"events.yaml":                      "account.opened:\n  provider: text\n  provider_event_id: text\n  provider_event_type: text\n  data: json\n",
		"parent/schema.yaml":               "name: parent\ninstance: id\nstages: []\npins: {inputs: [account.opened]}\n",
		"parent/entities.yaml":             "parent_state:\n  id: text\n  seen: {type: integer, initial: 0}\n",
		"parent/nodes.yaml":                "receiver:\n  execution_type: system_node\n  subscribes_to: [account.opened]\n  event_handlers:\n    account.opened:\n      data_accumulation:\n        source_event: account.opened\n        writes: [{target_field: seen, value: entity.seen + 1}]\n",
		"parent/middle/schema.yaml":        "name: middle\n",
		"parent/middle/leaf/schema.yaml":   "name: leaf\ninstance: id\nstages: []\npins: {inputs: [account.opened]}\n",
		"parent/middle/leaf/entities.yaml": "leaf_state:\n  id: text\n  seen: {type: integer, initial: 0}\n",
		"parent/middle/leaf/nodes.yaml":    "receiver:\n  execution_type: system_node\n  subscribes_to: [account.opened]\n  event_handlers:\n    account.opened:\n      data_accumulation:\n        source_event: account.opened\n        writes: [{target_field: seen, value: entity.seen + 1}]\n",
	} {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

func CopyNestedDeclarationLocalRawIngress(t testing.TB) string {
	t.Helper()
	root := CopyNestedKeyedRawIngress(t)
	if err := os.Mkdir(filepath.Join(root, "branch"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"schema.yaml", "events.yaml", "parent"} {
		if err := os.Rename(filepath.Join(root, label), filepath.Join(root, "branch", label)); err != nil {
			t.Fatal(err)
		}
	}
	applyClosedReplacement(t, filepath.Join(root, "branch/schema.yaml"), "name: nested-shop", "name: branch")
	writeClosedVariantFile(t, root, "schema.yaml", "name: nested-shop\nstages: []\n")
	return root
}

func CopyNestedConcurrentRawIngress(t testing.TB, distinctLeaves bool) string {
	t.Helper()
	root := CopyNestedKeyedRawIngress(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "to: parent, resolution: select-or-create, key_from: payload.provider_event_id", "to: parent, resolution: select-or-create, key_from: payload.provider")
	if distinctLeaves {
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "to: parent/middle/leaf, resolution: select-or-create, key_from: payload.provider", "to: parent/middle/leaf, resolution: select-or-create, key_from: payload.provider_event_id")
	}
	return root
}
