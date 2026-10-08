package canonicalrouting

import "testing"

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
