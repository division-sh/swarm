package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

func CopyKeyedRootRawIngress(t testing.TB) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "keyed-shop")
	for label, body := range map[string]string{
		"audit/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml": `# fixture: keyed-shop
instance: provider_event_id
stages: []
pins:
  inputs: [inbound.partner.raw]
`,
		"partner/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw], verify_secret: webhook_signing_secret}, cannot: [execute code]}\n",
		"partner/trigger.yaml":  "transport: webhook\npayload_object_required: true\nsecret: {required: true}\nsignature: {type: hmac_sha256, header: X-Signature, prefix: 'sha256=', signed_payload: raw_body}\ndelivery_id: {json_path: $.delivery_id, required: true}\nevent_type: {literal: update, required: true}\nevent_name: {literal: raw}\n",
		"state.yaml":            "provider_event_id: text\nprocessed_count: 0\n",
		"nodes.yaml": `receiver:
  execution_type: system_node
  subscribes_to: [inbound.partner.raw]
  event_handlers:
    inbound.partner.raw:
      data_accumulation:
        source_event: inbound.partner.raw
        writes:
          - {target_field: processed_count, value: state.processed_count + 1}
`,
		"audit/schema.yaml": "stages: []\n# fixture: audit\n",
	} {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

func CopyKeyedRootRawIngressCreationEvent(t testing.TB) string {
	t.Helper()
	root := CopyKeyedRootRawIngress(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "stages: []\n", "stages: []\nauto_emit_on_create: {event: root.created}\n")
	writeClosedVariantFile(t, root, "events.yaml", "root.created:\n  provider_event_id: text\n  processed_count: integer\n  creation_count: integer\n")
	applyClosedReplacement(t, filepath.Join(root, "state.yaml"), "processed_count: 0\n", "processed_count: 0\ncreation_count: 0\n")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "subscribes_to: [inbound.partner.raw]", "subscribes_to: [inbound.partner.raw, root.created]")
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "  event_handlers:\n", `  event_handlers:
    root.created:
      data_accumulation:
        source_event: root.created
        writes:
          - {target_field: creation_count, value: state.creation_count + 1}
`)
	return root
}

func CopyKeyedRootTelegramIngress(t testing.TB) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "keyed-chat")
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "audit/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "audit/schema.yaml", "stages: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", `# fixture: keyed-chat
instance: conversation_reference
stages: []
pins: {inputs: [inbound.telegram.raw, inbound.telegram.text_message]}
`)
	trigger, err := os.ReadFile(filepath.Join(RepoRoot(t), "packs/providers/telegram/trigger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	writeClosedVariantFile(t, root, "telegram/trigger.yaml", string(trigger))
	writeClosedVariantFile(t, root, "telegram/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw, text_message, callback_action], verify_secret: webhook_signing_secret, persist_dedupe_markers: true}, cannot: [execute code]}\n")
	writeClosedVariantFile(t, root, "state.yaml", "conversation_reference: text\nprocessed_count: 0\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "receiver:\n  execution_type: system_node\n  subscribes_to: [inbound.telegram.text_message]\n  event_handlers:\n    inbound.telegram.text_message:\n      data_accumulation:\n        source_event: inbound.telegram.text_message\n        writes: [{target_field: processed_count, value: state.processed_count + 1}]\n")
	return root
}

func CopyConcurrentKeyedRootRawIngress(t testing.TB) string {
	t.Helper()
	root := CopyKeyedRootRawIngressCreationEvent(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "instance: provider_event_id", "instance: provider")
	applyClosedReplacement(t, filepath.Join(root, "state.yaml"), "provider_event_id: text", "provider: text")
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "root.created:\n  provider_event_id: text", "root.created:\n  provider: text")
	return root
}

func CopySchemaOmittedConstructionTree(t testing.TB) string {
	t.Helper()
	root := CopyNestedKeyedConnectionSelection(t)
	removeClosedVariantFiles(t, root, "schema.yaml", "parent/middle/schema.yaml")
	writeClosedVariantFile(t, root, "resources/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "resources/data/probe.json", `{"value": 1}`)
	return root
}

// CopyNestedKeyedConnectionSelection gives two independent per-edge keys to
// a keyed parent and a keyed leaf beneath an eager keyless intermediate.
func CopyNestedKeyedConnectionSelection(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for label, body := range map[string]string{
		"manifest.yaml":                    "version: 1.0.0\nplatform_version: '*'\n",
		"parent/manifest.yaml":             "version: 1.0.0\nplatform_version: '*'\n",
		"parent/middle/leaf/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"parent/middle/manifest.yaml":      "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml": `# fixture: nested-construction
pins: {inputs: [start]}
connect:
  - {event: start, from: ., to: parent, resolution: select-or-create, key_from: payload.parent_key}
  - {event: start, from: ., to: parent/middle/leaf, resolution: select-or-create, key_from: payload.leaf_key}
`,
		"events.yaml":                    "start:\n  parent_key: text\n  leaf_key: text\n",
		"parent/schema.yaml":             "# fixture: parent\ninstance: id\npins: {inputs: [start]}\n",
		"parent/state.yaml":              "id: text\n",
		"parent/nodes.yaml":              "receiver:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      guard: {check: payload.parent_key != ''}\n",
		"parent/middle/schema.yaml":      "stages: []\n# fixture: middle\n",
		"parent/middle/leaf/schema.yaml": "# fixture: leaf\ninstance: id\npins: {inputs: [start]}\n",
		"parent/middle/leaf/state.yaml":  "id: text\n",
		"parent/middle/leaf/nodes.yaml":  "receiver:\n  execution_type: system_node\n  subscribes_to: [start]\n  event_handlers:\n    start:\n      guard: {check: payload.leaf_key != ''}\n",
	} {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

// CopyConnectionAncestorCandidates combines independent terminal receivers
// with an actual nested dependency; unrelated edges must not create ambiguity.
func CopyConnectionAncestorCandidates(t testing.TB, dependentEvent string, competing bool) string {
	t.Helper()
	if dependentEvent != "start" && dependentEvent != "other" {
		t.Fatalf("unsupported dependent event %q", dependentEvent)
	}
	root := CopyNestedKeyedConnectionSelection(t)
	edges := `  - {event: start, from: ., to: worker, resolution: select-or-create, key_from: payload.parent_key}
  - {event: start, from: ., to: worker, resolution: select-or-create, key_from: payload.leaf_key}
  - {event: start, from: ., to: parent, resolution: select-or-create, key_from: payload.parent_key}
`
	if competing {
		edges += "  - {event: start, from: ., to: parent, rename: alternate, resolution: select-or-create, key_from: payload.leaf_key}\n"
		writeClosedVariantFile(t, root, "parent/schema.yaml", "# fixture: parent\ninstance: id\npins: {inputs: [start, alternate]}\n")
		writeClosedVariantFile(t, root, "parent/nodes.yaml", "receiver:\n  execution_type: system_node\n  event_handlers:\n    start:\n      guard: {check: payload.parent_key != ''}\n    alternate:\n      guard: {check: payload.leaf_key != ''}\n")
	}
	rename := ""
	if dependentEvent == "other" {
		rename = "rename: start, "
	}
	edges += "  - {event: " + dependentEvent + ", from: ., to: parent/middle/leaf, " + rename + "resolution: select-or-create, key_from: payload.leaf_key}\n"
	for label, body := range map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"worker/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          "# fixture: construction-candidates\npins: {inputs: [start, other]}\nconnect:\n" + edges,
		"events.yaml":          "start:\n  parent_key: text\n  leaf_key: text\nother:\n  parent_key: text\n  leaf_key: text\n",
		"worker/schema.yaml":   "# fixture: worker\ninstance: id\npins: {inputs: [start]}\n",
		"worker/state.yaml":    "id: text\n",
		"worker/nodes.yaml":    "receiver:\n  execution_type: system_node\n  event_handlers:\n    start:\n      guard: {check: payload.leaf_key != ''}\n",
	} {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

// CopyNestedKeyedRawIngress keeps the same tree but exercises authenticated
// transport, state mutation and original construction evidence on both stores.
func CopyNestedKeyedRawIngress(t testing.TB) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "nested-shop")
	for label, body := range map[string]string{
		"manifest.yaml":                    "version: 1.0.0\nplatform_version: '*'\n",
		"parent/manifest.yaml":             "version: 1.0.0\nplatform_version: '*'\n",
		"parent/middle/leaf/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"parent/middle/manifest.yaml":      "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml": `# fixture: nested-shop
stages: []
pins: {inputs: [inbound.partner.raw]}
connect:
  - {event: inbound.partner.raw, from: ., to: parent, resolution: select-or-create, key_from: payload.provider_event_id}
  - {event: inbound.partner.raw, from: ., to: parent/middle/leaf, resolution: select-or-create, key_from: payload.provider}
`,
		"partner/manifest.yaml":          "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw], verify_secret: webhook_signing_secret}, cannot: [execute code]}\n",
		"partner/trigger.yaml":           "transport: webhook\npayload_object_required: true\nsecret: {required: true}\nsignature: {type: hmac_sha256, header: X-Signature, prefix: 'sha256=', signed_payload: raw_body}\ndelivery_id: {json_path: $.delivery_id, required: true}\nevent_type: {literal: update, required: true}\nevent_name: {literal: raw}\n",
		"parent/schema.yaml":             "# fixture: parent\ninstance: id\nstages: []\npins: {inputs: [inbound.partner.raw]}\n",
		"parent/state.yaml":              "id: text\nseen: 0\n",
		"parent/nodes.yaml":              "receiver:\n  execution_type: system_node\n  subscribes_to: [inbound.partner.raw]\n  event_handlers:\n    inbound.partner.raw:\n      data_accumulation:\n        source_event: inbound.partner.raw\n        writes: [{target_field: seen, value: state.seen + 1}]\n",
		"parent/middle/schema.yaml":      "stages: []\n# fixture: middle\n",
		"parent/middle/leaf/schema.yaml": "# fixture: leaf\ninstance: id\nstages: []\npins: {inputs: [inbound.partner.raw]}\n",
		"parent/middle/leaf/state.yaml":  "id: text\nseen: 0\n",
		"parent/middle/leaf/nodes.yaml":  "receiver:\n  execution_type: system_node\n  subscribes_to: [inbound.partner.raw]\n  event_handlers:\n    inbound.partner.raw:\n      data_accumulation:\n        source_event: inbound.partner.raw\n        writes: [{target_field: seen, value: state.seen + 1}]\n",
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
	for _, label := range []string{"schema.yaml", "parent", "partner"} {
		if err := os.Rename(filepath.Join(root, label), filepath.Join(root, "branch", label)); err != nil {
			t.Fatal(err)
		}
	}
	applyClosedReplacement(t, filepath.Join(root, "branch/schema.yaml"), "# fixture: nested-shop", "# fixture: branch")
	writeClosedVariantFile(t, root, "branch/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "schema.yaml", "# fixture: nested-shop\nstages: []\n")
	return root
}

func CopyNestedConcurrentRawIngress(t testing.TB, distinctLeaves bool) string {
	t.Helper()
	root := CopyNestedKeyedRawIngress(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "to: parent, resolution: select-or-create, key_from: payload.provider_event_id", "to: parent, resolution: select-or-create, key_from: payload.provider")
	if distinctLeaves {
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "to: parent/middle/leaf, resolution: select-or-create, key_from: payload.provider", "to: parent/middle/leaf, resolution: select-or-create, key_from: payload.provider_event_id")
	}
	applyClosedReplacement(t, filepath.Join(root, "parent/schema.yaml"), "stages: []\n", "stages: []\nauto_emit_on_create: {event: parent.created}\n")
	writeClosedVariantFile(t, root, "parent/events.yaml", "parent.created:\n  id: text\n  seen: integer\n  creation_count: integer\n")
	applyClosedReplacement(t, filepath.Join(root, "parent/state.yaml"), "seen: 0\n", "seen: 0\ncreation_count: 0\n")
	applyClosedReplacement(t, filepath.Join(root, "parent/nodes.yaml"), "subscribes_to: [inbound.partner.raw]", "subscribes_to: [inbound.partner.raw, parent.created]")
	applyClosedReplacement(t, filepath.Join(root, "parent/nodes.yaml"), "  event_handlers:\n", "  event_handlers:\n    parent.created:\n      data_accumulation:\n        source_event: parent.created\n        writes: [{target_field: creation_count, value: state.creation_count + 1}]\n")
	return root
}
