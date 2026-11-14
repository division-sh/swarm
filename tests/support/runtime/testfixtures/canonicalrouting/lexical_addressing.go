package canonicalrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const crossAccountRow = "  - {event: ticket.cross, from: account/ticket, to: account, resolution: select, key_from: payload.target_account_id}\n"

func CopyLexicalAddressing(t testing.TB, crossAccount bool) string {
	t.Helper()
	root := t.TempDir()
	documents := map[string]string{
		"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml": `inputs: [opened, root.audit, cross.requested]
connect:
  - {event: opened, from: ., to: account, resolution: select-or-create}
  - {event: opened, from: ., to: account/ticket, resolution: select-or-create}
  - {event: root.audit, from: ., to: account, resolution: select}
  - {event: root.audit, from: ., to: account/audit}
  - {event: cross.requested, from: ., to: account, rename: cross.forwarded, resolution: select, key_from: payload.source_account_id}
  - {event: cross.requested, from: ., to: account/ticket, resolution: select}
`,
		"events.yaml":           "opened:\n  account_id: text\n  ticket_id: text\nroot.audit:\n  account_id: text\ncross.requested:\n  source_account_id: text\n  ticket_id: text\n  target_account_id: text\n",
		"account/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"account/schema.yaml": `instance: account_id
inputs: [opened, root.audit, cross.forwarded]
connect:
  - {event: ticket.done, from: ticket, to: audit}
`,
		"account/state.yaml":           "account_id: text\ncross_count: 0\n",
		"account/nodes.yaml":           "receiver:\n  opened: {}\n  root.audit: {}\n  cross.forwarded: {}\n",
		"account/ticket/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"account/ticket/schema.yaml":   "instance: ticket_id\ninputs: [opened, cross.requested]\noutputs: [ticket.done]\n",
		"account/ticket/state.yaml":    "ticket_id: text\n",
		"account/ticket/events.yaml":   "ticket.done:\n",
		"account/ticket/nodes.yaml":    "reporter:\n  opened:\n    emit: {event: ticket.done}\ncross-reporter:\n  cross.requested: {}\n",
		"account/audit/manifest.yaml":  "version: 1.0.0\nplatform_version: '*'\n",
		"account/audit/schema.yaml":    "stages: []\ninputs: [ticket.done, root.audit]\n",
		"account/audit/nodes.yaml":     "audit:\n  ticket.done: {}\n  root.audit: {}\n",
	}
	if !crossAccount {
		documents["schema.yaml"] = "inputs: [opened, root.audit]\nconnect:\n  - {event: opened, from: ., to: account, resolution: select-or-create}\n  - {event: root.audit, from: ., to: account, resolution: select}\n  - {event: root.audit, from: ., to: account/audit}\n"
		documents["events.yaml"] = "opened:\n  account_id: text\n  ticket_id: text\nroot.audit:\n  account_id: text\n"
		documents["account/schema.yaml"] = "instance: account_id\ninputs: [opened, root.audit]\noutputs: [ticket.opened]\nconnect:\n  - {event: ticket.opened, from: ., to: ticket, rename: opened, resolution: select-or-create}\n  - {event: ticket.done, from: ticket, to: audit}\n"
		documents["account/events.yaml"] = "ticket.opened:\n  account_id: text\n  ticket_id: text\n"
		documents["account/state.yaml"] = "account_id: text\n"
		documents["account/nodes.yaml"] = "receiver:\n  opened:\n    emit: {event: ticket.opened, fields: {account_id: payload.account_id, ticket_id: payload.ticket_id}}\n  root.audit: {}\n"
		documents["account/ticket/schema.yaml"] = "instance: ticket_id\ninputs: [opened]\noutputs: [ticket.done]\n"
		documents["account/ticket/nodes.yaml"] = "reporter:\n  opened:\n    emit: {event: ticket.done}\n"
	}
	if crossAccount {
		documents["schema.yaml"] += crossAccountRow
		documents["account/schema.yaml"] = strings.Replace(documents["account/schema.yaml"], "inputs: [opened, root.audit, cross.forwarded]", "inputs: [opened, root.audit, cross.forwarded, ticket.cross]", 1)
		documents["account/nodes.yaml"] += "  ticket.cross:\n    data_accumulation:\n      writes:\n        - {target_field: cross_count, value: state.cross_count + 1}\n"
		documents["account/ticket/schema.yaml"] = strings.Replace(documents["account/ticket/schema.yaml"], "outputs: [ticket.done]", "outputs: [ticket.done, ticket.cross]", 1)
		documents["account/ticket/events.yaml"] += "ticket.cross:\n  target_account_id: text\n"
		documents["account/ticket/nodes.yaml"] = strings.Replace(documents["account/ticket/nodes.yaml"], "  cross.requested: {}", "  cross.requested:\n    emit: {event: ticket.cross, fields: {target_account_id: payload.target_account_id}}", 1)
	}
	for label, body := range documents {
		writeClosedVariantFile(t, root, label, body)
	}
	return root
}

func CopyWrappedLexicalAddressing(t testing.TB, higherOwner bool) string {
	t.Helper()
	original := CopyLexicalAddressing(t, true)
	root := t.TempDir()
	copyTree(t, original, filepath.Join(root, "envelope"))
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	schema := "stages: []\n"
	if higherOwner {
		path := filepath.Join(root, "envelope", "schema.yaml")
		raw, err := os.ReadFile(path)
		if err != nil || strings.Count(string(raw), crossAccountRow) != 1 {
			t.Fatalf("wrapped cross-account fixture lost its exact row: %v", err)
		}
		writeClosedVariantFile(t, root, "envelope/schema.yaml", strings.Replace(string(raw), crossAccountRow, "", 1))
		schema += "connect:\n  - {event: ticket.cross, from: envelope/account/ticket, to: envelope/account, resolution: select, key_from: payload.target_account_id}\n"
	}
	writeClosedVariantFile(t, root, "schema.yaml", schema)
	return root
}

func CopyUnselectedHigherCompositionConnect(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, CopyCompositionConnect(t, CompositionConnectValid), filepath.Join(root, "envelope"))
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "envelope/schema.yaml", "stages: []\n")
	writeClosedVariantFile(t, root, "schema.yaml", "connect:\n  - {event: deploy.done, from: envelope/producer, to: envelope/consumer, rename: deploy.completed}\n")
	return root
}
