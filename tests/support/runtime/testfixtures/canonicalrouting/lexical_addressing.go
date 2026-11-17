package canonicalrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const crossAccountRow = "  - {event: ticket.cross, from: account/ticket, to: account, resolution: select, key_from: payload.target_account_id}\n"

type CrossInstanceConnectionShape uint8

const (
	CrossInstanceSameKeyedFlow CrossInstanceConnectionShape = iota + 1
	CrossInstanceNestedKeyless
)

func crossInstanceConnectionRow(shape CrossInstanceConnectionShape) string {
	if shape == CrossInstanceSameKeyedFlow {
		return "  - {event: account.cross, from: account, to: account, rename: account.cross.accepted, resolution: select, key_from: payload.target_account_id}\n"
	}
	return "  - {event: ticket.cross, from: account/ticket, to: account/audit, resolution: select, key_from: payload.target_account_id}\n"
}

func CopyCrossInstanceConnection(t testing.TB, shape CrossInstanceConnectionShape) string {
	t.Helper()
	root := CopyLexicalAddressing(t, true)
	switch shape {
	case CrossInstanceSameKeyedFlow:
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), crossAccountRow, crossInstanceConnectionRow(shape))
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "  - {event: cross.requested, from: ., to: account/ticket, resolution: select}\n", "")
		writeClosedVariantFile(t, root, "account/schema.yaml", `instance: account_id
inputs: [opened, root.audit, cross.forwarded, account.cross.accepted]
outputs: [account.cross]
connect:
  - {event: ticket.done, from: ticket, to: audit}
`)
		writeClosedVariantFile(t, root, "account/events.yaml", "account.cross:\n  target_account_id: text\n")
		writeClosedVariantFile(t, root, "account/nodes.yaml", `receiver:
  opened: {}
  root.audit: {}
  cross.forwarded:
    emit: {event: account.cross, fields: {target_account_id: payload.target_account_id}}
  account.cross.accepted:
    data_accumulation:
      writes: [{target_field: cross_count, value: state.cross_count + 1}]
`)
		writeClosedVariantFile(t, root, "account/ticket/schema.yaml", "instance: ticket_id\ninputs: [opened]\noutputs: [ticket.done]\n")
		writeClosedVariantFile(t, root, "account/ticket/events.yaml", "ticket.done:\n")
		writeClosedVariantFile(t, root, "account/ticket/nodes.yaml", "reporter:\n  opened:\n    emit: {event: ticket.done}\n")
	case CrossInstanceNestedKeyless:
		applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), crossAccountRow, crossInstanceConnectionRow(shape))
		writeClosedVariantFile(t, root, "account/schema.yaml", "instance: account_id\ninputs: [opened, root.audit, cross.forwarded]\nconnect:\n  - {event: ticket.done, from: ticket, to: audit}\n")
		writeClosedVariantFile(t, root, "account/nodes.yaml", "receiver:\n  opened: {}\n  root.audit: {}\n  cross.forwarded: {}\n")
		writeClosedVariantFile(t, root, "account/audit/schema.yaml", "stages: []\ninputs: [ticket.done, root.audit, ticket.cross]\n")
		writeClosedVariantFile(t, root, "account/audit/state.yaml", "cross_count: 0\n")
		writeClosedVariantFile(t, root, "account/audit/nodes.yaml", `audit:
  ticket.done: {}
  root.audit: {}
  ticket.cross:
    data_accumulation:
      writes: [{target_field: cross_count, value: state.cross_count + 1}]
`)
	default:
		t.Fatalf("unsupported cross-instance fixture shape %d", shape)
	}
	return root
}

func UseCrossInstanceAgentReceiver(t testing.TB, root string, shape CrossInstanceConnectionShape) {
	t.Helper()
	flow, event := "account", "account.cross.accepted"
	if shape == CrossInstanceNestedKeyless {
		flow, event = "account/audit", "ticket.cross"
	}
	applyClosedReplacement(t, filepath.Join(root, flow, "nodes.yaml"), "  "+event+":\n    data_accumulation:\n      writes: [{target_field: cross_count, value: state.cross_count + 1}]\n", "")
	writeClosedVariantFile(t, root, filepath.Join(flow, "agents.yaml"), "receiver:\n  model: regular\n  intent:\n    inline: Consume the selected cross-instance event.\n  subscriptions: ["+event+"]\n")
}

func AddCrossInstanceKeylessIntermediate(t testing.TB, root string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "account/bridge"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "account/audit"), filepath.Join(root, "account/bridge/audit")); err != nil {
		t.Fatal(err)
	}
	writeClosedVariantFile(t, root, "account/bridge/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "account/bridge/schema.yaml", "stages: []\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "to: account/audit}", "to: account/bridge/audit}")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "to: account/audit,", "to: account/bridge/audit,")
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "to: audit}", "to: bridge/audit}")
}

func AddCrossInstancePreparedAccount(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "inputs: [opened, root.audit, cross.forwarded]", "inputs: [opened, root.audit, cross.forwarded, ticket.cross]")
	applyClosedReplacement(t, filepath.Join(root, "account/nodes.yaml"), "  cross.forwarded: {}\n", "  cross.forwarded: {}\n  ticket.cross: {}\n")
	path := filepath.Join(root, "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	writeClosedVariantFile(t, root, "schema.yaml", string(raw)+"  - {event: ticket.cross, from: account/ticket, to: account, resolution: select-or-create, key_from: payload.target_account_id}\n")
}

func UseCrossInstanceFanOut(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "  target_account_id: text\n", "  target_account_id: text\n  targets: [text]\n")
	applyClosedReplacement(t, filepath.Join(root, "account/ticket/nodes.yaml"), "    emit: {event: ticket.cross, fields: {target_account_id: payload.target_account_id}}", `    fan_out:
      items_from: payload.targets
      as: selected
      identity: selected
      emit:
        event: ticket.cross
        fields: {target_account_id: selected}`)
}

func BindCrossInstanceOuterKey(t testing.TB, root string) {
	t.Helper()
	applyClosedReplacement(t, filepath.Join(root, "envelope/schema.yaml"), "inputs:", "instance: envelope_id\ninputs:")
	writeClosedVariantFile(t, root, "envelope/state.yaml", "envelope_id: text\n")
}

func CopyBoundCrossInstanceConnection(t testing.TB) string {
	t.Helper()
	inner := CopyCrossInstanceConnection(t, CrossInstanceNestedKeyless)
	AddCrossInstanceKeylessIntermediate(t, inner)
	root := copyWrappedLexicalAddressing(t, inner, crossInstanceConnectionRow(CrossInstanceNestedKeyless), false)
	BindCrossInstanceOuterKey(t, root)
	writeClosedVariantFile(t, root, "schema.yaml", `inputs: [group.opened, group.cross.requested]
connect:
  - {event: group.opened, from: ., to: envelope, resolution: select-or-create}
  - {event: group.cross.requested, from: ., to: envelope, resolution: select}
`)
	groupEvents := `group.opened:
  envelope_id: text
  account_id: text
  ticket_id: text
group.cross.requested:
  envelope_id: text
  source_account_id: text
  ticket_id: text
  target_account_id: text
`
	writeClosedVariantFile(t, root, "events.yaml", groupEvents)
	applyClosedReplacement(t, filepath.Join(root, "envelope/schema.yaml"), "inputs: [opened, root.audit, cross.requested]", "inputs: [opened, cross.requested, group.opened, group.cross.requested]\noutputs: [opened, cross.requested]")
	applyClosedReplacement(t, filepath.Join(root, "envelope/schema.yaml"), "  - {event: root.audit, from: ., to: account, resolution: select}\n  - {event: root.audit, from: ., to: account/bridge/audit}\n", "")
	applyClosedReplacement(t, filepath.Join(root, "envelope/account/schema.yaml"), "inputs: [opened, root.audit, cross.forwarded]", "inputs: [opened, cross.forwarded]")
	applyClosedReplacement(t, filepath.Join(root, "envelope/account/nodes.yaml"), "  root.audit: {}\n", "")
	applyClosedReplacement(t, filepath.Join(root, "envelope/account/bridge/audit/schema.yaml"), "inputs: [ticket.done, root.audit, ticket.cross]", "inputs: [ticket.done, ticket.cross]")
	applyClosedReplacement(t, filepath.Join(root, "envelope/account/bridge/audit/nodes.yaml"), "  root.audit: {}\n", "")
	writeClosedVariantFile(t, root, "envelope/nodes.yaml", `forward:
  group.opened:
    emit: {event: opened, fields: {account_id: payload.account_id, ticket_id: payload.ticket_id}}
  group.cross.requested:
    emit: {event: cross.requested, fields: {source_account_id: payload.source_account_id, ticket_id: payload.ticket_id, target_account_id: payload.target_account_id}}
`)
	return root
}

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
	return copyWrappedLexicalAddressing(t, CopyLexicalAddressing(t, true), crossAccountRow, higherOwner)
}

func CopyWrappedCrossInstanceConnection(t testing.TB, shape CrossInstanceConnectionShape, higherOwner bool) string {
	t.Helper()
	return copyWrappedLexicalAddressing(t, CopyCrossInstanceConnection(t, shape), crossInstanceConnectionRow(shape), higherOwner)
}

func copyWrappedLexicalAddressing(t testing.TB, original, row string, higherOwner bool) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, original, filepath.Join(root, "envelope"))
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	schema := "stages: []\n"
	if higherOwner {
		path := filepath.Join(root, "envelope", "schema.yaml")
		raw, err := os.ReadFile(path)
		if err != nil || strings.Count(string(raw), row) != 1 {
			t.Fatalf("wrapped cross-account fixture lost its exact row: %v", err)
		}
		writeClosedVariantFile(t, root, "envelope/schema.yaml", strings.Replace(string(raw), row, "", 1))
		schema += "connect:\n" + strings.NewReplacer("from: account", "from: envelope/account", "to: account", "to: envelope/account").Replace(row)
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

const lexicalAccountReturnRow = "  - {event: ticket.done, from: ticket, to: .}\n"

func CopyLexicallyBoundAccountConnection(t testing.TB) string {
	t.Helper()
	root := CopyLexicalAddressing(t, false)
	writeClosedVariantFile(t, root, "account/ticket/events.yaml", "ticket.done:\n  account_id: text\n  target_account_id: text\n")
	writeClosedVariantFile(t, root, "account/ticket/nodes.yaml", "reporter:\n  opened:\n    emit: {event: ticket.done, fields: {account_id: payload.account_id, target_account_id: payload.account_id}}\n")
	writeClosedVariantFile(t, root, "account/nodes.yaml", "receiver:\n  root.audit: {}\n  ticket.done: {}\n")
	path := filepath.Join(root, "account", "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil || strings.Count(string(raw), "inputs: [opened, root.audit]") != 1 {
		t.Fatalf("lexical account input declaration: %s %v", raw, err)
	}
	body := strings.Replace(string(raw), "inputs: [opened, root.audit]", "inputs: [opened, root.audit, ticket.done]", 1)
	writeClosedVariantFile(t, root, "account/schema.yaml", body+lexicalAccountReturnRow)
	return root
}

func SetLexicalAccountKeySource(t testing.TB, root, source string) {
	t.Helper()
	path := filepath.Join(root, "account", "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil || strings.Count(string(raw), lexicalAccountReturnRow) != 1 {
		t.Fatalf("lexical account return row: %s %v", raw, err)
	}
	row := "  - {event: ticket.done, from: ticket, to: ., resolution: select, key_from: " + source + "}\n"
	writeClosedVariantFile(t, root, "account/schema.yaml", strings.Replace(string(raw), lexicalAccountReturnRow, row, 1))
}
