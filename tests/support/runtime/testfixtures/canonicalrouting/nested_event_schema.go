package canonicalrouting

import "testing"

func CopyNestedEventSchema(t testing.TB) string {
	t.Helper()
	root := CopyLexicalAddressing(t, false)
	documents := map[string]string{
		"schema.yaml": `inputs: [opened, root.audit, cross.requested]
connect:
  - {event: opened, from: ., to: account, resolution: select-or-create}
  - {event: root.audit, from: ., to: account, resolution: select}
  - {event: root.audit, from: ., to: account/audit}
  - {event: cross.requested, from: ., to: account, rename: cross.forwarded, resolution: select, key_from: payload.source_account_id}
` + crossAccountRow,
		"events.yaml": "opened:\n  account_id: text\n  ticket_id: text\nroot.audit:\n  account_id: text\ncross.requested:\n  source_account_id: text\n  ticket_id: text\n  target_account_id: text\n",
		"account/schema.yaml": `instance: account_id
inputs: [opened, root.audit, cross.forwarded, ticket.cross]
outputs: [ticket.opened, ticket.cross.requested]
connect:
  - {event: ticket.opened, from: ., to: ticket, rename: opened, resolution: select-or-create}
  - {event: ticket.cross.requested, from: ., to: ticket, rename: cross.requested, resolution: select}
  - {event: ticket.done, from: ticket, to: audit}
`,
		"account/events.yaml": "ticket.opened:\n  account_id: text\n  ticket_id: text\nticket.cross.requested:\n  ticket_id: text\n  target_account_id: text\n",
		"account/state.yaml":  "account_id: text\ncross_count: 0\n",
		"account/nodes.yaml": `receiver:
  opened:
    emit: {event: ticket.opened, fields: {account_id: payload.account_id, ticket_id: payload.ticket_id}}
  root.audit: {}
  cross.forwarded:
    emit: {event: ticket.cross.requested, fields: {ticket_id: payload.ticket_id, target_account_id: payload.target_account_id}}
  ticket.cross:
    data_accumulation:
      writes:
        - {target_field: cross_count, value: state.cross_count + 1}
`,
		"account/ticket/schema.yaml": "instance: ticket_id\ninputs: [opened, cross.requested]\noutputs: [ticket.done, ticket.cross]\n",
		"account/ticket/events.yaml": "ticket.done:\nticket.cross:\n  target_account_id: text\n  note: text?\n",
		"account/ticket/nodes.yaml":  "reporter:\n  opened:\n    emit: {event: ticket.done}\ncross-reporter:\n  cross.requested:\n    emit: {event: ticket.cross, fields: {target_account_id: payload.target_account_id}}\n",
		"account/audit/events.yaml":  "audit.ready:\n  target_account_id: text\n  note: text?\n",
		"account/audit/nodes.yaml":   "audit:\n  ticket.done: {}\n  root.audit:\n    emit: {event: audit.ready, fields: {target_account_id: payload.account_id}}\n  audit.ready: {}\n",
	}
	for name, body := range documents {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
