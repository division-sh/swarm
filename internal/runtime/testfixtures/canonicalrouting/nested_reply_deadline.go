package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// Each requester's return is owned by one stage-entry join. The proof controls
// registration/firing through the existing scheduler; the authored delay is 1h.
func CopyNestedNodeReplyDeadlines(t testing.TB) string {
	t.Helper()
	root := CopyNestedNodeReplyDepth3(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "  waiting: {}\n", "  waiting: {}\n  ready: {}\n  attention: {}\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "inputs: [request.started, request.stop]", "inputs: [request.started, request.stop, provider.replied]")
	for _, flow := range []string{"", "provider", "t"} {
		if flow != "" {
			applyClosedReplacement(t, filepath.Join(root, flow, "schema.yaml"), "pins:\n", "stages:\n  waiting: {}\n  ready: {}\n  attention: {}\npins:\n")
		}
		writeClosedVariantFile(t, root, filepath.Join(flow, "entities.yaml"), `request_state:
  expected:
    type: "[text]"
    initial: [same-business-key]
`)
	}
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `    provider.replied:
      emit:
        event: request.finished
        fields:
          request_id: payload.request_id
          result: payload.result
`, nestedDeadlineReturn("provider.replied", "request.finished", false))
	applyClosedReplacement(t, filepath.Join(root, "provider", "nodes.yaml"), `    t.replied:
      emit:
        event: provider.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: payload.result
`, nestedDeadlineReturn("t.replied", "provider.replied", true))
	applyClosedReplacement(t, filepath.Join(root, "t", "nodes.yaml"), `    u.replied:
      emit:
        event: t.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: payload.result
`, nestedDeadlineReturn("u.replied", "t.replied", true))
	return root
}

// An unclaimed inner request uses the existing deferred fan-out continuation,
// rather than requiring unsupported replay of a plain node delivery.
func CopyNestedFanOutReplyDeadlines(t testing.TB) string {
	t.Helper()
	root := CopyNestedNodeReplyDeadlines(t)
	writeClosedVariantFile(t, root, "u/entities.yaml", `work:
  items:
    type: "[text]"
    initial: [same-business-key]
`)
	writeClosedVariantFile(t, root, "u/events.yaml", `u.ready:
  account_id: text
  request_id: text
u.replied:
  account_id: text
  request_id: text
  result: text
`)
	writeClosedVariantFile(t, root, "u/nodes.yaml", `u-node:
  execution_type: system_node
  subscribes_to: [u.requested, u.ready]
  event_handlers:
    u.requested:
      fan_out:
        items_from: entity.items
        as: entry
        identity: entry
        emit:
          event: u.ready
          fields:
            account_id: payload.account_id
            request_id: entry
    u.ready:
      emit:
        event: u.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: "approved"
`)
	return root
}

// Two open reply levels retain the same stage-entry deadlines as the depth-three
// fixture. The inner request is executable through the existing fan-out owner.
func CopyTwoOpenFanOutReplyDeadlines(t testing.TB) string {
	t.Helper()
	root := CopyNestedFanOutReply(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "  waiting: {}\n", "  waiting: {}\n  ready: {}\n  attention: {}\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "inputs: [request.started, request.stop]", "inputs: [request.started, request.stop, provider.replied]")
	writeClosedVariantFile(t, root, "entities.yaml", `request_state:
  expected:
    type: "[text]"
    initial: [same-business-key]
`)
	applyClosedReplacement(t, filepath.Join(root, "provider/schema.yaml"), "pins:\n", "stages:\n  waiting: {}\n  ready: {}\n  attention: {}\npins:\n")
	writeClosedVariantFile(t, root, "provider/entities.yaml", `request_state:
  expected:
    type: "[text]"
    initial: [same-business-key]
`)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), `    provider.replied:
      emit:
        event: request.finished
        fields:
          request_id: payload.request_id
          result: payload.result
`, nestedDeadlineReturn("provider.replied", "request.finished", false))
	applyClosedReplacement(t, filepath.Join(root, "provider/nodes.yaml"), `    t.replied:
      emit:
        event: provider.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: payload.result
`, nestedDeadlineReturn("t.replied", "provider.replied", true))
	return root
}

func nestedDeadlineReturn(input, output string, account bool) string {
	fields := "              request_id: \"same-business-key\"\n              result: \"approved\"\n"
	if account {
		fields = "              account_id: \"same-account\"\n" + fields
	}
	return "    " + input + `:
      join:
        stage: waiting
        members: {from: state.expected, by: payload.request_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete:
          advances_to: ready
          emit:
            event: ` + output + `
            fields:
` + fields + `        on_deadline: {advances_to: attention}
`
}
