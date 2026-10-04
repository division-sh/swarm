package canonicalrouting

import "testing"

// CopyRootReplyBoundary fixes the topology for the two root-reply intersections.
// Only external lifecycle inputs and the committed result are public.
func CopyRootReplyBoundary(t testing.TB, rootRequester, explicitCorrelation bool) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "entities.yaml", "request_state: {}\n")
	requester, provider := ".", "provider"
	if !rootRequester {
		requester, provider = "requester", "."
	}
	correlation := ""
	if explicitCorrelation {
		correlation = "    correlation_key: request_id\n"
	}
	connect := "  - {event: request.started, from: ., to: requester}\n"
	if rootRequester {
		connect = ""
	}
	connect += "  - {event: provider.requested, from: " + requester + ", to: " + provider + "}\n"
	connect += "  - event: provider.replied\n    from: " + provider + "\n    to: " + requester + "\n    replies_to: provider.requested\n" + correlation
	if !rootRequester {
		connect += "  - {event: request.finished, from: requester, to: .}\n"
	}
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-reply-boundary\nstages:\n  waiting: {initial: true}\n  done: {terminal: true}\npins:\n  inputs: [request.started, request.stop]\n  outputs: [request.finished]\nconnect:\n"+connect)
	startSchema := "request.started:\n  account_id: text\n  request_id: text\nrequest.stop:\n"
	requestSchema := "provider.requested:\n  account_id: text\n  request_id: text\n"
	responseSchema := "provider.replied:\n  account_id: text\n  request_id: text\n  result: text\n"
	finishedSchema := "request.finished:\n  request_id: text\n  result: text\n"
	requesterNode := `requester-node:
  execution_type: system_node
  subscribes_to: [request.started, provider.replied]
  event_handlers:
    request.started:
      emit:
        event: provider.requested
        fields:
          account_id: ${payload.account_id}
          request_id: ${payload.request_id}
    provider.replied:
      emit:
        event: request.finished
        fields:
          request_id: ${payload.request_id}
          result: ${payload.result}
`
	providerNode := `provider-node:
  execution_type: system_node
  subscribes_to: [provider.requested]
  event_handlers:
    provider.requested:
      emit:
        event: provider.replied
        fields:
          account_id: ${payload.account_id}
          request_id: ${payload.request_id}
          result: {literal: approved}
`
	stopNode := `stop-node:
  execution_type: system_node
  subscribes_to: [request.stop]
  event_handlers:
    request.stop: {advances_to: done}
`
	if rootRequester {
		writeClosedVariantFile(t, root, "events.yaml", startSchema+requestSchema+finishedSchema)
		writeClosedVariantFile(t, root, "nodes.yaml", stopNode+requesterNode+`result-observer:
  execution_type: system_node
  subscribes_to: [request.finished]
  event_handlers:
    request.finished: {}
`)
		writeClosedVariantFile(t, root, "provider/schema.yaml", "name: provider\npins:\n  inputs: [provider.requested]\n  outputs: [provider.replied]\n")
		writeClosedVariantFile(t, root, "provider/events.yaml", responseSchema)
		writeClosedVariantFile(t, root, "provider/nodes.yaml", providerNode)
	} else {
		writeClosedVariantFile(t, root, "events.yaml", startSchema+responseSchema)
		writeClosedVariantFile(t, root, "nodes.yaml", stopNode+providerNode+`result-observer:
  execution_type: system_node
  subscribes_to: [request.finished]
  event_handlers:
    request.finished: {}
`)
		writeClosedVariantFile(t, root, "requester/schema.yaml", "name: requester\npins:\n  inputs: [request.started, provider.replied]\n  outputs: [provider.requested, request.finished]\n")
		writeClosedVariantFile(t, root, "requester/events.yaml", requestSchema+finishedSchema)
		writeClosedVariantFile(t, root, "requester/nodes.yaml", requesterNode)
	}
	return root
}
