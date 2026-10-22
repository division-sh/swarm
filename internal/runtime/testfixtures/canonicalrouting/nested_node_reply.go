package canonicalrouting

import (
	"strings"
	"testing"
)

// CopyNestedNodeReplyDepth3 authors R(.) -> P(provider) -> T(t) -> U(u).
// Q1, Q2, and Q3 are provider.requested, t.requested, and u.requested;
// each has exactly one paired return with explicit request_id correlation.
// Only request.started/request.stop and request.finished are public. The root
// stays waiting after every result; the proof owner sends request.stop last.
func CopyNestedNodeReplyDepth3(t testing.TB) string {
	t.Helper()
	root := CopyRootReplyBoundary(t, true, true)
	writeClosedVariantFile(t, root, "schema.yaml", `name: nested-node-reply-depth3
stages:
  waiting: {}
  done: {final: true}
pins:
  inputs: [request.started, request.stop]
  outputs: [request.finished]
connect:
  - {event: provider.requested, from: ., to: provider}
  - event: provider.replied
    from: provider
    to: .
    replies_to: provider.requested
    correlation_key: request_id
`+nestedNodeReplyChainConnect)
	writeNestedNodeReplyChain(t, root, false)
	return root
}

// CopyNestedNodeReplyTwoRequesters authors distinct static R1(r1) and R2(r2)
// sharing one P(provider), with the same P -> T(t) -> U(u) nested chain.
// Two independent external starts carry identical payloads, including request_id,
// to distinct requesters: return authority must distinguish their concrete scopes.
// The result's literal requester marker is observation, never routing authority.
// Distinct imported event names preserve one producer schema owner per input.
// Q1 correlates explicitly by request_id. Q2/Q3 use the default request event ID
// so overlapping subrequests from the shared provider retain distinct identity;
// their required request_id fields remain unchanged business data.
func CopyNestedNodeReplyTwoRequesters(t testing.TB) string {
	t.Helper()
	root := CopyRootReplyBoundary(t, true, true)
	writeClosedVariantFile(t, root, "schema.yaml", `name: nested-node-reply-two-requesters
stages:
  waiting: {}
  done: {final: true}
pins:
  inputs: [r1.started, r2.started, request.stop]
  outputs: [request.finished]
connect:
  - {event: r1.dispatched, from: ., to: r1, rename: request.dispatched}
  - {event: r2.dispatched, from: ., to: r2, rename: request.dispatched}
  - {event: provider.requested, from: r1, to: provider, rename: r1.requested}
  - event: provider.replied
    from: provider
    to: r1
    replies_to: provider.requested
    correlation_key: request_id
  - {event: provider.requested, from: r2, to: provider, rename: r2.requested}
  - event: provider.replied
    from: provider
    to: r2
    replies_to: provider.requested
    correlation_key: request_id
  - {event: request.finished, from: r1, to: ., rename: r1.finished}
  - {event: request.finished, from: r2, to: ., rename: r2.finished}
`+strings.ReplaceAll(nestedNodeReplyChainConnect, "    correlation_key: request_id\n", ""))
	writeClosedVariantFile(t, root, "events.yaml", `r1.started:
  account_id: text
  request_id: text
r2.started:
  account_id: text
  request_id: text
request.stop:
r1.dispatched:
  account_id: text
  request_id: text
r2.dispatched:
  account_id: text
  request_id: text
request.finished:
  request_id: text
  result: text
  requester: text
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `stop-node:
  execution_type: system_node
  subscribes_to: [request.stop]
  event_handlers:
    request.stop: {advances_to: done}
r1-start-node:
  execution_type: system_node
  subscribes_to: [r1.started]
  event_handlers:
    r1.started:
      emit:
        event: r1.dispatched
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
r2-start-node:
  execution_type: system_node
  subscribes_to: [r2.started]
  event_handlers:
    r2.started:
      emit:
        event: r2.dispatched
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
return-node:
  execution_type: system_node
  subscribes_to: [r1.finished, r2.finished]
  event_handlers:
    r1.finished:
      emit:
        event: request.finished
        fields:
          request_id: payload.request_id
          result: payload.result
          requester: payload.requester
    r2.finished:
      emit:
        event: request.finished
        fields:
          request_id: payload.request_id
          result: payload.result
          requester: payload.requester
result-observer:
  execution_type: system_node
  subscribes_to: [request.finished]
  event_handlers:
    request.finished: {}
`)
	for _, requester := range []string{"r1", "r2"} {
		writeClosedVariantFile(t, root, requester+"/schema.yaml", "name: "+requester+`
pins:
  inputs: [request.dispatched, provider.replied]
  outputs: [provider.requested, request.finished]
`)
		writeClosedVariantFile(t, root, requester+"/events.yaml", `provider.requested:
  account_id: text
  request_id: text
request.finished:
  request_id: text
  result: text
  requester: text
`)
		writeClosedVariantFile(t, root, requester+"/nodes.yaml", `requester-node:
  execution_type: system_node
  subscribes_to: [request.dispatched, provider.replied]
  event_handlers:
    request.dispatched:
      emit:
        event: provider.requested
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
    provider.replied:
      emit:
        event: request.finished
        fields:
          request_id: payload.request_id
          result: payload.result
          requester: "`+requester+`"
`)
	}
	writeNestedNodeReplyChain(t, root, true)
	return root
}

const nestedNodeReplyChainConnect = `  - {event: t.requested, from: provider, to: t}
  - event: t.replied
    from: t
    to: provider
    replies_to: t.requested
    correlation_key: request_id
  - {event: u.requested, from: t, to: u}
  - event: u.replied
    from: u
    to: t
    replies_to: u.requested
    correlation_key: request_id
`

// Keep the inherited entityless receivers entityless: no state, initialization,
// create_entity, or payload-carried reply context is needed to author this chain.
func writeNestedNodeReplyChain(t testing.TB, root string, twoRequesters bool) {
	t.Helper()
	requests := []string{"provider.requested"}
	if twoRequesters {
		requests = []string{"r1.requested", "r2.requested"}
	}
	writeClosedVariantFile(t, root, "provider/schema.yaml", `name: provider
pins:
  inputs: [`+strings.Join(requests, ", ")+`, t.replied]
  outputs: [t.requested, provider.replied]
`)
	writeClosedVariantFile(t, root, "provider/events.yaml", `provider.replied:
  account_id: text
  request_id: text
  result: text
t.requested:
  account_id: text
  request_id: text
`)
	nodes := `provider-node:
  execution_type: system_node
  subscribes_to: [` + strings.Join(requests, ", ") + `, t.replied]
  event_handlers:
`
	for _, request := range requests {
		nodes += "    " + request + `:
      emit:
        event: t.requested
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
`
	}
	nodes += `    t.replied:
      emit:
        event: provider.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: payload.result
`
	writeClosedVariantFile(t, root, "provider/nodes.yaml", nodes)
	writeClosedVariantFile(t, root, "t/schema.yaml", `name: t
pins:
  inputs: [t.requested, u.replied]
  outputs: [u.requested, t.replied]
`)
	writeClosedVariantFile(t, root, "t/events.yaml", `t.replied:
  account_id: text
  request_id: text
  result: text
u.requested:
  account_id: text
  request_id: text
`)
	writeClosedVariantFile(t, root, "t/nodes.yaml", `t-node:
  execution_type: system_node
  subscribes_to: [t.requested, u.replied]
  event_handlers:
    t.requested:
      emit:
        event: u.requested
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
    u.replied:
      emit:
        event: t.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: payload.result
`)
	writeClosedVariantFile(t, root, "u/schema.yaml", `name: u
pins:
  inputs: [u.requested]
  outputs: [u.replied]
`)
	writeClosedVariantFile(t, root, "u/events.yaml", `u.replied:
  account_id: text
  request_id: text
  result: text
`)
	writeClosedVariantFile(t, root, "u/nodes.yaml", `u-node:
  execution_type: system_node
  subscribes_to: [u.requested]
  event_handlers:
    u.requested:
      emit:
        event: u.replied
        fields:
          account_id: payload.account_id
          request_id: payload.request_id
          result: "approved"
`)
}
