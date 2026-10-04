package canonicalrouting

import (
	"fmt"
	"strings"
	"testing"
)

type ArrivalJoinRoutingFixture uint8

const (
	ArrivalJoinBoundReply ArrivalJoinRoutingFixture = iota + 1
	ArrivalJoinBoundReplyObserver
	ArrivalJoinFieldlessReply
	ArrivalJoinPayloadDirected
	ArrivalJoinPayloadDirectedMultipleRecipients
	ArrivalJoinMultiUntil
	ArrivalJoinPayloadDirectedBeforeArm
)

// ArrivalJoinRoutingFiles supplies closed, ordinary-connect source fixtures.
// Lifecycle tests own execution and fault injection, not routing declarations.
func ArrivalJoinRoutingFiles(t testing.TB, variant ArrivalJoinRoutingFixture) map[string]string {
	t.Helper()
	switch variant {
	case ArrivalJoinBoundReply, ArrivalJoinBoundReplyObserver:
		files := arrivalJoinBoundReplyFiles()
		if variant == ArrivalJoinBoundReplyObserver {
			files["schema.yaml"] += "  - {event: provider.replied, from: provider, to: observer, resolution: select}\n"
			files["schema.yaml"] += "  - {event: provider.notified, from: provider, to: observer, resolution: select}\n"
			files["provider/schema.yaml"] = strings.Replace(files["provider/schema.yaml"], "    - provider.replied\n", "    - provider.replied\n    - provider.notified\n", 1)
			files["provider/events.yaml"] += "ordinary.requested:\n  order_id: text\n  member_id: text\n  result: JoinResult\nprovider.notified:\n  order_id: text\n  member_id: text\n  result: JoinResult\n"
			files["provider/nodes.yaml"] += `    ordinary.requested:
      emit:
        event: provider.notified
        fields:
          order_id: "${payload.order_id}"
          member_id: "${payload.member_id}"
          result: "${payload.result}"
`
			files["observer/schema.yaml"] = strings.Replace(files["requester/schema.yaml"], "name: requester", "name: observer", 1)
			files["observer/schema.yaml"] = strings.Replace(files["observer/schema.yaml"], "  outputs:\n    - provider.requested\n", "", 1)
			files["observer/schema.yaml"] += "    - provider.notified\n"
			files["observer/entities.yaml"] = files["requester/entities.yaml"] + "  ordinary_result: JoinResult\n"
			start := strings.Index(files["requester/nodes.yaml"], "collector:\n")
			end := strings.Index(files["requester/nodes.yaml"], "dispatcher:\n")
			files["observer/nodes.yaml"] = files["requester/nodes.yaml"][start:end] + `    provider.notified:
      data_accumulation:
        writes:
          - {target_field: ordinary_result, value: "${payload.result}"}
`
		}
		return files
	case ArrivalJoinFieldlessReply:
		return arrivalJoinFieldlessReplyFiles()
	case ArrivalJoinPayloadDirected, ArrivalJoinPayloadDirectedMultipleRecipients, ArrivalJoinPayloadDirectedBeforeArm:
		files := arrivalJoinPayloadDirectedFiles()
		if variant == ArrivalJoinPayloadDirectedBeforeArm {
			files["orders/schema.yaml"] = strings.NewReplacer(
				"awaiting: {initial: true}", "awaiting: {}",
				"dispatching: {}", "dispatching: {initial: true}",
			).Replace(files["orders/schema.yaml"])
		}
		if variant == ArrivalJoinPayloadDirectedMultipleRecipients {
			files["schema.yaml"] += "  - {event: item.completed, from: ., to: mirror, resolution: select}\n"
			for _, name := range []string{"schema.yaml", "entities.yaml", "events.yaml", "nodes.yaml"} {
				files["mirror/"+name] = strings.Replace(files["orders/"+name], "name: orders", "name: mirror", 1)
			}
		}
		return files
	case ArrivalJoinMultiUntil:
		return arrivalJoinMultiUntilFiles()
	default:
		t.Fatalf("unsupported arrival-join routing fixture %d", variant)
		return nil
	}
}

func arrivalJoinPayloadDirectedFiles() map[string]string {
	return map[string]string{
		"schema.yaml":   "name: a2-publication-binding\nstages:\n  active: {initial: true}\npins:\n  inputs:\n    - work.requested\n  outputs:\n    - item.completed\nconnect:\n  - {event: item.completed, from: ., to: orders, resolution: select}\n",
		"entities.yaml": "root_state:\n  work_count: {type: integer, initial: 0}\n",
		"events.yaml":   "work.requested:\n  prefix: text\n  suffix: text\nitem.completed:\n  order_id: text\n  member_id: text\n  result: JoinResult\n",
		"types.yaml":    "types:\n  JoinResult:\n    value: text\n",
		"nodes.yaml": `worker:
  execution_type: system_node
  event_handlers:
    work.requested:
      data_accumulation:
        writes:
          - {target_field: work_count, value: "${entity.work_count + 1}"}
      emit:
        event: item.completed
        fields:
          order_id: "${payload.prefix + payload.suffix}"
          member_id: a
          result: {value: computed-by-worker}
`,
		"orders/schema.yaml": `name: orders
instance: order_id
stages:
  awaiting: {initial: true}
  dispatching: {}
  ready: {terminal: true}
  attention: {terminal: true}
pins:
  inputs:
    - item.completed
`,
		"orders/entities.yaml": "order_state:\n  order_id: {type: text, indexed: true}\n  expected: \"[text]\"\n",
		"orders/events.yaml":   "manual.abort:\ndispatch.completed:\n",
		"orders/nodes.yaml": `collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete: {advances_to: ready}
        on_deadline: {advances_to: attention}
dispatcher:
  execution_type: system_node
  event_handlers:
    manual.abort: {advances_to: dispatching}
    dispatch.completed: {advances_to: awaiting}
`,
	}
}

func arrivalJoinBoundReplyFiles() map[string]string {
	return map[string]string{
		"schema.yaml": "name: a2-bound-reply\nconnect:\n  - {event: provider.requested, from: requester, to: provider}\n  - {event: provider.replied, from: provider, to: requester, replies_to: provider.requested}\n",
		"types.yaml":  "types:\n  JoinResult:\n    value: text\n",
		"requester/schema.yaml": `name: requester
instance: order_id
stages:
  awaiting: {initial: true}
  dispatching: {}
  ready: {terminal: true}
  attention: {terminal: true}
pins:
  inputs:
    - provider.replied
  outputs:
    - provider.requested
`,
		"requester/entities.yaml": "request_state:\n  order_id: {type: text, indexed: true}\n  expected: \"[text]\"\n",
		"requester/events.yaml":   "request.send:\nmanual.abort:\ndispatch.completed:\nprovider.requested:\n  order_id: text\n",
		"requester/nodes.yaml": `requester:
  execution_type: system_node
  event_handlers:
    request.send:
      emit:
        event: provider.requested
        fields: {order_id: "${entity.order_id}"}
collector:
  execution_type: system_node
  event_handlers:
    provider.replied:
      join:
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete: {advances_to: ready}
        on_deadline: {advances_to: attention}
dispatcher:
  execution_type: system_node
  event_handlers:
    manual.abort: {advances_to: dispatching}
    dispatch.completed: {advances_to: awaiting}
`,
		"provider/schema.yaml": "name: provider\npins:\n  inputs:\n    - provider.requested\n  outputs:\n    - provider.replied\n",
		"provider/events.yaml": "provider.replied:\n  order_id: text\n  member_id: text\n  result: JoinResult\n",
		"provider/nodes.yaml": `provider:
  execution_type: system_node
  event_handlers:
    provider.requested:
      emit:
        event: provider.replied
        fields:
          order_id: "${payload.order_id}"
          member_id: a
          result: {value: provider-result}
`,
	}
}

func arrivalJoinFieldlessReplyFiles() map[string]string {
	return map[string]string{
		"schema.yaml": "name: a2-fieldless-reply\nconnect:\n  - {event: provider.requested, from: requester, to: provider}\n  - {event: provider.replied, from: provider, to: requester, replies_to: provider.requested}\n",
		"requester/schema.yaml": `name: requester
pins:
  inputs:
    - request.send
    - provider.replied
  outputs:
    - provider.requested
`,
		"requester/events.yaml": "request.send:\n  token: text\nprovider.requested:\n  token: text\nreply.observed:\n  token: text\n  value: text\n",
		"requester/nodes.yaml": `sender:
  execution_type: system_node
  event_handlers:
    request.send:
      emit:
        event: provider.requested
        fields: {token: "${payload.token}"}
receiver:
  execution_type: system_node
  event_handlers:
    provider.replied:
      emit:
        event: reply.observed
        fields: {token: "${payload.token}", value: "${payload.value}"}
`,
		"provider/schema.yaml": "name: provider\npins:\n  inputs:\n    - provider.requested\n  outputs:\n    - provider.replied\n",
		"provider/events.yaml": "provider.replied:\n  token: text\n  value: text\n",
		"provider/nodes.yaml": `provider:
  execution_type: system_node
  event_handlers:
    provider.requested:
      emit:
        event: provider.replied
        fields: {token: "${payload.token}", value: provider-result}
`,
	}
}

func arrivalJoinMultiUntilFiles() map[string]string {
	files := arrivalJoinPayloadDirectedFiles()
	files["schema.yaml"] = `name: a2-multi-until-binding
stages:
  active: {initial: true}
pins:
  inputs:
    - stop.requested
  outputs:
    - halt.requested
connect:
  - {event: halt.requested, from: ., to: orders, resolution: select}
  - {event: halt.requested, from: ., to: mirror, resolution: select}
`
	files["events.yaml"] = "stop.requested:\n  order_id: text\nhalt.requested:\n  order_id: text\n"
	files["nodes.yaml"] = `worker:
  execution_type: system_node
  event_handlers:
    stop.requested:
      emit:
        event: halt.requested
        fields: {order_id: "${payload.order_id}"}
`
	for _, flow := range []string{"orders", "mirror"} {
		files[flow+"/schema.yaml"] = fmt.Sprintf(`name: %s
instance: order_id
stages:
  awaiting: {initial: true}
  dispatching: {}
  ready: {terminal: true}
  attention: {terminal: true}
pins:
  inputs:
    - halt.requested
`, flow)
		files[flow+"/entities.yaml"] = "order_state:\n  order_id: {type: text, indexed: true}\n  expected: \"[text]\"\n  halt_count: {type: integer, initial: 0}\n"
		files[flow+"/events.yaml"] = "manual.abort:\ndispatch.completed:\nitem.completed:\n  member_id: text\n  result: JoinResult\nalternate.completed:\n  member_id: text\n  result: JoinResult\nhalt.observed:\n  order_id: text\n  count: integer\n"
		for _, name := range []string{"first.closed", "second.closed"} {
			files[flow+"/events.yaml"] += name + ":\n  expected: integer\n  completed: integer\n  missing: \"[text]\"\n  results: \"[JoinResult]\"\n  timed_out: boolean\n  close_reason: text\n"
		}
		files[flow+"/nodes.yaml"] = `collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        id: primary
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        until: halt.requested
        on_deadline: {advances_to: attention}
        on_complete:
          emit:
            event: first.closed
            fields:
              expected: "${join.expected}"
              completed: "${join.completed}"
              missing: "${join.missing}"
              results: "${join.results}"
              timed_out: "${join.timed_out}"
              close_reason: "${join.close_reason}"
    alternate.completed:
      join:
        id: alternate
        stage: awaiting
        members: {count: 3, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        until: halt.requested
        on_deadline: {advances_to: attention}
        on_complete:
          emit:
            event: second.closed
            fields:
              expected: "${join.expected}"
              completed: "${join.completed}"
              missing: "${join.missing}"
              results: "${join.results}"
              timed_out: "${join.timed_out}"
              close_reason: "${join.close_reason}"
    halt.requested:
      data_accumulation:
        writes:
          - {target_field: halt_count, value: "${entity.halt_count + 1}"}
      emit:
        event: halt.observed
        fields: {order_id: "${entity.order_id}", count: "${entity.halt_count}"}
dispatcher:
  execution_type: system_node
  event_handlers:
    manual.abort: {advances_to: dispatching}
    dispatch.completed: {advances_to: awaiting}
`
	}
	return files
}
