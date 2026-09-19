package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyNotifyAllChildrenNestedServing keeps the production example's root
// ingress and direct barrier, then makes each account a genuine child producer.
// Task identities repeat across account siblings but own distinct nested state.
func CopyNotifyAllChildrenNestedServing(t testing.TB) string {
	t.Helper()
	root := CopyNotifyAllChildren(t, NotifyAllChildrenOptions{FanOutDeliveryBarrier: true})
	applyClosedReplacement(t, filepath.Join(root, "portfolio", "nodes.yaml"),
		"            command: payload.command\n", "            command: payload.command\n            task_ids: {literal: [prepare, publish]}\n")
	applyClosedReplacement(t, filepath.Join(root, "portfolio", "events.yaml"),
		"  command: text\nportfolio.notify.completed:", "  command: text\n  task_ids: '[text]'\nportfolio.notify.completed:")
	// This closed variant uses business system-node recipients, not the example's
	// LLM agent that would terminate an account before its nested tasks settle.
	removeClosedVariantFiles(t, root, "account/agents.yaml")
	writeClosedVariantFile(t, root, "account/entities.yaml", "account_state:\n  account_id: text\n")
	writeClosedVariantFile(t, root, "account/schema.yaml", `name: account
mode: template
initial_state: active
states: [active]
instance: account_id
pins:
  inputs:
    events:
      - event: account.registered
        resolution:
          mode: select-or-create
      - event: account.notify.requested
        resolution:
          mode: select
  outputs:
    events:
      - account.task.requested
      - account.tasks.completed
connect:
  - event: account.task.requested
    from: .
    to: task
`)
	writeClosedVariantFile(t, root, "account/events.yaml", `account.task.requested:
  key: task_key
  account_id: text
  task: text
  task_key: text
account.tasks.completed:
  swarm:
    consumer: external
  account_id: text
  total: integer
  succeeded: integer
  dead_lettered: integer
  no_route: integer
  semantic_rejected: integer
  canceled: integer
`)
	writeClosedVariantFile(t, root, "account/nodes.yaml", `account-node:
  execution_type: system_node
  subscribes_to:
    - account.registered
    - account.notify.requested
  event_handlers:
    account.registered:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
    account.notify.requested:
      fan_out:
        items_from: payload.task_ids
        as: task
        identity: task
        max_items: 2
        emit:
          event: account.task.requested
          fields:
            account_id: payload.account_id
            task: task
            task_key: 'payload.account_id + ":" + task'
      join:
        id: direct-account-tasks-delivered
        members:
          from_fan_out: true
        on_complete:
          emit:
            event: account.tasks.completed
            fields:
              account_id: entity.account_id
              total: join.total
              succeeded: join.dispositions.succeeded
              dead_lettered: join.dispositions.dead_lettered
              no_route: join.dispositions.no_route
              semantic_rejected: join.dispositions.semantic_rejected
              canceled: join.dispositions.canceled
`)
	writeClosedVariantFile(t, root, "account/task/schema.yaml", `name: account-task
mode: template
instance: task_key
initial_state: pending
states: [pending, completed]
terminal_states: [completed]
pins:
  inputs:
    events:
      - event: account.task.requested
        resolution:
          mode: select-or-create
  outputs:
    events:
      - account.task.completed
`)
	writeClosedVariantFile(t, root, "account/task/entities.yaml", `task_state:
  account_id: text
  task: text
  task_key: text
`)
	writeClosedVariantFile(t, root, "account/task/events.yaml", `account.task.completed:
  swarm:
    consumer: external
  account_id: text
  task: text
`)
	writeClosedVariantFile(t, root, "account/task/nodes.yaml", `task-worker:
  execution_type: system_node
  subscribes_to:
    - account.task.requested
  event_handlers:
    account.task.requested:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: task
            target_field: task
          - source_field: task_key
            target_field: task_key
      advances_to: completed
      emit:
        event: account.task.completed
        fields:
          account_id: payload.account_id
          task: payload.task
`)
	return root
}
