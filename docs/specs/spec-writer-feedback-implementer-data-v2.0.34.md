# Spec Writer Handoff (Implementer Data)

Date: 2026-02-26  
Repo: `empireai`  
Scope: Data requested in "Ask the implementer these 5 things"

## 1) OpCo worker subscriptions / bootstrap routing (real values)

### 1.1 Raw command requested (`opco-*.yaml`)

```bash
for f in configs/agents/templates/opco-*.yaml; do
  echo "=== $(basename $f) ==="
  grep -A 20 'subscriptions\|subscriptions_bootstrap' "$f" | head -25
done
```

Observed result: only one file matches the pattern.

```text
=== opco-ceo.yaml ===
subscriptions:
  - opco.spinup_requested
  - product_report
  - growth_report
  - cross_domain_report
  - product_escalation
  - growth_escalation
  - spend_request
  - spend.approved
  - spend.rejected
  - founder_input.response
  - opco.escalation_response
  - cto.architecture_directive
...
```

### 1.2 Actual template file inventory (`configs/agents/templates/`)

```text
backend-agent.yaml
chief-of-staff.yaml
cto-agent.yaml
devops-agent.yaml
frontend-agent.yaml
marketing-agent.yaml
opco-ceo.yaml
pm-agent.yaml
qa-agent.yaml
routes.yaml
support-agent.yaml
tech-writer.yaml
vp-growth.yaml
vp-product.yaml
```

### 1.3 Bootstrap/seeded routing source

`configs/agents/templates/routes.yaml` contains bootstrap + seeded routes (not `subscriptions_bootstrap` fields inside most worker YAML files). Notable bootstrap patterns:

- `product_spec_ready -> cto-agent`
- `cto.tech_spec_review_requested -> tech-writer`
- `technical_spec_ready -> cto-agent/backend-agent/frontend-agent`
- `build_progress|build_blocked -> cto-agent`
- `deploy_requested -> devops-agent`
- `qa.validation_passed|qa.validation_failed -> cto-agent`
- `bug_reported -> cto-agent`
- `feature_request -> pm-agent`

Seeded examples include:

- `feature_deployed -> chief-of-staff` and `marketing-agent`
- `build_complete|prelaunch_ready|support_critical|channel_blocked|churn_risk -> chief-of-staff`

## 2) Tool inventory for `schedule` + `human_task_request` (real YAML)

### 2.1 Files containing `schedule`

- `configs/agents/templates/backend-agent.yaml`
- `configs/agents/templates/chief-of-staff.yaml`
- `configs/agents/templates/cto-agent.yaml`
- `configs/agents/templates/devops-agent.yaml`
- `configs/agents/templates/frontend-agent.yaml`
- `configs/agents/templates/opco-ceo.yaml`
- `configs/agents/templates/pm-agent.yaml`
- `configs/agents/templates/tech-writer.yaml`
- `configs/agents/templates/vp-growth.yaml`
- `configs/agents/templates/vp-product.yaml`
- `configs/agents/empire-coordinator.yaml`

### 2.2 Files containing `human_task_request`

- `configs/agents/templates/marketing-agent.yaml`
- `configs/agents/templates/opco-ceo.yaml`
- `configs/agents/templates/support-agent.yaml`

## 3) OpCo Support event naming (`customer_message` vs `inbound.*`)

### 3.1 What configs/runtime currently use

- `configs/agents/templates/support-agent.yaml` subscribes to `customer_message`.
- Runtime inbound gateway emits:
  - `inbound.<verticalSlug>.whatsapp_message`
  - `inbound.<verticalSlug>.email`
  (see `internal/runtime/inbound.go`)

### 3.2 Contract currently present

`contracts/event-catalog.yaml` includes:

- `inbound.whatsapp_message`
- `inbound.email`

with `_note` saying vertical_id is in payload for routing.

### 3.3 Practical conclusion

- `customer_message` appears in configs/commgraph, but no runtime emitter was found for that event type.
- Inbound events and support subscription naming are currently misaligned and need normalization (or explicit translation layer).

## 4) The 6 missing events (existence + emitter/consumer/payload in code)

### 4.1 Catalog presence check (`contracts/event-catalog.yaml`)

All 6 are currently missing from the catalog file:

```text
opco.routing_updated: MISSING
customer_message: MISSING
human_task.assigned: MISSING
review.product_spec_feedback: MISSING
review.deploy_feedback: MISSING
runtime.reset: MISSING
```

### 4.2 What code shows for each event

1. `opco.routing_updated`
- Emitted: yes, in `internal/runtime/tool_executor.go` (on `configure_routing` tool execution).
- Source: actor agent (`SourceAgent: actor.ID`).
- Payload fields:
  - `vertical_id`
  - `event_pattern`
  - `subscriber_id`
  - `installed_by`
  - `reason`
  - `status`
  - `source`
  - `bootstrap_version`
  - `runtime_tool_event`
- Consumer: no explicit static subscriber found in current agent YAML.

2. `customer_message`
- Emitted: no runtime producer found.
- Consumer: support agent subscribes to `customer_message` in `configs/agents/templates/support-agent.yaml`.
- Also referenced in default OpCo subscriptions in `internal/runtime/manager.go`.

3. `human_task.assigned`
- Emitted: yes, in `internal/dashboard/server.go` (`handleTaskClaim`).
- Source: `dashboard`.
- Delivery: targeted event to `requesting_agent`.
- Payload fields:
  - `task_id`
  - `requesting_agent`
  - `vertical_id`
  - `assigned_to`

4. `review.product_spec_feedback`
- Emitted: not found in runtime/dashboard emission paths.
- Present only as commgraph/human roundtrip declaration in `internal/commgraph/registry.go`.

5. `review.deploy_feedback`
- Emitted: not found in runtime/dashboard emission paths.
- Present only as commgraph/human roundtrip declaration in `internal/commgraph/registry.go`.

6. `runtime.reset`
- Emitted: yes, in `internal/dashboard/server.go` (`publishRuntimeReset`).
- Source: `runtime`.
- Payload fields:
  - `source`
  - `timestamp`
- Consumed:
  - `pipeline-coordinator` (`internal/runtime/pipeline_coordinator.go`)
  - `scan-campaign-manager` (`internal/runtime/scan_campaign_manager.go`)

## 5) `conversation_mode` + `max_turns_per_task` (OpCo template YAMLs)

From `configs/agents/templates/*.yaml`:

| File | max_turns_per_task | conversation_mode |
|---|---:|---|
| `opco-ceo.yaml` | 30 | session |
| `chief-of-staff.yaml` | 15 | session |
| `vp-product.yaml` | 20 | session |
| `vp-growth.yaml` | 20 | session |
| `cto-agent.yaml` | 30 | session |
| `pm-agent.yaml` | 30 | session |
| `tech-writer.yaml` | 30 | session |
| `backend-agent.yaml` | 40 | session |
| `frontend-agent.yaml` | 40 | session |
| `qa-agent.yaml` | 20 | session |
| `devops-agent.yaml` | 20 | session |
| `marketing-agent.yaml` | 25 | session |
| `support-agent.yaml` | 20 | session |

## Contract-facing notes for your patch pass

1. The `opco-*.yaml` filename assumption only captures `opco-ceo.yaml`; other OpCo roles are named role-first (`backend-agent.yaml`, `pm-agent.yaml`, etc.).  
2. `customer_message` vs `inbound.*` is currently unresolved in code/config alignment and should be explicitly normalized in contracts.  
3. The six events listed above should be added into `event-catalog.yaml` with explicit `delivery_channel`, emitter, consumer, and payload definitions.

## Appendix: Raw command outputs

### A) `opco-*.yaml` subscription dump

```text
=== opco-ceo.yaml ===
subscriptions:
  - opco.spinup_requested
  - product_report
  - growth_report
  - cross_domain_report
  - product_escalation
  - growth_escalation
  - spend_request
  - spend.approved
  - spend.rejected
  - founder_input.response
  - opco.escalation_response
  - cto.architecture_directive
tools:
  - agent_hire
  - agent_fire
  - agent_reconfigure
  - configure_routing
  - agent_message
  - schedule
  - mailbox_send
```

### B) `schedule` / `human_task_request` exact hits

```text
=== configs/agents/templates/backend-agent.yaml ===
9:  - schedule
=== configs/agents/templates/chief-of-staff.yaml ===
17:  - schedule
=== configs/agents/templates/cto-agent.yaml ===
20:  - schedule
=== configs/agents/templates/devops-agent.yaml ===
11:  - schedule
=== configs/agents/templates/frontend-agent.yaml ===
9:  - schedule
=== configs/agents/templates/marketing-agent.yaml ===
17:  - human_task_request
=== configs/agents/templates/opco-ceo.yaml ===
24:  - schedule
26:  - human_task_request
=== configs/agents/templates/pm-agent.yaml ===
11:  - schedule
=== configs/agents/templates/support-agent.yaml ===
11:  - human_task_request
=== configs/agents/templates/tech-writer.yaml ===
9:  - schedule
=== configs/agents/templates/vp-growth.yaml ===
18:  - schedule
=== configs/agents/templates/vp-product.yaml ===
20:  - schedule
=== configs/agents/empire-coordinator.yaml ===
36:  - schedule
```

### C) Support/inbound name check

```text
configs/agents/templates/support-agent.yaml:6:  - customer_message
internal/runtime/inbound.go:122: Type: events.EventType("inbound." + target.VerticalSlug + "." + provider + "_" + eventType)
contracts/event-catalog.yaml:651 inbound.email:
contracts/event-catalog.yaml:664 inbound.whatsapp_message:
```

### D) Missing event catalog check

```text
opco.routing_updated: MISSING
customer_message: MISSING
human_task.assigned: MISSING
review.product_spec_feedback: MISSING
review.deploy_feedback: MISSING
runtime.reset: MISSING
```
