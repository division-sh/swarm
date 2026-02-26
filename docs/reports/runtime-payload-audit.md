# Runtime Payload Completeness Audit

- generated_at: 2026-02-25T05:03:55Z
- runtime_dir: `internal/runtime`
- config_dirs: `configs/agents`, `configs/agents/templates`
- scope: runtime-emitted events (Go-side publish paths) vs subscribed agent prompt field expectations

## Runtime Event Contracts

| Event | Guaranteed Fields | Any Dynamic Path | Sites |
|---|---|---:|---|
| `agent.started` | `agent_id, agent_type, hired_by, mode, role, vertical_id` | no | `internal/runtime/manager.go:184` |
| `agent.tool_execution` | `agent_id, agent_role, duration_ms, error, input, ok, result, runtime_tool, tool_name, vertical_id` | no | `internal/runtime/tool_executor.go:185` |
| `brand.requested` | `business_brief, geography, name, scoring, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1339`, `internal/runtime/pipeline_coordinator.go:1608` |
| `brand.revision_needed` | `brand, feedback, geography, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1553` |
| `campaign.completed` | `campaign_id, completed_mode, directive_id, discoveries_count, geography_id, priority, source_event_id, strategic_context` | no | `internal/runtime/scan_campaign_manager.go:208` |
| `cto.spec_review_requested` | `geography, research, scoring, spec, spec_validation, spec_version, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1414`, `internal/runtime/pipeline_coordinator.go:1429`, `internal/runtime/pipeline_coordinator.go:1605` |
| `dedup.ambiguous` | `dedup_event_id, existing_vertical, new_candidate, scan_id, similarity` | no | `internal/runtime/pipeline_coordinator.go:1132` |
| `human_task.requested` | `category, description, expected_value, priority, requesting_agent, talking_points, task_id, vertical_id` | no | `internal/runtime/tool_executor.go:908` |
| `market_research.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:902`, `internal/runtime/pipeline_coordinator.go:909` |
| `opco.ceo_ready` | `agent_count, ceo_agent_id, mandate, priority, template_version, vertical_id` | no | `internal/runtime/manager.go:277`, `internal/runtime/manager.go:456` |
| `opco.routing_updated` | `bootstrap_version, event_pattern, installed_by, reason, runtime_tool_event, source, status, subscriber_id, vertical_id` | no | `internal/runtime/tool_executor.go:618` |
| `ops.agent_panic` | `agent_id, backoff_seconds, consecutive_panics, error, vertical_id` | no | `internal/runtime/manager.go:1422` |
| `runtime.auth_required` | `` | yes | `internal/runtime/manager.go:1756` |
| `scan.completed` | `agents_complete, agents_expected, campaign_id, geography, mode, pending_dedup, reports_received, scan_id, timed_out, verticals_discovered, verticals_skipped` | no | `internal/runtime/pipeline_coordinator.go:1264` |
| `scan.requested` | `campaign_id, depth, directive_id, geography, geography_id, mode, priority, strategic_context, taxonomy_categories` | no | `internal/runtime/scan_campaign_manager.go:245` |
| `scanner.google_maps.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:907` |
| `spec.contradiction_detected` | `agent_id, agent_role, reason, runtime_tool, tool_name, vertical_id` | no | `internal/runtime/tool_executor.go:223` |
| `spec.revision_requested` | `feedback, geography, research, scoring, source, spec, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1452`, `internal/runtime/pipeline_coordinator.go:1480`, `internal/runtime/pipeline_coordinator.go:1602` |
| `spec.validation_requested` | `geography, spec, spec_version, validation_tier, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1398` |
| `trend_research.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:904` |
| `validation.more_data_needed` | `geography, request, research, scoring, spec, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1531` |
| `validation.package_ready` | `brand, cto_notes, geography, research, scoring, spec, spec_version, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1401`, `internal/runtime/pipeline_coordinator.go:1611`, `internal/runtime/pipeline_coordinator.go:1742` |
| `validation.started` | `geography, name, scoring, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1337`, `internal/runtime/pipeline_coordinator.go:1599` |
| `vertical.discovered` | `campaign_id, discovery_source, geography, mode, name, raw_signals, scan_id, signal_strength, vertical_id` | no | `internal/runtime/pipeline_coordinator.go:1150`, `internal/runtime/pipeline_coordinator.go:1200` |
| `vertical.killed` | `geography, priority, reason, source_event, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1495` |

## Findings

| Event | Agent | Subscription | Prompt-Expected Fields | Guaranteed Fields | Missing |
|---|---|---|---|---|---|
| `market_research.scan_assigned` | `market-research-agent (market-research-agent)` | `market_research.scan_assigned` | `geography, research, scan_id, taxonomy_categories` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | `research` |
| `spec.contradiction_detected` | `spec-auditor (spec-auditor)` | `spec.contradiction_detected` | `spec` | `agent_id, agent_role, reason, runtime_tool, tool_name, vertical_id` | `spec` |
| `trend_research.scan_assigned` | `trend-research-agent (trend-research-agent)` | `trend_research.scan_assigned` | `geography, research, scan_id` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | `research` |
| `validation.started` | `business-research-agent (business-research-agent)` | `validation.started` | `business_brief, geography, name, spec, vertical_id, vertical_name` | `geography, name, scoring, vertical_id, vertical_name` | `business_brief, spec` |
| `vertical.discovered` | `scoring-coordinator (scoring-coordinator)` | `vertical.discovered` | `mode, scoring` | `campaign_id, discovery_source, geography, mode, name, raw_signals, scan_id, signal_strength, vertical_id` | `scoring` |

## Suggested Next Step

Define typed payload structs for each runtime-emitted event and route all `Publish` payload construction through them to enforce compile-time field contracts.
