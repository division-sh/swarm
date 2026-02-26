# Runtime Payload Completeness Audit

- generated_at: 2026-02-26T17:55:18Z
- runtime_dir: `internal/runtime`
- config_dirs: `configs/agents`, `configs/agents/templates`
- scope: runtime-emitted events (Go-side publish paths) vs subscribed agent prompt field expectations

## Runtime Event Contracts

| Event | Guaranteed Fields | Any Dynamic Path | Sites |
|---|---|---:|---|
| `agent.started` | `agent_id, agent_type, hired_by, mode, role, vertical_id` | no | `internal/runtime/manager.go:222` |
| `agent.tool_execution` | `agent_id, agent_role, duration_ms, error, input, ok, result, runtime_tool, tool_name, vertical_id` | no | `internal/runtime/tool_executor.go:186` |
| `brand.requested` | `business_brief, geography, name, scoring, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2452`, `internal/runtime/pipeline_coordinator.go:2731` |
| `brand.revision_needed` | `brand, feedback, geography, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2674` |
| `campaign.completed` | `campaign_id, completed_mode, directive_id, discoveries_count, geography_id, priority, source_event_id, strategic_context` | no | `internal/runtime/scan_campaign_manager.go:208` |
| `cto.spec_review_requested` | `geography, research, scoring, spec, spec_validation, spec_version, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2529`, `internal/runtime/pipeline_coordinator.go:2544`, `internal/runtime/pipeline_coordinator.go:2728` |
| `dedup.ambiguous` | `dedup_event_id, existing_vertical, new_candidate, scan_id, similarity` | no | `internal/runtime/pipeline_coordinator.go:1448` |
| `human_task.requested` | `category, description, expected_value, priority, requesting_agent, talking_points, task_id, vertical_id` | no | `internal/runtime/tool_executor.go:931` |
| `market_research.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1200`, `internal/runtime/pipeline_coordinator.go:1202`, `internal/runtime/pipeline_coordinator.go:1212` |
| `opco.ceo_ready` | `agent_count, ceo_agent_id, mandate, priority, template_version, vertical_id` | no | `internal/runtime/manager.go:315`, `internal/runtime/manager.go:494` |
| `opco.routing_updated` | `bootstrap_version, event_pattern, installed_by, reason, runtime_tool_event, source, status, subscriber_id, vertical_id` | no | `internal/runtime/tool_executor.go:638` |
| `opco.teardown_complete` | `agents_removed, priority, routing_cleared, vertical_id, workspace_stopped` | no | `internal/runtime/manager.go:1005` |
| `ops.agent_panic` | `agent_id, backoff_seconds, consecutive_panics, error, vertical_id` | no | `internal/runtime/manager.go:1621` |
| `scan.completed` | `agents_complete, agents_expected, campaign_id, geography, mode, pending_dedup, reports_received, scan_id, shards_completed, shards_failed, shards_total, timed_out, verticals_discovered, verticals_skipped` | no | `internal/runtime/pipeline_coordinator.go:1574`, `internal/runtime/pipeline_coordinator.go:2976` |
| `scan.requested` | `campaign_context, campaign_id, depth, directive_id, geography, geography_id, mode, priority, strategic_context, taxonomy_categories` | no | `internal/runtime/scan_campaign_manager.go:261` |
| `scanner.directories.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1209` |
| `scanner.google_maps.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1206` |
| `scanner.instagram.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1207` |
| `scanner.job_boards.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1210` |
| `scanner.reviews.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1208` |
| `scoring.contested` | `dimension, evidence, mode, rubric, scores, spread, vertical_id` | no | `internal/runtime/pipeline_coordinator.go:1911` |
| `scoring.requested` | `dimensions_requested, geography, mode, rubric, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:1840` |
| `spec.contradiction_detected` | `agent_id, agent_role, reason, runtime_tool, tool_name, vertical_id` | no | `internal/runtime/tool_executor.go:235` |
| `spec.revision_requested` | `feedback, geography, research, scoring, source, spec, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2568`, `internal/runtime/pipeline_coordinator.go:2597`, `internal/runtime/pipeline_coordinator.go:2725` |
| `spec.validation_requested` | `geography, spec, spec_version, validation_tier, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2513` |
| `synthesis.needed` | `campaign_id, category, conflict_notes, geography, mode, raw_report, scan_id, subcategory` | no | `internal/runtime/pipeline_coordinator.go:1387` |
| `timer.portfolio_digest` | `digest_text, message, metadata, recent_rejections, rejection_count, scoring_rejection_summaries, scoring_rejections_count, scoring_rejections_injected, snapshot, task_id, trigger_reason, vertical_id` | no | `internal/runtime/pipeline_coordinator.go:2181` |
| `trend_research.scan_assigned` | `campaign_context, campaign_id, directive_id, geography, geography_id, mode, planned_shards, priority, requested_at, scan_id, strategic_context, taxonomy_categories` | no | `internal/runtime/pipeline_coordinator.go:1204` |
| `validation.more_data_needed` | `geography, request, research, scoring, spec, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2651` |
| `validation.package_ready` | `brand, cto_notes, geography, research, scoring, spec, spec_version, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2516`, `internal/runtime/pipeline_coordinator.go:2734`, `internal/runtime/pipeline_coordinator.go:2896` |
| `validation.started` | `geography, name, scoring, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2450`, `internal/runtime/pipeline_coordinator.go:2722` |
| `vertical.discovered` | `campaign_id, discovery_source, geography, mode, name, raw_signals, scan_id, signal_strength, vertical_id` | no | `internal/runtime/pipeline_coordinator.go:1460`, `internal/runtime/pipeline_coordinator.go:1500` |
| `vertical.killed` | `geography, priority, reason, source_event, vertical_id, vertical_name` | no | `internal/runtime/pipeline_coordinator.go:2613` |
| `vertical.marginal` | `composite_score, dimensions, promotion_eligible, vertical_id, viability_score` | no | `internal/runtime/pipeline_coordinator.go:2127` |
| `vertical.rejected` | `reason, vertical_id` | no | `internal/runtime/pipeline_coordinator.go:2131` |
| `vertical.scored` | `composite_score, dimensions, geography, market_score, mode, partial, reason, result, rubric, vertical_id, vertical_name, viability_score` | no | `internal/runtime/pipeline_coordinator.go:2109` |
| `vertical.shortlisted` | `composite_score, scoring_payload, vertical_id, viability_score` | no | `internal/runtime/pipeline_coordinator.go:2115` |

## Findings

No prompt-to-payload gaps detected for runtime-emitted events.
