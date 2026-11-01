package effectpersistence

const postgresCompletionRecoverySelect = `
	SELECT o.operation_id::text,a.attempt_id::text,o.authority_kind,o.authority_id,a.authority_evidence::text,o.agent_frame_bytes,
	       o.execution_mode,a.execution_mode,
	       a.adapter,a.transport,a.state,a.usage_target_kind,a.usage_target_id::text,COALESCE(a.target_ordinal,0),
	       COALESCE(a.capability_surface_id::text,''),COALESCE(s.surface::text,''),
	       a.execution_owner,a.fence_generation,a.lease_expires_at,COALESCE(a.session_grant_id,''),COALESCE(a.session_lock_owner,''),
	       COALESCE(o.agent_run_id::text,''),COALESCE(o.lineage->>'run_id',''),
	       COALESCE(o.agent_id,''),COALESCE(o.agent_name_owner,''),COALESCE(o.agent_name_source,''),
	       COALESCE(o.agent_route_presence,''),COALESCE(o.flow_scope_key,''),COALESCE(o.flow_instance_id,''),COALESCE(o.flow_instance,''),
	       COALESCE(o.runtime_epoch,0),COALESCE(o.generation,0),
	       COALESCE(a.origin_kind,''),COALESCE(a.origin_delivery_id::text,''),COALESCE(a.origin_run_id::text,''),COALESCE(a.origin_route_identity,''),
	       COALESCE(a.origin_claim_token::text,''),COALESCE(a.origin_claim_version,0),COALESCE(a.origin_subscriber_id,''),
	       COALESCE(a.origin_directive_operation_id::text,''),COALESCE(a.origin_directive_owner_id,''),
	       s.execution_kind,s.execution_authority_id,s.run_id,s.selected_execution_id,s.selected_fork_run_id
	FROM runtime_external_effect_operations o
	JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
	LEFT JOIN managed_agent_capability_surfaces s ON s.surface_id=a.capability_surface_id
`

const sqliteCompletionRecoverySelect = `
	SELECT o.operation_id,a.attempt_id,o.authority_kind,o.authority_id,a.authority_evidence,o.agent_frame_bytes,
	       o.execution_mode,a.execution_mode,
	       a.adapter,a.transport,a.state,a.usage_target_kind,a.usage_target_id,COALESCE(a.target_ordinal,0),
	       COALESCE(a.capability_surface_id,''),COALESCE(s.surface,''),
	       a.execution_owner,a.fence_generation,a.lease_expires_at,COALESCE(a.session_grant_id,''),COALESCE(a.session_lock_owner,''),
	       COALESCE(o.agent_run_id,''),COALESCE(json_extract(o.lineage,'$.run_id'),''),
	       COALESCE(o.agent_id,''),COALESCE(o.agent_name_owner,''),COALESCE(o.agent_name_source,''),
	       COALESCE(o.agent_route_presence,''),COALESCE(o.flow_scope_key,''),COALESCE(o.flow_instance_id,''),COALESCE(o.flow_instance,''),
	       COALESCE(o.runtime_epoch,0),COALESCE(o.generation,0),
	       COALESCE(a.origin_kind,''),COALESCE(a.origin_delivery_id,''),COALESCE(a.origin_run_id,''),COALESCE(a.origin_route_identity,''),
	       COALESCE(a.origin_claim_token,''),COALESCE(a.origin_claim_version,0),COALESCE(a.origin_subscriber_id,''),
	       COALESCE(a.origin_directive_operation_id,''),COALESCE(a.origin_directive_owner_id,''),
	       s.execution_kind,s.execution_authority_id,s.run_id,s.selected_execution_id,s.selected_fork_run_id
	FROM runtime_external_effect_operations o
	JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
	LEFT JOIN managed_agent_capability_surfaces s ON s.surface_id=a.capability_surface_id
`
