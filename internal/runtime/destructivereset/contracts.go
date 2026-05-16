package destructivereset

const (
	ContractRunDeliveryQuiescence = "active_run_delivery_quiescence"
	ContractRunScopedTruncation   = "run_scoped_truncation_preservation"
	ContractManagedContainers     = "managed_entity_container_selection_preservation"
	ContractPublicAPIWrapper      = "runtime_nuke_api_spec_wrapper"
	ContractLegacyResetMigration  = "legacy_reset_seam_migration"
)

func DefaultDownstreamContracts() []DownstreamContract {
	return []DownstreamContract{
		{
			ID:          ContractRunDeliveryQuiescence,
			Status:      "implemented_internal_owner",
			Owner:       "internal/runtime/destructivereset Quiescer and PostgresStore.ApplyDestructiveResetQuiescence",
			Description: "Stop active runs and cancel pending/in-progress deliveries with nuke-specific reasons before destructive reset cleanup can apply.",
		},
		{
			ID:          ContractRunScopedTruncation,
			Status:      "implemented_internal_owner",
			Owner:       "internal/runtime/destructivereset cleanup catalog and PostgresStore.ApplyDestructiveResetCleanup",
			Description: "Define the exact run-scoped table set and preservation rules for schema/auth/operator/system state before cleanup can apply.",
		},
		{
			ID:          ContractManagedContainers,
			Status:      "split",
			Owner:       "future workspace destructive reset container owner",
			Description: "Select and stop only swarm-managed entity containers while proving system/operator-managed containers are preserved.",
		},
		{
			ID:          ContractPublicAPIWrapper,
			Status:      "split",
			Owner:       "future v1 runtime.nuke API wrapper",
			Description: "Expose /v1/rpc runtime.nuke and authoritative platform-spec/OpenRPC only after destructive reset owners exist.",
		},
		{
			ID:          ContractLegacyResetMigration,
			Status:      "split",
			Owner:       "future legacy reset migration owner",
			Description: "Retire, redirect, or explicitly split dashboard/Builder/run_clear reset seams after canonical reset ownership exists.",
		},
	}
}

func DefaultResetSeams() []ResetSeam {
	return []ResetSeam{
		{
			ID:             "dashboard_runtime_actions_reset_state",
			Classification: "legacy_deferred_sibling",
			RequiredAction: "Do not treat dashboard reset_state as canonical reset ownership; migrate, retire, or split before runtime.nuke closure.",
		},
		{
			ID:             "builder_runtime_reset_state",
			Classification: "legacy_deferred_sibling",
			RequiredAction: "Do not treat Builder reset_state/run hub reset as canonical reset ownership; migrate, retire, or split before runtime.nuke closure.",
		},
		{
			ID:             "agent_manager_reset_runtime_state_with_source",
			Classification: "partial_internal_primitive",
			RequiredAction: "May become a downstream primitive only after explicit contract repair; not dry-run or destructive-reset ownership today.",
		},
		{
			ID:             "startup_recovery_failed_reset",
			Classification: "safety_trigger_sibling",
			RequiredAction: "Keep as recovery-specific internal reset semantics unless a later gate explicitly absorbs it into the canonical reset owner.",
		},
		{
			ID:             "sessions_reset_all",
			Classification: "partial_session_primitive",
			RequiredAction: "Session reset is downstream-only and must not imply run, delivery, table, or container reset ownership.",
		},
		{
			ID:             "run_stop",
			Classification: "different_narrower_concept",
			RequiredAction: "Per-run stop can be a downstream primitive; it does not own all-run nuke planning or in-progress delivery cancellation.",
		},
		{
			ID:             "workspace_docker_helpers",
			Classification: "container_primitive",
			RequiredAction: "Workspace helpers can be downstream primitives; they do not own global preservation or selection semantics.",
		},
		{
			ID:             "scripts_run_clear_reset_dev",
			Classification: "direct_dev_bypass",
			RequiredAction: "Keep dev-only until migrated or split; raw truncate/docker stop must not become production reset ownership.",
		},
	}
}

func DefaultPreservedResources() PreservedResources {
	return PreservedResources{
		SystemContainers:        []string{"swarm-scaffold", "swarm-system"},
		OperatorManagedBoundary: "operator-managed containers are outside Swarm ownership and are not enumerable by this planner",
		SchemaMigrations:        true,
		AuthTokens:              true,
		BundleContracts:         true,
	}
}
