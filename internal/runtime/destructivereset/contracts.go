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
			Status:      "implemented_internal_owner",
			Owner:       "internal/runtime/containeridentity, internal/runtime/workspace managed identity stamping, and internal/runtime/destructivereset ManagedContainerStopper",
			Description: "Select and stop only label-proven reset-eligible managed runtime containers while preserving system/operator/unowned containers.",
		},
		{
			ID:          ContractPublicAPIWrapper,
			Status:      "implemented_public_owner",
			Owner:       "platform-spec.yaml, generated openrpc.json, and internal/apiv1 runtime.nuke handler over destructive reset owners",
			Description: "Expose /v1/rpc runtime.nuke as the authoritative public wrapper over the destructive reset plan, quiescence, cleanup, and managed-container owners.",
		},
		{
			ID:          ContractLegacyResetMigration,
			Status:      "split",
			Owner:       "future legacy reset migration owner",
			Description: "Retire, redirect, or explicitly split remaining CLI reset seams after canonical reset ownership exists; stale dashboard/Builder reset_state plumbing is retired/fail-closed and run_clear reset-dev consumes runtime.nuke.",
		},
	}
}

func DefaultResetSeams() []ResetSeam {
	return []ResetSeam{
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
