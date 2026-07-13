package destructivereset

const (
	CleanupTableKindPlatform  = "platform_table"
	CleanupTableKindGenerated = "generated_table_class"

	CleanupDeleteAll                = "delete_all"
	CleanupDeleteByRunID            = "delete_by_run_id"
	CleanupDeleteByEventJoin        = "delete_by_event_join"
	CleanupDeleteByRunLineage       = "delete_by_run_lineage"
	CleanupDeleteMixedRowPolicy     = "delete_mixed_row_policy"
	CleanupRetainDirectiveAuthority = "retain_directive_authority"
	CleanupPreserve                 = "preserve"
	CleanupSplitPreserve            = "split_preserve"
	CleanupRequestScopedBundles     = "request_scoped_bundle_catalog"
)

type CleanupPolicy struct {
	IncludeBundles bool
}

func DefaultPlatformCleanupCatalog() []CleanupCatalogEntry {
	return []CleanupCatalogEntry{
		{Table: "event_receipts", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByEventJoin, PredicateOwner: "events.run_id", DeleteOrderGroup: 1},
		{Table: "dead_letters", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByEventJoin, PredicateOwner: "dead_letters.original_event_id -> events.run_id", DeleteOrderGroup: 1},
		{Table: "run_fork_delivery_event_replays", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunLineage, PredicateOwner: "fork_run_id|source_run_id|source_event_id|fork_event_id -> events.run_id|source_delivery_id|fork_delivery_id -> event_deliveries/events.run_id", DeleteOrderGroup: 2},
		{Table: "event_deliveries", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "event_deliveries.run_id|event_id -> events.run_id", DeleteOrderGroup: 2},
		{Table: "run_fork_selected_contract_executions", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunLineage, PredicateOwner: "fork_run_id|source_run_id|source_event_id|fork_event_id -> events.run_id", DeleteOrderGroup: 2},
		{Table: "run_fork_selected_contract_branch_divergences", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunLineage, PredicateOwner: "fork_run_id|source_run_id|fork_event_id -> events.run_id", DeleteOrderGroup: 2},
		{Table: "run_fork_selected_contract_route_recoveries", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunLineage, PredicateOwner: "fork_run_id|source_run_id|fork_event_id -> events.run_id", DeleteOrderGroup: 2},
		{Table: "run_fork_selected_contract_bindings", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunLineage, PredicateOwner: "fork_run_id|source_run_id|fork_event_id -> events.run_id", DeleteOrderGroup: 2},
		{Table: "activity_attempts", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "activity_attempts.run_id", DeleteOrderGroup: 3},
		{Table: "decision_card_lifecycle_outbox", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "decision_card_lifecycle_outbox.run_id", DeleteOrderGroup: 1},
		{Table: "decision_card_route_obligations", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "decision_card_route_obligations.run_id", DeleteOrderGroup: 1},
		{Table: "decision_card_changes", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "decision_card_changes.run_id", DeleteOrderGroup: 1},
		{Table: "decision_card_input_drafts", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "decision_card_input_drafts.run_id", DeleteOrderGroup: 1},
		{Table: "decision_cards", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "decision_cards.run_id", DeleteOrderGroup: 2},
		{Table: "agent_directive_operations", TableKind: CleanupTableKindPlatform, Classification: CleanupRetainDirectiveAuthority, PredicateOwner: "agent_directive_operations.resolved_run_id plus operation state/expires_at", PreservationProof: "runtime.nuke fails closed while nonterminal, indeterminate, or unexpired terminal directive authority exists"},
		{Table: "agent_lifecycle_operations", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "durable agent lifecycle operation authority", PreservationProof: "lifecycle replay and reconciliation authority must survive destructive runtime cleanup"},
		{Table: "agent_lifecycle_transition_facts", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "immutable agent lifecycle transition evidence", PreservationProof: "lifecycle lineage evidence must survive destructive runtime cleanup"},
		{Table: "agent_lifecycle_diagnostic_outbox", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "durable lifecycle diagnostic projection authority", PreservationProof: "unprojected lifecycle outcomes must survive destructive runtime cleanup"},
		{Table: "runtime_external_effect_operations", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "durable managed-effect outcome authority", PreservationProof: "effect recovery and no-redispatch authority must survive destructive runtime cleanup"},
		{Table: "runtime_external_effect_attempts", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "immutable managed-effect attempt evidence", PreservationProof: "effect attempt evidence must survive destructive runtime cleanup"},
		{Table: "run_fork_selected_contract_runtime_executions", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "durable selected-contract runtime generation authority", PreservationProof: "selected-fork completion authority and terminal evidence must survive mutable runtime cleanup"},
		{Table: "budget_admission_scopes", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "durable completion budget admission lock authority", PreservationProof: "cross-process budget admission serialization must survive destructive runtime cleanup"},
		{Table: "runtime_effect_budget_reservations", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "active completion attempt budget claims", PreservationProof: "reservations must survive until terminal settlement or startup recovery releases them"},
		{Table: "agent_turns", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "agent_turns.run_id", DeleteOrderGroup: 3},
		{Table: "agent_conversation_audits", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "agent_conversation_audits.run_id", DeleteOrderGroup: 3},
		{Table: "agent_sessions", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "agent_sessions.run_id", DeleteOrderGroup: 3},
		{Table: "conversation_fork_snapshots", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "conversation_fork_snapshots.fork_id -> conversation_forks.source_run_id", DeleteOrderGroup: 3},
		{Table: "conversation_fork_turns", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "conversation_fork_turns.fork_id -> conversation_forks.source_run_id", DeleteOrderGroup: 3},
		{Table: "conversation_fork_turn_completions", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "conversation_fork_turn_completions.fork_turn_id -> conversation_fork_turns.fork_id -> conversation_forks.source_run_id", DeleteOrderGroup: 2},
		{Table: "conversation_forks", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "conversation_forks.source_run_id", DeleteOrderGroup: 3},
		{Table: "entity_mutations", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "entity_mutations.run_id", DeleteOrderGroup: 3},
		{Table: "entity_state", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "entity_state.run_id", DeleteOrderGroup: 3},
		{Table: "timers", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteMixedRowPolicy, PredicateOwner: "timers.run_id|forked_from_run_id|forked_from_event_id -> events.run_id", DeleteOrderGroup: 3},
		{Table: "run_control_state", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "run_control_state.run_id", DeleteOrderGroup: 3},
		{Table: "reply_contexts", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "reply_contexts.run_id", DeleteOrderGroup: 4},
		{Table: "events", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteByRunID, PredicateOwner: "events.run_id", DeleteOrderGroup: 5},
		{Table: "runs", TableKind: CleanupTableKindPlatform, Classification: CleanupDeleteAll, PredicateOwner: "runs.run_id cleanup set", DeleteOrderGroup: 6},
		{Table: "runtime_store_metadata", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "immutable runtime store creation origin", PreservationProof: "must survive destructive runtime cleanup unchanged"},
		{Table: "api_idempotency", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "API idempotency/auth-like state", PreservationProof: "must survive destructive runtime cleanup"},
		{Table: "runtime_ingress_state", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "singleton runtime ingress owner", PreservationProof: "must survive destructive runtime cleanup"},
		{Table: "agents", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "agent registry/config state", PreservationProof: "must survive destructive runtime cleanup"},
		{Table: "flow_instances", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "product/config state", PreservationProof: "must survive destructive runtime cleanup"},
		{Table: "routing_rules", TableKind: CleanupTableKindPlatform, Classification: CleanupPreserve, PredicateOwner: "routing/topology config", PreservationProof: "must survive destructive runtime cleanup"},
		{Table: "bundles", TableKind: CleanupTableKindPlatform, Classification: CleanupRequestScopedBundles, PredicateOwner: "runtime.nuke include_bundles request policy", PreservationProof: "include_bundles=false preserves bundle catalog state; include_bundles=true deletes it as part of server-wide runtime.nuke"},
		{Table: "mailbox", TableKind: CleanupTableKindPlatform, Classification: CleanupSplitPreserve, PredicateOwner: "no run_id; source_event_id policy split", PreservationProof: "preserve until a mailbox cleanup owner exists"},
		{Table: "spend_ledger", TableKind: CleanupTableKindPlatform, Classification: CleanupSplitPreserve, PredicateOwner: "no run_id; cost audit policy split", PreservationProof: "preserve until a spend cleanup owner exists"},
	}
}

func PlatformCleanupCatalogForPolicy(policy CleanupPolicy) []CleanupCatalogEntry {
	catalog := DefaultPlatformCleanupCatalog()
	for i := range catalog {
		catalog[i] = CleanupEntryForPolicy(catalog[i], policy)
	}
	return catalog
}

func DefaultGeneratedCleanupCatalog() []CleanupCatalogEntry {
	return []CleanupCatalogEntry{
		{Table: "generated_entity_tables", TableKind: CleanupTableKindGenerated, Classification: CleanupSplitPreserve, PredicateOwner: "no generated run_id", PreservationProof: "deleting by entity_id would cross runs"},
		{Table: "generated_node_state_tables", TableKind: CleanupTableKindGenerated, Classification: CleanupSplitPreserve, PredicateOwner: "no generated run_id", PreservationProof: "deleting by entity_id would cross runs"},
		{Table: "contract_driven_product_optimization_tables", TableKind: CleanupTableKindGenerated, Classification: CleanupSplitPreserve, PredicateOwner: "product-owned optimization state", PreservationProof: "not reset truth"},
	}
}

func DefaultCleanupCatalog() []CleanupCatalogEntry {
	out := DefaultPlatformCleanupCatalog()
	out = append(out, DefaultGeneratedCleanupCatalog()...)
	return out
}

func CleanupCatalogForPolicy(policy CleanupPolicy) []CleanupCatalogEntry {
	out := PlatformCleanupCatalogForPolicy(policy)
	out = append(out, DefaultGeneratedCleanupCatalog()...)
	return out
}

func CleanupCatalogByTable() map[string]CleanupCatalogEntry {
	out := map[string]CleanupCatalogEntry{}
	for _, entry := range DefaultPlatformCleanupCatalog() {
		out[entry.Table] = entry
	}
	return out
}

func CleanupCatalogByTableForPolicy(policy CleanupPolicy) map[string]CleanupCatalogEntry {
	out := map[string]CleanupCatalogEntry{}
	for _, entry := range PlatformCleanupCatalogForPolicy(policy) {
		out[entry.Table] = entry
	}
	return out
}

func CleanupEntryForPolicy(entry CleanupCatalogEntry, policy CleanupPolicy) CleanupCatalogEntry {
	if entry.Table != "bundles" || entry.Classification != CleanupRequestScopedBundles {
		return entry
	}
	if policy.IncludeBundles {
		entry.Classification = CleanupDeleteAll
		entry.PredicateOwner = "runtime.nuke include_bundles=true server-wide bundle catalog deletion"
		entry.DeleteOrderGroup = 7
		entry.PreservationProof = ""
		return entry
	}
	entry.Classification = CleanupPreserve
	entry.PredicateOwner = "runtime.nuke include_bundles=false bundle catalog preservation"
	entry.DeleteOrderGroup = 0
	entry.PreservationProof = "bundle catalog rows must survive runtime.nuke when include_bundles=false"
	return entry
}
