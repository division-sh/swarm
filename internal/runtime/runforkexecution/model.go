package runforkexecution

import (
	"fmt"
	"strings"

	"swarm/internal/store"
)

type SelectedContractExecutionModelRequest struct {
	Admission store.RunForkContractFrontierAdmission
}

func BuildSelectedContractExecutionModel(req SelectedContractExecutionModelRequest) (store.RunForkSelectedContractExecution, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != store.RunForkContractFrontierAdmissionOwner {
		return store.RunForkSelectedContractExecution{}, fmt.Errorf("selected-contract execution model requires %s admission; got %q", store.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return store.RunForkSelectedContractExecution{}, fmt.Errorf("selected-contract frontier admission must be non-mutating")
	}
	if admission.HistoricalExecutionSupported {
		return store.RunForkSelectedContractExecution{}, fmt.Errorf("selected-contract frontier admission unexpectedly supports historical execution")
	}

	return store.RunForkSelectedContractExecution{
		Owner:                store.RunForkSelectedContractExecutionModelOwner,
		FutureExecutionOwner: store.RunForkSelectedContractExecutionOwner,
		NonMutating:          true,
		ExecutionSupported:   false,
		ContractSelection:    admission.ContractSelection,
		AdmissionOwner:       admission.Owner,
		AdmissionUse:         store.RunForkSelectedContractExecutionAdmissionUseEvidenceOnly,
		FrontierEventCount:   admission.FrontierEventCount,
		FrontierEvents:       selectedContractFrontierEvents(admission.FrontierEvents),
		ContractBinding: store.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_binding",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			Reason:      "future execution must bind the selected contract source to the fork before handlers run",
		},
		RequiredConsumers: selectedContractExecutionRequiredConsumers(),
		BlockedSiblings:   selectedContractExecutionBlockedSiblings(),
		InvalidPaths:      selectedContractExecutionInvalidPaths(),
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{{
			Code:    store.RunForkBlockerSelectedContractExecutionModelNonMutating,
			Message: "selected-contract fork execution is model-only; executable fork work remains separately gated",
		}},
	}, nil
}

func selectedContractFrontierEvents(events []store.RunForkContractFrontierEvent) []store.RunForkSelectedContractFrontierEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]store.RunForkSelectedContractFrontierEvent, 0, len(events))
	for _, event := range events {
		out = append(out, store.RunForkSelectedContractFrontierEvent{
			SourceEventID:           event.SourceEventID,
			EventName:               event.EventName,
			RuntimeEventOwners:      append([]string(nil), event.RuntimeEventOwners...),
			WorkflowNodeSubscribers: append([]string(nil), event.WorkflowNodeSubscribers...),
			DerivedRecipients:       append([]store.RunForkContractFrontierRecipient(nil), event.DerivedRecipients...),
			Disposition:             store.RunForkSelectedContractDispositionEvidenceOnly,
		})
	}
	return out
}

func selectedContractExecutionRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "fork_run_id_runtime_context",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			Reason:      "future handlers must execute with the fork run_id, not the source run_id",
		},
		{
			Concept:     "fork_local_event_delivery_writes",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			Reason:      "future execution must create fresh fork-local IDs and lineage instead of copying source rows",
		},
		{
			Concept:     "handler_execution",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       "internal/runtime/pipeline",
			Reason:      "normal handler execution is a required future consumer, but this owner model does not run handlers",
		},
		{
			Concept:     "receipts_dead_letters_idempotency",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       "internal/store/event_receipt_store.go+internal/runtime/deadletters",
			Reason:      "future execution must write fork-local outcomes and must not use source outcomes as suppressors without an approved model",
		},
		{
			Concept:     "emitted_follow_up_events",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       "internal/runtime/bus",
			Reason:      "future follow-up events must be regenerated under the fork run_id through the runtime bus",
		},
		{
			Concept:     "safe_agent_delivery_event_replay",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkDeliveryEventReplayOwner,
			Reason:      "safe pending-agent replay remains a sibling pattern for fresh IDs and lineage, not the selected-contract execution owner",
		},
	}
}

func selectedContractExecutionBlockedSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "node_system_non_agent_execution",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractExecutionOwner,
			Reason:      "node/system execution requires a later mutating owner and remains blocked here",
		},
		{
			Concept:     "timers_routes",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer and route reconstruction remain separate fork replay/resume blockers",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "session, turn, and audit reconstruction remain separately gated",
		},
		{
			Concept:     "source_advanced_after_fork_point",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "source advancement remains fail-closed until a branch/suppression policy is approved",
		},
		{
			Concept:     "contract_swap_boot_resume",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "full selected-contract boot/resume execution remains outside this non-mutating model",
		},
		{
			Concept:     "builder_dashboard_ui",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "operator UI is a later consumer and must not become the execution owner",
		},
	}
}

func selectedContractExecutionInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "copy_source_event_deliveries",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source deliveries are lineage/blocker evidence, not executable fork work",
		},
		{
			Concept:     "copy_source_events",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "fork events require fresh fork-local event IDs and lineage",
		},
		{
			Concept:     "cli_owned_execution",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "CLI may consume the model but must not own selected-contract execution semantics",
		},
		{
			Concept:     "same_run_outbox_replay_as_fork_replay",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "same-run recovery does not define timestamp-fork selected-contract replay ownership",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, and post-T outcomes cannot suppress fork-local work without an approved model",
		},
	}
}
