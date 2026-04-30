package runforkexecution

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"swarm/internal/store"
)

type SelectedContractExecutionModelRequest struct {
	Admission      store.RunForkContractFrontierAdmission
	RouteAdmission store.RunForkSelectedContractRouteAdmission
	RouteTopology  store.RunForkSelectedContractRouteTopology
}

type SelectedContractRouteTopologyRequest struct {
	Admission      store.RunForkContractFrontierAdmission
	RouteAdmission store.RunForkSelectedContractRouteAdmission
}

func BuildSelectedContractRouteTopology(req SelectedContractRouteTopologyRequest) (store.RunForkSelectedContractRouteTopology, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != store.RunForkContractFrontierAdmissionOwner {
		return store.RunForkSelectedContractRouteTopology{}, fmt.Errorf("selected-contract route topology requires %s admission; got %q", store.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return store.RunForkSelectedContractRouteTopology{}, fmt.Errorf("selected-contract route topology requires non-mutating frontier admission")
	}
	if admission.HistoricalExecutionSupported {
		return store.RunForkSelectedContractRouteTopology{}, fmt.Errorf("selected-contract route topology unexpectedly supports historical execution")
	}
	routeAdmission := req.RouteAdmission
	if err := validateSelectedContractRouteAdmission(admission, routeAdmission); err != nil {
		return store.RunForkSelectedContractRouteTopology{}, err
	}
	return canonicalSelectedContractRouteTopology(admission, routeAdmission), nil
}

func canonicalSelectedContractRouteTopology(frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission) store.RunForkSelectedContractRouteTopology {
	blockers := []store.RunForkUnsupportedBlocker{{
		Code:    store.RunForkBlockerSelectedContractRouteTopologyNonMutating,
		Message: "selected-contract route topology is non-mutating; route persistence, recipient delivery writes, and handler execution remain separately gated",
	}}
	dynamicDisposition := store.RunForkSelectedContractDispositionForkLocalTruth
	dynamicFlowInstances := sortedTrimmedStrings(routeAdmission.DynamicFlowInstances)
	dynamicProofs := selectedContractDynamicRouteTopologyProofs(frontier, routeAdmission, dynamicFlowInstances)
	dynamicSupported := len(dynamicFlowInstances) == 0 || len(dynamicProofs) == len(dynamicFlowInstances)
	if len(dynamicFlowInstances) > 0 && !dynamicSupported {
		dynamicDisposition = store.RunForkSelectedContractDispositionFailClosed
		blockers = appendRunForkUnsupportedBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven,
			Message: "selected-contract dynamic flow-instance topology requires fork-local topology evidence before route reconstruction",
		})
	}
	for _, blocker := range routeAdmission.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}

	staticEvents := selectedContractRouteTopologyEvents(routeAdmission.SelectedRouteEvents)
	return store.RunForkSelectedContractRouteTopology{
		Owner:                          store.RunForkSelectedContractRouteTopologyOwner,
		RouteAdmissionOwner:            routeAdmission.Owner,
		FutureRouteReconstructionOwner: routeAdmission.FutureRouteReconstructionOwner,
		NonMutating:                    true,
		RoutePersistenceSupported:      false,
		ExecutableRecipientsSupported:  false,
		ContractSelection:              routeAdmission.ContractSelection,
		StaticTopologySupported:        true,
		DynamicTopologySupported:       dynamicSupported,
		DynamicTopologyOwner:           selectedContractDynamicRouteTopologyOwner(dynamicFlowInstances),
		SourceRouteFactsPresent:        routeAdmission.SourceRouteFactsPresent,
		StaticRouteEvents:              staticEvents,
		DynamicFlowInstances:           dynamicFlowInstances,
		DynamicTopologyProofs:          dynamicProofs,
		DynamicTopologyDisposition:     dynamicDisposition,
		FrontierAdmissionOwner:         routeAdmission.FrontierAdmissionOwner,
		FrontierEventCount:             routeAdmission.FrontierEventCount,
		FrontierSourceEventIDs:         append([]string(nil), routeAdmission.FrontierSourceEventIDs...),
		FrontierEvidenceFingerprint:    routeAdmission.FrontierEvidenceFingerprint,
		RequiredEvidence:               selectedContractRouteTopologyRequiredEvidence(routeAdmission),
		RequiredConsumers:              selectedContractRouteTopologyRequiredConsumers(),
		BlockedSiblings:                selectedContractRouteTopologyBlockedSiblings(),
		InvalidPaths:                   selectedContractRouteTopologyInvalidPaths(),
		UnsupportedBlockers:            blockers,
	}
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
	routeTopology := req.RouteTopology
	routeAdmission := req.RouteAdmission
	if err := validateSelectedContractRouteAdmission(admission, routeAdmission); err != nil {
		return store.RunForkSelectedContractExecution{}, err
	}
	if err := validateSelectedContractRouteTopology(admission, routeAdmission, routeTopology); err != nil {
		return store.RunForkSelectedContractExecution{}, err
	}
	recipientPlanning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		return store.RunForkSelectedContractExecution{}, err
	}

	unsupportedBlockers := append([]store.RunForkUnsupportedBlocker(nil), admission.UnsupportedBlockers...)
	for _, blocker := range routeTopology.UnsupportedBlockers {
		unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, blocker)
	}
	for _, blocker := range recipientPlanning.UnsupportedBlockers {
		unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, blocker)
	}
	unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, store.RunForkUnsupportedBlocker{
		Code:    store.RunForkBlockerSelectedContractExecutionModelNonMutating,
		Message: "selected-contract fork execution is model-only; executable fork work remains separately gated",
	})

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
		RouteTopology:        &routeTopology,
		RecipientPlanning:    &recipientPlanning,
		ContractBinding: store.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_binding",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractBindingOwner,
			Reason:      "future execution must consume the durable selected contract source bound to the fork run before handlers run",
		},
		RequiredConsumers:   selectedContractExecutionRequiredConsumers(),
		BlockedSiblings:     selectedContractExecutionBlockedSiblings(),
		InvalidPaths:        selectedContractExecutionInvalidPaths(),
		UnsupportedBlockers: unsupportedBlockers,
	}, nil
}

func validateSelectedContractRouteTopology(frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission, topology store.RunForkSelectedContractRouteTopology) error {
	if strings.TrimSpace(topology.Owner) != store.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract execution model requires %s route topology; got %q", store.RunForkSelectedContractRouteTopologyOwner, topology.Owner)
	}
	if strings.TrimSpace(topology.RouteAdmissionOwner) != store.RunForkSelectedContractRouteAdmissionOwner {
		return fmt.Errorf("selected-contract route topology must consume %s; got %q", store.RunForkSelectedContractRouteAdmissionOwner, topology.RouteAdmissionOwner)
	}
	if !topology.NonMutating {
		return fmt.Errorf("selected-contract route topology must be non-mutating")
	}
	if topology.RoutePersistenceSupported {
		return fmt.Errorf("selected-contract route topology unexpectedly supports route persistence")
	}
	if topology.ExecutableRecipientsSupported {
		return fmt.Errorf("selected-contract route topology unexpectedly supports executable recipients")
	}
	if strings.TrimSpace(topology.FrontierAdmissionOwner) != store.RunForkContractFrontierAdmissionOwner {
		return fmt.Errorf("selected-contract route topology must consume %s; got %q", store.RunForkContractFrontierAdmissionOwner, topology.FrontierAdmissionOwner)
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := store.RunForkContractFrontierEvidenceBinding(frontier)
	if topology.FrontierEventCount != frontierEventCount {
		return fmt.Errorf("selected-contract route topology frontier count mismatch: got %d want %d", topology.FrontierEventCount, frontierEventCount)
	}
	if !equalStringSlices(topology.FrontierSourceEventIDs, frontierSourceEventIDs) {
		return fmt.Errorf("selected-contract route topology frontier source event IDs do not match current frontier evidence")
	}
	if strings.TrimSpace(topology.FrontierEvidenceFingerprint) != frontierFingerprint {
		return fmt.Errorf("selected-contract route topology frontier fingerprint mismatch")
	}
	if err := validateSelectionMatches("route topology", frontier.ContractSelection, topology.ContractSelection); err != nil {
		return err
	}
	canonical := canonicalSelectedContractRouteTopology(frontier, routeAdmission)
	if !reflect.DeepEqual(topology, canonical) {
		return fmt.Errorf("selected-contract route topology does not match canonical route-admission evidence")
	}
	return nil
}

func validateSelectedContractRouteAdmission(frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission) error {
	if strings.TrimSpace(routeAdmission.Owner) != store.RunForkSelectedContractRouteAdmissionOwner {
		return fmt.Errorf("selected-contract execution model requires %s route admission; got %q", store.RunForkSelectedContractRouteAdmissionOwner, routeAdmission.Owner)
	}
	if !routeAdmission.NonMutating {
		return fmt.Errorf("selected-contract route admission must be non-mutating")
	}
	if routeAdmission.RouteReconstructionSupported {
		return fmt.Errorf("selected-contract route admission unexpectedly supports route reconstruction")
	}
	if strings.TrimSpace(routeAdmission.FrontierAdmissionOwner) != store.RunForkContractFrontierAdmissionOwner {
		return fmt.Errorf("selected-contract route admission must consume %s; got %q", store.RunForkContractFrontierAdmissionOwner, routeAdmission.FrontierAdmissionOwner)
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := store.RunForkContractFrontierEvidenceBinding(frontier)
	if routeAdmission.FrontierEventCount != frontierEventCount {
		return fmt.Errorf("selected-contract route admission frontier count mismatch: got %d want %d", routeAdmission.FrontierEventCount, frontierEventCount)
	}
	if !equalStringSlices(routeAdmission.FrontierSourceEventIDs, frontierSourceEventIDs) {
		return fmt.Errorf("selected-contract route admission frontier source event IDs do not match current frontier evidence")
	}
	if strings.TrimSpace(routeAdmission.FrontierEvidenceFingerprint) != frontierFingerprint {
		return fmt.Errorf("selected-contract route admission frontier fingerprint mismatch")
	}
	if err := validateSelectionMatches("route admission", frontier.ContractSelection, routeAdmission.ContractSelection); err != nil {
		return err
	}
	return nil
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func selectedContractRouteTopologyEvents(events []store.RunForkSelectedContractRouteEvent) []store.RunForkSelectedContractRouteEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]store.RunForkSelectedContractRouteEvent, 0, len(events))
	for _, event := range events {
		out = append(out, store.RunForkSelectedContractRouteEvent{
			SourceEventID:     event.SourceEventID,
			EventName:         event.EventName,
			DerivedRecipients: append([]store.RunForkContractFrontierRecipient(nil), event.DerivedRecipients...),
			Disposition:       store.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	return out
}

func selectedContractDynamicRouteTopologyOwner(instances []string) string {
	if len(instances) == 0 {
		return ""
	}
	return store.RunForkSelectedContractDynamicRouteTopologyOwner
}

func selectedContractDynamicRouteTopologyProofs(frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission, instances []string) []store.RunForkSelectedContractDynamicTopologyProof {
	if len(instances) == 0 {
		return nil
	}
	evidence := selectedContractDynamicTopologyEvidence(frontier, routeAdmission)
	proofs := make([]store.RunForkSelectedContractDynamicTopologyProof, 0, len(instances))
	for _, instance := range instances {
		item, ok := evidence[instance]
		if !ok || len(item.recipients) == 0 || len(item.eventNames) == 0 {
			continue
		}
		recipients := sortedFrontierRecipients(item.recipients)
		if len(recipients) == 0 {
			continue
		}
		proofs = append(proofs, store.RunForkSelectedContractDynamicTopologyProof{
			FlowInstance:      instance,
			SourceEventIDs:    sortedStringSet(item.sourceEventIDs),
			EventNames:        sortedStringSet(item.eventNames),
			DerivedRecipients: recipients,
			Disposition:       store.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	return proofs
}

type selectedContractDynamicTopologyEvidenceItem struct {
	sourceEventIDs map[string]struct{}
	eventNames     map[string]struct{}
	recipients     []store.RunForkContractFrontierRecipient
}

func selectedContractDynamicTopologyEvidence(frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission) map[string]*selectedContractDynamicTopologyEvidenceItem {
	out := map[string]*selectedContractDynamicTopologyEvidenceItem{}
	add := func(instance, sourceEventID, eventName string, recipients []store.RunForkContractFrontierRecipient) {
		instance = normalizeRouteInstance(instance)
		eventName = strings.TrimSpace(eventName)
		if instance == "" || eventName == "" {
			return
		}
		item := out[instance]
		if item == nil {
			item = &selectedContractDynamicTopologyEvidenceItem{
				sourceEventIDs: map[string]struct{}{},
				eventNames:     map[string]struct{}{},
			}
			out[instance] = item
		}
		if sourceEventID = strings.TrimSpace(sourceEventID); sourceEventID != "" {
			item.sourceEventIDs[sourceEventID] = struct{}{}
		}
		item.eventNames[eventName] = struct{}{}
		for _, recipient := range recipients {
			if normalizeRouteInstance(recipient.Path) != instance {
				continue
			}
			item.recipients = append(item.recipients, store.RunForkContractFrontierRecipient{
				SubscriberType: strings.TrimSpace(recipient.SubscriberType),
				SubscriberID:   strings.TrimSpace(recipient.SubscriberID),
				Path:           strings.TrimSpace(recipient.Path),
				RouteSource:    strings.TrimSpace(recipient.RouteSource),
			})
		}
	}
	for _, event := range frontier.FrontierEvents {
		for _, instance := range event.SourceFlowInstances {
			add(instance, event.SourceEventID, event.EventName, event.DerivedRecipients)
		}
	}
	for _, event := range routeAdmission.SelectedRouteEvents {
		for _, recipient := range event.DerivedRecipients {
			add(recipient.Path, event.SourceEventID, event.EventName, event.DerivedRecipients)
		}
	}
	return out
}

func normalizeRouteInstance(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}

func sortedStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func sortedTrimmedStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = normalizeRouteInstance(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	return sortedStringSet(seen)
}

func selectedContractRouteTopologyRequiredEvidence(routeAdmission store.RunForkSelectedContractRouteAdmission) []store.RunForkSelectedContractExecutionBoundary {
	evidence := []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_route_admission",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRouteAdmissionOwner,
			Reason:      "route topology consumes route admission as prerequisite evidence and does not duplicate its admission class",
		},
	}
	for _, item := range routeAdmission.RequiredConsumers {
		if strings.TrimSpace(item.Disposition) == store.RunForkSelectedContractDispositionPrerequisite {
			evidence = append(evidence, item)
		}
	}
	if len(routeAdmission.DynamicFlowInstances) > 0 {
		evidence = append(evidence, store.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_dynamic_route_topology",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractDynamicRouteTopologyOwner,
			Reason:      "dynamic flow-instance topology must be proven from fork-local selected-contract route evidence or remain fail-closed",
		})
	}
	return evidence
}

func selectedContractRouteTopologyRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_execution_model",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractExecutionModelOwner,
			Reason:      "selected-contract execution must consume route topology truth before future execution can derive recipients",
		},
		{
			Concept:     "fork_local_recipient_planning",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "recipient planning is a future consumer and must own executable selected-fork recipient evidence before delivery planning",
		},
	}
}

func selectedContractRouteTopologyBlockedSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "route topology is a non-mutating truth owner and does not persist fork-local route rows",
		},
		{
			Concept:     "dynamic_flow_instance_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			Reason:      "dynamic flow-instance route reconstruction needs fork-local topology evidence before route persistence",
		},
		{
			Concept:     "recipient_delivery_writes",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "delivery_and_replay_ownership",
			Reason:      "route topology does not derive executable recipients or create delivery rows",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains a separate scheduler lifecycle owner",
		},
	}
}

func selectedContractRouteTopologyInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "copy_source_routing_rules",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source routing_rules are current operational evidence, not fork-local topology truth",
		},
		{
			Concept:     "copy_source_flow_instance_routes",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source materialized route rows lack fork-local topology provenance",
		},
		{
			Concept:     "reuse_source_recipients",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source recipient decisions were made under source-run contracts and are not executable fork truth",
		},
		{
			Concept:     "delivery_planner_as_topology_owner",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "delivery planning is a future consumer and must not own route topology semantics",
		},
	}
}

func appendRunForkUnsupportedBlocker(blockers []store.RunForkUnsupportedBlocker, blocker store.RunForkUnsupportedBlocker) []store.RunForkUnsupportedBlocker {
	code := strings.TrimSpace(blocker.Code)
	if code == "" {
		return blockers
	}
	for _, existing := range blockers {
		if strings.TrimSpace(existing.Code) == code {
			return blockers
		}
	}
	blocker.Code = code
	return append(blockers, blocker)
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
			Concept:     "fork_local_recipient_planning",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "selected execution must consume canonical recipient-plan evidence before publish-path recipient derivation",
		},
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
			Concept:     "timer_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains a separate fork replay/resume blocker",
		},
		{
			Concept:     "mutating_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "route-history admission is non-mutating and must not persist routes or create executable recipients in this slice",
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
