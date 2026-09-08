package runforkexecution

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type SelectedContractExecutionModelRequest struct {
	Admission      runfork.RunForkContractFrontierAdmission
	RouteAdmission runfork.RunForkSelectedContractRouteAdmission
	RouteTopology  runfork.RunForkSelectedContractRouteTopology
}

type SelectedContractRouteTopologyRequest struct {
	Admission      runfork.RunForkContractFrontierAdmission
	RouteAdmission runfork.RunForkSelectedContractRouteAdmission
}

func BuildSelectedContractRouteTopology(req SelectedContractRouteTopologyRequest) (runfork.RunForkSelectedContractRouteTopology, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != runfork.RunForkContractFrontierAdmissionOwner {
		return runfork.RunForkSelectedContractRouteTopology{}, fmt.Errorf("selected-contract route topology requires %s admission; got %q", runfork.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return runfork.RunForkSelectedContractRouteTopology{}, fmt.Errorf("selected-contract route topology requires non-mutating frontier admission")
	}
	if admission.HistoricalExecutionSupported {
		return runfork.RunForkSelectedContractRouteTopology{}, fmt.Errorf("selected-contract route topology unexpectedly supports historical execution")
	}
	routeAdmission := req.RouteAdmission
	if err := validateSelectedContractRouteAdmission(admission, routeAdmission); err != nil {
		return runfork.RunForkSelectedContractRouteTopology{}, err
	}
	return canonicalSelectedContractRouteTopology(admission, routeAdmission), nil
}

func canonicalSelectedContractRouteTopology(frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission) runfork.RunForkSelectedContractRouteTopology {
	blockers := []runfork.RunForkUnsupportedBlocker{{
		Code:    runfork.RunForkBlockerSelectedContractRouteTopologyNonMutating,
		Message: "selected-contract route topology is non-mutating; route persistence, recipient delivery writes, and handler execution remain separately gated",
	}}
	dynamicDisposition := runfork.RunForkSelectedContractDispositionForkLocalTruth
	dynamicFlowInstances := sortedTrimmedStrings(routeAdmission.DynamicFlowInstances)
	dynamicProofs := selectedContractDynamicRouteTopologyProofs(frontier, routeAdmission, dynamicFlowInstances)
	dynamicSupported := len(dynamicFlowInstances) == 0 || len(dynamicProofs) == len(dynamicFlowInstances)
	if len(dynamicFlowInstances) > 0 && !dynamicSupported {
		dynamicDisposition = runfork.RunForkSelectedContractDispositionFailClosed
		blockers = appendRunForkUnsupportedBlocker(blockers, runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven,
			Message: "selected-contract dynamic flow-instance topology requires fork-local topology evidence before route reconstruction",
		})
	}
	for _, blocker := range routeAdmission.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == runfork.RunForkBlockerFlowRouteHistoryUnproven &&
			routeAdmission.SourceRouteFactsPresent && dynamicSupported {
			continue
		}
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}

	staticEvents := selectedContractRouteTopologyEvents(routeAdmission.SelectedRouteEvents)
	return runfork.RunForkSelectedContractRouteTopology{
		Owner:                          runfork.RunForkSelectedContractRouteTopologyOwner,
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

func BuildSelectedContractExecutionModel(req SelectedContractExecutionModelRequest) (runfork.RunForkSelectedContractExecution, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != runfork.RunForkContractFrontierAdmissionOwner {
		return runfork.RunForkSelectedContractExecution{}, fmt.Errorf("selected-contract execution model requires %s admission; got %q", runfork.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return runfork.RunForkSelectedContractExecution{}, fmt.Errorf("selected-contract frontier admission must be non-mutating")
	}
	if admission.HistoricalExecutionSupported {
		return runfork.RunForkSelectedContractExecution{}, fmt.Errorf("selected-contract frontier admission unexpectedly supports historical execution")
	}
	routeTopology := req.RouteTopology
	routeAdmission := req.RouteAdmission
	if err := validateSelectedContractRouteAdmission(admission, routeAdmission); err != nil {
		return runfork.RunForkSelectedContractExecution{}, err
	}
	if err := validateSelectedContractRouteTopology(admission, routeAdmission, routeTopology); err != nil {
		return runfork.RunForkSelectedContractExecution{}, err
	}
	recipientPlanning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		return runfork.RunForkSelectedContractExecution{}, err
	}

	unsupportedBlockers := append([]runfork.RunForkUnsupportedBlocker(nil), admission.UnsupportedBlockers...)
	for _, blocker := range routeTopology.UnsupportedBlockers {
		unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, blocker)
	}
	for _, blocker := range recipientPlanning.UnsupportedBlockers {
		unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, blocker)
	}
	unsupportedBlockers = appendRunForkUnsupportedBlocker(unsupportedBlockers, runfork.RunForkUnsupportedBlocker{
		Code:    runfork.RunForkBlockerSelectedContractExecutionModelNonMutating,
		Message: "selected-contract fork execution is model-only; executable fork work remains separately gated",
	})

	return runfork.RunForkSelectedContractExecution{
		Owner:                runfork.RunForkSelectedContractExecutionModelOwner,
		FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:          true,
		ExecutionSupported:   false,
		ContractSelection:    admission.ContractSelection,
		AdmissionOwner:       admission.Owner,
		AdmissionUse:         runfork.RunForkSelectedContractExecutionAdmissionUseEvidenceOnly,
		FrontierEventCount:   admission.FrontierEventCount,
		FrontierEvents:       selectedContractFrontierEvents(admission.FrontierEvents),
		RouteTopology:        &routeTopology,
		RecipientPlanning:    &recipientPlanning,
		ContractBinding: runfork.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_binding",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractBindingOwner,
			Reason:      "future execution must consume the durable selected contract source bound to the fork run before handlers run",
		},
		RequiredConsumers:   selectedContractExecutionRequiredConsumers(),
		BlockedSiblings:     selectedContractExecutionBlockedSiblings(),
		InvalidPaths:        selectedContractExecutionInvalidPaths(),
		UnsupportedBlockers: unsupportedBlockers,
	}, nil
}

func validateSelectedContractRouteTopology(frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission, topology runfork.RunForkSelectedContractRouteTopology) error {
	if strings.TrimSpace(topology.Owner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract execution model requires %s route topology; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, topology.Owner)
	}
	if strings.TrimSpace(topology.RouteAdmissionOwner) != runfork.RunForkSelectedContractRouteAdmissionOwner {
		return fmt.Errorf("selected-contract route topology must consume %s; got %q", runfork.RunForkSelectedContractRouteAdmissionOwner, topology.RouteAdmissionOwner)
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
	if strings.TrimSpace(topology.FrontierAdmissionOwner) != runfork.RunForkContractFrontierAdmissionOwner {
		return fmt.Errorf("selected-contract route topology must consume %s; got %q", runfork.RunForkContractFrontierAdmissionOwner, topology.FrontierAdmissionOwner)
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := runfork.RunForkContractFrontierEvidenceBinding(frontier)
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

func validateSelectedContractRouteAdmission(frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission) error {
	if strings.TrimSpace(routeAdmission.Owner) != runfork.RunForkSelectedContractRouteAdmissionOwner {
		return fmt.Errorf("selected-contract execution model requires %s route admission; got %q", runfork.RunForkSelectedContractRouteAdmissionOwner, routeAdmission.Owner)
	}
	if !routeAdmission.NonMutating {
		return fmt.Errorf("selected-contract route admission must be non-mutating")
	}
	if routeAdmission.RouteReconstructionSupported {
		return fmt.Errorf("selected-contract route admission unexpectedly supports route reconstruction")
	}
	if strings.TrimSpace(routeAdmission.FrontierAdmissionOwner) != runfork.RunForkContractFrontierAdmissionOwner {
		return fmt.Errorf("selected-contract route admission must consume %s; got %q", runfork.RunForkContractFrontierAdmissionOwner, routeAdmission.FrontierAdmissionOwner)
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := runfork.RunForkContractFrontierEvidenceBinding(frontier)
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

func selectedContractRouteTopologyEvents(events []runfork.RunForkSelectedContractRouteEvent) []runfork.RunForkSelectedContractRouteEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]runfork.RunForkSelectedContractRouteEvent, 0, len(events))
	for _, event := range events {
		out = append(out, runfork.RunForkSelectedContractRouteEvent{
			SourceEventID:     event.SourceEventID,
			EventName:         event.EventName,
			DerivedRecipients: append([]runfork.RunForkContractFrontierRecipient(nil), event.DerivedRecipients...),
			Disposition:       runfork.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	return out
}

func selectedContractDynamicRouteTopologyOwner(instances []string) string {
	if len(instances) == 0 {
		return ""
	}
	return runfork.RunForkSelectedContractDynamicRouteTopologyOwner
}

func selectedContractDynamicRouteTopologyProofs(frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission, instances []string) []runfork.RunForkSelectedContractDynamicTopologyProof {
	if len(instances) == 0 {
		return nil
	}
	evidence := selectedContractDynamicTopologyEvidence(frontier, routeAdmission)
	proofs := make([]runfork.RunForkSelectedContractDynamicTopologyProof, 0, len(instances))
	for _, instance := range instances {
		item, ok := evidence[instance]
		if !ok || !item.hasFrontierFlowInstance || len(item.recipients) == 0 || len(item.eventNames) == 0 {
			continue
		}
		recipients := sortedFrontierRecipients(item.recipients)
		if len(recipients) == 0 {
			continue
		}
		proofs = append(proofs, runfork.RunForkSelectedContractDynamicTopologyProof{
			FlowInstance:      instance,
			SourceEventIDs:    sortedStringSet(item.sourceEventIDs),
			EventNames:        sortedStringSet(item.eventNames),
			DerivedRecipients: recipients,
			Disposition:       runfork.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	return proofs
}

type selectedContractDynamicTopologyEvidenceItem struct {
	hasFrontierFlowInstance bool
	sourceEventIDs          map[string]struct{}
	eventNames              map[string]struct{}
	recipients              []runfork.RunForkContractFrontierRecipient
}

func selectedContractDynamicTopologyEvidence(frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission) map[string]*selectedContractDynamicTopologyEvidenceItem {
	out := map[string]*selectedContractDynamicTopologyEvidenceItem{}
	add := func(instance, sourceEventID, eventName string, hasFrontierFlowInstance bool, recipients []runfork.RunForkContractFrontierRecipient) {
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
		if hasFrontierFlowInstance {
			item.hasFrontierFlowInstance = true
		}
		item.eventNames[eventName] = struct{}{}
		for _, recipient := range recipients {
			if normalizeRouteInstance(recipient.Path) != instance {
				continue
			}
			item.recipients = append(item.recipients, runfork.NewRunForkContractFrontierRecipient(
				recipient.Recipient, recipient.Path, recipient.RouteSourceCode(), recipient.AgentPlan,
			))
		}
	}
	for _, event := range frontier.FrontierEvents {
		for _, instance := range event.SourceFlowInstances {
			add(instance, event.SourceEventID, event.EventName, true, event.DerivedRecipients)
		}
	}
	for _, event := range routeAdmission.SelectedRouteEvents {
		for _, recipient := range event.DerivedRecipients {
			add(recipient.Path, event.SourceEventID, event.EventName, false, event.DerivedRecipients)
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

func selectedContractRouteTopologyRequiredEvidence(routeAdmission runfork.RunForkSelectedContractRouteAdmission) []runfork.RunForkSelectedContractExecutionBoundary {
	evidence := []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_route_admission",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRouteAdmissionOwner,
			Reason:      "route topology consumes route admission as prerequisite evidence and does not duplicate its admission class",
		},
	}
	for _, item := range routeAdmission.RequiredConsumers {
		if strings.TrimSpace(item.Disposition) == runfork.RunForkSelectedContractDispositionPrerequisite {
			evidence = append(evidence, item)
		}
	}
	if len(routeAdmission.DynamicFlowInstances) > 0 {
		evidence = append(evidence, runfork.RunForkSelectedContractExecutionBoundary{
			Concept:     "selected_contract_dynamic_route_topology",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractDynamicRouteTopologyOwner,
			Reason:      "dynamic flow-instance topology must be proven from fork-local selected-contract route evidence or remain fail-closed",
		})
	}
	return evidence
}

func selectedContractRouteTopologyRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_execution_model",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractExecutionModelOwner,
			Reason:      "selected-contract execution must consume route topology truth before future execution can derive recipients",
		},
		{
			Concept:     "fork_local_recipient_planning",
			Disposition: runfork.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "recipient planning is a future consumer and must own executable selected-fork recipient evidence before delivery planning",
		},
	}
}

func selectedContractRouteTopologyBlockedSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "route topology is a non-mutating truth owner and does not persist fork-local route rows",
		},
		{
			Concept:     "dynamic_flow_instance_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			Reason:      "dynamic flow-instance route reconstruction needs fork-local topology evidence before route persistence",
		},
		{
			Concept:     "recipient_delivery_writes",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "delivery_and_replay_ownership",
			Reason:      "route topology does not derive executable recipients or create delivery rows",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains a separate scheduler lifecycle owner",
		},
	}
}

func selectedContractRouteTopologyInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "copy_source_routing_rules",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source routing_rules are current operational evidence, not fork-local topology truth",
		},
		{
			Concept:     "copy_source_flow_instance_routes",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source materialized route rows lack fork-local topology provenance",
		},
		{
			Concept:     "reuse_source_recipients",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source recipient decisions were made under source-run contracts and are not executable fork truth",
		},
		{
			Concept:     "delivery_planner_as_topology_owner",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "delivery planning is a future consumer and must not own route topology semantics",
		},
	}
}

func appendRunForkUnsupportedBlocker(blockers []runfork.RunForkUnsupportedBlocker, blocker runfork.RunForkUnsupportedBlocker) []runfork.RunForkUnsupportedBlocker {
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

func selectedContractFrontierEvents(events []runfork.RunForkContractFrontierEvent) []runfork.RunForkSelectedContractFrontierEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]runfork.RunForkSelectedContractFrontierEvent, 0, len(events))
	for _, event := range events {
		out = append(out, runfork.RunForkSelectedContractFrontierEvent{
			SourceEventID:           event.SourceEventID,
			EventName:               event.EventName,
			RuntimeEventOwners:      append([]string(nil), event.RuntimeEventOwners...),
			WorkflowNodeSubscribers: append([]string(nil), event.WorkflowNodeSubscribers...),
			DerivedRecipients:       append([]runfork.RunForkContractFrontierRecipient(nil), event.DerivedRecipients...),
			Disposition:             runfork.RunForkSelectedContractDispositionEvidenceOnly,
		})
	}
	return out
}

func selectedContractExecutionRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "fork_local_recipient_planning",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "selected execution must consume canonical recipient-plan evidence before publish-path recipient derivation",
		},
		{
			Concept:     "authoritative_agent_delivery_materialization",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
			Reason:      "selected execution must prove selected-fork agent handler materialization for authoritative agent recipients or fail closed before fork mutation",
		},
		{
			Concept:     "fork_local_runtime_container",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "selected execution constructs the canonical fork-local runtime container that consumes recipient planning, handler materialization, EventBus guards, lineage policy, cleanup, and quiescence",
		},
		{
			Concept:     "fork_run_id_runtime_context",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "the fork-local runtime container binds EventBus, AgentManager, RuntimeLogger, and handler execution to the fork run_id rather than the source run_id",
		},
		{
			Concept:     "fork_local_runtime_typed_lineage",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner,
			Reason:      "selected execution must carry typed runtime lineage for selected-fork diagnostics and platform events before compiling that model down to persisted ParentEventID/source_event_id truth",
		},
		{
			Concept:     "fork_local_event_delivery_writes",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "selected execution creates fresh fork-local event and delivery rows through the runtime container instead of copying source rows",
		},
		{
			Concept:     "handler_execution",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "normal selected-fork handler execution runs inside the fork-local runtime container, not as an unowned runtime side effect",
		},
		{
			Concept:     "receipts_dead_letters_idempotency",
			Disposition: runfork.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       "internal/store/internal/backend/eventpersistence/event_receipt_side_effects.go+internal/runtime/deadletters",
			Reason:      "future execution must write fork-local outcomes and must not use source outcomes as suppressors without an approved model",
		},
		{
			Concept:     "emitted_follow_up_events",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "normal selected-fork follow-up events are regenerated under the fork run_id through the runtime container and bus; retry/idempotency policy remains split",
		},
		{
			Concept:     "safe_agent_delivery_event_replay",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkDeliveryEventReplayOwner,
			Reason:      "safe pending-agent replay remains a sibling pattern for fresh IDs and lineage, not the selected-contract execution owner",
		},
	}
}

func selectedContractExecutionBlockedSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "node_system_non_agent_execution",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractExecutionOwner,
			Reason:      "node/system execution requires a later mutating owner and remains blocked here",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains a separate fork replay/resume blocker",
		},
		{
			Concept:     "mutating_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "route-history admission is non-mutating and must not persist routes or create executable recipients in this slice",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "session, turn, and audit reconstruction remain separately gated",
		},
		{
			Concept:     "source_advanced_after_fork_point",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "source advancement remains fail-closed until a branch/suppression policy is approved",
		},
		{
			Concept:     "contract_swap_boot_resume",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "full selected-contract boot/resume execution remains outside this non-mutating model",
		},
		{
			Concept:     "dashboard_ui",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "operator UI is a later consumer and must not become the execution owner",
		},
	}
}

func selectedContractExecutionInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "copy_source_event_deliveries",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source deliveries are lineage/blocker evidence, not executable fork work",
		},
		{
			Concept:     "copy_source_events",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "fork events require fresh fork-local event IDs and lineage",
		},
		{
			Concept:     "cli_owned_execution",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "CLI may consume the model but must not own selected-contract execution semantics",
		},
		{
			Concept:     "same_run_outbox_replay_as_fork_replay",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "same-run recovery does not define fixed-event fork selected-contract replay ownership",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, and post-T outcomes cannot suppress fork-local work without an approved model",
		},
	}
}
