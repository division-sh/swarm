package runforkadmission

import (
	"fmt"
	"sort"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

type ContractFrontierRequest struct {
	Plan              store.RunForkPlan
	Source            semanticview.Source
	ContractSelection store.RunForkContractSelection
}

func SelectedContractSelection(source semanticview.Source, contractsRoot string) store.RunForkContractSelection {
	selection := store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: strings.TrimSpace(contractsRoot),
	}
	if source != nil {
		selection.WorkflowName = strings.TrimSpace(source.WorkflowName())
		selection.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	return selection
}

func AdmitContractFrontier(req ContractFrontierRequest) (store.RunForkContractFrontierAdmission, error) {
	if req.Source == nil {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("selected contract semantic source is required")
	}
	selection := req.ContractSelection
	if strings.TrimSpace(selection.Mode) == "" {
		selection = SelectedContractSelection(req.Source, selection.ContractsRoot)
	}
	if strings.TrimSpace(selection.WorkflowName) == "" {
		selection.WorkflowName = strings.TrimSpace(req.Source.WorkflowName())
	}
	if strings.TrimSpace(selection.WorkflowVersion) == "" {
		selection.WorkflowVersion = strings.TrimSpace(req.Source.WorkflowVersion())
	}

	routeTable, err := runtimebus.DeriveRouteTable(req.Source)
	if err != nil {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract fork routes: %w", err)
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(req.Source)
	if err != nil {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract workflow nodes: %w", err)
	}

	frontier := runForkFrontierEvents(req.Plan.PendingWork)
	for i := range frontier {
		eventName := frontier[i].EventName
		frontier[i].RuntimeEventOwners = sortedUnique(req.Source.RuntimeEventOwners(eventName))
		frontier[i].WorkflowNodeSubscribers = workflowNodeSubscribers(workflowNodes, eventName)
		frontier[i].DerivedRecipients = contractFrontierRecipients(routeTable.Resolve(eventName))
	}
	sort.Slice(frontier, func(i, j int) bool {
		if frontier[i].EventName != frontier[j].EventName {
			return frontier[i].EventName < frontier[j].EventName
		}
		return frontier[i].SourceEventID < frontier[j].SourceEventID
	})

	blockers := []store.RunForkUnsupportedBlocker{}
	if len(frontier) > 0 {
		blockers = appendRunForkBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerContractFrontierExecutionUnsupported,
			Message: "selected-contract frontier admission is non-mutating; handler execution and fork-local delivery writes remain separately gated",
		})
	}
	for _, event := range frontier {
		if len(event.DerivedRecipients) > 0 || len(event.RuntimeEventOwners) > 0 || len(event.WorkflowNodeSubscribers) > 0 {
			continue
		}
		blockers = appendRunForkBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerContractFrontierRouteUnresolved,
			Message: "selected-contract frontier event has no derived route, workflow subscriber, or runtime event owner",
		})
	}

	return store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		ContractSelection:            selection,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		FrontierEventCount:           len(frontier),
		FrontierEvents:               frontier,
		UnsupportedBlockers:          blockers,
	}, nil
}

func runForkFrontierEvents(pending []store.RunForkPendingWork) []store.RunForkContractFrontierEvent {
	type aggregate struct {
		event           store.RunForkContractFrontierEvent
		classifications map[string]struct{}
		subscriberTypes map[string]struct{}
		subscriberIDs   map[string]struct{}
	}
	byEvent := map[string]*aggregate{}
	for _, item := range pending {
		if strings.TrimSpace(item.Classification) == store.RunForkPendingClassificationDeliveredCompleted {
			continue
		}
		eventID := strings.TrimSpace(item.EventID)
		if eventID == "" {
			continue
		}
		agg := byEvent[eventID]
		if agg == nil {
			agg = &aggregate{
				event: store.RunForkContractFrontierEvent{
					SourceEventID: eventID,
					EventName:     strings.TrimSpace(item.EventName),
				},
				classifications: map[string]struct{}{},
				subscriberTypes: map[string]struct{}{},
				subscriberIDs:   map[string]struct{}{},
			}
			byEvent[eventID] = agg
		}
		addString(agg.classifications, item.Classification)
		addString(agg.subscriberTypes, item.SubscriberType)
		addString(agg.subscriberIDs, item.SubscriberID)
	}
	out := make([]store.RunForkContractFrontierEvent, 0, len(byEvent))
	for _, agg := range byEvent {
		agg.event.SourceClassifications = sortedSet(agg.classifications)
		agg.event.SourceSubscriberTypes = sortedSet(agg.subscriberTypes)
		agg.event.SourceSubscriberIDs = sortedSet(agg.subscriberIDs)
		out = append(out, agg.event)
	}
	return out
}

func workflowNodeSubscribers(nodes []runtimepipeline.WorkflowNode, eventName string) []string {
	eventName = strings.TrimSpace(eventName)
	if eventName == "" {
		return nil
	}
	seen := map[string]struct{}{}
	for _, node := range nodes {
		for _, subscription := range node.Subscriptions {
			if strings.TrimSpace(string(subscription)) != eventName {
				continue
			}
			addString(seen, node.ID)
		}
	}
	return sortedSet(seen)
}

func contractFrontierRecipients(in []runtimebus.Subscriber) []store.RunForkContractFrontierRecipient {
	out := make([]store.RunForkContractFrontierRecipient, 0, len(in))
	seen := map[string]struct{}{}
	for _, subscriber := range in {
		recipient := store.RunForkContractFrontierRecipient{
			SubscriberType: strings.TrimSpace(subscriber.Type),
			SubscriberID:   strings.TrimSpace(subscriber.ID),
			Path:           strings.TrimSpace(subscriber.Path),
			RouteSource:    strings.TrimSpace(subscriber.RouteSource),
		}
		if recipient.SubscriberID == "" || recipient.SubscriberType == "" {
			continue
		}
		key := strings.Join([]string{recipient.SubscriberType, recipient.SubscriberID, recipient.Path, recipient.RouteSource}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, recipient)
	}
	sort.Slice(out, func(i, j int) bool {
		left := strings.Join([]string{out[i].SubscriberType, out[i].SubscriberID, out[i].Path, out[i].RouteSource}, "\x00")
		right := strings.Join([]string{out[j].SubscriberType, out[j].SubscriberID, out[j].Path, out[j].RouteSource}, "\x00")
		return left < right
	})
	return out
}

func appendRunForkBlocker(blockers []store.RunForkUnsupportedBlocker, blocker store.RunForkUnsupportedBlocker) []store.RunForkUnsupportedBlocker {
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
	blocker.Message = strings.TrimSpace(blocker.Message)
	return append(blockers, blocker)
}

func sortedUnique(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		addString(seen, value)
	}
	return sortedSet(seen)
}

func sortedSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func addString(values map[string]struct{}, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		values[value] = struct{}{}
	}
}
