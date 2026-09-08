package runforkexecution

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type SelectedContractRecipientPlanningRequest struct {
	Admission      runfork.RunForkContractFrontierAdmission
	RouteAdmission runfork.RunForkSelectedContractRouteAdmission
	RouteTopology  runfork.RunForkSelectedContractRouteTopology
}

func BuildSelectedContractRecipientPlanning(req SelectedContractRecipientPlanningRequest) (runfork.RunForkSelectedContractRecipientPlanning, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != runfork.RunForkContractFrontierAdmissionOwner {
		return runfork.RunForkSelectedContractRecipientPlanning{}, fmt.Errorf("selected-contract recipient planning requires %s admission; got %q", runfork.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return runfork.RunForkSelectedContractRecipientPlanning{}, fmt.Errorf("selected-contract recipient planning requires non-mutating frontier admission")
	}
	if admission.HistoricalExecutionSupported {
		return runfork.RunForkSelectedContractRecipientPlanning{}, fmt.Errorf("selected-contract recipient planning unexpectedly supports historical execution")
	}
	routeAdmission := req.RouteAdmission
	if err := validateSelectedContractRouteAdmission(admission, routeAdmission); err != nil {
		return runfork.RunForkSelectedContractRecipientPlanning{}, err
	}
	routeTopology := req.RouteTopology
	if err := validateSelectedContractRouteTopology(admission, routeAdmission, routeTopology); err != nil {
		return runfork.RunForkSelectedContractRecipientPlanning{}, err
	}
	return canonicalSelectedContractRecipientPlanning(admission, routeTopology)
}

func canonicalSelectedContractRecipientPlanning(frontier runfork.RunForkContractFrontierAdmission, routeTopology runfork.RunForkSelectedContractRouteTopology) (runfork.RunForkSelectedContractRecipientPlanning, error) {
	planEvents, err := selectedContractRecipientPlanEvents(frontier.FrontierEvents)
	if err != nil {
		return runfork.RunForkSelectedContractRecipientPlanning{}, err
	}
	blockers := []runfork.RunForkUnsupportedBlocker{{
		Code:    runfork.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
		Message: "selected-contract recipient planning is non-mutating; event append, delivery writes, and handler execution remain separately gated",
	}}
	for _, blocker := range routeTopology.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	return runfork.RunForkSelectedContractRecipientPlanning{
		Owner:                       runfork.RunForkSelectedContractRecipientPlanningOwner,
		RouteTopologyOwner:          routeTopology.Owner,
		RouteAdmissionOwner:         routeTopology.RouteAdmissionOwner,
		FutureExecutionOwner:        runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:                 true,
		RecipientPlanningSupported:  selectedContractRecipientPlanningSupported(blockers),
		DeliveryWritesSupported:     false,
		ContractSelection:           routeTopology.ContractSelection,
		FrontierEventCount:          routeTopology.FrontierEventCount,
		FrontierSourceEventIDs:      append([]string(nil), routeTopology.FrontierSourceEventIDs...),
		FrontierEvidenceFingerprint: routeTopology.FrontierEvidenceFingerprint,
		RecipientPlanEvents:         planEvents,
		RequiredEvidence:            selectedContractRecipientPlanningRequiredEvidence(routeTopology),
		RequiredConsumers:           selectedContractRecipientPlanningRequiredConsumers(),
		BlockedSiblings:             selectedContractRecipientPlanningBlockedSiblings(),
		InvalidPaths:                selectedContractRecipientPlanningInvalidPaths(),
		UnsupportedBlockers:         blockers,
	}, nil
}

func selectedContractRecipientPlanningSupported(blockers []runfork.RunForkUnsupportedBlocker) bool {
	for _, blocker := range blockers {
		switch strings.TrimSpace(blocker.Code) {
		case "", runfork.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
			runfork.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
			runfork.RunForkBlockerSelectedContractRouteTopologyNonMutating:
			continue
		default:
			return false
		}
	}
	return true
}

func selectedContractRecipientPlanEvents(events []runfork.RunForkContractFrontierEvent) ([]runfork.RunForkSelectedContractRecipientPlanEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}
	out := make([]runfork.RunForkSelectedContractRecipientPlanEvent, 0, len(events))
	for _, event := range events {
		recipients, err := sortedFrontierRecipients(event.DerivedRecipients)
		if err != nil {
			return nil, err
		}
		out = append(out, runfork.RunForkSelectedContractRecipientPlanEvent{
			SourceEventID: strings.TrimSpace(event.SourceEventID),
			EventName:     strings.TrimSpace(event.EventName),
			Recipients:    recipients,
			Disposition:   runfork.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceEventID != out[j].SourceEventID {
			return out[i].SourceEventID < out[j].SourceEventID
		}
		return out[i].EventName < out[j].EventName
	})
	return out, nil
}

func sortedFrontierRecipients(in []runfork.RunForkContractFrontierRecipient) ([]runfork.RunForkContractFrontierRecipient, error) {
	return forkrecipient.CanonicalSet(in)
}

func selectedContractRecipientPlanningRequiredEvidence(routeTopology runfork.RunForkSelectedContractRouteTopology) []runfork.RunForkSelectedContractExecutionBoundary {
	evidence := []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_route_topology",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractRouteTopologyOwner,
			Reason:      "recipient planning consumes canonical fork-local route topology before selected execution can publish fork work",
		},
		{
			Concept:     "selected_contract_binding",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractBindingOwner,
			Reason:      "recipient planning is selected-source specific and must remain bound to durable selected contract evidence",
		},
	}
	for _, item := range routeTopology.RequiredEvidence {
		if strings.TrimSpace(item.Disposition) == runfork.RunForkSelectedContractDispositionPrerequisite {
			evidence = append(evidence, item)
		}
	}
	return evidence
}

func selectedContractRecipientPlanningRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_execution_publish_path",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "selected execution consumes complete core/forkrecipient evidence through the fork-local runtime container before selected publication",
		},
		{
			Concept:     "eventbus_publish_recipient_guard",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "the live publish path validates effective typed delivery intents against selected recipient evidence after context projection and before delivery writes; diagnostics are not authority",
		},
	}
}

func selectedContractRecipientPlanningBlockedSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "fork_local_event_delivery_writes",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "recipient planning does not append events or create event_deliveries",
		},
		{
			Concept:     "handler_execution",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "recipient planning evidence is computed before handler execution",
		},
		{
			Concept:     "receipts_dead_letters_idempotency",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "outcome writes and suppressors remain separately gated",
		},
		{
			Concept:     "dynamic_flow_instance_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			Reason:      "dynamic topology remains fail-closed without fork-local topology proof",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains a separate scheduler lifecycle owner",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "session, turn, and audit reconstruction remain separately gated",
		},
	}
}

func selectedContractRecipientPlanningInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "source_route_rows_as_recipient_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source routing_rules and flow_instance_routes are not executable selected-fork recipient truth",
		},
		{
			Concept:     "source_event_deliveries_as_recipient_truth",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event_deliveries are source-run history and must not define selected-fork recipients",
		},
		{
			Concept:     "delivery_planner_as_canonical_owner",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "generic delivery planning may only be a downstream consumer guarded by recipient-plan evidence",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, retry state, and post-T outcomes cannot suppress selected-fork work",
		},
	}
}

func validateSelectedContractRecipientPlanning(frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission, routeTopology runfork.RunForkSelectedContractRouteTopology, planning runfork.RunForkSelectedContractRecipientPlanning) error {
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract execution requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	if strings.TrimSpace(planning.RouteTopologyOwner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract recipient planning must consume %s; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, planning.RouteTopologyOwner)
	}
	if strings.TrimSpace(planning.RouteAdmissionOwner) != runfork.RunForkSelectedContractRouteAdmissionOwner {
		return fmt.Errorf("selected-contract recipient planning must consume %s; got %q", runfork.RunForkSelectedContractRouteAdmissionOwner, planning.RouteAdmissionOwner)
	}
	if strings.TrimSpace(planning.FutureExecutionOwner) != runfork.RunForkSelectedContractExecutionOwner {
		return fmt.Errorf("selected-contract recipient planning must point to %s; got %q", runfork.RunForkSelectedContractExecutionOwner, planning.FutureExecutionOwner)
	}
	if !planning.NonMutating {
		return fmt.Errorf("selected-contract recipient planning must be non-mutating")
	}
	if planning.DeliveryWritesSupported {
		return fmt.Errorf("selected-contract recipient planning unexpectedly supports delivery writes")
	}
	if err := validateSelectionMatches("recipient planning", routeTopology.ContractSelection, planning.ContractSelection); err != nil {
		return err
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint, err := runfork.RunForkContractFrontierEvidenceBinding(frontier)
	if err != nil {
		return err
	}
	if planning.FrontierEventCount != frontierEventCount {
		return fmt.Errorf("selected-contract recipient planning frontier count mismatch: got %d want %d", planning.FrontierEventCount, frontierEventCount)
	}
	if !equalStringSlices(planning.FrontierSourceEventIDs, frontierSourceEventIDs) {
		return fmt.Errorf("selected-contract recipient planning frontier source event IDs do not match current frontier evidence")
	}
	if strings.TrimSpace(planning.FrontierEvidenceFingerprint) != frontierFingerprint {
		return fmt.Errorf("selected-contract recipient planning frontier fingerprint mismatch")
	}
	canonical, err := canonicalSelectedContractRecipientPlanning(frontier, routeTopology)
	if err != nil {
		return err
	}
	equal, err := runfork.EqualSelectedContractRecipientPlanning(planning, canonical)
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("selected-contract recipient planning does not match canonical route-topology evidence")
	}
	return validateSelectedContractRouteTopology(frontier, routeAdmission, routeTopology)
}

func validateSelectedContractRecipientPlanningForPublish(planning runfork.RunForkSelectedContractRecipientPlanning) error {
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract publish path requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	if !planning.NonMutating || planning.DeliveryWritesSupported {
		return fmt.Errorf("selected-contract publish path requires non-mutating recipient planning without delivery writes")
	}
	if !planning.RecipientPlanningSupported {
		return fmt.Errorf("selected-contract recipient planning is not supported for publish; blockers: %s", selectedContractBlockerCodes(planning.UnsupportedBlockers))
	}
	for _, blocker := range planning.UnsupportedBlockers {
		switch strings.TrimSpace(blocker.Code) {
		case "", runfork.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
			runfork.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
			runfork.RunForkBlockerSelectedContractRouteTopologyNonMutating:
			continue
		default:
			if msg := strings.TrimSpace(blocker.Message); msg != "" {
				return fmt.Errorf("%s: %s", blocker.Code, msg)
			}
			return fmt.Errorf("%s", blocker.Code)
		}
	}
	return nil
}

type selectedContractRecipientPlanPublishGuard struct {
	plansBySourceEvent map[string]runfork.RunForkSelectedContractRecipientPlanEvent
	sourceByForkEvent  map[string]string
	sourceAgents       map[string]struct{}
	semanticSource     semanticview.Source
	workflowProjection selectedContractWorkflowProjection
}

func newSelectedContractRecipientPlanPublishGuard(planning runfork.RunForkSelectedContractRecipientPlanning, source semanticview.Source, projection selectedContractWorkflowProjection, sourceAgents ...string) (*selectedContractRecipientPlanPublishGuard, error) {
	if err := validateSelectedContractRecipientPlanningForPublish(planning); err != nil {
		return nil, err
	}
	if err := projection.requireChildRun(projection.childRunID); err != nil {
		return nil, err
	}
	if len(sourceAgents) == 0 {
		sourceAgents = []string{runfork.RunForkSelectedContractExecutionOwner}
	}
	allowedAgents := map[string]struct{}{}
	for _, agent := range sourceAgents {
		agent = strings.TrimSpace(agent)
		if agent != "" {
			allowedAgents[agent] = struct{}{}
		}
	}
	if len(allowedAgents) == 0 {
		return nil, fmt.Errorf("selected-contract recipient planning publish guard requires source-agent owner")
	}
	plans := map[string]runfork.RunForkSelectedContractRecipientPlanEvent{}
	for _, event := range planning.RecipientPlanEvents {
		sourceEventID := strings.TrimSpace(event.SourceEventID)
		if sourceEventID == "" {
			continue
		}
		bound := event
		bound.Recipients = make([]runfork.RunForkContractFrontierRecipient, len(event.Recipients))
		for index, recipient := range event.Recipients {
			var err error
			bound.Recipients[index], err = projection.BindRecipient(sourceEventID, recipient)
			if err != nil {
				return nil, fmt.Errorf("bind selected-contract recipient for source event %s: %w", sourceEventID, err)
			}
		}
		plans[sourceEventID] = bound
	}
	return &selectedContractRecipientPlanPublishGuard{
		plansBySourceEvent: plans,
		sourceByForkEvent:  map[string]string{},
		sourceAgents:       allowedAgents,
		semanticSource:     source,
		workflowProjection: projection,
	}, nil
}

func (g *selectedContractRecipientPlanPublishGuard) ExpectForkEvent(forkEventID, sourceEventID string) {
	if g == nil {
		return
	}
	forkEventID = strings.TrimSpace(forkEventID)
	sourceEventID = strings.TrimSpace(sourceEventID)
	if forkEventID == "" || sourceEventID == "" {
		return
	}
	g.sourceByForkEvent[forkEventID] = sourceEventID
}

func (g *selectedContractRecipientPlanPublishGuard) AuthorizeEvent(ctx context.Context, evt events.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !g.authorizesEvent(evt) {
		return nil
	}
	_, _, err := g.expectedRecipientPlanEvent(evt)
	return err
}

func (g *selectedContractRecipientPlanPublishGuard) Authorize(ctx context.Context, evt events.Event, actual runtimebus.PublishRecipientPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !g.authorizesEvent(evt) {
		return nil
	}
	sourceEventID, expected, err := g.expectedRecipientPlanEvent(evt)
	if err != nil {
		return err
	}
	if len(actual.SubscriptionRecipients) > 0 {
		return fmt.Errorf("selected-contract publish path cannot use live subscriptions as fork recipient truth")
	}
	selected, err := forkrecipient.CanonicalSet(expected.Recipients)
	if err != nil {
		return err
	}
	actuals, err := actual.RecipientActuals()
	if err != nil {
		return err
	}
	remaining := make(map[forkrecipient.Key]forkrecipient.Evidence, len(selected))
	for _, recipient := range selected {
		key, err := recipient.Key()
		if err != nil {
			return err
		}
		remaining[key] = recipient
	}
	for _, actual := range actuals {
		projected, err := selectedEvidenceFromActual(g.semanticSource, evt, actual)
		if err != nil {
			return err
		}
		key, err := projected.Key()
		if err != nil {
			return err
		}
		want, exists := remaining[key]
		if !exists {
			return fmt.Errorf("selected-contract publish has an unselected execution authority for source event %s", sourceEventID)
		}
		if err := want.Satisfies(g.workflowProjection.childRunID, projected, actual.Route().AgentIdentity); err != nil {
			return fmt.Errorf("selected-contract recipient for source event %s: %w", sourceEventID, err)
		}
		delete(remaining, key)
	}
	if len(remaining) != 0 {
		return fmt.Errorf("selected-contract publish is missing %d selected execution authorities for source event %s", len(remaining), sourceEventID)
	}
	return nil
}

func selectedEvidenceFromActual(source semanticview.Source, evt events.Event, actual runtimebus.PublishRecipientActual) (forkrecipient.Evidence, error) {
	route := actual.Route()
	input := forkrecipient.Input{Recipient: route.Recipient, Path: route.Target.Route().FlowInstance}
	if route.Recipient.IsAgent() {
		plan, err := route.AgentIdentity.Plan()
		if err != nil {
			return forkrecipient.Evidence{}, err
		}
		input.AgentPlan = plan
		input.Path, err = selectedContractAgentRecipientPath(source, plan)
		if err != nil {
			return forkrecipient.Evidence{}, err
		}
		input.HandlerEvent = evt.Type()
	} else {
		input.HandlerNode = actual.Handler().Node()
		input.HandlerEvent, _ = actual.Handler().EventOverride()
	}
	if plan, connected := actual.ConnectPlan(); connected {
		pin, present := route.ConnectClaim.ReceiverIdentity()
		if !present {
			return forkrecipient.Evidence{}, fmt.Errorf("selected connect actual lacks receiver pin")
		}
		localEvent, present := route.ConnectClaim.ReceiverEvent()
		if !present {
			return forkrecipient.Evidence{}, fmt.Errorf("selected connect actual lacks receiver event")
		}
		if route.Recipient.IsNode() && input.HandlerEvent != localEvent {
			return forkrecipient.Evidence{}, fmt.Errorf("selected connect handler contradicts execution claim")
		}
		input.HandlerEvent = localEvent
		return forkrecipient.NewConnect(input, plan, pin)
	}
	return forkrecipient.NewLocal(input)
}

func (g *selectedContractRecipientPlanPublishGuard) MaterializeNodeDeliveryRoutes(ctx context.Context, evt events.Event, actual runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !g.authorizesEvent(evt) {
		return nil, nil
	}
	if err := g.AuthorizeEvent(ctx, evt); err != nil {
		return nil, err
	}
	_, expected, err := g.expectedRecipientPlanEvent(evt)
	if err != nil {
		return nil, err
	}
	return selectedContractNodeDeliveryRoutes(g.semanticSource, expected.EventName, expected.Recipients)
}

func (g *selectedContractRecipientPlanPublishGuard) authorizesEvent(event events.Event) bool {
	if g == nil {
		return false
	}
	if event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || event.ProducerType() != events.EventProducerPlatform {
		return false
	}
	_, ok := g.sourceAgents[event.Producer().ID()]
	return ok
}

func (g *selectedContractRecipientPlanPublishGuard) expectedRecipientPlanEvent(evt events.Event) (string, runfork.RunForkSelectedContractRecipientPlanEvent, error) {
	if err := g.workflowProjection.requireChildRun(evt.RunID()); err != nil {
		return "", runfork.RunForkSelectedContractRecipientPlanEvent{}, err
	}
	forkEventID := strings.TrimSpace(evt.ID())
	sourceEventID := strings.TrimSpace(g.sourceByForkEvent[forkEventID])
	if sourceEventID == "" {
		return "", runfork.RunForkSelectedContractRecipientPlanEvent{}, fmt.Errorf("selected-contract publish path missing %s evidence for fork event %s", runfork.RunForkSelectedContractRecipientPlanningOwner, forkEventID)
	}
	expected, ok := g.plansBySourceEvent[sourceEventID]
	if !ok {
		return "", runfork.RunForkSelectedContractRecipientPlanEvent{}, fmt.Errorf("selected-contract publish path has no recipient plan for source event %s", sourceEventID)
	}
	if strings.TrimSpace(expected.EventName) != strings.TrimSpace(string(evt.Type())) {
		return "", runfork.RunForkSelectedContractRecipientPlanEvent{}, fmt.Errorf("selected-contract publish event type mismatch for source event %s: got %q want %q", sourceEventID, evt.Type(), expected.EventName)
	}
	return sourceEventID, expected, nil
}

func selectedContractNodeDeliveryRoutes(source semanticview.Source, eventName string, in []runfork.RunForkContractFrontierRecipient) ([]runtimebus.DeliveryRouteBlueprint, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]runtimebus.DeliveryRouteBlueprint, 0, len(in))
	for _, recipient := range in {
		if err := recipient.Validate(); err != nil {
			return nil, err
		}
		if !recipient.Recipient.IsNode() {
			continue
		}
		if _, _, connected := recipient.Connect(); connected {
			return nil, fmt.Errorf("selected connect recipient requires canonical connect route production")
		}
		node, exact := recipient.Recipient.Node()
		if !exact {
			continue
		}
		id := node.Key()
		if source == nil {
			return nil, fmt.Errorf("selected-contract node recipient %s requires its admitted semantic source", id)
		}
		if _, ok := source.ExecutableNode(node); !ok {
			return nil, fmt.Errorf("selected-contract node recipient %s has no canonical declaration owner", id)
		}
		handler, err := runtimepipeline.AdmitDeliveryTargetHandler(source, node)
		if err != nil {
			return nil, fmt.Errorf("admit selected-contract handler for %s: %w", id, err)
		}
		route := runtimebus.DeliveryRouteBlueprint{
			Recipient: events.MustNodeDeliveryRecipient(node),
			Handler:   handler.ForEvent(recipient.HandlerEvent()),
		}
		if path := strings.Trim(strings.TrimSpace(recipient.Path), "/"); path != "" {
			route.Target.FlowInstance = path
		}
		out = append(out, route)
	}
	return out, nil
}
