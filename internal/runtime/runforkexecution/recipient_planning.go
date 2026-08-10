package runforkexecution

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
	return canonicalSelectedContractRecipientPlanning(admission, routeTopology), nil
}

func canonicalSelectedContractRecipientPlanning(frontier runfork.RunForkContractFrontierAdmission, routeTopology runfork.RunForkSelectedContractRouteTopology) runfork.RunForkSelectedContractRecipientPlanning {
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
		RecipientPlanEvents:         selectedContractRecipientPlanEvents(frontier.FrontierEvents),
		RequiredEvidence:            selectedContractRecipientPlanningRequiredEvidence(routeTopology),
		RequiredConsumers:           selectedContractRecipientPlanningRequiredConsumers(),
		BlockedSiblings:             selectedContractRecipientPlanningBlockedSiblings(),
		InvalidPaths:                selectedContractRecipientPlanningInvalidPaths(),
		UnsupportedBlockers:         blockers,
	}
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

func selectedContractRecipientPlanEvents(events []runfork.RunForkContractFrontierEvent) []runfork.RunForkSelectedContractRecipientPlanEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]runfork.RunForkSelectedContractRecipientPlanEvent, 0, len(events))
	for _, event := range events {
		out = append(out, runfork.RunForkSelectedContractRecipientPlanEvent{
			SourceEventID: strings.TrimSpace(event.SourceEventID),
			EventName:     strings.TrimSpace(event.EventName),
			Recipients:    sortedFrontierRecipients(event.DerivedRecipients),
			Disposition:   runfork.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceEventID != out[j].SourceEventID {
			return out[i].SourceEventID < out[j].SourceEventID
		}
		return out[i].EventName < out[j].EventName
	})
	return out
}

func sortedFrontierRecipients(in []runfork.RunForkContractFrontierRecipient) []runfork.RunForkContractFrontierRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]runfork.RunForkContractFrontierRecipient, 0, len(in))
	seen := map[frontierRecipientKey]struct{}{}
	for _, recipient := range in {
		recipient = runfork.NewRunForkContractFrontierRecipient(
			recipient.Recipient, recipient.Path, recipient.RouteSourceCode(), recipient.AgentIdentity,
		)
		if recipient.Recipient.Empty() {
			continue
		}
		key := recipientKey(recipient)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, recipient)
	}
	sort.Slice(out, func(i, j int) bool { return frontierRecipientLess(out[i], out[j]) })
	return out
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
			Reason:      "selected execution must consume recipient-plan evidence through the fork-local runtime container before EventBus.Publish can derive selected-fork recipients",
		},
		{
			Concept:     "eventbus_publish_recipient_guard",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "the live publish path remains a downstream consumer and must validate routed recipients against this owner before delivery writes",
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
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := runfork.RunForkContractFrontierEvidenceBinding(frontier)
	if planning.FrontierEventCount != frontierEventCount {
		return fmt.Errorf("selected-contract recipient planning frontier count mismatch: got %d want %d", planning.FrontierEventCount, frontierEventCount)
	}
	if !equalStringSlices(planning.FrontierSourceEventIDs, frontierSourceEventIDs) {
		return fmt.Errorf("selected-contract recipient planning frontier source event IDs do not match current frontier evidence")
	}
	if strings.TrimSpace(planning.FrontierEvidenceFingerprint) != frontierFingerprint {
		return fmt.Errorf("selected-contract recipient planning frontier fingerprint mismatch")
	}
	canonical := canonicalSelectedContractRecipientPlanning(frontier, routeTopology)
	if !reflect.DeepEqual(planning, canonical) {
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
}

func newSelectedContractRecipientPlanPublishGuard(planning runfork.RunForkSelectedContractRecipientPlanning, sourceAgents ...string) (*selectedContractRecipientPlanPublishGuard, error) {
	if err := validateSelectedContractRecipientPlanningForPublish(planning); err != nil {
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
		plans[sourceEventID] = event
	}
	return &selectedContractRecipientPlanPublishGuard{
		plansBySourceEvent: plans,
		sourceByForkEvent:  map[string]string{},
		sourceAgents:       allowedAgents,
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
	expectedKeys := expectedRecipientKeys(expected.Recipients)
	actualKeys := actualRecipientKeys(actual.RoutedRecipients)
	freshCreateProjection, err := hasFreshCreateRecipientProjection(actual)
	if err != nil {
		return err
	}
	if freshCreateProjection {
		// A fresh create projection proves that canonical lifecycle routing
		// replaced the source-run path with a new fork-local instance decision.
		expectedKeys = expectedRecipientIdentityKeys(expected.Recipients)
		actualKeys = actualRecipientIdentityKeys(actual.RoutedRecipients)
	}
	if !recipientKeysEqual(expectedKeys, actualKeys) {
		return fmt.Errorf("selected-contract publish routed recipients do not match %s for source event %s", runfork.RunForkSelectedContractRecipientPlanningOwner, sourceEventID)
	}
	return nil
}

func hasFreshCreateRecipientProjection(plan runtimebus.PublishRecipientPlan) (bool, error) {
	if len(plan.RoutedRecipients) == 0 || len(plan.DeliveryRoutes) == 0 {
		return false, nil
	}
	if err := events.ValidateDeliveryRouteProjections(plan.DeliveryRoutes); err != nil {
		return false, fmt.Errorf("selected-contract publish path has invalid delivery projection evidence: %w", err)
	}

	projectedPaths := make(map[events.DeliveryRecipient]string, len(plan.DeliveryRoutes))
	for _, route := range plan.DeliveryRoutes {
		route = route.Normalized()
		if route.PayloadProjection.Empty() {
			continue
		}
		key := route.Recipient
		path := route.Target.Route().FlowInstance
		if key.Empty() || path == "" {
			return false, nil
		}
		if previous, exists := projectedPaths[key]; exists && previous != path {
			return false, nil
		}
		projectedPaths[key] = path
	}
	if len(projectedPaths) == 0 {
		return false, nil
	}

	actualPaths := make(map[events.DeliveryRecipient]string, len(plan.RoutedRecipients))
	for _, recipient := range plan.RoutedRecipients {
		key, ok := deliveryRecipientFromReadback(recipient.Type, recipient.ID)
		path := strings.Trim(strings.TrimSpace(recipient.Path), "/")
		if !ok || path == "" {
			return false, nil
		}
		if previous, exists := actualPaths[key]; exists && previous != path {
			return false, nil
		}
		actualPaths[key] = path
	}
	if len(projectedPaths) != len(actualPaths) {
		return false, nil
	}
	for key, path := range actualPaths {
		if projectedPaths[key] != path {
			return false, nil
		}
	}
	return true, nil
}

func (g *selectedContractRecipientPlanPublishGuard) MaterializeNodeDeliveryRoutes(ctx context.Context, evt events.Event, actual runtimebus.PublishRecipientPlan) ([]runtimebus.DeliveryRouteBlueprint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !g.authorizesEvent(evt) {
		return nil, nil
	}
	if err := g.Authorize(ctx, evt, actual); err != nil {
		return nil, err
	}
	_, expected, err := g.expectedRecipientPlanEvent(evt)
	if err != nil {
		return nil, err
	}
	return selectedContractNodeDeliveryRoutes(expected.Recipients), nil
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

func selectedContractNodeDeliveryRoutes(in []runfork.RunForkContractFrontierRecipient) []runtimebus.DeliveryRouteBlueprint {
	if len(in) == 0 {
		return nil
	}
	out := make([]runtimebus.DeliveryRouteBlueprint, 0, len(in))
	for _, recipient := range in {
		if !recipient.Recipient.IsNode() {
			continue
		}
		id := recipient.Recipient.ID()
		if id == "" {
			continue
		}
		route := runtimebus.DeliveryRouteBlueprint{
			Recipient: events.MustNodeDeliveryRecipient(id),
		}
		if path := strings.Trim(strings.TrimSpace(recipient.Path), "/"); path != "" {
			route.Target.FlowInstance = path
		}
		out = append(out, route)
	}
	return out
}

func expectedRecipientKeys(in []runfork.RunForkContractFrontierRecipient) []frontierRecipientKey {
	out := make([]frontierRecipientKey, 0, len(in))
	for _, recipient := range in {
		recipient = runfork.NewRunForkContractFrontierRecipient(
			recipient.Recipient, recipient.Path, recipient.RouteSourceCode(), recipient.AgentIdentity,
		)
		if recipient.Recipient.Empty() {
			continue
		}
		out = append(out, recipientKey(recipient))
	}
	sort.Slice(out, func(i, j int) bool { return frontierRecipientKeyLess(out[i], out[j]) })
	return out
}

func actualRecipientKeys(in []runtimebus.PublishDiagnosticRecipient) []frontierRecipientKey {
	out := make([]frontierRecipientKey, 0, len(in))
	for _, recipient := range in {
		typedRecipient, ok := deliveryRecipientFromReadback(recipient.Type, recipient.ID)
		if !ok {
			continue
		}
		out = append(out, recipientKey(runfork.NewRunForkContractFrontierRecipient(
			typedRecipient, recipient.Path, recipient.RouteSource, agentidentity.Identity{},
		)))
	}
	sort.Slice(out, func(i, j int) bool { return frontierRecipientKeyLess(out[i], out[j]) })
	return out
}

func expectedRecipientIdentityKeys(in []runfork.RunForkContractFrontierRecipient) []frontierRecipientKey {
	out := make([]frontierRecipientKey, 0, len(in))
	for _, recipient := range in {
		if !recipient.Recipient.Empty() {
			out = append(out, frontierRecipientKey{recipient: recipient.Recipient})
		}
	}
	sort.Slice(out, func(i, j int) bool { return frontierRecipientKeyLess(out[i], out[j]) })
	return out
}

func actualRecipientIdentityKeys(in []runtimebus.PublishDiagnosticRecipient) []frontierRecipientKey {
	out := make([]frontierRecipientKey, 0, len(in))
	for _, recipient := range in {
		if typedRecipient, ok := deliveryRecipientFromReadback(recipient.Type, recipient.ID); ok {
			out = append(out, frontierRecipientKey{recipient: typedRecipient})
		}
	}
	sort.Slice(out, func(i, j int) bool { return frontierRecipientKeyLess(out[i], out[j]) })
	return out
}

func recipientKeysEqual(left, right []frontierRecipientKey) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

type frontierRecipientKey struct {
	recipient   events.DeliveryRecipient
	path        string
	routeSource string
}

func recipientKey(recipient runfork.RunForkContractFrontierRecipient) frontierRecipientKey {
	return frontierRecipientKey{
		recipient: recipient.Recipient,
		path:      strings.TrimSpace(recipient.Path), routeSource: recipient.RouteSourceCode(),
	}
}

func frontierRecipientKeyLess(left, right frontierRecipientKey) bool {
	if left.recipient.Code() != right.recipient.Code() {
		return left.recipient.Code() < right.recipient.Code()
	}
	if left.recipient.ID() != right.recipient.ID() {
		return left.recipient.ID() < right.recipient.ID()
	}
	if left.path != right.path {
		return left.path < right.path
	}
	return left.routeSource < right.routeSource
}

func frontierRecipientLess(left, right runfork.RunForkContractFrontierRecipient) bool {
	return frontierRecipientKeyLess(recipientKey(left), recipientKey(right))
}

func deliveryRecipientFromReadback(kind, id string) (events.DeliveryRecipient, bool) {
	var (
		recipient events.DeliveryRecipient
		err       error
	)
	switch strings.TrimSpace(kind) {
	case "node":
		recipient, err = events.NewNodeDeliveryRecipient(id)
	case "agent":
		recipient, err = events.NewAgentDeliveryRecipient(id)
	default:
		return events.DeliveryRecipient{}, false
	}
	return recipient, err == nil
}
