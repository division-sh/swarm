package runforkexecution

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store"
)

type SelectedContractRecipientPlanningRequest struct {
	Admission      store.RunForkContractFrontierAdmission
	RouteAdmission store.RunForkSelectedContractRouteAdmission
	RouteTopology  store.RunForkSelectedContractRouteTopology
}

func BuildSelectedContractRecipientPlanning(req SelectedContractRecipientPlanningRequest) (store.RunForkSelectedContractRecipientPlanning, error) {
	admission := req.Admission
	if strings.TrimSpace(admission.Owner) != store.RunForkContractFrontierAdmissionOwner {
		return store.RunForkSelectedContractRecipientPlanning{}, fmt.Errorf("selected-contract recipient planning requires %s admission; got %q", store.RunForkContractFrontierAdmissionOwner, admission.Owner)
	}
	if !admission.NonMutating {
		return store.RunForkSelectedContractRecipientPlanning{}, fmt.Errorf("selected-contract recipient planning requires non-mutating frontier admission")
	}
	if admission.HistoricalExecutionSupported {
		return store.RunForkSelectedContractRecipientPlanning{}, fmt.Errorf("selected-contract recipient planning unexpectedly supports historical execution")
	}
	routeAdmission := req.RouteAdmission
	if err := validateSelectedContractRouteAdmission(admission, routeAdmission); err != nil {
		return store.RunForkSelectedContractRecipientPlanning{}, err
	}
	routeTopology := req.RouteTopology
	if err := validateSelectedContractRouteTopology(admission, routeAdmission, routeTopology); err != nil {
		return store.RunForkSelectedContractRecipientPlanning{}, err
	}
	return canonicalSelectedContractRecipientPlanning(admission, routeTopology), nil
}

func canonicalSelectedContractRecipientPlanning(frontier store.RunForkContractFrontierAdmission, routeTopology store.RunForkSelectedContractRouteTopology) store.RunForkSelectedContractRecipientPlanning {
	blockers := []store.RunForkUnsupportedBlocker{{
		Code:    store.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
		Message: "selected-contract recipient planning is non-mutating; event append, delivery writes, and handler execution remain separately gated",
	}}
	for _, blocker := range routeTopology.UnsupportedBlockers {
		blockers = appendRunForkUnsupportedBlocker(blockers, blocker)
	}
	return store.RunForkSelectedContractRecipientPlanning{
		Owner:                       store.RunForkSelectedContractRecipientPlanningOwner,
		RouteTopologyOwner:          routeTopology.Owner,
		RouteAdmissionOwner:         routeTopology.RouteAdmissionOwner,
		FutureExecutionOwner:        store.RunForkSelectedContractExecutionOwner,
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

func selectedContractRecipientPlanningSupported(blockers []store.RunForkUnsupportedBlocker) bool {
	for _, blocker := range blockers {
		switch strings.TrimSpace(blocker.Code) {
		case "", store.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
			store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
			store.RunForkBlockerSelectedContractRouteTopologyNonMutating:
			continue
		default:
			return false
		}
	}
	return true
}

func selectedContractRecipientPlanEvents(events []store.RunForkContractFrontierEvent) []store.RunForkSelectedContractRecipientPlanEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]store.RunForkSelectedContractRecipientPlanEvent, 0, len(events))
	for _, event := range events {
		out = append(out, store.RunForkSelectedContractRecipientPlanEvent{
			SourceEventID: strings.TrimSpace(event.SourceEventID),
			EventName:     strings.TrimSpace(event.EventName),
			Recipients:    sortedFrontierRecipients(event.DerivedRecipients),
			Disposition:   store.RunForkSelectedContractDispositionForkLocalTruth,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := strings.Join([]string{out[i].SourceEventID, out[i].EventName}, "\x00")
		right := strings.Join([]string{out[j].SourceEventID, out[j].EventName}, "\x00")
		return left < right
	})
	return out
}

func sortedFrontierRecipients(in []store.RunForkContractFrontierRecipient) []store.RunForkContractFrontierRecipient {
	if len(in) == 0 {
		return nil
	}
	out := make([]store.RunForkContractFrontierRecipient, 0, len(in))
	seen := map[string]struct{}{}
	for _, recipient := range in {
		recipient = store.RunForkContractFrontierRecipient{
			SubscriberType: strings.TrimSpace(recipient.SubscriberType),
			SubscriberID:   strings.TrimSpace(recipient.SubscriberID),
			Path:           strings.TrimSpace(recipient.Path),
			RouteSource:    strings.TrimSpace(recipient.RouteSource),
		}
		if recipient.SubscriberType == "" || recipient.SubscriberID == "" {
			continue
		}
		key := recipientKey(recipient)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, recipient)
	}
	sort.Slice(out, func(i, j int) bool {
		return recipientKey(out[i]) < recipientKey(out[j])
	})
	return out
}

func selectedContractRecipientPlanningRequiredEvidence(routeTopology store.RunForkSelectedContractRouteTopology) []store.RunForkSelectedContractExecutionBoundary {
	evidence := []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_contract_route_topology",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractRouteTopologyOwner,
			Reason:      "recipient planning consumes canonical fork-local route topology before selected execution can publish fork work",
		},
		{
			Concept:     "selected_contract_binding",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractBindingOwner,
			Reason:      "recipient planning is selected-source specific and must remain bound to durable selected contract evidence",
		},
	}
	for _, item := range routeTopology.RequiredEvidence {
		if strings.TrimSpace(item.Disposition) == store.RunForkSelectedContractDispositionPrerequisite {
			evidence = append(evidence, item)
		}
	}
	return evidence
}

func selectedContractRecipientPlanningRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_execution_publish_path",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "selected execution must consume recipient-plan evidence through the fork-local runtime container before EventBus.Publish can derive selected-fork recipients",
		},
		{
			Concept:     "eventbus_publish_recipient_guard",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.EventBus.Publish",
			Reason:      "the live publish path remains a downstream consumer and must validate routed recipients against this owner before delivery writes",
		},
	}
}

func selectedContractRecipientPlanningBlockedSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "fork_local_event_delivery_writes",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "recipient planning does not append events or create event_deliveries",
		},
		{
			Concept:     "handler_execution",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractForkLocalRuntimeContainerOwner,
			Reason:      "recipient planning evidence is computed before handler execution",
		},
		{
			Concept:     "receipts_dead_letters_idempotency",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "outcome writes and suppressors remain separately gated",
		},
		{
			Concept:     "dynamic_flow_instance_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			Reason:      "dynamic topology remains fail-closed without fork-local topology proof",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction remains a separate scheduler lifecycle owner",
		},
		{
			Concept:     "sessions_turns_audits",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "session, turn, and audit reconstruction remain separately gated",
		},
	}
}

func selectedContractRecipientPlanningInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "source_route_rows_as_recipient_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source routing_rules and flow_instance_routes are not executable selected-fork recipient truth",
		},
		{
			Concept:     "source_event_deliveries_as_recipient_truth",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source event_deliveries are source-run history and must not define selected-fork recipients",
		},
		{
			Concept:     "delivery_planner_as_canonical_owner",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "generic delivery planning may only be a downstream consumer guarded by recipient-plan evidence",
		},
		{
			Concept:     "source_outcome_suppression",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source receipts, dead letters, retry state, and post-T outcomes cannot suppress selected-fork work",
		},
	}
}

func validateSelectedContractRecipientPlanning(frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission, routeTopology store.RunForkSelectedContractRouteTopology, planning store.RunForkSelectedContractRecipientPlanning) error {
	if strings.TrimSpace(planning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract execution requires %s; got %q", store.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	if strings.TrimSpace(planning.RouteTopologyOwner) != store.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract recipient planning must consume %s; got %q", store.RunForkSelectedContractRouteTopologyOwner, planning.RouteTopologyOwner)
	}
	if strings.TrimSpace(planning.RouteAdmissionOwner) != store.RunForkSelectedContractRouteAdmissionOwner {
		return fmt.Errorf("selected-contract recipient planning must consume %s; got %q", store.RunForkSelectedContractRouteAdmissionOwner, planning.RouteAdmissionOwner)
	}
	if strings.TrimSpace(planning.FutureExecutionOwner) != store.RunForkSelectedContractExecutionOwner {
		return fmt.Errorf("selected-contract recipient planning must point to %s; got %q", store.RunForkSelectedContractExecutionOwner, planning.FutureExecutionOwner)
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
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := store.RunForkContractFrontierEvidenceBinding(frontier)
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

func validateSelectedContractRecipientPlanningForPublish(planning store.RunForkSelectedContractRecipientPlanning) error {
	if strings.TrimSpace(planning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract publish path requires %s; got %q", store.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	if !planning.NonMutating || planning.DeliveryWritesSupported {
		return fmt.Errorf("selected-contract publish path requires non-mutating recipient planning without delivery writes")
	}
	if !planning.RecipientPlanningSupported {
		return fmt.Errorf("selected-contract recipient planning is not supported for publish; blockers: %s", selectedContractBlockerCodes(planning.UnsupportedBlockers))
	}
	for _, blocker := range planning.UnsupportedBlockers {
		switch strings.TrimSpace(blocker.Code) {
		case "", store.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
			store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
			store.RunForkBlockerSelectedContractRouteTopologyNonMutating:
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
	plansBySourceEvent map[string]store.RunForkSelectedContractRecipientPlanEvent
	sourceByForkEvent  map[string]string
	sourceAgents       map[string]struct{}
}

func newSelectedContractRecipientPlanPublishGuard(planning store.RunForkSelectedContractRecipientPlanning, sourceAgents ...string) (*selectedContractRecipientPlanPublishGuard, error) {
	if err := validateSelectedContractRecipientPlanningForPublish(planning); err != nil {
		return nil, err
	}
	if len(sourceAgents) == 0 {
		sourceAgents = []string{store.RunForkSelectedContractExecutionOwner}
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
	plans := map[string]store.RunForkSelectedContractRecipientPlanEvent{}
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
	if !g.authorizesSourceAgent(evt.SourceAgent()) {
		return nil
	}
	_, _, err := g.expectedRecipientPlanEvent(evt)
	return err
}

func (g *selectedContractRecipientPlanPublishGuard) Authorize(ctx context.Context, evt events.Event, actual runtimebus.PublishRecipientPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !g.authorizesSourceAgent(evt.SourceAgent()) {
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
	if actual.UsesCanonicalRouteAuthority() {
		// Canonical connect routing owns fork-local instance selection. The
		// source plan authorizes the subscriber identity, never the source-run
		// concrete path that create must replace with a fresh fork decision.
		expectedKeys = expectedRecipientIdentityKeys(expected.Recipients)
		actualKeys = actualRecipientIdentityKeys(actual.RoutedRecipients)
	}
	if !recipientKeysEqual(expectedKeys, actualKeys) {
		return fmt.Errorf("selected-contract publish routed recipients do not match %s for source event %s", store.RunForkSelectedContractRecipientPlanningOwner, sourceEventID)
	}
	return nil
}

func (g *selectedContractRecipientPlanPublishGuard) MaterializeNodeDeliveryRoutes(ctx context.Context, evt events.Event, actual runtimebus.PublishRecipientPlan) ([]events.DeliveryRoute, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !g.authorizesSourceAgent(evt.SourceAgent()) {
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

func (g *selectedContractRecipientPlanPublishGuard) authorizesSourceAgent(sourceAgent string) bool {
	if g == nil {
		return false
	}
	_, ok := g.sourceAgents[strings.TrimSpace(sourceAgent)]
	return ok
}

func (g *selectedContractRecipientPlanPublishGuard) expectedRecipientPlanEvent(evt events.Event) (string, store.RunForkSelectedContractRecipientPlanEvent, error) {
	forkEventID := strings.TrimSpace(evt.ID())
	sourceEventID := strings.TrimSpace(g.sourceByForkEvent[forkEventID])
	if sourceEventID == "" {
		return "", store.RunForkSelectedContractRecipientPlanEvent{}, fmt.Errorf("selected-contract publish path missing %s evidence for fork event %s", store.RunForkSelectedContractRecipientPlanningOwner, forkEventID)
	}
	expected, ok := g.plansBySourceEvent[sourceEventID]
	if !ok {
		return "", store.RunForkSelectedContractRecipientPlanEvent{}, fmt.Errorf("selected-contract publish path has no recipient plan for source event %s", sourceEventID)
	}
	if strings.TrimSpace(expected.EventName) != strings.TrimSpace(string(evt.Type())) {
		return "", store.RunForkSelectedContractRecipientPlanEvent{}, fmt.Errorf("selected-contract publish event type mismatch for source event %s: got %q want %q", sourceEventID, evt.Type(), expected.EventName)
	}
	return sourceEventID, expected, nil
}

func selectedContractNodeDeliveryRoutes(in []store.RunForkContractFrontierRecipient) []events.DeliveryRoute {
	if len(in) == 0 {
		return nil
	}
	out := make([]events.DeliveryRoute, 0, len(in))
	for _, recipient := range in {
		if strings.TrimSpace(recipient.SubscriberType) != "node" {
			continue
		}
		id := strings.TrimSpace(recipient.SubscriberID)
		if id == "" {
			continue
		}
		route := events.DeliveryRoute{
			SubscriberType: "node",
			SubscriberID:   id,
		}
		if path := strings.Trim(strings.TrimSpace(recipient.Path), "/"); path != "" {
			route.Target.FlowInstance = path
		}
		out = append(out, route)
	}
	return events.NormalizeDeliveryRoutes(out)
}

func expectedRecipientKeys(in []store.RunForkContractFrontierRecipient) []string {
	out := make([]string, 0, len(in))
	for _, recipient := range in {
		recipient = store.RunForkContractFrontierRecipient{
			SubscriberType: strings.TrimSpace(recipient.SubscriberType),
			SubscriberID:   strings.TrimSpace(recipient.SubscriberID),
			Path:           strings.TrimSpace(recipient.Path),
			RouteSource:    strings.TrimSpace(recipient.RouteSource),
		}
		if recipient.SubscriberType == "" || recipient.SubscriberID == "" {
			continue
		}
		out = append(out, recipientKey(recipient))
	}
	sort.Strings(out)
	return out
}

func actualRecipientKeys(in []runtimebus.PublishDiagnosticRecipient) []string {
	out := make([]string, 0, len(in))
	for _, recipient := range in {
		if strings.TrimSpace(recipient.Type) == "" || strings.TrimSpace(recipient.ID) == "" {
			continue
		}
		out = append(out, recipientKey(store.RunForkContractFrontierRecipient{
			SubscriberType: recipient.Type,
			SubscriberID:   recipient.ID,
			Path:           recipient.Path,
			RouteSource:    recipient.RouteSource,
		}))
	}
	sort.Strings(out)
	return out
}

func expectedRecipientIdentityKeys(in []store.RunForkContractFrontierRecipient) []string {
	out := make([]string, 0, len(in))
	for _, recipient := range in {
		if key := recipientIdentityKey(recipient.SubscriberType, recipient.SubscriberID); key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func actualRecipientIdentityKeys(in []runtimebus.PublishDiagnosticRecipient) []string {
	out := make([]string, 0, len(in))
	for _, recipient := range in {
		if key := recipientIdentityKey(recipient.Type, recipient.ID); key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func recipientIdentityKey(subscriberType, subscriberID string) string {
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if subscriberType == "" || subscriberID == "" {
		return ""
	}
	return subscriberType + "\x00" + subscriberID
}

func recipientKeysEqual(left, right []string) bool {
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

func recipientKey(recipient store.RunForkContractFrontierRecipient) string {
	return strings.Join([]string{
		strings.TrimSpace(recipient.SubscriberType),
		strings.TrimSpace(recipient.SubscriberID),
		strings.TrimSpace(recipient.Path),
		strings.TrimSpace(recipient.RouteSource),
	}, "\x00")
}
