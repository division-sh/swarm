package pinrouting

import (
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ConnectRoutePlanDelivery string

const (
	ConnectDeliveryOne       ConnectRoutePlanDelivery = "one"
	ConnectDeliveryMany      ConnectRoutePlanDelivery = "many"
	ConnectDeliveryBroadcast ConnectRoutePlanDelivery = "broadcast"
	ConnectDeliveryReply     ConnectRoutePlanDelivery = "reply"
)

type ConnectRoutePlanTargetKind string

const (
	ConnectTargetKindTarget    ConnectRoutePlanTargetKind = "target"
	ConnectTargetKindTargetSet ConnectRoutePlanTargetKind = "target_set"
	ConnectTargetKindReply     ConnectRoutePlanTargetKind = "reply"
)

type ConnectRoutePlanFailure string

const (
	ConnectFailureSourceMissing              ConnectRoutePlanFailure = "source_missing"
	ConnectFailurePinRefInvalid              ConnectRoutePlanFailure = "connect_pin_ref_invalid"
	ConnectFailureProducerFlowMissing        ConnectRoutePlanFailure = "producer_flow_missing"
	ConnectFailureProducerOutputPinMissing   ConnectRoutePlanFailure = "producer_output_pin_missing"
	ConnectFailureReceiverFlowMissing        ConnectRoutePlanFailure = "receiver_flow_missing"
	ConnectFailureReceiverInputPinMissing    ConnectRoutePlanFailure = "receiver_input_pin_missing"
	ConnectFailureReceiverAddressRuleMissing ConnectRoutePlanFailure = "receiver_address_rule_missing"
	ConnectFailureDeliveryTopologyInvalid    ConnectRoutePlanFailure = "delivery_topology_invalid"
	ConnectFailureReplyLineageMissing        ConnectRoutePlanFailure = "reply_lineage_missing"
	ConnectFailureAddressValueMissing        ConnectRoutePlanFailure = "route_plan_address_value_missing"
	ConnectFailureTargetUnsupported          ConnectRoutePlanFailure = "route_plan_target_unsupported"
	ConnectFailureTargetUnresolved           ConnectRoutePlanFailure = "route_plan_target_unresolved"
	ConnectFailureTargetAmbiguous            ConnectRoutePlanFailure = "route_plan_target_ambiguous"
)

type ConnectRoutePlanEndpoint struct {
	FlowID        string
	FlowPath      string
	Mode          string
	Pin           string
	Event         string
	ResolvedEvent string
}

type ConnectRoutePlanAddress struct {
	By          string
	Source      string
	Target      string
	Cardinality string
	Mode        string
}

type ConnectRoutePlanMapEntry struct {
	Key    string
	Source string
	Target string
}

type ConnectRoutePlan struct {
	PackageKey                string
	Source                    ConnectRoutePlanEndpoint
	Receiver                  ConnectRoutePlanEndpoint
	Adapter                   string
	Delivery                  ConnectRoutePlanDelivery
	TargetKind                ConnectRoutePlanTargetKind
	Address                   *ConnectRoutePlanAddress
	Map                       []ConnectRoutePlanMapEntry
	Reply                     map[string]string
	Target                    events.RouteIdentity
	TargetSet                 []events.RouteIdentity
	RequiresRuntimeResolution bool
}

type ConnectRoutePlanIssue struct {
	Connect runtimecontracts.FlowPackageConnect
	Failure ConnectRoutePlanFailure
	Detail  string
}

type ConnectRoutePlanMaterializationInput struct {
	MatchValues             map[string]string
	Descriptors             []Descriptor
	SupportedAddressTargets []string
}

type ConnectRoutePlanMaterialization struct {
	Target    events.RouteIdentity
	TargetSet []events.RouteIdentity
	Failure   ConnectRoutePlanFailure
}

func LowerCompositionConnectRoutePlans(source semanticview.Source) ([]ConnectRoutePlan, []ConnectRoutePlanIssue) {
	if source == nil {
		return nil, []ConnectRoutePlanIssue{{Failure: ConnectFailureSourceMissing, Detail: "semantic source is required"}}
	}
	plans := make([]ConnectRoutePlan, 0, len(source.CompositionConnects()))
	var issues []ConnectRoutePlanIssue
	for _, connect := range source.CompositionConnects() {
		plan, issue := LowerCompositionConnectRoutePlan(source, connect)
		if issue.Failure != "" {
			issues = append(issues, issue)
			continue
		}
		plans = append(plans, plan)
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].Source.FlowID != plans[j].Source.FlowID {
			return plans[i].Source.FlowID < plans[j].Source.FlowID
		}
		if plans[i].Source.Pin != plans[j].Source.Pin {
			return plans[i].Source.Pin < plans[j].Source.Pin
		}
		if plans[i].Receiver.FlowID != plans[j].Receiver.FlowID {
			return plans[i].Receiver.FlowID < plans[j].Receiver.FlowID
		}
		return plans[i].Receiver.Pin < plans[j].Receiver.Pin
	})
	return plans, issues
}

func LowerCompositionConnectRoutePlan(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlan, ConnectRoutePlanIssue) {
	if source == nil {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureSourceMissing, Detail: "semantic source is required"}
	}
	from, err := connect.FromRef()
	if err != nil {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailurePinRefInvalid, Detail: err.Error()}
	}
	to, err := connect.ToRef()
	if err != nil {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailurePinRefInvalid, Detail: err.Error()}
	}
	sourceScope, ok := source.FlowScopeByID(from.FlowID)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerFlowMissing, Detail: from.FlowID}
	}
	receiverScope, ok := source.FlowScopeByID(to.FlowID)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverFlowMissing, Detail: to.FlowID}
	}
	outputPin, ok := source.FlowOutputEventPin(from.FlowID, from.Pin)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerOutputPinMissing, Detail: connect.From}
	}
	inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverInputPinMissing, Detail: connect.To}
	}
	delivery, failure := connectDelivery(connect, inputPin)
	if failure != "" {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: failure, Detail: strings.TrimSpace(connect.Delivery)}
	}
	address := connectAddress(connect, inputPin)
	if receiverRequiresRuntimeResolution(receiverScope) && address == nil && delivery != ConnectDeliveryBroadcast {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverAddressRuleMissing, Detail: to.FlowID}
	}
	targetKind := connectTargetKind(delivery)
	plan := ConnectRoutePlan{
		PackageKey: strings.TrimSpace(connect.PackageKey),
		Source: ConnectRoutePlanEndpoint{
			FlowID:        strings.TrimSpace(from.FlowID),
			FlowPath:      strings.Trim(strings.TrimSpace(sourceScope.Path), "/"),
			Mode:          strings.TrimSpace(sourceScope.Mode),
			Pin:           strings.TrimSpace(from.Pin),
			Event:         eventidentity.Normalize(outputPin.EventType()),
			ResolvedEvent: eventidentity.Normalize(source.ResolveFlowEventReference(from.FlowID, outputPin.EventType())),
		},
		Receiver: ConnectRoutePlanEndpoint{
			FlowID:        strings.TrimSpace(to.FlowID),
			FlowPath:      strings.Trim(strings.TrimSpace(receiverScope.Path), "/"),
			Mode:          strings.TrimSpace(receiverScope.Mode),
			Pin:           strings.TrimSpace(to.Pin),
			Event:         eventidentity.Normalize(inputPin.EventType()),
			ResolvedEvent: eventidentity.Normalize(source.ResolveFlowEventReference(to.FlowID, inputPin.EventType())),
		},
		Adapter:    strings.TrimSpace(connect.Adapter),
		Delivery:   delivery,
		TargetKind: targetKind,
		Address:    address,
		Map:        connectMapEntries(connect.Map),
		Reply:      cloneStringMap(connect.Reply),
	}
	if !receiverRequiresRuntimeResolution(receiverScope) {
		route := staticConnectRoute(source, to.FlowID)
		if !route.Empty() {
			switch targetKind {
			case ConnectTargetKindTarget, ConnectTargetKindReply:
				plan.Target = route
			case ConnectTargetKindTargetSet:
				plan.TargetSet = []events.RouteIdentity{route}
			}
		}
		return plan, ConnectRoutePlanIssue{}
	}
	plan.RequiresRuntimeResolution = true
	return plan, ConnectRoutePlanIssue{}
}

func MaterializeConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
	if !plan.Target.Empty() {
		return ConnectRoutePlanMaterialization{Target: plan.Target}
	}
	if len(plan.TargetSet) > 0 {
		return ConnectRoutePlanMaterialization{TargetSet: append([]events.RouteIdentity{}, plan.TargetSet...)}
	}
	if plan.TargetKind == ConnectTargetKindTargetSet && plan.Delivery == ConnectDeliveryBroadcast && plan.Address == nil {
		return materializeBroadcastConnectRoutePlan(plan, input.Descriptors)
	}
	if plan.Address == nil {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureReceiverAddressRuleMissing}
	}
	value := connectAddressValue(plan, input.MatchValues)
	if value == "" {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureAddressValueMissing}
	}
	targetExpr := strings.TrimSpace(plan.Address.Target)
	if targetExpr == "" {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetUnsupported}
	}
	routes, supported := materializeConnectRoutes(plan, targetExpr, value, input.Descriptors, input.SupportedAddressTargets)
	if !supported {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetUnsupported}
	}
	routes = uniqueRoutes(routes)
	if len(routes) == 0 {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetUnresolved}
	}
	switch plan.TargetKind {
	case ConnectTargetKindTarget, ConnectTargetKindReply:
		if len(routes) > 1 {
			return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetAmbiguous}
		}
		return ConnectRoutePlanMaterialization{Target: routes[0]}
	case ConnectTargetKindTargetSet:
		return ConnectRoutePlanMaterialization{TargetSet: routes}
	default:
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureDeliveryTopologyInvalid}
	}
}

func materializeBroadcastConnectRoutePlan(plan ConnectRoutePlan, descriptors []Descriptor) ConnectRoutePlanMaterialization {
	routes := make([]events.RouteIdentity, 0, len(descriptors))
	for _, descriptor := range descriptors {
		route := descriptorRouteForReceiver(plan, descriptor)
		if route.Empty() {
			continue
		}
		routes = append(routes, route)
	}
	routes = uniqueRoutes(routes)
	if len(routes) == 0 {
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureTargetUnresolved}
	}
	return ConnectRoutePlanMaterialization{TargetSet: routes}
}

func connectDelivery(connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin) (ConnectRoutePlanDelivery, ConnectRoutePlanFailure) {
	delivery := strings.TrimSpace(connect.Delivery)
	cardinality := ""
	if inputPin.Address != nil {
		cardinality = strings.TrimSpace(inputPin.Address.Cardinality)
	}
	if delivery == "" {
		switch cardinality {
		case "one", "many":
			delivery = cardinality
		default:
			delivery = string(ConnectDeliveryOne)
		}
	}
	switch ConnectRoutePlanDelivery(delivery) {
	case ConnectDeliveryOne:
		if cardinality == "many" {
			return "", ConnectFailureDeliveryTopologyInvalid
		}
		return ConnectDeliveryOne, ""
	case ConnectDeliveryMany:
		if cardinality == "one" {
			return "", ConnectFailureDeliveryTopologyInvalid
		}
		return ConnectDeliveryMany, ""
	case ConnectDeliveryBroadcast:
		if cardinality == "one" {
			return "", ConnectFailureDeliveryTopologyInvalid
		}
		return ConnectDeliveryBroadcast, ""
	case ConnectDeliveryReply:
		if len(connect.Reply) == 0 {
			return "", ConnectFailureReplyLineageMissing
		}
		return ConnectDeliveryReply, ""
	default:
		return "", ConnectFailureDeliveryTopologyInvalid
	}
}

func connectAddress(connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin) *ConnectRoutePlanAddress {
	if inputPin.Address == nil {
		return nil
	}
	address := inputPin.Address
	out := ConnectRoutePlanAddress{
		By:          strings.TrimSpace(address.By),
		Source:      strings.TrimSpace(address.Source),
		Target:      strings.TrimSpace(address.Target),
		Cardinality: strings.TrimSpace(address.Cardinality),
		Mode:        strings.TrimSpace(address.Mode),
	}
	if mapped, ok := connect.Map[out.By]; ok {
		if source := strings.TrimSpace(mapped.Source); source != "" {
			out.Source = source
		}
		if target := strings.TrimSpace(mapped.Target); target != "" {
			out.Target = target
		}
	}
	return &out
}

func connectMapEntries(in map[string]runtimecontracts.FlowPackageConnectMap) []ConnectRoutePlanMapEntry {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for key := range in {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]ConnectRoutePlanMapEntry, 0, len(keys))
	for _, key := range keys {
		entry := in[key]
		out = append(out, ConnectRoutePlanMapEntry{
			Key:    key,
			Source: strings.TrimSpace(entry.Source),
			Target: strings.TrimSpace(entry.Target),
		})
	}
	return out
}

func connectTargetKind(delivery ConnectRoutePlanDelivery) ConnectRoutePlanTargetKind {
	switch delivery {
	case ConnectDeliveryMany, ConnectDeliveryBroadcast:
		return ConnectTargetKindTargetSet
	case ConnectDeliveryReply:
		return ConnectTargetKindReply
	default:
		return ConnectTargetKindTarget
	}
}

func receiverRequiresRuntimeResolution(scope semanticview.FlowScope) bool {
	switch strings.TrimSpace(scope.Mode) {
	case "template", "dynamic":
		return true
	default:
		return false
	}
}

func staticConnectRoute(source semanticview.Source, flowID string) events.RouteIdentity {
	flowInstance := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, flowID)), "/")
	if flowInstance == "" {
		return events.RouteIdentity{}
	}
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(flowID),
		FlowInstance: flowInstance,
		EntityID:     runtimeflowidentity.EntityID(flowInstance),
	}.Normalized()
}

func connectAddressValue(plan ConnectRoutePlan, values map[string]string) string {
	if len(values) == 0 || plan.Address == nil {
		return ""
	}
	keys := []string{
		plan.Address.By,
		plan.Address.Source,
		expressionLeaf(plan.Address.Source),
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func materializeConnectRoutes(plan ConnectRoutePlan, targetExpr, value string, descriptors []Descriptor, supportedTargets []string) ([]events.RouteIdentity, bool) {
	if !addressTargetSupported(targetExpr, supportedTargets) {
		return nil, false
	}
	var routes []events.RouteIdentity
	for _, descriptor := range descriptors {
		route := descriptorRouteForReceiver(plan, descriptor)
		if route.Empty() {
			continue
		}
		matches := connectDescriptorMatches(targetExpr, value, descriptor, route)
		if matches {
			routes = append(routes, route)
		}
	}
	return routes, true
}

func connectDescriptorMatches(targetExpr, value string, descriptor Descriptor, route events.RouteIdentity) bool {
	target := normalizeAddressTarget(targetExpr)
	value = strings.Trim(strings.TrimSpace(value), "/")
	switch target {
	case "entity_id":
		return strings.TrimSpace(descriptor.EntityID) == value || route.EntityID == value
	case "flow_instance":
		return strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/") == value || route.FlowInstance == value
	case "instance_id":
		return strings.TrimSpace(descriptor.ID) == value || runtimeflowidentity.LogicalInstanceID(route.FlowInstance) == value
	default:
		fieldValue, ok := descriptor.AddressFields[normalizeAddressTargetKey(targetExpr)]
		return ok && strings.Trim(strings.TrimSpace(fieldValue), "/") == value
	}
}

func descriptorRouteForReceiver(plan ConnectRoutePlan, descriptor Descriptor) events.RouteIdentity {
	if !descriptorBelongsToReceiver(plan, descriptor) {
		return events.RouteIdentity{}
	}
	return descriptorRoute(nil, plan.Receiver.FlowID, descriptor)
}

func descriptorBelongsToReceiver(plan ConnectRoutePlan, descriptor Descriptor) bool {
	flowInstance := strings.Trim(strings.TrimSpace(descriptor.FlowInstance), "/")
	if flowInstance == "" {
		return false
	}
	receiverPath := strings.Trim(strings.TrimSpace(plan.Receiver.FlowPath), "/")
	if receiverPath == "" {
		receiverPath = strings.Trim(strings.TrimSpace(plan.Receiver.FlowID), "/")
	}
	return receiverPath != "" && (flowInstance == receiverPath || strings.HasPrefix(flowInstance, receiverPath+"/"))
}

func addressTargetSupported(expr string, supportedTargets []string) bool {
	switch normalizeAddressTarget(expr) {
	case "entity_id", "flow_instance", "instance_id":
		return true
	default:
		needle := normalizeAddressTargetKey(expr)
		for _, supported := range supportedTargets {
			if normalizeAddressTargetKey(supported) == needle {
				return true
			}
		}
		return false
	}
}

func normalizeAddressTarget(expr string) string {
	expr = strings.TrimSpace(expr)
	for _, prefix := range []string{"entity.", "instance.", "event.target.", "target."} {
		if strings.HasPrefix(expr, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(expr, prefix))
		}
	}
	return expr
}

func normalizeAddressTargetKey(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}
	if strings.HasPrefix(expr, "entity.") {
		return "entity." + strings.TrimSpace(strings.TrimPrefix(expr, "entity."))
	}
	if strings.HasPrefix(expr, "config.") {
		return "config." + strings.TrimSpace(strings.TrimPrefix(expr, "config."))
	}
	if strings.HasPrefix(expr, "instance.") {
		return "instance." + strings.TrimSpace(strings.TrimPrefix(expr, "instance."))
	}
	switch normalizeAddressTarget(expr) {
	case "entity_id":
		return "entity.entity_id"
	case "flow_instance":
		return "instance.flow_instance"
	case "instance_id":
		return "instance.instance_id"
	default:
		return expr
	}
}

func SupportedConnectAddressTargets(source semanticview.Source, plan ConnectRoutePlan) []string {
	if plan.Address == nil {
		return nil
	}
	target := normalizeAddressTargetKey(plan.Address.Target)
	if target == "" {
		return nil
	}
	switch normalizeAddressTarget(target) {
	case "entity_id", "flow_instance", "instance_id":
		return nil
	}
	fieldPath, ok := strings.CutPrefix(target, "entity.")
	if !ok || strings.TrimSpace(fieldPath) == "" {
		return nil
	}
	contract, ok := entityruntime.ResolveForFlow(source, plan.Receiver.FlowID)
	if !ok {
		return nil
	}
	field, err := entityruntime.ResolveLeafField(contract, fieldPath)
	if err != nil || !field.FieldDecl.Indexed {
		return nil
	}
	return []string{target}
}

func expressionLeaf(expr string) string {
	expr = strings.TrimSpace(expr)
	if idx := strings.LastIndex(expr, "."); idx >= 0 && idx < len(expr)-1 {
		return strings.TrimSpace(expr[idx+1:])
	}
	return expr
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
