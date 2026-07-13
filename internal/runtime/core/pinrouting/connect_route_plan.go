package pinrouting

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

type ConnectRoutePlanResolutionKind string

const (
	ConnectResolutionStatic      ConnectRoutePlanResolutionKind = "static"
	ConnectResolutionAddress     ConnectRoutePlanResolutionKind = "address"
	ConnectResolutionInstanceKey ConnectRoutePlanResolutionKind = "instance_key"
	ConnectResolutionBroadcast   ConnectRoutePlanResolutionKind = "broadcast"
	ConnectResolutionReply       ConnectRoutePlanResolutionKind = "reply"
)

type ConnectRoutePlanFailure string

const (
	ConnectFailureSourceMissing              ConnectRoutePlanFailure = "source_missing"
	ConnectFailureSourceLocationMissing      ConnectRoutePlanFailure = "connect_source_location_missing"
	ConnectFailurePinRefInvalid              ConnectRoutePlanFailure = "connect_pin_ref_invalid"
	ConnectFailureProducerFlowMissing        ConnectRoutePlanFailure = "producer_flow_missing"
	ConnectFailureProducerOutputPinMissing   ConnectRoutePlanFailure = "producer_output_pin_missing"
	ConnectFailureReceiverRootUnsupported    ConnectRoutePlanFailure = "receiver_root_unsupported"
	ConnectFailureReceiverFlowMissing        ConnectRoutePlanFailure = "receiver_flow_missing"
	ConnectFailureReceiverInputPinMissing    ConnectRoutePlanFailure = "receiver_input_pin_missing"
	ConnectFailureReceiverAddressRuleMissing ConnectRoutePlanFailure = "receiver_address_rule_missing"
	ConnectFailureDeliveryTopologyInvalid    ConnectRoutePlanFailure = "delivery_topology_invalid"
	ConnectFailureReplyLineageMissing        ConnectRoutePlanFailure = "reply_lineage_missing"
	ConnectFailureAddressValueMissing        ConnectRoutePlanFailure = "route_plan_address_value_missing"
	ConnectFailureTargetUnsupported          ConnectRoutePlanFailure = "route_plan_target_unsupported"
	ConnectFailureTargetUnresolved           ConnectRoutePlanFailure = "route_plan_target_unresolved"
	ConnectFailureTargetAmbiguous            ConnectRoutePlanFailure = "route_plan_target_ambiguous"
	ConnectFailureInstanceKeyAdapterInvalid  ConnectRoutePlanFailure = "route_plan_instance_key_adapter_invalid"
	ConnectFailureInstanceResolutionInvalid  ConnectRoutePlanFailure = "route_plan_instance_resolution_invalid"
	ConnectFailureInstanceConflict           ConnectRoutePlanFailure = "route_plan_instance_conflict"
	ConnectFailureLifecycleUnavailable       ConnectRoutePlanFailure = "route_plan_lifecycle_unavailable"
)

type ConnectRoutePlanEndpoint struct {
	Root          bool
	FlowID        string
	FlowPath      string
	Mode          string
	Pin           string
	Event         string
	ResolvedEvent string
	Key           string
	Carries       []string
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

type ConnectRoutePlanInstanceKey struct {
	Mode       string
	Fields     []string
	Mappings   []ConnectRoutePlanInstanceKeyMapping
	Mint       string
	As         string
	OnMissing  string
	OnConflict string
}

type ConnectRoutePlanInstanceKeyMapping struct {
	Source   string
	Target   string
	Explicit bool
}

type ConnectRoutePlanFanIn struct {
	Aggregation string
	Window      string
	DedupBy     []string
	Singleton   string
}

type ConnectRoutePlanReplyResolution struct {
	Role              string
	RequesterFlowID   string
	RequestOutputPin  string
	ReplyInputPin     string
	ProviderFlowID    string
	ProviderInputPin  string
	ProviderOutputPin string
	CorrelationKey    string
}

const (
	ConnectReplyRoleRequest  = "request"
	ConnectReplyRoleResponse = "response"
)

type ConnectRoutePlan struct {
	PackageKey                string
	AuthoredLocation          string
	Source                    ConnectRoutePlanEndpoint
	Receiver                  ConnectRoutePlanEndpoint
	Adapter                   string
	Delivery                  ConnectRoutePlanDelivery
	TargetKind                ConnectRoutePlanTargetKind
	ResolutionKind            ConnectRoutePlanResolutionKind
	Address                   *ConnectRoutePlanAddress
	InstanceKey               *ConnectRoutePlanInstanceKey
	FanIn                     *ConnectRoutePlanFanIn
	ReplyResolution           *ConnectRoutePlanReplyResolution
	Map                       []ConnectRoutePlanMapEntry
	Reply                     map[string]string
	Target                    events.RouteIdentity
	TargetSet                 []events.RouteIdentity
	RequiresRuntimeResolution bool
}

type ConnectRoutePlanIssue struct {
	Connect          runtimecontracts.FlowPackageConnect
	AuthoredLocation string
	Failure          ConnectRoutePlanFailure
	Detail           string
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

type ConnectRoutePlanInstanceKeyMaterial struct {
	Values map[string]any
	Keys   []runtimecontracts.TemplateInstanceKeyValue
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

// LowerTargetFreeInputRoutePlans lowers exact external input pins for the
// explicitly authorized target-free event set. It reuses the same instance-key
// materialization model as composition connect routes without inventing a
// synthetic producer output pin.
func LowerTargetFreeInputRoutePlans(source semanticview.Source, eventNames []string) ([]ConnectRoutePlan, []ConnectRoutePlanIssue) {
	if source == nil || len(eventNames) == 0 {
		return nil, nil
	}
	allowed := map[string]struct{}{}
	for _, name := range eventNames {
		if normalized := eventidentity.Normalize(name); normalized != "" {
			allowed[normalized] = struct{}{}
		}
	}
	plans := make([]ConnectRoutePlan, 0)
	issues := make([]ConnectRoutePlanIssue, 0)
	for _, scope := range source.FlowScopes() {
		flowID := strings.TrimSpace(scope.ID)
		for _, inputPin := range source.FlowInputEventPins(flowID) {
			if strings.TrimSpace(inputPin.Source) != "external" {
				continue
			}
			resolved := eventidentity.Normalize(source.ResolveFlowEventReference(flowID, inputPin.EventType()))
			if _, ok := allowed[resolved]; !ok {
				continue
			}
			connect := runtimecontracts.FlowPackageConnect{To: flowID + "." + inputPin.PinName(), Delivery: string(ConnectDeliveryOne)}
			var instanceKey *ConnectRoutePlanInstanceKey
			if receiverRequiresRuntimeResolution(scope) {
				var issue ConnectRoutePlanIssue
				instanceKey, issue = connectResolutionInstanceKey(source, connect, inputPin, inputPin.Resolution, ConnectDeliveryOne, flowID)
				if issue.Failure != "" {
					issue.AuthoredLocation = flowID + "." + inputPin.PinName()
					issues = append(issues, issue)
					continue
				}
			}
			plan := ConnectRoutePlan{
				AuthoredLocation: flowID + "." + inputPin.PinName(),
				Source: ConnectRoutePlanEndpoint{
					Root: true, Pin: inputPin.PinName(), Event: resolved, ResolvedEvent: resolved, Mode: "external",
				},
				Receiver: ConnectRoutePlanEndpoint{
					FlowID: flowID, FlowPath: strings.Trim(strings.TrimSpace(scope.Path), "/"), Mode: strings.TrimSpace(scope.Mode),
					Pin: inputPin.PinName(), Event: eventidentity.Normalize(inputPin.EventType()), ResolvedEvent: resolved,
				},
				Delivery: ConnectDeliveryOne, TargetKind: ConnectTargetKindTarget,
				ResolutionKind: connectResolutionKind(scope, ConnectDeliveryOne, nil, instanceKey),
				InstanceKey:    instanceKey,
			}
			if receiverRequiresRuntimeResolution(scope) {
				if instanceKey == nil {
					issues = append(issues, ConnectRoutePlanIssue{Connect: connect, AuthoredLocation: plan.AuthoredLocation, Failure: ConnectFailureReceiverAddressRuleMissing, Detail: flowID})
					continue
				}
				plan.RequiresRuntimeResolution = true
			} else {
				plan.Target = staticConnectRoute(source, flowID)
			}
			plans = append(plans, plan)
		}
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].Receiver.FlowID != plans[j].Receiver.FlowID {
			return plans[i].Receiver.FlowID < plans[j].Receiver.FlowID
		}
		return plans[i].Receiver.Pin < plans[j].Receiver.Pin
	})
	return plans, issues
}

func LowerCompositionConnectRoutePlan(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlan, ConnectRoutePlanIssue) {
	plan, issue := lowerCompositionConnectRoutePlan(source, connect)
	authoredLocation := connect.AuthoredLocation()
	plan.AuthoredLocation = authoredLocation
	issue.AuthoredLocation = authoredLocation
	if issue.Failure == "" && authoredLocation == "" {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{
			Connect:          connect,
			AuthoredLocation: authoredLocation,
			Failure:          ConnectFailureSourceLocationMissing,
			Detail:           "connect requires exact package.yaml source file and line metadata",
		}
	}
	return plan, issue
}

func lowerCompositionConnectRoutePlan(source semanticview.Source, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlan, ConnectRoutePlanIssue) {
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
	sourceEndpoint, outputPin, sourceIssue := connectRoutePlanSourceEndpoint(source, from, connect)
	if sourceIssue.Failure != "" {
		return ConnectRoutePlan{}, sourceIssue
	}
	if to.Root {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverRootUnsupported, Detail: strings.TrimSpace(connect.To)}
	}
	receiverScope, ok := source.FlowScopeByID(to.FlowID)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverFlowMissing, Detail: to.FlowID}
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
	instanceKey, instanceKeyIssue := connectInstanceKey(source, connect, outputPin, inputPin, delivery, to.FlowID)
	if instanceKeyIssue.Failure != "" {
		return ConnectRoutePlan{}, instanceKeyIssue
	}
	fanIn, fanInIssue := connectFanIn(source, connect, inputPin, delivery, to.FlowID)
	if fanInIssue.Failure != "" {
		return ConnectRoutePlan{}, fanInIssue
	}
	replyResolution, replyIssue := connectReplyResolution(source, connect, sourceEndpoint, to, inputPin)
	if replyIssue.Failure != "" {
		return ConnectRoutePlan{}, replyIssue
	}
	if receiverRequiresRuntimeResolution(receiverScope) && address == nil && instanceKey == nil && delivery != ConnectDeliveryBroadcast && (replyResolution == nil || replyResolution.Role != ConnectReplyRoleResponse) {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverAddressRuleMissing, Detail: to.FlowID}
	}
	targetKind := connectTargetKind(delivery)
	plan := ConnectRoutePlan{
		PackageKey:       strings.TrimSpace(connect.PackageKey),
		AuthoredLocation: connect.AuthoredLocation(),
		Source:           sourceEndpoint,
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
		ResolutionKind: connectResolutionKind(
			receiverScope,
			delivery,
			address,
			instanceKey,
		),
		Address:         address,
		InstanceKey:     instanceKey,
		FanIn:           fanIn,
		ReplyResolution: replyResolution,
		Map:             connectMapEntries(connect.Map),
		Reply:           cloneStringMap(connect.Reply),
	}
	if replyResolution != nil && replyResolution.Role == ConnectReplyRoleResponse {
		plan.ResolutionKind = ConnectResolutionReply
		plan.RequiresRuntimeResolution = true
		return plan, ConnectRoutePlanIssue{}
	}
	if !receiverRequiresRuntimeResolution(receiverScope) {
		route := staticConnectRoute(source, to.FlowID)
		if fanIn != nil {
			route = fanInSingletonRoute(to.FlowID, fanIn.Singleton)
		}
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

func connectRoutePlanSourceEndpoint(source semanticview.Source, from runtimecontracts.FlowPackagePinRef, connect runtimecontracts.FlowPackageConnect) (ConnectRoutePlanEndpoint, runtimecontracts.FlowOutputEventPin, ConnectRoutePlanIssue) {
	if from.Root {
		outputPin, ok := source.FlowOutputEventPin("", from.Pin)
		if !ok {
			return ConnectRoutePlanEndpoint{}, runtimecontracts.FlowOutputEventPin{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerOutputPinMissing, Detail: strings.TrimSpace(connect.From)}
		}
		return ConnectRoutePlanEndpoint{
			Root:          true,
			Pin:           strings.TrimSpace(from.Pin),
			Event:         eventidentity.Normalize(outputPin.EventType()),
			ResolvedEvent: eventidentity.Normalize(source.ResolveFlowEventReference("", outputPin.EventType())),
			Mode:          "root",
			Key:           strings.TrimSpace(outputPin.Key),
			Carries:       normalizedPinCarries(outputPin.Carries),
		}, outputPin, ConnectRoutePlanIssue{}
	}
	sourceScope, ok := source.FlowScopeByID(from.FlowID)
	if !ok {
		return ConnectRoutePlanEndpoint{}, runtimecontracts.FlowOutputEventPin{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerFlowMissing, Detail: strings.TrimSpace(from.FlowID)}
	}
	outputPin, ok := source.FlowOutputEventPin(from.FlowID, from.Pin)
	if !ok {
		return ConnectRoutePlanEndpoint{}, runtimecontracts.FlowOutputEventPin{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerOutputPinMissing, Detail: strings.TrimSpace(connect.From)}
	}
	return ConnectRoutePlanEndpoint{
		FlowID:        strings.TrimSpace(from.FlowID),
		FlowPath:      strings.Trim(strings.TrimSpace(sourceScope.Path), "/"),
		Mode:          strings.TrimSpace(sourceScope.Mode),
		Pin:           strings.TrimSpace(from.Pin),
		Event:         eventidentity.Normalize(outputPin.EventType()),
		ResolvedEvent: eventidentity.Normalize(source.ResolveFlowEventReference(from.FlowID, outputPin.EventType())),
		Key:           strings.TrimSpace(outputPin.Key),
		Carries:       normalizedPinCarries(outputPin.Carries),
	}, outputPin, ConnectRoutePlanIssue{}
}

func MaterializeConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
	if !plan.Target.Empty() {
		return ConnectRoutePlanMaterialization{Target: plan.Target}
	}
	if len(plan.TargetSet) > 0 {
		return ConnectRoutePlanMaterialization{TargetSet: append([]events.RouteIdentity{}, plan.TargetSet...)}
	}
	switch connectRoutePlanResolutionKind(plan) {
	case ConnectResolutionBroadcast:
		return materializeBroadcastConnectRoutePlan(plan, input.Descriptors)
	case ConnectResolutionAddress:
		return materializeAddressConnectRoutePlan(plan, input)
	case ConnectResolutionInstanceKey:
		return materializeInstanceKeyConnectRoutePlan(plan, input)
	default:
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureReceiverAddressRuleMissing}
	}
}

func materializeAddressConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
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

func materializeInstanceKeyConnectRoutePlan(plan ConnectRoutePlan, input ConnectRoutePlanMaterializationInput) ConnectRoutePlanMaterialization {
	keyMaterial, failure := InstanceKeyMaterialForConnectRoutePlan(plan, input.MatchValues)
	if failure != "" {
		return ConnectRoutePlanMaterialization{Failure: failure}
	}
	routes := make([]events.RouteIdentity, 0, len(input.Descriptors))
	for _, descriptor := range input.Descriptors {
		route := descriptorRouteForReceiver(plan, descriptor)
		if route.Empty() {
			continue
		}
		if ConnectInstanceKeyDescriptorMatches(keyMaterial.Keys, descriptor) {
			routes = append(routes, route)
		}
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

func InstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, matchValues map[string]string) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.InstanceKey
	if instanceKey == nil || len(instanceKey.Fields) == 0 {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverAddressRuleMissing
	}
	if strings.TrimSpace(instanceKey.Mint) != "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureAddressValueMissing
	}
	values := make(map[string]any, len(instanceKey.Fields))
	mappings := connectInstanceKeyMaterializationMappings(instanceKey)
	for _, mapping := range mappings {
		source := strings.TrimSpace(mapping.Source)
		target := strings.TrimSpace(mapping.Target)
		if source == "" || target == "" {
			return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverAddressRuleMissing
		}
		value := ""
		if mapping.Explicit {
			value = firstMatchValue(matchValues, "payload."+source)
		} else {
			value = firstMatchValue(matchValues, source, "payload."+source)
		}
		if value == "" {
			return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureAddressValueMissing
		}
		values[target] = value
	}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.Receiver.FlowID,
		By:     append([]string{}, instanceKey.Fields...),
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureAddressValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		Values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, ""
}

func MintedInstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, eventID string) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.InstanceKey
	if instanceKey == nil || len(instanceKey.Fields) == 0 {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverAddressRuleMissing
	}
	mint := strings.TrimSpace(instanceKey.Mint)
	as := strings.TrimSpace(instanceKey.As)
	eventID = strings.TrimSpace(eventID)
	if mint == "" || as == "" || eventID == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureAddressValueMissing
	}
	value := ""
	switch mint {
	case runtimecontracts.FlowInputResolutionMintUUID:
		value = deterministicResolutionUUID(plan, eventID)
	case runtimecontracts.FlowInputResolutionMintEventID:
		value = eventID
	default:
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	values := map[string]any{as: value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.Receiver.FlowID,
		By:     append([]string{}, instanceKey.Fields...),
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureAddressValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		Values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, ""
}

func deterministicResolutionUUID(plan ConnectRoutePlan, eventID string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(strings.TrimSpace(eventID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(plan.Receiver.FlowID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(plan.Receiver.Pin)))
	_, _ = h.Write([]byte{0})
	if plan.InstanceKey != nil {
		_, _ = h.Write([]byte(strings.TrimSpace(plan.InstanceKey.As)))
	}
	sum := h.Sum(nil)
	b := append([]byte{}, sum[:16]...)
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[0:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:32])
}

func InstanceKeyDescriptorRoutesForConnectRoutePlan(plan ConnectRoutePlan, keyMaterial []runtimecontracts.TemplateInstanceKeyValue, descriptors []Descriptor) []events.RouteIdentity {
	if len(keyMaterial) == 0 {
		return nil
	}
	routes := make([]events.RouteIdentity, 0, len(descriptors))
	for _, descriptor := range descriptors {
		route := descriptorRouteForReceiver(plan, descriptor)
		if route.Empty() {
			continue
		}
		if ConnectInstanceKeyDescriptorMatches(keyMaterial, descriptor) {
			routes = append(routes, route)
		}
	}
	return uniqueRoutes(routes)
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

func connectInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, outputPin runtimecontracts.FlowOutputEventPin, inputPin runtimecontracts.FlowInputEventPin, delivery ConnectRoutePlanDelivery, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	adapter := connect.Using.Instance
	if source == nil {
		return nil, ConnectRoutePlanIssue{}
	}
	resolution := inputPin.Resolution
	if !resolution.Empty() {
		if inputPin.Address != nil {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "input pin resolution is incompatible with legacy address"}
		}
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "connect.using.instance is incompatible with input pin resolution"}
		}
		if len(connect.Map) > 0 {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "connect.map is incompatible with input pin resolution"}
		}
		return connectResolutionInstanceKey(source, connect, inputPin, resolution, delivery, receiverFlowID)
	}
	if inputPin.Address != nil {
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: "connect.using.instance requires an addressless receiver input"}
		}
		return nil, ConnectRoutePlanIssue{}
	}
	if delivery == ConnectDeliveryBroadcast {
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: "connect.using.instance is incompatible with delivery broadcast"}
		}
		return nil, ConnectRoutePlanIssue{}
	}
	if len(connect.Map) > 0 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: "connect.map is not instance-key adapter authority; use connect.using.instance"}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, ConnectRoutePlanIssue{}
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlowID)
	if err != nil {
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: err.Error()}
		}
		return nil, ConnectRoutePlanIssue{}
	}
	outputKey := strings.TrimSpace(outputPin.Key)
	if outputKey == "" {
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: "producer output pin key is required for connect.using.instance"}
		}
		return nil, ConnectRoutePlanIssue{}
	}
	carries := normalizedPinCarries(outputPin.Carries)
	if !stringListContains(carries, outputKey) {
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: "producer output pin carries must include key before connect.using.instance can route"}
		}
		return nil, ConnectRoutePlanIssue{}
	}
	fields := normalizedStringList(instance.By)
	mappings, ok := connectInstanceKeyMappings(adapter, fields, carries, outputKey)
	if !ok {
		if adapter.Declared {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceKeyAdapterInvalid, Detail: "connect.using.instance must completely map producer carries to receiver instance.by"}
		}
		return nil, ConnectRoutePlanIssue{}
	}
	return &ConnectRoutePlanInstanceKey{
		Fields:     fields,
		Mappings:   mappings,
		OnMissing:  strings.TrimSpace(instance.OnMissing),
		OnConflict: strings.TrimSpace(instance.OnConflict),
	}, ConnectRoutePlanIssue{}
}

func connectResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, resolution runtimecontracts.FlowInputPinResolution, delivery ConnectRoutePlanDelivery, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	switch resolution.Mode {
	case runtimecontracts.FlowInputResolutionModeCreate:
		return connectCreateResolutionInstanceKey(source, connect, resolution, delivery, receiverFlowID)
	case runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return connectCarriedKeyResolutionInstanceKey(source, connect, inputPin, resolution, delivery, receiverFlowID)
	case runtimecontracts.FlowInputResolutionModeFanIn:
		return nil, ConnectRoutePlanIssue{}
	case runtimecontracts.FlowInputResolutionModeReply:
		return nil, ConnectRoutePlanIssue{}
	default:
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %q is design-locked but not runnable in this slice", resolution.Mode)}
	}
}

func connectReplyResolution(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, sourceEndpoint ConnectRoutePlanEndpoint, receiverRef runtimecontracts.FlowPackagePinRef, inputPin runtimecontracts.FlowInputEventPin) (*ConnectRoutePlanReplyResolution, ConnectRoutePlanIssue) {
	if inputPin.Resolution.Mode == runtimecontracts.FlowInputResolutionModeReply {
		resolution := inputPin.Resolution
		if inputPin.Address != nil || !resolution.InstanceKey.Empty() || resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode reply may only declare replies_to and correlation_key"}
		}
		requestOutputPin := strings.TrimSpace(resolution.RepliesTo)
		if requestOutputPin == "" {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: "resolution mode reply requires replies_to"}
		}
		requestOutput, ok := source.FlowOutputEventPin(receiverRef.FlowID, requestOutputPin)
		if !ok {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply replies_to %q must name a same-flow output pin", requestOutputPin)}
		}
		correlationKey := strings.TrimSpace(resolution.CorrelationKey)
		if correlationKey != "" && !stringListContains(normalizedPinCarries(requestOutput.Carries), correlationKey) {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply correlation_key %q must name a carry declared by output pin %s", correlationKey, requestOutputPin)}
		}
		requestConnects := source.CompositionConnectsFrom(receiverRef.FlowID, requestOutputPin)
		if len(requestConnects) != 1 {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", receiverRef.FlowID, requestOutputPin, len(requestConnects))}
		}
		requestTarget, err := requestConnects[0].ToRef()
		if err != nil || requestTarget.Root || strings.TrimSpace(requestTarget.FlowID) != strings.TrimSpace(sourceEndpoint.FlowID) {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: "resolution mode reply request and reply edges must connect the same provider flow"}
		}
		return &ConnectRoutePlanReplyResolution{
			Role:              ConnectReplyRoleResponse,
			RequesterFlowID:   strings.TrimSpace(receiverRef.FlowID),
			RequestOutputPin:  requestOutputPin,
			ReplyInputPin:     strings.TrimSpace(receiverRef.Pin),
			ProviderFlowID:    strings.TrimSpace(sourceEndpoint.FlowID),
			ProviderInputPin:  strings.TrimSpace(requestTarget.Pin),
			ProviderOutputPin: strings.TrimSpace(sourceEndpoint.Pin),
			CorrelationKey:    correlationKey,
		}, ConnectRoutePlanIssue{}
	}

	var matches []ConnectRoutePlanReplyResolution
	for _, replyInput := range source.FlowInputEventPins(sourceEndpoint.FlowID) {
		if replyInput.Resolution.Mode != runtimecontracts.FlowInputResolutionModeReply || strings.TrimSpace(replyInput.Resolution.RepliesTo) != strings.TrimSpace(sourceEndpoint.Pin) {
			continue
		}
		for _, replyConnect := range source.CompositionConnectsTo(sourceEndpoint.FlowID, replyInput.PinName()) {
			from, err := replyConnect.FromRef()
			if err != nil || from.Root || strings.TrimSpace(from.FlowID) != strings.TrimSpace(receiverRef.FlowID) {
				continue
			}
			matches = append(matches, ConnectRoutePlanReplyResolution{
				Role:              ConnectReplyRoleRequest,
				RequesterFlowID:   strings.TrimSpace(sourceEndpoint.FlowID),
				RequestOutputPin:  strings.TrimSpace(sourceEndpoint.Pin),
				ReplyInputPin:     strings.TrimSpace(replyInput.PinName()),
				ProviderFlowID:    strings.TrimSpace(receiverRef.FlowID),
				ProviderInputPin:  strings.TrimSpace(receiverRef.Pin),
				ProviderOutputPin: strings.TrimSpace(from.Pin),
				CorrelationKey:    strings.TrimSpace(replyInput.Resolution.CorrelationKey),
			})
		}
	}
	if len(matches) > 1 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("request pin %s.%s participates in multiple reply loops", sourceEndpoint.FlowID, sourceEndpoint.Pin)}
	}
	if len(matches) == 1 {
		return &matches[0], ConnectRoutePlanIssue{}
	}
	return nil, ConnectRoutePlanIssue{}
}

func connectFanIn(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, delivery ConnectRoutePlanDelivery, receiverFlowID string) (*ConnectRoutePlanFanIn, ConnectRoutePlanIssue) {
	if inputPin.Resolution.Empty() || inputPin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeFanIn {
		return nil, ConnectRoutePlanIssue{}
	}
	resolution := inputPin.Resolution
	if delivery != ConnectDeliveryOne {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureDeliveryTopologyInvalid, Detail: "resolution mode fan-in requires delivery one"}
	}
	if !resolution.InstanceKey.Empty() || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in may only declare aggregation, window, dedup_by, singleton, and carries"}
	}
	if resolution.Aggregation != "stream" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in supports only aggregation: stream in this slice, got %q", resolution.Aggregation)}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "receiver singleton coordinator owner is unavailable for input pin resolution"}
	}
	if _, err := bundle.ResolveFlowSingletonCoordinator(receiverFlowID); err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	window := strings.TrimSpace(resolution.Window)
	if window == "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires window"}
	}
	dedupBy := normalizedStringList(resolution.DedupBy)
	if len(dedupBy) == 0 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires dedup_by; sender identity is not an implicit default"}
	}
	if len(dedupBy) != 1 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in stream supports exactly one dedup_by field in this slice, got %v", dedupBy)}
	}
	if !connectFanInDedupSupported(dedupBy[0]) {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in dedup_by %q must be event.id or one top-level payload field", dedupBy[0])}
	}
	if !connectFanInPayloadFieldSupported(window) {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in window %q must be one top-level payload field", window)}
	}
	singleton := strings.Trim(strings.TrimSpace(resolution.Singleton), "/")
	if singleton == "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires explicit singleton receiver identity"}
	}
	scopeKey := strings.Trim(strings.TrimSpace(runtimeflowidentity.ScopeKey(source, receiverFlowID)), "/")
	if scopeKey != "" && singleton != scopeKey && !strings.HasPrefix(singleton, scopeKey+"/") {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in singleton %q must be the receiver singleton route or a child of %q", singleton, scopeKey)}
	}
	return &ConnectRoutePlanFanIn{
		Aggregation: resolution.Aggregation,
		Window:      window,
		DedupBy:     dedupBy,
		Singleton:   singleton,
	}, ConnectRoutePlanIssue{}
}

func connectFanInDedupSupported(dedup string) bool {
	dedup = strings.TrimSpace(dedup)
	return dedup == "event.id" || connectFanInPayloadFieldSupported(dedup)
}

func connectFanInPayloadFieldSupported(path string) bool {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return false
	}
	field := strings.TrimSpace(strings.TrimPrefix(path, "payload."))
	return field != "" && !strings.Contains(field, ".")
}

func connectCreateResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, resolution runtimecontracts.FlowInputPinResolution, delivery ConnectRoutePlanDelivery, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	if delivery != ConnectDeliveryOne {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureDeliveryTopologyInvalid, Detail: "resolution mode create requires delivery one"}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureLifecycleUnavailable, Detail: "receiver instance contract owner is unavailable"}
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlowID)
	if err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	mint := strings.TrimSpace(resolution.InstanceKey.Mint)
	as := strings.TrimSpace(resolution.InstanceKey.As)
	switch mint {
	case runtimecontracts.FlowInputResolutionMintUUID, runtimecontracts.FlowInputResolutionMintEventID:
	default:
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode create mint %q must be uuid or event_id", mint)}
	}
	fields := normalizedStringList(instance.By)
	if as == "" || len(fields) != 1 || fields[0] != as {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode create instance_key.as %q must match the receiver's single instance.by field %v", as, fields)}
	}
	return &ConnectRoutePlanInstanceKey{
		Mode:       runtimecontracts.FlowInputResolutionModeCreate,
		Fields:     fields,
		Mint:       mint,
		As:         as,
		OnMissing:  "create",
		OnConflict: "reuse",
	}, ConnectRoutePlanIssue{}
}

func connectCarriedKeyResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, resolution runtimecontracts.FlowInputPinResolution, delivery ConnectRoutePlanDelivery, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	mode := strings.TrimSpace(resolution.Mode)
	if delivery != ConnectDeliveryOne {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureDeliveryTopologyInvalid, Detail: fmt.Sprintf("resolution mode %s requires delivery one", mode)}
	}
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" || strings.TrimSpace(resolution.InstanceKey.Mint) != "" || strings.TrimSpace(resolution.InstanceKey.As) != "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s may only declare instance_key and carries", mode)}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureLifecycleUnavailable, Detail: "receiver instance contract owner is unavailable"}
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlowID)
	if err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	fields := normalizedStringList(instance.By)
	key := strings.TrimSpace(resolution.InstanceKey.From)
	if key == "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s requires instance_key to name a carried field", mode)}
	}
	if len(fields) != 1 || fields[0] != key {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s instance_key %q must match the receiver's single instance.by field %v", mode, key, fields)}
	}
	carry, ok := inputPin.Carries[key]
	if !ok {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s instance_key %s must name declared carries.%s", mode, key, key)}
	}
	wantFrom := "payload." + key
	if strings.TrimSpace(carry.From) != wantFrom {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s carry %s must use from: %s", mode, key, wantFrom)}
	}
	if strings.TrimSpace(carry.Type) != "" {
		targetType, err := connectResolutionReceiverEntityFieldType(source, receiverFlowID, key)
		if err != nil {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
		}
		if !connectResolutionTypesCompatible(carry.Type, targetType) {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("key_types_incompatible: carry %s type %s is incompatible with receiver entity.%s type %s", key, carry.Type, key, targetType)}
		}
	}
	onMissing := "reject"
	onConflict := "reject"
	if mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		onMissing = "create"
		onConflict = "reuse"
	}
	return &ConnectRoutePlanInstanceKey{
		Mode:       mode,
		Fields:     fields,
		Mappings:   []ConnectRoutePlanInstanceKeyMapping{{Source: key, Target: key, Explicit: true}},
		OnMissing:  onMissing,
		OnConflict: onConflict,
	}, ConnectRoutePlanIssue{}
}

func connectResolutionReceiverEntityFieldType(source semanticview.Source, receiverFlowID, field string) (string, error) {
	contract, ok := entityruntime.ResolveForFlow(source, receiverFlowID)
	if !ok {
		return "", fmt.Errorf("receiver flow %s has no entity contract for entity.%s", receiverFlowID, field)
	}
	resolved, err := entityruntime.ResolveLeafField(contract, field)
	if err != nil {
		return "", fmt.Errorf("receiver entity.%s is invalid: %v", field, err)
	}
	return resolved.Type, nil
}

func connectResolutionTypesCompatible(sourceType, targetType string) bool {
	sourceType = connectResolutionTypeFamily(sourceType)
	targetType = connectResolutionTypeFamily(targetType)
	return sourceType != "" && targetType != "" && sourceType == targetType
}

func connectResolutionTypeFamily(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "string", "text", "uuid", "timestamp":
		return "string"
	case "integer", "number", "numeric", "float", "double", "real":
		return "number"
	case "boolean", "bool":
		return "boolean"
	default:
		return raw
	}
}

func connectInstanceKeyMaterializationMappings(instanceKey *ConnectRoutePlanInstanceKey) []ConnectRoutePlanInstanceKeyMapping {
	if instanceKey == nil {
		return nil
	}
	if len(instanceKey.Mappings) > 0 {
		out := make([]ConnectRoutePlanInstanceKeyMapping, 0, len(instanceKey.Mappings))
		for _, mapping := range instanceKey.Mappings {
			source := strings.TrimSpace(mapping.Source)
			target := strings.TrimSpace(mapping.Target)
			if source == "" || target == "" {
				continue
			}
			out = append(out, ConnectRoutePlanInstanceKeyMapping{Source: source, Target: target, Explicit: mapping.Explicit})
		}
		return out
	}
	out := make([]ConnectRoutePlanInstanceKeyMapping, 0, len(instanceKey.Fields))
	for _, field := range instanceKey.Fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, ConnectRoutePlanInstanceKeyMapping{Source: field, Target: field})
		}
	}
	return out
}

func connectInstanceKeyMappings(adapter runtimecontracts.FlowPackageConnectInstanceAdapter, receiverFields, carries []string, outputKey string) ([]ConnectRoutePlanInstanceKeyMapping, bool) {
	if adapter.Declared {
		sources := normalizedAdapterFields(adapter.Source)
		targets := normalizedAdapterFields(adapter.Target)
		if len(sources) == 0 || len(sources) != len(targets) || len(targets) != len(receiverFields) || duplicateString(sources) != "" || duplicateString(targets) != "" {
			return nil, false
		}
		if !sameStringSet(targets, receiverFields) {
			return nil, false
		}
		mappings := make([]ConnectRoutePlanInstanceKeyMapping, 0, len(targets))
		for idx, source := range sources {
			target := strings.TrimSpace(targets[idx])
			if !stringListContains(carries, source) || !stringListContains(receiverFields, target) {
				return nil, false
			}
			mappings = append(mappings, ConnectRoutePlanInstanceKeyMapping{Source: source, Target: target, Explicit: true})
		}
		return mappings, true
	}
	if !stringListContains(receiverFields, outputKey) {
		return nil, false
	}
	mappings := make([]ConnectRoutePlanInstanceKeyMapping, 0, len(receiverFields))
	for _, field := range receiverFields {
		if !stringListContains(carries, field) {
			return nil, false
		}
		mappings = append(mappings, ConnectRoutePlanInstanceKeyMapping{Source: field, Target: field})
	}
	return mappings, true
}

func normalizedAdapterFields(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func connectResolutionKind(scope semanticview.FlowScope, delivery ConnectRoutePlanDelivery, address *ConnectRoutePlanAddress, instanceKey *ConnectRoutePlanInstanceKey) ConnectRoutePlanResolutionKind {
	if !receiverRequiresRuntimeResolution(scope) {
		return ConnectResolutionStatic
	}
	if address != nil {
		return ConnectResolutionAddress
	}
	if delivery == ConnectDeliveryBroadcast {
		return ConnectResolutionBroadcast
	}
	if instanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	return ""
}

func connectRoutePlanResolutionKind(plan ConnectRoutePlan) ConnectRoutePlanResolutionKind {
	if plan.ResolutionKind != "" {
		return plan.ResolutionKind
	}
	if !plan.Target.Empty() || len(plan.TargetSet) > 0 {
		return ConnectResolutionStatic
	}
	if plan.Address != nil {
		return ConnectResolutionAddress
	}
	if plan.InstanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	if plan.TargetKind == ConnectTargetKindTargetSet && plan.Delivery == ConnectDeliveryBroadcast {
		return ConnectResolutionBroadcast
	}
	return ""
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

func fanInSingletonRoute(flowID, singleton string) events.RouteIdentity {
	singleton = strings.Trim(strings.TrimSpace(singleton), "/")
	if strings.TrimSpace(flowID) == "" || singleton == "" {
		return events.RouteIdentity{}
	}
	return events.RouteIdentity{
		FlowID:       strings.TrimSpace(flowID),
		FlowInstance: singleton,
		EntityID:     runtimeflowidentity.EntityID(singleton),
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

func ConnectInstanceKeyDescriptorMatches(keyMaterial []runtimecontracts.TemplateInstanceKeyValue, descriptor Descriptor) bool {
	if len(keyMaterial) == 0 {
		return false
	}
	for _, key := range keyMaterial {
		field := strings.TrimSpace(key.Field)
		value := strings.Trim(strings.TrimSpace(key.Value), "/")
		if field == "" || value == "" {
			return false
		}
		actual, ok := descriptor.AddressFields["entity."+field]
		if !ok {
			actual, ok = descriptor.AddressFields[field]
		}
		if !ok || strings.Trim(strings.TrimSpace(actual), "/") != value {
			return false
		}
	}
	return true
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
	if strings.HasPrefix(expr, "_entity.") {
		switch value := strings.TrimSpace(strings.TrimPrefix(expr, "_entity.")); value {
		case "id":
			return "entity_id"
		case "flow_instance":
			return "flow_instance"
		default:
			return value
		}
	}
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
	if strings.HasPrefix(expr, "_entity.") {
		return "_entity." + strings.TrimSpace(strings.TrimPrefix(expr, "_entity."))
	}
	if strings.HasPrefix(expr, "config.") {
		return "config." + strings.TrimSpace(strings.TrimPrefix(expr, "config."))
	}
	if strings.HasPrefix(expr, "instance.") {
		return "instance." + strings.TrimSpace(strings.TrimPrefix(expr, "instance."))
	}
	switch normalizeAddressTarget(expr) {
	case "entity_id":
		return "_entity.id"
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
	fieldPath = strings.TrimSpace(fieldPath)
	if !ok || fieldPath == "" || strings.Contains(fieldPath, ".") {
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

func firstMatchValue(values map[string]string, keys ...string) string {
	if len(values) == 0 {
		return ""
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

func normalizedPinCarries(in []string) []string {
	return normalizedStringList(in)
}

func normalizedStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func stringListContains(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == needle {
			return true
		}
	}
	return false
}

func duplicateString(values []string) string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := map[string]struct{}{}
	for _, value := range left {
		value = strings.TrimSpace(value)
		if value == "" {
			return false
		}
		seen[value] = struct{}{}
	}
	for _, value := range right {
		value = strings.TrimSpace(value)
		if value == "" {
			return false
		}
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
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
