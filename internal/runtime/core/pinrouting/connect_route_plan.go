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
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ConnectRoutePlanTargetKind string

const (
	ConnectTargetKindTarget    ConnectRoutePlanTargetKind = "target"
	ConnectTargetKindTargetSet ConnectRoutePlanTargetKind = "target_set"
)

type ConnectRoutePlanResolutionKind string

const (
	ConnectResolutionStatic      ConnectRoutePlanResolutionKind = "static"
	ConnectResolutionInstanceKey ConnectRoutePlanResolutionKind = "instance_key"
	ConnectResolutionReply       ConnectRoutePlanResolutionKind = "reply"
)

type ConnectRoutePlanFailure string

const (
	ConnectFailureSourceMissing              ConnectRoutePlanFailure = "source_missing"
	ConnectFailureSourceLocationMissing      ConnectRoutePlanFailure = "connect_source_location_missing"
	ConnectFailurePinRefInvalid              ConnectRoutePlanFailure = "connect_pin_ref_invalid"
	ConnectFailureProducerFlowMissing        ConnectRoutePlanFailure = "producer_flow_missing"
	ConnectFailureProducerOutputPinMissing   ConnectRoutePlanFailure = "producer_output_pin_missing"
	ConnectFailureReceiverFlowMissing        ConnectRoutePlanFailure = "receiver_flow_missing"
	ConnectFailureReceiverInputPinMissing    ConnectRoutePlanFailure = "receiver_input_pin_missing"
	ConnectFailureReceiverResolutionMissing  ConnectRoutePlanFailure = "receiver_resolution_missing"
	ConnectFailureDeliveryTopologyInvalid    ConnectRoutePlanFailure = "delivery_topology_invalid"
	ConnectFailureReplyLineageMissing        ConnectRoutePlanFailure = "reply_lineage_missing"
	ConnectFailureInstanceSourceValueMissing ConnectRoutePlanFailure = "route_plan_instance_source_value_missing"
	ConnectFailureTargetUnresolved           ConnectRoutePlanFailure = "route_plan_target_unresolved"
	ConnectFailureTargetAmbiguous            ConnectRoutePlanFailure = "route_plan_target_ambiguous"
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

// ConnectSourceEndpointMatches is the canonical source-side identity matcher
// for lowered connect plans.
func ConnectSourceEndpointMatches(endpoint ConnectRoutePlanEndpoint, eventType string, source events.RouteIdentity) bool {
	eventType = strings.Trim(strings.TrimSpace(eventType), "/")
	if eventType == "" {
		return false
	}
	source = source.Normalized()
	if !source.Empty() && !connectSourceRouteMatchesEndpoint(endpoint, source) {
		return false
	}
	sourceLocal := strings.Trim(strings.TrimSpace(endpoint.Event), "/")
	sourceResolved := strings.Trim(strings.TrimSpace(endpoint.ResolvedEvent), "/")
	sourcePath := strings.Trim(strings.TrimSpace(endpoint.FlowPath), "/")
	if sourcePath == "" {
		sourcePath = strings.Trim(strings.TrimSpace(endpoint.FlowID), "/")
	}
	if connectSourceEndpointIsTemplate(endpoint) {
		return source.FlowInstance != "" && sourceLocal != "" && eventType == source.FlowInstance+"/"+sourceLocal
	}
	sourceScoped := sourceLocal
	if sourcePath != "" && sourceLocal != "" {
		sourceScoped = sourcePath + "/" + sourceLocal
	}
	for _, candidate := range []string{sourceResolved, sourceScoped} {
		if candidate != "" && eventType == candidate {
			return true
		}
	}
	if sourceLocal != "" && eventType == sourceLocal {
		return !source.Empty()
	}
	if source.FlowInstance != "" && sourceLocal != "" && eventType == source.FlowInstance+"/"+sourceLocal {
		return true
	}
	return false
}

func ConnectSourceEndpointMatchesEvent(endpoint ConnectRoutePlanEndpoint, evt events.Event) bool {
	source := evt.SourceRoute().Normalized()
	// Legacy flow context can describe a child target; it cannot stand in for
	// missing producer provenance when selecting a package-root source.
	if endpoint.Root && source.Empty() && runtimeflowidentity.SemanticScopeFromFlowInstanceRef(evt.FlowInstance()) != "" {
		return false
	}
	return ConnectSourceEndpointMatches(endpoint, string(evt.Type()), source)
}

func connectSourceRouteMatchesEndpoint(endpoint ConnectRoutePlanEndpoint, source events.RouteIdentity) bool {
	source = source.Normalized()
	if source.Empty() {
		return true
	}
	if endpoint.Root {
		// Root ownership has no child flow identity to agree with. Entity-only
		// lineage is compatible, but any flow evidence names a non-root source.
		return source.FlowID == "" && source.FlowInstance == ""
	}

	sourcePath := strings.Trim(strings.TrimSpace(endpoint.FlowPath), "/")
	endpointFlowID := strings.Trim(strings.TrimSpace(endpoint.FlowID), "/")
	if sourcePath == "" {
		sourcePath = endpointFlowID
	}
	if source.FlowID == "" && source.FlowInstance == "" {
		return false
	}
	if source.FlowID != "" {
		sourceFlowID := strings.Trim(strings.TrimSpace(source.FlowID), "/")
		if sourceFlowID != endpointFlowID && sourceFlowID != sourcePath {
			return false
		}
	}
	if connectSourceEndpointIsTemplate(endpoint) {
		return source.FlowInstance != "" && source.FlowInstance != sourcePath && connectSourceFlowInstanceMatchesPath(source.FlowInstance, sourcePath)
	}
	if source.FlowInstance != "" && source.FlowInstance != sourcePath {
		return false
	}
	return true
}

func connectSourceEndpointIsTemplate(endpoint ConnectRoutePlanEndpoint) bool {
	return strings.EqualFold(strings.TrimSpace(endpoint.Mode), "template")
}

func connectSourceFlowInstanceMatchesPath(flowInstance, sourcePath string) bool {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	sourcePath = strings.Trim(strings.TrimSpace(sourcePath), "/")
	if sourcePath == "" {
		return flowInstance == "" || runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance) == ""
	}
	if flowInstance == sourcePath {
		return true
	}
	return runtimeflowidentity.SemanticScopeFromInstancePath(flowInstance) == sourcePath
}

type ConnectRoutePlanInstanceKey struct {
	Mode   runtimecontracts.FlowInputResolutionMode
	Field  runtimecontracts.TemplateInstanceField
	Source runtimecontracts.FlowInputInstanceSource
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
	PackageKey                  string
	AuthoredLocation            string
	Source                      ConnectRoutePlanEndpoint
	Receiver                    ConnectRoutePlanEndpoint
	Adapter                     string
	TargetKind                  ConnectRoutePlanTargetKind
	ResolutionKind              ConnectRoutePlanResolutionKind
	InstanceKey                 *ConnectRoutePlanInstanceKey
	FanIn                       *ConnectRoutePlanFanIn
	ReplyResolution             *ConnectRoutePlanReplyResolution
	Target                      events.RouteIdentity
	TargetSet                   []events.RouteIdentity
	RequiresRuntimeResolution   bool
	ProviderOutputAuthorization *runtimeprovideroutput.Authorization
}

type ConnectRoutePlanIssue struct {
	Connect                     runtimecontracts.FlowPackageConnect
	AuthoredLocation            string
	Failure                     ConnectRoutePlanFailure
	Detail                      string
	ProviderOutputAuthorization *runtimeprovideroutput.Authorization
}

type ConnectRoutePlanMaterializationInput struct {
	MatchValues map[string]string
	Descriptors []Descriptor
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
func LowerTargetFreeInputRoutePlans(source semanticview.Source, authorizations []runtimeprovideroutput.Authorization) ([]ConnectRoutePlan, []ConnectRoutePlanIssue) {
	if source == nil || len(authorizations) == 0 {
		return nil, nil
	}
	allowed := map[string]runtimeprovideroutput.Authorization{}
	for _, authorization := range authorizations {
		if authorization.Valid() {
			allowed[eventidentity.Normalize(authorization.Event())] = authorization
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
			authorization, ok := allowed[resolved]
			if !ok {
				continue
			}
			connect := runtimecontracts.FlowPackageConnect{To: flowID + "." + inputPin.PinName()}
			var instanceKey *ConnectRoutePlanInstanceKey
			if receiverRequiresRuntimeResolution(scope) {
				var issue ConnectRoutePlanIssue
				instanceKey, issue = connectResolutionInstanceKey(source, connect, inputPin, inputPin.Resolution, flowID)
				if issue.Failure != "" {
					issue.AuthoredLocation = flowID + "." + inputPin.PinName()
					issue.ProviderOutputAuthorization = &authorization
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
				TargetKind:                  ConnectTargetKindTarget,
				ResolutionKind:              connectResolutionKind(scope, instanceKey),
				InstanceKey:                 instanceKey,
				ProviderOutputAuthorization: &authorization,
			}
			if receiverRequiresRuntimeResolution(scope) {
				if instanceKey == nil {
					issues = append(issues, ConnectRoutePlanIssue{Connect: connect, AuthoredLocation: plan.AuthoredLocation, Failure: ConnectFailureReceiverResolutionMissing, Detail: flowID, ProviderOutputAuthorization: &authorization})
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
	if from.Root {
		flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey)
		if !ok {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureProducerFlowMissing, Detail: strings.TrimSpace(connect.PackageKey)}
		}
		from.FlowID, from.Root = flowID, flowID == ""
	}
	if to.Root {
		flowID, ok := semanticview.PackageRootFlowID(source, connect.PackageKey)
		if !ok {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverFlowMissing, Detail: strings.TrimSpace(connect.PackageKey)}
		}
		to.FlowID, to.Root = flowID, flowID == ""
	}
	sourceEndpoint, _, sourceIssue := connectRoutePlanSourceEndpoint(source, from, connect)
	if sourceIssue.Failure != "" {
		return ConnectRoutePlan{}, sourceIssue
	}
	if to.Root {
		inputPin, ok := source.FlowInputEventPin("", to.Pin)
		if !ok {
			return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverInputPinMissing, Detail: connect.To}
		}
		return ConnectRoutePlan{
			PackageKey:       strings.TrimSpace(connect.PackageKey),
			AuthoredLocation: connect.AuthoredLocation(),
			Source:           sourceEndpoint,
			Receiver: ConnectRoutePlanEndpoint{
				Root:          true,
				Mode:          "root",
				Pin:           strings.TrimSpace(to.Pin),
				Event:         eventidentity.Normalize(inputPin.EventType()),
				ResolvedEvent: eventidentity.Normalize(source.ResolveFlowEventReference("", inputPin.EventType())),
			},
			Adapter:        strings.TrimSpace(connect.Adapter),
			TargetKind:     ConnectTargetKindTarget,
			ResolutionKind: ConnectResolutionStatic,
		}, ConnectRoutePlanIssue{}
	}
	receiverScope, ok := source.FlowScopeByID(to.FlowID)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverFlowMissing, Detail: to.FlowID}
	}
	inputPin, ok := source.FlowInputEventPin(to.FlowID, to.Pin)
	if !ok {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverInputPinMissing, Detail: connect.To}
	}
	instanceKey, instanceKeyIssue := connectInstanceKey(source, connect, inputPin, to.FlowID)
	if instanceKeyIssue.Failure != "" {
		return ConnectRoutePlan{}, instanceKeyIssue
	}
	fanIn, fanInIssue := connectFanIn(source, connect, inputPin, to.FlowID)
	if fanInIssue.Failure != "" {
		return ConnectRoutePlan{}, fanInIssue
	}
	replyResolution, replyIssue := connectReplyResolution(source, connect, sourceEndpoint, to, inputPin)
	if replyIssue.Failure != "" {
		return ConnectRoutePlan{}, replyIssue
	}
	if receiverRequiresRuntimeResolution(receiverScope) && instanceKey == nil && (replyResolution == nil || replyResolution.Role != ConnectReplyRoleResponse) {
		return ConnectRoutePlan{}, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReceiverResolutionMissing, Detail: to.FlowID}
	}
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
		Adapter:         strings.TrimSpace(connect.Adapter),
		TargetKind:      ConnectTargetKindTarget,
		ResolutionKind:  connectResolutionKind(receiverScope, instanceKey),
		InstanceKey:     instanceKey,
		FanIn:           fanIn,
		ReplyResolution: replyResolution,
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
			plan.Target = route
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
	if plan.Receiver.Root {
		return ConnectRoutePlanMaterialization{}
	}
	if !plan.Target.Empty() {
		return ConnectRoutePlanMaterialization{Target: plan.Target}
	}
	if len(plan.TargetSet) > 0 {
		return ConnectRoutePlanMaterialization{TargetSet: append([]events.RouteIdentity{}, plan.TargetSet...)}
	}
	switch connectRoutePlanResolutionKind(plan) {
	case ConnectResolutionInstanceKey:
		return materializeInstanceKeyConnectRoutePlan(plan, input)
	default:
		return ConnectRoutePlanMaterialization{Failure: ConnectFailureReceiverResolutionMissing}
	}
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
	case ConnectTargetKindTarget:
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
	if instanceKey == nil || instanceKey.Field.Empty() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverResolutionMissing
	}
	if instanceKey.Source.RequiresDeliveryProjection() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	if instanceKey.Source.Kind != runtimecontracts.FlowInputInstanceSourcePayload {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	sourcePath := strings.TrimSpace(instanceKey.Source.Path)
	value := firstMatchValue(matchValues, sourcePath)
	if sourcePath == "" || value == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	values := map[string]any{instanceKey.Field.String(): value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.Receiver.FlowID,
		Field:  instanceKey.Field,
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	return ConnectRoutePlanInstanceKeyMaterial{
		Values: values,
		Keys:   append([]runtimecontracts.TemplateInstanceKeyValue{}, keys...),
	}, ""
}

func EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan ConnectRoutePlan, eventID string) (ConnectRoutePlanInstanceKeyMaterial, ConnectRoutePlanFailure) {
	instanceKey := plan.InstanceKey
	if instanceKey == nil || instanceKey.Field.Empty() {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureReceiverResolutionMissing
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
	}
	value := ""
	switch instanceKey.Source.Kind {
	case runtimecontracts.FlowInputInstanceSourceGeneratedUUID:
		value = deterministicResolutionUUID(plan, eventID)
	case runtimecontracts.FlowInputInstanceSourceEventID:
		value = eventID
	default:
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceResolutionInvalid
	}
	values := map[string]any{instanceKey.Field.String(): value}
	keys, err := (runtimecontracts.TemplateInstanceContract{
		FlowID: plan.Receiver.FlowID,
		Field:  instanceKey.Field,
	}).CanonicalKeyMaterial(values)
	if err != nil {
		return ConnectRoutePlanInstanceKeyMaterial{}, ConnectFailureInstanceSourceValueMissing
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
	if plan.InstanceKey != nil && !plan.InstanceKey.Field.Empty() {
		_, _ = h.Write([]byte(plan.InstanceKey.Field.String()))
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

func connectInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	if source == nil {
		return nil, ConnectRoutePlanIssue{}
	}
	resolution := inputPin.Resolution
	if resolution.Empty() {
		return nil, ConnectRoutePlanIssue{}
	}
	return connectResolutionInstanceKey(source, connect, inputPin, resolution, receiverFlowID)
}

func connectResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, resolution runtimecontracts.FlowInputPinResolution, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	switch resolution.Mode {
	case runtimecontracts.FlowInputResolutionModeCreate, runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate:
		return connectCanonicalResolutionInstanceKey(source, connect, inputPin, resolution, receiverFlowID)
	case runtimecontracts.FlowInputResolutionModeFanIn:
		return nil, ConnectRoutePlanIssue{}
	case runtimecontracts.FlowInputResolutionModeReply:
		return nil, ConnectRoutePlanIssue{}
	default:
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %q is design-locked but not runnable in this slice", resolution.Mode.String())}
	}
}

func connectReplyResolution(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, sourceEndpoint ConnectRoutePlanEndpoint, receiverRef runtimecontracts.FlowPackagePinRef, inputPin runtimecontracts.FlowInputEventPin) (*ConnectRoutePlanReplyResolution, ConnectRoutePlanIssue) {
	if inputPin.Resolution.Mode == runtimecontracts.FlowInputResolutionModeReply {
		resolution := inputPin.Resolution
		if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" {
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
		requestConnects := semanticview.ResolvedCompositionConnectsFrom(source, receiverRef.FlowID, requestOutputPin)
		if len(requestConnects) != 1 {
			return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureReplyLineageMissing, Detail: fmt.Sprintf("resolution mode reply request pin %s.%s must have exactly one connected counterpart, got %d", receiverRef.FlowID, requestOutputPin, len(requestConnects))}
		}
		requestTarget := requestConnects[0].To
		if requestTarget.Root || strings.TrimSpace(requestTarget.FlowID) != strings.TrimSpace(sourceEndpoint.FlowID) {
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
		for _, replyConnect := range semanticview.ResolvedCompositionConnectsTo(source, sourceEndpoint.FlowID, replyInput.PinName()) {
			from := replyConnect.From
			if from.Root || strings.TrimSpace(from.FlowID) != strings.TrimSpace(receiverRef.FlowID) {
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

func connectFanIn(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, receiverFlowID string) (*ConnectRoutePlanFanIn, ConnectRoutePlanIssue) {
	if inputPin.Resolution.Empty() || inputPin.Resolution.Mode != runtimecontracts.FlowInputResolutionModeFanIn {
		return nil, ConnectRoutePlanIssue{}
	}
	resolution := inputPin.Resolution
	if resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in may only declare aggregation, window, dedup_by, singleton, and carries"}
	}
	if resolution.Aggregation != "stream" && resolution.Aggregation != "barrier" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in aggregation must be stream or barrier, got %q", resolution.Aggregation)}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "receiver singleton coordinator owner is unavailable for input pin resolution"}
	}
	if _, err := bundle.ResolveFlowSingletonCoordinator(receiverFlowID); err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	window := strings.TrimSpace(resolution.Window)
	if resolution.Aggregation == "stream" && window == "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires window"}
	}
	dedupBy := normalizedStringList(resolution.DedupBy)
	if len(dedupBy) == 0 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: "resolution mode fan-in stream requires dedup_by; sender identity is not an implicit default"}
	}
	if len(dedupBy) != 1 {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in stream supports exactly one dedup_by field in this slice, got %v", dedupBy)}
	}
	if !connectFanInDedupSupported(dedupBy[0], resolution.Aggregation) {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode fan-in dedup_by %q must be event.id or one top-level payload field", dedupBy[0])}
	}
	if window != "" && !connectFanInPayloadFieldSupported(window) {
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

func connectFanInDedupSupported(dedup, aggregation string) bool {
	dedup = strings.TrimSpace(dedup)
	return (strings.TrimSpace(aggregation) == "stream" && dedup == "event.id") || connectFanInPayloadFieldSupported(dedup)
}

func connectFanInPayloadFieldSupported(path string) bool {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return false
	}
	field := strings.TrimSpace(strings.TrimPrefix(path, "payload."))
	return field != "" && !strings.Contains(field, ".")
}

func connectCanonicalResolutionInstanceKey(source semanticview.Source, connect runtimecontracts.FlowPackageConnect, inputPin runtimecontracts.FlowInputEventPin, resolution runtimecontracts.FlowInputPinResolution, receiverFlowID string) (*ConnectRoutePlanInstanceKey, ConnectRoutePlanIssue) {
	mode := resolution.Mode
	modeText := mode.String()
	if resolution.Aggregation != "" || resolution.Window != "" || len(resolution.DedupBy) > 0 || resolution.Singleton != "" || resolution.RepliesTo != "" || resolution.CorrelationKey != "" {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s may only declare mode and carries", modeText)}
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureLifecycleUnavailable, Detail: "receiver instance contract owner is unavailable"}
	}
	instance, err := bundle.ResolveFlowTemplateInstance(receiverFlowID)
	if err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	if instance.Field.Empty() {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: fmt.Sprintf("resolution mode %s requires receiver `instance: <field>`", modeText)}
	}
	evidence, err := bundle.ResolveFlowInputInstanceSourceType(source, receiverFlowID, inputPin, instance)
	if err != nil {
		return nil, ConnectRoutePlanIssue{Connect: connect, Failure: ConnectFailureInstanceResolutionInvalid, Detail: err.Error()}
	}
	return &ConnectRoutePlanInstanceKey{
		Mode:   mode,
		Field:  instance.Field,
		Source: evidence.Source,
	}, ConnectRoutePlanIssue{}
}

func connectResolutionKind(scope semanticview.FlowScope, instanceKey *ConnectRoutePlanInstanceKey) ConnectRoutePlanResolutionKind {
	if !receiverRequiresRuntimeResolution(scope) {
		return ConnectResolutionStatic
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
	if plan.InstanceKey != nil {
		return ConnectResolutionInstanceKey
	}
	return ""
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

func ConnectInstanceKeyDescriptorMatches(keyMaterial []runtimecontracts.TemplateInstanceKeyValue, descriptor Descriptor) bool {
	if len(keyMaterial) == 0 {
		return false
	}
	for _, key := range keyMaterial {
		field := key.Field.String()
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
