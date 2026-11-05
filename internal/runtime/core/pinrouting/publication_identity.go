package pinrouting

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// AdmitPublicationIdentity couples an already admitted business declaration and
// routing source before event construction or spelling-dependent identity hashes.
// Frozen continuations use their recorded declaration, not the current source.
func AdmitPublicationIdentity(flowID, declaredEvent string, source events.RoutingSource) (events.EventType, error) {
	return publicationIdentity(flowID, declaredEvent, source.Kind(), source.Route())
}

// PublicationDeclarationForSourceEvent resolves metadata for a committed
// business name, without changing that name or searching other flow scopes.
func PublicationDeclarationForSourceEvent(name events.EventType, source events.RoutingSource) (runtimeidentity.PublicationDeclaration, error) {
	route := source.Route()
	flowID, local := route.FlowID, string(name)
	if source.Kind() == events.RoutingSourceRoot {
		flowID = "."
	}
	if flowID != "." && route.FlowInstance != "" {
		local = strings.TrimPrefix(local, route.FlowInstance+"/")
	}
	declaration, err := runtimeidentity.AdmitPublicationDeclaration(flowID, local)
	if err != nil {
		return runtimeidentity.PublicationDeclaration{}, err
	}
	expected, err := AdmitPublicationIdentity(flowID, declaration.Local(), source)
	if err != nil || expected != name {
		return runtimeidentity.PublicationDeclaration{}, fmt.Errorf("publication %q does not match its admitted source", name)
	}
	return declaration, nil
}

func publicationIdentity(flowID, declaredEvent string, kind events.RoutingSourceKind, route events.RouteIdentity) (events.EventType, error) {
	declaration, err := runtimeidentity.AdmitPublicationDeclaration(flowID, declaredEvent)
	if err != nil {
		return "", err
	}
	var scope runtimeidentity.PublicationScope
	flow, instance := route.FlowID, route.FlowInstance
	switch kind {
	case events.RoutingSourceDeploymentFeed:
		if route.FlowID != flowID || route.FlowInstance != "" || route.EntityID != "" {
			return "", fmt.Errorf("deployment publication requires its exact declaration scope")
		}
		if flowID == "." {
			return events.EventType(declaration.Local()), nil
		}
		return events.EventType(flowID + "/" + declaration.Local()), nil
	case events.RoutingSourceRoot:
		scope, flow = runtimeidentity.PublicationRoot, "."
	case events.RoutingSourceStaticFlow:
		scope = runtimeidentity.PublicationStatic
		if flowID == "." && flow == "." {
			// O2 has admitted the selected entityless root. Its run route is
			// execution identity, never a business declaration namespace.
			scope, instance = runtimeidentity.PublicationRoot, ""
		}
	case events.RoutingSourceConcreteTemplateInstance:
		scope = runtimeidentity.PublicationTemplate
	default:
		return "", fmt.Errorf("routing source %s is not business publication authority", kind.StorageCode())
	}
	name, err := runtimeidentity.ProjectPublication(declaration, scope, flow, instance)
	return events.EventType(name), err
}
