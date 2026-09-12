package pinrouting

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

// AdmitPublicationIdentity couples an already admitted business declaration and
// routing source before event construction or spelling-dependent identity hashes.
// Frozen continuations use their recorded declaration, not the current source.
func AdmitPublicationIdentity(flowID, declaredEvent string, source events.RoutingSource) (events.EventType, error) {
	return publicationIdentity(flowID, declaredEvent, source.Kind(), source.Route())
}

func publicationIdentity(flowID, declaredEvent string, kind events.RoutingSourceKind, route events.RouteIdentity) (events.EventType, error) {
	declaration, err := eventidentity.AdmitPublicationDeclaration(flowID, declaredEvent)
	if err != nil {
		return "", err
	}
	var scope eventidentity.PublicationScope
	flow, instance := route.FlowID, route.FlowInstance
	switch kind {
	case events.RoutingSourceRoot:
		scope, flow = eventidentity.PublicationRoot, "."
	case events.RoutingSourceStaticFlow:
		scope = eventidentity.PublicationStatic
		if flowID == "." && flow == "." {
			// O2 has admitted the selected entityless root. Its run route is
			// execution identity, never a business declaration namespace.
			scope, instance = eventidentity.PublicationRoot, ""
		}
	case events.RoutingSourceConcreteTemplateInstance:
		scope = eventidentity.PublicationTemplate
	default:
		return "", fmt.Errorf("routing source %s is not business publication authority", kind.StorageCode())
	}
	name, err := eventidentity.ProjectPublication(declaration, scope, flow, instance)
	return events.EventType(name), err
}
