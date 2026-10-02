package events

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

// InstancePublicationEventInput carries an ordinary, parentless publication
// from a constructed instance. It never grants permission to create a run.
type InstancePublicationEventInput struct {
	Facts EventFacts
	RunID string
}

func NewInstancePublicationEvent(input InstancePublicationEventInput) (Event, error) {
	return newSemanticEvent(EventAdmissionInstancePublication, "", input.Facts, input.RunID, "", nil, nil)
}

func validateInstancePublication(event Event) error {
	if event.RunID() == "" || event.ParentEventID() != "" || event.ChainDepth() != 0 {
		return fmt.Errorf("instance publication requires an existing run without causal parent or chain depth")
	}
	source := event.RoutingSource()
	if source.Kind() != RoutingSourceStaticFlow {
		return fmt.Errorf("instance publication requires an exact static instance source")
	}
	route := source.Route()
	if event.Producer().ID() != route.FlowID {
		return fmt.Errorf("instance publication producer does not match its source declaration")
	}
	declaration, err := eventidentity.AdmitPublicationDeclaration(route.FlowID, string(event.Type()))
	if err != nil {
		return err
	}
	if strings.HasPrefix(declaration.Local(), "platform.") {
		return fmt.Errorf("instance publication cannot emit platform-control events")
	}
	scope := eventidentity.PublicationStatic
	instance := route.FlowInstance
	if route.FlowID == "." {
		if instance != event.RunID() {
			return fmt.Errorf("root instance publication source must match its run")
		}
		scope, instance = eventidentity.PublicationRoot, ""
	}
	name, err := eventidentity.ProjectPublication(declaration, scope, route.FlowID, instance)
	if err != nil {
		return err
	}
	if name != string(event.Type()) {
		return fmt.Errorf("instance publication event must use exact business identity %q", name)
	}
	return nil
}
