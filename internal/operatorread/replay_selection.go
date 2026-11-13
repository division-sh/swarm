package operatorread

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// ReplaySelection freezes a one-to-one selection of an observed event's delivery
// rows. It cannot be constructed from an arbitrary recipient or route list.
type ReplaySelection struct {
	original    events.Event
	routes      []events.DeliveryRoute
	deliveryIDs map[events.DeliveryRouteIdentity]string
}

func SelectReplayDeliveries(original OperatorEventFull, deliveryIDs []string) (ReplaySelection, error) {
	snapshot, err := original.EventSnapshot()
	if err != nil {
		return ReplaySelection{}, err
	}
	if snapshot.ID() != original.EventID || len(deliveryIDs) == 0 {
		return ReplaySelection{}, fmt.Errorf("replay requires original delivery rows and their exact event")
	}
	rows := make(map[string]OperatorEventDelivery, len(original.Deliveries))
	for _, row := range original.Deliveries {
		id := strings.TrimSpace(row.DeliveryID)
		if _, duplicate := rows[id]; duplicate {
			return ReplaySelection{}, fmt.Errorf("replay original delivery identity is ambiguous")
		}
		rows[id] = row
	}
	routes := make([]events.DeliveryRoute, 0, len(deliveryIDs))
	selectedIDs := make(map[events.DeliveryRouteIdentity]string, len(deliveryIDs))
	seen := make(map[string]struct{}, len(deliveryIDs))
	for _, id := range deliveryIDs {
		row, found := rows[id]
		if _, duplicate := seen[id]; !found || duplicate || id == "" {
			return ReplaySelection{}, fmt.Errorf("replay selection does not identify distinct original deliveries")
		}
		seen[id] = struct{}{}
		status, err := runtimedelivery.ParseStatus(row.Status)
		if err != nil || (status != runtimedelivery.StatusDelivered && status != runtimedelivery.StatusFailed && status != runtimedelivery.StatusDeadLetter) {
			return ReplaySelection{}, fmt.Errorf("replay original delivery is not terminal and replayable")
		}
		route := row.Route.Normalized()
		if !route.Recipient.IsAgent() || route.Recipient.ID() != strings.TrimSpace(row.SubscriberID) {
			return ReplaySelection{}, fmt.Errorf("replay original delivery has no exact agent route")
		}
		identity, err := route.Identity()
		if err != nil {
			return ReplaySelection{}, err
		}
		selectedIDs[identity] = id
		routes = append(routes, route)
	}
	routes = events.NormalizeDeliveryRoutes(routes)
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return ReplaySelection{}, err
	}
	if len(routes) != len(deliveryIDs) {
		return ReplaySelection{}, fmt.Errorf("replay routes are not one-to-one with original deliveries")
	}
	return ReplaySelection{original: snapshot.Clone(), routes: routes, deliveryIDs: selectedIDs}, nil
}

func (s ReplaySelection) Empty() bool { return s.original.ID() == "" }

func (s ReplaySelection) OriginalEventID() string { return s.original.ID() }

func (s ReplaySelection) Routes() []events.DeliveryRoute {
	return events.NormalizeDeliveryRoutes(s.routes)
}

func (s ReplaySelection) ValidateEvent(replay events.Event) error {
	original := s.original
	if s.Empty() || replay.AdmissionClass() != events.EventAdmissionReplay || replay.ID() == "" || replay.ID() == original.ID() || replay.ParentEventID() != original.ID() ||
		replay.RunID() != original.RunID() || replay.Type() != original.Type() || !replay.Producer().Equal(original.Producer()) ||
		replay.RoutingSource() != original.RoutingSource() || replay.TaskID() != original.TaskID() ||
		replay.ChainDepth() != original.ChainDepth()+1 || replay.ExecutionMode() != original.ExecutionMode() ||
		!bytes.Equal(replay.Payload(), original.Payload()) {
		return fmt.Errorf("replay publication differs from its selected original event")
	}
	return nil
}

// ValidateRecorded compares the selection with the native prepared-event owner,
// not current descriptors or subscriptions. All route-owned facts must match.
func (s ReplaySelection) ValidateRecorded(original events.Event, routes []events.DeliveryRoute) error {
	if s.Empty() {
		return fmt.Errorf("replay requires original delivery evidence")
	}
	want, err := events.IntegrityProjection(s.original)
	if err != nil {
		return err
	}
	actual, err := events.IntegrityProjection(original)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, actual) {
		return fmt.Errorf("replay original event differs from its native record")
	}
	for _, selected := range s.routes {
		identity, err := selected.Identity()
		if err != nil {
			return err
		}
		deliveryID, err := runtimedelivery.DeliveryID(original.ID(), selected)
		if err != nil || s.deliveryIDs[identity] != deliveryID {
			return fmt.Errorf("replay source delivery identity differs from its native route")
		}
		matches := 0
		for _, recorded := range events.NormalizeDeliveryRoutes(routes) {
			if reflect.DeepEqual(selected, recorded) {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("replay selection is not an exact original delivery route")
		}
	}
	return nil
}
