package runfork

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

// ProjectReplayEventID is the existing fork replay coordinate. It identifies a
// candidate only; recorded lineage and fixed-cut membership admit the event.
func ProjectReplayEventID(forkRunID, sourceEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/event/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(sourceEventID))).String()
}

// ProjectCapturedConstructionIdentity composes the established entity, route
// and structural-parent projections without deriving a new constructor.
func ProjectCapturedConstructionIdentity(sourceRunID, forkRunID string, fixed flowidentity.Instance) (flowidentity.Instance, error) {
	ownership, err := ProjectEntityOwnership(sourceRunID, forkRunID, fixed.EntityID, fixed.InstancePath)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	route, err := ProjectExecutionRoute(sourceRunID, forkRunID, fixed.ScopeKey, fixed.Route())
	if err != nil {
		return flowidentity.Instance{}, err
	}
	parent, err := ProjectParentRoute(sourceRunID, forkRunID, fixed.ParentRoute)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	fixed.EntityID, fixed.InstancePath, fixed.InstanceID = ownership.Fork.EntityID, route.InstancePath, route.InstanceID
	fixed.ParentRoute, fixed.ParentEntityID = parent, parent.EntityID
	return fixed, nil
}
