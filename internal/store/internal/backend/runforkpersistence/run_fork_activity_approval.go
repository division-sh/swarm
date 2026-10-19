package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
)

func (a runForkSourceStateAdmission) requireOriginalActivityApproval(ctx context.Context, tx *sql.Tx, origin semanticview.ActivityRequestOrigin) (bool, error) {
	var request runForkActivityRequestPayload
	if err := json.Unmarshal(origin.Request.Payload(), &request); err != nil {
		return false, err
	}
	owner, err := activityidentity.ParseOwnerKey(request.NodeID)
	if err != nil {
		return false, err
	}
	node, present := owner.Node()
	if !present {
		return false, fmt.Errorf("original activity approval requires its exact declared node")
	}
	decision, err := a.carriage.ResolveOriginalActivityApproval(origin, node, request.HandlerEventKey, request.ActivityID, request.Tool)
	if err != nil {
		return false, err
	}
	card, continuation, found, err := decisionpersistence.ReadProposedEffectEvidenceInTx(ctx, tx, origin.Request.ID(), a.postgres)
	if err != nil {
		return false, err
	}
	if decision == "" {
		if found {
			return false, fmt.Errorf("unapproved original activity contradicts retained proposed-effect evidence")
		}
		return false, nil
	}
	if !found {
		return false, fmt.Errorf("approved original activity is missing its exact proposed effect")
	}
	// Physical event history has no delivery context. The validated original
	// continuation owns the released request's durable reply binding.
	deliveryContext := events.DeliveryContext{}
	if continuation.ReplyContextID != "" {
		deliveryContext.Reply = &events.ReplyContextRef{ID: continuation.ReplyContextID}
	}
	requestView := events.NewContextDeliveryEvent(origin.Request, deliveryContext).Event()
	if err := pipeline.ValidateRecordedActivityProposal(requestView, card, continuation, decision, origin.Source().BundleHash()); err != nil {
		return false, err
	}
	return true, nil
}
