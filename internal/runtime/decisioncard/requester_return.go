package decisioncard

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/uuid"
)

// HumanTaskReturn binds one native card continuation to its outcome publication.
// It is process-local preparation evidence, never a stored edge or payload selector.
type HumanTaskReturn struct {
	cardID      string
	requesterID string
	replyID     string
	product     events.Event
	anchor      HumanTaskAnchor
	sourceID    string
}

func BindHumanTaskReturn(card Card, continuation HumanTaskContinuation, product events.Event) (HumanTaskReturn, error) {
	if err := continuation.Validate(card); err != nil {
		return HumanTaskReturn{}, err
	}
	anchor, err := card.Anchor.HumanTask()
	if err != nil {
		return HumanTaskReturn{}, err
	}
	if product.AdmissionClass() != events.EventAdmissionChild || !events.ProducerIs(product, events.EventProducerPlatform, "workflow-runtime") ||
		product.RunID() != card.RunID || product.ParentEventID() == "" ||
		product.RoutingSource() != anchor.Source || product.TargetRoute() != continuation.RequesterRoute.Normalized() {
		return HumanTaskReturn{}, fmt.Errorf("human-task return does not match its native requester source")
	}
	wantID := HumanTaskOutcomeEventID(card.CardID, product.ParentEventID())
	switch product.Type() {
	case "human_task.deferred":
		if card.Status != StatusPending || card.DeferredUntil.IsZero() ||
			continuation.State != HumanTaskContinuationPending || !continuation.DeferredUntil.Equal(card.DeferredUntil) {
			return HumanTaskReturn{}, fmt.Errorf("human-task return does not match its deferred continuation")
		}
		wantID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.deferred.v1\x00"+card.CardID+"\x00"+product.ParentEventID())).String()
	case "human_task.approved", "human_task.rejected", "human_task.expired":
		wantType := events.EventType("human_task.approved")
		if card.Verdict == "reject" {
			wantType = "human_task.rejected"
		} else if card.Verdict != "approve" && card.Status != StatusExpired {
			return HumanTaskReturn{}, fmt.Errorf("human-task return verdict %q is not supported", card.Verdict)
		}
		if card.Status == StatusExpired {
			wantType = "human_task.expired"
		} else if card.Status != StatusDecided || card.DecisionEventID != product.ParentEventID() {
			return HumanTaskReturn{}, fmt.Errorf("human-task return does not match its decided card")
		}
		if product.Type() != wantType || continuation.OutcomeEventID != product.ParentEventID() ||
			(continuation.State != HumanTaskContinuationDecisionCommitted && continuation.State != HumanTaskContinuationExpired && continuation.State != HumanTaskContinuationOutcomeDispatched) {
			return HumanTaskReturn{}, fmt.Errorf("human-task return does not match its terminal continuation")
		}
	default:
		return HumanTaskReturn{}, fmt.Errorf("human-task return event %q is not supported", product.Type())
	}
	if product.ID() != wantID || product.CreatedAt().IsZero() {
		return HumanTaskReturn{}, fmt.Errorf("human-task return has no exact outcome occurrence")
	}
	if _, err := events.IntegrityProjection(product); err != nil {
		return HumanTaskReturn{}, err
	}
	return HumanTaskReturn{cardID: card.CardID, requesterID: anchor.RequesterAgentID, replyID: strings.TrimSpace(continuation.ReplyContextID), product: product.Clone(), anchor: anchor, sourceID: continuation.SourceEventID}, nil
}

func (r HumanTaskReturn) Empty() bool { return r.cardID == "" }

func (r HumanTaskReturn) RequesterID() string { return r.requesterID }

func (r HumanTaskReturn) CardID() string { return r.cardID }

func (r HumanTaskReturn) ValidateNative(card Card, continuation HumanTaskContinuation) error {
	if err := continuation.Validate(card); err != nil {
		return err
	}
	anchor, err := card.Anchor.HumanTask()
	if err != nil {
		return err
	}
	if r.Empty() || card.CardID != r.cardID || card.RunID != r.product.RunID() || anchor != r.anchor ||
		continuation.SourceEventID != r.sourceID || strings.TrimSpace(continuation.ReplyContextID) != r.replyID ||
		continuation.RequesterRoute.Normalized() != r.product.TargetRoute() {
		return fmt.Errorf("human-task return differs from its native card and continuation")
	}
	return nil
}

func (r HumanTaskReturn) Context() events.DeliveryContext {
	if r.replyID == "" {
		return events.DeliveryContext{}
	}
	return events.DeliveryContext{Reply: &events.ReplyContextRef{ID: r.replyID}}
}

func (r HumanTaskReturn) ValidateEvent(product events.Event, delivery events.DeliveryContext) error {
	if r.Empty() || delivery.ReplyContextID() != r.replyID {
		return fmt.Errorf("human-task return requires its exact native binding and reply context")
	}
	want, err := events.IntegrityProjection(r.product)
	if err != nil {
		return err
	}
	actual, err := events.IntegrityProjection(product)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, actual) {
		return fmt.Errorf("human-task return publication differs from its native binding")
	}
	return nil
}

func (r HumanTaskReturn) ValidateCommand(cardID, routeEventID string, occurredAt time.Time, terminal bool) error {
	if r.Empty() || r.cardID != cardID || r.product.ParentEventID() != routeEventID ||
		!CanonicalTimestamp(r.product.CreatedAt()).Equal(CanonicalTimestamp(occurredAt)) ||
		terminal == (r.product.Type() == "human_task.deferred") {
		return fmt.Errorf("human-task route command differs from its native return binding")
	}
	return nil
}
