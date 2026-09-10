package runfork

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
)

// TerminalBarrierHistory is an immutable projection of the fixed-revision
// relation owner, not an execution capability or a wire-admitted exception.
// Only the store relation owner may construct it after validating the complete
// schedule/occurrence/outcome/dead-letter evidence. JSON exposes proof for
// readback but cannot manufacture an admitted value on decoding.
type TerminalBarrierHistory struct {
	runID        string
	revision     int64
	activationID string
	deliveryID   string
	eventID      string
	route        events.DeliveryRouteIdentity
	claimVersion int64
	settledAt    time.Time
	reasonCode   string
}

func NewTerminalBarrierHistory(runID string, revision int64, barrier fanoutbarrier.Barrier, delivery deliverylifecycle.Snapshot) (TerminalBarrierHistory, error) {
	if err := barrier.Validate(); err != nil {
		return TerminalBarrierHistory{}, err
	}
	if revision <= 0 || runID == "" || barrier.Registration.IntentKey.RunID != runID || delivery.RunID != runID ||
		barrier.Status != fanoutbarrier.StatusOutcomeDeadLettered || barrier.ScheduleActivationID == "" ||
		delivery.Status != deliverylifecycle.StatusDeadLetter || delivery.ClaimVersion <= 0 || delivery.SettledAt.IsZero() {
		return TerminalBarrierHistory{}, fmt.Errorf("terminal barrier history requires exact revision and settled failed delivery")
	}
	return TerminalBarrierHistory{runID: runID, revision: revision, activationID: barrier.ScheduleActivationID,
		deliveryID: delivery.DeliveryID, eventID: delivery.EventID, route: delivery.RouteIdentity,
		claimVersion: delivery.ClaimVersion, settledAt: delivery.SettledAt, reasonCode: delivery.ReasonCode}, nil
}

func (h TerminalBarrierHistory) matches(item RunForkPendingWork) bool {
	route, err := item.DeliveryRoute.Identity()
	return err == nil && h.runID != "" && h.revision > 0 && h.activationID != "" &&
		h.eventID == item.EventID && h.deliveryID == item.DeliveryID && h.route == route &&
		item.SubscriberType == item.DeliveryRoute.Recipient.Code() && item.SubscriberID == item.DeliveryRoute.Recipient.ID() &&
		h.claimVersion == item.ClaimVersion && h.reasonCode == item.ReasonCode &&
		item.Classification == RunForkPendingClassificationDeadLetter && item.Status == string(deliverylifecycle.StatusDeadLetter) &&
		item.DeliveredAt != nil && h.settledAt.Equal(*item.DeliveredAt)
}

func (h TerminalBarrierHistory) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RunID        string `json:"run_id"`
		Revision     int64  `json:"revision"`
		ActivationID string `json:"activation_id"`
		DeliveryID   string `json:"delivery_id"`
		EventID      string `json:"event_id"`
		ClaimVersion int64  `json:"claim_version"`
		Disposition  string `json:"disposition"`
	}{h.runID, h.revision, h.activationID, h.deliveryID, h.eventID, h.claimVersion, "terminal_failure_no_replay"})
}

func (item RunForkPendingWork) RetainsTerminalBarrierHistory() bool {
	return item.TerminalBarrierHistory != nil && item.TerminalBarrierHistory.matches(item)
}
