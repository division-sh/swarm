package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// admitRunForkTerminalBarrierHistory joins fixed-revision evidence, never live
// timer/delivery state. Its result applies to one exact failed recipient only.
func admitRunForkTerminalBarrierHistory(snapshot *runForkRevisionSnapshot, obligations []runfork.RunForkFanOutObligation, pending []runfork.RunForkPendingWork) error {
	histories := make(map[int]runfork.TerminalBarrierHistory)
	for _, obligation := range obligations {
		barrier := obligation.Barrier
		if barrier == nil || barrier.Status != fanoutbarrier.StatusOutcomeDeadLettered {
			continue
		}
		// A previously materialized terminal barrier has no local activation or
		// copied occurrence. It grants no delivery exception here: any pending
		// recipient still goes through ordinary replay/refusal classification.
		if barrier.ScheduleActivationID == "" {
			continue
		}
		var timer *runForkRevisionTimer
		for i := range snapshot.Timers {
			if snapshot.Timers[i].TimerID == barrier.ScheduleActivationID {
				if timer != nil {
					return fmt.Errorf("terminal barrier has duplicate schedule evidence")
				}
				timer = &snapshot.Timers[i]
			}
		}
		if timer == nil {
			return fmt.Errorf("terminal barrier has no fixed-revision activation")
		}
		activation, err := projectRunForkBarrierActivation(*timer)
		if err != nil {
			return err
		}
		if err := genericschedule.ValidateFanOutBarrierScheduleRelation(*barrier, activation); err != nil {
			return err
		}
		var occurrence *runForkRevisionEvent
		for i := range snapshot.Events {
			if snapshot.Events[i].EventID == activation.CurrentEventID {
				if occurrence != nil {
					return fmt.Errorf("terminal barrier has duplicate occurrence evidence")
				}
				occurrence = &snapshot.Events[i]
			}
		}
		if occurrence == nil {
			return fmt.Errorf("terminal barrier occurrence is missing at the selected revision")
		}
		payload, err := canonicaljson.Decode(occurrence.Payload)
		if err != nil {
			return err
		}
		command := activation.Command
		if occurrence.RunID != snapshot.RunID || command.RunID != snapshot.RunID ||
			occurrence.EventClass != string(events.EventAdmissionRuntimeControl) || occurrence.SourceEventID != "" ||
			occurrence.EventName != command.EventType || occurrence.ExecutionMode != string(command.ExecutionMode) ||
			occurrence.TaskID != command.TaskID || occurrence.ProducedBy != genericschedule.OccurrenceProducerID() ||
			occurrence.ProducedByType != string(events.EventProducerPlatform) || occurrence.HandlerNode != "" ||
			occurrence.ChainDepth != 0 || !occurrence.CreatedAt.Equal(activation.CurrentDueAt) ||
			!reflect.DeepEqual(occurrence.RoutingSource, command.RoutingSource) || !payload.Equal(command.Payload) {
			return fmt.Errorf("terminal barrier occurrence contradicts canonical schedule emission")
		}
		var settlement events.RouteSettlement
		if err := json.Unmarshal(occurrence.RouteSettlement, &settlement); err != nil {
			return err
		}
		var routes []events.DeliveryRoute
		var exact *deliverylifecycle.Snapshot
		ref, _ := barrier.Registration.Handle.JoinRef()
		for i := range snapshot.Deliveries {
			delivery := &snapshot.Deliveries[i].Snapshot
			if delivery.EventID != occurrence.EventID {
				continue
			}
			routes = append(routes, delivery.Route)
			if delivery.Route.Recipient != events.MustNodeDeliveryRecipient(ref.Node()) {
				continue
			}
			target := delivery.Route.Target
			coordinate := target.Route()
			if !target.ExistingEntity() || coordinate.EntityID != barrier.Registration.EntityID ||
				coordinate.FlowID != ref.FlowPath() || coordinate.FlowInstance != barrier.Registration.Route.InstancePath {
				return fmt.Errorf("terminal barrier recipient has contradictory receiver ownership")
			}
			expected := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: target}.Normalized()
			if !reflect.DeepEqual(delivery.Route.Normalized(), expected) {
				return fmt.Errorf("terminal barrier recipient has contradictory context or payload projection")
			}
			if exact != nil {
				return fmt.Errorf("terminal barrier has ambiguous outcome recipient")
			}
			exact = delivery
		}
		if err := settlement.Validate(routes); err != nil {
			return err
		}
		if settlement.NoDelivery() && len(routes) == 0 {
			continue // No recipient is granted an exception by a terminal no-route occurrence.
		}
		if exact == nil || exact.RunID != snapshot.RunID || exact.Status != deliverylifecycle.StatusDeadLetter ||
			exact.ClaimVersion <= 0 || exact.Failure == nil || exact.SettledAt.IsZero() ||
			!exact.NextEligibleAt.IsZero() || !exact.ClaimExpiresAt.IsZero() || exact.ActiveSessionID != "" {
			return fmt.Errorf("terminal barrier has no exact settled failed recipient")
		}
		var deadLetter *runForkRevisionDeadLetter
		for i := range snapshot.DeadLetters {
			candidate := &snapshot.DeadLetters[i]
			if candidate.DeliveryID != exact.DeliveryID {
				continue
			}
			if deadLetter != nil {
				return fmt.Errorf("terminal barrier has ambiguous dead-letter evidence")
			}
			deadLetter = candidate
		}
		if deadLetter == nil || deadLetter.DeadLetterID == "" || deadLetter.OriginalEventID != occurrence.EventID ||
			deadLetter.ClaimVersion != exact.ClaimVersion || deadLetter.Outcome != "dead_letter" ||
			deadLetter.OutcomeReasonCode != exact.ReasonCode || deadLetter.HandlerNode != exact.SubscriberID ||
			deadLetter.OutcomeSettledAt == nil || !deadLetter.OutcomeSettledAt.Equal(exact.SettledAt) ||
			!deadLetter.CreatedAt.Equal(exact.SettledAt) {
			return fmt.Errorf("terminal barrier dead-letter contradicts exact delivery claim/outcome")
		}
		failure, err := canonicaljson.Decode(deadLetter.OutcomeFailure)
		if err != nil {
			return err
		}
		expectedFailure, err := canonicaljson.FromGo(exact.Failure)
		if err != nil || !failure.Equal(expectedFailure) {
			return fmt.Errorf("terminal barrier outcome failure contradicts delivery")
		}
		history, err := runfork.NewTerminalBarrierHistory(snapshot.RunID, snapshot.Revision, *barrier, *exact)
		if err != nil {
			return err
		}
		matched := false
		for i := range pending {
			if pending[i].DeliveryID != exact.DeliveryID {
				continue
			}
			if matched {
				return fmt.Errorf("terminal barrier has duplicate pending projection")
			}
			projected := pending[i]
			projected.TerminalBarrierHistory = &history
			if !projected.RetainsTerminalBarrierHistory() {
				return fmt.Errorf("terminal barrier pending projection contradicts its outcome")
			}
			if _, duplicate := histories[i]; duplicate {
				return fmt.Errorf("terminal barrier delivery has multiple owners")
			}
			histories[i] = history
			matched = true
		}
		if !matched {
			return fmt.Errorf("terminal barrier recipient missing from pending projection")
		}
	}
	for i, history := range histories {
		pending[i].TerminalBarrierHistory = &history
	}
	return nil
}
