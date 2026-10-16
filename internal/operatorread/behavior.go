package operatorread

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

const (
	AgentUsageAccountingExact              = "exact"
	AgentUsageAccountingEstimated          = "estimated"
	DefaultAgentDeliveryDiagnosticsLimit   = 50
	MaxAgentDeliveryDiagnosticsLimit       = 200
	DefaultAgentDeliveryLifecycleLimit     = 50
	MaxAgentDeliveryLifecycleLimit         = 200
	DefaultPendingAgentDeliveryDetailLimit = 50
	MaxPendingAgentDeliveryDetailLimit     = 500
	MaxAgentDiagnosisQueueLimit            = 200
)

func NewOperatorEventFull(event events.Event) (OperatorEventFull, error) {
	payload := map[string]any{}
	if raw := event.Payload(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			return OperatorEventFull{}, fmt.Errorf("decode operator event payload: %w", err)
		}
	}
	operatorReferenceEventID := ""
	if provenance, ok := event.OperatorReference(); ok {
		operatorReferenceEventID = provenance.ReferencedEventID()
	}
	var inheritedOrigin *events.InheritedFanOutOrigin
	if origin, ok := event.InheritedFanOutOrigin(); ok {
		inheritedOrigin = &origin
	}
	return OperatorEventFull{
		EventID: event.ID(), EventName: strings.TrimSpace(string(event.Type())), ExecutionMode: event.ExecutionMode(),
		EntityID: event.EntityID(), RunID: event.RunID(), SourceEventID: event.ParentEventID(),
		InheritedFanOutOrigin:    inheritedOrigin,
		OperatorReferenceEventID: operatorReferenceEventID, CreatedAt: event.CreatedAt(), Source: event.SourceAgent(),
		ProducerType: event.ProducerType(), Payload: payload, Deliveries: []OperatorEventDelivery{},
		DeadLetters: []OperatorDeadLetterRecord{}, event: event.Clone(),
	}, nil
}

func (e OperatorEventFull) EventSnapshot() (events.Event, error) {
	if strings.TrimSpace(e.event.ID()) == "" {
		var empty events.Event
		return empty, fmt.Errorf("operator event %s has no canonical event snapshot", strings.TrimSpace(e.EventID))
	}
	return e.event.Clone(), nil
}

func EnrichOperatorEventFailureEvidence(event OperatorEventFull) OperatorEventFull {
	event.Deliveries = EnrichOperatorDeliveryFailureEvidence(event.Deliveries, event.DeadLetters)
	if event.DeadLetters == nil {
		event.DeadLetters = []OperatorDeadLetterRecord{}
	}
	return event
}

func EnrichOperatorDeliveryFailureEvidence(deliveries []OperatorEventDelivery, deadLetters []OperatorDeadLetterRecord) []OperatorEventDelivery {
	out := make([]OperatorEventDelivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		delivery.Status = strings.TrimSpace(delivery.Status)
		delivery.ReasonCode = strings.TrimSpace(delivery.ReasonCode)
		if delivery.Status == "dead_letter" {
			delivery.DeadLetters = nil
			for _, record := range deadLetters {
				if record.DeliveryID == delivery.DeliveryID && record.ClaimVersion == delivery.ClaimVersion {
					delivery.DeadLetters = append(delivery.DeadLetters, record)
				}
			}
		}
		out = append(out, delivery)
	}
	if out == nil {
		return []OperatorEventDelivery{}
	}
	return out
}

func ProjectRunOperationalStatus(report RunDebugReport) RunOperationalStatus {
	out := RunOperationalStatus{
		Heuristics: runOperationalStatusHeuristics(report),
		Instances:  []RunInstanceOperationalStatus{}, Attributions: []RunTurnAttribution{},
	}
	status := strings.ToLower(strings.TrimSpace(report.RunTableStatus))
	if status == "" {
		return out
	}
	if status != "running" {
		out.State = status
		out.Instances, _, _ = projectRunInstances(report.OperationalFacts)
		if report.OperationalFacts != nil {
			for _, attribution := range report.OperationalFacts.HistoricalTurns {
				if attribution.Historical {
					out.Attributions = append(out.Attributions, detachedRunAttribution(attribution))
				}
			}
		}
		return out
	}
	activeDeliveries := 0
	for _, item := range report.Deliveries {
		switch strings.ToLower(strings.TrimSpace(item.Status)) {
		case "pending", "in_progress":
			activeDeliveries += item.Count
		}
	}
	var busy bool
	out.Instances, out.Attributions, busy = projectRunInstances(report.OperationalFacts)
	work := report.TestQuiescence
	busy = busy || activeDeliveries != 0 || work.ActiveDeliveries != 0 || work.UnsettledPipelineEvents != 0 ||
		work.FanOutOwed != 0 || work.FanOutUnsettled != 0 || work.FanOutBarriers != 0 || work.DueTimers != 0 || work.ActiveSessionLeases != 0
	stalled := false
	for _, instance := range out.Instances {
		stalled = stalled || instance.Disposition == "stalled"
	}
	if !busy && stalled {
		out.State, out.BlockingLayer, out.BlockingReason = "stalled", "delivery_lifecycle", "no_active_deliveries"
		return out
	}
	out.State = "running"
	return out
}

func runOperationalStatusHeuristics(report RunDebugReport) []string {
	if len(report.DeadLetters) == 0 {
		return []string{}
	}
	return []string{"dead letters exist for this run"}
}

func AgentLifecycleBlockingLayer(state runtimedelivery.State) string {
	switch state {
	case runtimedelivery.StateQueued:
		return "delivery_queue"
	case runtimedelivery.StateLaunching:
		return "session_launch"
	case runtimedelivery.StateActive:
		return "session_execution"
	case runtimedelivery.StateRetrying:
		return "delivery_retry"
	case runtimedelivery.StateExhausted:
		return "delivery_terminal"
	default:
		return ""
	}
}
