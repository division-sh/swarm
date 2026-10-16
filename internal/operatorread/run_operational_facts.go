package operatorread

import (
	"slices"
	"time"
)

type RunEmissionEvidence string

const (
	RunEmissionUnknown  RunEmissionEvidence = "unknown"
	RunEmissionAccepted RunEmissionEvidence = "accepted"
	RunEmissionNone     RunEmissionEvidence = "none"
)

type RunAcceptedEmission struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
}

// RunTurnAttribution is detached historical evidence, never execution authority.
type RunTurnAttribution struct {
	FlowInstance     string                `json:"flow_instance"`
	AgentID          string                `json:"agent_id,omitempty"`
	EventID          string                `json:"event_id,omitempty"`
	EventType        string                `json:"event_type,omitempty"`
	DeliveryID       string                `json:"delivery_id,omitempty"`
	LogicalTurnID    string                `json:"logical_turn_id,omitempty"`
	ProviderTurns    []string              `json:"provider_turns"`
	GrantedEmitTools []string              `json:"granted_emit_tools"`
	EmittedEvents    []RunAcceptedEmission `json:"emitted_events"`
	Emission         RunEmissionEvidence   `json:"emission_evidence"`
	EvidenceReason   string                `json:"evidence_reason,omitempty"`
	Historical       bool                  `json:"historical"`
}

type RunInstanceWork struct {
	Deliveries int `json:"deliveries"`
	Retries    int `json:"retries"`
	Pipeline   int `json:"pipeline"`
	FanOut     int `json:"fan_out"`
	Barriers   int `json:"barriers"`
	Timers     int `json:"timers"`
	Await      int `json:"await"`
	Sessions   int `json:"sessions"`
}

func (w RunInstanceWork) active() bool {
	return w.Deliveries != 0 || w.Retries != 0 || w.Pipeline != 0 || w.FanOut != 0 || w.Barriers != 0 || w.Sessions != 0
}

func (w RunInstanceWork) waiting() bool { return w.Timers != 0 || w.Await != 0 }

type RunInstanceFacts struct {
	FlowInstance string
	Stage        string
	Revision     int64
	Final        bool
	Complete     bool
	Work         RunInstanceWork
	LastTurn     *RunTurnAttribution
	Retired      bool
}

// RunOperationalFacts must be assembled within one selected-store snapshot.
// Incomplete facts may explain uncertainty but cannot prove quiescence.
type RunOperationalFacts struct {
	ObservedAt      time.Time
	LastProgressAt  time.Time
	Complete        bool
	UnscopedWork    RunInstanceWork
	Instances       []RunInstanceFacts
	HistoricalTurns []RunTurnAttribution
}

type RunInstanceOperationalStatus struct {
	FlowInstance string          `json:"flow_instance"`
	Stage        string          `json:"stage,omitempty"`
	Revision     int64           `json:"revision"`
	Disposition  string          `json:"disposition"`
	Work         RunInstanceWork `json:"work"`
}

func projectRunInstances(facts *RunOperationalFacts) ([]RunInstanceOperationalStatus, []RunTurnAttribution, bool) {
	instances := make([]RunInstanceOperationalStatus, 0)
	attributions := make([]RunTurnAttribution, 0)
	if facts == nil {
		return instances, attributions, false
	}
	busy := !facts.Complete || facts.UnscopedWork.active() || facts.UnscopedWork.waiting()
	for _, historical := range facts.HistoricalTurns {
		if historical.Historical {
			attributions = append(attributions, detachedRunAttribution(historical))
		}
	}
	for _, instance := range facts.Instances {
		disposition := "stalled"
		switch {
		case !instance.Complete:
			disposition, busy = "unknown", true
		case instance.Work.active():
			disposition, busy = "active", true
		case instance.Work.waiting():
			disposition, busy = "waiting", true
		case instance.Final:
			disposition = "final"
		case instance.Retired:
			disposition = "retired"
		case facts.UnscopedWork.active() || facts.UnscopedWork.waiting():
			disposition, busy = "unknown", true
		}
		instances = append(instances, RunInstanceOperationalStatus{
			FlowInstance: instance.FlowInstance, Stage: instance.Stage, Revision: instance.Revision, Disposition: disposition, Work: instance.Work,
		})
		if disposition != "stalled" {
			continue
		}
		attribution := RunTurnAttribution{FlowInstance: instance.FlowInstance, Emission: RunEmissionUnknown, EvidenceReason: "settled_origin_unproven"}
		if instance.LastTurn != nil && instance.LastTurn.FlowInstance == instance.FlowInstance && !instance.LastTurn.Historical {
			attribution = *instance.LastTurn
		}
		attributions = append(attributions, detachedRunAttribution(attribution))
	}
	slices.SortFunc(instances, func(a, b RunInstanceOperationalStatus) int {
		return compareFlowInstance(a.FlowInstance, b.FlowInstance)
	})
	slices.SortStableFunc(attributions, func(a, b RunTurnAttribution) int { return compareFlowInstance(a.FlowInstance, b.FlowInstance) })
	return instances, attributions, busy
}

func detachedRunAttribution(attribution RunTurnAttribution) RunTurnAttribution {
	attribution.ProviderTurns = append([]string{}, attribution.ProviderTurns...)
	attribution.GrantedEmitTools = append([]string{}, attribution.GrantedEmitTools...)
	attribution.EmittedEvents = append([]RunAcceptedEmission{}, attribution.EmittedEvents...)
	return attribution
}

func compareFlowInstance(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
