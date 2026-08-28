// Package fanoutbarrier owns the durable terminal-disposition barrier for one
// exact fan-out intent. It deliberately does not own fan-out enumeration,
// delivery lifecycle, routing, or generic schedule execution.
package fanoutbarrier

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

type Disposition string

const (
	DispositionSucceeded        Disposition = "succeeded"
	DispositionDeadLettered     Disposition = "dead_lettered"
	DispositionNoRoute          Disposition = "no_route"
	DispositionSemanticRejected Disposition = "semantic_rejected"
	DispositionCanceled         Disposition = "canceled"
)

func (d Disposition) Valid() bool {
	switch d {
	case DispositionSucceeded, DispositionDeadLettered, DispositionNoRoute, DispositionSemanticRejected, DispositionCanceled:
		return true
	default:
		return false
	}
}

type Summary struct {
	Total            int `json:"total"`
	Succeeded        int `json:"succeeded"`
	DeadLettered     int `json:"dead_lettered"`
	NoRoute          int `json:"no_route"`
	SemanticRejected int `json:"semantic_rejected"`
	Canceled         int `json:"canceled"`
}

// Fold is the selected-store interpretation of one intent's direct ordinal
// facts. PendingCommitted counts committed publications whose canonical route
// or delivery lifecycle has not reached a terminal disposition yet.
type Fold struct {
	Summary           Summary
	EnumerationClosed bool
	PendingCommitted  int
}

func (f Fold) Terminal() bool {
	return f.EnumerationClosed && f.PendingCommitted == 0 && f.Summary.Settled() == f.Summary.Total
}

func (f Fold) Validate() error {
	if f.Summary.Total < 0 || f.Summary.Succeeded < 0 || f.Summary.DeadLettered < 0 || f.Summary.NoRoute < 0 ||
		f.Summary.SemanticRejected < 0 || f.Summary.Canceled < 0 || f.PendingCommitted < 0 {
		return fmt.Errorf("fan-out barrier fold counts cannot be negative")
	}
	classified := f.Summary.Settled() + f.PendingCommitted
	if classified > f.Summary.Total {
		return fmt.Errorf("fan-out barrier fold classifies %d ordinals, want at most %d", classified, f.Summary.Total)
	}
	if f.EnumerationClosed && classified != f.Summary.Total {
		return fmt.Errorf("closed fan-out barrier fold classifies %d ordinals, want %d", classified, f.Summary.Total)
	}
	if f.Terminal() {
		return f.Summary.Validate()
	}
	return nil
}

func (s Summary) Settled() int {
	return s.Succeeded + s.DeadLettered + s.NoRoute + s.SemanticRejected + s.Canceled
}

func (s Summary) Validate() error {
	if s.Total < 0 || s.Succeeded < 0 || s.DeadLettered < 0 || s.NoRoute < 0 || s.SemanticRejected < 0 || s.Canceled < 0 {
		return fmt.Errorf("fan-out barrier disposition counts cannot be negative")
	}
	if s.Settled() != s.Total {
		return fmt.Errorf("fan-out barrier disposition counts sum to %d, want cardinality %d", s.Settled(), s.Total)
	}
	return nil
}

func (s Summary) Context() map[string]any {
	return map[string]any{
		"total": s.Total,
		"dispositions": map[string]any{
			"succeeded":         s.Succeeded,
			"dead_lettered":     s.DeadLettered,
			"no_route":          s.NoRoute,
			"semantic_rejected": s.SemanticRejected,
			"canceled":          s.Canceled,
		},
	}
}

func SummaryFromContext(raw map[string]any) (Summary, error) {
	dispositions, ok := raw["dispositions"].(map[string]any)
	if !ok || !exactKeys(raw, "total", "dispositions") || !exactKeys(dispositions, "succeeded", "dead_lettered", "no_route", "semantic_rejected", "canceled") {
		return Summary{}, fmt.Errorf("fan-out barrier join context requires only total and the five canonical dispositions")
	}
	summary := Summary{
		Total:            exactInt(raw["total"]),
		Succeeded:        exactInt(dispositions["succeeded"]),
		DeadLettered:     exactInt(dispositions["dead_lettered"]),
		NoRoute:          exactInt(dispositions["no_route"]),
		SemanticRejected: exactInt(dispositions["semantic_rejected"]),
		Canceled:         exactInt(dispositions["canceled"]),
	}
	if err := summary.Validate(); err != nil {
		return Summary{}, err
	}
	return summary, nil
}

type Status string

const (
	StatusArmed                          Status = "armed"
	StatusClosedPending                  Status = "closed_pending"
	StatusFired                          Status = "fired"
	StatusOutcomeDeadLettered            Status = "outcome_dead_lettered"
	StatusSuppressedRunTerminal          Status = "outcome_suppressed_run_terminal"
	StatusSuppressedGenerationSuperseded Status = "outcome_suppressed_generation_superseded"
)

func (s Status) Valid() bool {
	switch s {
	case StatusArmed, StatusClosedPending, StatusFired, StatusOutcomeDeadLettered, StatusSuppressedRunTerminal, StatusSuppressedGenerationSuperseded:
		return true
	default:
		return false
	}
}

func (s Status) BlocksCompletion() bool { return s == StatusArmed || s == StatusClosedPending }
func (s Status) Terminal() bool         { return s.Valid() && !s.BlocksCompletion() }

type Registration struct {
	IntentKey     fanoutobligation.IntentKey
	PlanRef       runtimecontracts.FanOutPlanRef
	Handle        timeridentity.TimerHandle
	Route         flowidentity.Route
	EntityID      string
	RoutingSource events.RoutingSource
	ExecutionMode executionmode.Mode
	CreatedAt     time.Time
}

func (r Registration) Validate() error {
	if err := r.IntentKey.Validate(); err != nil {
		return err
	}
	if r.PlanRef.ElementRef != r.IntentKey.ElementRef || strings.TrimSpace(r.PlanRef.BundleHash) == "" || strings.TrimSpace(r.PlanRef.SemanticDigest) == "" {
		return fmt.Errorf("fan-out barrier plan identity is incomplete or contradicts its exact intent")
	}
	joinRef, ok := r.Handle.JoinRef()
	if !ok || r.Handle.Kind() != timeridentity.TimerHandleJoinComplete || joinRef.Mode() != timeridentity.JoinRefModeFanOutDelivery {
		return fmt.Errorf("fan-out barrier registration requires a typed delivery completion handle")
	}
	fanOut, ok := joinRef.FanOutDelivery()
	if !ok || fanOut.TriggeringDeliveryID() != strings.TrimSpace(r.IntentKey.TriggeringDeliveryID) ||
		fanOut.PackageKey() != strings.TrimSpace(r.IntentKey.ElementRef.PackageKey) ||
		fanOut.ElementID() != strings.TrimSpace(r.IntentKey.ElementRef.ElementID) ||
		fanOut.BundleHash() != strings.TrimSpace(r.PlanRef.BundleHash) ||
		fanOut.SemanticDigest() != strings.TrimSpace(r.PlanRef.SemanticDigest) {
		return fmt.Errorf("fan-out barrier handle disagrees with exact intent identity")
	}
	if !r.Route.Valid() || strings.TrimSpace(r.EntityID) == "" || !r.ExecutionMode.Valid() || r.CreatedAt.IsZero() {
		return fmt.Errorf("fan-out barrier registration requires exact route, entity, execution mode, and creation time")
	}
	route := r.RoutingSource.Route().Normalized()
	if joinRef.FlowID() == "" {
		if r.RoutingSource.Kind() != events.RoutingSourceRoot || route.EntityID != strings.TrimSpace(r.EntityID) || route.FlowID != "" || route.FlowInstance != "" {
			return fmt.Errorf("root fan-out barrier routing source contradicts its declaration")
		}
	} else if r.RoutingSource.Kind() != events.RoutingSourceFlowOwnedControl ||
		route.EntityID != strings.TrimSpace(r.EntityID) || route.FlowID != joinRef.FlowID() || route.FlowInstance != strings.Trim(strings.TrimSpace(r.Route.InstancePath), "/") {
		return fmt.Errorf("flow fan-out barrier routing source contradicts its declaration and route")
	}
	return nil
}

type Barrier struct {
	Registration         Registration
	Status               Status
	Summary              *Summary
	ScheduleKey          string
	ScheduleActivationID string
	UpdatedAt            time.Time
}

type Completion struct {
	Handle  timeridentity.TimerHandle
	Summary Summary
}

func (c Completion) Validate() error {
	joinRef, ok := c.Handle.JoinRef()
	if !ok || c.Handle.Kind() != timeridentity.TimerHandleJoinComplete || joinRef.Mode() != timeridentity.JoinRefModeFanOutDelivery {
		return fmt.Errorf("fan-out barrier completion requires its typed completion handle")
	}
	if _, ok := joinRef.FanOutDelivery(); !ok {
		return fmt.Errorf("fan-out barrier completion requires exact intent identity")
	}
	return c.Summary.Validate()
}

func (c Completion) IntentKey(runID string) (fanoutobligation.IntentKey, error) {
	if err := c.Validate(); err != nil {
		return fanoutobligation.IntentKey{}, err
	}
	ref, _ := c.Handle.JoinRef()
	fanOut, _ := ref.FanOutDelivery()
	key := fanoutobligation.IntentKey{
		RunID:                strings.TrimSpace(runID),
		TriggeringDeliveryID: fanOut.TriggeringDeliveryID(),
	}
	key.ElementRef.PackageKey = fanOut.PackageKey()
	key.ElementRef.ElementID = fanOut.ElementID()
	return key, key.Validate()
}

func (b Barrier) Validate() error {
	if err := b.Registration.Validate(); err != nil {
		return err
	}
	if !b.Status.Valid() || b.UpdatedAt.IsZero() || b.UpdatedAt.Before(b.Registration.CreatedAt) {
		return fmt.Errorf("fan-out barrier requires valid state and timestamps")
	}
	scheduleKey := strings.TrimSpace(b.ScheduleKey)
	activationID := strings.TrimSpace(b.ScheduleActivationID)
	if activationID != "" {
		if _, err := uuid.Parse(activationID); err != nil {
			return fmt.Errorf("fan-out barrier schedule activation identity is invalid")
		}
		if scheduleKey == "" {
			return fmt.Errorf("fan-out barrier schedule activation requires its exact schedule key")
		}
	}
	if b.Status == StatusArmed {
		if b.Summary != nil || scheduleKey != "" || activationID != "" {
			return fmt.Errorf("armed fan-out barrier cannot carry a terminal summary or schedule")
		}
		return nil
	}
	if b.Status == StatusSuppressedGenerationSuperseded && b.Summary == nil {
		if scheduleKey != "" || activationID != "" {
			return fmt.Errorf("unscheduled superseded fan-out barrier cannot carry schedule identity")
		}
		return nil
	}
	if b.Summary == nil {
		return fmt.Errorf("closed fan-out barrier requires its exact terminal summary")
	}
	if (b.Status == StatusClosedPending || b.Status == StatusFired || b.Status == StatusOutcomeDeadLettered) && scheduleKey == "" {
		return fmt.Errorf("scheduled fan-out barrier state requires its exact schedule key")
	}
	if b.Status == StatusClosedPending && activationID == "" {
		return fmt.Errorf("pending fan-out barrier state requires its exact schedule activation")
	}
	return b.Summary.Validate()
}

type RunSummary struct {
	RunID         string `json:"run_id"`
	Intents       int    `json:"intents"`
	Armed         int    `json:"armed"`
	ClosedPending int    `json:"closed_pending"`
	Terminal      int    `json:"terminal"`
}

func (s RunSummary) BlocksCompletion() bool { return s.Armed > 0 || s.ClosedPending > 0 }

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" || s.Intents < 0 || s.Armed < 0 || s.ClosedPending < 0 || s.Terminal < 0 || s.Intents != s.Armed+s.ClosedPending+s.Terminal {
		return fmt.Errorf("fan-out barrier run summary is invalid")
	}
	return nil
}

func exactKeys(values map[string]any, keys ...string) bool {
	if len(values) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := values[key]; !ok {
			return false
		}
	}
	return true
}

func exactInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		if typed == float64(int(typed)) {
			return int(typed)
		}
	}
	return -1
}
