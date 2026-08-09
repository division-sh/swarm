package timerobligation

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Family string

const (
	FamilyTimer           Family = "timer"
	FamilyScheduledTask   Family = "scheduled_task"
	FamilyGlobalRecurring Family = "global_recurring"
	FamilyWorkflowTimer   Family = "workflow_timer"
)

var families = [...]Family{
	FamilyTimer,
	FamilyScheduledTask,
	FamilyGlobalRecurring,
	FamilyWorkflowTimer,
}

func ParseFamily(value string) (Family, error) {
	family := Family(strings.TrimSpace(value))
	for _, candidate := range families {
		if family == candidate {
			return family, nil
		}
	}
	return "", fmt.Errorf("unknown timer family %q", value)
}

func AllFamilies() []Family {
	return append([]Family(nil), families[:]...)
}

type Scope struct {
	runID string
}

func All() Scope {
	return Scope{}
}

func Run(runID string) (Scope, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return Scope{}, fmt.Errorf("timer obligation run scope requires run_id")
	}
	return Scope{runID: runID}, nil
}

func (s Scope) RunID() string {
	return s.runID
}

type FamilyObligation struct {
	Family           Family `json:"family"`
	ActiveCount      int    `json:"active_count"`
	DueCount         int    `json:"due_count"`
	RecoverableCount int    `json:"recoverable_count"`
}

type RunObligations struct {
	RunID    string             `json:"run_id"`
	Families []FamilyObligation `json:"families"`
}

type RunSummary struct {
	RunID      string
	Active     int
	Due        int
	ObservedAt time.Time
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("timer run summary requires run_id")
	}
	if s.Active < 0 || s.Due < 0 || s.Due > s.Active {
		return fmt.Errorf("timer run summary counts are invalid")
	}
	if s.ObservedAt.IsZero() {
		return fmt.Errorf("timer run summary requires selected-store observation time")
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.Active > 0
}

func (r RunObligations) Summary(observedAt time.Time) RunSummary {
	total := r.Totals()
	return RunSummary{
		RunID:      r.RunID,
		Active:     total.ActiveCount,
		Due:        total.DueCount,
		ObservedAt: observedAt.UTC(),
	}
}

type Snapshot struct {
	ObservedAt     time.Time          `json:"observed_at"`
	GlobalFamilies []FamilyObligation `json:"global_families"`
	Runs           []RunObligations   `json:"runs"`
	Activations    []Activation       `json:"activations"`
}

type Activation struct {
	ActivationID              string    `json:"activation_id"`
	Family                    Family    `json:"family"`
	RunID                     string    `json:"run_id,omitempty"`
	Status                    string    `json:"status"`
	DueAt                     time.Time `json:"due_at"`
	InitialDueAt              time.Time `json:"initial_due_at,omitempty"`
	OccurrenceEventID         string    `json:"occurrence_event_id,omitempty"`
	OccurrenceEventAdmittedAt time.Time `json:"occurrence_event_admitted_at,omitempty"`
	AcceptedAt                time.Time `json:"accepted_at,omitempty"`
	CancelCause               string    `json:"cancel_cause,omitempty"`
	CancelledAt               time.Time `json:"cancelled_at,omitempty"`
	FailureCode               string    `json:"failure_code,omitempty"`
	FailureMessage            string    `json:"failure_message,omitempty"`
	FailedAt                  time.Time `json:"failed_at,omitempty"`
}

func (s Snapshot) Run(runID string) (RunObligations, bool) {
	runID = strings.TrimSpace(runID)
	for _, run := range s.Runs {
		if run.RunID == runID {
			return run, true
		}
	}
	return RunObligations{}, false
}

func (r RunObligations) Totals() FamilyObligation {
	var total FamilyObligation
	for _, family := range r.Families {
		total.ActiveCount += family.ActiveCount
		total.DueCount += family.DueCount
		total.RecoverableCount += family.RecoverableCount
	}
	return total
}

func (s Snapshot) GlobalTotals() FamilyObligation {
	var total FamilyObligation
	for _, family := range s.GlobalFamilies {
		total.ActiveCount += family.ActiveCount
		total.DueCount += family.DueCount
		total.RecoverableCount += family.RecoverableCount
	}
	return total
}

func ZeroFamilies() []FamilyObligation {
	out := make([]FamilyObligation, 0, len(families))
	for _, family := range families {
		out = append(out, FamilyObligation{Family: family})
	}
	return out
}

func SortedRunIDs(values map[string][]FamilyObligation) []string {
	ids := make([]string, 0, len(values))
	for runID := range values {
		ids = append(ids, runID)
	}
	sort.Strings(ids)
	return ids
}
