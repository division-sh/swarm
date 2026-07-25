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
	FamilyDeadline        Family = "deadline"
	FamilyGlobalRecurring Family = "global_recurring"
	FamilyWorkflowTimer   Family = "workflow_timer"
)

var families = [...]Family{
	FamilyTimer,
	FamilyScheduledTask,
	FamilyDeadline,
	FamilyGlobalRecurring,
	FamilyWorkflowTimer,
}

func parseFamily(value string) (Family, error) {
	family := Family(strings.TrimSpace(value))
	for _, candidate := range families {
		if family == candidate {
			return family, nil
		}
	}
	return "", fmt.Errorf("unknown timer family %q", value)
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

type Snapshot struct {
	ObservedAt     time.Time          `json:"observed_at"`
	GlobalFamilies []FamilyObligation `json:"global_families"`
	Runs           []RunObligations   `json:"runs"`
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

func zeroFamilies() []FamilyObligation {
	out := make([]FamilyObligation, 0, len(families))
	for _, family := range families {
		out = append(out, FamilyObligation{Family: family})
	}
	return out
}

func sortedRunIDs(values map[string][]FamilyObligation) []string {
	ids := make([]string, 0, len(values))
	for runID := range values {
		ids = append(ids, runID)
	}
	sort.Strings(ids)
	return ids
}
