package budgetspend

import (
	"context"
	"strings"
	"time"
)

type Scope string

const (
	ScopeSystem Scope = "system"
	ScopeGlobal Scope = "global"
	ScopeEntity Scope = "entity"
)

// Store is the backend-neutral owner for runtime budget/spend persistence.
// Runtime budget policy lives in BudgetTracker; this contract owns the
// persisted spend_ledger/entity_state reads and writes it needs.
type Store interface {
	RecordSpend(ctx context.Context, rec SpendRecord) error
	ResolveFlowInstance(ctx context.Context, runID string, entityID string) (string, error)
	ListActiveEntityIDs(ctx context.Context, runID string, terminalStates []string) ([]string, error)
	SumSpendUSD(ctx context.Context, query SpendQuery) (float64, error)
}

type SpendRecord struct {
	EntityID        string
	FlowInstance    string
	AgentID         string
	Model           string
	InputTokens     int
	OutputTokens    int
	CostUSD         float64
	InvocationType  string
	UsageAccounting string
	RecordedAt      time.Time
}

func (r SpendRecord) EffectiveEntityID() string {
	return strings.TrimSpace(r.EntityID)
}

func (r *SpendRecord) NormalizeEntityID() {
	if r == nil {
		return
	}
	r.EntityID = r.EffectiveEntityID()
}

type SpendQuery struct {
	Scope    Scope
	EntityID string
	Since    time.Time
}
