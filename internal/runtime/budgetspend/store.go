package budgetspend

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
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
	ListBudgetProjectionTargets(ctx context.Context, terminalStates []string) ([]ProjectionTarget, error)
	SumSpendUSD(ctx context.Context, query SpendQuery) (float64, error)
}

// ProjectionTarget identifies one active run-scoped entity whose retained
// spend must be reflected in runtime budget state during startup recovery.
type ProjectionTarget struct {
	RunID    string
	EntityID string
}

type SpendRecord struct {
	EntityID        string
	FlowInstance    string
	AgentID         string
	Model           string
	ModelAlias      string
	BackendProfile  string
	Provider        string
	Transport       string
	ResolvedModel   string
	InputTokens     int
	OutputTokens    int
	CostUSD         float64
	InvocationType  string
	UsageAccounting string
	ExecutionMode   executionmode.Mode
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
	LiveOnly bool
}
