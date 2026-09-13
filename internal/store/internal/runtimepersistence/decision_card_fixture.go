package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

// DecisionCardDomainFixture is only for domain-level tests. Public human
// operations must enter the pipeline request lease and commit API completion.
type DecisionCardDomainFixture interface {
	ApplyDecisionForTest(context.Context, decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error)
	ApplyDeferralForTest(context.Context, decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error)
	BeginInputForTest(context.Context, decisioncard.BeginInputRequest) (decisioncard.InputDraft, error)
	CancelInputForTest(context.Context, decisioncard.CancelInputRequest) (decisioncard.InputDraft, error)
}

func DecisionCardDomainForTest(selected any) DecisionCardDomainFixture {
	switch s := selected.(type) {
	case *PostgresStore:
		return s.decisionPostgresOwner
	case *SQLiteRuntimeStore:
		return s.decisionSQLiteOwner
	default:
		panic(fmt.Sprintf("unsupported decision-card fixture store %T", selected))
	}
}
