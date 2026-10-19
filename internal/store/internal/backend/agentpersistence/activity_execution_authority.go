package agentpersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

// AuthorizeActivityExecutionTx shares the canonical admitted-generation/run
// fence. It does not construct a flow, agent, or alternative execution grant.
func AuthorizeActivityExecutionTx(ctx context.Context, tx *sql.Tx, binding processbinding.Binding, runID string, sqlite bool) error {
	return authorizeDynamicFlowGrantTx(ctx, tx, binding, runID, sqlite, flowGrantExecution)
}
