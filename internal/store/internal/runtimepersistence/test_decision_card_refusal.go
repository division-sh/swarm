package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

// SupersedeDecisionCardAnchorForTest pins a hostile pending-card/superseded-anchor
// cut under the selected writer. It is not a production lifecycle operation.
func SupersedeDecisionCardAnchorForTest(ctx context.Context, selected any, card decisioncard.Card, now time.Time) error {
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.BundleHash() != card.BundleHash || card.Status != decisioncard.StatusPending {
		return fmt.Errorf("superseded anchor fixture requires an exact pending card/source")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres fixture store is required")
		}
		return store.decisionPostgresOwner.SupersedeDecisionCardAnchorForTest(ctx, card, now)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite fixture store is required")
		}
		return store.decisionSQLiteOwner.SupersedeDecisionCardAnchorForTest(ctx, card, now)
	default:
		return fmt.Errorf("unsupported decision-card fixture store %T", selected)
	}
}
