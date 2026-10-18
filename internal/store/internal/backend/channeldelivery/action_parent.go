package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

// This is a query constraint, not authority. Mutation admission still checks
// the complete action join and intent under the original transaction.
type actionParent struct {
	operationID, activationID           string
	activationRevision, bindingRevision int64
}

func discoverActionParentTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (actionParent, error) {
	resolved, found, err := ResolveActionFactTx(ctx, tx, fact, postgres)
	if err != nil {
		return actionParent{}, err
	}
	if !found {
		return actionParent{}, fmt.Errorf("channel action is no longer current")
	}
	query := `SELECT operation_id FROM connected_channel_activations WHERE activation_id=?`
	if postgres {
		query = `SELECT operation_id::text FROM connected_channel_activations WHERE activation_id=$1::uuid`
	}
	parent := actionParent{activationID: resolved.ActivationID, activationRevision: resolved.ActivationRevision, bindingRevision: resolved.BindingRevision}
	if err := tx.QueryRowContext(ctx, query, resolved.ActivationID).Scan(&parent.operationID); err != nil {
		return actionParent{}, err
	}
	return parent, nil
}

func lockActionParentTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (actionParent, error) {
	parent, err := discoverActionParentTx(ctx, tx, fact, postgres)
	if err != nil {
		return actionParent{}, err
	}
	query := `SELECT operation_id FROM channel_onboarding_operations WHERE operation_id=?`
	if postgres {
		query = `SELECT operation_id::text FROM channel_onboarding_operations WHERE operation_id=$1::uuid FOR UPDATE`
	}
	var locked string
	if err := tx.QueryRowContext(ctx, query, parent.operationID).Scan(&locked); err != nil {
		return actionParent{}, err
	}
	if locked != parent.operationID {
		return actionParent{}, fmt.Errorf("channel action parent changed")
	}
	return parent, nil
}

func (parent actionParent) resolve(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (render.ResolvedAction, bool, error) {
	return resolveActionFactTx(ctx, tx, fact, postgres, true, &parent)
}
