package channelonboarding

import (
	"context"
	"database/sql"
	"fmt"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	nativeinput "github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	identityowner "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

// RequireSessionBusinessInputTx fences the original owners in the publication
// transaction. It never performs root-store reads or provider I/O under SQL ownership.
func RequireSessionBusinessInputTx(ctx context.Context, tx *sql.Tx, postgres bool, input nativeinput.Admission) error {
	if tx == nil || input.Scope() != domain.SessionInputBusiness || !input.LifetimeCurrent(ctx) {
		return fmt.Errorf("current owned native business input is required")
	}
	d := dialectSQLite
	if postgres {
		d = dialectPostgres
	}
	expected := input.Responsibility()
	op, found, err := loadOperation(ctx, tx, d, expected.OperationID, true)
	if err != nil {
		return err
	}
	if !found || !expected.MatchesOperation(op) {
		return domain.ErrRevisionConflict
	}
	// Preserve confirmation's parent-before-principal order and the shared
	// principal-before-binding/activation fence used by retirement and unbind.
	if err := channeldelivery.LockPrincipalTx(ctx, tx, op.PrincipalID, postgres); err != nil {
		return err
	}
	binding, found, err := identityowner.LockBindingTx(ctx, tx, postgres, op.Interface)
	if err != nil {
		return err
	}
	if !found {
		return domain.ErrRevisionConflict
	}
	activation, found, err := loadActivationBySlot(ctx, tx, d, op.SlotKey, true)
	if err != nil {
		return err
	}
	if !found || !expected.MatchesBusinessBinding(op, activation, binding, input.BindingRevision()) || !input.LifetimeCurrent(ctx) {
		return domain.ErrRevisionConflict
	}
	return nil
}
