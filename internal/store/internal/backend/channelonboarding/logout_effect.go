package channelonboarding

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

// LogoutEffectAuthorityCurrent consumes the existing strict teardown codec.
// Retired business admission cannot authorize a message, but the frozen logout
// responsibility can still admit its one original unlink.
func LogoutEffectAuthorityCurrent(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, a effects.Authority, postgres, lock bool) (bool, error) {
	if !a.Valid() || a.Kind != effects.AuthorityChannelLogout {
		return false, nil
	}
	d := logoutDialect(postgres)
	op, found, err := loadTeardown(ctx, q, d, a.ChannelLogout.TeardownID, lock)
	if err != nil || !found {
		return false, err
	}
	if !logoutEffectMatches(op, a) || op.Phase != domain.TeardownAuthorityRetired || op.Revision != a.ChannelLogout.TeardownRevision {
		return false, nil
	}
	retained, found, err := loadOperation(ctx, q, d, op.Logout.OperationID, false)
	if err != nil || !found {
		return false, err
	}
	target := op.Logout
	if retained.Phase != domain.PhaseRetired || retained.Revision != target.OperationRevision+1 ||
		retained.FailureCode != "logout_reserved" || retained.PrincipalID != op.PrincipalID ||
		retained.Interface.Normalized() != op.Scope.Interface.Normalized() || retained.Provider != target.Account.Provider ||
		retained.SessionConnectionID != target.Account.ConnectionID || retained.SessionAccount != target.Account ||
		retained.Coordinate != target.Coordinate || retained.TargetSelector != target.TargetSelector {
		return false, nil
	}
	var live int
	query := `SELECT COUNT(*) FROM connected_channel_activations a
		JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
		WHERE o.session_connection_id=? AND a.status='current'`
	if err := q.QueryRowContext(ctx, d.bind(query), target.Account.ConnectionID).Scan(&live); err != nil {
		return false, err
	}
	return live == 0, ctx.Err()
}

func logoutDialect(postgres bool) dialect {
	if postgres {
		return dialectPostgres
	}
	return dialectSQLite
}

func logoutEffectMatches(op domain.TeardownOperation, a effects.Authority) bool {
	if op.Kind != domain.TeardownLogout || op.Logout == nil || op.Logout.Validate() != nil {
		return false
	}
	raw, err := json.Marshal(op.Logout)
	l := a.ChannelLogout
	return err == nil && op.TeardownID == l.TeardownID && op.PrincipalID == l.PrincipalID &&
		a.ExecutionOwner == "channel-logout:"+op.Logout.OccurrenceID &&
		op.Logout.Coordinate.BundleHash == l.BundleHash && op.Logout.Coordinate.RuntimeInstanceID == l.RuntimeInstanceID &&
		effects.Fingerprint(raw) == l.TargetFingerprint
}

// SettleLogoutEffectTx is called only inside the existing effect settlement
// transaction, after exact journal evidence has been checked. Ordinary teardown
// completion intentionally cannot project this result.
func SettleLogoutEffectTx(ctx context.Context, tx *sql.Tx, s effects.Settlement, postgres bool) error {
	if !s.Authority.Valid() || s.Authority.Kind != effects.AuthorityChannelLogout ||
		s.OperationID != s.Authority.ChannelLogout.EffectOperationID || s.Now.IsZero() {
		return fmt.Errorf("%w: invalid logout effect settlement", domain.ErrConflict)
	}
	d := logoutDialect(postgres)
	op, found, err := loadTeardown(ctx, tx, d, s.Authority.ChannelLogout.TeardownID, true)
	if err != nil {
		return err
	}
	if !found || !logoutEffectMatches(op, s.Authority) {
		return domain.ErrConflict
	}
	phase, code, message, err := logoutEffectResult(s)
	if err != nil {
		return err
	}
	if op.Phase.Terminal() {
		if op.Revision != s.Authority.ChannelLogout.TeardownRevision+1 || op.Phase != phase || op.FailureCode != code || op.FailureMessage != message {
			return domain.ErrConflict
		}
		return nil
	}
	if op.Phase != domain.TeardownAuthorityRetired || op.Revision != s.Authority.ChannelLogout.TeardownRevision {
		return domain.ErrRevisionConflict
	}
	now := canonicalTime(s.Now)
	result, err := tx.ExecContext(ctx, d.bind(`UPDATE channel_onboarding_teardowns
		SET phase=?,teardown_revision=teardown_revision+1,failure_code=?,failure_message=?,updated_at=?,completed_at=?
		WHERE teardown_id=? AND teardown_revision=? AND phase='authority_retired'`),
		string(phase), nullable(code), nullable(message), now, now, op.TeardownID, op.Revision)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("%w: logout projection did not change its exact responsibility: %v", domain.ErrConflict, err)
	}
	return nil
}

func logoutEffectResult(s effects.Settlement) (domain.TeardownPhase, string, string, error) {
	switch s.State {
	case effects.StateSettled:
		if s.Evidence["remote_unlinked"] != true || s.Evidence["local_device_deleted"] != true || s.Evidence["original_joined"] != true {
			return "", "", "", fmt.Errorf("%w: logout success lacks unlink, deletion or complete join evidence", domain.ErrConflict)
		}
		return domain.TeardownSucceeded, "", "", nil
	case effects.StateOutcomeUncertain:
		return domain.TeardownFailed, "logout_outcome_uncertain", "Logout was launched; its complete outcome is unconfirmed and will not be resent", nil
	case effects.StateTerminalFailure:
		return domain.TeardownFailed, "logout_not_launched", "Logout did not launch; pairing evidence is retained", nil
	default:
		return "", "", "", fmt.Errorf("%w: unsupported logout effect result %q", domain.ErrConflict, s.State)
	}
}
