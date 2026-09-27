package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

// Default is the selected-store delivery destination, not a selection from the
// currently available channel bindings. A retired row deliberately remains.
type Default struct {
	PrincipalID        string
	InterfaceKey       string
	BindingRevision    int64
	DeliveryEpoch      int64
	ExternalAccountRef string
	ConversationRef    string
	ConversationScope  operatorchannel.ConversationScope
	State              string
	FirstOperationID   string
}

const (
	StateCurrent = "current"
	StateRetired = "retired"
)

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// LockPrincipalTx is the ordering fence shared by binding confirmation and
// future notice admission. Callers acquire it before reading a binding/default.
func LockPrincipalTx(ctx context.Context, tx *sql.Tx, principalID string, postgres bool) error {
	if tx == nil || strings.TrimSpace(principalID) == "" {
		return fmt.Errorf("channel delivery default requires a transaction and principal")
	}
	current, found, err := LockCurrentPrincipalTx(ctx, tx, postgres)
	if err != nil {
		return err
	}
	if !found || current != principalID {
		return fmt.Errorf("channel delivery principal contradicts selected store")
	}
	return nil
}

// LockCurrentPrincipalTx also fences the not-yet-created principal. That case
// matters for a notice racing the first connection ceremony.
func LockCurrentPrincipalTx(ctx context.Context, tx *sql.Tx, postgres bool) (string, bool, error) {
	if tx == nil {
		return "", false, fmt.Errorf("channel delivery principal requires a transaction")
	}
	if postgres {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('operator_principal_singleton'))`); err != nil {
			return "", false, fmt.Errorf("lock channel delivery principal singleton: %w", err)
		}
	}
	query := `SELECT principal_id FROM operator_principals WHERE singleton_id = 1`
	if postgres {
		query += ` FOR UPDATE`
	}
	var current string
	if err := tx.QueryRowContext(ctx, query).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("lock channel delivery principal: %w", err)
	}
	if current == "" {
		return "", false, fmt.Errorf("stored channel delivery principal is empty")
	}
	return current, true, nil
}

func LoadDefault(ctx context.Context, db queryer, postgres bool) (Default, bool, error) {
	if db == nil {
		return Default{}, false, fmt.Errorf("channel delivery default store is required")
	}
	query := `SELECT principal_id, interface_key, binding_revision, delivery_epoch, external_account_reference,
		conversation_reference, conversation_scope, state, first_operation_id
		FROM channel_delivery_defaults WHERE singleton_id = 1`
	if postgres {
		query += ` FOR UPDATE`
	}
	var current Default
	var scope string
	err := db.QueryRowContext(ctx, query).Scan(&current.PrincipalID, &current.InterfaceKey, &current.BindingRevision, &current.DeliveryEpoch,
		&current.ExternalAccountRef, &current.ConversationRef, &scope, &current.State, &current.FirstOperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return Default{}, false, nil
	}
	if err != nil {
		return Default{}, false, err
	}
	current.ConversationScope = operatorchannel.ConversationScope(scope)
	if current.PrincipalID == "" || current.InterfaceKey == "" || current.BindingRevision < 1 || current.DeliveryEpoch < 1 ||
		current.ExternalAccountRef == "" || current.ConversationRef == "" || !current.ConversationScope.Valid() ||
		(current.State != StateCurrent && current.State != StateRetired) || current.FirstOperationID == "" {
		return Default{}, false, fmt.Errorf("stored channel delivery default is invalid")
	}
	return current, true, nil
}

// ApplyBindingTx materializes first-confirmed selection and follows only that
// interface's verified successor. Another connected interface cannot steal it.
func ApplyBindingTx(ctx context.Context, tx *sql.Tx, binding operatorchannel.Binding, kind operatorchannel.OperationKind, postgres bool) error {
	if tx == nil || binding.Status != operatorchannel.BindingCurrent || binding.Interface.Validate() != nil ||
		binding.PrincipalID == "" || binding.Revision < 1 || binding.ExternalAccountRef == "" ||
		binding.ConversationRef == "" || !binding.ConversationScope.Valid() || binding.OperationID == "" || binding.UpdatedAt.IsZero() {
		return fmt.Errorf("current channel delivery binding is incomplete")
	}
	if kind != operatorchannel.OperationConnect && kind != operatorchannel.OperationReconnect && kind != operatorchannel.OperationRebind {
		return fmt.Errorf("channel delivery default cannot consume %s binding", kind)
	}
	current, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil {
		return err
	}
	if !found {
		query := `INSERT INTO channel_delivery_defaults (singleton_id, principal_id, interface_key, binding_revision, delivery_epoch,
			external_account_reference, conversation_reference, conversation_scope, state, first_operation_id, updated_at)
			VALUES (1, ?, ?, ?, 1, ?, ?, ?, 'current', ?, ?)`
		if postgres {
			query = `INSERT INTO channel_delivery_defaults (singleton_id, principal_id, interface_key, binding_revision, delivery_epoch,
				external_account_reference, conversation_reference, conversation_scope, state, first_operation_id, updated_at)
				VALUES (1, $1, $2, $3, 1, $4, $5, $6, 'current', $7, $8)`
		}
		_, err = tx.ExecContext(ctx, query, binding.PrincipalID, binding.Interface.Key(), binding.Revision,
			binding.ExternalAccountRef, binding.ConversationRef, string(binding.ConversationScope), binding.OperationID, binding.UpdatedAt.UTC())
		if err != nil {
			return err
		}
		return PlanFirstSummaryTx(ctx, tx, Default{
			PrincipalID: binding.PrincipalID, InterfaceKey: binding.Interface.Key(), BindingRevision: binding.Revision,
			DeliveryEpoch: 1, ExternalAccountRef: binding.ExternalAccountRef,
			ConversationRef: binding.ConversationRef, ConversationScope: binding.ConversationScope,
			State: StateCurrent, FirstOperationID: binding.OperationID,
		}, postgres)
	}
	if current.PrincipalID != binding.PrincipalID {
		return fmt.Errorf("channel delivery default principal changed")
	}
	if current.InterfaceKey != binding.Interface.Key() {
		return nil
	}
	if binding.Revision <= current.BindingRevision {
		return fmt.Errorf("channel delivery default binding revision did not advance")
	}
	if current.State == StateCurrent && kind != operatorchannel.OperationRebind &&
		(current.ExternalAccountRef != binding.ExternalAccountRef || current.ConversationRef != binding.ConversationRef || current.ConversationScope != binding.ConversationScope) {
		return fmt.Errorf("channel delivery default changed without verified rebind")
	}
	epoch := current.DeliveryEpoch
	if current.State == StateRetired || kind == operatorchannel.OperationRebind {
		epoch++
	}
	query := `UPDATE channel_delivery_defaults SET binding_revision = ?, delivery_epoch = ?, external_account_reference = ?,
		conversation_reference = ?, conversation_scope = ?, state = 'current', updated_at = ? WHERE singleton_id = 1`
	if postgres {
		query = `UPDATE channel_delivery_defaults SET binding_revision = $1, delivery_epoch = $2, external_account_reference = $3,
			conversation_reference = $4, conversation_scope = $5, state = 'current', updated_at = $6 WHERE singleton_id = 1`
	}
	_, err = tx.ExecContext(ctx, query, binding.Revision, epoch, binding.ExternalAccountRef, binding.ConversationRef,
		string(binding.ConversationScope), binding.UpdatedAt.UTC())
	return err
}

func RetireBindingTx(ctx context.Context, tx *sql.Tx, binding operatorchannel.Binding, postgres bool) error {
	if tx == nil || binding.Status != operatorchannel.BindingUnbound || binding.Interface.Validate() != nil ||
		binding.PrincipalID == "" || binding.Revision < 1 || binding.UpdatedAt.IsZero() {
		return fmt.Errorf("retired channel delivery binding is incomplete")
	}
	current, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil || !found {
		return err
	}
	if current.PrincipalID != binding.PrincipalID {
		return fmt.Errorf("channel delivery default principal changed")
	}
	if current.InterfaceKey != binding.Interface.Key() {
		return nil
	}
	if current.State != StateCurrent || binding.Revision != current.BindingRevision+1 {
		return fmt.Errorf("channel delivery default retirement does not match current binding")
	}
	query := `UPDATE channel_delivery_defaults SET state = 'retired', binding_revision = ?, updated_at = ? WHERE singleton_id = 1`
	if postgres {
		query = `UPDATE channel_delivery_defaults SET state = 'retired', binding_revision = $1, updated_at = $2 WHERE singleton_id = 1`
	}
	_, err = tx.ExecContext(ctx, query, binding.Revision, binding.UpdatedAt.UTC())
	return err
}
