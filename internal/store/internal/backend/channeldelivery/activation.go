package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
)

// CurrentActivationID follows the binding operation that selected the default.
// Merely matching interface and revision cannot select an arbitrary sibling
// context's activation for a provider write.
func CurrentActivationID(ctx context.Context, tx *sql.Tx, postgres bool) (string, bool, error) {
	if tx == nil {
		return "", false, fmt.Errorf("channel delivery activation requires a transaction")
	}
	query := `SELECT activation.activation_id
		FROM channel_delivery_defaults selected
		JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
		JOIN channel_onboarding_operations onboarding ON onboarding.identity_operation_id=binding.operation_id
		JOIN connected_channel_activations activation ON activation.operation_id=onboarding.operation_id
		WHERE selected.singleton_id=1 AND selected.state='current'
		AND binding.status='current' AND binding.principal_id=selected.principal_id
		AND binding.binding_revision=selected.binding_revision
		AND binding.external_account_reference=selected.external_account_reference
		AND binding.conversation_reference=selected.conversation_reference
		AND binding.conversation_scope=selected.conversation_scope
		AND onboarding.phase='succeeded'
		AND activation.status='current' AND activation.principal_id=selected.principal_id
		AND activation.interface_key=selected.interface_key
		AND activation.binding_revision=selected.binding_revision
		AND activation.conversation_reference=selected.conversation_reference
		LIMIT 2`
	if postgres {
		query = `SELECT activation.activation_id::text
			FROM channel_delivery_defaults selected
			JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
			JOIN channel_onboarding_operations onboarding ON onboarding.identity_operation_id=binding.operation_id
			JOIN connected_channel_activations activation ON activation.operation_id=onboarding.operation_id
			WHERE selected.singleton_id=1 AND selected.state='current'
			AND binding.status='current' AND binding.principal_id=selected.principal_id
			AND binding.binding_revision=selected.binding_revision
			AND binding.external_account_reference=selected.external_account_reference
			AND binding.conversation_reference=selected.conversation_reference
			AND binding.conversation_scope=selected.conversation_scope
			AND onboarding.phase='succeeded'
			AND activation.status='current' AND activation.principal_id=selected.principal_id
			AND activation.interface_key=selected.interface_key
			AND activation.binding_revision=selected.binding_revision
			AND activation.conversation_reference=selected.conversation_reference
			LIMIT 2`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	var id string
	for rows.Next() {
		if id != "" {
			return "", false, fmt.Errorf("selected channel delivery binding has multiple active activations")
		}
		if err := rows.Scan(&id); err != nil {
			return "", false, err
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return id, id != "", nil
}
