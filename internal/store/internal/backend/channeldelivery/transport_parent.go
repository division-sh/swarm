package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

// The parent is discovered without locks and revalidated under parent then
// principal. The bound identity child is immutable evidence, not a late lock.
func LockTransportParentTx(ctx context.Context, tx *sql.Tx, parent channelnative.TransportParent, postgres bool) error {
	if tx == nil || parent.Validate() != nil {
		return fmt.Errorf("channel transport parent is incomplete")
	}
	query := `SELECT operation_id FROM channel_onboarding_operations
		WHERE operation_id=(SELECT operation_id FROM connected_channel_activations WHERE activation_id=?)`
	if parent.Kind == channelnative.TransportDeploymentRegistration {
		query = `SELECT operation_id FROM deployment_channel_operations WHERE operation_id=? AND operation_revision=?`
	}
	args := []any{parent.ID}
	if parent.Kind == channelnative.TransportDeploymentRegistration {
		args = append(args, parent.Revision)
	}
	if postgres {
		if parent.Kind == channelnative.TransportDeploymentRegistration {
			query = `SELECT operation_id::text FROM deployment_channel_operations WHERE operation_id=$1::uuid AND operation_revision=$2 FOR UPDATE`
		} else {
			query = `SELECT operation_id::text FROM channel_onboarding_operations
				WHERE operation_id=(SELECT operation_id FROM connected_channel_activations WHERE activation_id=$1::uuid) FOR UPDATE`
		}
	}
	var id string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&id); err != nil {
		return err
	}
	return nil
}

// Registration selection is a distinct case, never a connected activation.
// Credentials/session possession and saved-proof currentness are additionally
// checked by the executable transport owner, outside these row locks.
const deploymentTransportRelations = `FROM deployment_channel_operations o
	JOIN operator_channel_operations child ON child.operation_id=o.identity_operation_id
	JOIN operator_channel_bindings b ON b.operation_id=child.operation_id`

const deploymentTransportConditions = `o.phase='succeeded' AND o.account_confirmed AND NOT o.logout_completed
	AND COALESCE(o.failure_code,'')='' AND o.binding_revision=b.binding_revision
	AND child.state='bound' AND child.onboarding_operation_id=o.operation_id
	AND child.principal_id=o.principal_id AND child.interface_key=o.interface_key
	AND child.binding_revision=b.binding_revision
	AND b.status='current' AND b.principal_id=o.principal_id AND b.interface_key=o.interface_key
	AND child.external_account=b.external_account AND child.conversation_reference=b.conversation_reference
	AND child.conversation_scope=b.conversation_scope
	AND child.provider_authority=b.provider_authority
	AND (b.provider_authority->>'parent_scope')='deployment'
	AND ((child.proof_id=b.proof_id AND child.proof_revision=b.proof_revision)
	 OR (child.proof_id IS NULL AND b.proof_id IS NULL AND child.proof_revision IS NULL AND b.proof_revision IS NULL))
	AND (b.proof_id IS NULL OR child.proof_status='active')`

const deploymentTransportJoin = deploymentTransportRelations + ` WHERE ` + deploymentTransportConditions

func bindingUsesDeployment(ctx context.Context, q queryer, key string) (bool, error) {
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT provider_authority FROM operator_channel_bindings WHERE interface_key=$1 AND status='current'`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var record operatorchannel.ProviderAuthorityRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return false, err
	}
	provider, err := record.Admit()
	return provider.Scope == authority.DeploymentParent, err
}

func deploymentBindingParent(ctx context.Context, q queryer, key string) (channelnative.TransportParent, bool, error) {
	query := `SELECT CAST(o.operation_id AS TEXT), o.operation_revision, CAST(child.operation_id AS TEXT), child.operation_revision,
		COALESCE(CAST(b.proof_id AS TEXT),''), COALESCE(b.proof_revision,0) ` + deploymentTransportJoin + ` AND b.interface_key=$1`
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration}
	err := q.QueryRowContext(ctx, query, key).Scan(&parent.ID, &parent.Revision, &parent.BindingOperationID, &parent.BindingOperationRevision,
		&parent.ProofID, &parent.ProofRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return channelnative.TransportParent{}, false, nil
	}
	if err != nil {
		return channelnative.TransportParent{}, false, err
	}
	return parent, true, parent.Validate()
}

func DeploymentParentCurrent(ctx context.Context, q queryer, parent channelnative.TransportParent) (bool, error) {
	if parent.Validate() != nil || parent.Kind != channelnative.TransportDeploymentRegistration {
		return false, nil
	}
	var key string
	err := q.QueryRowContext(ctx, `SELECT interface_key FROM deployment_channel_operations WHERE operation_id=$1`, parent.ID).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	current, found, err := deploymentBindingParent(ctx, q, key)
	return found && current == parent, err
}

func LockDeploymentTransportTx(ctx context.Context, tx *sql.Tx, parent channelnative.TransportParent, postgres bool) error {
	if err := LockTransportParentTx(ctx, tx, parent, postgres); err != nil {
		return err
	}
	var principal string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(principal_id AS TEXT) FROM deployment_channel_operations WHERE operation_id=$1`, parent.ID).Scan(&principal); err != nil {
		return err
	}
	if err := LockPrincipalTx(ctx, tx, principal, postgres); err != nil {
		return err
	}
	current, err := DeploymentParentCurrent(ctx, tx, parent)
	if err != nil || !current {
		return errors.Join(err, fmt.Errorf("deployment transport parent is no longer current"))
	}
	return nil
}

func DeploymentTransportCurrent(ctx context.Context, q queryer, parent channelnative.TransportParent,
	principal, interfaceKey string, bindingRevision int64, account, conversation, generation string, postgres bool) (bool, error) {
	if parent.Validate() != nil || parent.Kind != channelnative.TransportDeploymentRegistration {
		return false, nil
	}
	query := `SELECT o.operation_id ` + deploymentTransportJoin + `
		AND o.operation_id=? AND o.operation_revision=? AND child.operation_id=? AND child.operation_revision=?
		AND COALESCE(b.proof_id,'')=? AND COALESCE(b.proof_revision,0)=?
		AND b.principal_id=? AND b.interface_key=? AND b.binding_revision=?
		AND b.external_account=? AND b.conversation_reference=? AND o.semantic_generation=?`
	if postgres {
		query = `SELECT o.operation_id::text ` + deploymentTransportJoin + `
			AND o.operation_id=$1::uuid AND o.operation_revision=$2 AND child.operation_id=$3::uuid AND child.operation_revision=$4
			AND COALESCE(b.proof_id::text,'')=$5 AND COALESCE(b.proof_revision,0)=$6
			AND b.principal_id=$7::uuid AND b.interface_key=$8 AND b.binding_revision=$9
			AND b.external_account=$10 AND b.conversation_reference=$11 AND o.semantic_generation=$12`
	}
	var id string
	err := q.QueryRowContext(ctx, query, parent.ID, parent.Revision, parent.BindingOperationID, parent.BindingOperationRevision,
		parent.ProofID, parent.ProofRevision, principal, interfaceKey, bindingRevision, account, conversation, generation).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && id == parent.ID, err
}

func CurrentTransportParent(ctx context.Context, tx *sql.Tx, postgres bool) (channelnative.TransportParent, bool, error) {
	query := `SELECT o.operation_id, o.operation_revision, child.operation_id, child.operation_revision,
		COALESCE(b.proof_id,''), COALESCE(b.proof_revision,0) ` + deploymentTransportJoin + `
		AND EXISTS (SELECT 1 FROM channel_delivery_defaults d WHERE d.singleton_id=1 AND d.state='current'
		AND d.principal_id=b.principal_id AND d.interface_key=b.interface_key AND d.binding_revision=b.binding_revision
		AND d.external_account=b.external_account AND d.conversation_reference=b.conversation_reference
		AND d.conversation_scope=b.conversation_scope) LIMIT 2`
	if postgres {
		query = `SELECT o.operation_id::text, o.operation_revision, child.operation_id::text, child.operation_revision,
			COALESCE(b.proof_id::text,''), COALESCE(b.proof_revision,0) ` + deploymentTransportJoin + `
			AND EXISTS (SELECT 1 FROM channel_delivery_defaults d WHERE d.singleton_id=1 AND d.state='current'
			AND d.principal_id=b.principal_id AND d.interface_key=b.interface_key AND d.binding_revision=b.binding_revision
			AND d.external_account=b.external_account AND d.conversation_reference=b.conversation_reference
			AND d.conversation_scope=b.conversation_scope) LIMIT 2`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return channelnative.TransportParent{}, false, err
	}
	var parent channelnative.TransportParent
	for rows.Next() {
		if parent.ID != "" {
			_ = rows.Close()
			return channelnative.TransportParent{}, false, fmt.Errorf("channel default has competing deployment parents")
		}
		parent.Kind = channelnative.TransportDeploymentRegistration
		if err := rows.Scan(&parent.ID, &parent.Revision, &parent.BindingOperationID, &parent.BindingOperationRevision,
			&parent.ProofID, &parent.ProofRevision); err != nil {
			_ = rows.Close()
			return channelnative.TransportParent{}, false, err
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return channelnative.TransportParent{}, false, err
	}
	runtimeID, runtimeFound, err := CurrentActivationID(ctx, tx, postgres)
	if err != nil || runtimeFound && parent.ID != "" {
		return channelnative.TransportParent{}, false, errors.Join(err, fmt.Errorf("channel default has competing transport parents"))
	}
	if runtimeFound {
		query := `SELECT activation_revision FROM connected_channel_activations WHERE activation_id=?`
		if postgres {
			query = `SELECT activation_revision FROM connected_channel_activations WHERE activation_id=$1::uuid`
		}
		parent = channelnative.TransportParent{Kind: channelnative.TransportRuntimeActivation, ID: runtimeID}
		if err := tx.QueryRowContext(ctx, query, runtimeID).Scan(&parent.Revision); err != nil {
			return channelnative.TransportParent{}, false, err
		}
	}
	if parent.ID == "" {
		return parent, false, nil
	}
	return parent, true, parent.Validate()
}

// All arguments are internal SQL expressions, never authored or request text.
// The evidence selects its own kind and exact existing identity; no ID lookup
// or missing tag can transfer a runtime parent into deployment authority.
func deploymentParentPredicate(evidence, interfaceKey, bindingRevision string, postgres bool) string {
	field := func(name string) string {
		if postgres {
			return "(" + evidence + "->>'" + name + "')"
		}
		return "json_extract(" + evidence + ",'$." + name + "')"
	}
	return `EXISTS (SELECT 1 ` + deploymentTransportJoin + `
		AND ` + field("kind") + `='deployment_registration'
		AND CAST(o.operation_id AS TEXT)=` + field("id") + `
		AND o.operation_revision=CAST(` + field("revision") + ` AS BIGINT)
		AND CAST(child.operation_id AS TEXT)=` + field("binding_operation_id") + `
		AND child.operation_revision=CAST(` + field("binding_operation_revision") + ` AS BIGINT)
		AND COALESCE(CAST(b.proof_id AS TEXT),'')=COALESCE(` + field("proof_id") + `,'')
		AND COALESCE(b.proof_revision,0)=COALESCE(CAST(` + field("proof_revision") + ` AS BIGINT),0)
		AND b.interface_key=` + interfaceKey + ` AND b.binding_revision=` + bindingRevision + `)`
}
