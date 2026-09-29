package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func requireChannelActionAckAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, postgres bool) error {
	if err := channeldelivery.LockPrincipalTx(ctx, tx, authority.ChannelActionAck.PrincipalID, postgres); err != nil {
		return invalidExternalAuthority(authority, "principal_not_current")
	}
	current, err := channelActionAckAuthorityCurrent(ctx, tx, authority, postgres, true)
	if err != nil {
		return err
	}
	if !current {
		return invalidExternalAuthority(authority, "stale")
	}
	return nil
}

func channelActionAckAuthorityCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, postgres, lock bool) (bool, error) {
	if !authority.Valid() || authority.Kind != runtimeeffects.AuthorityChannelActionAck {
		return false, nil
	}
	ack := authority.ChannelActionAck
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state
		FROM operator_channel_action_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state
			FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var provider, eventID, interfaceKey, authorization, state string
	var raw []byte
	err := q.QueryRowContext(ctx, query, ack.PublicationID).Scan(&provider, &eventID, &interfaceKey, &raw, &authorization, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read verified channel action: %w", err)
	}
	var fact operatorchannel.ActionFact
	if err := json.Unmarshal(raw, &fact); err != nil {
		return false, fmt.Errorf("decode verified channel action: %w", err)
	}
	if state != "pending" || provider != ack.Provider || eventID != ack.ProviderEventID ||
		interfaceKey != ack.InterfaceKey || authorization != ack.ProviderAuthorization || fact.Interface.Key() != ack.InterfaceKey ||
		fact.ExternalAccountRef != ack.ExternalAccountRef || fact.ConversationRef != ack.ConversationRef ||
		string(fact.ConversationScope) != ack.ConversationScope || fact.MessageReference != ack.MessageReference ||
		fact.InteractionRef != ack.InteractionRef || fact.Token != ack.Token {
		return false, nil
	}
	resolved, found, err := channeldelivery.ResolveActionFact(ctx, q, fact, postgres)
	if err != nil || !found {
		return false, err
	}
	if !resolved.CurrentRender || resolved.ReceiptOperationID != ack.ReceiptOperationID ||
		resolved.PrincipalID != ack.PrincipalID || resolved.BindingRevision != ack.BindingRevision ||
		resolved.ActivationID != ack.ActivationID || resolved.ActivationRevision != ack.ActivationRevision {
		return false, nil
	}
	return channelActionAckActivationCurrent(ctx, q, ack, postgres, lock)
}

func channelActionAckActivationCurrent(ctx context.Context, q schemaQueryer, ack runtimeeffects.ChannelActionAckAuthority, postgres, lock bool) (bool, error) {
	query := `SELECT activation_id FROM connected_channel_activations
		WHERE activation_id=? AND status='current' AND principal_id=? AND binding_revision=?
		AND activation_revision=? AND bundle_hash=? AND bundle_identity=?
		AND pack_inventory_generation=? AND runtime_instance_id=?
		AND context_publication_generation=? AND plan_generation=? AND target_generation=?`
	if postgres {
		query = `SELECT activation_id::text FROM connected_channel_activations
			WHERE activation_id=$1::uuid AND status='current' AND principal_id=$2::uuid AND binding_revision=$3
			AND activation_revision=$4 AND bundle_hash=$5 AND bundle_identity=$6
			AND pack_inventory_generation=$7 AND runtime_instance_id=$8::uuid
			AND context_publication_generation=$9 AND plan_generation=$10 AND target_generation=$11`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var activationID string
	err := q.QueryRowContext(ctx, query, ack.ActivationID, ack.PrincipalID, ack.BindingRevision, ack.ActivationRevision,
		ack.BundleHash, ack.BundleIdentity, ack.PackInventoryGeneration, ack.RuntimeInstanceID,
		ack.ContextPublicationGeneration, ack.PlanGeneration.Diagnostic(), ack.TargetGeneration).Scan(&activationID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read current channel action activation: %w", err)
	}
	return activationID == ack.ActivationID, nil
}
