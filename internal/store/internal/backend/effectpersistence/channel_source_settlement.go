package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
)

func requireChannelSourceSettlementTx(ctx context.Context, tx *sql.Tx, s runtimeeffects.Settlement, postgres bool) error {
	switch s.Authority.Kind {
	case runtimeeffects.AuthorityChannelDelivery:
		return requireChannelDeliverySettlementAuthorityTx(ctx, tx, s, postgres)
	case runtimeeffects.AuthorityChannelNativeSetting:
		return requireChannelNativeSettingSettlementAuthorityTx(ctx, tx, s, postgres)
	default:
		return nil
	}
}

func projectChannelSourceSettlementTx(ctx context.Context, tx *sql.Tx, s runtimeeffects.Settlement, postgres bool) error {
	switch s.Authority.Kind {
	case runtimeeffects.AuthorityChannelDelivery:
		return projectChannelDeliverySettlementTx(ctx, tx, s, postgres)
	case runtimeeffects.AuthorityChannelNativeSetting:
		return projectChannelNativeSettingSettlementTx(ctx, tx, s, postgres)
	default:
		return nil
	}
}

// This codec restores original journal evidence, never current execution authority.
// The typed source owners compare its complete re-encoding with the stored evidence.
type channelSettlementEvidence struct {
	Kind                         runtimeeffects.AuthorityKind `json:"authority_kind"`
	ID                           string                       `json:"authority_id"`
	ExecutionOwner               string                       `json:"execution_owner"`
	FenceGeneration              uint64                       `json:"fence_generation"`
	ExecutionMode                runtimeeffects.ExecutionMode `json:"execution_mode"`
	EffectOperationID            string                       `json:"effect_operation_id"`
	DeliveryID                   string                       `json:"delivery_id"`
	RenderID                     string                       `json:"render_id"`
	RenderHash                   string                       `json:"render_hash"`
	PreviousReceiptOperationID   string                       `json:"previous_receipt_operation_id"`
	PrincipalID                  string                       `json:"principal_id"`
	InterfaceKey                 string                       `json:"interface_key"`
	DeliveryEpoch                int64                        `json:"delivery_epoch"`
	BindingRevision              int64                        `json:"binding_revision"`
	ExternalAccountRef           string                       `json:"external_account_reference"`
	ConversationRef              string                       `json:"conversation_reference"`
	ActivationID                 string                       `json:"activation_id"`
	ActivationRevision           int64                        `json:"activation_revision"`
	BundleHash                   string                       `json:"bundle_hash"`
	BundleIdentity               string                       `json:"bundle_identity"`
	PackInventoryGeneration      string                       `json:"pack_inventory_generation"`
	RuntimeInstanceID            string                       `json:"runtime_instance_id"`
	ContextPublicationGeneration uint64                       `json:"context_publication_generation"`
	PlanGeneration               plangeneration.Generation    `json:"plan_generation"`
	TargetGeneration             uint64                       `json:"target_generation"`
	SettingID                    string                       `json:"setting_id"`
	SettingGeneration            int64                        `json:"setting_generation"`
	Provider                     string                       `json:"provider"`
	ResourceSlotID               string                       `json:"resource_slot_id"`
	ScopeKind                    string                       `json:"scope_kind"`
	MemberReference              string                       `json:"member_reference"`
	EntryContractHash            string                       `json:"entry_contract_hash"`
	EntryCommand                 string                       `json:"entry_command"`
	PackID                       string                       `json:"pack_id"`
	PackVersion                  string                       `json:"pack_version"`
	PackManifestHash             string                       `json:"pack_manifest_hash"`
}

func (e channelSettlementEvidence) authority(lease time.Time) (runtimeeffects.Authority, error) {
	a := runtimeeffects.Authority{Kind: e.Kind, ID: e.ID, ExecutionOwner: e.ExecutionOwner,
		FenceGeneration: e.FenceGeneration, LeaseExpiresAt: lease, ExecutionMode: e.ExecutionMode}
	switch e.Kind {
	case runtimeeffects.AuthorityChannelDelivery:
		a.ChannelDelivery = runtimeeffects.ChannelDeliveryAuthority{
			EffectOperationID: e.EffectOperationID, DeliveryID: e.DeliveryID, RenderID: e.RenderID, RenderHash: e.RenderHash,
			PreviousReceiptOperationID: e.PreviousReceiptOperationID, PrincipalID: e.PrincipalID, InterfaceKey: e.InterfaceKey,
			DeliveryEpoch: e.DeliveryEpoch, BindingRevision: e.BindingRevision, ExternalAccountRef: e.ExternalAccountRef,
			ConversationRef: e.ConversationRef, ActivationID: e.ActivationID, ActivationRevision: e.ActivationRevision,
			BundleHash: e.BundleHash, BundleIdentity: e.BundleIdentity, PackInventoryGeneration: e.PackInventoryGeneration,
			RuntimeInstanceID: e.RuntimeInstanceID, ContextPublicationGeneration: e.ContextPublicationGeneration,
			PlanGeneration: e.PlanGeneration, TargetGeneration: e.TargetGeneration,
		}
	case runtimeeffects.AuthorityChannelNativeSetting:
		a.ChannelNativeSetting = runtimeeffects.ChannelNativeSettingAuthority{
			EffectOperationID: e.EffectOperationID, SettingID: e.SettingID, SettingGeneration: e.SettingGeneration,
			Provider: e.Provider, ResourceSlotID: e.ResourceSlotID, ConversationRef: e.ConversationRef, ScopeKind: e.ScopeKind,
			MemberReference: e.MemberReference, PrincipalID: e.PrincipalID, EntryContractHash: e.EntryContractHash,
			EntryCommand: e.EntryCommand, PackID: e.PackID, PackVersion: e.PackVersion, PackManifestHash: e.PackManifestHash,
			ActivationID: e.ActivationID, ActivationRevision: e.ActivationRevision, BindingRevision: e.BindingRevision,
			BundleHash: e.BundleHash, BundleIdentity: e.BundleIdentity, PackInventoryGeneration: e.PackInventoryGeneration,
			RuntimeInstanceID: e.RuntimeInstanceID, ContextPublicationGeneration: e.ContextPublicationGeneration,
			PlanGeneration: e.PlanGeneration, TargetGeneration: e.TargetGeneration,
		}
	default:
		return runtimeeffects.Authority{}, fmt.Errorf("unsupported channel settlement authority %q", e.Kind)
	}
	if !a.Valid() {
		return runtimeeffects.Authority{}, fmt.Errorf("original channel settlement authority is invalid")
	}
	return a, nil
}

func loadChannelRecoverySettlementTx(ctx context.Context, tx *sql.Tx, c externalEffectRecoveryCandidate,
	state runtimeeffects.State, failure []byte, now time.Time, postgres bool) (runtimeeffects.Settlement, error) {
	query := `SELECT a.authority_evidence,a.evidence,a.lease_expires_at,o.authority_id
		FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		WHERE o.operation_id=$1 AND a.attempt_id=$2 AND a.state=$3`
	if postgres {
		query += ` FOR UPDATE OF o,a`
	}
	var authorityRaw, evidenceRaw []byte
	var leaseRaw any
	var authorityID string
	if err := tx.QueryRowContext(ctx, query, c.OperationID, c.AttemptID, c.State).Scan(&authorityRaw, &evidenceRaw, &leaseRaw, &authorityID); err != nil {
		return runtimeeffects.Settlement{}, err
	}
	lease, ok, err := sqliteTimeValue(leaseRaw)
	if err != nil || !ok {
		return runtimeeffects.Settlement{}, fmt.Errorf("original channel settlement lease is invalid: %v", err)
	}
	canonical, err := canonicaljson.Canonicalize(authorityRaw)
	if err != nil {
		return runtimeeffects.Settlement{}, err
	}
	var evidence channelSettlementEvidence
	if err := json.Unmarshal(canonical, &evidence); err != nil {
		return runtimeeffects.Settlement{}, err
	}
	authority, err := evidence.authority(lease)
	if err != nil {
		return runtimeeffects.Settlement{}, err
	}
	if string(authority.Kind) != c.AuthorityKind || authority.ID != authorityID || string(authority.ExecutionMode) != c.AttemptMode {
		return runtimeeffects.Settlement{}, fmt.Errorf("original channel settlement coordinates contradict the journal")
	}
	s := runtimeeffects.Settlement{OperationID: c.OperationID, AttemptID: c.AttemptID, Authority: authority, State: state, Now: now}
	if len(evidenceRaw) != 0 && string(evidenceRaw) != "null" {
		canonical, err := canonicaljson.Canonicalize(evidenceRaw)
		if err != nil {
			return runtimeeffects.Settlement{}, err
		}
		if err := canonicaljson.DecodePreservingNumberLexemes(canonical, &s.Evidence); err != nil {
			return runtimeeffects.Settlement{}, err
		}
	}
	envelope, err := runtimefailures.UnmarshalEnvelope(failure)
	if err != nil {
		return runtimeeffects.Settlement{}, err
	}
	s.Failure = &envelope
	return s, nil
}

func recoverChannelSourceSettlementTx(ctx context.Context, tx *sql.Tx, c externalEffectRecoveryCandidate,
	state runtimeeffects.State, failure []byte, now time.Time, postgres bool) (bool, error) {
	s, err := loadChannelRecoverySettlementTx(ctx, tx, c, state, failure, now, postgres)
	if err != nil {
		return false, err
	}
	if err := requireChannelSourceSettlementTx(ctx, tx, s, postgres); err != nil {
		return false, err
	}
	var changed bool
	if postgres {
		changed, err = settleExternalAttemptPostgres(ctx, tx, s)
	} else {
		changed, err = settleExternalAttemptSQLiteTx(ctx, tx, s)
	}
	if err != nil || !changed {
		return changed, err
	}
	return true, projectChannelSourceSettlementTx(ctx, tx, s, postgres)
}

func requireChannelSettlementEvidence(authority runtimeeffects.Authority, attempt, original, operation []byte) error {
	wantRaw, err := json.Marshal(authority.Evidence())
	if err != nil {
		return err
	}
	want, err := canonicaljson.Canonicalize(wantRaw)
	if err != nil {
		return err
	}
	got, err := canonicaljson.Canonicalize(attempt)
	if err != nil || !bytes.Equal(want, got) {
		return fmt.Errorf("channel settlement evidence contradicts exact attempt authority")
	}
	first, err := canonicaljson.Canonicalize(original)
	if err != nil {
		return err
	}
	history, err := canonicaljson.Canonicalize(operation)
	if err != nil || !bytes.Equal(first, history) {
		return fmt.Errorf("channel operation history contradicts original attempt authority")
	}
	return nil
}
