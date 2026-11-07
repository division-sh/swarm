package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
)

func loadDeploymentNativeQualification(ctx context.Context, tx *sql.Tx, parent channelnative.TransportParent, postgres, lock bool) (nativeQualificationFacts, bool, error) {
	if parent.Kind != channelnative.TransportDeploymentRegistration || parent.Validate() != nil {
		return nativeQualificationFacts{}, false, fmt.Errorf("native qualification requires an explicit deployment parent")
	}
	if lock {
		if err := LockTransportParentTx(ctx, tx, parent, postgres); err != nil {
			return nativeQualificationFacts{}, false, err
		}
		var principal string
		if err := tx.QueryRowContext(ctx, `SELECT CAST(principal_id AS TEXT) FROM deployment_channel_operations WHERE operation_id=$1`, parent.ID).Scan(&principal); err != nil {
			return nativeQualificationFacts{}, false, err
		}
		if err := LockPrincipalTx(ctx, tx, principal, postgres); err != nil {
			return nativeQualificationFacts{}, false, err
		}
	}
	query := `SELECT o.client_language, o.client_locale_revision, CAST(s.setting_id AS TEXT), s.generation,
		s.state, s.entry_contract_hash, c.activation_revision, c.binding_revision,
		c.qualification_state, c.qualification_locale_revision, c.qualification_setting_generation,
		c.qualification_activation_revision, c.qualification_binding_revision, c.qualification_context_generation,
		c.qualification_contract_hash, c.qualification_reason, c.qualification_observed_at, c.qualification_readback_hash, c.parent_evidence
		FROM channel_native_setting_consumers c
		JOIN deployment_channel_operations o ON o.operation_id=c.activation_id AND o.operation_revision=c.activation_revision
		JOIN operator_channel_bindings b ON b.interface_key=c.interface_key
		JOIN channel_native_settings s ON s.setting_id=c.setting_id AND s.principal_id=b.principal_id
		AND s.provider=o.provider AND s.conversation_reference=b.conversation_reference
		AND s.pack_id=o.channel_pack_id AND s.pack_version=o.channel_pack_version AND s.pack_manifest_hash=o.channel_manifest_hash
		AND s.scope_kind=CASE WHEN b.conversation_scope='direct' THEN 'chat' ELSE 'chat_member' END
		AND s.member_reference=CASE WHEN b.conversation_scope='direct' THEN '' ELSE b.external_account END
		WHERE c.parent_kind='deployment_registration' AND c.state='current' AND c.context_publication_generation IS NULL
		AND c.activation_id=$1 AND ` + deploymentParentPredicate("c.parent_evidence", "c.interface_key", "c.binding_revision", postgres)
	if postgres && lock {
		query += ` FOR UPDATE OF b,c,s`
	}
	var facts nativeQualificationFacts
	var observed any
	var raw []byte
	p := &facts.projection
	err := tx.QueryRowContext(ctx, query, parent.ID).Scan(&p.ClientLanguage, &p.LocaleRevision, &p.SettingID, &p.SettingGeneration,
		&facts.settingState, &facts.contractHash, &facts.activationRevision, &facts.bindingRevision,
		&p.State, &facts.localeRevision, &facts.settingGeneration, &facts.qualifiedActivation, &facts.qualifiedBinding,
		&facts.qualifiedContext, &facts.qualifiedContract, &p.Reason, &observed, &facts.readbackHash, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return facts, false, nil
	}
	if err != nil {
		return facts, false, err
	}
	stored, err := channelnative.DecodeTransportParent(raw)
	if err != nil || stored != parent {
		return facts, false, errors.Join(err, fmt.Errorf("native qualification parent changed"))
	}
	if count, err := currentNativeConsumersTx(ctx, tx, p.SettingID, postgres); err != nil || count != 1 {
		return facts, false, errors.Join(err, fmt.Errorf("native qualification requires one current connection"))
	}
	if observed != nil {
		switch value := observed.(type) {
		case time.Time:
			p.ObservedAt = value.UTC()
		case string:
			p.ObservedAt, err = time.Parse(time.RFC3339Nano, value)
		default:
			err = fmt.Errorf("native qualification observation has invalid type")
		}
	}
	return facts, true, err
}

func ReadNativeInboxQualificationParentTx(ctx context.Context, tx *sql.Tx, parent channelnative.TransportParent, postgres bool) (channelnative.Qualification, error) {
	if parent.Validate() != nil {
		return channelnative.Qualification{}, fmt.Errorf("native qualification parent is incomplete")
	}
	if parent.Kind == channelnative.TransportRuntimeActivation {
		facts, found, err := loadNativeQualification(ctx, tx, parent.ID, postgres, false)
		if err != nil {
			return channelnative.Qualification{}, err
		}
		if found && facts.activationRevision != parent.Revision {
			return channelnative.Qualification{State: channelnative.QualificationStale, Reason: "native qualification activation changed"}, nil
		}
		return projectNativeQualification(facts, found), nil
	}
	facts, found, err := loadDeploymentNativeQualification(ctx, tx, parent, postgres, false)
	if err != nil {
		return channelnative.Qualification{}, err
	}
	return projectNativeQualification(facts, found), nil
}

func recordDeploymentNativeQualificationTx(ctx context.Context, tx *sql.Tx, req channelnative.QualificationRequest, postgres bool) (bool, error) {
	facts, found, err := loadDeploymentNativeQualification(ctx, tx, req.Parent, postgres, true)
	if err != nil {
		return false, err
	}
	p := facts.projection
	if !found || p.SettingID != req.SettingID || p.SettingGeneration != req.SettingGeneration ||
		p.ClientLanguage != req.ClientLanguage || p.LocaleRevision != req.LocaleRevision ||
		facts.bindingRevision != req.BindingRevision || facts.contractHash != req.EntryContractHash ||
		(req.State == channelnative.QualificationQualified && facts.settingState != "installed" && facts.settingState != "retired") {
		return false, fmt.Errorf("native qualification contradicts current deployment or setting authority")
	}
	comparison := req
	comparison.ActivationRevision = req.Parent.Revision
	changed := facts.qualificationChanged(comparison)
	if req.State == channelnative.QualificationQualified && facts.settingState == "retired" {
		if err := reactivateAcknowledgedNativeInboxSettingTx(ctx, tx, req.SettingID, req.SettingGeneration, postgres); err != nil {
			return false, err
		}
		changed = true
	}
	query := `UPDATE channel_native_setting_consumers SET qualification_state=$1, qualification_locale_revision=$2,
		qualification_setting_generation=$3, qualification_activation_revision=$4, qualification_binding_revision=$5,
		qualification_context_generation=0, qualification_contract_hash=$6, qualification_readback_hash=$7,
		qualification_reason=$8, qualification_observed_at=$9 WHERE setting_id=$10 AND activation_id=$11
		AND parent_kind='deployment_registration' AND state='current'`
	result, err := tx.ExecContext(ctx, query, string(req.State), req.LocaleRevision, req.SettingGeneration, req.Parent.Revision,
		req.BindingRevision, req.EntryContractHash, req.ReadbackHash, req.Reason, req.ObservedAt.UTC().Format(time.RFC3339Nano), req.SettingID, req.Parent.ID)
	if err != nil {
		return false, err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return false, fmt.Errorf("native qualification current deployment consumer changed: %w", err)
	}
	return changed, nil
}
