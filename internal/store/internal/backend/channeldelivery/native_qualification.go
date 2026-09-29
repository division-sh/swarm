package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/google/uuid"
)

const nativeQualificationSelect = `SELECT o.client_language, o.client_locale_revision,
	CAST(s.setting_id AS TEXT), s.generation, s.state, s.entry_contract_hash,
	a.activation_revision, a.context_publication_generation, a.binding_revision,
	c.qualification_state, c.qualification_locale_revision, c.qualification_setting_generation,
	c.qualification_activation_revision, c.qualification_binding_revision, c.qualification_context_generation, c.qualification_contract_hash,
	c.qualification_reason, c.qualification_observed_at
	FROM connected_channel_activations a
	JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id AND o.phase='succeeded'
	JOIN operator_channel_bindings b ON b.interface_key=a.interface_key AND b.status='current'
		AND b.operation_id=o.identity_operation_id AND b.principal_id=a.principal_id
		AND b.binding_revision=a.binding_revision AND b.conversation_reference=a.conversation_reference
	JOIN channel_native_setting_consumers c ON c.activation_id=a.activation_id AND c.state='current'
		AND c.activation_revision=a.activation_revision AND c.binding_revision=a.binding_revision
		AND c.context_publication_generation=a.context_publication_generation AND c.interface_key=a.interface_key
	JOIN channel_native_settings s ON s.setting_id=c.setting_id AND s.principal_id=a.principal_id
		AND s.provider=a.provider AND s.conversation_reference=a.conversation_reference
		AND s.pack_id=a.channel_pack_id AND s.pack_version=a.channel_pack_version AND s.pack_manifest_hash=a.channel_manifest_hash
		AND s.scope_kind=CASE WHEN b.conversation_scope='direct' THEN 'chat' ELSE 'chat_member' END
		AND s.member_reference=CASE WHEN b.conversation_scope='direct' THEN '' ELSE b.external_account_reference END
	WHERE a.status='current' AND a.activation_id=`

type nativeQualificationFacts struct {
	projection                                                               channelnative.Qualification
	settingState, contractHash                                               string
	activationRevision, contextGeneration, bindingRevision                   int64
	localeRevision, settingGeneration, qualifiedActivation, qualifiedContext int64
	qualifiedBinding                                                         int64
	qualifiedContract                                                        string
}

func loadNativeQualification(ctx context.Context, tx *sql.Tx, activationID string, postgres, lock bool) (nativeQualificationFacts, bool, error) {
	query := nativeQualificationSelect + "?"
	if postgres {
		query = nativeQualificationSelect + "$1::uuid"
		if lock {
			query += " FOR UPDATE OF a, o, b, c, s"
		}
	}
	var facts nativeQualificationFacts
	var observed any
	p := &facts.projection
	err := tx.QueryRowContext(ctx, query, activationID).Scan(&p.ClientLanguage, &p.LocaleRevision,
		&p.SettingID, &p.SettingGeneration, &facts.settingState, &facts.contractHash,
		&facts.activationRevision, &facts.contextGeneration, &facts.bindingRevision,
		&p.State, &facts.localeRevision, &facts.settingGeneration, &facts.qualifiedActivation,
		&facts.qualifiedBinding, &facts.qualifiedContext, &facts.qualifiedContract, &p.Reason, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		return facts, false, nil
	}
	if err != nil {
		return facts, false, err
	}
	count, err := currentNativeConsumersTx(ctx, tx, p.SettingID, postgres)
	if err != nil || count != 1 {
		return facts, false, errors.Join(err, fmt.Errorf("native qualification requires one current connection"))
	}
	if observed != nil {
		switch value := observed.(type) {
		case time.Time:
			p.ObservedAt = value.UTC()
		case string:
			p.ObservedAt, err = time.Parse(time.RFC3339Nano, value)
		default:
			err = fmt.Errorf("native qualification has invalid observation time %T", observed)
		}
	}
	return facts, true, err
}

func ReadNativeInboxQualificationTx(ctx context.Context, tx *sql.Tx, activationID string, postgres bool) (channelnative.Qualification, error) {
	if tx == nil || uuid.Validate(activationID) != nil {
		return channelnative.Qualification{}, fmt.Errorf("native qualification requires exact selected activation")
	}
	facts, found, err := loadNativeQualification(ctx, tx, activationID, postgres, false)
	if err != nil {
		return channelnative.Qualification{}, err
	}
	p := facts.projection
	if !found {
		return channelnative.Qualification{State: channelnative.QualificationMissing, Reason: "current native setting has not been qualified"}, nil
	}
	if p.ClientLanguage == "" {
		p.State, p.Reason = channelnative.QualificationMissing, "explicit client language is required"
	} else if facts.localeRevision != p.LocaleRevision || facts.settingGeneration != p.SettingGeneration ||
		facts.qualifiedActivation != facts.activationRevision || facts.qualifiedBinding != facts.bindingRevision ||
		facts.qualifiedContext != facts.contextGeneration ||
		facts.qualifiedContract != facts.contractHash ||
		(p.State == channelnative.QualificationQualified && facts.settingState != "installed") {
		p.State, p.Reason = channelnative.QualificationStale, "native qualification no longer matches its declaration, setting or connection"
	}
	return p, nil
}

func RecordNativeInboxQualificationTx(ctx context.Context, tx *sql.Tx, req channelnative.QualificationRequest, postgres bool) error {
	if tx == nil || uuid.Validate(req.ActivationID) != nil || uuid.Validate(req.SettingID) != nil ||
		req.SettingGeneration < 1 || req.LocaleRevision < 1 || req.ObservedAt.IsZero() ||
		(req.State != channelnative.QualificationQualified && req.State != channelnative.QualificationInvalid && req.State != channelnative.QualificationMissing) ||
		(req.State == channelnative.QualificationQualified && (req.ReadbackHash == "" || req.ClientLanguage == "")) {
		return fmt.Errorf("native qualification requires exact coordinates and observation")
	}
	facts, found, err := loadNativeQualification(ctx, tx, req.ActivationID, postgres, true)
	if err != nil {
		return err
	}
	p := facts.projection
	if !found || p.SettingID != req.SettingID || p.SettingGeneration != req.SettingGeneration ||
		p.ClientLanguage != req.ClientLanguage || p.LocaleRevision != req.LocaleRevision ||
		facts.activationRevision != req.ActivationRevision || facts.contextGeneration != req.ContextGeneration ||
		facts.bindingRevision != req.BindingRevision || facts.contractHash != req.EntryContractHash ||
		(req.State == channelnative.QualificationQualified && facts.settingState != "installed" && facts.settingState != "retired") {
		return fmt.Errorf("native qualification contradicts current declaration or setting authority")
	}
	if req.State == channelnative.QualificationQualified && facts.settingState == "retired" {
		if err := reactivateAcknowledgedNativeInboxSettingTx(ctx, tx, req.SettingID, req.SettingGeneration, postgres); err != nil {
			return err
		}
	}
	query := `UPDATE channel_native_setting_consumers SET qualification_state=?, qualification_locale_revision=?,
		qualification_setting_generation=?, qualification_activation_revision=?, qualification_binding_revision=?, qualification_context_generation=?,
		qualification_contract_hash=?, qualification_readback_hash=?, qualification_reason=?, qualification_observed_at=?
		WHERE setting_id=? AND activation_id=? AND state='current'`
	if postgres {
		query = `UPDATE channel_native_setting_consumers SET qualification_state=$1, qualification_locale_revision=$2,
			qualification_setting_generation=$3, qualification_activation_revision=$4, qualification_binding_revision=$5, qualification_context_generation=$6,
			qualification_contract_hash=$7, qualification_readback_hash=$8, qualification_reason=$9, qualification_observed_at=$10
			WHERE setting_id=$11::uuid AND activation_id=$12::uuid AND state='current'`
	}
	result, err := tx.ExecContext(ctx, query, string(req.State), req.LocaleRevision, req.SettingGeneration,
		req.ActivationRevision, req.BindingRevision, req.ContextGeneration, req.EntryContractHash, req.ReadbackHash, req.Reason,
		req.ObservedAt.UTC().Format(time.RFC3339Nano), req.SettingID, req.ActivationID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("native qualification current consumer changed: %w", err)
	}
	return nil
}
