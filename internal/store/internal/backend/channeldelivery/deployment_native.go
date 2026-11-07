package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
)

func attachDeploymentNativeInboxSettingTx(ctx context.Context, tx *sql.Tx, admission channelnative.Admission, postgres bool) (channelnative.Setting, bool, error) {
	if err := LockTransportParentTx(ctx, tx, admission.Parent, postgres); err != nil {
		return channelnative.Setting{}, false, err
	}
	if err := LockPrincipalTx(ctx, tx, admission.PrincipalID, postgres); err != nil {
		return channelnative.Setting{}, false, err
	}
	query := `SELECT o.provider, o.operator_account, b.external_account, b.conversation_scope,
		o.client_language, o.client_locale_revision, o.channel_pack_id, o.channel_pack_version, o.channel_manifest_hash
		` + deploymentTransportJoin + ` AND o.operation_id=?`
	if postgres {
		query = `SELECT o.provider, o.operator_account, b.external_account, b.conversation_scope,
			o.client_language, o.client_locale_revision, o.channel_pack_id, o.channel_pack_version, o.channel_manifest_hash
			` + deploymentTransportJoin + ` AND o.operation_id=$1::uuid`
	}
	var provider, bot, account, scope, pack, version, manifest string
	setting := channelnative.Setting{Provider: admission.Provider, ResourceSlotID: admission.ResourceSlotID,
		ConversationRef: admission.ConversationReference, PrincipalID: admission.PrincipalID, EntryContractHash: admission.EntryContractHash}
	err := tx.QueryRowContext(ctx, query, admission.Parent.ID).Scan(&provider, &bot, &account, &scope,
		&setting.ClientLanguage, &setting.ClientLocaleRevision, &pack, &version, &manifest)
	if err != nil {
		return channelnative.Setting{}, false, err
	}
	current, err := DeploymentTransportCurrent(ctx, tx, admission.Parent, admission.PrincipalID, admission.InterfaceKey,
		admission.BindingRevision, account, admission.ConversationReference, admission.PlanGeneration.Diagnostic(), postgres)
	if err != nil || !current || provider != admission.Provider || pack != admission.PackID || version != admission.PackVersion || manifest != admission.PackManifestHash ||
		!strings.HasSuffix(admission.ResourceSlotID, ":"+bot) {
		return channelnative.Setting{}, false, fmt.Errorf("native inbox deployment parent is not exact-current: %w", err)
	}
	switch scope {
	case "direct":
		setting.ScopeKind = "chat"
	case "shared":
		setting.ScopeKind, setting.MemberReference = "chat_member", account
	default:
		return channelnative.Setting{}, false, fmt.Errorf("native inbox deployment conversation scope is invalid")
	}
	return attachNativeSettingTx(ctx, tx, admission, setting, postgres)
}
