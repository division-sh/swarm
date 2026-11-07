package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/google/uuid"
)

// Only this projection constructor consumes provider admission. The SQL owner
// consumes the frozen result and a local lifetime check, never projects through
// a session reader or acquires its source/run while holding the transaction.
type PreparedDeploymentInput struct {
	request           render.DeploymentInput
	interfaceKey      string
	generation        string
	id, fingerprint   string
	action            *operatorchannel.InboundAction
	text              *operatorchannel.InboundText
	challenge         bool
	nativeFingerprint string
}

func PrepareDeploymentInput(ctx context.Context, request render.DeploymentInput) (PreparedDeploymentInput, error) {
	if request.Parent.Validate() != nil || request.Parent.Kind != channelnative.TransportDeploymentRegistration ||
		request.Account.Validate() != nil || request.BindingRevision < 1 || request.ReceivedAt.IsZero() {
		return PreparedDeploymentInput{}, fmt.Errorf("deployment operator input is incomplete")
	}
	if err := request.Input.RequireDeploymentOperator(ctx, request.Account, request.Parent.ID, request.Parent.Revision, request.Session); err != nil {
		return PreparedDeploymentInput{}, err
	}
	identity, err := request.Channel.InterfaceIdentity()
	if err != nil {
		return PreparedDeploymentInput{}, err
	}
	delivery, err := request.Trigger.ProjectDelivery(request.Input)
	if err != nil {
		return PreparedDeploymentInput{}, err
	}
	providerID, nativeFingerprint, err := request.Input.NativeCaptureEvidence()
	if err != nil {
		return PreparedDeploymentInput{}, err
	}
	if nativeFingerprint != "" {
		delivery.ProviderEventID = providerID
	}
	parent, err := canonicaljson.Bytes(request.Parent)
	if err != nil {
		return PreparedDeploymentInput{}, err
	}
	prepared := PreparedDeploymentInput{request: request, interfaceKey: identity.Key(), generation: identity.SemanticGeneration,
		nativeFingerprint: nativeFingerprint,
		id:                uuid.NewSHA1(uuid.MustParse(request.Parent.ID), []byte("deployment-operator-input:"+delivery.ProviderEventID)).String(),
		fingerprint:       operatorchannel.Hash("deployment-operator-request-v1", request.Input.SemanticContentDigest(), string(parent))}
	for _, output := range delivery.Events {
		if output.Kind != providertriggers.OutputKindNormalized {
			continue
		}
		action, matched, err := request.Channel.ProjectActionFact(string(output.Name), output.Authorization, output.Payload)
		if err != nil {
			return PreparedDeploymentInput{}, err
		}
		authorization := operatorchannel.Hash("deployment-operator-output-v1", string(parent), prepared.fingerprint,
			output.Authorization.Provider(), output.Authorization.Event(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic())
		if matched {
			if prepared.action != nil || prepared.text != nil {
				return PreparedDeploymentInput{}, fmt.Errorf("deployment operator output is ambiguous")
			}
			prepared.action = &operatorchannel.InboundAction{ActionFact: action, PublicationID: prepared.id, Provider: request.Account.Provider,
				ProviderEventID: delivery.ProviderEventID, ProviderAuthorization: authorization}
		}
		text, matched, err := request.Channel.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
		if err != nil {
			return PreparedDeploymentInput{}, err
		}
		if matched {
			if prepared.action != nil || prepared.text != nil {
				return PreparedDeploymentInput{}, fmt.Errorf("deployment operator output is ambiguous")
			}
			prepared.text = &operatorchannel.InboundText{TextFact: text, PublicationID: prepared.id, Provider: request.Account.Provider,
				ProviderEventID: delivery.ProviderEventID, ProviderAuthorization: authorization}
			_, prepared.challenge = operatorchannel.ChallengeFromText(text.Text)
		}
	}
	return prepared, nil
}

func AdmitDeploymentInputTx(ctx context.Context, tx *sql.Tx, prepared PreparedDeploymentInput, postgres bool) (bool, bool, error) {
	r := prepared.request
	if prepared.id == "" || r.Input.RequireDeploymentOperator(ctx, r.Account, r.Parent.ID, r.Parent.Revision, r.Session) != nil {
		return false, false, fmt.Errorf("deployment operator input lost its original admission")
	}
	if err := LockTransportParentTx(ctx, tx, r.Parent, postgres); err != nil {
		return false, false, err
	}
	if err := LockPrincipalTx(ctx, tx, r.PrincipalID, postgres); err != nil {
		return false, false, err
	}
	query := `SELECT b.external_account,b.conversation_reference,b.conversation_scope,o.operator_account,o.session_account_admission ` + deploymentTransportJoin + ` AND o.operation_id=$1`
	var account, conversation, scope, bot string
	var sessionRaw []byte
	if err := tx.QueryRowContext(ctx, query, r.Parent.ID).Scan(&account, &conversation, &scope, &bot, &sessionRaw); err != nil {
		return false, false, err
	}
	var session operatorchannel.SessionAccountAdmission
	if len(sessionRaw) != 0 && string(sessionRaw) != "null" {
		if err := json.Unmarshal(sessionRaw, &session); err != nil {
			return false, false, err
		}
	}
	current, err := DeploymentTransportCurrent(ctx, tx, r.Parent, r.PrincipalID, prepared.interfaceKey, r.BindingRevision,
		account, conversation, prepared.generation, postgres)
	if err != nil || !current || bot != r.Account.Account || r.Session != session {
		return false, false, errors.Join(err, fmt.Errorf("deployment operator input parent changed"))
	}
	if prepared.action == nil && prepared.text == nil {
		return false, false, nil
	}
	table := "operator_channel_action_intents"
	if prepared.text != nil {
		table = "operator_channel_text_intents"
		fact := prepared.text
		if fact.ConversationRef != conversation || string(fact.ConversationScope) != scope {
			return false, false, nil
		}
		if fact.ExternalAccountRef != account {
			return true, false, nil // The operator conversation is never customer input.
		}
	} else {
		fact := prepared.action
		if fact.ConversationRef != conversation || string(fact.ConversationScope) != scope {
			return false, false, nil
		}
		if fact.ExternalAccountRef != account {
			return true, false, nil
		}
	}
	var fingerprint sql.NullString
	var count, complete, distinct int64
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(request_fingerprint),COUNT(DISTINCT request_fingerprint),MIN(request_fingerprint)
		FROM (SELECT request_fingerprint FROM operator_channel_action_intents WHERE publication_id=$1
		UNION ALL SELECT request_fingerprint FROM operator_channel_text_intents WHERE publication_id=$1) original`, prepared.id).
		Scan(&count, &complete, &distinct, &fingerprint)
	if err != nil {
		return false, false, err
	}
	if count != 0 {
		if complete != count || distinct != 1 || !fingerprint.Valid || fingerprint.String != prepared.fingerprint {
			return false, false, fmt.Errorf("deployment provider occurrence contradicts its original request")
		}
		if prepared.nativeFingerprint != "" {
			receipt, found, err := LoadDeploymentInputReceipt(ctx, tx, r.Parent.ID, deliveryProviderID(prepared))
			if err != nil || !found || receipt.Parent != r.Parent || receipt.Fingerprint != prepared.nativeFingerprint {
				return false, false, errors.Join(err, fmt.Errorf("native operator occurrence contradicts its original capture"))
			}
		}
		return true, false, nil
	}
	if prepared.action != nil {
		err = InsertActionIntentTx(ctx, tx, *prepared.action, r.ReceivedAt, postgres)
	} else {
		err = InsertTextIntentTx(ctx, tx, *prepared.text, r.ReceivedAt, postgres)
	}
	if err != nil {
		return false, false, err
	}
	parent, err := canonicaljson.Bytes(r.Parent)
	if err != nil {
		return false, false, err
	}
	query = `UPDATE ` + table + ` SET transport_parent=$1,request_fingerprint=$2,native_capture_fingerprint=$4 WHERE publication_id=$3`
	if postgres {
		query = `UPDATE ` + table + ` SET transport_parent=$1::jsonb,request_fingerprint=$2,native_capture_fingerprint=$4 WHERE publication_id=$3::uuid`
	}
	var native any
	if prepared.nativeFingerprint != "" {
		native = prepared.nativeFingerprint
	}
	_, err = tx.ExecContext(ctx, query, string(parent), prepared.fingerprint, prepared.id, native)
	if err == nil && prepared.challenge {
		_, err = tx.ExecContext(ctx, `UPDATE operator_channel_text_intents SET state='settled',disposition='unsupported',settled_at=$1 WHERE publication_id=$2`, r.ReceivedAt.UTC(), prepared.id)
	}
	return err == nil, err == nil, err
}

func deliveryProviderID(prepared PreparedDeploymentInput) string {
	if prepared.action != nil {
		return prepared.action.ProviderEventID
	}
	if prepared.text != nil {
		return prepared.text.ProviderEventID
	}
	return ""
}

// Historical readback permits only retirement of these exact captured bytes;
// it never re-admits an action, parent, socket or source.
func LoadDeploymentInputReceipt(ctx context.Context, q actionQueryer, operationID, providerEventID string) (render.DeploymentInputReceipt, bool, error) {
	if uuid.Validate(operationID) != nil || providerEventID == "" {
		return render.DeploymentInputReceipt{}, false, fmt.Errorf("native operator receipt requires its original occurrence")
	}
	id := uuid.NewSHA1(uuid.MustParse(operationID), []byte("deployment-operator-input:"+providerEventID)).String()
	rows, err := q.QueryContext(ctx, `SELECT transport_parent,native_capture_fingerprint,provider_event_id FROM operator_channel_action_intents WHERE publication_id=$1
		UNION ALL SELECT transport_parent,native_capture_fingerprint,provider_event_id FROM operator_channel_text_intents WHERE publication_id=$1`, id)
	if err != nil {
		return render.DeploymentInputReceipt{}, false, err
	}
	var result render.DeploymentInputReceipt
	for rows.Next() {
		var raw []byte
		var fingerprint, eventID string
		if err := rows.Scan(&raw, &fingerprint, &eventID); err != nil {
			_ = rows.Close()
			return result, false, err
		}
		parent, err := channelnative.DecodeTransportParent(raw)
		if err != nil || parent.Kind != channelnative.TransportDeploymentRegistration || parent.ID != operationID ||
			fingerprint == "" || eventID != providerEventID || result.Fingerprint != "" && (result.Parent != parent || result.Fingerprint != fingerprint) {
			_ = rows.Close()
			return result, false, errors.Join(err, fmt.Errorf("native operator receipt evidence conflicts"))
		}
		result = render.DeploymentInputReceipt{Parent: parent, ProviderEventID: eventID, Fingerprint: fingerprint}
	}
	return result, result.Fingerprint != "", errors.Join(rows.Err(), rows.Close())
}

func requireInputParentCurrent(ctx context.Context, q intentQueryer, table, publicationID, interfaceKey string) error {
	if table != "operator_channel_action_intents" && table != "operator_channel_text_intents" {
		return fmt.Errorf("operator input parent requires its exact intent owner")
	}
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT transport_parent FROM `+table+` WHERE publication_id=$1`, publicationID).Scan(&raw); err != nil {
		return err
	}
	deployment, err := bindingUsesDeployment(ctx, q, interfaceKey)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		if deployment {
			return fmt.Errorf("deployment operator intent is missing its parent")
		}
		return nil
	}
	parent, err := channelnative.DecodeTransportParent(raw)
	if err != nil || parent.Kind != channelnative.TransportDeploymentRegistration || !deployment {
		return errors.Join(err, fmt.Errorf("operator intent parent kind changed"))
	}
	current, found, err := deploymentBindingParent(ctx, q, interfaceKey)
	if err != nil || !found || current != parent {
		return errors.Join(err, fmt.Errorf("operator intent parent is no longer current"))
	}
	return nil
}

func lockTextParentTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT transport_parent FROM operator_channel_text_intents WHERE publication_id=$1`, text.PublicationID).Scan(&raw); err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	parent, err := channelnative.DecodeTransportParent(raw)
	if err != nil || parent.Kind != channelnative.TransportDeploymentRegistration {
		return errors.Join(err, fmt.Errorf("operator text parent is incomplete"))
	}
	return LockTransportParentTx(ctx, tx, parent, postgres)
}

func LockActionInputParentTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, postgres bool) error {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT transport_parent FROM operator_channel_action_intents WHERE publication_id=$1`, action.PublicationID).Scan(&raw); err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	parent, err := channelnative.DecodeTransportParent(raw)
	if err != nil || parent.Kind != channelnative.TransportDeploymentRegistration {
		return errors.Join(err, fmt.Errorf("operator action parent is incomplete"))
	}
	return LockDeploymentTransportTx(ctx, tx, parent, postgres)
}

func originalResponseParentTx(ctx context.Context, tx *sql.Tx, publicationID, interfaceKey string) (channelnative.TransportParent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT transport_parent FROM operator_channel_text_intents WHERE publication_id=$1
		UNION ALL SELECT transport_parent FROM operator_channel_action_intents WHERE publication_id=$1`, publicationID)
	if err != nil {
		return channelnative.TransportParent{}, err
	}
	var parent channelnative.TransportParent
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return parent, err
		}
		selected, err := channelnative.DecodeTransportParent(raw)
		if err != nil || selected.Kind != channelnative.TransportDeploymentRegistration || parent.ID != "" && selected != parent {
			_ = rows.Close()
			return parent, errors.Join(err, fmt.Errorf("response requires its original exact deployment input parent"))
		}
		parent = selected
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return parent, err
	}
	current, found, err := deploymentBindingParent(ctx, tx, interfaceKey)
	if err != nil || !found || current != parent || parent.Validate() != nil {
		return parent, errors.Join(err, fmt.Errorf("response input parent is no longer current"))
	}
	return parent, nil
}
