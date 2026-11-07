//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func (c *RuntimeConnection) AcknowledgeRegisteredOperator(ctx context.Context, store effects.Store, expected channelonboarding.Operation, child operatorchannel.Operation, binding operatorchannel.Binding) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	ctx = work.Context()
	op, err := c.currentOperation(ctx)
	if err != nil || !op.DeploymentRegistration || op.OperationID != expected.OperationID || op.Revision != expected.Revision ||
		op.IdentityOperationID != child.OperationID || child.State != operatorchannel.StateBound || binding.OperationID != child.OperationID {
		return errors.Join(errRuntimeConnection, err)
	}
	mutation, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return err
	}
	process := mutation.ProcessAuthority()
	parent := channelnative.TransportParent{Kind: "deployment_registration", ID: op.OperationID, Revision: op.Revision,
		BindingOperationID: child.OperationID, BindingOperationRevision: child.Revision, ProofID: binding.ProofID, ProofRevision: binding.ProofRevision}
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("registered-operator-ack-v1:"+child.OperationID+":"+fmt.Sprint(binding.Revision))).String()
	a := effects.Authority{Kind: effects.AuthorityChannelConfirmation, ID: id, ExecutionOwner: process.BootID,
		FenceGeneration: process.AuthorityGeneration, LeaseExpiresAt: time.Now().UTC().Add(5 * time.Minute), ExecutionMode: effects.ExecutionModeLive,
		ChannelConfirmation: effects.ChannelConfirmationAuthority{TransportParent: &parent, Destination: binding.ConversationRef,
			EffectOperationID: id, OnboardingOperationID: op.OperationID, OnboardingRevision: op.Revision, BindingRevision: binding.Revision, PrincipalID: op.PrincipalID}}
	reader, ok := store.(effects.OutcomeStore)
	if !ok || !a.Valid() || process.RuntimeInstanceID == "" {
		return fmt.Errorf("registered acknowledgement requires its exact journal and process authority")
	}
	if outcome, found, err := reader.GetExternalEffectOutcome(ctx, id); err != nil {
		return err
	} else if found {
		if outcome.Kind != effects.KindChannelConfirmation || outcome.AuthorityKind != a.Kind || outcome.AuthorityID != id {
			return fmt.Errorf("registered acknowledgement journal identity contradicts the binding")
		}
		if outcome.TerminalSuccess() {
			return nil
		}
		if outcome.AttemptState != effects.StateAuthorized && outcome.AttemptState != effects.StateTerminalFailure {
			return fmt.Errorf("registered acknowledgement outcome is %s; refusing blind resend", outcome.AttemptState)
		}
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(process.RuntimeInstanceID))
	ctx = effects.WithExecutionMode(ctx, a.ExecutionMode)
	ctx = effects.WithAuthority(ctx, a)
	ctx = effects.WithController(ctx, effects.NewController(store).WithExecutionPosture(executionposture.Live))
	owner, err := newSessionAuthorityOwner(c.state, c.operations, op, c.credentials, c.parent)
	if err != nil {
		return err
	}
	account, err := owner.AdmitSessionAccount(ctx, op.SessionAccount)
	if err != nil {
		return err
	}
	defer account.Close()
	occurrence := c.state.currentOccurrence()
	preflight := func(ctx context.Context) error {
		if !c.state.ownsConnectedOccurrence(ctx, occurrence) {
			return errClientOccurrenceFenced
		}
		return account.Validate(ctx, op.SessionAccount)
	}
	to, message, err := encodeOutboundText(binding.ConversationRef, "Swarm operator successfully connected", nil)
	if err != nil {
		return err
	}
	handle, err := effects.BeginInProcessChannelConfirmation(ctx, []byte(to.String()+"\nSwarm operator successfully connected"), nil)
	if err != nil {
		return err
	}
	messageID := strings.ReplaceAll(id, "-", "")
	response, launched, launchErr, err := executeChannelSend(ctx, occurrence, preflight, handle, to, message, messageID)
	if err != nil {
		_, err = registration.FailChannelWrite(ctx, handle, "registered_operator_acknowledgement", launched, launchErr, err)
		return err
	}
	if response.ID != messageID || response.Timestamp.IsZero() {
		_, err = registration.FailChannelWrite(ctx, handle, "registered_operator_acknowledgement", true, launchErr, fmt.Errorf("registered acknowledgement response mismatch"))
		return err
	}
	evidence := map[string]any{"provider": "whatsapp", "message_id": string(response.ID)}
	if err := handle.MarkResponseObserved(ctx, evidence); err != nil {
		return errors.Join(launchErr, err)
	}
	return errors.Join(launchErr, handle.Succeed(ctx, evidence))
}
