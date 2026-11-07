package effects

import (
	"context"
	"fmt"
)

// RegisteredAcknowledgementSettled retains the journal's single retry policy
// for the registered acknowledgement, independently of its provider transport.
func RegisteredAcknowledgementSettled(ctx context.Context, reader OutcomeStore, id string) (bool, error) {
	outcome, found, err := reader.GetExternalEffectOutcome(ctx, id)
	if err != nil || !found {
		return false, err
	}
	if outcome.OperationID != id || outcome.Kind != KindChannelConfirmation || outcome.AuthorityKind != AuthorityChannelConfirmation || outcome.AuthorityID != id {
		return false, fmt.Errorf("registered acknowledgement journal identity contradicts the binding")
	}
	if outcome.TerminalSuccess() {
		return true, nil
	}
	if outcome.AttemptState != StateAuthorized && outcome.AttemptState != StateTerminalFailure {
		return false, fmt.Errorf("registered acknowledgement outcome is %s; refusing blind resend", outcome.AttemptState)
	}
	return false, nil
}
