//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"net/http"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

// PollDeploymentClaim uses the original claim settlement owner; only an exact
// pending code from an authenticated provider read can enter it.
func PollDeploymentClaim(ctx context.Context, client *http.Client, operations channelonboarding.OperationStore, identities operatorchannel.Store, currentness *credentials.SnapshotOwner, channel packs.SatisfactionPlan, trigger providertriggers.InboundAdmissionPlan, op channelonboarding.Operation, offset int64) (int64, error) {
	identity, identityErr := channel.InterfaceIdentity()
	profile, profilePresent := channel.OnboardingProfile()
	if !op.DeploymentRegistration || !op.AccountConfirmed || op.Provider != "telegram" || op.IdentityOperationID == "" || !profilePresent ||
		identityErr != nil || identity.Normalized() != op.Interface.Normalized() || trigger.Provider() != op.Provider ||
		operations == nil || identities == nil || currentness == nil {
		return offset, channelonboarding.ErrInvalidRequest
	}
	admission, err := channelonboarding.ProviderCredentialAdmission(op, profile.ProviderCredential())
	if err != nil {
		return offset, err
	}
	evidence := credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}
	snapshot, current, err := currentness.ObserveValueMatchingSeal(ctx, evidence)
	if err != nil || !current {
		return offset, errors.Join(operatorchannel.ErrCredentialStale, err)
	}
	valid := func() bool {
		current, err := operations.GetChannelOnboarding(ctx, op.OperationID)
		matches, credentialErr := currentness.CurrentValueMatchesSeal(ctx, evidence)
		return err == nil && credentialErr == nil && matches && current.DeploymentRegistration && current.AccountConfirmed &&
			current.Revision == op.Revision && current.PrincipalID == op.PrincipalID && current.Provider == op.Provider &&
			current.AccountReference == op.AccountReference && current.IdentityOperationID == op.IdentityOperationID &&
			current.Interface == op.Interface && current.FailureCode == ""
	}
	return pollDeploymentIdentityClaim(ctx, client, identities, channel, trigger, op, snapshot.CredentialValue(), valid, offset)
}

func pollDeploymentIdentityClaim(ctx context.Context, client *http.Client, identities operatorchannel.Store, channel packs.SatisfactionPlan, trigger providertriggers.InboundAdmissionPlan, op channelonboarding.Operation, token string, valid func() bool, offset int64) (int64, error) {
	rows, err := identities.ListOperatorChannelOperations(ctx, op.PrincipalID)
	if err != nil {
		return offset, err
	}
	pollAuthorization := operatorchannel.Hash("authenticated-telegram-poll-v1", trigger.Generation().Diagnostic())
	confirm := func(identity operatorchannel.Operation, eventID string) (int64, error) {
		receipt, found, err := identities.LoadOperatorChannelClaimReceipt(ctx, operatorchannel.SessionClaimReceiptID(op.OperationID, eventID))
		if err != nil || !found || receipt.OperationID != identity.OperationID || receipt.Provider != op.Provider ||
			receipt.ProviderEventID != eventID || receipt.InterfaceKey != op.Interface.Key() || receipt.Challenge != identity.Challenge ||
			receipt.ProviderAuthorization != pollAuthorization || receipt.NativeCaptureFingerprint == "" {
			return offset, errors.Join(operatorchannel.ErrConflict, err)
		}
		if !valid() {
			return offset, operatorchannel.ErrCredentialStale
		}
		next, err := trigger.ConfirmTelegramClaim(ctx, client, token, eventID)
		if err != nil {
			return offset, err
		}
		return next, nil
	}
	for _, identity := range rows {
		if identity.OperationID != op.IdentityOperationID || identity.OnboardingOperationID != op.OperationID {
			continue
		}
		if identity.State == operatorchannel.StateAwaitingConfirmation && identity.ClaimAuthorization == pollAuthorization {
			return confirm(identity, identity.ClaimProviderEventID)
		}
		if identity.State != operatorchannel.StateAwaitingClaim {
			continue
		}
		claim, next, err := trigger.PollTelegramClaim(ctx, client, token, channel, identity.Challenge, offset)
		if err != nil || claim.Challenge == "" {
			return next, err
		}
		err = settleDeploymentClaim(ctx, identities, op, claim, valid)
		if err != nil {
			return offset, err
		}
		return confirm(identity, claim.ProviderEventID)
	}
	return offset, nil
}

func ConsumeDeploymentWebhookClaim(ctx context.Context, opts DeploymentConnectionOptions, identities operatorchannel.Store, account channelonboarding.AccountReference, credential func(channelonboarding.CredentialAdmission) bool, admit func() (providertriggers.Delivery, error), current func() bool) (bool, error) {
	if account.Provider != "telegram" || account.Validate() != nil || credential == nil || admit == nil || current == nil {
		return false, channelonboarding.ErrInvalidRequest
	}
	selected, err := opts.Plan.InterfaceIdentity()
	if err != nil {
		return false, err
	}
	profile, profilePresent := opts.Plan.OnboardingProfile()
	if !profilePresent {
		return false, channelonboarding.ErrInvalidRequest
	}
	rows, err := identities.ListOperatorChannelOperations(ctx, opts.Principal.ID)
	if err != nil {
		return false, err
	}
	for _, identity := range rows {
		if identity.ProviderAuthority.Scope != authority.DeploymentParent || identity.State != operatorchannel.StateAwaitingClaim && identity.State != operatorchannel.StateAwaitingConfirmation {
			continue
		}
		op, err := opts.Operations.GetChannelOnboarding(ctx, identity.OnboardingOperationID)
		if err != nil {
			return false, err
		}
		if !op.PendingDeploymentClaim(account) ||
			op.PrincipalID != opts.Principal.ID || identity.OperationID != op.IdentityOperationID || op.Interface.Normalized() != selected.Normalized() {
			continue
		}
		provider, err := channelonboarding.ProviderCredentialAdmission(op, profile.ProviderCredential())
		if err != nil {
			return false, err
		}
		if !current() {
			return false, channelonboarding.ErrConflict
		}
		valid := func() bool { return current() && credential(provider) }
		delivery, err := admit()
		if err != nil {
			return false, err
		}
		for _, output := range delivery.Events {
			fact, matches, err := opts.Plan.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
			if err != nil {
				return false, err
			}
			if !matches || fact.Text != identity.Challenge {
				continue
			}
			claim := operatorchannel.InboundClaim{TextFact: fact, Provider: op.Provider, ProviderEventID: delivery.ProviderEventID,
				Challenge: identity.Challenge, ProviderAuthorization: operatorchannel.Hash("authenticated-deployment-webhook-v1", op.AccountReference, output.Authorization.Generation().Diagnostic())}
			err = settleDeploymentClaim(ctx, identities, op, claim, valid)
			return err == nil, err
		}
	}
	return false, nil
}
