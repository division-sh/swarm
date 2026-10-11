package effects

import (
	"context"
	"encoding/hex"
	"fmt"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

// ChannelLogoutAuthority identifies retained destructive responsibility. It is
// not session authority: the original private occurrence still admits unlink.
type ChannelLogoutAuthority struct {
	EffectOperationID string
	TeardownID        string
	TeardownRevision  int64
	PrincipalID       string
	BundleHash        string
	RuntimeInstanceID string
	TargetFingerprint string
}

func (a ChannelLogoutAuthority) valid() bool {
	id, err := ChannelLogoutOperationID(a.TeardownID)
	fingerprint, fingerprintErr := hex.DecodeString(a.TargetFingerprint)
	return err == nil && id == a.EffectOperationID && a.TeardownRevision > 0 &&
		validUUIDs(a.PrincipalID, a.RuntimeInstanceID) && nonEmpty(a.BundleHash) &&
		fingerprintErr == nil && len(fingerprint) == 32 && hex.EncodeToString(fingerprint) == a.TargetFingerprint
}

func (a Authority) validChannelLogout() bool {
	return a.ChannelLogout.valid() && a.ID == a.ChannelLogout.EffectOperationID &&
		a.FenceGeneration == uint64(a.ChannelLogout.TeardownRevision) && a.ExecutionMode == ExecutionModeLive &&
		a.Target == (UsageTarget{}) && len(a.BudgetScopes) == 0 && a.Normal == (LifecycleToken{}) &&
		a.SelectedFork == (SelectedContractForkAuthority{}) && a.ForkChat == (ConversationForkChatAuthority{}) &&
		a.StartupProbe == (StartupProbeAuthority{}) && a.ServeRegistration == (ServeRegistrationAuthority{}) &&
		a.ChannelConfirmation == (ChannelConfirmationAuthority{}) && a.ChannelDelivery == (ChannelDeliveryAuthority{}) &&
		a.ChannelActionAck == (ChannelActionAckAuthority{}) && a.ChannelNativeSetting == (ChannelNativeSettingAuthority{})
}

func BeginChannelLogout(ctx context.Context, request []byte) (*Handle, error) {
	const adapter = "channel_logout_whatsapp"
	if err := admitExecutionMode(ctx, adapter); err != nil {
		return nil, err
	}
	if _, differentOwner := DifferentOwnerFromContext(ctx); differentOwner {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_owner_conflict", "external-effects", "authorize_channel_logout", nil)
	}
	controller, ok := ControllerFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_channel_logout", nil)
	}
	authority, ok := AuthorityFromContext(ctx)
	if !ok || authority.Kind != AuthorityChannelLogout || Fingerprint(request) != authority.ChannelLogout.TargetFingerprint {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "channel_logout_authority_invalid", "external-effects", "authorize_channel_logout", nil)
	}
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: authority.ChannelLogout.EffectOperationID, Adapter: adapter, RequestFingerprint: Fingerprint(request),
	})
	return authorizedHandle(controller, attempt, err)
}

// ChannelLogoutOperationID binds the destructive responsibility to exactly one
// journal operation. Neither a caller's key nor a successor SDK selects it.
func ChannelLogoutOperationID(teardownID string) (string, error) {
	id, err := uuid.Parse(teardownID)
	if err != nil || id == uuid.Nil || id.String() != teardownID {
		return "", fmt.Errorf("channel logout requires its exact teardown identity")
	}
	return uuid.NewSHA1(id, []byte("channel.logout")).String(), nil
}
