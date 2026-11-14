package serveapp

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/sessionprovider/execution"
)

// This is the existing owned connection role, not a provider registration API.
// Only platform construction supplies it; readback never creates a connection.
type serveSessionConnection interface {
	ConnectionID() string
	Closed() bool
	Close(context.Context) error
	Connect(context.Context) error
	CheckBootstrap(context.Context) error
	AwaitSessionAccount(context.Context) error
	CheckSessionReuse(context.Context, channelonboarding.Operation) (bool, error)
	CheckpointPairing(context.Context, int64) (channelonboarding.Operation, bool, error)
	PairingReadback(context.Context, operatorchannel.Principal) (channelonboarding.PairingReadback, error)
	AccountPresentation(context.Context) (string, string, error)
	DisconnectionReason() string
	FailureReport() (reason, cause string)
	ReadClaimCodeNotice(context.Context, channelonboarding.Operation) (*channelonboarding.ClaimCodeNotice, error)
	SessionPreviewConnected(context.Context, channelonboarding.Operation) (bool, error)
	UnclaimedCount(context.Context) (int64, error)
	AdmitSessionAccount(context.Context, operatorchannel.SessionAccountAdmission) (authority.Admission, error)
	AdmitActivityAccount(context.Context, operatorchannel.SessionAccountAdmission) (authority.Admission, error)
	ValidateActivityRequestAccount(context.Context, operatorchannel.SessionAccountAdmission, string, int64) error
	ObserveSession(context.Context) (operatorchannel.ProviderAuthority, operatorchannel.SessionConnectionObservation, error)
	SessionDiagnostic(context.Context) (sessionprovider.SessionDiagnostic, string, error)
	SessionRecovery(context.Context, channelonboarding.Operation) (time.Time, bool, error)
	RecoveryFailure(context.Context, channelonboarding.Operation, error) error
	ChannelExecution(context.Context) (execution.Channel, error)
	VerifyDeploymentRegistration(context.Context, channelonboarding.Operation) (bool, error)
	LogoutDeploymentRegistration(context.Context, channelonboarding.Operation) error
	AcknowledgeRegisteredOperator(context.Context, effects.Store, channelonboarding.Operation, operatorchannel.Operation, operatorchannel.Binding) error
	PrepareSourceRollover(context.Context) error
	PrepareSessionLogout(context.Context, channelonboarding.Operation) (channelonboarding.SessionLogoutPreparation, error)
	DispatchSessionLogout(context.Context, channelonboarding.TeardownOperation) error
	RevokedSession(context.Context, channelonboarding.Operation) (channelonboarding.SessionRevocation, error)
	ReconcileIncoming(context.Context) error
}
