//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"go.mau.fi/whatsmeow/socket"
)

func missingSessionDevice(err error) bool {
	return errors.Is(err, sessionstate.ErrSessionNotLinked) && failures.OnlyBranches(err, func(e error) bool { return e == errSessionAccount || e == sessionstate.ErrSessionNotLinked })
}

func (c *RuntimeConnection) RecoveryFailure(ctx context.Context, op channelonboarding.Operation, err error) error {
	if err != nil && failures.OnlyBranches(err, func(e error) bool { return e == context.Canceled || e == errClientOccurrenceFenced }) {
		at, _, readErr := c.SessionRecovery(ctx, op)
		if readErr == nil && !at.IsZero() {
			return channelonboarding.ErrSessionUnavailable
		}
	}
	return err
}

func transientConnectError(err error) bool {
	_, typed := failures.As(err)
	network := false
	only := failures.OnlyBranches(err, func(e error) bool {
		if e == socket.ErrDialFailed {
			return true
		}
		accepted := e == syscall.ECONNREFUSED || e == syscall.ECONNRESET || e == syscall.ENETUNREACH || e == syscall.EHOSTUNREACH || e == syscall.ETIMEDOUT
		network = network || accepted
		return accepted
	})
	return !typed && only && network
}

// SessionRecovery checks the live original owner; diagnostic facts are not permits.
func (c *RuntimeConnection) SessionRecovery(ctx context.Context, expected channelonboarding.Operation) (at time.Time, connected bool, err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return at, false, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.currentOperation(work.Context())
	if err != nil || op.Revision != expected.Revision || op.Provider != expected.Provider || op.PrincipalID != expected.PrincipalID || op.SessionAccount != expected.SessionAccount || op.OperationID != expected.OperationID || op.SessionConnectionID != expected.SessionConnectionID || op.AccountReference != expected.AccountReference || op.Coordinate != expected.Coordinate || op.Interface.Normalized() != expected.Interface.Normalized() || op.RequestHash != expected.RequestHash || (op.Phase != channelonboarding.PhaseSucceeded && (!op.DeploymentRegistration || op.Phase != channelonboarding.PhaseAwaitingExternalIdentity && op.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation)) || op.DeploymentRegistration && !op.AccountConfirmed || op.FailureCode != "" || op.LogoutCompleted {
		return at, false, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	o := c.state.currentOccurrence()
	if o == nil {
		return at, false, errRuntimeConnection
	}
	o.mu.Lock()
	revoked, start := o.revoked != nil, o.startFailure
	at, healthy, callbacks, permanent := o.disconnectedAt, !o.fenced && o.client.IsConnected() && o.client.IsLoggedIn(), o.callbacks, o.failureReason != "" && !o.transientDisconnect
	o.mu.Unlock()
	if _, err := c.state.device(work.Context()); err != nil {
		if missingSessionDevice(err) {
			return at, false, channelonboarding.ErrSessionNotLinked
		}
		return at, false, err
	}
	if revoked {
		return at, false, channelonboarding.ErrSessionNotLinked
	}
	if healthy {
		admission, err := c.AdmitSessionAccount(work.Context(), op.SessionAccount)
		admission.Close()
		return time.Time{}, err == nil, err
	}
	if start && !permanent && (callbacks == nil || callbacks.currentFailure() == nil) {
		return at, false, nil
	}
	err = c.CheckBootstrap(work.Context())
	if o.transientAdmissionRefusal(err) {
		return at, false, nil
	}
	return time.Time{}, false, err
}
