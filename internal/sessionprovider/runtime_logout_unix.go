//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

// Preparation observes the original live owner. It neither opens a connection
// nor grants an unlink permit; selected teardown and journal admission follow.
func (c *RuntimeConnection) PrepareSessionLogout(ctx context.Context, expected channelonboarding.Operation) (target channelonboarding.SessionLogoutTarget, err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return target, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.currentOperation(work.Context())
	if err != nil {
		return target, err
	}
	if op.PrincipalID != expected.PrincipalID || op.Interface.Normalized() != expected.Interface.Normalized() {
		return target, errRuntimeConnection
	}
	provider, observed, err := c.ObserveSession(work.Context())
	if err != nil {
		return target, err
	}
	defer provider.CloseExecution()
	target = channelonboarding.SessionLogoutTarget{OperationID: op.OperationID, OperationRevision: op.Revision,
		OccurrenceID: observed.OccurrenceID, TargetSelector: op.TargetSelector,
		Account: observed.Admission, Coordinate: op.Coordinate}
	if !observed.Connected || !target.MatchesOperation(expected) {
		return channelonboarding.SessionLogoutTarget{}, errRuntimeConnection
	}
	if err := work.Context().Err(); err != nil {
		return channelonboarding.SessionLogoutTarget{}, err
	}
	return target, nil
}

func (c *RuntimeConnection) DispatchSessionLogout(ctx context.Context, responsibility channelonboarding.TeardownOperation) error {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	tail, err := c.beginLogoutSettlement()
	if err != nil {
		return errors.Join(err, work.Done())
	}
	owned, handle, occurrence, err := c.admitSessionLogout(work.Context(), responsibility)
	if err != nil {
		return errors.Join(err, work.Done(), tail.Done())
	}
	coordinate := responsibility.Logout.Coordinate
	settlementCtx := authoractivity.WithScope(tail.Context(), authoractivity.BundleScope(coordinate.RuntimeInstanceID, coordinate.BundleHash))
	settlementCtx = runtimeeffects.WithAuthority(settlementCtx, handle.Attempt().Authority)
	done := make(chan error, 1)
	go func() {
		err := executeSessionLogout(owned, settlementCtx, occurrence, handle, func(ctx context.Context) error {
			return c.requireOriginalLogoutOccurrence(ctx, responsibility)
		})
		done <- errors.Join(err, work.Done(), tail.Done())
	}()
	select {
	case err := <-done:
		return errors.Join(err, context.Cause(ctx))
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.ctx.Done():
		return context.Cause(c.ctx)
	}
}

// Journal/history settlement is accepted process work, not renewed execution
// in a retired runtime. The separate SDK context retains all dispatch fences.
func (c *RuntimeConnection) beginLogoutSettlement() (*worklifetime.Lease, error) {
	process, ok := worklifetime.ProcessFromContext(c.ctx)
	if !ok {
		return nil, errRuntimeConnection
	}
	return process.Begin(context.Background())
}

func (c *RuntimeConnection) admitSessionLogout(ctx context.Context, op channelonboarding.TeardownOperation) (context.Context, *runtimeeffects.Handle, *clientOccurrence, error) {
	if op.Kind != channelonboarding.TeardownLogout || op.Phase != channelonboarding.TeardownAuthorityRetired || op.Logout == nil || op.Revision < 1 {
		return nil, nil, nil, channelonboarding.ErrConflict
	}
	retained, err := c.store.GetChannelTeardown(ctx, op.TeardownID)
	if err != nil {
		return nil, nil, nil, err
	}
	if retained.Logout == nil || *retained.Logout != *op.Logout || retained.PrincipalID != op.PrincipalID ||
		retained.Revision != op.Revision || retained.Phase != op.Phase || retained.Scope != op.Scope {
		return nil, nil, nil, channelonboarding.ErrConflict
	}
	if err := c.requireOriginalLogoutOccurrence(ctx, retained); err != nil {
		return nil, nil, nil, err
	}
	raw, err := json.Marshal(retained.Logout)
	if err != nil {
		return nil, nil, nil, err
	}
	identity, err := runtimeeffects.ChannelLogoutOperationID(retained.TeardownID)
	if err != nil {
		return nil, nil, nil, err
	}
	coordinate := retained.Logout.Coordinate
	a := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityChannelLogout, ID: identity,
		ExecutionOwner: "channel-logout:" + retained.Logout.OccurrenceID, LeaseExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		FenceGeneration: uint64(retained.Revision), ExecutionMode: runtimeeffects.ExecutionModeLive,
		ChannelLogout: runtimeeffects.ChannelLogoutAuthority{EffectOperationID: identity, TeardownID: retained.TeardownID,
			TeardownRevision: retained.Revision, PrincipalID: retained.PrincipalID, BundleHash: coordinate.BundleHash,
			RuntimeInstanceID: coordinate.RuntimeInstanceID, TargetFingerprint: runtimeeffects.Fingerprint(raw)}}
	owned := runtimeeffects.WithAuthority(runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive), a)
	handle, err := runtimeeffects.BeginChannelLogout(owned, raw)
	return owned, handle, c.state.currentOccurrence(), err
}

func (c *RuntimeConnection) requireOriginalLogoutOccurrence(ctx context.Context, op channelonboarding.TeardownOperation) error {
	if c == nil || c.ctx == nil || c.ctx.Err() != nil || ctx == nil || ctx.Err() != nil || c.state == nil || op.Logout == nil {
		return errRuntimeConnection
	}
	target := op.Logout
	occurrence := c.state.currentOccurrence()
	if target.Validate() != nil || op.PrincipalID != c.operation.PrincipalID ||
		op.Scope.Interface.Normalized() != c.operation.Interface.Normalized() || target.OperationID != c.operation.OperationID ||
		target.OperationRevision < c.operation.Revision || target.TargetSelector != c.operation.TargetSelector ||
		target.Account != c.sessionAccount() || !target.Coordinate.MatchesRuntimeContext(c.operation.Coordinate) ||
		occurrence == nil || occurrence.occurrenceID != target.OccurrenceID || !c.state.ownsConnectedOccurrence(ctx, occurrence) ||
		occurrence.client.Store.Deleted || occurrence.client.Store.ID == nil || occurrence.client.Store.ID.ToNonAD().String() != target.Account.AccountRef {
		return errRuntimeConnection
	}
	return ctx.Err()
}

func executeSessionLogout(ctx, settlementCtx context.Context, occurrence *clientOccurrence, handle *runtimeeffects.Handle, preflight func(context.Context) error) error {
	workCtx, release, err := occurrence.prepareLogout(ctx)
	if err != nil {
		return handle.Fail(settlementCtx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassLifecycleConflict, "channel_logout_drain_failed", "channel-logout", "drain", nil, err)
	}
	launched := false
	launchErr := error(nil)
	func() {
		defer release()
		if err = preflight(workCtx); err != nil {
			return
		}
		launchErr = handle.MarkLaunched(workCtx)
		launched = launchErr == nil || runtimeeffects.CommittedMutationPhase(launchErr, runtimeeffects.MutationLaunch, handle.Attempt())
		if !launched {
			err = launchErr
			return
		}
		if err = preflight(workCtx); err != nil {
			return
		}
		err = occurrence.unlink(workCtx)
	}()
	if err != nil {
		state, class := runtimeeffects.StateTerminalFailure, runtimefailures.ClassLifecycleConflict
		if launched {
			state, class = runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain
		}
		return errors.Join(launchErr, handle.Fail(settlementCtx, state, class, "channel_logout_failed", "channel-logout", "unlink", nil, err))
	}
	// SDK success includes its owned local deletion; original socket, callbacks
	// and private transactions must also join before projecting complete success.
	if err := occurrence.join(settlementCtx); err != nil {
		return errors.Join(launchErr, handle.Fail(settlementCtx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain,
			"channel_logout_join_failed", "channel-logout", "join", nil, err))
	}
	if !occurrence.client.Store.Deleted {
		return handle.Fail(settlementCtx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain,
			"channel_logout_deletion_unconfirmed", "channel-logout", "settle", nil, fmt.Errorf("SDK unlink did not confirm original device deletion"))
	}
	return errors.Join(launchErr, handle.Succeed(settlementCtx, map[string]any{
		"remote_unlinked": true, "local_device_deleted": true, "original_joined": true,
	}))
}
